package master

import (
	"log"
	"sort"
	"time"

	"gfs/internal/rpc"
	"gfs/internal/types"
)

// ---- chunk 分配与写时复制 ----

// allocateChunkLocked 为文件分配一个新 chunk:自增 handle、选择副本、预创建空文件、授予租约。
// 调用方需持有 m.mu 与命名空间锁。
func (m *Master) allocateChunkLocked(fm *types.FileMetadata, index int) (types.ChunkHandle, error) {
	handle := m.nextHandle
	m.nextHandle++
	cm := &types.ChunkMetadata{Handle: handle, Version: 1, RefCount: 1}
	m.chunkMeta[handle] = cm
	fm.Chunks = append(fm.Chunks, handle)

	servers := m.pickServers(m.config.ReplicationFactor, nil)
	if len(servers) == 0 {
		// 无可用服务器:回滚分配
		delete(m.chunkMeta, handle)
		fm.Chunks = fm.Chunks[:len(fm.Chunks)-1]
		m.nextHandle--
		return 0, types.ErrNoServer
	}
	loc := make(map[string]bool)
	for _, s := range servers {
		loc[s] = true
	}
	m.chunkLocations[handle] = loc
	// 异步预创建空 chunk 文件:保证 Master 重启后位置信息可经心跳重建
	for _, s := range servers {
		m.goCreateChunk(s, handle, 1)
	}
	// 授予租约(选择主副本)
	live := m.liveLocations(handle)
	_ = m.grantLeaseLocked(handle, live, cm)
	if err := m.appendOp(types.OpAllocateChunk, types.AllocateChunkPayload{
		Path: fm.Path, Index: index, Handle: handle, Version: 1,
	}); err != nil {
		return 0, err
	}
	return handle, nil
}

// cowChunkLocked 执行写时复制:当 chunk 被多个文件共享(快照)且即将被写入时,
// 为目标文件复制出新 chunk,旧 chunk 保持只读。
// 复制是同步的,保证客户端在写入前新副本数据已就绪。
func (m *Master) cowChunkLocked(path string, index int, old types.ChunkHandle) (types.ChunkHandle, error) {
	fm := m.namespace[path]
	if fm == nil {
		return 0, types.ErrFileNotFound
	}
	oldCM := m.chunkMeta[old]
	if oldCM == nil {
		return 0, types.ErrChunkNotFound
	}
	if oldCM.RefCount <= 1 {
		return old, nil // 没有被共享,无需复制
	}
	live := m.liveLocations(old)
	if len(live) == 0 {
		return 0, types.ErrNoServer
	}

	newHandle := m.nextHandle
	m.nextHandle++
	newCM := &types.ChunkMetadata{Handle: newHandle, Version: 1, RefCount: 1}
	m.chunkMeta[newHandle] = newCM
	fm.Chunks[index] = newHandle
	oldCM.RefCount--

	servers := m.pickServers(m.config.ReplicationFactor, nil)
	loc := make(map[string]bool)
	// 源副本:持有旧 chunk 的存活服务器
	src := live[0]
	for _, s := range servers {
		loc[s] = true
	}
	m.chunkLocations[newHandle] = loc

	// 同步复制:源端读旧 handle,本地写新 handle
	succeeded := 0
	for _, s := range servers {
		if err := m.callChunkServer(s, rpc.ServiceMethod(types.ChunkServerService, "CopyChunk"),
			&types.CopyArgs{Handle: newHandle, Version: 1, SourceServer: src, SourceHandle: old}, &types.CopyReply{}); err != nil {
			// 复制失败的目标:移除位置记录,等待副本再复制任务补齐
			log.Printf("master: cow copy chunk %d to %s: %v", newHandle, s, err)
			delete(loc, s)
		} else {
			succeeded++
		}
	}
	if succeeded == 0 {
		// 全部失败:回滚,保持旧 chunk 可写(客户端下次写入会再次触发 COW)
		delete(m.chunkMeta, newHandle)
		delete(m.chunkLocations, newHandle)
		delete(m.leases, newHandle)
		fm.Chunks[index] = old
		oldCM.RefCount++
		m.nextHandle--
		return 0, types.ErrNoServer
	}

	if err := m.appendOp(types.OpAllocateChunk, types.AllocateChunkPayload{
		Path: fm.Path, Index: index, Handle: newHandle, Version: 1,
	}); err != nil {
		return 0, err
	}
	if err := m.appendOp(types.OpSnapshotRef, types.SnapshotRefPayload{
		Path: path, Index: index, OldHandle: old, NewHandle: newHandle,
	}); err != nil {
		return 0, err
	}
	// 授予新 chunk 的租约
	liveNew := m.liveLocations(newHandle)
	_ = m.grantLeaseLocked(newHandle, liveNew, newCM)
	return newHandle, nil
}

// ---- 后台任务 ----

// startBackground 启动心跳监控、垃圾回收、副本再复制三个后台任务。
func (m *Master) startBackground() {
	m.wg.Add(3)
	go m.monitorLoop()
	go m.gcLoop()
	go m.replicationLoop()
}

// monitorLoop 周期性检测心跳超时的 ChunkServer,将其从位置信息中剔除。
func (m *Master) monitorLoop() {
	defer m.wg.Done()
	interval := m.config.HeartbeatTimeout / 2
	if interval < 50*time.Millisecond {
		interval = 50 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-m.stopCh:
			return
		case <-ticker.C:
			m.monitorHeartbeats()
		}
	}
}

func (m *Master) monitorHeartbeats() {
	m.mu.Lock()
	defer m.mu.Unlock()
	dead := time.Now().Add(-m.config.HeartbeatTimeout)
	for addr, st := range m.chunkServers {
		if !st.LastHeartbeat.Before(dead) {
			continue
		}
		delete(m.chunkServers, addr)
		// 该服务器持有的所有副本位置一并失效
		for _, loc := range m.chunkLocations {
			delete(loc, addr)
		}
	}
}

// gcLoop 垃圾回收:阶段一清理过期隐藏文件,阶段二清除无引用的孤儿 chunk。
func (m *Master) gcLoop() {
	defer m.wg.Done()
	ticker := time.NewTicker(m.config.GCInterval)
	defer ticker.Stop()
	for {
		select {
		case <-m.stopCh:
			return
		case <-ticker.C:
			m.gcOnce()
		}
	}
}

func (m *Master) gcOnce() {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	// 阶段一:删除超过 GCDelay 的隐藏文件,释放其 chunk 引用
	var expired []string
	for p, fm := range m.namespace {
		if fm.IsDeleted && now.Sub(fm.DeletedAt) > m.config.GCDelay {
			expired = append(expired, p)
		}
	}
	sort.Strings(expired)
	for _, p := range expired {
		fm := m.namespace[p]
		for _, h := range fm.Chunks {
			if cm := m.chunkMeta[h]; cm != nil && cm.RefCount > 0 {
				cm.RefCount--
			}
		}
		delete(m.namespace, p)
		if err := m.appendOp(types.OpDelete, types.DeletePayload{Path: p}); err != nil {
			log.Printf("master: gc log delete %s: %v", p, err)
		}
	}

	// 阶段二:扫描无引用的孤儿 chunk,通知所有持有者删除数据
	var orphans []types.ChunkHandle
	for h, cm := range m.chunkMeta {
		if cm.RefCount <= 0 {
			orphans = append(orphans, h)
		}
	}
	sort.Slice(orphans, func(i, j int) bool { return orphans[i] < orphans[j] })
	for _, h := range orphans {
		for addr := range m.chunkLocations[h] {
			m.goDeleteChunk(addr, h)
		}
		delete(m.chunkLocations, h)
		delete(m.chunkMeta, h)
		delete(m.leases, h)
		if err := m.appendOp(types.OpDeleteChunk, types.DeleteChunkPayload{Handle: h}); err != nil {
			log.Printf("master: gc log delete chunk %d: %v", h, err)
		}
	}
}

// replicationLoop 副本再复制:定期检查被引用 chunk 的存活副本数,
// 低于复制因子时从现存副本复制到新服务器,只剩 1 个副本的 chunk 优先处理。
func (m *Master) replicationLoop() {
	defer m.wg.Done()
	ticker := time.NewTicker(m.config.RecheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-m.stopCh:
			return
		case <-ticker.C:
			m.replicationOnce()
		}
	}
}

type replicateJob struct {
	handle types.ChunkHandle
	live   []string
}

func (m *Master) replicationOnce() {
	m.mu.RLock()
	var jobs []replicateJob
	for h, cm := range m.chunkMeta {
		if cm.RefCount <= 0 {
			continue // 孤儿 chunk 交给 GC
		}
		live := m.liveLocations(h)
		if len(live) >= m.config.ReplicationFactor || len(live) == 0 {
			continue
		}
		jobs = append(jobs, replicateJob{handle: h, live: live})
	}
	// 只剩 1 个副本的 chunk 优先
	sort.Slice(jobs, func(i, j int) bool { return len(jobs[i].live) < len(jobs[j].live) })
	m.mu.RUnlock()

	for _, job := range jobs {
		m.csMu.Lock()
		if m.copying[job.handle] {
			m.csMu.Unlock()
			continue
		}
		m.copying[job.handle] = true
		m.csMu.Unlock()

		go func(j replicateJob) {
			defer func() {
				m.csMu.Lock()
				delete(m.copying, j.handle)
				m.csMu.Unlock()
			}()
			if err := m.replicateChunk(j); err != nil {
				log.Printf("master: replicate chunk %d: %v", j.handle, err)
			}
		}(job)
	}
}

// replicateChunk 为 chunk 补齐缺失副本:选源副本(优先非主副本,避免复制到半写状态),
// 机架感知地选择目标服务器并逐个 CopyChunk。
func (m *Master) replicateChunk(j replicateJob) error {
	m.mu.RLock()
	cm := m.chunkMeta[j.handle]
	if cm == nil {
		m.mu.RUnlock()
		return nil
	}
	exclude := make(map[string]bool)
	for _, s := range j.live {
		exclude[s] = true
	}
	targets := m.pickServers(m.config.ReplicationFactor-len(j.live), exclude)
	version := cm.Version
	lease := m.leases[j.handle]
	m.mu.RUnlock()

	if len(targets) == 0 {
		return nil
	}
	// 源副本:优先选择非主副本
	src := j.live[0]
	if lease != nil {
		for _, s := range j.live {
			if s != lease.Primary {
				src = s
				break
			}
		}
	}
	for _, t := range targets {
		if err := m.callChunkServer(t, rpc.ServiceMethod(types.ChunkServerService, "CopyChunk"),
			&types.CopyArgs{Handle: j.handle, Version: version, SourceServer: src}, &types.CopyReply{}); err != nil {
			return err
		}
		// 复制成功后登记位置(心跳会随后确认版本)
		m.mu.Lock()
		loc := m.chunkLocations[j.handle]
		if loc == nil {
			loc = make(map[string]bool)
			m.chunkLocations[j.handle] = loc
		}
		loc[t] = true
		m.mu.Unlock()
	}
	return nil
}

// ---- Master 主动调用 ChunkServer 的通信辅助 ----

// chunkServerClient 获取到指定 ChunkServer 的连接(带缓存)。
func (m *Master) chunkServerClient(addr string) (*rpc.Client, error) {
	m.csMu.Lock()
	defer m.csMu.Unlock()
	if c := m.csClients[addr]; c != nil {
		return c, nil
	}
	c, err := rpc.Dial(addr)
	if err != nil {
		return nil, err
	}
	m.csClients[addr] = c
	return c, nil
}

// callChunkServer 同步调用 ChunkServer;失败时关闭缓存连接。
func (m *Master) callChunkServer(addr, method string, args, reply any) error {
	c, err := m.chunkServerClient(addr)
	if err != nil {
		return err
	}
	err = c.Call(method, args, reply)
	if err != nil {
		m.csMu.Lock()
		_ = c.Close()
		delete(m.csClients, addr)
		m.csMu.Unlock()
	}
	return err
}

// goCreateChunk 异步通知服务器预创建空 chunk 文件。
func (m *Master) goCreateChunk(addr string, h types.ChunkHandle, v types.ChunkVersion) {
	go func() {
		_ = m.callChunkServer(addr, rpc.ServiceMethod(types.ChunkServerService, "CreateChunk"),
			&types.CreateChunkArgs{Handle: h, Version: v}, &types.CreateChunkReply{})
	}()
}

// goDeleteChunk 异步通知服务器删除 chunk 数据(GC 阶段二)。
func (m *Master) goDeleteChunk(addr string, h types.ChunkHandle) {
	go func() {
		_ = m.callChunkServer(addr, rpc.ServiceMethod(types.ChunkServerService, "DeleteChunk"),
			&types.DeleteChunkArgs{Handle: h}, &types.DeleteChunkReply{})
	}()
}
