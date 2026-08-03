package master

import (
	"sort"
	"time"

	"gfs/internal/types"
	"gfs/internal/utils"
)

// ---- 命名空间操作 ----

// Create 创建文件并分配第一个 chunk。
func (m *Master) Create(args *types.CreateArgs, reply *types.CreateReply) error {
	path, err := utils.NormalizePath(args.Path)
	if err != nil || path == "/" {
		return types.ErrInvalidPath
	}
	if utils.IsDeletedPath(path) {
		return types.ErrInvalidPath
	}
	unlock := m.pl.lockFileWrite(path)
	defer unlock()

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.namespace[path]; ok {
		return types.ErrFileExists
	}
	fm := &types.FileMetadata{Path: path}
	m.namespace[path] = fm
	if err := m.appendOp(types.OpCreate, types.CreatePayload{Path: path}); err != nil {
		return err
	}
	handle, err := m.allocateChunkLocked(fm, 0)
	if err != nil {
		return err
	}
	reply.Handle = handle
	return nil
}

// Delete 惰性删除文件:移入 /.deleted/ 隐藏目录,延迟到 GC 阶段真正清理。
func (m *Master) Delete(args *types.DeleteArgs, reply *types.DeleteReply) error {
	path, err := utils.NormalizePath(args.Path)
	if err != nil || path == "/" {
		return types.ErrInvalidPath
	}
	unlock := m.pl.lockFileWrite(path)
	defer unlock()

	m.mu.Lock()
	defer m.mu.Unlock()

	fm := m.namespace[path]
	if fm == nil {
		return types.ErrFileNotFound
	}
	if fm.IsDeleted {
		return types.ErrFileNotFound
	}
	// 不支持删除带子条目的"目录",避免子树悬挂
	for p := range m.namespace {
		if len(p) > len(path) && p[:len(path)+1] == path+"/" {
			return types.ErrInvalidPath
		}
	}
	hidden := utils.HiddenPathFor(path)
	fm.Path = hidden
	fm.IsDeleted = true
	fm.DeletedAt = time.Now()
	m.namespace[hidden] = fm
	delete(m.namespace, path)
	return m.appendOp(types.OpDelete, types.DeletePayload{Path: path, HiddenPath: hidden})
}

// Rename 重命名文件;按字典序对源/目标路径加锁,防止死锁。
func (m *Master) Rename(args *types.RenameArgs, reply *types.RenameReply) error {
	src, err := utils.NormalizePath(args.Src)
	if err != nil || src == "/" {
		return types.ErrInvalidPath
	}
	dst, err := utils.NormalizePath(args.Dst)
	if err != nil || dst == "/" {
		return types.ErrInvalidPath
	}
	if utils.IsDeletedPath(src) || utils.IsDeletedPath(dst) {
		return types.ErrInvalidPath
	}
	unlock := m.pl.lockFileWrite(src, dst)
	defer unlock()

	m.mu.Lock()
	defer m.mu.Unlock()

	fm := m.namespace[src]
	if fm == nil {
		return types.ErrFileNotFound
	}
	if _, ok := m.namespace[dst]; ok {
		return types.ErrFileExists
	}
	// 不支持移动整个子树
	for p := range m.namespace {
		if len(p) > len(src) && p[:len(src)+1] == src+"/" {
			return types.ErrInvalidPath
		}
	}
	fm.Path = dst
	m.namespace[dst] = fm
	delete(m.namespace, src)
	return m.appendOp(types.OpRename, types.RenamePayload{Src: src, Dst: dst})
}

// GetChunkHandle 返回文件指定 chunk 索引的 handle。
func (m *Master) GetChunkHandle(args *types.GCHArgs, reply *types.GCHReply) error {
	path, err := utils.NormalizePath(args.Path)
	if err != nil {
		return types.ErrInvalidPath
	}
	unlock := m.pl.lockFileRead(path)
	defer unlock()

	m.mu.RLock()
	defer m.mu.RUnlock()

	fm := m.namespace[path]
	if fm == nil {
		return types.ErrFileNotFound
	}
	if args.ChunkIndex < 0 || args.ChunkIndex >= len(fm.Chunks) {
		return types.ErrIndexOutOfRange
	}
	reply.Handle = fm.Chunks[args.ChunkIndex]
	return nil
}

// AllocateChunk 为文件追加一个新 chunk,分配 handle 并选择副本放置。
// 幂等:若 index 已有 chunk 则直接返回现有 handle。
func (m *Master) AllocateChunk(args *types.ACArgs, reply *types.ACReply) error {
	path, err := utils.NormalizePath(args.Path)
	if err != nil {
		return types.ErrInvalidPath
	}
	unlock := m.pl.lockFileWrite(path)
	defer unlock()

	m.mu.Lock()
	defer m.mu.Unlock()

	fm := m.namespace[path]
	if fm == nil {
		return types.ErrFileNotFound
	}
	if args.ChunkIndex < 0 || args.ChunkIndex > len(fm.Chunks) {
		return types.ErrIndexOutOfRange
	}
	if args.ChunkIndex < len(fm.Chunks) {
		// 已存在,幂等返回
		h := fm.Chunks[args.ChunkIndex]
		return m.fillLocationsReply(h, reply)
	}
	handle, err := m.allocateChunkLocked(fm, args.ChunkIndex)
	if err != nil {
		return err
	}
	return m.fillLocationsReply(handle, reply)
}

// fillLocationsReply 填充 ACReply 的副本与主副本信息(需持有 m.mu 写锁或已授权租约)。
func (m *Master) fillLocationsReply(h types.ChunkHandle, reply *types.ACReply) error {
	cm := m.chunkMeta[h]
	if cm == nil {
		return types.ErrChunkNotFound
	}
	live := m.liveLocations(h)
	if len(live) == 0 {
		return types.ErrNoServer
	}
	lease := m.grantLeaseLocked(h, live, cm)
	reply.Handle = h
	reply.Primary = lease.Primary
	for _, s := range live {
		if s != lease.Primary {
			reply.Secondaries = append(reply.Secondaries, s)
		}
	}
	reply.Version = cm.Version
	return nil
}

// ---- 位置与租约 ----

// GetLocations 返回 chunk 的位置与租约信息。
// ForWrite 时先执行写时复制(COW)检查:若 chunk 被快照共享,则为目标文件
// 复制出新 chunk,返回新 handle。
func (m *Master) GetLocations(args *types.GLArgs, reply *types.GLReply) error {
	// 写路径需要命名空间写锁(COW 会修改文件 chunk 列表),须先于 m.mu 获取,
	// 与 Create/Delete/Rename 等操作的加锁顺序一致,避免死锁。
	var unlock func()
	if args.ForWrite {
		path, err := utils.NormalizePath(args.Path)
		if err != nil {
			return types.ErrInvalidPath
		}
		unlock = m.pl.lockFileWrite(path)
		defer unlock()
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	cm := m.chunkMeta[args.Handle]
	if cm == nil {
		return types.ErrChunkNotFound
	}

	handle := args.Handle
	if args.ForWrite && cm.RefCount > 1 {
		newHandle, err := m.cowChunkLocked(args.Path, args.ChunkIndex, args.Handle)
		if err != nil {
			return err
		}
		handle = newHandle
		cm = m.chunkMeta[handle]
	}

	live := m.liveLocations(handle)
	if len(live) == 0 {
		return types.ErrNoServer
	}
	lease := m.grantLeaseLocked(handle, live, cm)
	reply.Handle = handle
	reply.Primary = lease.Primary
	for _, s := range live {
		if s != lease.Primary {
			reply.Secondaries = append(reply.Secondaries, s)
		}
	}
	reply.Version = cm.Version
	return nil
}

// grantLeaseLocked 为 chunk 授予/续期租约(需持有 m.mu):
//   - 无租约或租约过期且主副本死亡:选择新主副本,必要时提升版本号;
//   - 已有有效租约:原样返回。
func (m *Master) grantLeaseLocked(h types.ChunkHandle, live []string, cm *types.ChunkMetadata) *types.LeaseInfo {
	if len(live) == 0 {
		// 无存活副本:无法授予租约,返回 nil,调用方会返回 ErrNoServer
		return nil
	}
	now := time.Now()
	lease := m.leases[h]
	if lease != nil && lease.ExpiresAt.After(now) && m.isPrimaryAlive(lease.Primary) {
		return lease
	}
	if lease != nil && lease.ExpiresAt.After(now) {
		// 租约未过期但主副本疑似死亡:等待租约自然过期(防脑裂),期间不可写
		// 由 GetLocations 直接失败,客户端重试
		return lease
	}

	var primary string
	if lease != nil && m.isPrimaryAlive(lease.Primary) {
		// 租约过期但原主存活:续期,不换主
		primary = lease.Primary
	} else {
		// 选择新主副本:优先机架多样、负载低的服务器
		cands := append([]string(nil), live...)
		sort.Slice(cands, func(i, j int) bool {
			si, sj := m.chunkServers[cands[i]], m.chunkServers[cands[j]]
			if (si == nil) != (sj == nil) {
				return si != nil
			}
			if len(si.Chunks) != len(sj.Chunks) {
				return len(si.Chunks) < len(sj.Chunks)
			}
			return cands[i] < cands[j]
		})
		primary = cands[0]
		if lease != nil && lease.Primary != primary {
			// 主副本变更:提升版本号,使陈旧的旧主副本失效
			cm.Version++
			_ = m.appendOp(types.OpUpdateVersion, types.UpdateVersionPayload{Handle: h, Version: cm.Version})
		}
	}
	lease = &types.LeaseInfo{
		Primary:   primary,
		ExpiresAt: now.Add(m.config.LeaseTimeout),
		Version:   cm.Version,
	}
	m.leases[h] = lease
	_ = m.appendOp(types.OpGrantLease, types.GrantLeasePayload{
		Handle: h, Primary: primary, ExpiresAt: lease.ExpiresAt, Version: cm.Version,
	})
	return lease
}

// ---- 快照与统计 ----

// Snapshot 对文件创建快照:共享所有 chunk 并增加引用计数,
// 后续对原文件的写入会触发写时复制。
func (m *Master) Snapshot(args *types.SnapshotArgs, reply *types.SnapshotReply) error {
	src, err := utils.NormalizePath(args.Path)
	if err != nil || src == "/" {
		return types.ErrInvalidPath
	}
	dst, err := utils.NormalizePath(args.SnapPath)
	if err != nil || dst == "/" {
		return types.ErrInvalidPath
	}
	if utils.IsDeletedPath(src) || utils.IsDeletedPath(dst) {
		return types.ErrInvalidPath
	}
	unlock := m.pl.lockFileWrite(src, dst)
	defer unlock()

	m.mu.Lock()
	defer m.mu.Unlock()

	fm := m.namespace[src]
	if fm == nil {
		return types.ErrFileNotFound
	}
	if _, ok := m.namespace[dst]; ok {
		return types.ErrFileExists
	}
	snap := &types.FileMetadata{
		Path:         dst,
		Chunks:       append([]types.ChunkHandle(nil), fm.Chunks...),
		Size:         fm.Size,
		SnapRefCount: fm.SnapRefCount + 1,
	}
	fm.SnapRefCount++
	m.namespace[dst] = snap
	for _, h := range fm.Chunks {
		if cm := m.chunkMeta[h]; cm != nil {
			cm.RefCount++
		}
	}
	return m.appendOp(types.OpSnapshot, types.SnapshotPayload{Path: src, SnapPath: dst})
}

// Stat 返回文件大小与 chunk 数量。
func (m *Master) Stat(args *types.StatArgs, reply *types.StatReply) error {
	path, err := utils.NormalizePath(args.Path)
	if err != nil {
		return types.ErrInvalidPath
	}
	unlock := m.pl.lockFileRead(path)
	defer unlock()

	m.mu.RLock()
	defer m.mu.RUnlock()

	fm := m.namespace[path]
	if fm == nil {
		return types.ErrFileNotFound
	}
	reply.Size = fm.Size
	reply.ChunkCount = len(fm.Chunks)
	return nil
}

// UpdateSize 客户端在每次写成功后上报文件大小,Master 取最大值(容忍乱序)。
func (m *Master) UpdateSize(args *types.UpdateSizeArgs, reply *types.UpdateSizeReply) error {
	path, err := utils.NormalizePath(args.Path)
	if err != nil {
		return types.ErrInvalidPath
	}
	unlock := m.pl.lockFileWrite(path)
	defer unlock()

	m.mu.Lock()
	defer m.mu.Unlock()

	fm := m.namespace[path]
	if fm == nil || fm.IsDeleted {
		return types.ErrFileNotFound
	}
	if args.Size > fm.Size {
		fm.Size = args.Size
		return m.appendOp(types.OpUpdateSize, types.UpdateSizePayload{Path: path, Size: args.Size})
	}
	return nil
}

// ---- 心跳与上报 ----

// Heartbeat 处理 ChunkServer 心跳:
//   - 登记服务器存活状态,重建 chunk 位置信息;
//   - 对比版本号:低版本副本标记为陈旧,高版本(主节点重启落后)予以采纳;
//   - 为仍是主副本的服务器续期租约,并返回已失效/版本变化的租约列表。
func (m *Master) Heartbeat(args *types.HBArgs, reply *types.HBReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	state := m.chunkServers[args.Addr]
	if state == nil {
		state = &types.ChunkServerState{
			Address: args.Addr,
			Rack:    args.Rack,
			Chunks:  make(map[types.ChunkHandle]bool),
		}
		m.chunkServers[args.Addr] = state
	}
	if args.Rack != "" {
		state.Rack = args.Rack
	}
	state.DiskUsage = args.DiskUsage
	state.LastHeartbeat = now

	for _, cr := range args.Chunks {
		state.Chunks[cr.Handle] = true
		loc := m.chunkLocations[cr.Handle]
		if loc == nil {
			loc = make(map[string]bool)
			m.chunkLocations[cr.Handle] = loc
		}
		cm := m.chunkMeta[cr.Handle]
		if cm == nil {
			// 未知 chunk(孤儿/尚未记录):创建占位元数据,等待 GC 清理或正式引用
			m.chunkMeta[cr.Handle] = &types.ChunkMetadata{Handle: cr.Handle, Version: cr.Version}
		} else if cr.Version < cm.Version {
			// 陈旧副本:不作为有效位置
			delete(loc, args.Addr)
			continue
		} else if cr.Version > cm.Version {
			// Master 重启后数据落后:采纳更高的版本
			cm.Version = cr.Version
			_ = m.appendOp(types.OpUpdateVersion, types.UpdateVersionPayload{Handle: cr.Handle, Version: cr.Version})
		}
		loc[args.Addr] = true
	}

	// 租约续期与失效通知
	for h, lease := range m.leases {
		if lease.Primary != args.Addr {
			continue
		}
		cm := m.chunkMeta[h]
		version := types.ChunkVersion(0)
		if cm != nil {
			version = cm.Version
		}
		if !lease.ExpiresAt.After(now) {
			// 租约已过期:告知该服务器失去主副本身份
			delete(m.leases, h)
			reply.ExpiredLeases = append(reply.ExpiredLeases, types.ExpiredLease{Handle: h, Version: version})
			continue
		}
		if lease.Version != version {
			// 版本已变化(主副本切换):同样告知失效
			reply.ExpiredLeases = append(reply.ExpiredLeases, types.ExpiredLease{Handle: h, Version: version})
			continue
		}
		// 主副本心跳续期:每次心跳把租约延长到 LeaseTimeout
		if lease.ExpiresAt.Before(now.Add(m.config.LeaseTimeout)) {
			lease.ExpiresAt = now.Add(m.config.LeaseTimeout)
		}
	}
	return nil
}

// ReportChunk 处理 ChunkServer 启动时的单 chunk 上报,语义与心跳中的 chunk 处理一致。
func (m *Master) ReportChunk(args *types.RCArgs, reply *types.RCReply) error {
	return m.Heartbeat(&types.HBArgs{
		Addr:   args.Addr,
		Chunks: []types.ChunkReport{{Handle: args.Handle, Version: args.Version}},
	}, &types.HBReply{})
}
