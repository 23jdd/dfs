// Package master 实现 GFS 主节点(Master):
//   - 维护命名空间、文件到 chunk 的映射、chunk 版本号与租约;
//   - 通过 checkpoint + 操作日志持久化所有元数据;
//   - chunk 位置信息不持久化,通过 ChunkServer 心跳重建;
//   - 后台负责心跳监控、垃圾回收与副本再复制。
package master

import (
	"log"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"gfs/internal/rpc"
	"gfs/internal/types"
)

// Master 是主节点核心结构。
type Master struct {
	mu sync.RWMutex

	// 命名空间:文件路径 -> 文件元数据(持久化)
	namespace map[string]*types.FileMetadata

	// chunk 元数据:handle -> 元数据(持久化)
	chunkMeta map[types.ChunkHandle]*types.ChunkMetadata

	// chunk 位置:handle -> set<ChunkServerAddr>
	// 注意:不持久化,由心跳重建,Master 重启后首轮心跳即可恢复。
	chunkLocations map[types.ChunkHandle]map[string]bool

	// 租约:handle -> 租约信息(不持久化)
	leases map[types.ChunkHandle]*types.LeaseInfo

	// 已注册的 ChunkServer:addr -> 状态
	chunkServers map[string]*types.ChunkServerState

	// 内存中的操作日志镜像(持久化到 op.log)
	opLog []types.OperationLogEntry

	// 自增 chunk handle 分配器(持久化)
	nextHandle types.ChunkHandle

	config *types.Config
	pl     *pathLocker

	// ---- 生命周期 ----
	dir       string
	opFile    *file     // 操作日志文件
	opCount   int       // 自上次 checkpoint 以来的操作数
	rpcServer *rpc.Server
	stopCh    chan struct{}
	wg        sync.WaitGroup
	stopped   atomic.Bool

	// ---- 与 ChunkServer 通信 ----
	csClients map[string]*rpc.Client
	csMu      sync.Mutex

	// 正在进行的副本再复制任务(去重)
	copying map[types.ChunkHandle]bool
}

// New 创建 Master 并加载持久化状态(checkpoint + 操作日志回放)。
func New(cfg *types.Config, dir string) (*Master, error) {
	m := &Master{
		namespace:      make(map[string]*types.FileMetadata),
		chunkMeta:      make(map[types.ChunkHandle]*types.ChunkMetadata),
		chunkLocations: make(map[types.ChunkHandle]map[string]bool),
		leases:         make(map[types.ChunkHandle]*types.LeaseInfo),
		chunkServers:   make(map[string]*types.ChunkServerState),
		nextHandle:     1,
		config:         cfg,
		pl:             newPathLocker(),
		dir:            dir,
		stopCh:         make(chan struct{}),
		csClients:      make(map[string]*rpc.Client),
		copying:        make(map[types.ChunkHandle]bool),
	}
	if err := m.loadState(); err != nil {
		return nil, err
	}
	return m, nil
}

// Addr 返回 Master 实际监听地址。
func (m *Master) Addr() string {
	if m.rpcServer == nil {
		return ""
	}
	return m.rpcServer.Addr()
}

// Start 启动 RPC 服务与后台任务。
func (m *Master) Start(addr string) error {
	srv, err := rpc.Listen(addr)
	if err != nil {
		return err
	}
	if err := srv.Register(types.MasterService, m); err != nil {
		srv.Close()
		return err
	}
	m.rpcServer = srv
	go func() { _ = srv.Serve() }()
	m.startBackground()
	return nil
}

// Stop 停止后台任务、保存 checkpoint 并关闭监听。
func (m *Master) Stop() {
	if m.stopped.Swap(true) {
		return
	}
	close(m.stopCh)
	m.wg.Wait()
	if err := m.saveCheckpoint(); err != nil {
		log.Printf("master: save checkpoint on stop: %v", err)
	}
	if m.opFile != nil {
		m.opFile.close()
		m.opFile = nil
	}
	if m.rpcServer != nil {
		_ = m.rpcServer.Close()
	}
	m.csMu.Lock()
	for addr, c := range m.csClients {
		_ = c.Close()
		delete(m.csClients, addr)
	}
	m.csMu.Unlock()
}

// ---- 诊断接口(供测试使用,不注册为 RPC 方法) ----

// LiveServerCount 返回最近有心跳的 ChunkServer 数量。
func (m *Master) LiveServerCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	dead := time.Now().Add(-m.config.HeartbeatTimeout)
	n := 0
	for _, st := range m.chunkServers {
		if st.LastHeartbeat.After(dead) {
			n++
		}
	}
	return n
}

// ReplicaCount 返回指定 chunk 的存活副本数。
func (m *Master) ReplicaCount(h types.ChunkHandle) int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.liveLocations(h))
}

// PrimaryOf 返回指定 chunk 当前的主副本地址(诊断/测试辅助):
// 优先返回存活租约的主副本,否则返回任一存活副本。
func (m *Master) PrimaryOf(h types.ChunkHandle) string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if lease := m.leases[h]; lease != nil && m.isPrimaryAlive(lease.Primary) {
		return lease.Primary
	}
	live := m.liveLocations(h)
	if len(live) == 0 {
		return ""
	}
	return live[0]
}

// ChunkVersion 返回指定 chunk 的版本号(诊断/测试辅助)。
func (m *Master) ChunkVersion(h types.ChunkHandle) types.ChunkVersion {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if cm := m.chunkMeta[h]; cm != nil {
		return cm.Version
	}
	return 0
}

// ChunkReplicaStats 返回所有被引用 chunk 的副本数统计。
func (m *Master) ChunkReplicaStats() map[types.ChunkHandle]int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	stats := make(map[types.ChunkHandle]int)
	for h, cm := range m.chunkMeta {
		if cm.RefCount > 0 {
			stats[h] = len(m.liveLocations(h))
		}
	}
	return stats
}

// ---- 内部辅助 ----

// liveLocations 返回 chunk 在仍存活服务器上的位置(需持有 m.mu)。
func (m *Master) liveLocations(h types.ChunkHandle) []string {
	dead := time.Now().Add(-m.config.HeartbeatTimeout)
	var live []string
	for addr := range m.chunkLocations[h] {
		if st := m.chunkServers[addr]; st != nil && st.LastHeartbeat.After(dead) {
			live = append(live, addr)
		}
	}
	return live
}

// pickServers 机架感知地选择 count 个 ChunkServer 作为副本放置目标:
// 优先分散到不同机架(至少 2 个机架),同机架内选择持有 chunk 最少的服务器。
func (m *Master) pickServers(count int, exclude map[string]bool) []string {
	type cand struct {
		addr string
		rack string
		n    int
	}
	var cands []cand
	for addr, st := range m.chunkServers {
		if st.LastHeartbeat.Before(time.Now().Add(-m.config.HeartbeatTimeout)) {
			continue // 跳过已死亡的服务器
		}
		if exclude[addr] {
			continue
		}
		cands = append(cands, cand{addr: addr, rack: st.Rack, n: len(st.Chunks)})
	}
	if len(cands) == 0 {
		return nil
	}
	// 按机架分组,机架间轮转,每个机架内按 chunk 数升序
	byRack := make(map[string][]cand)
	var racks []string
	rackSeen := make(map[string]bool)
	for _, c := range cands {
		if !rackSeen[c.rack] {
			rackSeen[c.rack] = true
			racks = append(racks, c.rack)
		}
		byRack[c.rack] = append(byRack[c.rack], c)
	}
	sort.Strings(racks)
	for r := range byRack {
		sort.Slice(byRack[r], func(i, j int) bool {
			if byRack[r][i].n != byRack[r][j].n {
				return byRack[r][i].n < byRack[r][j].n
			}
			return byRack[r][i].addr < byRack[r][j].addr
		})
	}
	var picked []string
	for round := 0; len(picked) < count && round < len(cands); round++ {
		for _, r := range racks {
			group := byRack[r]
			if round < len(group) {
				picked = append(picked, group[round].addr)
				if len(picked) >= count {
					break
				}
			}
		}
	}
	return picked
}

// isPrimaryAlive 判断主副本是否仍存活(需持有 m.mu)。
func (m *Master) isPrimaryAlive(primary string) bool {
	if primary == "" {
		return false
	}
	st := m.chunkServers[primary]
	if st == nil {
		return false
	}
	return st.LastHeartbeat.After(time.Now().Add(-m.config.HeartbeatTimeout))
}
