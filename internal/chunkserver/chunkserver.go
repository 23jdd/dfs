// Package chunkserver 实现 GFS 块服务器(ChunkServer):
//   - 每个 chunk 一个独立文件,附带 checksum 边车文件(.cks);
//   - 数据推送采用流水线转发,控制阶段由主副本统一下发偏移;
//   - 周期心跳向 Master 报告持有的 chunk 与版本,支持位置重建。
package chunkserver

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"gfs/internal/rpc"
	"gfs/internal/types"
)

// ChunkServer 是块服务器核心结构。
type ChunkServer struct {
	addr       string
	rack       string
	masterAddr string
	dataDir    string
	cfg        *types.Config

	// 本地存储的 chunks:handle -> 本地文件路径
	mu       sync.Mutex
	chunks   map[types.ChunkHandle]string
	versions map[types.ChunkHandle]types.ChunkVersion

	// 每 chunk 互斥锁:串行化同一 chunk 的写入/追加/读取,保证偏移一致性
	chunkLocks   map[types.ChunkHandle]*sync.Mutex
	chunkLocksMu sync.Mutex

	// 数据推送缓冲区:dataID -> 数据(流水线阶段暂存,应用后删除)
	dataBuffer map[uint64]bufferedData

	rpcServer *rpc.Server
	stopCh    chan struct{}
	wg        sync.WaitGroup
	stopped   atomic.Bool

	// 与 Master / 其他 ChunkServer 的通信
	masterClient   *rpc.Client
	masterClientMu sync.Mutex
	peers          map[string]*rpc.Client
	peersMu        sync.Mutex
}

// bufferedData 是推送缓冲中的一条数据。
type bufferedData struct {
	data   []byte
	handle types.ChunkHandle
	ts     time.Time
}

// New 创建 ChunkServer,扫描数据目录恢复本地 chunk 清单与版本。
func New(cfg *types.Config, addr, masterAddr, dataDir, rack string) (*ChunkServer, error) {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, err
	}
	cs := &ChunkServer{
		addr:       addr,
		rack:       rack,
		masterAddr: masterAddr,
		dataDir:    dataDir,
		cfg:        cfg,
		chunks:     make(map[types.ChunkHandle]string),
		versions:   make(map[types.ChunkHandle]types.ChunkVersion),
		chunkLocks: make(map[types.ChunkHandle]*sync.Mutex),
		dataBuffer: make(map[uint64]bufferedData),
		stopCh:     make(chan struct{}),
		peers:      make(map[string]*rpc.Client),
	}
	if err := cs.scanDataDir(); err != nil {
		return nil, err
	}
	return cs, nil
}

// Addr 返回实际监听地址。
func (cs *ChunkServer) Addr() string {
	if cs.rpcServer == nil {
		return ""
	}
	return cs.rpcServer.Addr()
}

// Start 启动 RPC 服务、心跳与缓冲清理任务。
func (cs *ChunkServer) Start() error {
	srv, err := rpc.Listen(cs.addr)
	if err != nil {
		return err
	}
	if err := srv.Register(types.ChunkServerService, cs); err != nil {
		srv.Close()
		return err
	}
	// 绑定成功后以真实地址作为本机身份(addr 为 ":0" 时端口随机分配),
	// 心跳与位置信息必须以真实地址上报,Master 才能回连本机。
	cs.addr = srv.Addr()
	cs.rpcServer = srv
	go func() { _ = srv.Serve() }()
	// 启动前先向 Master 报告一次持有的所有 chunk(重建位置信息)
	cs.reportAllChunks()
	cs.wg.Add(2)
	go cs.heartbeatLoop()
	go cs.bufferCleanLoop()
	return nil
}

// Stop 停止后台任务并关闭监听(幂等,可重复调用)。
func (cs *ChunkServer) Stop() {
	if cs.stopped.Swap(true) {
		return
	}
	close(cs.stopCh)
	cs.wg.Wait()
	if cs.rpcServer != nil {
		_ = cs.rpcServer.Close()
	}
	cs.masterClientMu.Lock()
	if cs.masterClient != nil {
		_ = cs.masterClient.Close()
		cs.masterClient = nil
	}
	cs.masterClientMu.Unlock()
	// 关闭与其他 ChunkServer 的连接
	cs.peersMu.Lock()
	for addr, c := range cs.peers {
		_ = c.Close()
		delete(cs.peers, addr)
	}
	cs.peersMu.Unlock()
}

// ---- 磁盘布局 ----

// chunkPath 返回 chunk 数据文件路径。
func (cs *ChunkServer) chunkPath(h types.ChunkHandle) string {
	return filepath.Join(cs.dataDir, fmt.Sprintf("chunk_%d", h))
}

// cksPath 返回 chunk checksum 边车文件路径。
func (cs *ChunkServer) cksPath(h types.ChunkHandle) string {
	return filepath.Join(cs.dataDir, fmt.Sprintf("chunk_%d.cks", h))
}

// scanDataDir 扫描数据目录,加载已有 chunk 及其版本号(从边车文件恢复)。
func (cs *ChunkServer) scanDataDir() error {
	entries, err := os.ReadDir(cs.dataDir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		name := e.Name()
		if !strings.HasPrefix(name, "chunk_") || strings.HasSuffix(name, ".cks") {
			continue
		}
		handleStr := strings.TrimPrefix(name, "chunk_")
		h, err := strconv.ParseUint(handleStr, 10, 64)
		if err != nil {
			continue
		}
		handle := types.ChunkHandle(h)
		cs.chunks[handle] = cs.chunkPath(handle)
		cs.versions[handle] = cs.loadVersion(handle)
	}
	return nil
}

// ---- 通信辅助 ----

func (cs *ChunkServer) getMasterClient() (*rpc.Client, error) {
	cs.masterClientMu.Lock()
	defer cs.masterClientMu.Unlock()
	if cs.masterClient != nil {
		return cs.masterClient, nil
	}
	c, err := rpc.Dial(cs.masterAddr)
	if err != nil {
		return nil, err
	}
	cs.masterClient = c
	return c, nil
}

// peerClient 获取到其他 ChunkServer 的连接(带缓存)。
func (cs *ChunkServer) peerClient(addr string) (*rpc.Client, error) {
	cs.peersMu.Lock()
	defer cs.peersMu.Unlock()
	if c := cs.peers[addr]; c != nil {
		return c, nil
	}
	c, err := rpc.Dial(addr)
	if err != nil {
		return nil, err
	}
	cs.peers[addr] = c
	return c, nil
}

// peerCall 调用其他 ChunkServer;失败时关闭缓存连接。
func (cs *ChunkServer) peerCall(addr, method string, args, reply any) error {
	c, err := cs.peerClient(addr)
	if err != nil {
		return err
	}
	err = c.Call(method, args, reply)
	if err != nil {
		cs.peersMu.Lock()
		_ = c.Close()
		delete(cs.peers, addr)
		cs.peersMu.Unlock()
	}
	return err
}

// ---- 心跳 ----

// heartbeatLoop 每 HeartbeatInterval 向 Master 发送心跳,
// 报告自身地址、机架、磁盘使用量与全部 chunk 及版本。
func (cs *ChunkServer) heartbeatLoop() {
	defer cs.wg.Done()
	ticker := time.NewTicker(cs.cfg.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-cs.stopCh:
			return
		case <-ticker.C:
			cs.heartbeatOnce()
		}
	}
}

func (cs *ChunkServer) heartbeatOnce() {
	args := types.HBArgs{
		Addr:      cs.addr,
		Rack:      cs.rack,
		DiskUsage: cs.diskUsage(),
	}
	cs.mu.Lock()
	handles := make([]types.ChunkHandle, 0, len(cs.chunks))
	for h := range cs.chunks {
		handles = append(handles, h)
	}
	sort.Slice(handles, func(i, j int) bool { return handles[i] < handles[j] })
	for _, h := range handles {
		args.Chunks = append(args.Chunks, types.ChunkReport{Handle: h, Version: cs.versions[h]})
	}
	cs.mu.Unlock()

	var reply types.HBReply
	c, err := cs.getMasterClient()
	if err != nil {
		log.Printf("chunkserver %s: heartbeat dial master: %v", cs.addr, err)
		return // Master 未就绪,下轮重试
	}
	if err := c.Call(rpc.ServiceMethod(types.MasterService, "Heartbeat"), &args, &reply); err != nil {
		// 连接失效:清理连接,下轮自动重连
		log.Printf("chunkserver %s: heartbeat call: %v", cs.addr, err)
		cs.masterClientMu.Lock()
		_ = c.Close()
		cs.masterClient = nil
		cs.masterClientMu.Unlock()
		return
	}
	// 处理租约失效通知:丢弃相关 chunk 的推送缓冲
	if len(reply.ExpiredLeases) > 0 {
		cs.dropBuffersForLeases(reply.ExpiredLeases)
	}
}

// reportAllChunks 启动时向 Master 上报全部 chunk(单次,位置重建用)。
func (cs *ChunkServer) reportAllChunks() {
	cs.mu.Lock()
	handles := make([]types.ChunkHandle, 0, len(cs.chunks))
	for h := range cs.chunks {
		handles = append(handles, h)
	}
	cs.mu.Unlock()
	sort.Slice(handles, func(i, j int) bool { return handles[i] < handles[j] })
	c, err := cs.getMasterClient()
	if err != nil {
		return
	}
	for _, h := range handles {
		cs.mu.Lock()
		v := cs.versions[h]
		cs.mu.Unlock()
		var reply types.RCReply
		_ = c.Call(rpc.ServiceMethod(types.MasterService, "ReportChunk"),
			&types.RCArgs{Addr: cs.addr, Handle: h, Version: v}, &reply)
	}
}

// diskUsage 统计数据目录中 chunk 文件的总体积。
func (cs *ChunkServer) diskUsage() int64 {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	var total int64
	for _, p := range cs.chunks {
		if st, err := os.Stat(p); err == nil {
			total += st.Size()
		}
	}
	return total
}

// bufferCleanLoop 周期清理过期的推送缓冲数据。
func (cs *ChunkServer) bufferCleanLoop() {
	defer cs.wg.Done()
	ticker := time.NewTicker(cs.cfg.MaxBufferAge / 2)
	defer ticker.Stop()
	for {
		select {
		case <-cs.stopCh:
			return
		case <-ticker.C:
			cs.mu.Lock()
			cutoff := time.Now().Add(-cs.cfg.MaxBufferAge)
			for id, b := range cs.dataBuffer {
				if b.ts.Before(cutoff) {
					delete(cs.dataBuffer, id)
				}
			}
			cs.mu.Unlock()
		}
	}
}

// dropBuffersForLeases 丢弃已失效 chunk 的推送缓冲(失去主副本身份后,缓冲不再需要)。
func (cs *ChunkServer) dropBuffersForLeases(leases []types.ExpiredLease) {
	dead := make(map[types.ChunkHandle]bool)
	for _, l := range leases {
		dead[l.Handle] = true
	}
	cs.mu.Lock()
	defer cs.mu.Unlock()
	for id, b := range cs.dataBuffer {
		if dead[b.handle] {
			delete(cs.dataBuffer, id)
		}
	}
}

// chunkLockFor 返回指定 chunk 的互斥锁(串行化写入/追加/读取)。
func (cs *ChunkServer) chunkLockFor(h types.ChunkHandle) *sync.Mutex {
	cs.chunkLocksMu.Lock()
	defer cs.chunkLocksMu.Unlock()
	lk := cs.chunkLocks[h]
	if lk == nil {
		lk = &sync.Mutex{}
		cs.chunkLocks[h] = lk
	}
	return lk
}
