// Package client 是 GFS 的客户端库,提供类似 POSIX 的文件操作 API:
//
//	Create / Delete / Rename / Snapshot / Open / Stat
//	File: ReadAt / WriteAt / Append / Size / Close
//
// 设计要点:
//   - 缓存 chunk 位置(带 TTL),读失败/写失败时失效并重新查询 Master;
//   - 写采用两阶段协议:先推送数据到主副本(流水线到从副本),再向主副本发控制请求;
//   - 原子追加(Record Append)由主副本决定偏移,保证记录不重叠不撕裂。
package client

import (
	"math/rand"
	"sync"
	"time"

	"gfs/internal/rpc"
	"gfs/internal/types"
)

// GFSClient 是 GFS 客户端。
type GFSClient struct {
	masterAddr string
	cfg        *types.Config
	clientID   uint32 // 客户端实例 ID,与自增序号拼成全局唯一 DataID

	master   *rpc.Client
	masterMu sync.Mutex

	// 服务器连接池:addr -> 连接;同一 addr 的调用由 serverLocks[addr] 串行化
	servers     map[string]*rpc.Client
	serverLocks map[string]*sync.Mutex
	serversMu   sync.Mutex

	// chunk 位置缓存:handle -> 位置信息(带 TTL)
	locationCache map[types.ChunkHandle]*CachedLocation
	cacheMu       sync.RWMutex

	dataID   uint64
	dataIDMu sync.Mutex
}

// CachedLocation 是客户端缓存的 chunk 位置信息。
type CachedLocation struct {
	// Handle 是实际生效的 chunk handle:写时复制(COW)后可能与查询时的 handle 不同。
	Handle      types.ChunkHandle
	Primary     string
	Secondaries []string
	Version     types.ChunkVersion
	CachedAt    time.Time
}

// New 创建客户端。
func New(masterAddr string, cfg *types.Config) *GFSClient {
	if cfg == nil {
		cfg = types.DefaultConfig()
	}
	return &GFSClient{
		masterAddr:    masterAddr,
		cfg:           cfg,
		clientID:      rand.Uint32(),
		servers:       make(map[string]*rpc.Client),
		serverLocks:   make(map[string]*sync.Mutex),
		locationCache: make(map[types.ChunkHandle]*CachedLocation),
	}
}

// Close 释放客户端持有的所有连接。
func (c *GFSClient) Close() {
	c.masterMu.Lock()
	if c.master != nil {
		_ = c.master.Close()
		c.master = nil
	}
	c.masterMu.Unlock()

	c.serversMu.Lock()
	for addr, cl := range c.servers {
		_ = cl.Close()
		delete(c.servers, addr)
	}
	c.serversMu.Unlock()
}

// ---- 通信辅助 ----

// callMaster 调用 Master 服务。
// 整个调用在 masterMu 临界区内执行:mrpc 客户端单连接非并发安全,
// 且服务端在错误响应后会关闭连接,持有锁可避免并发调用互相踩踏连接。
// 业务错误(哨兵)丢弃连接后直接返回;连接类错误重试一次。
func (c *GFSClient) callMaster(method string, args, reply any) error {
	c.masterMu.Lock()
	defer c.masterMu.Unlock()
	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		if c.master == nil {
			cl, err := rpc.Dial(c.masterAddr)
			if err != nil {
				return err
			}
			c.master = cl
		}
		cl := c.master
		err := cl.Call(rpc.ServiceMethod(types.MasterService, method), args, reply)
		if err == nil {
			return nil
		}
		matched := types.MatchError(err)
		// 错误响应后服务端已关闭连接,丢弃池中连接
		_ = cl.Close()
		c.master = nil
		if matched != err {
			return matched // 业务错误:错误信息有效,直接返回
		}
		lastErr = err
	}
	return types.MatchError(lastErr)
}

// callServer 调用 ChunkServer 服务。
// 对同一服务器的调用在 per-addr 锁内串行执行:mrpc 客户端单连接非并发安全,
// 且服务端在错误响应后会关闭连接,持有锁可避免并发调用互相踩踏连接。
// 业务错误(哨兵)丢弃连接后直接返回;连接类错误重试一次。
func (c *GFSClient) callServer(addr, method string, args, reply any) error {
	c.serversMu.Lock()
	lk := c.serverLocks[addr]
	if lk == nil {
		lk = &sync.Mutex{}
		c.serverLocks[addr] = lk
	}
	c.serversMu.Unlock()

	lk.Lock()
	defer lk.Unlock()
	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		cl := c.servers[addr]
		if cl == nil {
			d, err := rpc.Dial(addr)
			if err != nil {
				return err
			}
			cl = d
			c.servers[addr] = cl
		}
		err := cl.Call(rpc.ServiceMethod(types.ChunkServerService, method), args, reply)
		if err == nil {
			return nil
		}
		matched := types.MatchError(err)
		// 错误响应后服务端已关闭连接,丢弃池中连接
		_ = cl.Close()
		delete(c.servers, addr)
		if matched != err {
			return matched // 业务错误:错误信息有效,直接返回
		}
		lastErr = err
	}
	return types.MatchError(lastErr)
}

// nextDataID 生成全局唯一的数据推送 ID(客户端实例 ID + 自增序号)。
func (c *GFSClient) nextDataID() uint64 {
	c.dataIDMu.Lock()
	defer c.dataIDMu.Unlock()
	c.dataID++
	return (uint64(c.clientID) << 32) | c.dataID
}

// ---- 文件系统操作 ----

// Create 创建文件并分配第一个 chunk。
func (c *GFSClient) Create(path string) error {
	var reply types.CreateReply
	return c.callMaster("Create", &types.CreateArgs{Path: path}, &reply)
}

// Delete 惰性删除文件(移入隐藏目录,延迟 GC)。
func (c *GFSClient) Delete(path string) error {
	var reply types.DeleteReply
	return c.callMaster("Delete", &types.DeleteArgs{Path: path}, &reply)
}

// Rename 重命名文件。
func (c *GFSClient) Rename(src, dst string) error {
	var reply types.RenameReply
	return c.callMaster("Rename", &types.RenameArgs{Src: src, Dst: dst}, &reply)
}

// Snapshot 对文件创建快照(写时复制)。
func (c *GFSClient) Snapshot(src, dst string) error {
	var reply types.SnapshotReply
	return c.callMaster("Snapshot", &types.SnapshotArgs{Path: src, SnapPath: dst}, &reply)
}

// Stat 返回文件大小与 chunk 数量。
func (c *GFSClient) Stat(path string) (size int64, chunks int, err error) {
	var reply types.StatReply
	if err = c.callMaster("Stat", &types.StatArgs{Path: path}, &reply); err != nil {
		return 0, 0, err
	}
	return reply.Size, reply.ChunkCount, nil
}

// ChunkHandle 返回文件指定 chunk 索引的 handle(诊断/测试辅助)。
func (c *GFSClient) ChunkHandle(path string, index int) (types.ChunkHandle, error) {
	return c.getChunkHandle(path, index)
}

// Open 打开文件(校验存在性),返回文件句柄。
func (c *GFSClient) Open(path string) (*File, error) {
	if _, _, err := c.Stat(path); err != nil {
		return nil, err
	}
	return &File{c: c, path: path}, nil
}

// WriteFile 便捷函数:创建(若不存在)并写入完整文件内容。
func (c *GFSClient) WriteFile(path string, data []byte) error {
	if _, _, err := c.Stat(path); err != nil {
		if err := c.Create(path); err != nil {
			return err
		}
	}
	f, err := c.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.WriteAt(data, 0)
	return err
}

// ReadFile 便捷函数:读取完整文件内容。
func (c *GFSClient) ReadFile(path string) ([]byte, error) {
	f, err := c.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	size, err := f.Size()
	if err != nil {
		return nil, err
	}
	data := make([]byte, size)
	if _, err := f.ReadAt(data, 0); err != nil {
		return nil, err
	}
	return data, nil
}

// ---- chunk 位置缓存 ----

// getCachedLocation 读取位置缓存;过期返回 nil。
func (c *GFSClient) getCachedLocation(h types.ChunkHandle) *CachedLocation {
	c.cacheMu.RLock()
	defer c.cacheMu.RUnlock()
	loc := c.locationCache[h]
	if loc == nil {
		return nil
	}
	if time.Since(loc.CachedAt) > c.cfg.CacheTimeout {
		return nil
	}
	return loc
}

// putCachedLocation 写入位置缓存。
func (c *GFSClient) putCachedLocation(h types.ChunkHandle, loc *CachedLocation) {
	c.cacheMu.Lock()
	defer c.cacheMu.Unlock()
	c.locationCache[h] = loc
}

// invalidateLocation 使位置缓存失效(读/写失败后调用)。
func (c *GFSClient) invalidateLocation(h types.ChunkHandle) {
	c.cacheMu.Lock()
	defer c.cacheMu.Unlock()
	delete(c.locationCache, h)
}

// queryLocations 向 Master 查询 chunk 位置(写模式永远走 Master,不做缓存,
// 因为快照 COW 可能随时产生新 handle)。
func (c *GFSClient) queryLocations(path string, handle types.ChunkHandle, index int, forWrite bool) (*CachedLocation, error) {
	if !forWrite {
		if loc := c.getCachedLocation(handle); loc != nil {
			return loc, nil
		}
	}
	var reply types.GLReply
	err := c.callMaster("GetLocations", &types.GLArgs{
		Handle: handle, ForWrite: forWrite, Path: path, ChunkIndex: index,
	}, &reply)
	if err != nil {
		return nil, err
	}
	loc := &CachedLocation{
		Handle:      reply.Handle,
		Primary:     reply.Primary,
		Secondaries: reply.Secondaries,
		Version:     reply.Version,
		CachedAt:    time.Now(),
	}
	if reply.Handle != handle {
		// COW 后 handle 变化:新 handle 的旧缓存一律失效
		c.invalidateLocation(handle)
	}
	c.putCachedLocation(reply.Handle, loc)
	return loc, nil
}

// getChunkHandle 获取文件指定 chunk 的 handle。
func (c *GFSClient) getChunkHandle(path string, index int) (types.ChunkHandle, error) {
	var reply types.GCHReply
	if err := c.callMaster("GetChunkHandle", &types.GCHArgs{Path: path, ChunkIndex: index}, &reply); err != nil {
		return 0, err
	}
	return reply.Handle, nil
}
