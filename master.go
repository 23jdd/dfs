package dfs

import (
	"time"

	"github.com/23jdd/SamKv/pkg/store"
	"github.com/23jdd/mrpc"
)

type Master struct {
	namespace      map[string]*FileMeta       // 文件路径 -> 元数据
	chunkIndex     map[ChunkHandle]*ChunkMeta // Chunk ID -> 元数据
	chunkLocations map[ChunkHandle][]string   // Chunk ID -> ChunkServer 地址（内存重建）
	leases         map[ChunkHandle]*Lease     // 租约信息
	kvStore        *store.StoreManager        // 复用 SamKv 持久化
	rpcServer      *mrpc.Server               // 复用 mrpc
}
type FileMeta struct {
	Path      string
	Chunks    []ChunkHandle
	CreatedAt time.Time
	Size      int64
}
type ChunkMeta struct {
	Handle      ChunkHandle
	Version     uint32    // 版本号，用于过期副本检测
	Primary     string    // 当前 Primary ChunkServer 地址
	LeaseExpire time.Time // 租约过期时间
}
type Lease struct {
	Primary     string
	ExpireAt    time.Time
	Secondaries []string
}
