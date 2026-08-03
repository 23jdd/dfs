package dfs

import (
	"net"
	"time"

	"github.com/23jdd/SamKv/pkg/store"
	"github.com/23jdd/mrpc"
)

type Master struct {
	namespace      map[string]*FileMeta       // 文件路径 -> 元数据
	chunkIndex     map[ChunkHandle]*ChunkMeta // Chunk ID -> 元数据
	chunkLocations map[ChunkHandle][]string   // Chunk ID -> ChunkServer 地址（内存重建）
	leases         map[ChunkHandle]*Lease     // 租约信息
	kvStore        *store.StoreManager        // SamKv 持久化
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

func NewMaster(dir string, opt store.Options) (*Master, error) {
	st, err := store.NewStoreMangerWithOptions(dir, opt)
	if err != nil {
		return nil, err
	}
	ms := &Master{}
	ms.kvStore = st
	ms.leases = make(map[ChunkHandle]*Lease)
	ms.chunkIndex = make(map[ChunkHandle]*ChunkMeta)
	ms.chunkLocations = make(map[ChunkHandle][]string)
	ms.namespace = make(map[string]*FileMeta)
	return ms, nil
}

func (ms *Master) Run(host string) {
	lis, err := net.Listen("tcp", host)
	if err != nil {
		panic(err)
	}
	sv := mrpc.NewServer(lis)
	err = sv.Register(MasterSerivceName, ms)
	if err != nil {
		panic(err)
	}
	sv.Run()
}
