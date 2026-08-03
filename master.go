package dfs

import (
	"errors"
	"net"
	"sync"
	"time"

	"github.com/23jdd/mrpc"
	"github.com/tidwall/buntdb"
)

var (
	ErrFileExist     = errors.New("File Is Exist")
	ErrNotFoundFile  = errors.New("Not Found File")
	ErrIndexOutBound = errors.New("Index Out Bound")
)

type Master struct {
	namespace      map[string]*FileMeta       // 文件路径 -> 元数据
	chunkIndex     map[ChunkHandle]*ChunkMeta // Chunk ID -> 元数据
	chunkLocations map[ChunkHandle][]string   // Chunk ID -> ChunkServer 地址（内存重建）
	leases         map[ChunkHandle]*Lease     // 租约信息
	ChunkServers   []string
	kvStore        *buntdb.DB // SamKv 持久化
	codec          Codec
	mu             sync.RWMutex
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

func NewMaster(path string, chunks ...string) (*Master, error) {
	st, err := buntdb.Open(path)
	if err != nil {
		return nil, err
	}
	ms := &Master{}
	ms.namespace = make(map[string]*FileMeta)
	ms.kvStore = st
	err = ms.ReLoad()
	if err != nil {
		return nil, err
	}
	ms.leases = make(map[ChunkHandle]*Lease)
	ms.chunkIndex = make(map[ChunkHandle]*ChunkMeta)
	ms.chunkLocations = make(map[ChunkHandle][]string)
	ms.codec = NewJsonCodec()
	ms.ChunkServers = chunks
	return ms, nil
}
func (ms *Master) ReLoad() error {
	// get all keys
	return ms.kvStore.View(func(tx *buntdb.Tx) error {
		return tx.AscendKeys("*", func(key, value string) bool {
			fm := &FileMeta{}
			err := ms.codec.Decode(value, fm)
			if err != nil {
				return true
			}
			ms.namespace[key] = fm
			return true
		})
	})

}
func (ms *Master) CreateFile(req CreateFileRequest, rep *CreateFileReply) error {
	fm := &FileMeta{
		Path:      req.Path,
		Size:      0,
		CreatedAt: time.Now().UTC(),
	}
	return ms.kvStore.Update(func(tx *buntdb.Tx) error {
		_, err := tx.Get(req.Path)
		if err == nil {
			return ErrFileExist
		}
		if errors.Is(err, buntdb.ErrNotFound) {
			val, err := ms.codec.Encode(fm)
			if err != nil {
				return err
			}
			_, _, err = tx.Set(req.Path, val, &buntdb.SetOptions{})
			if err != nil {
				return err
			}
			ms.mu.Lock()
			defer ms.mu.Unlock()
			ms.namespace[req.Path] = fm
			return nil
		}
		return err
	})

}

func (ms *Master) DeleteFile(req DeleteFileRequest, rep *DeleteFileReply) error {
	err := ms.kvStore.Update(func(tx *buntdb.Tx) error {
		_, err := tx.Delete(req.Path)
		return err
	})
	if err == nil {
		ms.mu.Lock()
		defer ms.mu.Unlock()
		delete(ms.namespace, req.Path)
	}
	return err
}
func (ms *Master) GetFileInfo(req GetFileInfoRequest, rep *GetFileInfoReply) error {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	val, ok := ms.namespace[req.Path]
	if !ok {
		return ErrNotFoundFile
	}
	rep.Info = *val
	return nil
}
func (ms *Master) ReadLocations(req ReadLocationsRequest, rep *ReadLocationsReply) error {
	meta, ok := ms.namespace[req.FileName]
	if !ok {
		return ErrNotFoundFile
	}
	if len(meta.Chunks) < int(req.Index) {
        return ErrIndexOutBound 
	}
	handler := meta.Chunks[req.Index]
	rep.Handler=handler
	rep.Replicas=ms.chunkLocations[handler]
	cm := ms.chunkIndex[handler]
	rep.Version=cm.Version
	return nil
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

// Wait All ChunkServer report chunks
func (ms *Master) Wait() {

}
