package dfs

import (
	"errors"
	"io"
	"net"
	"os"
	"sync"

	"github.com/23jdd/mrpc"
)

var (
	ErrMissChunk  = errors.New("this chunk is Missing")
	ErrVersionMod = errors.New("Chunk Version Is Error")
)

type LocalChunk struct {
	Handle   ChunkHandle
	Version  uint32
	FilePath string // /data/chunks/chunk_12345
	Size     int64
	Checksum uint32 // CRC32
	mu       sync.RWMutex
}
type ChunkServer struct {
	addr       string
	dataDir    string
	chunks     map[ChunkHandle]*LocalChunk // 本地 Chunk 缓存
	masterAddr string
}

func NewChunkServer(dir string) *ChunkServer {
	return &ChunkServer{dataDir: dir}
}

func (cs *ChunkServer) Run(host string) {
	lis, err := net.Listen("tcp", host)
	if err != nil {
		panic(err)
	}
	sv := mrpc.NewServer(lis)
	err = sv.Register(ChunkServiceName, cs)
	if err != nil {
		panic(err)
	}
	sv.Run()
}
func (cs *ChunkServer) ReadChunk(req ReadChunkRequest, rep *ReadChunkReply) error {
	chunk, ok := cs.chunks[req.Handle]
	if !ok {
		return ErrMissChunk
	}
	if chunk.Version != req.Version {
		return ErrVersionMod
	}
	reader, err := os.Open(chunk.FilePath)
	if err != nil {
		return err
	}
	defer reader.Close()
	_, err = reader.Seek(req.Offset, io.SeekStart)
	if err != nil {
		return err
	}
	buf := make([]byte, req.Length)
	n, err := io.ReadFull(reader, buf)
	rep.Data = buf[:n]
	return err
}
