package dfs

import (
	"net"

	"github.com/23jdd/mrpc"
)

type ChunkServer struct {
}

func NewChunkServer() *ChunkServer {
	return &ChunkServer{}
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
