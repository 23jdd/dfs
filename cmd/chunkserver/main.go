// ChunkServer 块服务器入口。
//
// 用法示例:
//
//	go run ./cmd/chunkserver -addr :8901 -master :8888 -dir ./cs_data1 -rack rack0
package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"gfs/internal/chunkserver"
	"gfs/internal/types"
)

func main() {
	addr := flag.String("addr", ":8901", "监听地址")
	masterAddr := flag.String("master", ":8888", "Master 地址")
	dir := flag.String("dir", "./cs_data", "chunk 数据目录")
	rack := flag.String("rack", "rack0", "机架标识(机架感知放置)")
	chunkSizeMB := flag.Int("chunk-size-mb", 64, "chunk 大小(MB),需与 Master 一致")
	heartbeatMs := flag.Int("heartbeat-ms", 500, "心跳间隔(毫秒)")
	flag.Parse()

	cfg := types.DefaultConfig()
	cfg.ChunkSize = int64(*chunkSizeMB) * 1024 * 1024
	cfg.HeartbeatInterval = time.Duration(*heartbeatMs) * time.Millisecond

	cs, err := chunkserver.New(cfg, *addr, *masterAddr, *dir, *rack)
	if err != nil {
		log.Fatalf("chunkserver: init: %v", err)
	}
	if err := cs.Start(); err != nil {
		log.Fatalf("chunkserver: start: %v", err)
	}
	log.Printf("chunkserver: listening on %s, master %s, rack %s, data dir %s",
		cs.Addr(), *masterAddr, *rack, *dir)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig
	cs.Stop()
	log.Printf("chunkserver: stopped")
}
