// Master 主节点入口。
//
// 用法示例:
//
//	go run ./cmd/master -addr :8888 -dir ./master_data -replication 3
package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"gfs/internal/master"
	"gfs/internal/types"
)

func main() {
	addr := flag.String("addr", ":8888", "监听地址")
	dir := flag.String("dir", "./master_data", "持久化数据目录(checkpoint + op log)")
	replication := flag.Int("replication", 3, "副本数")
	chunkSizeMB := flag.Int("chunk-size-mb", 64, "chunk 大小(MB)")
	leaseSec := flag.Int("lease-sec", 60, "租约时长(秒)")
	heartbeatMs := flag.Int("heartbeat-ms", 500, "心跳间隔(毫秒)")
	gcDelaySec := flag.Int("gc-delay-sec", 180, "垃圾回收延迟(秒)")
	flag.Parse()

	cfg := types.DefaultConfig()
	cfg.ReplicationFactor = *replication
	cfg.ChunkSize = int64(*chunkSizeMB) * 1024 * 1024
	cfg.LeaseTimeout = time.Duration(*leaseSec) * time.Second
	cfg.HeartbeatInterval = time.Duration(*heartbeatMs) * time.Millisecond
	cfg.HeartbeatTimeout = 6 * cfg.HeartbeatInterval
	cfg.GCDelay = time.Duration(*gcDelaySec) * time.Second

	m, err := master.New(cfg, *dir)
	if err != nil {
		log.Fatalf("master: init: %v", err)
	}
	if err := m.Start(*addr); err != nil {
		log.Fatalf("master: start: %v", err)
	}
	log.Printf("master: listening on %s, data dir %s, replication=%d", m.Addr(), *dir, cfg.ReplicationFactor)

	// 优雅退出:保存 checkpoint
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig
	m.Stop()
	log.Printf("master: stopped")
}
