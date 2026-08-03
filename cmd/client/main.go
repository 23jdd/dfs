// Client 示例程序:向 GFS 写入一个指定大小的文件,读回并校验 MD5。
//
// 用法示例:
//
//	go run ./cmd/client -master :8888 -path /demo.bin -size-mb 16
package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"gfs/internal/client"
	"gfs/internal/types"
	"gfs/internal/utils"
)

// pattern 用确定性伪随机(LCG)生成数据,便于 MD5 校验。
type pattern struct {
	seed uint32
}

func (p *pattern) fill(buf []byte) {
	state := p.seed
	for i := range buf {
		state = state*1664525 + 1013904223
		buf[i] = byte(state >> 24)
	}
	p.seed = state
}

func main() {
	masterAddr := flag.String("master", ":8888", "Master 地址")
	path := flag.String("path", "/demo.bin", "文件路径")
	sizeMB := flag.Int("size-mb", 16, "写入大小(MB)")
	flag.Parse()

	cfg := types.DefaultConfig()
	c := client.New(*masterAddr, cfg)
	defer c.Close()

	size := int64(*sizeMB) * 1024 * 1024
	buf := make([]byte, size)
	gen := &pattern{seed: 42}
	gen.fill(buf)
	expectedMD5 := utils.MD5Hex(buf)

	start := time.Now()
	if err := c.WriteFile(*path, buf); err != nil {
		log.Fatalf("write: %v", err)
	}
	writeDur := time.Since(start)

	start = time.Now()
	got, err := c.ReadFile(*path)
	if err != nil {
		log.Fatalf("read: %v", err)
	}
	readDur := time.Since(start)

	gotMD5 := utils.MD5Hex(got)
	fmt.Printf("path       : %s\n", *path)
	fmt.Printf("size       : %d bytes (%d MB)\n", size, *sizeMB)
	fmt.Printf("write      : %v\n", writeDur)
	fmt.Printf("read       : %v\n", readDur)
	if gotMD5 != expectedMD5 {
		log.Fatalf("MD5 mismatch: expected %s, got %s", expectedMD5, gotMD5)
	}
	fmt.Printf("md5        : %s\n", gotMD5)
	fmt.Println("verify     : OK")
}
