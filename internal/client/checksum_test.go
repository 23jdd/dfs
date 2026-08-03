package client_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"gfs/internal/utils"
)

// TestChecksumCorruption 验证数据校验:破坏主副本的 chunk 文件后,
// 读取该 chunk 时主副本校验失败,客户端自动回退到从副本,数据仍完整正确。
func TestChecksumCorruption(t *testing.T) {
	tc := startCluster(t, 3, 3, 1<<20)
	defer tc.stop()
	c := tc.client()

	const size = 2 << 20
	data := make([]byte, size)
	gen := &lcgPattern{seed: 51}
	gen.fill(data)
	if err := c.WriteFile("/corrupt.bin", data); err != nil {
		t.Fatalf("write: %v", err)
	}

	handle, err := c.ChunkHandle("/corrupt.bin", 0)
	if err != nil {
		t.Fatalf("chunk handle: %v", err)
	}
	primary := tc.master.PrimaryOf(handle)
	if primary == "" {
		t.Fatal("no primary")
	}
	dir := tc.serverDirOf(primary)

	// 破坏主副本上 chunk0 文件中间一个字节(位于某个 64KB 校验块内)
	path := filepath.Join(dir, fmt.Sprintf("chunk_%d", handle))
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("open chunk file: %v", err)
	}
	if _, err := f.WriteAt([]byte{0xAA}, size/2); err != nil {
		f.Close()
		t.Fatalf("corrupt chunk: %v", err)
	}
	f.Close()

	// 读取整个文件:chunk0 从主副本读会校验失败,应回退到从副本
	got, err := c.ReadFile("/corrupt.bin")
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if utils.MD5Hex(got) != utils.MD5Hex(data) {
		t.Fatal("data mismatch after checksum corruption")
	}
}

// TestChecksumCorruptionAllReplicas 验证全部副本损坏时读取返回校验错误。
func TestChecksumCorruptionAllReplicas(t *testing.T) {
	tc := startCluster(t, 3, 3, 1<<20)
	defer tc.stop()
	c := tc.client()

	data := make([]byte, 1<<20)
	gen := &lcgPattern{seed: 52}
	gen.fill(data)
	if err := c.WriteFile("/corrupt_all.bin", data); err != nil {
		t.Fatalf("write: %v", err)
	}
	handle, err := c.ChunkHandle("/corrupt_all.bin", 0)
	if err != nil {
		t.Fatalf("chunk handle: %v", err)
	}
	// 破坏所有副本上 chunk0 文件的同一位置
	for _, dir := range tc.serverDirs {
		path := filepath.Join(dir, fmt.Sprintf("chunk_%d", handle))
		f, err := os.OpenFile(path, os.O_RDWR, 0)
		if err != nil {
			t.Fatalf("open chunk file in %s: %v", dir, err)
		}
		if _, err := f.WriteAt([]byte{0xBB}, 512<<10); err != nil {
			f.Close()
			t.Fatalf("corrupt chunk: %v", err)
		}
		f.Close()
	}
	// 读取应失败(所有副本校验都不过)
	if _, err := c.ReadFile("/corrupt_all.bin"); err == nil {
		t.Fatal("read should fail when all replicas are corrupt")
	}
}

// serverDirOf 返回指定 ChunkServer 的数据目录。
func (tc *testCluster) serverDirOf(addr string) string {
	tc.t.Helper()
	for i, cs := range tc.servers {
		if cs.Addr() == addr {
			return tc.serverDirs[i]
		}
	}
	tc.t.Fatalf("chunkserver %s not found", addr)
	return ""
}
