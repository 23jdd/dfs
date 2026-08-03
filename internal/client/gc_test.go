package client_test

import (
	"os"
	"strings"
	"testing"
	"time"
)

// countChunkFiles 统计所有 ChunkServer 数据目录中的 chunk 数据文件数量。
func (tc *testCluster) countChunkFiles() int {
	tc.t.Helper()
	total := 0
	for _, dir := range tc.serverDirs {
		entries, err := os.ReadDir(dir)
		if err != nil {
			continue
		}
		for _, e := range entries {
			name := e.Name()
			if strings.HasPrefix(name, "chunk_") && !strings.HasSuffix(name, ".cks") {
				total++
			}
		}
	}
	return total
}

// TestGarbageCollection 验证垃圾回收全链路:
// 删除文件 -> 移入隐藏目录 -> GC 清理元数据 -> 各 ChunkServer 上的 chunk 文件被删除。
func TestGarbageCollection(t *testing.T) {
	tc := startCluster(t, 3, 3, 1<<20)
	defer tc.stop()
	c := tc.client()

	// 2 个 chunk x 3 副本 = 6 个数据文件
	if err := c.WriteFile("/gc_disk.bin", make([]byte, 2<<20)); err != nil {
		t.Fatalf("write: %v", err)
	}
	if n := tc.countChunkFiles(); n != 6 {
		t.Fatalf("chunk files on disk = %d, want 6", n)
	}

	if err := c.Delete("/gc_disk.bin"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	// 阶段一:GC 清除隐藏文件元数据(GCInterval 1s + GCDelay 5s)
	waitFor(t, 20*time.Second, func() bool { return len(tc.master.ChunkReplicaStats()) == 0 })

	// 阶段二:DeleteChunk RPC 生效,磁盘上的 chunk 文件被清除
	waitFor(t, 20*time.Second, func() bool { return tc.countChunkFiles() == 0 })

	// 同名文件可重新创建并正常读写
	if err := c.WriteFile("/gc_disk.bin", []byte("reborn")); err != nil {
		t.Fatalf("recreate: %v", err)
	}
	got, err := c.ReadFile("/gc_disk.bin")
	if err != nil || string(got) != "reborn" {
		t.Fatalf("read reborn: %q, %v", got, err)
	}
}

// TestGCOrphanChunks 验证快照写时复制产生的孤儿 chunk 也会被 GC 回收。
func TestGCOrphanChunks(t *testing.T) {
	tc := startCluster(t, 3, 3, 1<<20)
	defer tc.stop()
	c := tc.client()

	orig := make([]byte, 1<<20)
	gen := &lcgPattern{seed: 61}
	gen.fill(orig)
	if err := c.WriteFile("/orphan.bin", orig); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := c.Snapshot("/orphan.bin", "/orphan.snap"); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	// 写时复制:原文件 chunk 被复制成新 chunk(旧 chunk 仍被快照引用);
	// 追加放不下又分配一个新 chunk。磁盘文件 = 旧(3) + COW 副本(3) + 追加 chunk(3) = 9
	f, _ := c.Open("/orphan.bin")
	if _, err := f.Append([]byte("more")); err != nil {
		t.Fatalf("append: %v", err)
	}
	if n := tc.countChunkFiles(); n != 9 {
		t.Fatalf("chunk files after COW+append = %d, want 9", n)
	}
	// 删除快照 -> 旧 chunk 无引用 -> GC 回收其数据文件(9 -> 6)
	if err := c.Delete("/orphan.snap"); err != nil {
		t.Fatalf("delete snapshot: %v", err)
	}
	waitFor(t, 20*time.Second, func() bool { return tc.countChunkFiles() == 6 })
	// 原文件数据仍然完整:COW 副本保持原内容,追加记录在文件末尾(1MB 处)
	got, err := c.ReadFile("/orphan.bin")
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(got) != 1<<20+4 {
		t.Fatalf("file size wrong after orphan GC: len=%d", len(got))
	}
	if string(got[1<<20:]) != "more" {
		t.Fatal("appended record missing after orphan GC")
	}
}
