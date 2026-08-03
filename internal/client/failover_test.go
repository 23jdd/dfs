package client_test

import (
	"testing"
	"time"

	"gfs/internal/utils"
)

// TestPrimaryFailover 验证主副本故障后的租约失效与自动切换:
// 杀死 chunk 的主副本后,客户端写入会经历一段失败重试,待租约过期后
// Master 选出新主副本并提升版本号,写入恢复且数据一致。
func TestPrimaryFailover(t *testing.T) {
	tc := startCluster(t, 3, 3, 1<<20)
	defer tc.stop()
	c := tc.client()

	const size = 3 << 20
	data := make([]byte, size)
	gen := &lcgPattern{seed: 31}
	gen.fill(data)
	if err := c.WriteFile("/failover.bin", data); err != nil {
		t.Fatalf("write: %v", err)
	}

	handle, err := c.ChunkHandle("/failover.bin", 0)
	if err != nil {
		t.Fatalf("chunk handle: %v", err)
	}
	primary := tc.master.PrimaryOf(handle)
	if primary == "" {
		t.Fatal("no primary for chunk")
	}
	versionBefore := tc.master.ChunkVersion(handle)
	if versionBefore == 0 {
		t.Fatal("chunk version should be >= 1")
	}

	// 杀死主副本所在服务器
	tc.killServerByAddr(primary)
	waitFor(t, 10*time.Second, func() bool { return tc.master.LiveServerCount() == 2 })

	// 覆盖写 chunk0:客户端带退避重试,应能熬过租约过期并切换到新主
	over := make([]byte, 1<<20)
	gen2 := &lcgPattern{seed: 32}
	gen2.fill(over)
	f, err := c.Open("/failover.bin")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	start := time.Now()
	if _, err := f.WriteAt(over, 0); err != nil {
		t.Fatalf("write after primary failover: %v", err)
	}
	elapsed := time.Since(start)
	if elapsed > 6*time.Second {
		t.Fatalf("failover took too long: %v", elapsed)
	}
	t.Logf("failover write recovered after %v", elapsed)

	// 新主副本已选出且与旧主不同,版本号已提升
	newPrimary := tc.master.PrimaryOf(handle)
	if newPrimary == "" || newPrimary == primary {
		t.Fatalf("primary not switched: old=%s new=%s", primary, newPrimary)
	}
	if v := tc.master.ChunkVersion(handle); v <= versionBefore {
		t.Fatalf("version not bumped: %d -> %d", versionBefore, v)
	}

	// 全量数据校验:chunk0 被覆盖,其余保持原样
	want := make([]byte, size)
	copy(want, over)
	copy(want[1<<20:], data[1<<20:])
	got, err := c.ReadFile("/failover.bin")
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if utils.MD5Hex(got) != utils.MD5Hex(want) {
		t.Fatal("data mismatch after primary failover")
	}
}

// TestAppendFailover 验证主副本故障后原子追加同样能恢复。
func TestAppendFailover(t *testing.T) {
	tc := startCluster(t, 3, 3, 1<<20)
	defer tc.stop()
	c := tc.client()

	if err := c.Create("/af.log"); err != nil {
		t.Fatalf("create: %v", err)
	}
	f, err := c.Open("/af.log")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	rec := make([]byte, 128<<10)
	gen := &lcgPattern{seed: 41}
	gen.fill(rec)
	if _, err := f.Append(rec); err != nil {
		t.Fatalf("first append: %v", err)
	}

	handle, _ := c.ChunkHandle("/af.log", 0)
	primary := tc.master.PrimaryOf(handle)
	tc.killServerByAddr(primary)
	waitFor(t, 10*time.Second, func() bool { return tc.master.LiveServerCount() == 2 })

	// 追加应经过租约切换后成功
	start := time.Now()
	if _, err := f.Append(rec); err != nil {
		t.Fatalf("append after failover: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 6*time.Second {
		t.Fatalf("append failover took too long: %v", elapsed)
	}

	got, err := c.ReadFile("/af.log")
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(got) != 2*len(rec) {
		t.Fatalf("file size = %d, want %d", len(got), 2*len(rec))
	}
	for i := 0; i < 2; i++ {
		if string(got[i*len(rec):(i+1)*len(rec)]) != string(rec) {
			t.Fatalf("record %d mismatch after failover", i)
		}
	}
}

// TestNoServerError 验证所有副本死亡时写入返回明确错误(ErrNoServer)。
func TestNoServerError(t *testing.T) {
	tc := startCluster(t, 2, 2, 1<<20)
	defer tc.stop()
	c := tc.client()

	if err := c.WriteFile("/nosrv.bin", []byte("x")); err != nil {
		t.Fatalf("write: %v", err)
	}
	// 停掉全部 ChunkServer
	for i := range tc.servers {
		tc.killServer(i)
	}
	waitFor(t, 10*time.Second, func() bool { return tc.master.LiveServerCount() == 0 })

	f, err := c.Open("/nosrv.bin")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := f.WriteAt([]byte("y"), 0); err == nil {
		t.Fatal("write with no live servers should fail")
	}
}
