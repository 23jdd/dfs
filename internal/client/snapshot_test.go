package client_test

import (
	"bytes"
	"testing"
)

// TestMultiSnapshot 验证同一文件创建多个快照后,各快照互不影响:
// 修改原文件只触发写时复制,所有快照保持快照时刻的内容。
func TestMultiSnapshot(t *testing.T) {
	tc := startCluster(t, 3, 3, 1<<20)
	defer tc.stop()
	c := tc.client()

	const size = 2 << 20
	orig := make([]byte, size)
	gen := &lcgPattern{seed: 81}
	gen.fill(orig)
	if err := c.WriteFile("/ms/src.bin", orig); err != nil {
		t.Fatalf("write: %v", err)
	}
	// 连续创建两个快照
	for _, p := range []string{"/ms/snap1.bin", "/ms/snap2.bin"} {
		if err := c.Snapshot("/ms/src.bin", p); err != nil {
			t.Fatalf("snapshot %s: %v", p, err)
		}
	}
	// 修改原文件:覆盖第一块 + 追加一块(两次写时复制)
	f, err := c.Open("/ms/src.bin")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	over := make([]byte, 1<<20)
	gen2 := &lcgPattern{seed: 82}
	gen2.fill(over)
	if _, err := f.WriteAt(over, 0); err != nil {
		t.Fatalf("overwrite: %v", err)
	}
	extra := make([]byte, 256<<10)
	gen3 := &lcgPattern{seed: 83}
	gen3.fill(extra)
	if _, err := f.Append(extra); err != nil {
		t.Fatalf("append: %v", err)
	}

	// 原文件 = 覆盖数据 + 原数据后半 + 追加
	want := make([]byte, 0, size+len(extra))
	want = append(want, over...)
	want = append(want, orig[size-len(over):]...)
	want = append(want, extra...)
	got, err := c.ReadFile("/ms/src.bin")
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("src content mismatch (err=%v)", err)
	}

	// 两个快照都必须保持原内容
	for _, p := range []string{"/ms/snap1.bin", "/ms/snap2.bin"} {
		got, err := c.ReadFile(p)
		if err != nil {
			t.Fatalf("read %s: %v", p, err)
		}
		if !bytes.Equal(got, orig) {
			t.Fatalf("snapshot %s changed (copy-on-write broken)", p)
		}
		sz, _, err := c.Stat(p)
		if err != nil || sz != size {
			t.Fatalf("snapshot %s size = %d, want %d", p, sz, size)
		}
	}
}

// TestSnapshotChain 验证对快照再拍快照(快照链)也能正确工作。
func TestSnapshotChain(t *testing.T) {
	tc := startCluster(t, 3, 3, 1<<20)
	defer tc.stop()
	c := tc.client()

	v1 := make([]byte, 1<<20)
	gen := &lcgPattern{seed: 91}
	gen.fill(v1)
	if err := c.WriteFile("/chain/a.bin", v1); err != nil {
		t.Fatalf("write: %v", err)
	}
	// a -> b -> c 快照链
	for _, p := range []string{"/chain/b.bin", "/chain/c.bin"} {
		if err := c.Snapshot("/chain/a.bin", p); err != nil {
			t.Fatalf("snapshot %s: %v", p, err)
		}
	}
	// 追加修改 a:三份都仍是 v1 的内容?不 —— a 变 v2,b/c 保持 v1
	v2 := make([]byte, 512<<10)
	gen2 := &lcgPattern{seed: 92}
	gen2.fill(v2)
	f, _ := c.Open("/chain/a.bin")
	if _, err := f.Append(v2); err != nil {
		t.Fatalf("append: %v", err)
	}
	gotA, err := c.ReadFile("/chain/a.bin")
	if err != nil {
		t.Fatalf("read a: %v", err)
	}
	wantA := append(append([]byte(nil), v1...), v2...)
	if !bytes.Equal(gotA, wantA) {
		t.Fatal("chain head mismatch")
	}
	for _, p := range []string{"/chain/b.bin", "/chain/c.bin"} {
		got, err := c.ReadFile(p)
		if err != nil || !bytes.Equal(got, v1) {
			t.Fatalf("snapshot %s changed (err=%v)", p, err)
		}
	}
}
