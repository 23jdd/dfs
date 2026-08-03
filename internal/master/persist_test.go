package master

import (
	"path/filepath"
	"testing"
	"time"

	"gfs/internal/types"
)

// newTestMaster 创建 Master 并注入一个"存活"服务器,使 chunk 分配可以成功。
func newTestMaster(t *testing.T, cfg *types.Config, dir string) *Master {
	t.Helper()
	m, err := New(cfg, dir)
	if err != nil {
		t.Fatalf("new master: %v", err)
	}
	m.mu.Lock()
	m.chunkServers["127.0.0.1:1"] = &types.ChunkServerState{
		Address:       "127.0.0.1:1",
		Rack:          "r0",
		LastHeartbeat: time.Now(),
		Chunks:        make(map[types.ChunkHandle]bool),
	}
	m.mu.Unlock()
	return m
}

// TestPersistRoundTrip 验证 checkpoint + 操作日志的持久化与回放:
// 写入一组操作,重启 Master 后状态必须完整恢复(文件/chunk/引用计数/大小/handle 分配器)。
func TestPersistRoundTrip(t *testing.T) {
	cfg := types.DefaultConfig()
	dir := filepath.Join(t.TempDir(), "master")
	m := newTestMaster(t, cfg, dir)

	// 执行一组命名空间操作
	if err := m.Create(&types.CreateArgs{Path: "/x"}, &types.CreateReply{}); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := m.UpdateSize(&types.UpdateSizeArgs{Path: "/x", Size: 12345}, &types.UpdateSizeReply{}); err != nil {
		t.Fatalf("update size: %v", err)
	}
	if err := m.Snapshot(&types.SnapshotArgs{Path: "/x", SnapPath: "/snap"}, &types.SnapshotReply{}); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if err := m.Rename(&types.RenameArgs{Src: "/x", Dst: "/y"}, &types.RenameReply{}); err != nil {
		t.Fatalf("rename: %v", err)
	}
	// 停服触发 checkpoint
	m.Stop()

	// ---- 方式一:从 checkpoint 恢复 ----
	m2 := newTestMaster(t, cfg, dir)
	m2.mu.RLock()
	fm := m2.namespace["/y"]
	snap := m2.namespace["/snap"]
	if fm == nil || snap == nil {
		m2.mu.RUnlock()
		t.Fatal("files not restored from checkpoint")
	}
	if fm.Size != 12345 {
		t.Errorf("size restored = %d, want 12345", fm.Size)
	}
	if len(fm.Chunks) != 1 || fm.Chunks[0] != 1 {
		t.Errorf("file chunks = %v, want [1]", fm.Chunks)
	}
	if len(snap.Chunks) != 1 || snap.Chunks[0] != 1 {
		t.Errorf("snapshot chunks = %v, want [1]", snap.Chunks)
	}
	// 快照共享:两个文件各引用一次 chunk 1,RefCount 应为 2
	if cm := m2.chunkMeta[1]; cm == nil || cm.RefCount != 2 {
		t.Errorf("shared chunk refcount = %v, want 2", cm)
	}
	if m2.nextHandle <= 1 {
		t.Errorf("nextHandle = %d, want > 1", m2.nextHandle)
	}
	if m2.chunkMeta[1].Version < 1 {
		t.Errorf("chunk version = %v, want >= 1", m2.chunkMeta[1].Version)
	}
	m2.mu.RUnlock()
	m2.Stop()

	// ---- 方式二:不做 checkpoint,仅保留操作日志,验证纯日志回放 ----
	replayDir := filepath.Join(t.TempDir(), "master")
	m4 := newTestMaster(t, cfg, replayDir)
	if err := m4.Create(&types.CreateArgs{Path: "/z"}, &types.CreateReply{}); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := m4.UpdateSize(&types.UpdateSizeArgs{Path: "/z", Size: 777}, &types.UpdateSizeReply{}); err != nil {
		t.Fatalf("update size: %v", err)
	}
	// 直接关闭日志文件(不保存 checkpoint),模拟崩溃
	if err := m4.opFile.close(); err != nil {
		t.Fatalf("close oplog: %v", err)
	}
	m4.opFile = nil

	m5 := newTestMaster(t, cfg, replayDir)
	m5.mu.RLock()
	if m5.namespace["/z"] == nil {
		m5.mu.RUnlock()
		t.Fatal("state not restored from op log replay")
	}
	if m5.namespace["/z"].Size != 777 {
		t.Errorf("replayed size = %d, want 777", m5.namespace["/z"].Size)
	}
	if len(m5.namespace["/z"].Chunks) != 1 || m5.namespace["/z"].Chunks[0] != 1 {
		t.Errorf("replayed chunks = %v, want [1]", m5.namespace["/z"].Chunks)
	}
	if m5.chunkMeta[1].RefCount != 1 {
		t.Errorf("replayed chunk refcount = %d, want 1", m5.chunkMeta[1].RefCount)
	}
	m5.mu.RUnlock()
	m5.Stop()
}

// TestPersistDeleteGC 验证删除/GC 操作在回放后状态一致:
// 隐藏文件保留引用,GC 硬删除后引用释放。
func TestPersistDeleteGC(t *testing.T) {
	cfg := types.DefaultConfig()
	dir := filepath.Join(t.TempDir(), "master")
	m := newTestMaster(t, cfg, dir)

	if err := m.Create(&types.CreateArgs{Path: "/del"}, &types.CreateReply{}); err != nil {
		t.Fatalf("create: %v", err)
	}
	// 惰性删除
	if err := m.Delete(&types.DeleteArgs{Path: "/del"}, &types.DeleteReply{}); err != nil {
		t.Fatalf("delete: %v", err)
	}
	m.Stop()

	m2 := newTestMaster(t, cfg, dir)
	m2.mu.RLock()
	hidden := ""
	for p, fm := range m2.namespace {
		if fm.IsDeleted {
			hidden = p
		}
	}
	if hidden == "" {
		m2.mu.RUnlock()
		t.Fatal("deleted file not restored under hidden dir")
	}
	// 隐藏文件仍引用 chunk
	if m2.chunkMeta[1].RefCount != 1 {
		m2.mu.RUnlock()
		t.Fatalf("hidden file chunk refcount = %d, want 1", m2.chunkMeta[1].RefCount)
	}
	m2.mu.RUnlock()

	// 模拟 GC 硬删除
	m2.mu.Lock()
	fm := m2.namespace[hidden]
	for _, h := range fm.Chunks {
		if cm := m2.chunkMeta[h]; cm != nil && cm.RefCount > 0 {
			cm.RefCount--
		}
	}
	delete(m2.namespace, hidden)
	_ = m2.appendOp(types.OpDelete, types.DeletePayload{Path: hidden})
	m2.mu.Unlock()
	m2.Stop()

	// 回放后:文件被清除,chunk 成为孤儿(RefCount 0)
	m3 := newTestMaster(t, cfg, dir)
	m3.mu.RLock()
	if len(m3.namespace) != 0 {
		m3.mu.RUnlock()
		t.Fatalf("namespace should be empty after GC, got %d entries", len(m3.namespace))
	}
	if m3.chunkMeta[1].RefCount != 0 {
		m3.mu.RUnlock()
		t.Fatalf("chunk refcount = %d, want 0 after GC", m3.chunkMeta[1].RefCount)
	}
	m3.mu.RUnlock()
	m3.Stop()
}
