package client_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"gfs/internal/chunkserver"
	"gfs/internal/client"
	"gfs/internal/master"
	"gfs/internal/types"
	"gfs/internal/utils"
)

// testCluster 启动一套进程内集群:1 个 Master + N 个 ChunkServer。
type testCluster struct {
	t          *testing.T
	cfg        *types.Config
	master     *master.Master
	masterAddr string
	masterDir  string
	servers    []*chunkserver.ChunkServer
	serverDirs []string
}

// startCluster 启动集群并等待所有 ChunkServer 完成首次心跳注册。
func startCluster(t *testing.T, numServers, replication int, chunkSize int64) *testCluster {
	t.Helper()
	cfg := types.DefaultConfig()
	cfg.ChunkSize = chunkSize
	cfg.ReplicationFactor = replication
	cfg.HeartbeatInterval = 50 * time.Millisecond
	cfg.HeartbeatTimeout = 300 * time.Millisecond
	cfg.LeaseTimeout = 2 * time.Second
	cfg.GCDelay = 5 * time.Second
	cfg.GCInterval = 1 * time.Second
	cfg.RecheckInterval = 300 * time.Millisecond
	cfg.MaxRPCDataSize = 1 << 20 // 1MB,便于测试多 chunk/多块路径

	dir := t.TempDir()
	m, err := master.New(cfg, filepath.Join(dir, "master"))
	if err != nil {
		t.Fatalf("start master: %v", err)
	}
	if err := m.Start("127.0.0.1:0"); err != nil {
		t.Fatalf("listen master: %v", err)
	}
	tc := &testCluster{
		t:          t,
		cfg:        cfg,
		master:     m,
		masterAddr: m.Addr(),
		masterDir:  filepath.Join(dir, "master"),
	}
	for i := 0; i < numServers; i++ {
		sdir := filepath.Join(dir, fmt.Sprintf("cs%d", i))
		cs, err := chunkserver.New(cfg, "127.0.0.1:0", m.Addr(), sdir, fmt.Sprintf("rack%d", i%2))
		if err != nil {
			t.Fatalf("start chunkserver %d: %v", i, err)
		}
		if err := cs.Start(); err != nil {
			t.Fatalf("listen chunkserver %d: %v", i, err)
		}
		tc.servers = append(tc.servers, cs)
		tc.serverDirs = append(tc.serverDirs, sdir)
	}
	// 等待所有服务器注册(心跳重建)
	waitFor(t, 10*time.Second, func() bool { return m.LiveServerCount() >= numServers })
	return tc
}

// stop 停止集群:先停 ChunkServer,再停 Master。
func (tc *testCluster) stop() {
	for _, cs := range tc.servers {
		cs.Stop()
	}
	tc.master.Stop()
}

// client 创建客户端。
func (tc *testCluster) client() *client.GFSClient {
	return client.New(tc.masterAddr, tc.cfg)
}

// killServer 停止指定 ChunkServer(模拟故障)。
func (tc *testCluster) killServer(i int) {
	tc.t.Helper()
	tc.servers[i].Stop()
}

// killServerByAddr 按地址停止 ChunkServer(模拟主副本所在服务器故障)。
func (tc *testCluster) killServerByAddr(addr string) {
	tc.t.Helper()
	for i, cs := range tc.servers {
		if cs.Addr() == addr {
			tc.killServer(i)
			return
		}
	}
	tc.t.Fatalf("chunkserver %s not found", addr)
}

// restartMaster 重启 Master(复用同一数据目录与地址,验证 checkpoint + 心跳重建)。
func (tc *testCluster) restartMaster() {
	tc.t.Helper()
	addr := tc.masterAddr
	tc.master.Stop()
	m, err := master.New(tc.cfg, tc.masterDir)
	if err != nil {
		tc.t.Fatalf("reinit master: %v", err)
	}
	if err := m.Start(addr); err != nil {
		tc.t.Fatalf("restart master: %v", err)
	}
	tc.master = m
	waitFor(tc.t, 10*time.Second, func() bool { return m.LiveServerCount() >= len(tc.servers) })
}

// waitFor 轮询等待条件成立。
func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("condition not met within %v", timeout)
}

// lcgPattern 确定性伪随机数据生成器(同一 seed 产生同一序列)。
type lcgPattern struct{ seed uint32 }

func (p *lcgPattern) fill(buf []byte) {
	state := p.seed
	for i := range buf {
		state = state*1664525 + 1013904223
		buf[i] = byte(state >> 24)
	}
	p.seed = state
}

// ---- 测试场景 1:基础读写(创建、写入 100MB、读取并校验 MD5) ----

func TestBasicReadWrite(t *testing.T) {
	tc := startCluster(t, 3, 3, 1<<20) // chunk 1MB,共 100 个 chunk
	defer tc.stop()

	c := tc.client()
	const size = 100 << 20
	data := make([]byte, size)
	gen := &lcgPattern{seed: 42}
	gen.fill(data)
	expect := utils.MD5Hex(data)

	if err := c.Create("/basic.bin"); err != nil {
		t.Fatalf("create: %v", err)
	}
	f, err := c.Open("/basic.bin")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	n, err := f.WriteAt(data, 0)
	if err != nil || n != len(data) {
		t.Fatalf("write: n=%d err=%v", n, err)
	}

	got := make([]byte, size)
	n, err = f.ReadAt(got, 0)
	if err != nil || n != len(got) {
		t.Fatalf("read: n=%d err=%v", n, err)
	}
	if utils.MD5Hex(got) != expect {
		t.Fatal("MD5 mismatch: read data differs from written data")
	}
	// 稀疏读取:跳过中间区域,验证偏移正确
	mid := make([]byte, 3<<20)
	if _, err := f.ReadAt(mid, 50<<20); err != nil {
		t.Fatalf("read at offset: %v", err)
	}
	if !bytes.Equal(mid, data[50<<20:53<<20]) {
		t.Fatal("read at offset mismatch")
	}
	// 越界读取:返回 EOF 语义(0 字节)
	if n, _ := f.ReadAt(got[:100], size); n != 0 {
		t.Fatalf("read beyond EOF returned %d bytes", n)
	}
}

// ---- 测试场景 2:并发原子追加(10 goroutine,记录完整且不重叠) ----

const (
	appendPayloadSize   = 2048
	appendHeaderSize    = 16
	appendRecordSize    = appendHeaderSize + appendPayloadSize
	appendGoroutines    = 10
	appendPerGoroutine  = 30
)

// appendRecord 构造一条可校验的记录:magic + goroutineID + seq + 负载 CRC32。
func appendRecord(gid, seq uint32) []byte {
	rec := make([]byte, appendRecordSize)
	binary.BigEndian.PutUint32(rec[0:4], 0x47465341) // magic "GFSA"
	binary.BigEndian.PutUint32(rec[4:8], gid)
	binary.BigEndian.PutUint32(rec[8:12], seq)
	payload := rec[appendHeaderSize:]
	gen := &lcgPattern{seed: gid*1000003 + seq}
	gen.fill(payload)
	binary.BigEndian.PutUint32(rec[12:16], utils.CRC32(payload))
	return rec
}

func TestConcurrentAppend(t *testing.T) {
	tc := startCluster(t, 3, 3, 4<<20)
	defer tc.stop()

	c := tc.client()
	if err := c.Create("/append.log"); err != nil {
		t.Fatalf("create: %v", err)
	}
	f, err := c.Open("/append.log")
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	var wg sync.WaitGroup
	errorsCh := make(chan error, appendGoroutines)
	for g := uint32(0); g < appendGoroutines; g++ {
		wg.Add(1)
		go func(gid uint32) {
			defer wg.Done()
			for seq := uint32(0); seq < appendPerGoroutine; seq++ {
				rec := appendRecord(gid, seq)
				if _, err := f.Append(rec); err != nil {
					errorsCh <- fmt.Errorf("gid=%d seq=%d append: %w", gid, seq, err)
					return
				}
			}
		}(g)
	}
	wg.Wait()
	close(errorsCh)
	for err := range errorsCh {
		t.Fatal(err)
	}

	// 读回全部数据并校验:总大小正确、每条记录完整出现一次、无重叠
	data, err := c.ReadFile("/append.log")
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	want := appendGoroutines * appendPerGoroutine * appendRecordSize
	if len(data) != want {
		t.Fatalf("file size %d, want %d (records overlap or lost)", len(data), want)
	}
	found := make(map[[2]uint32]int)
	// 扫描每条记录
	for off := 0; off+appendRecordSize <= len(data); {
		hdr := data[off : off+appendHeaderSize]
		if binary.BigEndian.Uint32(hdr[0:4]) != 0x47465341 {
			t.Fatalf("corrupted header at offset %d (record torn/overlapped)", off)
		}
		gid := binary.BigEndian.Uint32(hdr[4:8])
		seq := binary.BigEndian.Uint32(hdr[8:12])
		crc := binary.BigEndian.Uint32(hdr[12:16])
		payload := data[off+appendHeaderSize : off+appendRecordSize]
		if utils.CRC32(payload) != crc {
			t.Fatalf("payload checksum mismatch at offset %d (gid=%d seq=%d)", off, gid, seq)
		}
		// 校验负载内容与预期一致
		expect := make([]byte, appendPayloadSize)
		gen := &lcgPattern{seed: gid*1000003 + seq}
		gen.fill(expect)
		if !bytes.Equal(payload, expect) {
			t.Fatalf("payload content mismatch at offset %d (gid=%d seq=%d)", off, gid, seq)
		}
		found[[2]uint32{gid, seq}]++
		off += appendRecordSize
	}
	if len(found) != appendGoroutines*appendPerGoroutine {
		t.Fatalf("found %d distinct records, want %d", len(found), appendGoroutines*appendPerGoroutine)
	}
	for key, cnt := range found {
		if cnt != 1 {
			t.Fatalf("record %v appears %d times", key, cnt)
		}
	}
}

// ---- 测试场景 3:ChunkServer 故障(写继续,副本最终补齐) ----

func TestChunkServerFailure(t *testing.T) {
	tc := startCluster(t, 4, 3, 1<<20)
	defer tc.stop()

	c := tc.client()
	const size = 40 << 20
	data := make([]byte, size)
	gen := &lcgPattern{seed: 7}
	gen.fill(data)

	if err := c.Create("/fail.bin"); err != nil {
		t.Fatalf("create: %v", err)
	}
	f, err := c.Open("/fail.bin")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	// 第一阶段:写入 20MB
	if n, err := f.WriteAt(data[:20<<20], 0); err != nil || n != 20<<20 {
		t.Fatalf("write phase1: n=%d err=%v", n, err)
	}
	// 杀掉一台 ChunkServer
	tc.killServer(0)
	// 等待 Master 判定其死亡
	waitFor(t, 10*time.Second, func() bool { return tc.master.LiveServerCount() == 3 })

	// 第二阶段:继续写入剩余 20MB(系统必须继续工作)
	if n, err := f.WriteAt(data[20<<20:], 20<<20); err != nil || n != 20<<20 {
		t.Fatalf("write phase2: n=%d err=%v", n, err)
	}

	// 等待副本再复制:所有 chunk 重新达到 3 个存活副本
	waitFor(t, 20*time.Second, func() bool {
		for _, n := range tc.master.ChunkReplicaStats() {
			if n < 3 {
				return false
			}
		}
		return true
	})

	// 读取全量数据并校验
	got, err := c.ReadFile("/fail.bin")
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if utils.MD5Hex(got) != utils.MD5Hex(data) {
		t.Fatal("MD5 mismatch after chunkserver failure")
	}
}

// ---- 测试场景 4:Master 重启(心跳重建位置,读写正常) ----

func TestMasterRestart(t *testing.T) {
	tc := startCluster(t, 3, 3, 1<<20)
	defer tc.stop()

	c := tc.client()
	const size = 12 << 20
	data := make([]byte, size)
	gen := &lcgPattern{seed: 99}
	gen.fill(data)

	if err := c.Create("/restart.bin"); err != nil {
		t.Fatalf("create: %v", err)
	}
	f, err := c.Open("/restart.bin")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if n, err := f.WriteAt(data[:8<<20], 0); err != nil || n != 8<<20 {
		t.Fatalf("write phase1: n=%d err=%v", n, err)
	}

	// 重启 Master
	tc.restartMaster()

	// 继续写入(位置信息已通过心跳重建)
	c2 := tc.client()
	f2, err := c2.Open("/restart.bin")
	if err != nil {
		t.Fatalf("open after restart: %v", err)
	}
	if n, err := f2.WriteAt(data[8<<20:], 8<<20); err != nil || n != size-8<<20 {
		t.Fatalf("write phase2: n=%d want=%d err=%v", n, size-8<<20, err)
	}
	// 读取全部并校验
	got, err := c2.ReadFile("/restart.bin")
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if utils.MD5Hex(got) != utils.MD5Hex(data) {
		t.Fatal("MD5 mismatch after master restart")
	}
	// 文件大小在重启后仍正确
	size2, _, err := c2.Stat("/restart.bin")
	if err != nil || size2 != size {
		t.Fatalf("stat after restart: size=%d err=%v", size2, err)
	}
}

// ---- 测试场景 5:快照(写时复制,修改原文件后快照内容不变) ----

func TestSnapshot(t *testing.T) {
	tc := startCluster(t, 3, 3, 1<<20)
	defer tc.stop()

	c := tc.client()
	const originalSize = 5 << 20
	original := make([]byte, originalSize)
	gen := &lcgPattern{seed: 1234}
	gen.fill(original)

	if err := c.Create("/snap_src.bin"); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := c.WriteFile("/snap_src.bin", original); err != nil {
		t.Fatalf("write: %v", err)
	}

	// 创建快照
	if err := c.Snapshot("/snap_src.bin", "/snap_dst.bin"); err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	// 修改原文件:末尾追加 512KB + 覆盖中间 1MB(触发写时复制)
	f, err := c.Open("/snap_src.bin")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	appendData := make([]byte, 512<<10)
	gen2 := &lcgPattern{seed: 555}
	gen2.fill(appendData)
	if _, err := f.Append(appendData); err != nil {
		t.Fatalf("append: %v", err)
	}
	overwrite := make([]byte, 1<<20)
	gen3 := &lcgPattern{seed: 777}
	gen3.fill(overwrite)
	if _, err := f.WriteAt(overwrite, 2<<20); err != nil {
		t.Fatalf("overwrite: %v", err)
	}

	// 原文件内容 = 原数据 + 追加(中间被覆盖)
	expected := make([]byte, 0, originalSize+len(appendData))
	expected = append(expected, original[:2<<20]...)
	expected = append(expected, overwrite...)
	expected = append(expected, original[3<<20:]...)
	expected = append(expected, appendData...)

	gotSrc, err := c.ReadFile("/snap_src.bin")
	if err != nil {
		t.Fatalf("read src: %v", err)
	}
	if len(gotSrc) != len(expected) {
		t.Fatalf("src len=%d want=%d", len(gotSrc), len(expected))
	}
	if utils.MD5Hex(gotSrc) != utils.MD5Hex(expected) {
		t.Fatal("original file content mismatch after modification")
	}
	// 快照内容必须保持原样
	gotDst, err := c.ReadFile("/snap_dst.bin")
	if err != nil {
		t.Fatalf("read snapshot: %v", err)
	}
	if utils.MD5Hex(gotDst) != utils.MD5Hex(original) {
		t.Fatal("snapshot content changed (copy-on-write broken)")
	}
	// 快照大小冻结
	sz, _, err := c.Stat("/snap_dst.bin")
	if err != nil || sz != originalSize {
		t.Fatalf("snapshot size = %d, want %d", sz, originalSize)
	}
}

// ---- 补充:删除 + 垃圾回收 ----

func TestDeleteAndGC(t *testing.T) {
	tc := startCluster(t, 3, 3, 1<<20)
	defer tc.stop()

	c := tc.client()
	data := make([]byte, 2<<20)
	gen := &lcgPattern{seed: 1}
	gen.fill(data)
	if err := c.WriteFile("/gc.bin", data); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := c.Delete("/gc.bin"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	// 删除后路径不可访问
	if _, err := c.Open("/gc.bin"); err == nil {
		t.Fatal("open deleted file should fail")
	}
	// 重命名已删除文件应失败
	if err := c.Rename("/gc.bin", "/gc2.bin"); err == nil {
		t.Fatal("rename deleted file should fail")
	}
	// 等 GC 清除隐藏文件(GCInterval 1s,GCDelay 5s)
	waitFor(t, 20*time.Second, func() bool {
		stats := tc.master.ChunkReplicaStats()
		return len(stats) == 0
	})
	// 重新创建同名文件应当成功
	if err := c.Create("/gc.bin"); err != nil {
		t.Fatalf("recreate after GC: %v", err)
	}
}
