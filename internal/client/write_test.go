package client_test

import (
	"bytes"
	"sync"
	"testing"
)

// TestSparseWrite 验证稀疏写:未写区域读回为全零,文件大小正确。
func TestSparseWrite(t *testing.T) {
	tc := startCluster(t, 3, 3, 1<<20)
	defer tc.stop()
	c := tc.client()

	first := make([]byte, 256<<10)
	last := make([]byte, 256<<10)
	gen := &lcgPattern{seed: 21}
	gen.fill(first)
	gen.fill(last)

	f, err := c.Open("/sparse.bin")
	if err == nil {
		t.Fatal("open missing file should fail")
	}
	if err := c.Create("/sparse.bin"); err != nil {
		t.Fatalf("create: %v", err)
	}
	f, err = c.Open("/sparse.bin")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := f.WriteAt(first, 0); err != nil {
		t.Fatalf("write head: %v", err)
	}
	if _, err := f.WriteAt(last, 3<<20); err != nil {
		t.Fatalf("write tail: %v", err)
	}
	size, err := f.Size()
	if err != nil || size != 3<<20+256<<10 {
		t.Fatalf("size = %d, want %d", size, 3<<20+256<<10)
	}
	got, err := c.ReadFile("/sparse.bin")
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	want := make([]byte, 3<<20+256<<10)
	copy(want, first)
	copy(want[3<<20:], last)
	if !bytes.Equal(got, want) {
		t.Fatal("sparse read mismatch (middle should be zeros)")
	}
}

// TestSpanningWrite 验证单次 WriteAt 跨越多个 chunk 边界。
func TestSpanningWrite(t *testing.T) {
	tc := startCluster(t, 3, 3, 1<<20)
	defer tc.stop()
	c := tc.client()

	data := make([]byte, 3<<20) // 3MB,覆盖 3 个 chunk
	gen := &lcgPattern{seed: 33}
	gen.fill(data)

	if err := c.WriteFile("/span.bin", make([]byte, 2<<20)); err != nil {
		t.Fatalf("create+write: %v", err)
	}
	f, err := c.Open("/span.bin")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	// 从 512KB 起写入 3MB:跨越 chunk 0/1/2/3 四个 chunk
	if n, err := f.WriteAt(data, 512<<10); err != nil || n != len(data) {
		t.Fatalf("spanning write: n=%d err=%v", n, err)
	}
	got, err := c.ReadFile("/span.bin")
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	want := make([]byte, 512<<10+len(data))
	copy(want[512<<10:], data)
	if !bytes.Equal(got, want) {
		t.Fatal("spanning write mismatch")
	}
}

// TestOverwrite 验证覆盖写:中间区域被新数据替换,其余保持不变。
func TestOverwrite(t *testing.T) {
	tc := startCluster(t, 3, 3, 1<<20)
	defer tc.stop()
	c := tc.client()

	orig := make([]byte, 1<<20)
	genA := &lcgPattern{seed: 44}
	genA.fill(orig)
	if err := c.WriteFile("/ow.bin", orig); err != nil {
		t.Fatalf("write: %v", err)
	}
	repl := make([]byte, 256<<10)
	genB := &lcgPattern{seed: 55}
	genB.fill(repl)

	f, _ := c.Open("/ow.bin")
	if _, err := f.WriteAt(repl, 384<<10); err != nil {
		t.Fatalf("overwrite: %v", err)
	}
	got, err := c.ReadFile("/ow.bin")
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	want := make([]byte, 1<<20)
	copy(want, orig)
	copy(want[384<<10:384<<10+len(repl)], repl)
	if !bytes.Equal(got, want) {
		t.Fatal("overwrite mismatch")
	}
	// 大小不变
	if size, _, _ := c.Stat("/ow.bin"); size != 1<<20 {
		t.Fatalf("size = %d, want %d", size, 1<<20)
	}
}

// TestAppendUntilChunkFull 验证原子追加填满 chunk 后自动分配新 chunk:
// 记录完整、偏移单调、可全部读回。
func TestAppendUntilChunkFull(t *testing.T) {
	tc := startCluster(t, 3, 3, 1<<20) // chunk 1MB
	defer tc.stop()
	c := tc.client()

	if err := c.Create("/full.log"); err != nil {
		t.Fatalf("create: %v", err)
	}
	f, err := c.Open("/full.log")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	rec := make([]byte, 300<<10) // 300KB:4 条 = 1.2MB,跨越 2 个 chunk
	gen := &lcgPattern{seed: 66}
	gen.fill(rec)

	var offsets []int64
	for i := 0; i < 4; i++ {
		off, err := f.Append(rec)
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		offsets = append(offsets, off)
	}
	// 第 4 条超出 chunk0 剩余空间(100KB),应落入新分配的 chunk(偏移回到 0)
	if offsets[3] != 0 {
		t.Fatalf("4th append should start a new chunk, got offset %d", offsets[3])
	}

	data, err := c.ReadFile("/full.log")
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	// 第 4 条放不进 chunk0 剩余 100KB:主副本把 chunk0 零填充到整块,
	// 记录落入新分配的 chunk1(文件偏移 1MB),保持 chunk 边界对齐。
	wantSize := int64(1<<20) + int64(len(rec))
	if int64(len(data)) != wantSize {
		t.Fatalf("file size = %d, want %d", len(data), wantSize)
	}
	// 前三条记录连续,位于 0/300KB/600KB
	for i := 0; i < 3; i++ {
		if !bytes.Equal(data[i*len(rec):(i+1)*len(rec)], rec) {
			t.Fatalf("record %d content mismatch", i)
		}
	}
	// 零填充间隙区(900KB..1MB)全零
	for _, b := range data[3*len(rec) : 1<<20] {
		if b != 0 {
			t.Fatalf("padding region should be zeros, got %02x", b)
		}
	}
	// 第 4 条记录在新 chunk 起始处(文件偏移 1MB)
	if !bytes.Equal(data[1<<20:1<<20+len(rec)], rec) {
		t.Fatal("record 3 (new chunk) content mismatch")
	}
	// chunk 数应扩展到 2
	_, chunks, _ := c.Stat("/full.log")
	if chunks != 2 {
		t.Fatalf("chunk count = %d, want 2", chunks)
	}
}

// TestConcurrentFiles 验证并发写多个独立文件:各文件内容互不影响。
func TestConcurrentFiles(t *testing.T) {
	tc := startCluster(t, 3, 3, 1<<20)
	defer tc.stop()

	const nFiles = 5
	const size = 2 << 20
	type result struct {
		path string
		data []byte
	}
	results := make([]result, nFiles)
	var wg sync.WaitGroup
	for i := 0; i < nFiles; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c := tc.client() // 每 goroutine 独立客户端
			path := "/multi/file_" + string(rune('a'+i)) + ".bin"
			data := make([]byte, size)
			gen := &lcgPattern{seed: uint32(100 + i)}
			gen.fill(data)
			if err := c.WriteFile(path, data); err != nil {
				t.Errorf("write %s: %v", path, err)
				return
			}
			got, err := c.ReadFile(path)
			if err != nil {
				t.Errorf("read %s: %v", path, err)
				return
			}
			results[i] = result{path: path, data: got}
		}(i)
	}
	wg.Wait()
	for i, r := range results {
		if r.path == "" {
			t.Fatalf("file %d produced no result", i)
		}
		want := make([]byte, size)
		gen := &lcgPattern{seed: uint32(100 + i)}
		gen.fill(want)
		if !bytes.Equal(r.data, want) {
			t.Fatalf("file %s content mismatch", r.path)
		}
	}
}
