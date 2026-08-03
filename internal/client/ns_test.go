package client_test

import (
	"errors"
	"sync"
	"testing"

	"gfs/internal/types"
)

// TestNamespaceOps 验证命名空间的语义:创建/重复创建/重命名/删除/非法路径。
func TestNamespaceOps(t *testing.T) {
	tc := startCluster(t, 2, 2, 1<<20)
	defer tc.stop()
	c := tc.client()

	// 创建与写入
	if err := c.Create("/ns/a.txt"); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := c.WriteFile("/ns/a.txt", []byte("hello")); err != nil {
		t.Fatalf("write: %v", err)
	}

	// 重复创建 → ErrFileExists
	if err := c.Create("/ns/a.txt"); !errors.Is(err, types.ErrFileExists) {
		t.Fatalf("dup create err = %v, want ErrFileExists", err)
	}
	// 创建已存在的快照目标 → ErrFileExists
	if err := c.Snapshot("/ns/a.txt", "/ns/a.txt"); !errors.Is(err, types.ErrFileExists) {
		t.Fatalf("snapshot onto existing err = %v, want ErrFileExists", err)
	}

	// 非法路径
	if err := c.Create("relative"); !errors.Is(err, types.ErrInvalidPath) {
		t.Fatalf("relative path err = %v, want ErrInvalidPath", err)
	}
	if err := c.Create("/"); !errors.Is(err, types.ErrInvalidPath) {
		t.Fatalf("root create err = %v, want ErrInvalidPath", err)
	}

	// 缺失文件
	if _, _, err := c.Stat("/ns/nope"); !errors.Is(err, types.ErrFileNotFound) {
		t.Fatalf("stat missing err = %v, want ErrFileNotFound", err)
	}
	if err := c.Delete("/ns/nope"); !errors.Is(err, types.ErrFileNotFound) {
		t.Fatalf("delete missing err = %v, want ErrFileNotFound", err)
	}

	// 重命名:旧路径失效,新路径可读
	if err := c.Rename("/ns/a.txt", "/ns/b.txt"); err != nil {
		t.Fatalf("rename: %v", err)
	}
	if _, _, err := c.Stat("/ns/a.txt"); !errors.Is(err, types.ErrFileNotFound) {
		t.Fatalf("old path after rename err = %v, want ErrFileNotFound", err)
	}
	got, err := c.ReadFile("/ns/b.txt")
	if err != nil || string(got) != "hello" {
		t.Fatalf("read renamed: %q, %v", got, err)
	}
	// 重命名到已存在 → ErrFileExists
	if err := c.Create("/ns/c.txt"); err != nil {
		t.Fatalf("create c: %v", err)
	}
	if err := c.Rename("/ns/b.txt", "/ns/c.txt"); !errors.Is(err, types.ErrFileExists) {
		t.Fatalf("rename to existing err = %v, want ErrFileExists", err)
	}

	// 删除后不可访问,删除已删除文件报错
	if err := c.Delete("/ns/c.txt"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := c.Open("/ns/c.txt"); !errors.Is(err, types.ErrFileNotFound) {
		t.Fatalf("open deleted err = %v, want ErrFileNotFound", err)
	}
	if err := c.Delete("/ns/c.txt"); !errors.Is(err, types.ErrFileNotFound) {
		t.Fatalf("double delete err = %v, want ErrFileNotFound", err)
	}

	// 深路径文件(父目录无需显式创建)
	if err := c.Create("/a/b/c/d.txt"); err != nil {
		t.Fatalf("deep create: %v", err)
	}
	// 子路径与父路径共存互不影响
	if err := c.Create("/a"); err != nil {
		t.Fatalf("create /a: %v", err)
	}
	if err := c.Create("/a/b/c/d2.txt"); err != nil {
		t.Fatalf("create sibling: %v", err)
	}
}

// TestConcurrentCreate 并发创建同一文件:必须恰好一个成功。
func TestConcurrentCreate(t *testing.T) {
	tc := startCluster(t, 2, 2, 1<<20)
	defer tc.stop()
	c := tc.client()

	const workers = 10
	var wg sync.WaitGroup
	results := make(chan error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results <- c.Create("/race.txt")
		}()
	}
	wg.Wait()
	close(results)

	okCount := 0
	for err := range results {
		if err == nil {
			okCount++
		} else if !errors.Is(err, types.ErrFileExists) {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if okCount != 1 {
		t.Fatalf("concurrent create succeeded %d times, want exactly 1", okCount)
	}
	// 文件最终可写可读
	if err := c.WriteFile("/race.txt", []byte("race")); err != nil {
		t.Fatalf("write after race: %v", err)
	}
	got, err := c.ReadFile("/race.txt")
	if err != nil || string(got) != "race" {
		t.Fatalf("read after race: %q, %v", got, err)
	}
}

// TestRenameWhileReading 重命名期间并发读取:读取要么成功(新旧路径其一),
// 要么返回 ErrFileNotFound,绝不读到错误内容。
func TestRenameWhileReading(t *testing.T) {
	tc := startCluster(t, 2, 2, 1<<20)
	defer tc.stop()
	c := tc.client()

	data := make([]byte, 512<<10)
	gen := &lcgPattern{seed: 11}
	gen.fill(data)
	if err := c.WriteFile("/rn/src.bin", data); err != nil {
		t.Fatalf("write: %v", err)
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})
	readErr := make(chan error, 32)
	// 读者:轮流读新旧路径
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				for _, p := range []string{"/rn/src.bin", "/rn/dst.bin"} {
					got, err := c.ReadFile(p)
					if err != nil {
						if errors.Is(err, types.ErrFileNotFound) {
							continue // 重命名瞬间旧路径消失属正常
						}
						readErr <- err
						return
					}
					if len(got) != len(data) {
						readErr <- errors.New("short read")
						return
					}
				}
			}
		}()
	}
	// 写者:反复重命名
	for i := 0; i < 50; i++ {
		_ = c.Rename("/rn/src.bin", "/rn/dst.bin")
		_ = c.Rename("/rn/dst.bin", "/rn/src.bin")
	}
	close(stop)
	wg.Wait()
	close(readErr)
	for err := range readErr {
		t.Fatal(err)
	}
}
