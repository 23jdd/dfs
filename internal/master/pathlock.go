package master

import (
	"sort"
	"sync"

	"gfs/internal/utils"
)

// pathLocker 实现路径级读写锁:
//   - 对文件路径 /a/b/c 操作时,祖先路径(/、/a、/a/b)加读锁,叶子路径加写锁;
//   - 多路径操作(如重命名)按字典序获取所有锁,避免死锁;
//   - 锁对象带引用计数,无人使用后自动回收,防止 map 无限增长。
type pathLocker struct {
	mu    sync.Mutex
	locks map[string]*sync.RWMutex
	refs  map[string]int
}

func newPathLocker() *pathLocker {
	return &pathLocker{
		locks: make(map[string]*sync.RWMutex),
		refs:  make(map[string]int),
	}
}

// lock 对 writes 中的路径加写锁、reads 中的路径加读锁。
// 所有涉及的锁按字典序获取(写锁优先于读锁获取同一条路径的情况已在入参去重),
// 返回释放函数(按逆序解锁)。
func (pl *pathLocker) lock(writes, reads []string) func() {
	// 合并并去重
	set := make(map[string]bool)
	for _, p := range writes {
		set[p] = true
	}
	for _, p := range reads {
		set[p] = true
	}
	paths := make([]string, 0, len(set))
	for p := range set {
		paths = append(paths, p)
	}
	sort.Strings(paths)

	pl.mu.Lock()
	for _, p := range paths {
		if pl.locks[p] == nil {
			pl.locks[p] = &sync.RWMutex{}
		}
		pl.refs[p]++
	}
	pl.mu.Unlock()

	isWrite := func(p string) bool {
		for _, w := range writes {
			if w == p {
				return true
			}
		}
		return false
	}

	held := make([]string, 0, len(paths))
	for _, p := range paths {
		if isWrite(p) {
			pl.locks[p].Lock()
		} else {
			pl.locks[p].RLock()
		}
		held = append(held, p)
	}

	return func() {
		for i := len(held) - 1; i >= 0; i-- {
			p := held[i]
			if isWrite(p) {
				pl.locks[p].Unlock()
			} else {
				pl.locks[p].RUnlock()
			}
			pl.mu.Lock()
			pl.refs[p]--
			if pl.refs[p] == 0 {
				delete(pl.locks, p)
				delete(pl.refs, p)
			}
			pl.mu.Unlock()
		}
	}
}

// lockFileWrite 对多个文件路径加锁:祖先路径读锁,叶子路径写锁。
func (pl *pathLocker) lockFileWrite(paths ...string) func() {
	var writes, reads []string
	for _, p := range paths {
		parts := utils.SplitPath(p)
		for i, part := range parts {
			if i == len(parts)-1 {
				writes = append(writes, part)
			} else {
				reads = append(reads, part)
			}
		}
	}
	return pl.lock(writes, reads)
}

// lockFileRead 对多个文件路径加读锁。
func (pl *pathLocker) lockFileRead(paths ...string) func() {
	var reads []string
	for _, p := range paths {
		reads = append(reads, utils.SplitPath(p)...)
	}
	return pl.lock(nil, reads)
}
