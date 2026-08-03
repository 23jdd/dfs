package master

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"

	"gfs/internal/types"
	"gfs/internal/utils"
)

// file 是对持久化日志文件的轻封装:所有写入带长度前缀帧 + fsync。
type file struct {
	path string
	f    *os.File
}

func openLogFile(dir string) (*file, error) {
	path := filepath.Join(dir, "op.log")
	// 注意:不能使用 O_APPEND。Go 在 Windows 上以 O_APPEND 打开文件时
	// 只授予 FILE_APPEND_DATA 权限,后续 Truncate(checkpoint 清空日志)会失败。
	// 改为普通读写打开,写入前手动 seek 到末尾。
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	return &file{path: path, f: f}, nil
}

// writeFrame 追加写入一条长度前缀帧并 fsync,保证崩溃后日志可达。
func (lf *file) writeFrame(data []byte) error {
	if _, err := lf.f.Seek(0, io.SeekEnd); err != nil {
		return err
	}
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], uint32(len(data)))
	if _, err := lf.f.Write(buf[:]); err != nil {
		return err
	}
	if _, err := lf.f.Write(data); err != nil {
		return err
	}
	return lf.f.Sync()
}

// truncate 清空日志文件(用于 checkpoint 之后)。
func (lf *file) truncate() error {
	return lf.f.Truncate(0)
}

func (lf *file) close() error {
	return lf.f.Close()
}

func (lf *file) readAll() ([][]byte, error) {
	data, err := os.ReadFile(lf.path)
	if err != nil {
		return nil, err
	}
	var frames [][]byte
	for off := 0; off < len(data); {
		if off+4 > len(data) {
			break // 截断的尾部长度字段,忽略
		}
		n := binary.BigEndian.Uint32(data[off : off+4])
		off += 4
		if off+int(n) > len(data) {
			break // 截断的帧,忽略
		}
		frames = append(frames, data[off:off+int(n)])
		off += int(n)
	}
	return frames, nil
}

// checkpointState 是持久化的元数据快照。
type checkpointState struct {
	NextHandle types.ChunkHandle
	Namespace  map[string]*types.FileMetadata
	ChunkMeta  map[types.ChunkHandle]*types.ChunkMetadata
}

func init() {
	// 注册操作日志载荷类型,支持 gob 对接口字段的编码
	gob.Register(types.CreatePayload{})
	gob.Register(types.DeletePayload{})
	gob.Register(types.RenamePayload{})
	gob.Register(types.AllocateChunkPayload{})
	gob.Register(types.UpdateVersionPayload{})
	gob.Register(types.GrantLeasePayload{})
	gob.Register(types.SnapshotRefPayload{})
	gob.Register(types.DeleteChunkPayload{})
	gob.Register(types.UpdateSizePayload{})
	gob.Register(types.SnapshotPayload{})
}

// loadState 加载持久化状态:先读 checkpoint,再回放操作日志。
func (m *Master) loadState() error {
	if err := os.MkdirAll(m.dir, 0o755); err != nil {
		return err
	}
	cpPath := filepath.Join(m.dir, "checkpoint.dat")
	if data, err := os.ReadFile(cpPath); err == nil {
		var st checkpointState
		if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&st); err != nil {
			return fmt.Errorf("decode checkpoint: %w", err)
		}
		m.nextHandle = st.NextHandle
		if st.Namespace != nil {
			m.namespace = st.Namespace
		}
		if st.ChunkMeta != nil {
			m.chunkMeta = st.ChunkMeta
		}
	}

	lf, err := openLogFile(m.dir)
	if err != nil {
		return err
	}
	m.opFile = lf
	frames, err := lf.readAll()
	if err != nil {
		return err
	}
	valid := 0
	for _, frame := range frames {
		var entry types.OperationLogEntry
		if err := gob.NewDecoder(bytes.NewReader(frame)).Decode(&entry); err != nil {
			break
		}
		if entry.Checksum != utils.CRC32(append([]byte{byte(entry.OpType)}, entry.Payload...)) {
			log.Printf("master: op log checksum mismatch at entry %d, stop replay", valid)
			break
		}
		m.opLog = append(m.opLog, entry)
		m.applyOp(&entry)
		valid++
	}
	if valid < len(frames) {
		// 日志尾部损坏/截断:截断日志,丢弃损坏部分
		if err := lf.truncate(); err != nil {
			return err
		}
	}
	// 恢复内存日志镜像(仅保留有效条目)
	m.opLog = m.opLog[:valid]
	return nil
}

// appendOp 追加一条操作日志:编码载荷 -> 计算校验和 -> 写入文件并同步内存镜像。
// 调用方必须已持有 m.mu(写锁)。Master 停止后不再写日志,
// 避免在途 RPC 处理器重新打开已关闭的日志文件造成句柄泄漏。
func (m *Master) appendOp(op types.OpType, payload any) error {
	if m.stopped.Load() {
		return nil
	}
	entry := types.OperationLogEntry{
		Timestamp: time.Now().UnixNano(),
		OpType:    op,
	}
	if payload != nil {
		var buf bytes.Buffer
		if err := gob.NewEncoder(&buf).Encode(payload); err != nil {
			return err
		}
		entry.Payload = buf.Bytes()
	}
	entry.Checksum = utils.CRC32(append([]byte{byte(op)}, entry.Payload...))

	if m.opFile == nil {
		// Master 停止后不再写日志(opFile 已被关闭置空),
		// 避免在途 RPC 处理器重新打开已关闭的日志文件造成句柄泄漏。
		return nil
	}

	var frame bytes.Buffer
	if err := gob.NewEncoder(&frame).Encode(&entry); err != nil {
		return err
	}
	if err := m.opFile.writeFrame(frame.Bytes()); err != nil {
		return err
	}
	m.opLog = append(m.opLog, entry)

	m.opCount++
	if m.opCount >= checkpointEvery {
		m.opCount = 0
		// 调用方已持有 m.mu,必须走不重新加锁的内部版本
		if err := m.saveCheckpointLocked(); err != nil {
			return err
		}
	}
	return nil
}

// checkpointEvery 每累积多少条操作做一次 checkpoint。
const checkpointEvery = 1024

// saveCheckpoint 将内存元数据整体快照写入 checkpoint.dat,并清空操作日志。
// 先写临时文件再原子改名,避免写一半的 checkpoint 被加载。
func (m *Master) saveCheckpoint() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.saveCheckpointLocked()
}

// saveCheckpointLocked 是 saveCheckpoint 的内部实现,调用方需持有 m.mu。
func (m *Master) saveCheckpointLocked() error {
	st := checkpointState{
		NextHandle: m.nextHandle,
		Namespace:  m.namespace,
		ChunkMeta:  m.chunkMeta,
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(&st); err != nil {
		return err
	}
	tmp := filepath.Join(m.dir, "checkpoint.dat.tmp")
	if err := os.WriteFile(tmp, buf.Bytes(), 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, filepath.Join(m.dir, "checkpoint.dat")); err != nil {
		return err
	}
	if m.opFile != nil {
		if err := m.opFile.truncate(); err != nil {
			return err
		}
	}
	m.opCount = 0
	// 清空内存镜像,避免 checkpoint 后日志内存无限增长
	m.opLog = nil
	return nil
}

// applyOp 将一条操作日志应用(回放)到内存状态。回放语义与线上操作完全一致,
// 且幂等,保证 checkpoint 与日志交叠时不会重复生效。
func (m *Master) applyOp(entry *types.OperationLogEntry) {
	switch entry.OpType {
	case types.OpCreate:
		var p types.CreatePayload
		if err := gob.NewDecoder(bytes.NewReader(entry.Payload)).Decode(&p); err != nil {
			return
		}
		if _, ok := m.namespace[p.Path]; !ok {
			m.namespace[p.Path] = &types.FileMetadata{Path: p.Path}
		}
	case types.OpDelete:
		var p types.DeletePayload
		if err := gob.NewDecoder(bytes.NewReader(entry.Payload)).Decode(&p); err != nil {
			return
		}
		fm := m.namespace[p.Path]
		if fm == nil {
			return
		}
		if p.HiddenPath != "" {
			// 惰性删除:移入隐藏目录
			fm.Path = p.HiddenPath
			fm.IsDeleted = true
			fm.DeletedAt = time.Unix(0, entry.Timestamp)
			m.namespace[p.HiddenPath] = fm
			delete(m.namespace, p.Path)
		} else {
			// GC 硬删除:释放 chunk 引用
			for _, h := range fm.Chunks {
				if cm := m.chunkMeta[h]; cm != nil && cm.RefCount > 0 {
					cm.RefCount--
				}
			}
			delete(m.namespace, p.Path)
		}
	case types.OpRename:
		var p types.RenamePayload
		if err := gob.NewDecoder(bytes.NewReader(entry.Payload)).Decode(&p); err != nil {
			return
		}
		if fm := m.namespace[p.Src]; fm != nil {
			fm.Path = p.Dst
			m.namespace[p.Dst] = fm
			delete(m.namespace, p.Src)
		}
	case types.OpAllocateChunk:
		var p types.AllocateChunkPayload
		if err := gob.NewDecoder(bytes.NewReader(entry.Payload)).Decode(&p); err != nil {
			return
		}
		fm := m.namespace[p.Path]
		if fm == nil {
			return
		}
		cm := m.chunkMeta[p.Handle]
		if cm == nil {
			cm = &types.ChunkMetadata{Handle: p.Handle, Version: p.Version, RefCount: 1}
			m.chunkMeta[p.Handle] = cm
		}
		if cm.Version < p.Version {
			cm.Version = p.Version
		}
		if p.Index >= len(fm.Chunks) {
			// 追加到文件 chunk 列表末尾(补齐中间空洞)
			for i := len(fm.Chunks); i <= p.Index; i++ {
				fm.Chunks = append(fm.Chunks, p.Handle)
			}
		}
		if p.Handle >= m.nextHandle {
			m.nextHandle = p.Handle + 1
		}
	case types.OpUpdateVersion:
		var p types.UpdateVersionPayload
		if err := gob.NewDecoder(bytes.NewReader(entry.Payload)).Decode(&p); err != nil {
			return
		}
		if cm := m.chunkMeta[p.Handle]; cm != nil && cm.Version < p.Version {
			cm.Version = p.Version
		}
	case types.OpGrantLease:
		// 租约是瞬态信息,重启后由 GetLocations 重新授予,回放忽略。
	case types.OpSnapshotRef:
		// 写时复制交换:旧 chunk 引用数减一;新 chunk 已在 OpAllocateChunk 中计为 1。
		var p types.SnapshotRefPayload
		if err := gob.NewDecoder(bytes.NewReader(entry.Payload)).Decode(&p); err != nil {
			return
		}
		if old := m.chunkMeta[p.OldHandle]; old != nil && old.RefCount > 0 {
			old.RefCount--
		}
	case types.OpDeleteChunk:
		var p types.DeleteChunkPayload
		if err := gob.NewDecoder(bytes.NewReader(entry.Payload)).Decode(&p); err != nil {
			return
		}
		delete(m.chunkMeta, p.Handle)
	case types.OpUpdateSize:
		var p types.UpdateSizePayload
		if err := gob.NewDecoder(bytes.NewReader(entry.Payload)).Decode(&p); err != nil {
			return
		}
		if fm := m.namespace[p.Path]; fm != nil && p.Size > fm.Size {
			fm.Size = p.Size
		}
	case types.OpSnapshot:
		var p types.SnapshotPayload
		if err := gob.NewDecoder(bytes.NewReader(entry.Payload)).Decode(&p); err != nil {
			return
		}
		if _, ok := m.namespace[p.SnapPath]; ok {
			return
		}
		if fm := m.namespace[p.Path]; fm != nil {
			snap := &types.FileMetadata{
				Path:         p.SnapPath,
				Chunks:       append([]types.ChunkHandle(nil), fm.Chunks...),
				Size:         fm.Size,
				SnapRefCount: fm.SnapRefCount + 1,
			}
			fm.SnapRefCount++
			m.namespace[p.SnapPath] = snap
			for _, h := range fm.Chunks {
				if cm := m.chunkMeta[h]; cm != nil {
					cm.RefCount++
				}
			}
		}
	}
}
