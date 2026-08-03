package chunkserver

import (
	"bytes"
	"encoding/gob"
	"os"

	"gfs/internal/rpc"
	"gfs/internal/types"
	"gfs/internal/utils"
)

// chunkChecksums 是 chunk 的 checksum 边车文件内容:
//   - Version: chunk 版本号(与 Master 记录比对,用于陈旧副本检测);
//   - BlockSize: checksum 块大小(默认 64KB);
//   - Blocks: 每块一个 CRC32;索引缺失表示该块从未写入(按全零校验)。
type chunkChecksums struct {
	Version   types.ChunkVersion
	BlockSize int
	Blocks    []uint32
}

// loadChecksums 读取边车文件;文件不存在时返回空校验和结构。
func (cs *ChunkServer) loadChecksums(h types.ChunkHandle) *chunkChecksums {
	data, err := os.ReadFile(cs.cksPath(h))
	if err != nil {
		return &chunkChecksums{Version: cs.versions[h], BlockSize: cs.cfg.ChecksumBlockSize}
	}
	var ck chunkChecksums
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&ck); err != nil {
		return &chunkChecksums{Version: cs.versions[h], BlockSize: cs.cfg.ChecksumBlockSize}
	}
	if ck.BlockSize != cs.cfg.ChecksumBlockSize {
		// 块大小配置变化:丢弃旧校验,下次写入重建
		return &chunkChecksums{Version: ck.Version, BlockSize: cs.cfg.ChecksumBlockSize}
	}
	return &ck
}

// saveChecksums 写回边车文件(原子替换)。
func (cs *ChunkServer) saveChecksums(h types.ChunkHandle, ck *chunkChecksums) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(ck); err != nil {
		return err
	}
	tmp := cs.cksPath(h) + ".tmp"
	if err := os.WriteFile(tmp, buf.Bytes(), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, cs.cksPath(h))
}

// loadVersion 从边车文件恢复 chunk 版本号(重启后使用)。
func (cs *ChunkServer) loadVersion(h types.ChunkHandle) types.ChunkVersion {
	return cs.loadChecksums(h).Version
}

// verifyChecksums 校验 [offset, offset+length) 区间(按块对齐扩大)的 checksum。
// 未写入过的块按全零计算期望值;校验失败返回 ErrChecksum。
// 调用方需持有该 chunk 的锁,且文件内容不会被并发修改。
func (cs *ChunkServer) verifyChecksums(h types.ChunkHandle, offset, length int64) error {
	ck := cs.loadChecksums(h)
	bs := int64(ck.BlockSize)
	start := offset - offset%bs
	end := offset + length
	if end%bs != 0 {
		end = end - end%bs + bs
	}
	blockLen := int(bs)
	// 一次性读取整个对齐区间,逐块校验
	buf := make([]byte, end-start)
	n, err := readAtFile(cs.chunkPath(h), buf, start)
	if err != nil && n == 0 {
		return err
	}
	buf = buf[:n]
	for bi := int(start / bs); bi < int(end/bs); bi++ {
		block := blockBytes(buf, (bi-int(start/bs))*blockLen, blockLen)
		got := utils.CRC32Block(block, blockLen)
		var want uint32
		if bi < len(ck.Blocks) {
			want = ck.Blocks[bi]
		} else {
			want = utils.CRC32Block(nil, blockLen) // 从未写入:期望全零
		}
		if got != want {
			return types.ErrChecksum
		}
	}
	return nil
}

// blockBytes 取出 buf 中从 off 开始最多 blockLen 字节的块(尾部按零补齐)。
func blockBytes(buf []byte, off, blockLen int) []byte {
	if off >= len(buf) {
		return nil
	}
	end := off + blockLen
	if end > len(buf) {
		end = len(buf)
	}
	return buf[off:end]
}

// applyData 将数据写入 chunk 文件的指定偏移,并更新受影响块的 checksum。
// 允许稀疏写入:越过文件末尾的区间由文件系统补零,校验和同样按零计算。
// 调用方需持有该 chunk 的锁。
func (cs *ChunkServer) applyData(h types.ChunkHandle, data []byte, offset int64) error {
	if int64(len(data))+offset > cs.cfg.ChunkSize {
		return types.ErrChunkFull
	}
	path := cs.chunkPath(h)
	// 确认文件存在(首次写入时创建)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.WriteAt(data, offset); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}

	// 更新受影响的 checksum 块
	ck := cs.loadChecksums(h)
	bs := int64(ck.BlockSize)
	first := offset / bs
	last := (offset + int64(len(data)) - 1) / bs
	readSize := (last - first + 1) * bs
	rbuf := make([]byte, readSize)
	n, _ := readAtFile(path, rbuf, first*bs)
	rbuf = rbuf[:n]
	for bi := first; bi <= last; bi++ {
		idx := int(bi)
		for len(ck.Blocks) <= idx {
			ck.Blocks = append(ck.Blocks, 0)
		}
		ck.Blocks[idx] = utils.CRC32Block(blockBytes(rbuf, int((bi-first)*bs), int(bs)), int(bs))
	}
	ck.Version = cs.versions[h]
	return cs.saveChecksums(h, ck)
}

// readAtFile 读取文件指定区间的数据,返回实际读到的字节数。
func readAtFile(path string, buf []byte, off int64) (int, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	n, err := f.ReadAt(buf, off)
	if err != nil && n == 0 {
		return 0, err
	}
	return n, nil
}

// readChunkLocked 读取 chunk 数据并校验 checksum(调用方持有 chunk 锁)。
// 返回 [offset, offset+length) 的实际数据;越界部分截断。
func (cs *ChunkServer) readChunkLocked(h types.ChunkHandle, offset, length int64) ([]byte, error) {
	st, err := os.Stat(cs.chunkPath(h))
	if err != nil {
		return nil, err
	}
	if offset >= st.Size() || length <= 0 {
		return nil, nil
	}
	length = utils.MinI64(length, st.Size()-offset)
	if err := cs.verifyChecksums(h, offset, length); err != nil {
		return nil, err
	}
	data := make([]byte, length)
	n, err := readAtFile(cs.chunkPath(h), data, offset)
	if err != nil && n == 0 {
		return nil, err
	}
	return data[:n], nil
}

// copyChunkFromServer 从源服务器循环读取整个 chunk 并写入本地,
// 完成后重建全部 checksum 并记录版本。
// 源端读取的是 srcHandle(写时复制时为旧 handle),本地落盘的是 h。
// 调用方需持有该 chunk 的锁。
func (cs *ChunkServer) copyChunkFromServer(h, srcHandle types.ChunkHandle, version types.ChunkVersion, src string) error {
	path := cs.chunkPath(h)
	_ = os.Remove(path)
	_ = os.Remove(cs.cksPath(h))

	var offset int64
	step := utils.MinI64(cs.cfg.MaxRPCDataSize, cs.cfg.ChunkSize)
	for {
		var chunk []byte
		if src == cs.addr {
			// 源即本服务器:直接本地读取(调用方已持有 chunk 锁,不能走 RPC 重入)
			data, err := cs.readChunkLocked(srcHandle, offset, step)
			if err != nil {
				return err
			}
			chunk = data
		} else {
			var reply types.ReadReply
			err := cs.peerCall(src, rpc.ServiceMethod(types.ChunkServerService, "ReadChunk"),
				&types.ReadArgs{Handle: srcHandle, Offset: offset, Length: step}, &reply)
			if err != nil {
				return err
			}
			chunk = reply.Data
		}
		if len(chunk) == 0 {
			break // 源已到 EOF
		}
		if offset+int64(len(chunk)) > cs.cfg.ChunkSize {
			return types.ErrChunkFull
		}
		f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0o644)
		if err != nil {
			return err
		}
		if _, err := f.WriteAt(chunk, offset); err != nil {
			f.Close()
			return err
		}
		if err := f.Sync(); err != nil {
			f.Close()
			return err
		}
		if err := f.Close(); err != nil {
			return err
		}
		offset += int64(len(chunk))
	}

	// 全量重建校验和
	ck := &chunkChecksums{Version: version, BlockSize: cs.cfg.ChecksumBlockSize}
	if st, err := os.Stat(path); err == nil && st.Size() > 0 {
		bs := int64(ck.BlockSize)
		nBlocks := (st.Size() + bs - 1) / bs
		rbuf := make([]byte, nBlocks*bs)
		n, _ := readAtFile(path, rbuf, 0)
		rbuf = rbuf[:n]
		for bi := int64(0); bi < nBlocks; bi++ {
			ck.Blocks = append(ck.Blocks, utils.CRC32Block(blockBytes(rbuf, int(bi*bs), int(bs)), int(bs)))
		}
	}
	if err := cs.saveChecksums(h, ck); err != nil {
		return err
	}
	cs.mu.Lock()
	cs.chunks[h] = path
	cs.versions[h] = version
	cs.mu.Unlock()
	return nil
}

// createChunkFile 创建空的 chunk 文件并记录版本(调用方持有 chunk 锁)。
func (cs *ChunkServer) createChunkFile(h types.ChunkHandle, version types.ChunkVersion) error {
	path := cs.chunkPath(h)
	if _, err := os.Stat(path); err != nil {
		f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0o644)
		if err != nil {
			return err
		}
		if err := f.Close(); err != nil {
			return err
		}
	}
	// 采纳最高版本(versions 由 cs.mu 保护)
	cs.mu.Lock()
	if version > cs.versions[h] {
		cs.versions[h] = version
	}
	cs.chunks[h] = path
	cs.mu.Unlock()
	ck := cs.loadChecksums(h)
	cs.mu.Lock()
	ck.Version = cs.versions[h]
	cs.mu.Unlock()
	return cs.saveChecksums(h, ck)
}
