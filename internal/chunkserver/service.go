package chunkserver

import (
	"os"
	"time"

	"gfs/internal/rpc"
	"gfs/internal/types"
)

// ---- 两阶段写:数据推送阶段 ----

// PushData 接收推送数据并沿 ForwardTo 链流水线转发。
// 数据先存入本地缓冲,再转发给下一跳,保证所有副本最终都缓存同一份数据。
func (cs *ChunkServer) PushData(args *types.PushDataArgs, reply *types.PushDataReply) error {
	if int64(len(args.Data)) > cs.cfg.ChunkSize {
		return types.ErrChunkFull
	}
	cs.mu.Lock()
	cs.dataBuffer[args.DataID] = bufferedData{data: args.Data, handle: args.Handle, ts: time.Now()}
	cs.mu.Unlock()

	if len(args.ForwardTo) > 0 {
		next := args.ForwardTo[0]
		if err := cs.peerCall(next, rpc.ServiceMethod(types.ChunkServerService, "PushData"),
			&types.PushDataArgs{DataID: args.DataID, Data: args.Data, Handle: args.Handle, ForwardTo: args.ForwardTo[1:]},
			&types.PushDataReply{}); err != nil {
			// 流水线断链:返回错误,客户端会重试整个写入
			return err
		}
	}
	return nil
}

// ---- 两阶段写:控制阶段(主副本) ----

// WriteChunk 由客户端发送给主副本:
//   - 校验版本(版本落后则采纳新版本,版本超前则拒绝);
//   - 普通写:偏移由客户端指定,所有副本在相同偏移写入相同数据;
//   - 原子追加:偏移由主副本根据 chunk 当前长度决定,不足时返回 ErrChunkFull;
//   - 本地应用后,把相同偏移的应用请求并行转发给所有从副本。
func (cs *ChunkServer) WriteChunk(args *types.WriteArgs, reply *types.WriteReply) error {
	lk := cs.chunkLockFor(args.Handle)
	lk.Lock()
	defer lk.Unlock()

	// 版本校验/采纳:写入操作把本副本版本提升到不低于 Master 指示的版本
	if err := cs.adoptVersion(args.Handle, args.Version); err != nil {
		return err
	}

	data := cs.takeBuffer(args.DataID)
	if data == nil {
		return types.ErrDataNotFound
	}

	offset := args.Offset
	if args.Append {
		// 原子追加:偏移 = chunk 当前长度
		offset = cs.chunkLength(args.Handle)
		if offset+int64(len(data)) > cs.cfg.ChunkSize {
			// 记录放不下:把剩余空间零填充(所有副本一致),保持
			// "非末尾 chunk 均为整块"的对齐不变量,然后返回 ErrChunkFull,
			// 客户端会分配新 chunk 重试。
			cs.padChunk(args.Handle, cs.cfg.ChunkSize)
			for _, s := range args.Secondaries {
				_ = cs.peerCall(s, rpc.ServiceMethod(types.ChunkServerService, "ApplyMutation"),
					&types.ApplyArgs{Handle: args.Handle, Version: args.Version, PadTo: cs.cfg.ChunkSize},
					&types.ApplyReply{})
			}
			return types.ErrChunkFull
		}
	} else if offset < 0 || offset+int64(len(data)) > cs.cfg.ChunkSize {
		return types.ErrChunkFull
	}

	// 本地应用(主副本)
	if err := cs.applyData(args.Handle, data, offset); err != nil {
		return err
	}

	// 通知所有从副本以相同偏移应用变更
	var firstErr error
	for _, s := range args.Secondaries {
		if err := cs.peerCall(s, rpc.ServiceMethod(types.ChunkServerService, "ApplyMutation"),
			&types.ApplyArgs{Handle: args.Handle, DataID: args.DataID, Offset: offset, Version: args.Version},
			&types.ApplyReply{}); err != nil {
			// 记录首个错误;本地已应用,客户端重试时同偏移同数据,幂等
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	reply.Offset = offset
	return firstErr
}

// ApplyMutation 由主副本调用,让从副本应用缓冲中的数据或执行零填充扩展。
func (cs *ChunkServer) ApplyMutation(args *types.ApplyArgs, reply *types.ApplyReply) error {
	lk := cs.chunkLockFor(args.Handle)
	lk.Lock()
	defer lk.Unlock()

	if err := cs.adoptVersion(args.Handle, args.Version); err != nil {
		return err
	}
	if args.PadTo > 0 {
		// 零填充扩展(原子追加放不下时的对齐补齐),无需缓冲数据
		return cs.padChunk(args.Handle, args.PadTo)
	}
	data := cs.takeBuffer(args.DataID)
	if data == nil {
		return types.ErrDataNotFound
	}
	if args.Offset < 0 || args.Offset+int64(len(data)) > cs.cfg.ChunkSize {
		return types.ErrChunkFull
	}
	return cs.applyData(args.Handle, data, args.Offset)
}

// padChunk 把 chunk 文件零填充扩展到指定长度(仅扩展,不截断)。
// 调用方需持有该 chunk 的锁。
func (cs *ChunkServer) padChunk(h types.ChunkHandle, to int64) error {
	cur := cs.chunkLength(h)
	if cur >= to {
		return nil
	}
	zeros := make([]byte, to-cur)
	return cs.applyData(h, zeros, cur)
}

// adoptVersion 版本校验/采纳:本地版本低于参数版本则采纳(主副本切换后由客户端
// 携带新版本),高于参数版本则拒绝(说明本副本更新,不应被旧写入覆盖)。
func (cs *ChunkServer) adoptVersion(h types.ChunkHandle, v types.ChunkVersion) error {
	cs.mu.Lock()
	cur := cs.versions[h]
	cs.mu.Unlock()
	if cur > v {
		return types.ErrStaleVersion
	}
	if cur < v {
		cs.mu.Lock()
		if v > cs.versions[h] {
			cs.versions[h] = v
		}
		cs.mu.Unlock()
		// 版本变化同步写入边车文件
		ck := cs.loadChecksums(h)
		ck.Version = v
		return cs.saveChecksums(h, ck)
	}
	return nil
}

// takeBuffer 取出并删除缓冲中的数据。
func (cs *ChunkServer) takeBuffer(id uint64) []byte {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	b := cs.dataBuffer[id]
	if b.data == nil {
		return nil
	}
	delete(cs.dataBuffer, id)
	return b.data
}

// chunkLength 返回 chunk 文件当前长度(原子追加偏移的依据)。
func (cs *ChunkServer) chunkLength(h types.ChunkHandle) int64 {
	st, err := os.Stat(cs.chunkPath(h))
	if err != nil {
		return 0
	}
	return st.Size()
}

// ---- 读取 ----

// ReadChunk 读取 chunk 数据(读取前校验 checksum,损坏返回错误,客户端换副本重试)。
func (cs *ChunkServer) ReadChunk(args *types.ReadArgs, reply *types.ReadReply) error {
	if args.Length > cs.cfg.MaxRPCDataSize {
		args.Length = cs.cfg.MaxRPCDataSize
	}
	lk := cs.chunkLockFor(args.Handle)
	lk.Lock()
	defer lk.Unlock()

	if _, ok := cs.chunks[args.Handle]; !ok {
		return types.ErrChunkNotFound
	}
	data, err := cs.readChunkLocked(args.Handle, args.Offset, args.Length)
	if err != nil {
		return err
	}
	reply.Data = data
	return nil
}

// ---- 副本管理 ----

// CopyChunk 从源服务器复制整个 chunk(供 Master 副本再复制与写时复制使用)。
func (cs *ChunkServer) CopyChunk(args *types.CopyArgs, reply *types.CopyReply) error {
	lk := cs.chunkLockFor(args.Handle)
	lk.Lock()
	defer lk.Unlock()
	srcHandle := args.SourceHandle
	if srcHandle == 0 {
		srcHandle = args.Handle // 副本再复制:源与目标同 handle
	}
	return cs.copyChunkFromServer(args.Handle, srcHandle, args.Version, args.SourceServer)
}

// CreateChunk 预创建空 chunk 文件(Master 分配 chunk 时调用)。
func (cs *ChunkServer) CreateChunk(args *types.CreateChunkArgs, reply *types.CreateChunkReply) error {
	lk := cs.chunkLockFor(args.Handle)
	lk.Lock()
	defer lk.Unlock()
	return cs.createChunkFile(args.Handle, args.Version)
}

// DeleteChunk 删除本地 chunk 数据文件与校验和边车文件。
func (cs *ChunkServer) DeleteChunk(args *types.DeleteChunkArgs, reply *types.DeleteChunkReply) error {
	lk := cs.chunkLockFor(args.Handle)
	lk.Lock()
	defer lk.Unlock()

	cs.mu.Lock()
	_, ok := cs.chunks[args.Handle]
	if ok {
		delete(cs.chunks, args.Handle)
		delete(cs.versions, args.Handle)
	}
	cs.mu.Unlock()
	_ = os.Remove(cs.chunkPath(args.Handle))
	_ = os.Remove(cs.cksPath(args.Handle))

	// 清理该 chunk 的残留推送缓冲
	cs.mu.Lock()
	for id, b := range cs.dataBuffer {
		if b.handle == args.Handle {
			delete(cs.dataBuffer, id)
		}
	}
	cs.mu.Unlock()
	return nil
}
