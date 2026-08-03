package client

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"gfs/internal/types"
	"gfs/internal/utils"
)

// File 是 GFS 文件句柄,提供 POSIX 风格的读写与原子追加。
// 同一句柄上的操作由 mu 串行化,保证大小上报的单调性。
type File struct {
	c    *GFSClient
	path string
	mu   sync.Mutex
}

// Close 关闭文件句柄(客户端无状态,此处仅作 API 完整性)。
func (f *File) Close() error {
	return nil
}

// Size 返回文件当前大小。
func (f *File) Size() (int64, error) {
	size, _, err := f.c.Stat(f.path)
	return size, err
}

// ReadAt 从指定偏移读取数据,返回实际读到的字节数。
// 数据直达 ChunkServer:按 chunk 边界切分,逐块读取(优先主副本,失败切换从副本)。
func (f *File) ReadAt(p []byte, off int64) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	size, _, err := f.c.Stat(f.path)
	if err != nil {
		return 0, err
	}
	if off >= size {
		return 0, nil // EOF
	}
	total := utils.MinI64(int64(len(p)), size-off)
	read := int64(0)
	chunkSize := f.c.cfg.ChunkSize
	for read < total {
		index := int((off + read) / chunkSize)
		chunkOff := (off + read) % chunkSize
		n := utils.MinI64(total-read, chunkSize-chunkOff, f.c.cfg.MaxRPCDataSize)

		handle, err := f.c.getChunkHandle(f.path, index)
		if err != nil {
			return int(read), err
		}
		loc, err := f.c.queryLocations(f.path, handle, index, false)
		if err != nil {
			return int(read), err
		}
		data, err := f.readFromReplicas(loc, chunkOff, n)
		if err != nil {
			return int(read), err
		}
		copy(p[read:read+int64(len(data))], data)
		read += int64(len(data))
		if int64(len(data)) < n {
			// 稀疏空洞或数据不足:剩余部分按零补齐,并推进游标
			// (调用方缓冲区本就是零,这里只推进位置,继续读后续 chunk)
			read += n - int64(len(data))
		}
	}
	return int(read), nil
}

// readFromReplicas 从主副本读取,失败依次尝试从副本;
// 全部失败则失效缓存并重查一次 Master,仍失败返回错误。
func (f *File) readFromReplicas(loc *CachedLocation, off, n int64) ([]byte, error) {
	handle := loc.Handle
	attempts := 0
	for attempts < 2 {
		for _, addr := range append([]string{loc.Primary}, loc.Secondaries...) {
			var reply types.ReadReply
			err := f.c.callServer(addr, "ReadChunk", &types.ReadArgs{Handle: handle, Offset: off, Length: n}, &reply)
			if err == nil {
				return reply.Data, nil
			}
		}
		// 全部失败:失效缓存,重新查询
		f.c.invalidateLocation(handle)
		newLoc, err := f.c.queryLocations(f.path, handle, 0, false)
		if err != nil {
			return nil, err
		}
		loc = newLoc
		attempts++
	}
	return nil, errors.New("client: all replicas failed to serve read")
}

// WriteAt 从指定偏移写入数据,返回写入字节数。
// 实现两阶段协议:
//  1. 数据推送:把数据推送到主副本,主副本沿流水线转发给所有从副本;
//  2. 控制阶段:向主副本发送 WriteChunk,主副本为所有副本分配相同偏移并应用;
//  3. 写失败时使缓存失效、重查 Master 后重试一次;
//  4. 成功后向 Master 上报文件大小。
func (f *File) WriteAt(p []byte, off int64) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	chunkSize := f.c.cfg.ChunkSize
	blockSize := utils.MinI64(f.c.cfg.MaxRPCDataSize, chunkSize)

	// 预取 chunk 数量,后续按需分配
	_, chunkCount, err := f.c.Stat(f.path)
	if err != nil {
		return 0, err
	}

	written := int64(0)
	cur := off
	for written < int64(len(p)) {
		index := int(cur / chunkSize)
		chunkOff := cur % chunkSize
		n := utils.MinI64(int64(len(p))-written, chunkSize-chunkOff, blockSize)

		// 确保 chunk 存在:稀疏写入时按序补齐中间缺失的 chunk(空 chunk)
		for index >= chunkCount {
			var ac types.ACReply
			if err := f.c.callMaster("AllocateChunk", &types.ACArgs{Path: f.path, ChunkIndex: chunkCount}, &ac); err != nil {
				return int(written), err
			}
			chunkCount++
		}

		handle, err := f.c.getChunkHandle(f.path, index)
		if err != nil {
			return int(written), err
		}
		block := p[written : written+n]

		if err := f.writeBlock(handle, block, chunkOff, index); err != nil {
			return int(written), err
		}

		// 上报文件大小(容忍乱序,Master 取最大值)
		newSize := cur + n
		var us types.UpdateSizeReply
		if err := f.c.callMaster("UpdateSize", &types.UpdateSizeArgs{Path: f.path, Size: newSize}, &us); err != nil {
			return int(written + n), err
		}

		written += n
		cur += n
	}
	return int(written), nil
}

// writeRetryInterval 与 writeRetryAttempts 控制写入失败后的重试节奏。
// 主副本故障时,Master 需要等待租约过期才能选出新主,重试窗口须覆盖该时段。
const (
	writeRetryInterval = 250 * time.Millisecond
	writeRetryAttempts = 10 // 10 x 250ms = 2.5s,覆盖默认测试租约(2s)
)

// writeBlock 完成单个数据块的推送与写入,失败时按退避重试。
// 每次重试都重新查询 Master(写模式不做缓存),保证主副本切换后立即生效。
// 注意:COW 后必须使用 GetLocations 返回的新 handle 进行推送与写入。
func (f *File) writeBlock(handle types.ChunkHandle, block []byte, chunkOff int64, index int) error {
	var lastErr error
	for attempt := 0; attempt < writeRetryAttempts; attempt++ {
		dataID := f.c.nextDataID()
		// 阶段一:推送数据(写位置永远实时查询 Master,不做缓存)
		loc, err := f.c.queryLocations(f.path, handle, index, true)
		if err != nil {
			lastErr = err
			time.Sleep(writeRetryInterval)
			continue
		}
		err = f.pushAndWrite(loc, loc.Handle, dataID, block, chunkOff)
		if err == nil {
			return nil
		}
		lastErr = err
		// 阶段二失败:使缓存失效,下一轮重新查询
		f.c.invalidateLocation(loc.Handle)
		time.Sleep(writeRetryInterval)
	}
	return fmt.Errorf("client: write retries exhausted (handle=%d): %w", handle, lastErr)
}

// pushAndWrite 执行一次两阶段写入:推送数据到主副本,再发控制请求。
func (f *File) pushAndWrite(loc *CachedLocation, handle types.ChunkHandle, dataID uint64, block []byte, chunkOff int64) error {
	// 阶段一:数据推送(主副本 -> 流水线 -> 从副本)
	err := f.c.callServer(loc.Primary, "PushData", &types.PushDataArgs{
		DataID:    dataID,
		Data:      block,
		Handle:    handle,
		ForwardTo: loc.Secondaries,
	}, &types.PushDataReply{})
	if err != nil {
		return err
	}
	// 阶段二:控制阶段(写入主副本,主副本转发从副本)
	var wr types.WriteReply
	return f.c.callServer(loc.Primary, "WriteChunk", &types.WriteArgs{
		Handle:      handle,
		DataID:      dataID,
		Offset:      chunkOff,
		Version:     loc.Version,
		Secondaries: loc.Secondaries,
	}, &wr)
}

// Append 原子追加一条记录,返回记录实际写入的偏移。
// 主副本为该记录选择偏移,保证所有副本在相同偏移写入相同数据;
// 若当前最后一个 chunk 已满,则分配新 chunk 后重试。
// appendRetryInterval 返回追加重试间隔:取租约时长的 1/4(上下限 100ms~1s)。
// 主副本故障时,需等待租约过期 Master 才能选出新主,重试间隔须覆盖该窗口。
func (f *File) appendRetryInterval() time.Duration {
	d := f.c.cfg.LeaseTimeout / 4
	if d > time.Second {
		d = time.Second
	}
	if d < 100*time.Millisecond {
		d = 100 * time.Millisecond
	}
	return d
}

// appendAttempts 追加最大尝试次数(8 x 500ms = 4s,覆盖测试租约 2s)。
const appendAttempts = 8

func (f *File) Append(p []byte) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(p) == 0 {
		size, err := f.Size()
		return size, err
	}
	if int64(len(p)) > f.c.cfg.ChunkSize {
		// 单条记录不能超过一个 chunk
		return 0, errors.New("client: append record larger than chunk size")
	}
	var lastErr error
	for attempt := 0; attempt < appendAttempts; attempt++ {
		_, chunkCount, err := f.c.Stat(f.path)
		if err != nil {
			return 0, err
		}
		if chunkCount == 0 {
			// 空文件:分配第一个 chunk
			var ac types.ACReply
			if err := f.c.callMaster("AllocateChunk", &types.ACArgs{Path: f.path, ChunkIndex: 0}, &ac); err != nil {
				return 0, err
			}
			chunkCount = 1
		}
		index := chunkCount - 1
		handle, err := f.c.getChunkHandle(f.path, index)
		if err != nil {
			return 0, err
		}
		dataID := f.c.nextDataID()
		loc, err := f.c.queryLocations(f.path, handle, index, true)
		if err != nil {
			return 0, err
		}
		err = f.c.callServer(loc.Primary, "PushData", &types.PushDataArgs{
			DataID:    dataID,
			Data:      p,
			Handle:    loc.Handle,
			ForwardTo: loc.Secondaries,
		}, &types.PushDataReply{})
		if err != nil {
			f.c.invalidateLocation(loc.Handle)
			lastErr = err
			time.Sleep(f.appendRetryInterval())
			continue
		}
		var wr types.WriteReply
		err = f.c.callServer(loc.Primary, "WriteChunk", &types.WriteArgs{
			Handle:      loc.Handle,
			DataID:      dataID,
			Version:     loc.Version,
			Secondaries: loc.Secondaries,
			Append:      true,
		}, &wr)
		if err != nil {
			if errors.Is(err, types.ErrChunkFull) {
				// 最后一个 chunk 已满:分配新 chunk 后立即重试
				var ac types.ACReply
				if aerr := f.c.callMaster("AllocateChunk", &types.ACArgs{Path: f.path, ChunkIndex: chunkCount}, &ac); aerr != nil {
					return 0, aerr
				}
				continue
			}
			f.c.invalidateLocation(loc.Handle)
			lastErr = err
			time.Sleep(f.appendRetryInterval())
			continue
		}
		// 上报文件大小:追加永远扩展文件。追加落在 chunk index 处,
		// 由于对齐不变量(前面 chunk 均整块/零填充),文件末尾 =
		// index*chunkSize + wr.Offset + 记录长度。
		var us types.UpdateSizeReply
		newSize := int64(index)*f.c.cfg.ChunkSize + wr.Offset + int64(len(p))
		if err := f.c.callMaster("UpdateSize", &types.UpdateSizeArgs{Path: f.path, Size: newSize}, &us); err != nil {
			return wr.Offset, err
		}
		return wr.Offset, nil
	}
	return 0, fmt.Errorf("client: append retries exhausted (path=%s len=%d): %w", f.path, len(p), lastErr)
}
