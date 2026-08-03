// Package types 定义 GFS 中跨模块(主节点/块服务器/客户端)共享的数据结构与配置。
package types

import (
	"errors"
	"time"
)

// ChunkHandle 是全局唯一的 chunk 编号,由 Master 自增分配。
type ChunkHandle uint64

// ChunkVersion 是 chunk 的版本号,用于检测陈旧副本(stale replica)。
type ChunkVersion uint64

// OpType 是操作日志(op log)中记录的操作类型,用于崩溃恢复后的回放。
type OpType int

const (
	// OpCreate 创建文件。
	OpCreate OpType = iota
	// OpDelete 惰性删除(移入 /.deleted/)或 GC 硬删除。
	OpDelete
	// OpRename 重命名。
	OpRename
	// OpAllocateChunk 分配新 chunk。
	OpAllocateChunk
	// OpUpdateVersion chunk 版本号提升(主副本切换/主节点落后时)。
	OpUpdateVersion
	// OpGrantLease 授予租约(租约本身不持久化,回放时忽略)。
	OpGrantLease
	// OpSnapshotRef 快照引用调整(共享 chunk / 写时复制交换)。
	OpSnapshotRef
	// OpDeleteChunk 删除无引用的孤儿 chunk 元数据。
	OpDeleteChunk
	// OpUpdateSize 更新文件逻辑大小。
	OpUpdateSize
	// OpSnapshot 创建快照文件。
	OpSnapshot
)

// 服务名常量,mrpc 调用使用 "ServiceName.MethodName" 格式。
const (
	MasterService      = "MasterService"
	ChunkServerService = "ChunkServerService"
)

// DeletedPrefix 是垃圾回收隐藏目录:被删除的文件先移到这里,延迟 GCDelay 后被清除。
const DeletedPrefix = "/.deleted/"

// 常见错误,各模块通过 errors.Is 判断。
var (
	ErrFileExists      = errors.New("file already exists")
	ErrFileNotFound    = errors.New("file not found")
	ErrChunkNotFound   = errors.New("chunk not found")
	ErrIndexOutOfRange = errors.New("chunk index out of range")
	ErrChunkFull       = errors.New("chunk is full")
	ErrStaleVersion    = errors.New("stale chunk version")
	ErrDataNotFound    = errors.New("pushed data not found")
	ErrChecksum        = errors.New("checksum mismatch")
	ErrNoServer        = errors.New("no live chunkserver")
	ErrInvalidPath     = errors.New("invalid path")
	ErrReadOnly        = errors.New("file is read-only snapshot reference")
)

// errorByMessage 是错误消息到哨兵错误的映射。
// mrpc 通过字符串传输错误,收到的一方无法直接用 errors.Is 匹配原哨兵,
// 需要借助 MatchError 还原。
var errorByMessage = func() map[string]error {
	m := map[string]error{
		ErrFileExists.Error():      ErrFileExists,
		ErrFileNotFound.Error():    ErrFileNotFound,
		ErrChunkNotFound.Error():   ErrChunkNotFound,
		ErrIndexOutOfRange.Error(): ErrIndexOutOfRange,
		ErrChunkFull.Error():       ErrChunkFull,
		ErrStaleVersion.Error():    ErrStaleVersion,
		ErrDataNotFound.Error():    ErrDataNotFound,
		ErrChecksum.Error():        ErrChecksum,
		ErrNoServer.Error():        ErrNoServer,
		ErrInvalidPath.Error():     ErrInvalidPath,
		ErrReadOnly.Error():        ErrReadOnly,
	}
	return m
}()

// MatchError 将 RPC 传回的字符串错误还原为哨兵错误,
// 使 errors.Is(err, ErrChunkFull) 之类的判断在跨进程后依然成立;
// 未知错误原样返回。
func MatchError(err error) error {
	if err == nil {
		return nil
	}
	if sentinel, ok := errorByMessage[err.Error()]; ok {
		return sentinel
	}
	return err
}

// FileMetadata 是命名空间中一个文件的元数据。
type FileMetadata struct {
	Path   string
	Chunks []ChunkHandle // 按 chunk index 顺序排列
	Size   int64         // 文件逻辑大小(字节),由客户端写成功后上报

	IsDeleted bool      // 是否已被惰性删除
	DeletedAt time.Time // 删除时间,GC 据此判断何时可清除

	// SnapRefCount 表示与本文件共享 chunk 的快照副本数(仅信息性),
	// 写时复制判定以 ChunkMetadata.RefCount 为准。
	SnapRefCount int
}

// ChunkMetadata 是 Master 记录的 chunk 元数据。
type ChunkMetadata struct {
	Handle  ChunkHandle
	Version ChunkVersion

	// RefCount 是引用该 chunk 的文件数(>=1 表示有文件使用,0 表示孤儿,等待 GC)。
	RefCount int
}

// LeaseInfo 是 Master 授予的租约信息:主副本负责串行化该 chunk 的所有变更。
type LeaseInfo struct {
	Primary   string       // 主 ChunkServer 地址
	ExpiresAt time.Time    // 租约过期时间
	Version   ChunkVersion // 授予租约时的 chunk 版本号
}

// ChunkServerState 是 Master 记录的一个 ChunkServer 的存活状态。
type ChunkServerState struct {
	Address       string
	Rack          string
	DiskUsage     int64
	LastHeartbeat time.Time
	Chunks        map[ChunkHandle]bool // 该服务器持有的 chunks(由心跳报告)
}

// OperationLogEntry 是一条操作日志记录,追加写入 op.log 文件。
type OperationLogEntry struct {
	Timestamp int64  // 时间戳(UnixNano),回放时用于排序
	OpType    OpType // 操作类型
	Payload   []byte // gob 编码的操作详情
	Checksum  uint32 // CRC32(OpType+Payload),检测日志文件损坏
}

// ---- 操作日志的载荷(Payload 的 gob 编码对象) ----

type CreatePayload struct {
	Path string
}

// DeletePayload 中 HiddenPath 非空表示惰性删除(移入 /.deleted/),
// 为空表示 GC 硬删除(直接移除文件条目)。
type DeletePayload struct {
	Path       string
	HiddenPath string
}

type RenamePayload struct {
	Src string
	Dst string
}

type AllocateChunkPayload struct {
	Path    string
	Index   int
	Handle  ChunkHandle
	Version ChunkVersion
}

type UpdateVersionPayload struct {
	Handle  ChunkHandle
	Version ChunkVersion
}

type GrantLeasePayload struct {
	Handle    ChunkHandle
	Primary   string
	ExpiresAt time.Time
	Version   ChunkVersion
}

// SnapshotRefPayload 描述一次快照引用调整。
type SnapshotRefPayload struct {
	Path      string
	Index     int // -1 表示不涉及 chunk 交换
	OldHandle ChunkHandle
	NewHandle ChunkHandle
}

type DeleteChunkPayload struct {
	Handle ChunkHandle
}

type UpdateSizePayload struct {
	Path string
	Size int64
}

type SnapshotPayload struct {
	Path     string
	SnapPath string
}

// ---- Master 提供的 RPC 请求/响应类型 ----

type CreateArgs struct {
	Path string
}

type CreateReply struct {
	Handle ChunkHandle // 第一个 chunk 的 handle
}

type DeleteArgs struct {
	Path string
}

type DeleteReply struct{}

type RenameArgs struct {
	Src string
	Dst string
}

type RenameReply struct{}

type GCHArgs struct {
	Path       string
	ChunkIndex int
}

type GCHReply struct {
	Handle ChunkHandle
}

// GLArgs 请求获取 chunk 位置;ForWrite 为 true 时,Master 会先执行
// 写时复制(COW)检查,Path/ChunkIndex 用于定位引用文件。
type GLArgs struct {
	Handle     ChunkHandle
	ForWrite   bool
	Path       string
	ChunkIndex int
}

type GLReply struct {
	Handle      ChunkHandle // COW 后可能返回新 handle
	Primary     string
	Secondaries []string
	Version     ChunkVersion
}

type ACArgs struct {
	Path       string
	ChunkIndex int
}

type ACReply struct {
	Handle      ChunkHandle
	Primary     string
	Secondaries []string
	Version     ChunkVersion
}

type ChunkReport struct {
	Handle  ChunkHandle
	Version ChunkVersion
}

type HBArgs struct {
	Addr      string
	Rack      string
	DiskUsage int64
	Chunks    []ChunkReport
}

// ExpiredLease 告知 ChunkServer:其持有的指定 chunk 租约已失效或版本已变化。
type ExpiredLease struct {
	Handle  ChunkHandle
	Version ChunkVersion
}

type HBReply struct {
	ExpiredLeases []ExpiredLease
}

type RCArgs struct {
	Addr    string
	Handle  ChunkHandle
	Version ChunkVersion
}

type RCReply struct{}

type SnapshotArgs struct {
	Path     string
	SnapPath string
}

type SnapshotReply struct{}

type StatArgs struct {
	Path string
}

type StatReply struct {
	Size       int64
	ChunkCount int
}

type UpdateSizeArgs struct {
	Path string
	Size int64
}

type UpdateSizeReply struct{}

// ---- ChunkServer 提供的 RPC 请求/响应类型 ----

// PushDataArgs 是数据推送阶段(第一阶段)的参数:
// 数据先沿 ForwardTo 链流水线转发,直到所有副本都缓存了数据。
type PushDataArgs struct {
	DataID    uint64
	Data      []byte
	Handle    ChunkHandle // 数据所属 chunk,用于租约失效时的缓冲清理
	ForwardTo []string    // 流水线下一跳
}

type PushDataReply struct{}

// WriteArgs 是控制阶段(第二阶段)的参数,由客户端发给主副本。
type WriteArgs struct {
	Handle      ChunkHandle
	DataID      uint64
	Offset      int64
	Version     ChunkVersion
	Secondaries []string // 主副本需要通知的从副本
	Append      bool     // 原子记录追加:偏移由主副本决定
}

type WriteReply struct {
	Offset int64 // Append 模式下返回实际写入偏移
}

type ReadArgs struct {
	Handle ChunkHandle
	Offset int64
	Length int64
}

type ReadReply struct {
	Data []byte
}

// CopyArgs 从源服务器复制一个 chunk 到本服务器。
// SourceHandle 是源服务器上需要读取的 chunk(写时复制时源为旧 handle,
// 目标为新 handle;副本再复制时二者相同)。
type CopyArgs struct {
	Handle       ChunkHandle
	Version      ChunkVersion
	SourceServer string
	SourceHandle ChunkHandle
}

type CopyReply struct{}

// ApplyArgs 是主副本转发给从副本的应用变更请求。
type ApplyArgs struct {
	Handle  ChunkHandle
	DataID  uint64
	Offset  int64
	Version ChunkVersion
}

type ApplyReply struct{}

// CreateChunkArgs 由 Master 在 AllocateChunk 时调用,让目标服务器
// 预创建空 chunk 文件,保证 Master 重启后仍能通过心跳重建位置信息。
type CreateChunkArgs struct {
	Handle  ChunkHandle
	Version ChunkVersion
}

type CreateChunkReply struct{}

// DeleteChunkArgs 请求删除本服务器上的一个 chunk 文件。
type DeleteChunkArgs struct {
	Handle ChunkHandle
}

type DeleteChunkReply struct{}
