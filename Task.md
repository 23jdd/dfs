
# Role
你是一位精通分布式系统与 Go 语言的高级工程师。你的任务是用 Go 实现一个简化但完整的 Google File System (GFS)，RPC 通信必须使用 `github.com/23jdd/mrpc` 库。
# notice
如果现有代码不符合要求可以直接delete
# 目标
实现一个可运行的 GFS 原型，包含 Master、ChunkServer、Client 三个角色，支持文件创建、读取、写入、追加、快照、垃圾回收、故障恢复等核心机制。
# git commit
git commit 应该做到细分,尽可能多
# 技术约束
- 语言：Go 1.22+
- RPC 库：`github.com/23jdd/mrpc`（假设 API 兼容 `net/rpc` 风格：Register / Dial / Call / Go / ServeConn / Accept）
- 序列化：使用 `encoding/gob` 或 `mrpc` 内置的 codec
- 不允许使用外部分布式框架（如 etcd、Raft、gRPC、Protobuf）
- 所有元数据先存内存，使用 checkpoint + operation log 持久化
- Chunk 数据存储在本地文件系统（每个 chunk 一个独立文件）

# 架构设计

## 1. 整体架构
- **Master（单点）**：管理所有元数据，包括命名空间、文件到 chunk 的映射、chunk 位置、租约、chunk 版本号。
- **ChunkServer（多节点）**：存储实际 chunk 数据（默认 64MB/块），处理客户端的读写请求，定期向 Master 发送心跳。
- **Client（库形式）**：提供类似 POSIX 的文件操作 API，缓存 chunk 位置，直接与 ChunkServer 交互读写数据。

## 2. 核心数据结构

### Master
```go
type Master struct {
    mu sync.RWMutex
    
    // 命名空间：文件路径 -> FileMetadata
    namespace map[string]*FileMetadata
    
    // Chunk 元数据：ChunkHandle -> ChunkMetadata
    chunkMeta map[ChunkHandle]*ChunkMetadata
    
    // Chunk 位置信息：ChunkHandle -> set<ChunkServerAddr>
    // 注意：不持久化，通过心跳重建
    chunkLocations map[ChunkHandle]map[string]bool
    
    // 租约：ChunkHandle -> LeaseInfo（主副本 + 过期时间）
    leases map[ChunkHandle]*LeaseInfo
    
    // 已注册的 ChunkServer：addr -> ChunkServerState
    chunkServers map[string]*ChunkServerState
    
    // 操作日志（持久化）
    opLog []OperationLogEntry
    
    nextHandle uint64 // 自增 chunk handle 分配器
    config *Config
}

type FileMetadata struct {
    Path      string
    Chunks    []ChunkHandle // 按 chunk index 顺序
    IsDeleted bool
    DeletedAt time.Time
    // 支持快照：引用计数
    SnapRefCount int
}

type ChunkMetadata struct {
    Handle  ChunkHandle
    Version ChunkVersion
    // 其他元数据
}

type LeaseInfo struct {
    Primary    string    // 主 ChunkServer 地址
    ExpiresAt  time.Time // 租约过期时间（默认 60s）
    Version    ChunkVersion
}

type ChunkServerState struct {
    Address   string
    Rack      string // 机架感知
    DiskUsage int64
    LastHeartbeat time.Time
    Chunks    map[ChunkHandle]bool // 该 server 持有的 chunks
}
type OpLogEntry struct {
    Timestamp int64       // 时间戳，用于回放时排序
    OpType    OpType      // CREATE / DELETE / RENAME / ALLOCATE / UPDATE_VERSION / GRANT_LEASE 等
    Payload   []byte      // gob/json 编码的操作详情
    Checksum  uint32      // CRC32，防止日志文件损坏
}

type OpType int
const (
    OpCreate OpType = iota
    OpDelete
    OpRename
    OpAllocateChunk
    OpUpdateVersion
    OpGrantLease
    OpSnapshotRef
)
```

### ChunkServer
```go
type ChunkServer struct {
    addr string
    masterAddr string
    
    // 本地存储的 chunks：handle -> 本地文件路径
    chunks map[ChunkHandle]string
    
    // 数据推送缓冲区（用于流水线）：dataID -> []byte
    dataBuffer map[uint64][]byte
    
    config *Config
}
```

### Client
```go
type GFSClient struct {
    masterAddr string
    
    // Chunk 位置缓存：handle -> CachedLocation（带 TTL）
    locationCache map[ChunkHandle]*CachedLocation
    cacheMu sync.RWMutex
    cacheTimeout time.Duration
    
    dataIDCounter uint64 // 用于数据推送的 ID
}

type CachedLocation struct {
    Primary     string
    Secondaries []string
    Version     ChunkVersion
    CachedAt    time.Time
}
```

## 3. 必须实现的 RPC 接口

使用 `mrpc` 注册以下服务：

### Master 提供的服务（`MasterService`）
| 方法 | 请求 | 响应 | 说明 |
|------|------|------|------|
| `Create` | `CreateArgs{Path}` | `CreateReply{Handle}` | 创建文件，分配第一个 chunk |
| `Delete` | `DeleteArgs{Path}` | `DeleteReply{}` | 惰性删除（移到隐藏目录） |
| `Rename` | `RenameArgs{Src, Dst}` | `RenameReply{}` | 重命名，需防死锁（按字典序加锁） |
| `GetChunkHandle` | `GCHArgs{Path, ChunkIndex}` | `GCHReply{Handle}` | 获取指定 chunk handle |
| `GetLocations` | `GLArgs{Handle}` | `GLReply{Primary, Secondaries, Version}` | 获取 chunk 位置 + 租约信息 |
| `AllocateChunk` | `ACArgs{Path, ChunkIndex}` | `ACReply{Handle, Primary, Secondaries}` | 为新 chunk 分配 handle 和副本 |
| `Heartbeat` | `HBArgs{Addr, Rack, DiskUsage, Chunks[]}` | `HBReply{ExpiredLeases[]}` | ChunkServer 心跳 |
| `ReportChunk` | `RCArgs{Handle, Version}` | `RCReply{}` | ChunkServer 报告自己持有的 chunk |

### ChunkServer 提供的服务（`ChunkServerService`）
| 方法 | 请求 | 响应 | 说明 |
|------|------|------|------|
| `PushData` | `PushDataArgs{DataID, Data, ForwardTo[]}` | `PushDataReply{}` | 接收数据并流水线转发 |
| `WriteChunk` | `WriteArgs{Handle, DataID, Offset, Version}` | `WriteReply{}` | 主副本执行写入，转发到从副本 |
| `ReadChunk` | `ReadArgs{Handle, Offset, Length}` | `ReadReply{Data}` | 读取 chunk 数据 |
| `CopyChunk` | `CopyArgs{Handle, SourceServer}` | `CopyReply{}` | 从其他 server 复制 chunk |
| `ApplyMutation` | `ApplyArgs{Handle, DataID, Offset}` | `ApplyReply{}` | 从副本应用变更 |
| `DeleteChunk` | `DeleteArgs{Handle}` | `DeleteReply{}` | 删除本地 chunk 文件 |

## 4. 关键机制详细说明

### 4.1 命名空间锁（Namespace Locking）
- 使用**路径级读写锁**（`map[string]*sync.RWMutex`），而非全局锁。
- 对 `/data/logs/access.log` 操作：
  - 读锁 `/data`
  - 读锁 `/data/logs`
  - 写锁 `/data/logs/access.log`（仅对叶子节点写锁）
- 重命名时按**字典序**锁定源路径和目标路径，防止死锁。

### 4.2 写操作流程（两阶段协议）
1. **客户端向 Master 请求**：获取 chunk 的 primary 和 secondaries。
2. **数据推送阶段（Push Phase）**：
   - 客户端生成 `DataID`，将数据推送到最近的 ChunkServer。
   - 该 ChunkServer 通过流水线转发给下一个，直到所有副本都缓存了数据。
3. **控制阶段（Write Phase）**：
   - 客户端向 **Primary** 发送 `WriteChunk` 请求（携带 DataID、Offset、Version）。
   - Primary 为所有副本分配**相同的序列号/偏移**，先本地应用，再并行转发给所有 Secondaries。
   - Secondaries 完成后回复 Primary。
   - Primary 回复客户端。
4. 如果任何一步失败，客户端使缓存失效，重新向 Master 查询。

### 4.3 原子记录追加（Atomic Record Append）
- 客户端调用 `Append(path, data)`。
- Master 返回该文件**最后一个 chunk** 的 primary（如果该 chunk 已满，则分配新 chunk）。
- 数据推送流程与普通写相同。
- Primary 检查该 chunk 剩余空间是否足够：
  - 足够：选择一个偏移，保证所有副本在**相同偏移**写入相同数据。
  - 不足：返回错误，客户端重试（Master 会分配新 chunk）。
- 返回追加成功的偏移给客户端。

### 4.4 租约（Lease）机制
- Master 为每个 chunk 的副本集选择一个 **Primary**，授予 60 秒租约。
- Primary 负责序列化该 chunk 的所有变更（写/追加）。
- 租约可通过心跳续期（心跳回复中携带 `ExpiredLeases`，Primary 可请求续期）。
- 如果 Primary 故障，Master 等待租约过期后，选择新 Primary（提升版本号）。

### 4.5 心跳与元数据重建
- ChunkServer 每 500ms 向 Master 发送心跳，报告：
  - 自身地址、机架、磁盘使用量
  - 持有的所有 chunk handle 及其版本号
- Master 根据心跳**重建 `chunkLocations`**（不持久化）。
- Master 对比心跳中的版本号与自身记录：
  - 如果心跳版本号 < Master 记录，说明该副本是**陈旧的（stale）**，标记为无效。
  - 如果心跳版本号 > Master 记录，说明 Master 重启后数据落后，更新版本号。

### 4.6 副本管理
- **默认复制因子**：3。
- **机架感知放置**：选择 ChunkServer 时，优先分散到不同机架（至少 2 个不同机架）。
- **副本再复制（Re-replication）**：Master 后台 goroutine 定期检查：
  - 如果某 chunk 的存活副本数 < 复制因子，选择源副本和目标 ChunkServer，发起 `CopyChunk`。
  - 优先处理只剩 1 个副本的 chunk。

### 4.7 垃圾回收（GC）
- 删除文件时，Master 将其重命名到 `/.deleted/` 命名空间，并记录删除时间。
- 后台 GC goroutine 每 30s 运行一次：
  - Phase 1：删除 `/.deleted/` 中超过 `GCDelay`（如 3 分钟）的文件，释放其 chunk 引用。
  - Phase 2：扫描所有 `chunkMeta`，找出没有任何文件引用的 orphan chunk，向持有它的 ChunkServer 发送 `DeleteChunk`。
- 好处：避免删除操作阻塞，允许误删恢复。

### 4.8 快照（Snapshot）
- 使用**写时复制（Copy-on-Write）**。
- 快照请求时，Master 对目标文件的 chunks 增加引用计数（`SnapRefCount`）。
- 当某个 chunk 需要被修改且 `SnapRefCount > 0` 时：
  - Master 分配新的 chunk handle。
  - 选择源副本，向目标 ChunkServer 发送 `CopyChunk` 创建新副本。
  - 新副本成为可写版本，旧副本保持只读。

### 4.9 客户端缓存
- 客户端缓存 chunk 位置（primary + secondaries + version），TTL 60 秒。
- 读/写失败时（如连接超时、版本不匹配），使缓存失效，重新查询 Master。
- 读取时直接与 ChunkServer 交互，不经过 Master。

## 5. 代码组织要求

```
gfs/
├── cmd/
│   ├── master/         # Master 入口
│   ├── chunkserver/    # ChunkServer 入口
│   └── client/         # 示例客户端程序
├── internal/
│   ├── master/         # Master 核心逻辑
│   ├── chunkserver/    # ChunkServer 核心逻辑
│   ├── client/         # Client 库
│   ├── rpc/            # mrpc 服务注册与类型定义
│   ├── types/          # 共享数据结构（ChunkHandle、Config 等）
│   └── utils/          # 工具函数（checksum、路径处理等）
├── go.mod
└── README.md
```

## 6. 使用 mrpc 的具体要求

- 所有 RPC 服务通过 `mrpc.Register(service)` 注册。
- 服务端使用 `mrpc.Accept(listener)` 或 `for { conn, _ := listener.Accept(); go mrpc.ServeConn(conn) }` 处理连接。
- 客户端使用 `mrpc.Dial("tcp", addr)` 建立连接，通过 `client.Call("Service.Method", args, &reply)` 同步调用。
- 对于心跳、后台复制等异步操作，使用 `client.Go("Service.Method", args, &reply, doneChan)`。
- 如果 `mrpc` 支持自定义 codec，使用 gob codec；否则使用默认 codec。
- 所有 RPC 参数和返回值必须定义为**导出的 struct 类型**，字段也必须导出。

## 7. 配置项（Config）

```go
type Config struct {
    MasterAddr        string        // ":8888"
    ChunkSize         int64         // 64 * 1024 * 1024 (64MB)
    ReplicationFactor int           // 3
    LeaseTimeout      time.Duration // 60s
    HeartbeatInterval time.Duration // 500ms
    GCDelay           time.Duration // 3m
    CacheTimeout      time.Duration // 60s
    ChecksumBlockSize int           // 64KB (CRC32 per block)
}
```

## 8. 测试场景要求
实现完成后，必须能通过以下测试：
1. **基础读写**：创建文件、写入 100MB 数据、读取并校验 MD5。
2. **并发追加**：10 个 goroutine 同时向同一文件追加记录，验证每条记录完整且不重叠。
3. **ChunkServer 故障**：写入过程中 kill 一个 ChunkServer，验证系统继续工作，且 Master 最终重新复制缺失副本。
4. **Master 重启**：重启 Master，验证通过心跳重建所有 chunk 位置信息，读写正常。
5. **快照**：对文件创建快照，修改原文件，验证快照内容不变。

## 9. 输出要求
- 提供完整的、可编译的 Go 代码。
- 每个关键函数必须有中文注释说明设计意图。
- 在 README 中说明如何启动 Master、ChunkServer 和运行测试。

请开始实现。


