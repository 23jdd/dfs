# GFS — 简化版 Google File System(Go 实现)

一个可运行的 GFS 原型,包含 **Master / ChunkServer / Client** 三个角色,支持文件创建、读写、原子追加、快照(写时复制)、垃圾回收、故障恢复等核心机制。

RPC 通信基于 [`github.com/23jdd/mrpc`](https://github.com/23jdd/mrpc) 库,元数据使用 **checkpoint + 操作日志(op log)** 持久化,Chunk 数据存放在本地文件系统(每个 chunk 一个文件)。

## 目录结构

```
.
├── cmd/
│   ├── master/        # Master 入口
│   ├── chunkserver/   # ChunkServer 入口
│   └── client/        # 示例客户端(写入-读取-MD5 校验)
├── internal/
│   ├── master/        # Master:命名空间/租约/持久化/GC/副本再复制
│   ├── chunkserver/   # ChunkServer:数据存储/流水线/追加/复制
│   ├── client/        # Client 库:POSIX 风格 API + 位置缓存
│   ├── rpc/           # mrpc 适配封装(net/rpc 风格接口)
│   ├── types/         # 共享数据结构、配置、RPC 请求/响应类型
│   └── utils/         # 校验和、路径处理等工具
├── go.mod
└── README.md
```

## 快速开始

### 1. 启动 Master

```bash
go run ./cmd/master -addr :8888 -dir ./master_data -replication 3
```

### 2. 启动 ChunkServer(3 台,分属不同机架)

```bash
go run ./cmd/chunkserver -addr :8901 -master :8888 -dir ./cs_data1 -rack rack0
go run ./cmd/chunkserver -addr :8902 -master :8888 -dir ./cs_data2 -rack rack1
go run ./cmd/chunkserver -addr :8903 -master :8888 -dir ./cs_data3 -rack rack2
```

> 注意:Master 与 ChunkServer 的 `-chunk-size-mb` 必须一致;`-master` 指向 Master 的**实际监听地址**(`-addr :0` 时以启动日志打印的地址为准)。

### 3. 运行示例客户端(写入 16MB 文件并回读校验)

```bash
go run ./cmd/client -master :8888 -path /demo.bin -size-mb 16
```

## 运行测试

```bash
# 全部集成测试(进程内起 1 Master + N ChunkServer)
go test ./internal/client/ -count=1

# 单测指定场景
go test ./internal/client/ -run TestBasicReadWrite      # 基础读写(100MB)
go test ./internal/client/ -run TestConcurrentAppend    # 并发原子追加(10 goroutine)
go test ./internal/client/ -run TestChunkServerFailure  # ChunkServer 故障与副本再复制
go test ./internal/client/ -run TestMasterRestart       # Master 重启(心跳重建位置)
go test ./internal/client/ -run TestSnapshot            # 快照(写时复制)

# 竞态检测
go test ./internal/client/ -count=1 -race
```

## 架构设计

### 角色

| 角色 | 职责 |
|------|------|
| **Master** | 命名空间、文件→chunk 映射、chunk 位置(心跳重建)、租约、chunk 版本号、GC、副本再复制 |
| **ChunkServer** | 存储 chunk 数据(默认 64MB/块,每块一个文件 + CRC32 边车),处理读写,每 500ms 心跳 |
| **Client** | POSIX 风格 API,缓存 chunk 位置(60s TTL),数据读写直达 ChunkServer |

### 两阶段写协议

1. **数据推送(Push)**:客户端生成全局唯一 `DataID`,把数据推给主副本,主副本沿流水线转发给所有从副本;
2. **控制(Write)**:客户端向主副本发送 `WriteChunk(DataID, Offset, Version)`,主副本为所有副本分配**相同偏移**、本地应用后并行通知从副本 `ApplyMutation`;
3. 任一步失败:客户端使缓存失效、重新查询 Master、重试一次(同偏移同数据,幂等)。

### 原子记录追加(Record Append)

- 客户端向最后一个 chunk 的主副本请求追加;偏移由主副本根据 chunk 当前长度决定;
- 主副本上的每 chunk 锁串行化并发追加,保证记录**不重叠、不撕裂**;
- 最后一个 chunk 已满时返回 `ErrChunkFull`,客户端分配新 chunk 后重试。

### 租约(Lease)

- Master 为每个 chunk 选择主副本并授予 60s 租约;主副本通过心跳自动续期;
- 主副本故障时,Master 等待租约过期后选择新主副本并**提升 chunk 版本号**;
- 版本号使陈旧副本(stale replica)失效,心跳中版本号低于记录的副本不会被返回给客户端。

### 心跳与元数据重建

- ChunkServer 每 500ms 上报自身地址/机架/磁盘用量/全部 chunk 及版本;
- `chunkLocations` **不持久化**,Master 重启后由心跳重建;
- 心跳版本比对:低版本 → 标记陈旧;高版本(Master 重启落后)→ 采纳并记日志。

### 副本管理

- 默认复制因子 3;机架感知放置(至少 2 个机架);
- Master 后台每 5s 扫描:存活副本数 < 因子时,从现存副本(优先非主副本)复制到新服务器;只剩 1 个副本的 chunk 优先。

### 垃圾回收(GC)

- 删除文件 = 移入 `/.deleted/` 隐藏目录并记录时间(惰性删除,不阻塞、可恢复);
- 后台每 30s:清除超过 `GCDelay`(默认 3 分钟)的隐藏文件 → 释放 chunk 引用 → 扫描无引用的孤儿 chunk,通知持有者删除数据。

### 快照(Snapshot,写时复制)

- 快照请求:新文件共享所有 chunk 并增加引用计数(`SnapRefCount`/`RefCount`);
- 对共享 chunk 的写入:Master 同步复制出新 chunk(源端读旧 handle,目标写新 handle),新副本成为可写版本,旧副本保持只读;
- 客户端始终使用 `GetLocations` 返回的**生效 handle** 执行写入。

### 持久化(checkpoint + op log)

- 所有元数据先存内存;每次变更追加一条 CRC32 校验的操作日志(长度前缀帧 + fsync);
- 每 1024 条操作(或优雅退出时)保存 checkpoint 快照并清空日志;
- 启动时:加载 checkpoint → 回放 op log(幂等),chunk 位置由心跳重建。

## 配置项

见 `internal/types/config.go`,关键项:

| 配置 | 默认值 | 说明 |
|------|--------|------|
| `ChunkSize` | 64MB | chunk 大小 |
| `ReplicationFactor` | 3 | 副本数 |
| `LeaseTimeout` | 60s | 租约时长 |
| `HeartbeatInterval` | 500ms | 心跳间隔 |
| `GCDelay` | 3m | 删除到 GC 清理的延迟 |
| `CacheTimeout` | 60s | 客户端位置缓存 TTL |
| `ChecksumBlockSize` | 64KB | 每块一个 CRC32 |
| `MaxRPCDataSize` | 4MB | 单次 RPC 数据上限(mrpc 协议层限 10MB,留编码余量) |

## RPC 服务

| 服务 | 方法 |
|------|------|
| `MasterService` | Create / Delete / Rename / GetChunkHandle / GetLocations / AllocateChunk / Heartbeat / ReportChunk / Snapshot / Stat / UpdateSize |
| `ChunkServerService` | PushData / WriteChunk / ReadChunk / CopyChunk / ApplyMutation / CreateChunk / DeleteChunk |

## 客户端 API 示例

```go
c := client.New("127.0.0.1:8888", types.DefaultConfig())
defer c.Close()

c.Create("/hello.txt")
f, _ := c.Open("/hello.txt")

f.WriteAt([]byte("hello "), 0)          // 按偏移写
off, _ := f.Append([]byte("world\n"))   // 原子追加,返回偏移
data, _ := c.ReadFile("/hello.txt")     // 读整文件

c.Snapshot("/hello.txt", "/hello.snap") // 快照
c.Delete("/hello.txt")                  // 惰性删除
```

## 实现说明与已知简化

- 单 Master(无主备切换),租约与元数据重建聚焦单点恢复;
- `mrpc` 的 `Call` 非并发安全,`internal/rpc` 用互斥锁串行化,并提供 `Go` 异步调用;
- 副本再复制期间对同一 chunk 的写入与复制存在理论上的读撕裂窗口(原型简化);
- 追加记录必须小于一个 chunk;`MaxRPCDataSize` 受 mrpc 10MB 单帧上限约束;
- Windows 平台注意事项:`op.log` 不能以 `O_APPEND` 打开(Truncate 权限不足),实现中采用 seek-to-end 写入。
