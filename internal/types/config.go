package types

import "time"

// Config 是 GFS 全局配置,Master / ChunkServer / Client 启动时使用同一份默认值。
type Config struct {
	MasterAddr string // Master 监听地址,如 ":8888"

	ChunkSize         int64         // 单个 chunk 大小,默认 64MB
	ReplicationFactor int           // 副本数,默认 3
	LeaseTimeout      time.Duration // 租约时长,默认 60s
	HeartbeatInterval time.Duration // ChunkServer 心跳间隔,默认 500ms
	HeartbeatTimeout  time.Duration // Master 判定服务器死亡的超时,默认 3s
	GCDelay           time.Duration // 文件删除后到 GC 清理的延迟,默认 3m
	CacheTimeout      time.Duration // 客户端 chunk 位置缓存 TTL,默认 60s
	ChecksumBlockSize int           // checksum 块大小,默认 64KB(每块一个 CRC32)

	RecheckInterval time.Duration // 副本再复制检查周期,默认 5s
	GCInterval      time.Duration // 垃圾回收周期,默认 30s

	// MaxRPCDataSize 单次 RPC 传输的数据上限。
	// mrpc 协议层限制单帧 10MB,这里留出编码余量,默认 4MB。
	MaxRPCDataSize int64

	// MaxBufferAge 数据推送缓冲区中数据的最长缓存时间,默认 60s。
	MaxBufferAge time.Duration
}

// DefaultConfig 返回 GFS 默认配置。
func DefaultConfig() *Config {
	return &Config{
		MasterAddr:        ":8888",
		ChunkSize:         64 * 1024 * 1024,
		ReplicationFactor: 3,
		LeaseTimeout:      60 * time.Second,
		HeartbeatInterval: 500 * time.Millisecond,
		HeartbeatTimeout:  3 * time.Second,
		GCDelay:           3 * time.Minute,
		CacheTimeout:      60 * time.Second,
		ChecksumBlockSize: 64 * 1024,
		RecheckInterval:   5 * time.Second,
		GCInterval:        30 * time.Second,
		MaxRPCDataSize:    4 * 1024 * 1024,
		MaxBufferAge:      60 * time.Second,
	}
}
