package dfs

type FileSystem interface {
	CreateFile(path string) error
	DeleteFile(path string) error
	GetFileInfo(path string) (*FileMeta, error)

	// 获取 Chunk 位置（读/写前调用）
	GetChunkLocations(handle ChunkHandle) (*ChunkMeta, []string, error)

	// 分配新 Chunk（写文件时）
	AllocateChunk(path string) (ChunkHandle, []string, error)

	// 租约相关
	GrantLease(handle ChunkHandle) (*Lease, error)
	RenewLease(handle ChunkHandle, primary string) error

	// ChunkServer 调用
	Heartbeat(addr string, chunks []ChunkHandle) error
	ReportChunk(handle ChunkHandle, version uint32) error
}
