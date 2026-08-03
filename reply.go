package dfs

type CreateFileReply struct {
}
type DeleteFileReply struct {
}
type GetFileInfoReply struct {
	Info FileMeta
}
type ReadLocationsReply struct {
	Handler ChunkHandle
	Version uint32
	Replicas []string
}
type ReadChunkReply struct{
	 Data []byte
}