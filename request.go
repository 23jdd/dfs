package dfs

type CreateFileRequest struct {
	Path string
}

type DeleteFileRequest struct {
	Path string
}
type GetFileInfoRequest struct {
	Path string
}
type ReadLocationsRequest struct {
	FileName string
	Index    ChunkHandle
}
type ReadChunkRequest struct {
	Handle  ChunkHandle
	Version uint32
	Offset int64
	Length int
}
