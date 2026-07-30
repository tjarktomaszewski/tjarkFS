package domain

type FileID string

type File struct {
	ID     FileID
	Name   string
	Size   int64
	Chunks []ChunkID
}
