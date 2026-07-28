package domain

type File struct {
	ID     string
	Name   string
	Chunks []ChunkID
}
