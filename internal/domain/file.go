package domain

type File struct {
	ID     [32]byte
	Name   string
	Chunks []ChunkID
}
