package domain

type ChunkID string

type Chunk struct {
	ID   ChunkID
	Size int64
}
