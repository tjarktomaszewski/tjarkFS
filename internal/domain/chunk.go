package domain

type ChunkID [32]byte

type Chunk struct {
	ID   ChunkID
	Size int64
}
