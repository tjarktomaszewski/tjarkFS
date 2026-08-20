package domain

type ChunkRemover interface {
	Remove(id ChunkID) error
}
