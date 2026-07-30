package storage

import "github.com/tjarktomaszewski/tjarkFS/internal/domain"

type StoreWriter struct {
	store Store
}

func NewStoreWriter(store Store) *StoreWriter {
	return &StoreWriter{
		store: store,
	}
}

func (w *StoreWriter) Write(stream *domain.ChunkStream) error {
	id := stream.Chunk.ID
	return w.store.Put(string(id[:]), stream.Reader)
}
