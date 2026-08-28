package storage

import (
	"io"

	"github.com/tjarktomaszewski/tjarkFS/internal/domain"
)

type StoreReader struct {
	store domain.Store
}

func NewStoreReader(store domain.Store) *StoreReader {
	return &StoreReader{
		store: store,
	}
}

func (r *StoreReader) Read(id domain.ChunkID) (io.ReadCloser, error) {
	// The store returns an open file (io.ReadCloser) directly; the caller
	// owns the file descriptor and must close it after consuming the chunk.
	return r.store.Get(string(id[:]))
}
