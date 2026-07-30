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
	reader, err := r.store.Get(string(id[:]))
	if err != nil {
		return nil, err
	}

	return io.NopCloser(reader), nil
}
