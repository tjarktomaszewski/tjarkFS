package store

import (
	"io"

	"github.com/tjarktomaszewski/tjarkFS/internal/domain"
)

// Adapter gives a local store the three narrow chunk ports the client is
// written against: domain.ChunkWriter, domain.ChunkReader and
// domain.ChunkRemover. Every method forwards to the store and does nothing
// else.
type Adapter struct {
	store domain.Store
}

var (
	_ domain.ChunkWriter  = (*Adapter)(nil)
	_ domain.ChunkReader  = (*Adapter)(nil)
	_ domain.ChunkRemover = (*Adapter)(nil)
)

// NewAdapter builds the adapter over a store.
func NewAdapter(store domain.Store) *Adapter {
	return &Adapter{store: store}
}

// Write stores the chunk the stream carries.
func (a *Adapter) Write(stream *domain.ChunkStream) error {
	return a.store.Put(string(stream.Chunk.ID), stream.Reader)
}

// Read opens the chunk for reading. The caller owns the returned reader and
// has to close it — the one thing the narrow port documents that the store
// does not.
func (a *Adapter) Read(id domain.ChunkID) (io.ReadCloser, error) {
	return a.store.Get(string(id))
}

// Remove deletes the chunk. A chunk that is not there comes back as
// fs.ErrNotExist and is left to the caller: DeleteService reads it as
// "nothing left to delete" instead of an error.
func (a *Adapter) Remove(id domain.ChunkID) error {
	return a.store.Delete(string(id))
}
