package storage

import (
	"fmt"

	"github.com/tjarktomaszewski/tjarkFS/internal/domain"
)

// StoreRemover adapts a domain.Store to the domain.ChunkRemover interface.
type StoreRemover struct {
	store domain.Store
}

func NewStoreRemover(store domain.Store) *StoreRemover {
	return &StoreRemover{store: store}
}

func (r *StoreRemover) Remove(id domain.ChunkID) error {
	if err := r.store.Delete(string(id)); err != nil {
		return fmt.Errorf("remove chunk %x from store: %w", id, err)
	}
	return nil
}