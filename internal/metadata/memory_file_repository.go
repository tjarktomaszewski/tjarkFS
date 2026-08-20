package metadata

import (
	"fmt"
	"maps"
	"slices"
	"sync"

	"github.com/tjarktomaszewski/tjarkFS/internal/domain"
)

type MemoryFileRepository struct {
	sync.RWMutex
	files map[string]domain.File
}

func NewMemoryFileRepository() *MemoryFileRepository {
	return &MemoryFileRepository{
		files: make(map[string]domain.File),
	}
}

func (m *MemoryFileRepository) Save(file domain.File) error {
	m.Lock()
	defer m.Unlock()
	m.files[string(file.ID)] = file

	return nil
}

func (m *MemoryFileRepository) Get(id domain.FileID) (*domain.File, error) {
	m.RLock()
	defer m.RUnlock()
	file, ok := m.files[string(id)]
	if !ok {
		return nil, fmt.Errorf("file %s not found in memory repository", id)
	}
	return &file, nil
}

func (m *MemoryFileRepository) List() ([]domain.File, error) {
	m.RLock()
	defer m.RUnlock()

	return slices.Collect(maps.Values(m.files)), nil
}

func (m *MemoryFileRepository) Delete(id domain.FileID) error {
	delete(m.files, string(id))
	return nil
}
