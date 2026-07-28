package metadata

import (
	"fmt"
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
	m.files[file.ID] = file

	return nil
}

func (m *MemoryFileRepository) Get(id string) (*domain.File, error) {
	m.RLock()
	defer m.RUnlock()
	file, ok := m.files[id]
	if !ok {
		return nil, fmt.Errorf("file %s not found in memory repository", id)
	}
	return &file, nil
}

func (m *MemoryFileRepository) Delete(id string) {
	delete(m.files, id)
}
