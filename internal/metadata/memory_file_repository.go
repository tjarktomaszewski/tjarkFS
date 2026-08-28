package metadata

import (
	"maps"
	"slices"
	"sync"

	"github.com/tjarktomaszewski/tjarkFS/internal/domain"
)

// MemoryFileRepository is an in-memory domain.FileRepository. It maintains a
// per-chunk reference count so that deleting a file only releases chunks that
// no other file references anymore.
type MemoryFileRepository struct {
	sync.RWMutex
	files     map[string]domain.File
	chunkRefs map[domain.ChunkID]int
}

func NewMemoryFileRepository() *MemoryFileRepository {
	return &MemoryFileRepository{
		files:     make(map[string]domain.File),
		chunkRefs: make(map[domain.ChunkID]int),
	}
}

func (m *MemoryFileRepository) Save(file domain.File) error {
	m.Lock()
	defer m.Unlock()

	// A re-save may change the file's chunks: release the old references
	// before taking the new ones.
	if old, ok := m.files[string(file.ID)]; ok {
		for _, chunkID := range distinctChunkIDs(old.Chunks) {
			m.decrementChunkRef(chunkID)
		}
	}

	m.files[string(file.ID)] = file
	for _, chunkID := range distinctChunkIDs(file.Chunks) {
		m.chunkRefs[chunkID]++
	}

	return nil
}

func (m *MemoryFileRepository) Get(id domain.FileID) (*domain.File, error) {
	m.RLock()
	defer m.RUnlock()
	file, ok := m.files[string(id)]
	if !ok {
		return nil, domain.ErrFileNotFound
	}
	return &file, nil
}

func (m *MemoryFileRepository) List() ([]domain.File, error) {
	m.RLock()
	defer m.RUnlock()

	return slices.Collect(maps.Values(m.files)), nil
}

// Delete removes the file and releases one reference of each of its chunks.
// It returns the IDs of the chunks whose reference count dropped to zero.
// Deleting an unknown file is a no-op.
func (m *MemoryFileRepository) Delete(id domain.FileID) ([]domain.ChunkID, error) {
	m.Lock()
	defer m.Unlock()

	file, ok := m.files[string(id)]
	if !ok {
		return []domain.ChunkID{}, nil
	}
	delete(m.files, string(id))

	unreferenced := make([]domain.ChunkID, 0)
	for _, chunkID := range distinctChunkIDs(file.Chunks) {
		m.decrementChunkRef(chunkID)
		if m.chunkRefs[chunkID] == 0 {
			unreferenced = append(unreferenced, chunkID)
		}
	}
	return unreferenced, nil
}

// decrementChunkRef releases one reference of the chunk; entries that drop
// to zero are removed from the map.
func (m *MemoryFileRepository) decrementChunkRef(chunkID domain.ChunkID) {
	if m.chunkRefs[chunkID] <= 1 {
		delete(m.chunkRefs, chunkID)
		return
	}
	m.chunkRefs[chunkID]--
}
