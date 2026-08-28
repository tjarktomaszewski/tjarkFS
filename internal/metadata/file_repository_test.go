package metadata

import (
	"path/filepath"
	"sort"
	"testing"

	"github.com/google/uuid"
	"github.com/tjarktomaszewski/tjarkFS/internal/domain"
)

func TestMemoryFileRepository(t *testing.T) {
	id, err := uuid.NewUUID()
	if err != nil {
		t.Error(err)
	}
	file := &domain.File{
		ID:   domain.FileID(id.String()),
		Name: "Hello,world!",
	}

	repo := NewMemoryFileRepository()

	err = repo.Save(*file)
	if err != nil {
		t.Error(err)
	}
	f, err := repo.Get(file.ID)
	if err != nil {
		t.Error(err)
	}
	if f.Name != file.Name {
		t.Errorf("expected %s, got %s", file.Name, f.Name)
	}
	if f.ID != file.ID {
		t.Errorf("expected %s, got %s", file.ID, f.ID)
	}

	_, err = repo.Delete(f.ID)
	if err != nil {
		t.Error(err)
	}
	f, err = repo.Get(file.ID)
	if err == nil {
		t.Error("expected an error, did not get one")
	}
}

func TestSQLiteFileRepository(t *testing.T) {
	id, err := uuid.NewUUID()
	if err != nil {
		t.Error(err)
	}
	file := &domain.File{
		ID:     domain.FileID(id.String()),
		Name:   "Hello,world!",
		Size:   14,
		Chunks: []domain.ChunkID{"chunk-1", "chunk-2"},
	}

	dbPath := filepath.Join(t.TempDir(), "test.sqlite")
	repo, err := NewSQLiteFileRepository(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer repo.Close()

	err = repo.Save(*file)
	if err != nil {
		t.Error(err)
	}
	f, err := repo.Get(file.ID)
	if err != nil {
		t.Error(err)
	}
	if f.Name != file.Name {
		t.Errorf("expected %s, got %s", file.Name, f.Name)
	}
	if f.ID != file.ID {
		t.Errorf("expected %s, got %s", file.ID, f.ID)
	}
	if len(f.Chunks) != len(file.Chunks) {
		t.Errorf("expected %d chunks, got %d", len(file.Chunks), len(f.Chunks))
	} else {
		for i, chunkID := range file.Chunks {
			if f.Chunks[i] != chunkID {
				t.Errorf("chunk %d: expected %s, got %s", i, chunkID, f.Chunks[i])
			}
		}
	}

	// Persisted across reopen
	if err := repo.Close(); err != nil {
		t.Fatal(err)
	}
	repo, err = NewSQLiteFileRepository(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer repo.Close()

	f, err = repo.Get(file.ID)
	if err != nil {
		t.Error("expected file to be persisted, got:", err)
	}

	unreferenced, err := repo.Delete(f.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(unreferenced) != len(file.Chunks) {
		t.Errorf("expected %d unreferenced chunks, got %d", len(file.Chunks), len(unreferenced))
	}
	f, err = repo.Get(file.ID)
	if err == nil {
		t.Error("expected an error, did not get one")
	}
}

// testFileRepository is the subset of domain.FileRepository needed to test
// chunk reference counting.
type testFileRepository interface {
	Save(file domain.File) error
	Get(id domain.FileID) (*domain.File, error)
	Delete(id domain.FileID) ([]domain.ChunkID, error)
}

// TestFileRepositoryChunkRefCounts verifies that deleting one file only
// releases chunks that no other file references anymore, for both repository
// implementations.
func TestFileRepositoryChunkRefCounts(t *testing.T) {
	a := domain.File{ID: "a", Name: "a.txt", Size: 10, Chunks: []domain.ChunkID{"c1", "c2"}}
	b := domain.File{ID: "b", Name: "b.txt", Size: 10, Chunks: []domain.ChunkID{"c2", "c3"}}

	tests := []struct {
		name string
		new  func(t *testing.T) testFileRepository
	}{
		{
			name: "memory",
			new:  func(t *testing.T) testFileRepository { return NewMemoryFileRepository() },
		},
		{
			name: "sqlite",
			new: func(t *testing.T) testFileRepository {
				repo, err := NewSQLiteFileRepository(filepath.Join(t.TempDir(), "test.sqlite"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = repo.Close() })
			return repo
		},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := tt.new(t)

			if err := repo.Save(a); err != nil {
				t.Fatal(err)
			}
			if err := repo.Save(b); err != nil {
				t.Fatal(err)
			}

			// c2 is referenced by both files: deleting a must not release it.
			unreferenced, err := repo.Delete(a.ID)
			if err != nil {
				t.Fatal(err)
			}
			if len(unreferenced) != 1 || unreferenced[0] != "c1" {
				t.Errorf("expected [c1] to be unreferenced, got %v", unreferenced)
			}

			f, err := repo.Get(b.ID)
			if err != nil {
				t.Fatal(err)
			}
			if len(f.Chunks) != 2 {
				t.Errorf("expected b to keep its chunks, got %v", f.Chunks)
			}

			// b goes: c2 and c3 are finally unreferenced.
			unreferenced, err = repo.Delete(b.ID)
			if err != nil {
				t.Fatal(err)
			}
			sorted := append([]domain.ChunkID(nil), unreferenced...)
			sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
			if len(sorted) != 2 || sorted[0] != "c2" || sorted[1] != "c3" {
				t.Errorf("expected [c2 c3] to be unreferenced, got %v", unreferenced)
			}

			// Deleting again is a no-op.
			unreferenced, err = repo.Delete(b.ID)
			if err != nil {
				t.Fatal(err)
			}
			if len(unreferenced) != 0 {
				t.Errorf("expected no unreferenced chunks, got %v", unreferenced)
			}

			// Re-saving a resurrects its references; deleting it releases them again.
			if err := repo.Save(a); err != nil {
				t.Fatal(err)
			}
			unreferenced, err = repo.Delete(a.ID)
			if err != nil {
				t.Fatal(err)
			}
			sorted = append([]domain.ChunkID(nil), unreferenced...)
			sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
			if len(sorted) != 2 || sorted[0] != "c1" || sorted[1] != "c2" {
				t.Errorf("expected [c1 c2] to be unreferenced, got %v", unreferenced)
			}
		})
	}
}
