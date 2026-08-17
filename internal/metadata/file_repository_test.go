package metadata

import (
	"path/filepath"
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

	repo.Delete(f.ID)
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

	repo.Delete(f.ID)
	f, err = repo.Get(file.ID)
	if err == nil {
		t.Error("expected an error, did not get one")
	}
}
