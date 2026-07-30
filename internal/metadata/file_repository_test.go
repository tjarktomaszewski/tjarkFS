package metadata

import (
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
