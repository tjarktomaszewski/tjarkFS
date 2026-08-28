package metadata

import (
	"path/filepath"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/tjarktomaszewski/tjarkFS/internal/domain"
)

// Verifies that the _pragma DSN decoration is applied on every pooled
// connection (foreign_keys is per-connection in SQLite) and that the
// ON DELETE CASCADE declaration actually works.
func TestForeignKeysPragmaApplied(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "fk.sqlite")
	repo, err := NewSQLiteFileRepository(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer repo.Close()

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var on int
			if err := repo.db.QueryRow(`PRAGMA foreign_keys`).Scan(&on); err != nil {
				t.Error(err)
				return
			}
			if on != 1 {
				t.Errorf("foreign_keys = %d on a pooled connection, want 1", on)
			}
		}()
	}
	wg.Wait()

	id, _ := uuid.NewUUID()
	file := domain.File{
		ID:     domain.FileID(id.String()),
		Name:   "fk-test",
		Chunks: []domain.ChunkID{"c1", "c2"},
	}
	if err := repo.Save(file); err != nil {
		t.Fatal(err)
	}
	// Delete the parent without manual child cleanup -> relies on FK cascade.
	if _, err := repo.db.Exec(`DELETE FROM files`); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := repo.db.QueryRow(`SELECT COUNT(*) FROM file_chunks`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("file_chunks rows after parent delete: %d, want 0 (cascade not active)", n)
	}
}
