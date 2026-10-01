package store

import (
	"errors"
	"io"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestFileSystem(t *testing.T) {
	tempDir := t.TempDir()
	store := NewFileSystemStorage(tempDir, nil)

	id := uuid.New().String()
	content := "Hello, world!"
	reader := strings.NewReader(content)

	if err := store.Put(id, reader); err != nil {
		t.Fatalf("put failed: %v", err)
	}

	exists, err := store.Exists(id)
	if err != nil {
		t.Fatalf("exists failed: %v", err)
	}
	if !exists {
		t.Errorf("%s does not exist, expected it to exist", id)
	}

	f, err := store.Get(id)
	if err != nil {
		t.Fatal(err)
	}

	b, err := io.ReadAll(f)
	if err != nil {
		_ = f.Close()
		t.Fatalf("read failed: %v", err)
	}
	// The reader is closed before the delete, not deferred past it: Windows
	// refuses to remove a file with an open handle, while POSIX unlinks it
	// and lets the open handle read to the end. The same test therefore
	// means two different things on the two platforms unless the handle is
	// gone first. See the Delete contract in filesystem_store.go.
	if err := f.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	if string(b) != content {
		t.Errorf("expected %s, got %s", content, string(b))
	}

	if err := store.Delete(id); err != nil {
		t.Fatalf("delete failed: %v", err)
	}

	exists, err = store.Exists(id)
	if err != nil {
		t.Fatalf("exists failed: %v", err)
	}
	if exists {
		t.Errorf("%s does exist, expected it to not exist", id)
	}
}

func TestFileSystemPutAtomicLeavesNothingBehindOnFillError(t *testing.T) {
	tempDir := t.TempDir()
	store := NewFileSystemStorage(tempDir, nil)
	id := uuid.New().String()

	fillErr := errors.New("content rejected")
	err := store.PutAtomic(id, func(w io.Writer) error {
		if _, err := w.Write([]byte("half a chunk")); err != nil {
			return err
		}
		return fillErr
	})

	if !errors.Is(err, fillErr) {
		t.Fatalf("expected the fill error to be wrapped, got %v", err)
	}

	exists, err := store.Exists(id)
	if err != nil {
		t.Fatalf("exists failed: %v", err)
	}
	if exists {
		t.Errorf("chunk %s is visible although fill failed", id)
	}

	var leftovers []string
	walkErr := filepath.WalkDir(tempDir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() && strings.Contains(entry.Name(), ".tmp-") {
			leftovers = append(leftovers, path)
		}
		return nil
	})
	if walkErr != nil {
		t.Fatalf("walk failed: %v", walkErr)
	}
	if len(leftovers) > 0 {
		t.Errorf("temporary files left behind: %v", leftovers)
	}
}
