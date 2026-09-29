package store

import (
	"errors"
	"fmt"
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
	defer f.Close()

	b, err := io.ReadAll(f)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	if string(b) != content {
		t.Errorf("expected %s, got %s", content, string(b))
	}
	fmt.Printf("file content: %s", string(b))

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
