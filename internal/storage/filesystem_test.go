package storage

import (
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestFileSystem(t *testing.T) {
	tempDir := t.TempDir()
	store := &FilesystemStore{tempDir}

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
