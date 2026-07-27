package storage

import (
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestFileSystem(t *testing.T) {
	store := &FilesystemStore{"./"}

	id := uuid.New().String()
	content := "Hello, world!"
	reader := strings.NewReader(content)
	store.Put(id, reader)

	if !store.Exists(id) {
		t.Errorf("%s does not exists, expected it to exist", id)
	}
	f, err := store.Get(id)
	if err != nil {
		t.Error(err)
	}
	b, _ := io.ReadAll(f)

	if string(b) != content {
		t.Errorf("expected %s, got %s", content, string(b))
	}
	fmt.Printf("file content: %s", string(b))

	store.Delete(id)
	if store.Exists(id) {
		t.Errorf("%s does exist, expected it to not exist", id)
	}
}
