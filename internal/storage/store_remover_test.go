package storage

import (
	"errors"
	"io"
	"io/fs"
	"strings"
	"testing"
)

type fakeStore struct {
	deleteErr error
	deleted   []string
}

func (f *fakeStore) Put(string, io.Reader) error { return nil }

func (f *fakeStore) Get(string) (io.ReadCloser, error) { return nil, nil }

func (f *fakeStore) Delete(id string) error {
	f.deleted = append(f.deleted, id)
	return f.deleteErr
}

func (f *fakeStore) Exists(string) (bool, error) { return false, nil }

func TestStoreRemoverRemove(t *testing.T) {
	store := &fakeStore{}
	remover := NewStoreRemover(store)

	if err := remover.Remove("c1"); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
	if len(store.deleted) != 1 || store.deleted[0] != "c1" {
		t.Fatalf("expected [c1] to be deleted, got %v", store.deleted)
	}
}

func TestStoreRemoverRemoveWrapsError(t *testing.T) {
	underlying := errors.New("i/o failure")
	store := &fakeStore{deleteErr: underlying}
	remover := NewStoreRemover(store)

	err := remover.Remove("c1")
	if err == nil {
		t.Fatal("expected an error, got nil")
	}
	if !errors.Is(err, underlying) {
		t.Fatalf("expected the store error to be wrapped, got %v", err)
	}
	if !strings.Contains(err.Error(), "i/o failure") {
		t.Fatalf("expected error text to mention the store error, got %v", err)
	}
}

func TestStoreRemoverRemoveKeepsNotExistSentinel(t *testing.T) {
	store := &fakeStore{deleteErr: FileNotFoundErr}
	remover := NewStoreRemover(store)

	err := remover.Remove("c1")
	if !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("errors.Is(err, fs.ErrNotExist) should hold through the wrapping, got %v", err)
	}
}