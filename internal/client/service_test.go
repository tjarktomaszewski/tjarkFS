package client

import (
	"errors"
	"io/fs"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/tjarktomaszewski/tjarkFS/internal/domain"
)

// fakeRepository is an in-memory domain.FileRepository test double. Like the
// real implementations it maintains a per-chunk reference count: Delete
// returns the IDs of the chunks whose reference count dropped to zero. Get
// returns domain.ErrFileNotFound for unknown IDs.
type fakeRepository struct {
	mu        sync.Mutex
	files     map[domain.FileID]domain.File
	chunkRefs map[domain.ChunkID]int
	delErr    error
	listErr   error
}

func newFakeRepository(files ...domain.File) *fakeRepository {
	repo := &fakeRepository{
		files:     make(map[domain.FileID]domain.File),
		chunkRefs: make(map[domain.ChunkID]int),
	}
	for _, f := range files {
		for _, chunkID := range distinct(f.Chunks) {
			repo.chunkRefs[chunkID]++
		}
		repo.files[f.ID] = f
	}
	return repo
}

func (r *fakeRepository) Save(file domain.File) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if old, ok := r.files[file.ID]; ok {
		for _, chunkID := range distinct(old.Chunks) {
			r.decr(chunkID)
		}
	}
	r.files[file.ID] = file
	for _, chunkID := range distinct(file.Chunks) {
		r.chunkRefs[chunkID]++
	}
	return nil
}

func (r *fakeRepository) Get(id domain.FileID) (*domain.File, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	file, ok := r.files[id]
	if !ok {
		return nil, domain.ErrFileNotFound
	}
	return &file, nil
}

func (r *fakeRepository) Delete(id domain.FileID) ([]domain.ChunkID, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.delErr != nil {
		return nil, r.delErr
	}
	file, ok := r.files[id]
	if !ok {
		return []domain.ChunkID{}, nil
	}
	delete(r.files, id)

	unreferenced := make([]domain.ChunkID, 0)
	for _, chunkID := range distinct(file.Chunks) {
		r.decr(chunkID)
		if r.chunkRefs[chunkID] == 0 {
			unreferenced = append(unreferenced, chunkID)
		}
	}
	return unreferenced, nil
}

func (r *fakeRepository) List() ([]domain.File, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.listErr != nil {
		return nil, r.listErr
	}
	out := make([]domain.File, 0, len(r.files))
	for _, f := range r.files {
		out = append(out, f)
	}
	return out, nil
}

func (r *fakeRepository) decr(chunkID domain.ChunkID) {
	if r.chunkRefs[chunkID] <= 1 {
		delete(r.chunkRefs, chunkID)
		return
	}
	r.chunkRefs[chunkID]--
}

// distinct returns the unique chunk IDs, preserving first-occurrence order.
func distinct(ids []domain.ChunkID) []domain.ChunkID {
	seen := make(map[domain.ChunkID]bool, len(ids))
	out := make([]domain.ChunkID, 0, len(ids))
	for _, id := range ids {
		if !seen[id] {
			seen[id] = true
			out = append(out, id)
		}
	}
	return out
}

// fakeRemover is a domain.ChunkRemover test double. It records removed chunk
// IDs and can be configured to fail for specific chunks.
type fakeRemover struct {
	mu        sync.Mutex
	removed   []domain.ChunkID
	failFor   map[domain.ChunkID]error
	globalErr error
}

func (r *fakeRemover) Remove(id domain.ChunkID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.removed = append(r.removed, id)
	if r.globalErr != nil {
		return r.globalErr
	}
	if err, ok := r.failFor[id]; ok {
		return err
	}
	return nil
}

func (r *fakeRemover) removedChunks() []domain.ChunkID {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.removed)
}

func testFile() domain.File {
	return domain.File{
		ID:     "file-1",
		Name:   "a.txt",
		Size:   40,
		Chunks: []domain.ChunkID{"c1", "c2"},
	}
}

func TestDeleteServiceDeletesFileAndChunks(t *testing.T) {
	file := testFile()
	repo := newFakeRepository(file)
	remover := &fakeRemover{}
	svc := NewDeleteService(repo, remover)

	if err := svc.Delete(file.ID); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}

	if got := remover.removedChunks(); !slices.Equal(got, file.Chunks) {
		t.Errorf("expected chunks %v to be removed, got %v", file.Chunks, got)
	}

	if _, err := repo.Get(file.ID); !errors.Is(err, domain.ErrFileNotFound) {
		t.Errorf("expected metadata to be gone, got %v", err)
	}
}

func TestDeleteServiceKeepsSharedChunks(t *testing.T) {
	a := domain.File{ID: "a", Name: "a.txt", Chunks: []domain.ChunkID{"c1", "c2"}}
	b := domain.File{ID: "b", Name: "b.txt", Chunks: []domain.ChunkID{"c2", "c3"}}
	repo := newFakeRepository(a, b)
	remover := &fakeRemover{}
	svc := NewDeleteService(repo, remover)

	// c2 is still referenced by b: only c1 may be removed.
	if err := svc.Delete(a.ID); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
	if got := remover.removedChunks(); !slices.Equal(got, []domain.ChunkID{"c1"}) {
		t.Errorf("expected only [c1] to be removed, got %v", got)
	}

	// b is intact and still has both chunks.
	f, err := repo.Get(b.ID)
	if err != nil {
		t.Fatalf("expected b to survive, got %v", err)
	}
	if !slices.Equal(f.Chunks, []domain.ChunkID{"c2", "c3"}) {
		t.Errorf("expected b to keep [c2 c3], got %v", f.Chunks)
	}

	// Now b goes: c2 and c3 are finally unreferenced.
	remover2 := &fakeRemover{}
	svc2 := NewDeleteService(repo, remover2)
	if err := svc2.Delete(b.ID); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
	got := remover2.removedChunks()
	slices.Sort(got)
	if !slices.Equal(got, []domain.ChunkID{"c2", "c3"}) {
		t.Errorf("expected [c2 c3] to be removed, got %v", got)
	}
}

func TestDeleteServiceIsIdempotent(t *testing.T) {
	file := testFile()
	repo := newFakeRepository(file)
	remover := &fakeRemover{}
	svc := NewDeleteService(repo, remover)

	if err := svc.Delete(file.ID); err != nil {
		t.Fatalf("first delete: expected nil, got %v", err)
	}
	if err := svc.Delete(file.ID); err != nil {
		t.Fatalf("second delete: expected nil (idempotent), got %v", err)
	}
	// Nothing was removed on the second call.
	if got := remover.removedChunks(); !slices.Equal(got, file.Chunks) {
		t.Errorf("expected chunks to be removed exactly once, got %v", got)
	}
}

func TestDeleteServiceUnknownFile(t *testing.T) {
	repo := newFakeRepository()
	remover := &fakeRemover{}
	svc := NewDeleteService(repo, remover)

	if err := svc.Delete("does-not-exist"); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
	if got := remover.removedChunks(); len(got) != 0 {
		t.Errorf("expected no chunk removals, got %v", got)
	}
}

func TestDeleteServiceToleratesMissingChunks(t *testing.T) {
	file := testFile()
	repo := newFakeRepository(file)
	remover := &fakeRemover{failFor: map[domain.ChunkID]error{"c1": fs.ErrNotExist}}
	svc := NewDeleteService(repo, remover)

	if err := svc.Delete(file.ID); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
	if _, err := repo.Get(file.ID); !errors.Is(err, domain.ErrFileNotFound) {
		t.Errorf("expected metadata to be gone, got %v", err)
	}
}

func TestDeleteServiceReturnsChunkErrors(t *testing.T) {
	file := testFile()
	repo := newFakeRepository(file)
	remover := &fakeRemover{failFor: map[domain.ChunkID]error{"c2": errors.New("i/o failure")}}
	svc := NewDeleteService(repo, remover)

	err := svc.Delete(file.ID)
	if err == nil {
		t.Fatal("expected an error, got nil")
	}
	if !strings.Contains(err.Error(), "i/o failure") {
		t.Errorf("expected error to mention the chunk failure, got %v", err)
	}
	// Metadata is deleted even if chunk removal partially failed.
	if _, err := repo.Get(file.ID); !errors.Is(err, domain.ErrFileNotFound) {
		t.Errorf("expected metadata to be gone, got %v", err)
	}
}

func TestDeleteServicePropagatesMetadataDeleteErrors(t *testing.T) {
	file := testFile()
	repo := newFakeRepository(file)
	repo.delErr = errors.New("fs down")
	svc := NewDeleteService(repo, &fakeRemover{})

	err := svc.Delete(file.ID)
	if err == nil || !strings.Contains(err.Error(), "fs down") {
		t.Fatalf("expected the metadata delete error to be returned, got %v", err)
	}
}

func TestListServiceReturnsFiles(t *testing.T) {
	other := domain.File{ID: "file-2", Name: "b.txt", Size: 8}
	repo := newFakeRepository(testFile(), other)
	svc := NewListService(repo)

	files, err := svc.List()
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
	if len(files) != 2 {
		t.Fatalf("expected 2 files, got %d", len(files))
	}
	ids := make([]string, 0, len(files))
	for _, f := range files {
		ids = append(ids, string(f.ID))
	}
	slices.Sort(ids)
	if !slices.Equal(ids, []string{"file-1", "file-2"}) {
		t.Errorf("unexpected file IDs: %v", ids)
	}
}

func TestListServiceEmpty(t *testing.T) {
	svc := NewListService(newFakeRepository())

	files, err := svc.List()
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
	if len(files) != 0 {
		t.Errorf("expected no files, got %v", files)
	}
}

func TestListServicePropagatesErrors(t *testing.T) {
	repo := newFakeRepository(testFile())
	repo.listErr = errors.New("query failed")
	svc := NewListService(repo)

	_, err := svc.List()
	if err == nil || !strings.Contains(err.Error(), "query failed") {
		t.Fatalf("expected the list error to be returned, got %v", err)
	}
}
