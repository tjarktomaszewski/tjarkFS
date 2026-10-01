package fence_test

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/tjarktomaszewski/tjarkFS/internal/domain"
	"github.com/tjarktomaszewski/tjarkFS/internal/storagenode/fence"
)

// The fence store is the storage node's half of split-brain protection: it
// remembers the highest fence token per file, so a writer whose lease has
// been superseded is rejected no matter what it believes.

func newFenceStore(t *testing.T) (*fence.Store, string) {
	t.Helper()

	dataDir := t.TempDir()
	store, err := fence.NewStore(dataDir)
	if err != nil {
		t.Fatalf("open fence store: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})

	return store, dataDir
}

func TestCheckAndUpdateAcceptsTheFirstTokenForAFile(t *testing.T) {
	store, _ := newFenceStore(t)

	// No row for this file yet: the first writer sets the bar.
	if err := store.CheckAndUpdate("file-a", 1); err != nil {
		t.Fatalf("first token rejected: %v", err)
	}
}

func TestCheckAndUpdateAcceptsTheSameTokenAgain(t *testing.T) {
	store, _ := newFenceStore(t)

	if err := store.CheckAndUpdate("file-a", 3); err != nil {
		t.Fatalf("token 3 rejected: %v", err)
	}

	// A retried PutChunk carries the same token — the client re-sends a chunk
	// it is not sure arrived. Rejecting it as stale would make the retry
	// logic of Phase 4 useless, so the check is "< older", not "<= newest".
	if err := store.CheckAndUpdate("file-a", 3); err != nil {
		t.Fatalf("the same token was rejected on the second call: %v", err)
	}
}

func TestCheckAndUpdateAcceptsAHigherToken(t *testing.T) {
	store, _ := newFenceStore(t)

	if err := store.CheckAndUpdate("file-a", 3); err != nil {
		t.Fatalf("token 3 rejected: %v", err)
	}
	if err := store.CheckAndUpdate("file-a", 4); err != nil {
		t.Fatalf("token 4 rejected: %v", err)
	}
}

func TestCheckAndUpdateRejectsALowerTokenAndKeepsTheMaximum(t *testing.T) {
	store, _ := newFenceStore(t)
	if err := store.CheckAndUpdate("file-a", 5); err != nil {
		t.Fatalf("token 5 rejected: %v", err)
	}

	// A writer that lost the lease for this file is what the token is for.
	err := store.CheckAndUpdate("file-a", 4)
	if !errors.Is(err, domain.ErrStaleFence) {
		t.Fatalf("token 4 after 5: %v, want ErrStaleFence", err)
	}

	// The rejected write must change nothing. If it lowered the maximum back
	// to 4, a third writer holding token 4 would still be let through.
	if err := store.CheckAndUpdate("file-a", 5); err != nil {
		t.Fatalf("token 5 after a rejected token 4: %v", err)
	}
	if err := store.CheckAndUpdate("file-a", 6); err != nil {
		t.Fatalf("token 6 after a rejected token 4: %v", err)
	}
}

func TestCheckAndUpdateCountsEveryFileSeparately(t *testing.T) {
	store, _ := newFenceStore(t)

	// A high token for one file says nothing about any other file: the fence
	// is per file, and a busy file must not lock out every other upload.
	if err := store.CheckAndUpdate("file-a", 10); err != nil {
		t.Fatalf("file-a token 10 rejected: %v", err)
	}
	if err := store.CheckAndUpdate("file-b", 1); err != nil {
		t.Fatalf("file-b token 1 rejected: %v", err)
	}
	if err := store.CheckAndUpdate("file-b", 10); err != nil {
		t.Fatalf("file-b token 10 rejected: %v", err)
	}

	if err := store.CheckAndUpdate("file-a", 1); !errors.Is(err, domain.ErrStaleFence) {
		t.Fatalf("file-a token 1 after 10: %v, want ErrStaleFence", err)
	}
	if err := store.CheckAndUpdate("file-c", 1); err != nil {
		t.Fatalf("file-c token 1 rejected: %v", err)
	}
}

func TestCheckAndUpdateRemembersTheTokenAcrossARestart(t *testing.T) {
	dataDir := t.TempDir()
	store, err := fence.NewStore(dataDir)
	if err != nil {
		t.Fatalf("open fence store: %v", err)
	}
	if err := store.CheckAndUpdate("file-a", 7); err != nil {
		t.Fatalf("token 7 rejected: %v", err)
	}
	// The sidecar has to live next to the chunks, otherwise a restart comes
	// up with an empty database.
	if _, err := os.Stat(filepath.Join(dataDir, "fences.db")); err != nil {
		t.Fatalf("the sidecar is not at <data-dir>/fences.db: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close fence store: %v", err)
	}

	restarted, err := fence.NewStore(dataDir)
	if err != nil {
		t.Fatalf("reopen fence store: %v", err)
	}
	t.Cleanup(func() {
		_ = restarted.Close()
	})

	// A node that forgot the highest token it ever saw would let a superseded
	// writer back in after every restart — the one bug the sidecar exists to
	// prevent.
	if err := restarted.CheckAndUpdate("file-a", 6); !errors.Is(err, domain.ErrStaleFence) {
		t.Fatalf("token 6 after the restart: %v, want ErrStaleFence", err)
	}
	if err := restarted.CheckAndUpdate("file-a", 7); err != nil {
		t.Fatalf("token 7 after the restart: %v", err)
	}
	if err := restarted.CheckAndUpdate("file-a", 8); err != nil {
		t.Fatalf("token 8 after the restart: %v", err)
	}
}

func TestCheckAndUpdateRaisesTheMaximumUnderConcurrentWrites(t *testing.T) {
	store, _ := newFenceStore(t)

	const writers = 8
	const tokens = 8
	var wg sync.WaitGroup
	errs := make(chan error, writers*tokens)
	for w := range writers {
		for token := range tokens {
			wg.Add(1)
			go func() {
				defer wg.Done()
				errs <- store.CheckAndUpdate("file-a", int64(w*tokens+token+1))
			}()
		}
	}
	wg.Wait()
	close(errs)

	// Tokens 1 to 64 raced. Which of them won depends on the order, but a
	// failure may only ever be ErrStaleFence — a busy database is not an
	// answer the node can give.
	for err := range errs {
		if err != nil && !errors.Is(err, domain.ErrStaleFence) {
			t.Fatalf("concurrent check failed: %v", err)
		}
	}

	// 64 is the highest token that was ever sent, so it can never be stale,
	// and once it is through, everything below it has to be.
	if err := store.CheckAndUpdate("file-a", writers*tokens); err != nil {
		t.Fatalf("the highest token of the race was rejected: %v", err)
	}
	for token := int64(1); token < writers*tokens; token++ {
		if err := store.CheckAndUpdate("file-a", token); !errors.Is(err, domain.ErrStaleFence) {
			t.Fatalf("token %d after the race: %v, want ErrStaleFence", token, err)
		}
	}
}
