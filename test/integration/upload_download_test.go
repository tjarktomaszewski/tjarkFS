package integration

import (
	"bytes"
	"log/slog"
	"slices"
	"testing"

	"github.com/tjarktomaszewski/tjarkFS/internal/chunker"
	"github.com/tjarktomaszewski/tjarkFS/internal/client"
	"github.com/tjarktomaszewski/tjarkFS/internal/controlplane/metadata"
	"github.com/tjarktomaszewski/tjarkFS/internal/identity"
	"github.com/tjarktomaszewski/tjarkFS/internal/storagenode/store"
)

func TestUploadDownload(t *testing.T) {
	input := []byte(
		"This is a test file for tjarkFS. " +
			"It should be split into multiple chunks.",
	)

	tempDir := t.TempDir()

	// Storage

	chunkStore := store.NewFileSystemStorage(
		tempDir,
	)
	t.Logf("storage directory: %s", tempDir)
	writer := store.NewStoreWriter(chunkStore)

	reader := store.NewStoreReader(chunkStore)

	// Metadata

	repository := metadata.NewMemoryFileRepository()

	// Identity

	idGenerator := identity.NewUUIDFileIDGenerator(slog.Default())

	// Services

	uploadService := client.NewUploadService(
		chunker.NewChunker,
		writer,
		repository,
		idGenerator,
		10, // kleine Chunks für Test
	)

	downloadService := client.NewDownloadService(
		reader,
		repository,
	)

	// Upload

	file, err := uploadService.Upload(
		bytes.NewReader(input),
		"test.txt",
	)

	if err != nil {
		t.Fatalf(
			"upload failed: %v",
			err,
		)
	}

	if len(file.Chunks) <= 1 {
		t.Fatalf(
			"expected multiple chunks, got %d",
			len(file.Chunks),
		)
	}

	// Download

	var output bytes.Buffer

	err = downloadService.Download(
		file.ID,
		&output,
	)

	if err != nil {
		t.Fatalf(
			"download failed: %v",
			err,
		)
	}

	// Verify

	if !bytes.Equal(
		input,
		output.Bytes(),
	) {
		t.Fatalf(
			"downloaded data differs from uploaded data",
		)
	}
}

// TestUploadDownloadDeleteSharedChunks verifies that deleting one file does
// not remove chunks that are still referenced by another file with identical
// content (deduplication), and that the chunk is only physically removed
// when its last reference is gone.
func TestUploadDownloadDeleteSharedChunks(t *testing.T) {
	input := []byte(
		"This content is uploaded twice, so both files " +
			"share the same chunks.",
	)

	tempDir := t.TempDir()

	chunkStore := store.NewFileSystemStorage(tempDir)
	writer := store.NewStoreWriter(chunkStore)
	reader := store.NewStoreReader(chunkStore)

	repository := metadata.NewMemoryFileRepository()

	idGenerator := identity.NewUUIDFileIDGenerator(slog.Default())

	uploadService := client.NewUploadService(
		chunker.NewChunker,
		writer,
		repository,
		idGenerator,
		10, // kleine Chunks für Test
	)
	downloadService := client.NewDownloadService(reader, repository)
	deleteService := client.NewDeleteService(
		repository,
		store.NewStoreRemover(chunkStore),
	)

	// Upload the same content under two names; both files must end up with
	// the identical chunk list.
	first, err := uploadService.Upload(bytes.NewReader(input), "first.txt")
	if err != nil {
		t.Fatalf("upload first: %v", err)
	}
	second, err := uploadService.Upload(bytes.NewReader(input), "second.txt")
	if err != nil {
		t.Fatalf("upload second: %v", err)
	}

	if len(first.Chunks) < 2 {
		t.Fatalf("expected multiple chunks, got %d", len(first.Chunks))
	}
	if !slices.Equal(first.Chunks, second.Chunks) {
		t.Fatalf("expected identical chunk lists, got %v and %v", first.Chunks, second.Chunks)
	}

	// Delete the first file: the chunks must remain on disk for the second.
	if err := deleteService.Delete(first.ID); err != nil {
		t.Fatalf("delete first: %v", err)
	}

	for _, chunkID := range first.Chunks {
		ok, err := chunkStore.Exists(string(chunkID))
		if err != nil {
			t.Fatalf("exists check: %v", err)
		}
		if !ok {
			t.Fatalf("chunk %x removed although still referenced by the second file", chunkID)
		}
	}

	// The second file must still be fully downloadable.
	var out bytes.Buffer
	if err := downloadService.Download(second.ID, &out); err != nil {
		t.Fatalf("download second after shared delete: %v", err)
	}
	if !bytes.Equal(input, out.Bytes()) {
		t.Fatal("second file content differs after shared delete")
	}

	// Delete the second file: the chunks can finally be removed.
	if err := deleteService.Delete(second.ID); err != nil {
		t.Fatalf("delete second: %v", err)
	}

	for _, chunkID := range first.Chunks {
		ok, err := chunkStore.Exists(string(chunkID))
		if err != nil {
			t.Fatalf("exists check: %v", err)
		}
		if ok {
			t.Fatalf("chunk %x still present after its last file was deleted", chunkID)
		}
	}
}
