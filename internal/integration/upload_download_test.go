package integration_test

import (
	"bytes"
	"testing"

	"github.com/tjarktomaszewski/tjarkFS/internal/chunking"
	"github.com/tjarktomaszewski/tjarkFS/internal/identity"
	"github.com/tjarktomaszewski/tjarkFS/internal/metadata"
	"github.com/tjarktomaszewski/tjarkFS/internal/service"
	"github.com/tjarktomaszewski/tjarkFS/internal/storage"
)

func TestUploadDownload(t *testing.T) {
	input := []byte(
		"This is a test file for tjarkFS. " +
			"It should be split into multiple chunks.",
	)

	tempDir := t.TempDir()

	// Storage

	store := storage.NewFileSystemStorage(
		tempDir,
	)
	t.Logf("storage directory: %s", tempDir)
	writer := storage.NewStoreWriter(store)

	reader := storage.NewStoreReader(store)

	// Metadata

	repository := metadata.NewMemoryFileRepository()

	// Identity

	idGenerator := identity.UUIDFileIDGenerator{}

	// Services

	uploadService := service.NewUploadService(
		chunking.NewChunker,
		writer,
		repository,
		idGenerator,
		10, // kleine Chunks für Test
	)

	downloadService := service.NewDownloadService(
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
