package main

import (
	"bytes"
	"fmt"
	"log/slog"
	"os"

	"github.com/tjarktomaszewski/tjarkFS/internal/chunking"
	"github.com/tjarktomaszewski/tjarkFS/internal/identity"
	"github.com/tjarktomaszewski/tjarkFS/internal/metadata"
	"github.com/tjarktomaszewski/tjarkFS/internal/service"
	"github.com/tjarktomaszewski/tjarkFS/internal/storage"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {

	// Expected content, only for verifying the download round-trip.
	original, err := os.ReadFile("test.txt")
	if err != nil {
		return fmt.Errorf("read input file: %w", err)
	}

	// A separate handle that is streamed into the upload, so the file is
	// chunked on the fly instead of being buffered fully in memory first.
	input, err := os.Open("test.txt")
	if err != nil {
		return fmt.Errorf("open input file: %w", err)
	}
	defer input.Close()

	// Setup Storage
	store := storage.NewFileSystemStorage(
		"./data",
	)

	writer := storage.NewStoreWriter(store)

	reader := storage.NewStoreReader(store)

	// Metadata
	repository, err := metadata.NewSQLiteFileRepository("./data/tjarkfs.sqlite")
	if err != nil {
		return fmt.Errorf("open metadata db: %w", err)
	}
	defer repository.Close()

	// File ID generator
	idGenerator := identity.NewUUIDFileIDGenerator(slog.Default())

	// Services
	uploadService := service.NewUploadService(
		chunking.NewChunker,
		writer,
		repository,
		idGenerator,
		16, // absichtlich klein, damit mehrere Chunks entstehen
	)

	downloadService := service.NewDownloadService(
		reader,
		repository,
	)

	listService := service.NewListService(repository)

	deleteService := service.NewDeleteService(
		repository,
		storage.NewStoreRemover(store),
	)

	// Upload

	uploadedFile, err := uploadService.Upload(
		input,
		"test.txt",
	)

	if err != nil {
		return fmt.Errorf("upload failed: %w", err)
	}

	fmt.Printf(
		"File uploaded\nID: %x\nChunks: %d\nSize: %d\n",
		uploadedFile.ID,
		len(uploadedFile.Chunks),
		uploadedFile.Size,
	)

	// Download

	var downloaded bytes.Buffer

	err = downloadService.Download(
		uploadedFile.ID,
		&downloaded,
	)

	if err != nil {
		return fmt.Errorf("download failed: %w", err)
	}

	// Verify

	if !bytes.Equal(
		original,
		downloaded.Bytes(),
	) {
		return fmt.Errorf(
			"downloaded file differs from original",
		)
	}

	fmt.Println("Upload and download successful")

	// List

	files, err := listService.List()
	if err != nil {
		return fmt.Errorf("list failed: %w", err)
	}

	fmt.Printf("Files in store: %d\n", len(files))

	// Delete (called twice to demonstrate idempotency)

	if err := deleteService.Delete(uploadedFile.ID); err != nil {
		return fmt.Errorf("delete failed: %w", err)
	}

	fmt.Println("File deleted")

	if err := deleteService.Delete(uploadedFile.ID); err != nil {
		return fmt.Errorf("delete (second call) failed: %w", err)
	}

	fmt.Println("Second delete call was a no-op (idempotent)")

	return nil
}
