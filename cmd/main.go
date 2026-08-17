package main

import (
	"bytes"
	"fmt"
	"os"

	"github.com/tjarktomaszewski/tjarkFS/internal/chunking"
	"github.com/tjarktomaszewski/tjarkFS/internal/identity"
	"github.com/tjarktomaszewski/tjarkFS/internal/metadata"
	"github.com/tjarktomaszewski/tjarkFS/internal/service"
	"github.com/tjarktomaszewski/tjarkFS/internal/storage"
)

func main() {
	if err := run(); err != nil {
		panic(err)
	}
}

func run() error {

	original, err := os.ReadFile("test.txt")
	if err != nil {
		return fmt.Errorf("read input file: %w", err)
	}

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
	idGenerator := identity.UUIDFileIDGenerator{}

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

	// Upload

	uploadedFile, err := uploadService.Upload(
		bytes.NewReader(original),
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

	return nil
}
