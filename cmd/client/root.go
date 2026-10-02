package main

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
	"github.com/tjarktomaszewski/tjarkFS/internal/chunker"
	"github.com/tjarktomaszewski/tjarkFS/internal/client"
	"github.com/tjarktomaszewski/tjarkFS/internal/config"
	"github.com/tjarktomaszewski/tjarkFS/internal/controlplane/metadata"
	"github.com/tjarktomaszewski/tjarkFS/internal/identity"
	logger "github.com/tjarktomaszewski/tjarkFS/internal/logging"
	"github.com/tjarktomaszewski/tjarkFS/internal/storagenode/store"
)

var cfg *config.Client

type app struct {
	repository *metadata.SQLiteFileRepository
	upload     *client.UploadService
	download   *client.DownloadService
	list       *client.ListService
	deleteSvc  *client.DeleteService
}

func (a *app) Close() error {
	return a.repository.Close()
}

func newApp() (*app, error) {
	chunkSize, err := config.ParseChunkSize(cfg.ChunkSize)
	if err != nil {
		return nil, fmt.Errorf("parse --chunk-size %q: %w", cfg.ChunkSize, err)
	}

	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}
	logger := logger.NewLogger(slog.LevelInfo)
	chunkStore := store.NewFileSystemStorage(cfg.DataDir, logger)
	chunks := store.NewAdapter(chunkStore)

	repository, err := metadata.NewSQLiteFileRepository(filepath.Join(cfg.DataDir, "tjarkfs.sqlite"))
	if err != nil {
		return nil, fmt.Errorf("open metadata db: %w", err)
	}

	idGenerator := identity.NewUUIDFileIDGenerator(slog.Default())

	return &app{
		repository: repository,
		upload:     client.NewUploadService(chunker.NewChunker, chunks, repository, idGenerator, chunkSize),
		download:   client.NewDownloadService(chunks, repository),
		list:       client.NewListService(repository),
		deleteSvc:  client.NewDeleteService(repository, chunks),
	}, nil
}

func newRootCmd() (*cobra.Command, error) {
	c, err := config.NewClient()
	if err != nil {
		return nil, err
	}
	cfg = c

	root := &cobra.Command{
		Use:           "tjarkfs",
		Short:         "tjark's distributed filesystem",
		SilenceErrors: true,
		SilenceUsage:  true,
	}

	root.PersistentFlags().AddGoFlagSet(c.FlagSet)
	root.AddCommand(uploadCmd, downloadCmd, listCmd, removeCmd)

	return root, nil
}
