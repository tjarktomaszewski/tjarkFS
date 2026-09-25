package main

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"

	"github.com/spf13/cobra"
	"github.com/tjarktomaszewski/tjarkFS/internal/chunker"
	"github.com/tjarktomaszewski/tjarkFS/internal/client"
	"github.com/tjarktomaszewski/tjarkFS/internal/controlplane/metadata"
	"github.com/tjarktomaszewski/tjarkFS/internal/identity"
	"github.com/tjarktomaszewski/tjarkFS/internal/storagenode/store"
)

var (
	dataDir         string // --data-dir
	chunkSizeString string // --chunk-size
	verbose         bool   // --verbose
)

// app bündelt das für alle Commands gemeinsame Wiring: Storage,
// Metadaten-DB und die vier Services.
type app struct {
	repository *metadata.SQLiteFileRepository
	upload     *client.UploadService
	download   *client.DownloadService
	list       *client.ListService
	deleteSvc  *client.DeleteService
}

// Close gibt die Metadaten-DB frei.
func (a *app) Close() error {
	return a.repository.Close()
}

// newApp baut das komplette System auf: Logger, Datenverzeichnis,
// Storage mit Adaptern, Metadaten-DB und die Services.
//
// Es wird von jedem Command zu Beginn der RunE aufgerufen, damit
// reine Aufrufe wie --help keine Seiteneffekte produzieren.
func newApp() (*app, error) {
	level := slog.LevelInfo
	if verbose {
		level = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(logger)

	chunkSize, err := parseChunkSize(chunkSizeString)
	if err != nil {
		return nil, fmt.Errorf("parse --chunk-size %q: %w", chunkSizeString, err)
	}

	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}

	chunkStore := store.NewFileSystemStorage(dataDir)
	writer := store.NewStoreWriter(chunkStore)
	reader := store.NewStoreReader(chunkStore)
	remover := store.NewStoreRemover(chunkStore)

	repository, err := metadata.NewSQLiteFileRepository(filepath.Join(dataDir, "tjarkfs.sqlite"))
	if err != nil {
		return nil, fmt.Errorf("open metadata db: %w", err)
	}

	idGenerator := identity.NewUUIDFileIDGenerator(slog.Default())

	return &app{
		repository: repository,
		upload:     client.NewUploadService(chunker.NewChunker, writer, repository, idGenerator, chunkSize),
		download:   client.NewDownloadService(reader, repository),
		list:       client.NewListService(repository),
		deleteSvc:  client.NewDeleteService(repository, remover),
	}, nil
}

func newRootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:   "tjarkfs",
		Short: "distributed filesystem",
		// main() druckt Fehler selbst (vermeidet Doppel-Ausgabe).
		SilenceErrors: true,
		// Bei Laufzeit-Fehlern keine Usage nachdrucken.
		SilenceUsage: true,
	}

	// Env-Fallback für die Flag-Defaults.
	// Vorrang-Reihenfolge: explizites Flag > Env > hartkodierter Default.
	defaultDataDir := "./data"
	if v := os.Getenv("DATA_DIR"); v != "" {
		defaultDataDir = v
	}
	defaultChunkSize := "1m"
	if v := os.Getenv("CHUNK_SIZE"); v != "" {
		defaultChunkSize = v
	}

	// Globale Flags (werden von allen Subcommands geerbt).
	root.PersistentFlags().StringVar(&dataDir, "data-dir", defaultDataDir,
		"Verzeichnis für Chunks und Metadaten-DB")
	root.PersistentFlags().StringVar(&chunkSizeString, "chunk-size", defaultChunkSize,
		"Chunk-Größe (z. B. 64k, 1m, 64m)")
	root.PersistentFlags().BoolVar(&verbose, "verbose", false,
		"Verboseres Logging")

	root.AddCommand(uploadCmd, downloadCmd, listCmd, removeCmd)

	return root
}

// parseChunkSize wandelt eine Größenangabe (optionale k/m/g-Suffixe)
// in Bytes um; ohne Suffix ist der Wert bereits Bytes.
func parseChunkSize(chunkSizeStr string) (int64, error) {
	if chunkSizeStr == "" {
		return 0, fmt.Errorf("chunk size cannot be empty")
	}

	lastChar := chunkSizeStr[len(chunkSizeStr)-1]

	// No suffix: interpret as bytes
	if lastChar >= '0' && lastChar <= '9' {
		return strconv.ParseInt(chunkSizeStr, 10, 64)
	}

	value, err := strconv.ParseInt(chunkSizeStr[:len(chunkSizeStr)-1], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse chunk size %q: %w", chunkSizeStr, err)
	}

	switch lastChar {
	case 'k':
		return value * 1024, nil
	case 'm':
		return value * 1024 * 1024, nil
	case 'g':
		return value * 1024 * 1024 * 1024, nil
	default:
		return 0, fmt.Errorf("invalid chunk size suffix: %c", lastChar)
	}
}
