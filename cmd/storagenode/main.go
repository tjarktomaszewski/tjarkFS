package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/tjarktomaszewski/tjarkFS/internal/config"
	"github.com/tjarktomaszewski/tjarkFS/internal/domain"
	logger "github.com/tjarktomaszewski/tjarkFS/internal/logging"
	"github.com/tjarktomaszewski/tjarkFS/internal/storagenode/fence"
	"github.com/tjarktomaszewski/tjarkFS/internal/storagenode/server"
	"github.com/tjarktomaszewski/tjarkFS/internal/storagenode/store"
)

// shutdownTimeout bounds how long in-flight RPCs may delay the exit after a
// signal. Past it the node stops hard rather than blocking its supervisor.
const shutdownTimeout = 10 * time.Second

func main() {
	// run returns instead of calling os.Exit itself: os.Exit skips deferred
	// calls, which would leave the fence db unclosed and its wal uncheckpointed.
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "storagenode: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.NewStorageNode()
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}
	if err := cfg.FlagSet.Parse(os.Args[1:]); err != nil {
		return err
	}

	level := slog.LevelInfo
	if cfg.Verbose {
		level = slog.LevelDebug
	}
	log := logger.NewLogger(level)

	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return fmt.Errorf("create data dir %s: %w", cfg.DataDir, err)
	}

	nodeID, err := resolveNodeID(cfg.DataDir, cfg.NodeID)
	if err != nil {
		return fmt.Errorf("resolve node id: %w", err)
	}

	capacity, err := config.ParseChunkSize(cfg.Capacity)
	if err != nil {
		return fmt.Errorf("capacity %q: %w", cfg.Capacity, err)
	}

	// The listen address has to be bindable, the advertised one has to be
	// dialable, and ":9100" is only ever the first of the two.
	advertise := cfg.AdvertiseAddr
	if advertise == "" {
		advertise = cfg.ListenAddr
	}
	if !strings.Contains(advertise, ":") {
		return fmt.Errorf("advertise address %q carries no port", advertise)
	}
	log.Info("starting storage node",
		"node_id", nodeID,
		"listen", cfg.ListenAddr,
		"advertise", advertise,
	)

	fc, err := fence.NewStore(cfg.DataDir)
	if err != nil {
		return fmt.Errorf("open fence store: %w", err)
	}
	defer fc.Close()

	srv := server.NewStorageServer(
		store.NewFileSystemStorage(cfg.DataDir, log),
		fc,
		cfg.MaxRecvBytes,
		server.NodeInfo{
			NodeID:        domain.NodeID(nodeID),
			ListenAddr:    cfg.ListenAddr,
			AdvertiseAddr: advertise,
			DataDir:       cfg.DataDir,
			Capacity:      capacity,
		},
		log,
	)
	if err := srv.Start(); err != nil {
		return fmt.Errorf("start server: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()
	log.Info("shutdown signal received")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := srv.GracefulStop(shutdownCtx); err != nil {
		// Hitting the deadline is the configured policy, not a fault: the
		// node gave every running RPC shutdownTimeout seconds and then cut
		// them off. Exiting non-zero here would have every supervisor report
		// a crashed node whenever one upload outlived the deadline.
		log.Warn("shutting down with pending rpcs cancelled", "err", err)
	}
	return nil
}

// resolveNodeID returns the node's persistent identity: an explicit flag wins,
// otherwise the UUID already stored in <dataDir>/node-id, otherwise a fresh one
// that is written so it survives restarts. Without that file a restarted node
// looks new to the controlplane and its chunks look orphaned.
func resolveNodeID(dataDir, flagValue string) (string, error) {
	if flagValue != "" {
		return flagValue, nil
	}
	path := filepath.Join(dataDir, "node-id")

	data, err := os.ReadFile(path)
	switch {
	case err == nil:
		if id := strings.TrimSpace(string(data)); id != "" {
			return id, nil
		}
	case !os.IsNotExist(err):
		return "", fmt.Errorf("read %s: %w", path, err)
	}

	// v4 is random on purpose: uuid.NewUUID builds a v1, which encodes this
	// machine's MAC address into the id — no business of a storage node's name.
	id, err := uuid.NewRandom()
	if err != nil {
		return "", fmt.Errorf("generate node id: %w", err)
	}
	if err := os.WriteFile(path, []byte(id.String()), 0o644); err != nil {
		return "", fmt.Errorf("write %s: %w", path, err)
	}
	return id.String(), nil
}
