package logger

import (
	"log/slog"
	"os"

	"github.com/tjarktomaszewski/tjarkFS/internal/config"
)

func NewLogger(cfg *config.Client) *slog.Logger {
	level := slog.LevelInfo
	if cfg.Verbose {
		level = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(logger)

	return logger
}
