package logger

import (
	"log/slog"
	"os"
)

func NewLogger(level slog.Leveler) *slog.Logger {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(logger)

	return logger
}
