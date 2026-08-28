package identity

import (
	"log/slog"

	"github.com/google/uuid"
	"github.com/tjarktomaszewski/tjarkFS/internal/domain"
)

// UUIDFileIDGenerator generates random UUIDv4 file IDs.
type UUIDFileIDGenerator struct {
	logger *slog.Logger
}

// NewUUIDFileIDGenerator returns a UUIDFileIDGenerator that logs each
// generated ID using logger. A nil logger falls back to slog.Default().
func NewUUIDFileIDGenerator(logger *slog.Logger) *UUIDFileIDGenerator {
	if logger == nil {
		logger = slog.Default()
	}
	return &UUIDFileIDGenerator{logger: logger}
}

func (g *UUIDFileIDGenerator) Generate() (domain.FileID, error) {
	id, err := uuid.NewUUID()
	if err != nil {
		return domain.FileID(""), err
	}

	g.logger.Info("generated file id", "id", id.String())

	return domain.FileID(id.String()), nil
}
