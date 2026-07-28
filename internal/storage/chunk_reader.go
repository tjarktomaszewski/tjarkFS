package storage

import (
	"io"

	"github.com/tjarktomaszewski/tjarkFS/internal/domain"
)

type ChunkReader interface {
	Read(id domain.ChunkID) (io.ReadCloser, error)
}
