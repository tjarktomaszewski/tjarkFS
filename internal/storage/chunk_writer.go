package storage

import "github.com/tjarktomaszewski/tjarkFS/internal/domain"

type ChunkWriter interface {
	Write(stream *domain.ChunkStream) error
}
