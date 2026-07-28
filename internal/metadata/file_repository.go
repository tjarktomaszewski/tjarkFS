package metadata

import "github.com/tjarktomaszewski/tjarkFS/internal/domain"

type FileRepository interface {
	Save(file domain.File) error
	Get(id [32]byte) (*domain.File, error)
}
