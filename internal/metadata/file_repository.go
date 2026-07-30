package metadata

import "github.com/tjarktomaszewski/tjarkFS/internal/domain"

type FileRepository interface {
	Save(file domain.File) error
	Get(id domain.FileID) (*domain.File, error)
	Delete(id domain.FileID)
}
