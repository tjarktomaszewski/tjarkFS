package service

import (
	"errors"
	"fmt"
	"io/fs"

	"github.com/tjarktomaszewski/tjarkFS/internal/domain"
)

type DeleteService struct {
	repo    domain.FileRepository
	remover domain.ChunkRemover
}

func NewDeleteService(repo domain.FileRepository, remover domain.ChunkRemover) *DeleteService {
	return &DeleteService{
		repo:    repo,
		remover: remover,
	}
}

func (d *DeleteService) Delete(id domain.FileID) error {
	file, err := d.repo.Get(id)
	if err != nil {
		return fmt.Errorf("get file: %w", err)
	}

	var chunkErrors []error
	for _, chunkID := range file.Chunks {
		if err := d.remover.Remove(chunkID); err != nil && !errors.Is(err, fs.ErrNotExist) {
			chunkErrors = append(chunkErrors, fmt.Errorf("remove chunk %x: %w", chunkID, err))
		}
	}

	if err := d.repo.Delete(id); err != nil {
		return fmt.Errorf("delete metadata: %w", err)
	}

	if len(chunkErrors) > 0 {
		return fmt.Errorf("delete file %s: %w", id, errors.Join(chunkErrors...))
	}

	return nil
}
