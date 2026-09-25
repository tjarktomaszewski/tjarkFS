package client

import (
	"errors"
	"fmt"
	"io/fs"

	"github.com/tjarktomaszewski/tjarkFS/internal/domain"
)

// DeleteService removes a file's metadata and the chunks that are no longer
// referenced by any remaining file.
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

// Delete removes the file's metadata and physically deletes the chunks whose
// reference count dropped to zero. Chunks still referenced by other files
// remain in the store. Deleting a file that does not exist (or was already
// deleted) is a no-op.
func (d *DeleteService) Delete(id domain.FileID) error {
	unreferenced, err := d.repo.Delete(id)
	if err != nil {
		return fmt.Errorf("delete file %s: %w", id, err)
	}

	var chunkErrors []error
	for _, chunkID := range unreferenced {
		if err := d.remover.Remove(chunkID); err != nil && !errors.Is(err, fs.ErrNotExist) {
			chunkErrors = append(chunkErrors, fmt.Errorf("remove chunk %x: %w", chunkID, err))
		}
	}

	if len(chunkErrors) > 0 {
		return fmt.Errorf("delete file %s: %w", id, errors.Join(chunkErrors...))
	}

	return nil
}
