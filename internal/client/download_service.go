package client

import (
	"fmt"
	"io"

	"github.com/tjarktomaszewski/tjarkFS/internal/domain"
)

type DownloadService struct {
	reader domain.ChunkReader
	repo   domain.FileRepository
}

func NewDownloadService(reader domain.ChunkReader, repo domain.FileRepository) *DownloadService {
	return &DownloadService{
		reader: reader,
		repo:   repo,
	}
}

func (d *DownloadService) Download(id domain.FileID, w io.Writer) error {
	file, err := d.repo.Get(id)
	if err != nil {
		return fmt.Errorf("get file %s: %w", id, err)
	}

	for _, chunkID := range file.Chunks {
		chunkReader, err := d.reader.Read(chunkID)
		if err != nil {
			return fmt.Errorf("read chunk %x: %w", chunkID, err)
		}

		// Close immediately after the copy, not via defer inside the loop:
		// defers accumulate until the function returns, which would keep one
		// file descriptor open per chunk and leak them for large files.
		_, copyErr := io.Copy(w, chunkReader)
		closeErr := chunkReader.Close()
		if copyErr != nil {
			return fmt.Errorf("write chunk %x: %w", chunkID, copyErr)
		}
		if closeErr != nil {
			return fmt.Errorf("close chunk %x: %w", chunkID, closeErr)
		}
	}

	return nil
}
