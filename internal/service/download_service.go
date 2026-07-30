package service

import (
	"io"

	"github.com/tjarktomaszewski/tjarkFS/internal/domain"
	"github.com/tjarktomaszewski/tjarkFS/internal/metadata"
	"github.com/tjarktomaszewski/tjarkFS/internal/storage"
)

type DownloadService struct {
	reader storage.ChunkReader
	repo   metadata.FileRepository
}

func NewDownloadService(reader storage.ChunkReader, repo metadata.FileRepository) *DownloadService {
	return &DownloadService{
		reader: reader,
		repo:   repo,
	}
}

func (d *DownloadService) Download(id domain.FileID, w io.Writer) error {
	file, err := d.repo.Get(id)

	if err != nil {
		return err
	}

	for _, chunkID := range file.Chunks {
		chunkReader, err := d.reader.Read(chunkID)
		if err != nil {
			return err
		}

		defer chunkReader.Close()

		_, err = io.Copy(w, chunkReader)
		if err != nil {
			return err
		}
	}
	return nil
}
