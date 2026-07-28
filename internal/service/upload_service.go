package service

import (
	"errors"
	"fmt"
	"io"

	"github.com/google/uuid"
	"github.com/tjarktomaszewski/tjarkFS/internal/chunking"
	"github.com/tjarktomaszewski/tjarkFS/internal/domain"
	"github.com/tjarktomaszewski/tjarkFS/internal/metadata"
	"github.com/tjarktomaszewski/tjarkFS/internal/storage"
)

type ChunkerFactory func(r io.Reader, chunkSize int64) (*chunking.Chunker, error)

// TODO: move to designated package
type FileIDGenerator interface {
	Generate() string
}

type UploadService struct {
	newChunker  ChunkerFactory
	chunkSize   int64
	writer      storage.ChunkWriter
	idGenerator FileIDGenerator // TODO: move to designated package
	repository  metadata.FileRepository
}

func NewUploadService(newChunker ChunkerFactory, writer storage.ChunkWriter, chunkSize int64) *UploadService {
	return &UploadService{
		newChunker: newChunker,
		writer:     writer,
		chunkSize:  chunkSize,
	}
}

func (u *UploadService) Upload(r io.Reader) (*domain.File, error) {
	chunker, err := u.newChunker(r, u.chunkSize)
	if err != nil {
		return nil, fmt.Errorf("chunker created: %w", err)
	}

	fileId, err := uuid.NewUUID()
	if err != nil {
		return nil, fmt.Errorf("generate file id: %w", err)
	}
	file := &domain.File{
		ID: fileId.String(),
	}

	for {
		cs, err := chunker.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			// TODO: Later implement cleanup on failed uploads
			return nil, fmt.Errorf("chunk read: %w", err)
		}

		if err := u.writer.Write(cs); err != nil {
			// TODO: Later implement cleanup on failed uploads
			return nil, fmt.Errorf("chunk %x written: %w", cs.Chunk.ID, err)
		}
		file.Chunks = append(file.Chunks, cs.Chunk.ID)
	}

	u.repository.Save(*file)

	return file, nil
}
