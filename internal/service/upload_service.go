package service

import (
	"errors"
	"fmt"
	"io"

	"github.com/tjarktomaszewski/tjarkFS/internal/chunking"
	"github.com/tjarktomaszewski/tjarkFS/internal/domain"
	"github.com/tjarktomaszewski/tjarkFS/internal/identity"
	"github.com/tjarktomaszewski/tjarkFS/internal/metadata"
	"github.com/tjarktomaszewski/tjarkFS/internal/storage"
)

type ChunkerFactory func(r io.Reader, chunkSize int64) (*chunking.Chunker, error)

type UploadService struct {
	newChunker ChunkerFactory
	chunkSize  int64

	writer     storage.ChunkWriter
	repository metadata.FileRepository

	idGenerator identity.FileIDGenerator
}

func NewUploadService(
	newChunker ChunkerFactory,
	writer storage.ChunkWriter,
	repository metadata.FileRepository,
	idGenerator identity.FileIDGenerator,
	chunkSize int64,
) *UploadService {

	return &UploadService{
		newChunker:  newChunker,
		chunkSize:   chunkSize,
		writer:      writer,
		repository:  repository,
		idGenerator: idGenerator,
	}
}

func (u *UploadService) Upload(r io.Reader, name string) (*domain.File, error) {
	chunker, err := u.newChunker(r, u.chunkSize)
	if err != nil {
		return nil, fmt.Errorf("create chunker: %w", err)
	}

	fileId, err := u.idGenerator.Generate()

	if err != nil {
		return nil, err
	}

	file := domain.File{
		ID:   fileId,
		Name: name,
	}

	for {
		stream, err := chunker.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			// TODO: Later implement cleanup on failed uploads
			return nil, fmt.Errorf("chunk read: %w", err)
		}

		if err := u.writer.Write(stream); err != nil {
			// TODO: Later implement cleanup on failed uploads
			return nil, fmt.Errorf("chunk %x written: %w", stream.Chunk.ID, err)
		}
		file.Chunks = append(file.Chunks, stream.Chunk.ID)
		file.Size += stream.Chunk.Size
	}

	if err := u.repository.Save(file); err != nil {
		return nil, fmt.Errorf(
			"save metadata: %w",
			err,
		)
	}

	return &file, nil
}
