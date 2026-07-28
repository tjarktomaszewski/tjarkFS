package chunking

import (
	"bytes"
	"errors"
	"io"

	"github.com/tjarktomaszewski/tjarkFS/internal/domain"
	"github.com/tjarktomaszewski/tjarkFS/internal/storage"
)

type Chunker struct {
	chunkSize int64
	reader    io.Reader
}

func NewChunker(r io.Reader, chunkSize int64) (*Chunker, error) {
	if chunkSize <= 0 {
		return nil, errors.New("invalid chunk size")
	}

	return &Chunker{
		reader:    r,
		chunkSize: chunkSize,
	}, nil
}

func (c *Chunker) Next() (*domain.ChunkStream, error) {
	buf := make([]byte, c.chunkSize)
	n, err := io.ReadFull(c.reader, buf)

	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, io.EOF
		}
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, err
		}
	}

	if n == 0 {
		return nil, io.EOF
	}
	buf = buf[:n]
	sum := storage.SHA256(buf)
	chunk := domain.Chunk{
		ID:   sum,
		Size: int64(n),
	}

	return &domain.ChunkStream{
		Chunk: chunk,
		Reader: bytes.NewReader(
			buf,
		),
	}, nil
}
