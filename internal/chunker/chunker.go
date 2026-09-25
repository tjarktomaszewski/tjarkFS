package chunker

import (
	"bytes"
	"errors"
	"io"

	"github.com/tjarktomaszewski/tjarkFS/internal/domain"
)

type Chunker struct {
	chunkSize int64
	reader    io.Reader
	// buf is reused for every chunk to avoid allocating a fresh
	// chunkSize-sized buffer per chunk.
	buf []byte
}

func NewChunker(r io.Reader, chunkSize int64) (*Chunker, error) {
	if chunkSize <= 0 {
		return nil, errors.New("invalid chunk size")
	}

	return &Chunker{
		reader:    r,
		chunkSize: chunkSize,
		buf:       make([]byte, chunkSize),
	}, nil
}

// Next reads the next chunk from the underlying reader.
//
// The returned ChunkStream's Reader aliases the chunker's internal buffer:
// it must be fully consumed before Next is called again.
func (c *Chunker) Next() (*domain.ChunkStream, error) {
	n, err := io.ReadFull(c.reader, c.buf)

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

	buf := c.buf[:n]
	sum := SHA256(buf)
	chunk := domain.Chunk{
		ID:   domain.ChunkID(sum),
		Size: int64(n),
	}

	return &domain.ChunkStream{
		Chunk:  chunk,
		Reader: bytes.NewReader(buf),
	}, nil
}
