package domain

import (
	"io"
)

type ChunkReader interface {
	Read(id ChunkID) (io.ReadCloser, error)
}
