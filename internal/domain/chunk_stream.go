package domain

import "io"

type ChunkStream struct {
	Chunk  Chunk
	Reader io.Reader
}
