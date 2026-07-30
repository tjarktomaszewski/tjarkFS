package domain

type ChunkWriter interface {
	Write(stream *ChunkStream) error
}
