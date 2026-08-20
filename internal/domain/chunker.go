package domain

import "io"

// Chunker liest eine Datei abschnittsweise aus. Jede Iteration liefert den
// nächsten Chunk samt eines Readers für dessen Bytes oder io.EOF, sobald die
// Quelle erschöpft ist.
type Chunker interface {
	Next() (*ChunkStream, error)
}

// ChunkerFactory erstellt einen neuen Chunker für den gegebenen Reader und die
// gewünschte Chunk-Größe. Sie ist als Port definiert, damit die
// Applikationsschicht (z. B. UploadService) unabhängig von einer konkreten
// Implementierung bleibt.
type ChunkerFactory func(r io.Reader, chunkSize int64) (Chunker, error)
