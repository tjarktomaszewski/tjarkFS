package domain

import "time"

type FileID string
type FileState string

const (
	StateUploading FileState = "uploading"
	StateCommitted FileState = "committed"
)

type File struct {
	ID        FileID
	Name      string
	Size      int64
	State     FileState
	Chunks    []ChunkID
	CreatedAt time.Time
}
