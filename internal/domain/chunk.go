package domain

import "github.com/google/uuid"

type Chunk struct {
	ID       uuid.UUID
	Size     int64
	Checksum string
}
