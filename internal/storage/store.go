package storage

import (
	"io"
)

type Store interface {
	Put(id string, r io.Reader) error
	Get(id string) (io.Reader, error)
	Delete(id string) error
	Exists(id string) bool
}
