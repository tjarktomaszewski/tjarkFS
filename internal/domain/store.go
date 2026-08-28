package domain

import (
	"io"
)

type Store interface {
	Put(id string, r io.Reader) error
	Get(id string) (io.ReadCloser, error)
	Delete(id string) error
	Exists(id string) (bool, error)
}
