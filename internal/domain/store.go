package domain

import (
	"io"
)

type Store interface {
	Put(id string, r io.Reader) error
	// PutAtomic writes the chunk identified by id from the bytes fill writes
	// to its writer. The chunk only becomes visible under id once fill
	// returned without error.
	PutAtomic(id string, fill func(w io.Writer) error) error
	Get(id string) (io.ReadCloser, error)
	Delete(id string) error
	Exists(id string) (bool, error)
}
