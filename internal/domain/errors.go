package domain

import (
	"errors"
	"io/fs"
)

var (
	ErrFileNotFound = fs.ErrNotExist
	ErrStaleFence   = errors.New("stale fence token")
)
