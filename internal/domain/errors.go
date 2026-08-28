package domain

import "io/fs"

var (
	ErrFileNotFound = fs.ErrNotExist
)
