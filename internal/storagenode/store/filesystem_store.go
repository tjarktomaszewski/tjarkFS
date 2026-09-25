package store

import (
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"
)

var (
	FileNotFoundErr = fs.ErrNotExist
)

type FilesystemStore struct {
	rootDir string
}

func NewFileSystemStorage(root string) *FilesystemStore {
	return &FilesystemStore{
		rootDir: root,
	}
}

func (s *FilesystemStore) Put(id string, r io.Reader) error {
	target := s.getPathAndFileName(id)
	dir := filepath.Dir(target)

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create directory %s: %w", dir, err)
	}

	// Write to a temporary file in the target directory first, then rename
	// it atomically onto the final path. This guarantees the target path
	// never points at a partially written chunk and protects against
	// concurrent writers of the same (content-addressed) chunk ID.
	tmp, err := os.CreateTemp(dir, filepath.Base(target)+".tmp-*")
	if err != nil {
		return fmt.Errorf("create temp file for %s: %w", target, err)
	}
	tmpPath := tmp.Name()
	// Removes the temp file on any error path; a no-op after a successful rename.
	defer os.Remove(tmpPath)

	n, err := io.Copy(tmp, r)
	if err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write chunk %s: %w", id, err)
	}

	// On *os.File, write errors (e.g. a full disk) often only surface
	// during Sync or Close, so both must be checked explicitly.
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("sync chunk %s: %w", id, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close chunk %s: %w", id, err)
	}

	if err := os.Rename(tmpPath, target); err != nil {
		return fmt.Errorf("rename chunk %s: %w", id, err)
	}

	log.Printf(
		"written (%d) bytes to disk: %s",
		n,
		target,
	)

	return nil
}

func (s *FilesystemStore) Get(id string) (io.ReadCloser, error) {
	file, err := os.Open(s.getPathAndFileName(id))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, FileNotFoundErr
		}
		return nil, err
	}

	return file, nil
}

func (s *FilesystemStore) Delete(id string) error {
	exists, err := s.Exists(id)
	if err != nil {
		return err
	}
	if !exists {
		return FileNotFoundErr
	}

	path := s.getPathAndFileName(id)
	if err := os.Remove(path); err != nil {
		return fmt.Errorf("remove %s: %w", path, err)
	}

	return s.removeEmptyDirRecursive(filepath.Dir(path))
}

func (s *FilesystemStore) Exists(id string) (bool, error) {
	_, err := os.Stat(s.getPathAndFileName(id))
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}

	return false, err
}

// Creates file path based on given id
func (s *FilesystemStore) chunkPath(id string) string {
	blockSize := 4
	sliceLen := len(id) / blockSize
	paths := make([]string, sliceLen)

	for i := 0; i < sliceLen; i++ {
		from, to := i*blockSize, (i*blockSize)+blockSize
		paths[i] = id[from:to]
	}
	return strings.Join(paths, "/")
}

// Returns path + filename
func (s *FilesystemStore) getPathAndFileName(id string) string {
	return filepath.Join(
		s.rootDir,
		s.chunkPath(id),
		id,
	)
}

// Removes the given directory and all empty parent directories, stopping
// (without removing) at rootDir so the store root itself is never deleted.
func (s *FilesystemStore) removeEmptyDirRecursive(dir string) error {
	for dir != s.rootDir {
		entries, err := os.ReadDir(dir)
		if err != nil {
			return fmt.Errorf("failed to read directory %s: %w", dir, err)
		}
		if len(entries) > 0 {
			break
		}

		if err := os.Remove(dir); err != nil {
			return fmt.Errorf("failed to remove empty directory %s: %w", dir, err)
		}

		dir = filepath.Dir(dir)
	}

	return nil
}
