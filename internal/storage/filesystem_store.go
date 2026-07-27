package storage

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
)

var (
	FileNotFoundErr = errors.New("file not found")
)

type FilesystemStore struct {
	rootDir string
}

func (s *FilesystemStore) Put(id string, r io.Reader) error {
	if err := os.MkdirAll(s.chunkPath(id), os.ModePerm); err != nil {
		return err
	}
	pathAndFilename := s.getPathAndFileName(id)

	f, err := os.Create(pathAndFilename)
	defer f.Close()
	if err != nil {
		return err
	}

	n, err := io.Copy(f, r)
	if err != nil {
		return err
	}

	log.Printf("written (%d) bytes to disk: %s", n, pathAndFilename)

	return nil
}

func (s *FilesystemStore) Get(id string) (io.Reader, error) {
	if !s.Exists(id) {
		return nil, FileNotFoundErr
	}
	file, err := os.Open(s.getPathAndFileName(id))
	defer file.Close()
	if err != nil {
		return nil, err
	}
	buf := new(bytes.Buffer)
	n, err := io.Copy(buf, file)

	log.Printf("read (%d) bytes from disk: %s", n, s.getPathAndFileName(id))

	return buf, err
}

func (s *FilesystemStore) Delete(id string) error {
	if !s.Exists(id) {
		return FileNotFoundErr
	}
	err := os.Remove(s.getPathAndFileName(id))
	if err != nil {
		log.Print(err.Error())
		return err
	}
	currentDir := filepath.Dir(s.getPathAndFileName(id))

	err = s.removeEmptyDirRecursive(currentDir)
	if err != nil {
		return nil
	}

	return nil
}

func (s *FilesystemStore) Exists(id string) bool {
	_, err := os.Stat(s.getPathAndFileName(id))

	if err == nil {
		return true
	}

	return !errors.Is(err, os.ErrNotExist)
}

/**
 * Creates file path based on given id
 */
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

/**
 * Returns path + filename
 */
func (s *FilesystemStore) getPathAndFileName(id string) string {
	return s.chunkPath(id) + "/" + id
}

/**
 * Removes given directory and all empty parent directories
 */
func (s *FilesystemStore) removeEmptyDirRecursive(dir string) error {
	for {
		entries, err := os.ReadDir(dir)
		if err != nil {
			return fmt.Errorf("failed to read directory %s: %w", dir, err)
		}
		if len(entries) > 0 {
			break
		}

		if err = os.Remove(dir); err != nil {
			return fmt.Errorf("failed to remove empty directory %s: %s", dir, err)
		}

		nextDir := filepath.Dir(dir)
		if nextDir == dir {
			break
		}
		dir = nextDir
	}
	return nil
}
