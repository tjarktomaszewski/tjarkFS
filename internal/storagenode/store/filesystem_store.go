package store

import (
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
)

type FilesystemStore struct {
	rootDir string
	logger  *slog.Logger
	// renameMu makes concurrent writers of the same content-addressed chunk
	// take turns over the final rename. It is not paranoia, it is Windows:
	// os.Rename is MoveFileEx with MOVEFILE_REPLACE_EXISTING there, and that
	// call returns ERROR_ACCESS_DENIED when two of them overlap on the same
	// destination, even though nothing is wrong. POSIX rename(2) has no such
	// window, which is why this never fires on Linux or macOS. Concurrent
	// writers are normal here — a retried PutChunk looks exactly like that —
	// and the lock is held for the rename alone, microseconds next to writing
	// a whole chunk. Retrying instead does not work: the next attempt just
	// runs into the next collision.
	renameMu sync.Mutex
}

func NewFileSystemStorage(root string, logger *slog.Logger) *FilesystemStore {
	if logger == nil {
		logger = slog.Default()
	}
	return &FilesystemStore{
		rootDir: root,
		logger:  logger,
	}
}

// Put writes the chunk read from r. Callers that need to inspect the bytes
// while they are written — the storage node verifies the content hash —
// use PutAtomic.
func (s *FilesystemStore) Put(id string, r io.Reader) error {
	return s.PutAtomic(id, func(w io.Writer) error {
		_, err := io.Copy(w, r)
		return err
	})
}

// PutAtomic writes the chunk from the bytes fill writes to its writer. The
// write happens in a temporary file that is renamed onto the final path only
// after fill returned without error.
func (s *FilesystemStore) PutAtomic(id string, fill func(w io.Writer) error) error {
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

	if err := fill(tmp); err != nil {
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

	if err := s.rename(tmpPath, target); err != nil {
		return fmt.Errorf("rename chunk %s: %w", id, err)
	}

	s.logger.Info(
		"written chunk to disk",
		"target", target,
	)

	return nil
}

// rename moves the finished temporary file onto the chunk's path, where it
// becomes visible under the chunk id.
func (s *FilesystemStore) rename(from, to string) error {
	s.renameMu.Lock()
	defer s.renameMu.Unlock()

	return os.Rename(from, to)
}

func (s *FilesystemStore) Get(id string) (io.ReadCloser, error) {
	file, err := os.Open(s.getPathAndFileName(id))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fs.ErrNotExist
		}
		return nil, err
	}

	return file, nil
}

// Delete removes the chunk and the directories it leaves empty. A chunk that
// is not there comes back as fs.ErrNotExist.
//
// Callers have to close a reader before deleting the chunk. Windows refuses to
// remove a file that still has an open handle, while POSIX unlinks it and lets
// the open handle read to the end — so deleting a chunk that somebody is
// downloading right now works on the platforms the project targets and fails
// on Windows. That is a contract, not an error case.
func (s *FilesystemStore) Delete(id string) error {
	exists, err := s.Exists(id)
	if err != nil {
		return err
	}
	if !exists {
		return fs.ErrNotExist
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


func (s *FilesystemStore) Usage() (used, chunkCount int64, err error) {
	err = filepath.WalkDir(s.rootDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if d.IsDir() {
			return nil
		}
		if len(d.Name()) != 64 {
			return nil
		}
		if !isHexadecimal(d.Name()) {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		used += info.Size()
		chunkCount++
		return nil
	})
	return
}

func isHexadecimal(string string) bool {
	// ^ matches start, [0-9a-fA-F]+ matches one or more hex chars, $ matches end
	return regexp.MustCompile(`^[0-9a-fA-F]+$`).MatchString(string)
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
