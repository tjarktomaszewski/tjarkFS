package metadata

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/tjarktomaszewski/tjarkFS/internal/domain"
	_ "modernc.org/sqlite"
)

type SQLiteFileRepository struct {
	db *sql.DB
	mu sync.RWMutex
}

func NewSQLiteFileRepository(dbPath string) (*SQLiteFileRepository, error) {
	if dbPath != ":memory:" && !strings.HasPrefix(dbPath, "file:") {
		if dir := filepath.Dir(dbPath); dir != "." {
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return nil, fmt.Errorf("create db dir: %w", err)
			}
		}
	}

	db, err := sql.Open("sqlite", sqliteDSN(dbPath))
	if err != nil {
		return nil, fmt.Errorf("open sqlite db: %w", err)
	}

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS files (
			id   TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			size INTEGER NOT NULL
		);

		CREATE TABLE IF NOT EXISTS file_chunks (
			file_id  TEXT NOT NULL,
			position INTEGER NOT NULL,
			chunk_id TEXT NOT NULL,
			PRIMARY KEY (file_id, position),
			FOREIGN KEY (file_id) REFERENCES files(id) ON DELETE CASCADE
		);

		CREATE TABLE IF NOT EXISTS chunks (
			id        TEXT PRIMARY KEY,
			ref_count INTEGER NOT NULL
		);
	`)
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("create schema: %w", err)
	}

	return &SQLiteFileRepository{db: db}, nil
}

// sqliteDSN decorates the given database path (or DSN) with the pragmas that
// must be in effect on every connection SQLite opens. foreign_keys is a
// per-connection setting in SQLite, so without it the ON DELETE CASCADE
// declaration in the schema has no effect; the driver (modernc.org/sqlite)
// applies each _pragma value when a connection is opened. journal_mode=WAL
// additionally enables concurrent reader access.
func sqliteDSN(dbPath string) string {
	const pragmas = "_pragma=foreign_keys(1)&_pragma=journal_mode(WAL)"
	if strings.Contains(dbPath, "?") {
		return dbPath + "&" + pragmas
	}
	return dbPath + "?" + pragmas
}

func (r *SQLiteFileRepository) Save(file domain.File) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	tx, err := r.db.Begin()
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	_, err = tx.Exec(
		`INSERT INTO files (id, name, size) VALUES (?, ?, ?)
		 ON CONFLICT (id) DO UPDATE SET name = excluded.name, size = excluded.size`,
		string(file.ID), file.Name, file.Size,
	)
	if err != nil {
		return fmt.Errorf("upsert file: %w", err)
	}

	// A re-save may change the file's chunks: release the old references
	// before taking the new ones.
	oldChunkIDs, err := r.chunkIDsForFile(tx, file.ID)
	if err != nil {
		return fmt.Errorf("query old chunks: %w", err)
	}
	for _, chunkID := range oldChunkIDs {
		if err := r.decrementChunkRef(tx, chunkID); err != nil {
			return fmt.Errorf("release old chunk %s: %w", chunkID, err)
		}
	}

	_, err = tx.Exec(`DELETE FROM file_chunks WHERE file_id = ?`, string(file.ID))
	if err != nil {
		return fmt.Errorf("delete old chunks: %w", err)
	}

	if len(file.Chunks) > 0 {
		var query strings.Builder
		query.WriteString("INSERT INTO file_chunks (file_id, position, chunk_id) VALUES ")
		values := make([]any, 0, len(file.Chunks)*3)
		for i, chunkID := range file.Chunks {
			if i > 0 {
				query.WriteString(",")
			}
			query.WriteString("(?, ?, ?)")
			values = append(values, string(file.ID), i, string(chunkID))
		}
		if _, err = tx.Exec(query.String(), values...); err != nil {
			return fmt.Errorf("insert chunks: %w", err)
		}
	}

	for _, chunkID := range distinctChunkIDs(file.Chunks) {
		if err := r.incrementChunkRef(tx, chunkID); err != nil {
			return fmt.Errorf("reference chunk %s: %w", chunkID, err)
		}
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	return nil
}

func (r *SQLiteFileRepository) Get(id domain.FileID) (*domain.File, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var file domain.File
	err := r.db.QueryRow(
		`SELECT id, name, size FROM files WHERE id = ?`, string(id),
	).Scan(&file.ID, &file.Name, &file.Size)
	if err == sql.ErrNoRows {
		return nil, domain.ErrFileNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("query file: %w", err)
	}

	rows, err := r.db.Query(
		`SELECT chunk_id FROM file_chunks WHERE file_id = ? ORDER BY position`,
		string(id),
	)
	if err != nil {
		return nil, fmt.Errorf("query chunks: %w", err)
	}
	defer rows.Close()

	file.Chunks = make([]domain.ChunkID, 0)
	for rows.Next() {
		var chunkID domain.ChunkID
		if err := rows.Scan(&chunkID); err != nil {
			return nil, fmt.Errorf("scan chunk: %w", err)
		}
		file.Chunks = append(file.Chunks, chunkID)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate chunks: %w", err)
	}

	return &file, nil
}

// Delete removes the file's metadata and releases one reference of each of
// its chunks. It returns the IDs of the chunks whose reference count dropped
// to zero, i.e. the chunks that no other file references anymore and that
// the caller can physically remove from the chunk store. Deleting an unknown
// file is a no-op.
func (r *SQLiteFileRepository) Delete(id domain.FileID) ([]domain.ChunkID, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	tx, err := r.db.Begin()
	if err != nil {
		return nil, fmt.Errorf("begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	chunkIDs, err := r.chunkIDsForFile(tx, id)
	if err != nil {
		return nil, fmt.Errorf("query chunks: %w", err)
	}

	_, err = tx.Exec(`DELETE FROM file_chunks WHERE file_id = ?`, string(id))
	if err != nil {
		return nil, fmt.Errorf("delete file chunks: %w", err)
	}

	_, err = tx.Exec(`DELETE FROM files WHERE id = ?`, string(id))
	if err != nil {
		return nil, fmt.Errorf("delete file: %w", err)
	}

	unreferenced := make([]domain.ChunkID, 0)
	for _, chunkID := range chunkIDs {
		if err := r.decrementChunkRef(tx, chunkID); err != nil {
			return nil, fmt.Errorf("release chunk %s: %w", chunkID, err)
		}
		var remaining int
		if err := tx.QueryRow(`SELECT COUNT(*) FROM chunks WHERE id = ?`, string(chunkID)).Scan(&remaining); err != nil {
			return nil, fmt.Errorf("count remaining references of chunk %s: %w", chunkID, err)
		}
		if remaining == 0 {
			unreferenced = append(unreferenced, chunkID)
		}
	}

	if err = tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit transaction: %w", err)
	}

	return unreferenced, nil
}

// chunkIDsForFile returns the distinct chunk IDs referenced by the file.
func (r *SQLiteFileRepository) chunkIDsForFile(tx *sql.Tx, id domain.FileID) ([]domain.ChunkID, error) {
	rows, err := tx.Query(`SELECT DISTINCT chunk_id FROM file_chunks WHERE file_id = ?`, string(id))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	ids := make([]domain.ChunkID, 0)
	for rows.Next() {
		var chunkID domain.ChunkID
		if err := rows.Scan(&chunkID); err != nil {
			return nil, err
		}
		ids = append(ids, chunkID)
	}
	return ids, rows.Err()
}

// incrementChunkRef adds one reference to the chunk registry, creating the
// row on first use.
func (r *SQLiteFileRepository) incrementChunkRef(tx *sql.Tx, id domain.ChunkID) error {
	_, err := tx.Exec(
		`INSERT INTO chunks (id, ref_count) VALUES (?, 1)
		 ON CONFLICT (id) DO UPDATE SET ref_count = ref_count + 1`,
		string(id),
	)
	return err
}

// decrementChunkRef releases one reference of the chunk; rows that drop to
// zero are removed from the registry.
func (r *SQLiteFileRepository) decrementChunkRef(tx *sql.Tx, id domain.ChunkID) error {
	if _, err := tx.Exec(
		`UPDATE chunks SET ref_count = ref_count - 1 WHERE id = ? AND ref_count > 0`,
		string(id),
	); err != nil {
		return err
	}
	_, err := tx.Exec(`DELETE FROM chunks WHERE id = ? AND ref_count = 0`, string(id))
	return err
}

func (r *SQLiteFileRepository) List() ([]domain.File, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	rows, err := r.db.Query(`SELECT id, name, size FROM files`)
	if err != nil {
		return nil, fmt.Errorf("query file list: %w", err)
	}
	defer rows.Close()

	files := make([]domain.File, 0)

	for rows.Next() {
		var f domain.File

		if err := rows.Scan(&f.ID, &f.Name, &f.Size); err != nil {
			return nil, fmt.Errorf("scan file: %w", err)
		}
		files = append(files, f)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate file list: %w", err)
	}

	return files, nil
}

func (r *SQLiteFileRepository) Close() error {
	return r.db.Close()
}
