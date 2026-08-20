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

	db, err := sql.Open("sqlite", dbPath)
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
	`)
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("create schema: %w", err)
	}

	return &SQLiteFileRepository{db: db}, nil
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
		return nil, fmt.Errorf("file %s not found in sqlite repository", id)
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

func (r *SQLiteFileRepository) Delete(id domain.FileID) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	_, err := r.db.Exec(`DELETE FROM file_chunks WHERE file_id = ?`, string(id))
	if err != nil {
		return err
	}
	_, err = r.db.Exec(`DELETE FROM files WHERE id = ?`, string(id))
	return err
}

func (r *SQLiteFileRepository) List() ([]domain.File, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	rows, err := r.db.Query(`SELECT id, name, size FROM files`)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("no files found in sqlite repository")
	}
	if err != nil {
		return nil, fmt.Errorf("query file list: %w", err)
	}
	files := make([]domain.File, 0)

	for rows.Next() {
		var f domain.File

		err = rows.Scan(&f.ID, &f.Name, &f.Size)
		if err != nil {
			return nil, fmt.Errorf("error scanning file: %w", err)
		}
		files = append(files, f)
	}

	return files, nil
}

func (r *SQLiteFileRepository) Close() error {
	return r.db.Close()
}
