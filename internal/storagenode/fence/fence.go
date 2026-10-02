package fence

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/tjarktomaszewski/tjarkFS/internal/domain"
	_ "modernc.org/sqlite"
)

const dbName = "fences.db"

const schema = `
	CREATE TABLE IF NOT EXISTS fences (
		file_id   TEXT    PRIMARY KEY,
		max_fence INTEGER NOT NULL
	);`

// Store is the per-file fence counter of one storage node.
type Store struct {
	// mu serialises the read-modify-write. A transaction alone is not enough:
	// two concurrent write transactions on one SQLite database are
	// SQLITE_BUSY, and the loser would fail a chunk write for a reason that
	// has nothing to do with fencing.
	mu sync.Mutex
	db *sql.DB
}

// NewStore opens the fence db in dataDir, creating the directory and the
// table if they are not there yet.
func NewStore(dataDir string) (*Store, error) {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("create data dir %s: %w", dataDir, err)
	}

	db, err := sql.Open("sqlite", sqliteDSN(filepath.Join(dataDir, dbName)))
	if err != nil {
		return nil, fmt.Errorf("open fence sidecar: %w", err)
	}
	if _, err := db.Exec(schema); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("create fence schema: %w", err)
	}

	return &Store{db: db}, nil
}

// Close releases the db. It does not delete it: the highest token the
// node has ever seen has to outlive the process, or a node that comes back up
// would happily accept the writer it just fenced off.
func (s *Store) Close() error {
	return s.db.Close()
}

// CheckAndUpdate records token as the newest token seen for fileID and rejects
// a token older than the one already recorded, with domain.ErrStaleFence.
// A token equal to the recorded one is accepted
func (s *Store) CheckAndUpdate(fileID domain.FileID, token int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	// A no-op once the transaction is committed, and the safety net for every
	// path in between: an open transaction holds the write lock, and a
	// leaked one blocks every later write on this database.
	defer func() { _ = tx.Rollback() }()

	highest, err := maxFence(tx, fileID)
	if err != nil {
		return err
	}
	if token < highest {
		return fmt.Errorf("%w: file %s token %d is below %d", domain.ErrStaleFence, fileID, token, highest)
	}

	const raise = `
		INSERT INTO fences (file_id, max_fence) VALUES (?, ?)
		ON CONFLICT (file_id) DO UPDATE SET max_fence = excluded.max_fence`
	if _, err := tx.Exec(raise, string(fileID), token); err != nil {
		return fmt.Errorf("record token %d for file %s: %w", token, fileID, err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit token %d for file %s: %w", token, fileID, err)
	}

	return nil
}

// maxFence reads the highest token recorded for a file. A file without a row
// has never been written, which is the same thing as a maximum of zero: the
// first writer of a file cannot be stale.
func maxFence(tx *sql.Tx, fileID domain.FileID) (int64, error) {
	var highest int64
	err := tx.QueryRow(`SELECT max_fence FROM fences WHERE file_id = ?`, string(fileID)).Scan(&highest)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("read fence for file %s: %w", fileID, err)
	}

	return highest, nil
}

// sqliteDSN decorates the database path with the pragmas that must be in effect
// on every connection SQLite opens — both are per-connection settings in
// SQLite, so a value missing here applies to whichever pooled connection
// happens to serve the next query. busy_timeout makes a writer wait instead of
// failing with SQLITE_BUSY right away, and WAL lets a reader and a writer work
// at the same time.
func sqliteDSN(dbPath string) string {
	const pragmas = "_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)"
	if strings.Contains(dbPath, "?") {
		return dbPath + "&" + pragmas
	}

	return dbPath + "?" + pragmas
}
