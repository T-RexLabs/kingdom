package opencode

import (
	"crypto/sha256"
	"database/sql"
	"fmt"
	"os"

	_ "modernc.org/sqlite"
)

// The provider can emit arbitrarily many tool identities. Keep last-observed
// states on disk and retain only a diagnostic preview in completion metadata.
// Raw events remain in the full invocation diagnostic stream.
type toolStateStorage struct {
	db   *sql.DB
	path string
	err  error
}

func newToolStateStorage() (*toolStateStorage, error) {
	f, err := os.CreateTemp("", "kingdom-provider-tools-*")
	if err != nil {
		return nil, err
	}
	path := f.Name()
	if err := f.Close(); err != nil {
		os.Remove(path)
		return nil, err
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		os.Remove(path)
		return nil, err
	}
	db.SetMaxOpenConns(1)
	s := &toolStateStorage{db: db, path: path}
	// This is disposable stream assembly, not application storage. The complete
	// durable raw stream and lease-fenced settlement retain application evidence.
	_, err = db.Exec(`PRAGMA journal_mode=OFF; PRAGMA synchronous=OFF; PRAGMA cache_size=-256;
		CREATE TABLE tools(id TEXT PRIMARY KEY, name TEXT NOT NULL, status TEXT NOT NULL)`)
	if err != nil {
		s.close()
		return nil, err
	}
	return s, nil
}

func (s *toolStateStorage) observe(id, name, status string) error {
	if s.err != nil {
		return s.err
	}
	_, s.err = s.db.Exec(`INSERT INTO tools(id,name,status) VALUES(?,?,?) ON CONFLICT(id) DO UPDATE SET name=excluded.name,status=excluded.status`, id, name, status)
	return s.err
}

func (s *toolStateStorage) pending() int {
	if s.err != nil {
		return 1
	}
	var n int
	s.err = s.db.QueryRow(`SELECT count(*) FROM tools WHERE status NOT IN ('completed','error')`).Scan(&n)
	if s.err != nil {
		return 1
	}
	return n
}

func (s *toolStateStorage) preview() []InvocationToolObservation {
	if s.err != nil {
		return nil
	}
	rows, err := s.db.Query(`SELECT id,name,status FROM tools ORDER BY id LIMIT 64`)
	if err != nil {
		s.err = err
		return nil
	}
	defer rows.Close()
	var result []InvocationToolObservation
	for rows.Next() {
		var tool InvocationToolObservation
		if err := rows.Scan(&tool.ID, &tool.Name, &tool.Status); err != nil {
			s.err = err
			return nil
		}
		if len(tool.ID) > 128 {
			tool.ID = fmt.Sprintf("provider_tool_%x", sha256.Sum256([]byte(tool.ID)))
		}
		tool.Name = textPreview(tool.Name, 512)
		tool.Status = textPreview(tool.Status, 128)
		result = append(result, tool)
	}
	s.err = rows.Err()
	return result
}

func (s *toolStateStorage) close() { s.db.Close(); os.Remove(s.path) }
