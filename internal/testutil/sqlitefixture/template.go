// Package sqlitefixture builds isolated test databases from a migrated template.
package sqlitefixture

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"

	"kingdom/internal/dbmigrations"

	"modernc.org/sqlite"
)

// Template retains only immutable serialized database bytes. Each Open restores
// those bytes into a separate in-memory database and connection.
type Template struct {
	initialize func(context.Context, *sql.DB) error
	once       sync.Once
	data       []byte
	err        error
}

// New records the initializer. The first Open executes it once, using that
// caller's context. Initialization errors are retained for subsequent callers.
// The initializer must return errors; it must not call testing.Fatal or Goexit.
func New(initialize func(context.Context, *sql.DB) error) *Template {
	return &Template{initialize: initialize}
}

// NewMigrated builds the canonical forward schema used by business-test
// fixtures. It executes the embedded SQL directly, without migration-history
// records. Tests of the migration runner must execute that runner themselves.
func NewMigrated() *Template {
	return New(initializeCanonicalMigrations)
}

func initializeCanonicalMigrations(ctx context.Context, db *sql.DB) error {
	names, err := dbmigrations.List()
	if err != nil {
		return err
	}
	for _, name := range names {
		if !strings.HasSuffix(name, ".up.sql") {
			continue
		}
		raw, err := dbmigrations.Read(name)
		if err != nil {
			return err
		}
		if _, err := db.ExecContext(ctx, string(raw)); err != nil {
			return fmt.Errorf("apply SQLite fixture migration %s: %w", name, err)
		}
	}
	return nil
}

// Open returns a database owned by the caller, who must close it. Foreign keys
// are enabled on its single connection; pragmas are not part of serialization.
func (t *Template) Open(ctx context.Context) (*sql.DB, error) {
	if t == nil || t.initialize == nil {
		return nil, fmt.Errorf("SQLite fixture initializer is required")
	}
	t.once.Do(func() {
		t.data, t.err = buildTemplate(ctx, t.initialize)
	})
	if t.err != nil {
		return nil, t.err
	}
	db, err := openDatabase(ctx)
	if err != nil {
		return nil, err
	}
	if err := restoreTemplate(ctx, db, t.data); err != nil {
		return nil, errors.Join(fmt.Errorf("restore SQLite fixture: %w", err), db.Close())
	}
	// Explicitly restore this connection setting after replacing the database.
	if _, err := db.ExecContext(ctx, "PRAGMA foreign_keys = ON"); err != nil {
		return nil, errors.Join(fmt.Errorf("enable SQLite fixture foreign keys: %w", err), db.Close())
	}
	return db, nil
}

func restoreTemplate(ctx context.Context, db *sql.DB, data []byte) (err error) {
	// Pinned modernc v1.46.1 Deserialize produced a native fault on close after
	// restored-database writes in isolation tests. Use SQLite's backup API instead.
	// The private image is removed before Open returns.
	image, err := os.CreateTemp("", "kingdom-sqlite-fixture-*.db")
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, os.Remove(image.Name())) }()
	_, writeErr := image.Write(data)
	if err := errors.Join(writeErr, image.Close()); err != nil {
		return err
	}
	return useConnection(ctx, db, func(driver any) error {
		restorer, ok := driver.(interface {
			NewRestore(string) (*sqlite.Backup, error)
		})
		if !ok {
			return fmt.Errorf("SQLite fixture driver does not support NewRestore")
		}
		backup, err := restorer.NewRestore(image.Name())
		if err != nil {
			return err
		}
		more, stepErr := backup.Step(-1)
		if more && stepErr == nil {
			stepErr = fmt.Errorf("SQLite fixture backup did not finish")
		}
		return errors.Join(stepErr, backup.Finish())
	})
}

func buildTemplate(ctx context.Context, initialize func(context.Context, *sql.DB) error) (data []byte, err error) {
	db, err := openDatabase(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, db.Close()) }()
	if err := initialize(ctx, db); err != nil {
		return nil, fmt.Errorf("initialize SQLite fixture: %w", err)
	}
	if err := useConnection(ctx, db, func(driver any) error {
		serializer, ok := driver.(interface{ Serialize() ([]byte, error) })
		if !ok {
			return fmt.Errorf("SQLite fixture driver does not support Serialize")
		}
		var serializeErr error
		data, serializeErr = serializer.Serialize()
		return serializeErr
	}); err != nil {
		return nil, fmt.Errorf("serialize SQLite fixture: %w", err)
	}
	return data, nil
}

func openDatabase(ctx context.Context) (*sql.DB, error) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		return nil, fmt.Errorf("open SQLite fixture: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if _, err := db.ExecContext(ctx, "PRAGMA foreign_keys = ON"); err != nil {
		return nil, errors.Join(fmt.Errorf("enable SQLite fixture foreign keys: %w", err), db.Close())
	}
	return db, nil
}

func useConnection(ctx context.Context, db *sql.DB, use func(any) error) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	return errors.Join(conn.Raw(use), conn.Close())
}
