package sqlitefixture

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
)

const fixtureDDL = `
CREATE TABLE parents (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL);
CREATE TABLE children (id INTEGER PRIMARY KEY, parent_id INTEGER NOT NULL REFERENCES parents(id));
CREATE TABLE audit (parent_id INTEGER NOT NULL);
CREATE TRIGGER parent_insert AFTER INSERT ON parents BEGIN INSERT INTO audit VALUES (NEW.id); END;
CREATE VIRTUAL TABLE documents USING fts5(body);
INSERT INTO parents(name) VALUES ('baseline');
INSERT INTO children VALUES (1, 1);
INSERT INTO documents(body) VALUES ('template searchable content');
`

func initializeFixture(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, fixtureDDL)
	return err
}

func TestTemplatePreservesDatabaseBehaviorAndIsolation(t *testing.T) {
	var calls atomic.Int32
	var source *sql.DB
	template := New(func(ctx context.Context, db *sql.DB) error {
		calls.Add(1)
		source = db
		return initializeFixture(ctx, db)
	})
	first := mustOpen(t, template)
	second := mustOpen(t, template)
	if calls.Load() != 1 {
		t.Fatalf("initializer calls=%d, want 1", calls.Load())
	}
	if err := source.PingContext(t.Context()); err == nil {
		t.Fatal("source database remains open")
	}
	for _, db := range []*sql.DB{first, second} {
		if db.Stats().MaxOpenConnections != 1 {
			t.Fatal("fixture permits more than one database connection")
		}
		assertCount(t, db, "PRAGMA foreign_keys", 1)
		assertCount(t, db, "SELECT COUNT(*) FROM documents WHERE documents MATCH 'searchable'", 1)
		if _, err := db.Exec("INSERT INTO children VALUES (2, 999)"); err == nil {
			t.Fatal("restored database accepts an invalid foreign key")
		}
	}
	result, err := first.Exec("INSERT INTO parents(name) VALUES ('first only')")
	if err != nil {
		t.Fatal(err)
	}
	id, err := result.LastInsertId()
	if err != nil || id != 2 {
		t.Fatalf("restored AUTOINCREMENT: id=%d err=%v", id, err)
	}
	assertCount(t, first, "SELECT COUNT(*) FROM audit", 2)
	assertCount(t, second, "SELECT COUNT(*) FROM parents", 1)
	assertCount(t, second, "SELECT COUNT(*) FROM audit", 1)
	if _, err := first.Exec("DELETE FROM documents"); err != nil {
		t.Fatal(err)
	}
	assertCount(t, second, "SELECT COUNT(*) FROM documents WHERE documents MATCH 'searchable'", 1)
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	third := mustOpen(t, template)
	assertCount(t, third, "SELECT COUNT(*) FROM parents", 1)
}

func TestTemplateRetainsInitializerError(t *testing.T) {
	sentinel := errors.New("initializer failed")
	var calls atomic.Int32
	var source *sql.DB
	template := New(func(_ context.Context, db *sql.DB) error {
		calls.Add(1)
		source = db
		return sentinel
	})
	var initialError error
	for range 2 {
		db, err := template.Open(t.Context())
		if db != nil || !errors.Is(err, sentinel) {
			t.Fatalf("initializer failure: db=%v err=%v", db, err)
		}
		if initialError == nil {
			initialError = err
		} else if err != initialError {
			t.Fatal("initialization error was not retained")
		}
	}
	if calls.Load() != 1 || source.PingContext(t.Context()) == nil {
		t.Fatal("failed initialization was repeated or its source was not closed")
	}
}

func TestTemplateConcurrentRestorationDoesNotShareWrites(t *testing.T) {
	var calls atomic.Int32
	template := New(func(ctx context.Context, db *sql.DB) error {
		calls.Add(1)
		return initializeFixture(ctx, db)
	})
	const workers = 8
	var ready sync.WaitGroup
	ready.Add(workers)
	release := make(chan struct{})
	errorsFound := make(chan error, workers)
	for i := range workers {
		go func() {
			db, err := template.Open(t.Context())
			if err == nil {
				_, err = db.ExecContext(t.Context(), "INSERT INTO parents(name) VALUES (?)", fmt.Sprintf("worker %d", i))
			}
			ready.Done()
			<-release
			if db != nil {
				if err == nil {
					var count int
					err = db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM parents").Scan(&count)
					if err == nil && count != 2 {
						err = fmt.Errorf("worker %d sees %d parents, want 2", i, count)
					}
				}
				err = errors.Join(err, db.Close())
			}
			errorsFound <- err
		}()
	}
	ready.Wait()
	close(release)
	for range workers {
		if err := <-errorsFound; err != nil {
			t.Error(err)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("concurrent initializer calls=%d, want 1", calls.Load())
	}
}

func TestCancelledRestoreDoesNotDamageInitializedTemplate(t *testing.T) {
	template := New(initializeFixture)
	mustOpen(t, template)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if db, err := template.Open(ctx); db != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled restore: db=%v err=%v", db, err)
	}
	assertCount(t, mustOpen(t, template), "SELECT COUNT(*) FROM parents", 1)
}

func TestTemplateRequiresInitializer(t *testing.T) {
	for _, template := range []*Template{nil, New(nil)} {
		if db, err := template.Open(t.Context()); db != nil || err == nil {
			t.Fatalf("missing initializer: db=%v err=%v", db, err)
		}
	}
}

func TestTemplateRemovesTemporaryImagesBeforeReturning(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "scratch images")
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("TMPDIR", directory)
	template := New(initializeFixture)
	for range 3 {
		assertCount(t, mustOpen(t, template), "SELECT COUNT(*) FROM parents", 1)
		entries, err := os.ReadDir(directory)
		if err != nil || len(entries) != 0 {
			t.Fatalf("temporary restore images remain: entries=%v err=%v", entries, err)
		}
	}
}

func TestRestoreFailureDoesNotPoisonTemplate(t *testing.T) {
	var calls atomic.Int32
	template := New(func(ctx context.Context, db *sql.DB) error {
		calls.Add(1)
		return initializeFixture(ctx, db)
	})
	directory := t.TempDir()
	t.Setenv("TMPDIR", filepath.Join(directory, "does not exist"))
	if db, err := template.Open(t.Context()); db != nil || err == nil {
		t.Fatalf("invalid image directory: db=%v err=%v", db, err)
	}
	t.Setenv("TMPDIR", directory)
	assertCount(t, mustOpen(t, template), "SELECT COUNT(*) FROM parents", 1)
	if calls.Load() != 1 {
		t.Fatalf("restore failure repeated initialization: calls=%d", calls.Load())
	}
}

func TestTemplateMatchesCanonicalDDL(t *testing.T) {
	direct, err := openDatabase(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { direct.Close() })
	if err := initializeCanonicalMigrations(t.Context(), direct); err != nil {
		t.Fatal(err)
	}
	restored := mustOpen(t, NewMigrated())
	if got, want := databaseSchema(t, restored), databaseSchema(t, direct); !reflect.DeepEqual(got, want) {
		t.Fatal("restored schema differs from direct canonical DDL execution")
	}
	assertCount(t, restored, "SELECT COUNT(*) FROM sqlite_schema WHERE name = 'schema_migrations'", 0)
}

func databaseSchema(t *testing.T, db *sql.DB) []string {
	t.Helper()
	rows, err := db.Query("SELECT type || ':' || name || ':' || COALESCE(sql, '') FROM sqlite_schema ORDER BY type, name")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var schema []string
	for rows.Next() {
		var entry string
		if err := rows.Scan(&entry); err != nil {
			t.Fatal(err)
		}
		schema = append(schema, entry)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return schema
}

func mustOpen(t *testing.T, template *Template) *sql.DB {
	t.Helper()
	db, err := template.Open(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	return db
}

func assertCount(t *testing.T, db *sql.DB, query string, want int) {
	t.Helper()
	var got int
	if err := db.QueryRow(query).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("%s: got %d, want %d", query, got, want)
	}
}

func BenchmarkCanonicalDDLFixture(b *testing.B) {
	b.ReportAllocs()
	for range b.N {
		db, err := openDatabase(b.Context())
		if err != nil {
			b.Fatal(err)
		}
		initErr := initializeCanonicalMigrations(b.Context(), db)
		if err := errors.Join(initErr, db.Close()); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCanonicalTemplateRestore(b *testing.B) {
	template := NewMigrated()
	db, err := template.Open(b.Context())
	if err != nil {
		b.Fatal(err)
	}
	if err := db.Close(); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		db, err := template.Open(b.Context())
		if err != nil {
			b.Fatal(err)
		}
		if err := db.Close(); err != nil {
			b.Fatal(err)
		}
	}
}
