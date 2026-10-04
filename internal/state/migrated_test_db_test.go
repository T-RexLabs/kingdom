package state

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"kingdom/internal/dbmigrations"
	"kingdom/internal/testutil/sqlitefixture"
)

// Behavior tests restore an empty canonical database into their own connection.
// Migration tests use openTestDB and applyMigration to exercise real migrations.
var migratedTestDatabase = sqlitefixture.New(func(ctx context.Context, db *sql.DB) error {
	names, err := dbmigrations.List()
	if err != nil {
		return fmt.Errorf("list canonical migrations: %w", err)
	}
	sort.Strings(names)
	migrations := make([]SQLMigration, 0, len(names))
	for _, name := range names {
		if !strings.HasSuffix(name, ".up.sql") {
			continue
		}
		raw, err := dbmigrations.Read(name)
		if err != nil {
			return fmt.Errorf("read canonical migration %s: %w", name, err)
		}
		migrations = append(migrations, SQLMigration{Name: name, SQL: raw})
	}
	return ApplySQLMigrations(ctx, db, migrations, time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC))
})

func openMigratedTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := migratedTestDatabase.Open(t.Context())
	if err != nil {
		t.Fatalf("restore canonical test database: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestMigratedTestDatabaseMatchesCanonicalSchemaAndHistory(t *testing.T) {
	t.Parallel()
	direct := openTestDB(t)
	applyMigration(t, direct, "0001_initial_schema.up.sql")
	restored := openMigratedTestDB(t)
	for _, query := range []string{
		`SELECT type, name, tbl_name, COALESCE(sql, '') FROM sqlite_schema ORDER BY type, name`,
		`SELECT name, sha256, applied_at FROM schema_migrations ORDER BY name`,
	} {
		load := func(db *sql.DB) [][]string {
			t.Helper()
			rows, err := db.QueryContext(t.Context(), query)
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			columns, err := rows.Columns()
			if err != nil {
				t.Fatal(err)
			}
			var values [][]string
			for rows.Next() {
				row := make([]string, len(columns))
				pointers := make([]any, len(columns))
				for i := range row {
					pointers[i] = &row[i]
				}
				if err := rows.Scan(pointers...); err != nil {
					t.Fatal(err)
				}
				values = append(values, row)
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			return values
		}
		if want, got := load(direct), load(restored); !reflect.DeepEqual(want, got) {
			t.Fatalf("restored database differs from canonical migration result for %s", query)
		}
	}
}

func TestMigratedTestDatabaseKeepsWritesAndSchemaChangesIsolated(t *testing.T) {
	t.Parallel()
	left := openMigratedTestDB(t)
	right := openMigratedTestDB(t)
	mustExec(t, left, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('fixture_only', 'Fixture', 0, '2026-03-01T12:00:00Z')`)
	mustExec(t, left, `CREATE TABLE fixture_only (value TEXT)`)
	mustExec(t, left, `CREATE TRIGGER reject_fixture_role BEFORE INSERT ON roles BEGIN SELECT RAISE(ABORT, 'fixture-only failure'); END`)
	for _, db := range []*sql.DB{right, openMigratedTestDB(t)} {
		if tableExists(t, db, "fixture_only") {
			t.Fatal("test schema change escaped its database")
		}
		var count int
		if err := db.QueryRow(`SELECT COUNT(*) FROM roles WHERE role_id = 'fixture_only'`).Scan(&count); err != nil || count != 0 {
			t.Fatalf("test write escaped its database: count=%d err=%v", count, err)
		}
		mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('fixture_only', 'Fixture', 0, '2026-03-01T12:00:00Z')`)
		if _, err := db.Exec(`INSERT INTO agents (agent_id, role_id, status, spawn_depth, created_at, updated_at) VALUES ('invalid_fixture_agent', 'missing_role', 'idle', 0, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`); err == nil {
			t.Fatal("restored database did not enforce foreign keys")
		}
	}
}
