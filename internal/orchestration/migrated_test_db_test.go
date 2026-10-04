package orchestration

import (
	"database/sql"
	"reflect"
	"testing"

	"kingdom/internal/testutil/sqlitefixture"
)

// Each behavior test owns its database. The template uses the same canonical
// migration sequence as the raw applyMigration helper, once per test process.
var migratedTestDatabase = sqlitefixture.NewMigrated()

func openMigratedTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := migratedTestDatabase.Open(t.Context())
	if err != nil {
		t.Fatalf("restore canonical test database: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestMigratedTestDatabaseMatchesRawCanonicalMigration(t *testing.T) {
	t.Parallel()
	raw := openTestDB(t)
	applyMigration(t, raw)
	restored := openMigratedTestDB(t)
	loadSchema := func(db *sql.DB) [][4]string {
		t.Helper()
		rows, err := db.QueryContext(t.Context(), `SELECT type, name, tbl_name, COALESCE(sql, '') FROM sqlite_schema ORDER BY type, name`)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var schema [][4]string
		for rows.Next() {
			var row [4]string
			if err := rows.Scan(&row[0], &row[1], &row[2], &row[3]); err != nil {
				t.Fatal(err)
			}
			schema = append(schema, row)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		return schema
	}
	if want, got := loadSchema(raw), loadSchema(restored); !reflect.DeepEqual(want, got) {
		t.Fatal("restored schema differs from the raw canonical migration result")
	}
	// Seed writes belong only to this test's restored connection.
	seedMinimumGraph(t, restored)
	other := openMigratedTestDB(t)
	var count int
	if err := other.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM workflow_runs`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("run fixture escaped its database: count=%d err=%v", count, err)
	}
}
