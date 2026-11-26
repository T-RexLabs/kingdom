package state

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"time"
)

type SQLMigration struct {
	Name string
	SQL  []byte
}

// ApplySQLMigrations is intentionally forward-only. Runtime bootstrap executes
// source-controlled *.up.sql files in sorted order and ignores any other asset
// names until explicit rollback tooling exists. Callers supply the complete
// canonical sequence; applied migrations must be an unchanged prefix of it.
func ApplySQLMigrations(ctx context.Context, db *sql.DB, migrations []SQLMigration, now time.Time) error {
	if db == nil {
		return fmt.Errorf("database handle is required")
	}

	ordered := make([]SQLMigration, 0, len(migrations))
	for _, migration := range migrations {
		name := strings.TrimSpace(migration.Name)
		if name == "" || !strings.HasSuffix(name, ".up.sql") {
			continue
		}
		ordered = append(ordered, SQLMigration{Name: name, SQL: migration.SQL})
	}
	sort.Slice(ordered, func(i, j int) bool {
		return ordered[i].Name < ordered[j].Name
	})

	for i := 1; i < len(ordered); i++ {
		if ordered[i-1].Name == ordered[i].Name {
			return fmt.Errorf("duplicate migration name %q", ordered[i].Name)
		}
	}

	applied, err := loadAppliedMigrations(ctx, db, ordered)
	if err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		name TEXT PRIMARY KEY,
		sha256 TEXT NOT NULL CHECK (length(sha256) = 64 AND sha256 NOT GLOB '*[^0-9a-f]*'),
		applied_at TEXT NOT NULL
	)`); err != nil {
		return fmt.Errorf("ensure schema_migrations table: %w", err)
	}

	for _, migration := range ordered {
		if applied[migration.Name] {
			continue
		}

		if err := applyMigrationTx(ctx, db, migration, now); err != nil {
			return err
		}
	}

	return nil
}

func applyMigrationTx(ctx context.Context, db *sql.DB, migration SQLMigration, now time.Time) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin migration %s: %w", migration.Name, err)
	}

	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	if _, err := tx.ExecContext(ctx, string(migration.SQL)); err != nil {
		return fmt.Errorf("apply migration %s: %w", migration.Name, err)
	}
	if violations, err := foreignKeyViolationsTx(ctx, tx); err != nil {
		return fmt.Errorf("validate foreign keys for migration %s: %w", migration.Name, err)
	} else if len(violations) > 0 {
		return fmt.Errorf("validate foreign keys for migration %s: %s", migration.Name, strings.Join(violations, "; "))
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO schema_migrations (name, sha256, applied_at) VALUES (?, ?, ?)`, migration.Name, migrationChecksum(migration.SQL), now.UTC().Format(time.RFC3339)); err != nil {
		return fmt.Errorf("record migration %s: %w", migration.Name, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit migration %s: %w", migration.Name, err)
	}
	committed = true
	return nil
}

// Native validation uses SQLite's parent-key, collation, and NULL semantics.
// The separate schema check preserves rejection of missing parents even when
// their child tables are empty, which foreign_key_check alone permits.
func foreignKeyViolationsTx(ctx context.Context, tx *sql.Tx) ([]string, error) {
	var child, parent string
	err := tx.QueryRowContext(ctx, `SELECT child.name, fk."table"
  FROM main.sqlite_schema AS child
  JOIN pragma_foreign_key_list(child.name, 'main') AS fk
  WHERE child.type = 'table' AND child.name NOT LIKE 'sqlite_%'
    AND fk."table" COLLATE NOCASE NOT IN (
      SELECT name FROM main.sqlite_schema WHERE type = 'table'
    )
  ORDER BY child.name, fk.id LIMIT 1`).Scan(&child, &parent)
	if err == nil {
		return nil, fmt.Errorf("table=%s references missing parent=%s", child, parent)
	}
	if err != sql.ErrNoRows {
		return nil, err
	}

	rows, err := tx.QueryContext(ctx, `SELECT "table", parent, fkid, COUNT(*)
  FROM pragma_foreign_key_check(NULL, 'main')
  GROUP BY "table", parent, fkid
  ORDER BY "table", fkid`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	violations := make([]string, 0)
	for rows.Next() {
		var child, parent string
		var id, count int64
		if err := rows.Scan(&child, &parent, &id, &count); err != nil {
			return nil, err
		}
		violations = append(violations, fmt.Sprintf("table=%s parent=%s fk=%d count=%d", child, parent, id, count))
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return violations, rows.Close()
}

func migrationChecksum(raw []byte) string {
	return fmt.Sprintf("%x", sha256.Sum256(raw))
}

func incompatibleMigrationHistory(format string, args ...any) error {
	return fmt.Errorf("database migration history is incompatible: %s; back up this workspace before resetting with kd clean and kd init", fmt.Sprintf(format, args...))
}

// loadAppliedMigrations validates the entire history before bootstrap can write
// anything. A previously applied migration must retain its exact SQL identity,
// and the applied set must be a prefix of the supplied canonical sequence.
func loadAppliedMigrations(ctx context.Context, db *sql.DB, ordered []SQLMigration) (map[string]bool, error) {
	var historyExists bool
	if err := db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name='schema_migrations')`).Scan(&historyExists); err != nil {
		return nil, fmt.Errorf("inspect migration history: %w", err)
	}
	applied := make(map[string]bool)
	if !historyExists {
		var schemaExists bool
		if err := db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%')`).Scan(&schemaExists); err != nil {
			return nil, fmt.Errorf("inspect database schema: %w", err)
		}
		if schemaExists {
			return nil, incompatibleMigrationHistory("existing schema has no migration history")
		}
		return applied, nil
	}

	var hasChecksum bool
	if err := db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pragma_table_info('schema_migrations') WHERE name='sha256')`).Scan(&hasChecksum); err != nil {
		return nil, fmt.Errorf("inspect migration checksum metadata: %w", err)
	}
	if !hasChecksum {
		return nil, incompatibleMigrationHistory("applied migration checksums are missing")
	}

	known := make(map[string]string, len(ordered))
	for _, migration := range ordered {
		known[migration.Name] = migrationChecksum(migration.SQL)
	}
	rows, err := db.QueryContext(ctx, `SELECT name, sha256 FROM schema_migrations ORDER BY name`)
	if err != nil {
		return nil, fmt.Errorf("load applied migrations: %w", err)
	}
	defer rows.Close()
	index := 0
	for rows.Next() {
		var name, checksum string
		if err := rows.Scan(&name, &checksum); err != nil {
			return nil, fmt.Errorf("scan applied migration: %w", err)
		}
		expectedChecksum, exists := known[name]
		if !exists {
			return nil, incompatibleMigrationHistory("applied migration %q is not in the canonical sequence", name)
		}
		if index >= len(ordered) || ordered[index].Name != name {
			return nil, incompatibleMigrationHistory("applied migrations are not a contiguous prefix of the canonical sequence at %q", name)
		}
		if checksum != expectedChecksum {
			return nil, incompatibleMigrationHistory("checksum differs for applied migration %q", name)
		}
		applied[name] = true
		index++
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate applied migrations: %w", err)
	}
	return applied, nil
}
