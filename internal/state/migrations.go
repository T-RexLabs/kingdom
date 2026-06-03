package state

import (
	"context"
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
// names until explicit rollback tooling exists.
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

	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (name TEXT PRIMARY KEY, applied_at TEXT NOT NULL)`); err != nil {
		return fmt.Errorf("ensure schema_migrations table: %w", err)
	}

	applied, err := loadAppliedMigrations(ctx, db)
	if err != nil {
		return err
	}

	for _, migration := range ordered {
		if applied[migration.Name] {
			continue
		}

		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("begin migration %s: %w", migration.Name, err)
		}

		if _, err := tx.ExecContext(ctx, string(migration.SQL)); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("apply migration %s: %w", migration.Name, err)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO schema_migrations (name, applied_at) VALUES (?, ?)`, migration.Name, now.UTC().Format(time.RFC3339)); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("record migration %s: %w", migration.Name, err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit migration %s: %w", migration.Name, err)
		}
	}

	return nil
}

func loadAppliedMigrations(ctx context.Context, db *sql.DB) (map[string]bool, error) {
	rows, err := db.QueryContext(ctx, `SELECT name FROM schema_migrations`)
	if err != nil {
		return nil, fmt.Errorf("load applied migrations: %w", err)
	}
	defer rows.Close()

	applied := make(map[string]bool)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("scan applied migration: %w", err)
		}
		applied[name] = true
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate applied migrations: %w", err)
	}
	return applied, nil
}
