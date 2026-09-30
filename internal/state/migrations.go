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

type migrationForeignKeyReference struct {
	id          int64
	seq         int64
	childColumn string
	parentTable string
	parentCol   string
}

func foreignKeyViolationsTx(ctx context.Context, tx *sql.Tx) ([]string, error) {
	tables, err := migrationForeignKeyTablesTx(ctx, tx)
	if err != nil {
		return nil, err
	}

	violations := make([]string, 0)
	for _, tableName := range tables {
		references, err := migrationForeignKeyReferencesTx(ctx, tx, tableName)
		if err != nil {
			return nil, err
		}
		byID := make(map[int64][]migrationForeignKeyReference)
		for _, reference := range references {
			byID[reference.id] = append(byID[reference.id], reference)
		}
		foreignKeyIDs := make([]int64, 0, len(byID))
		for id := range byID {
			foreignKeyIDs = append(foreignKeyIDs, id)
		}
		sort.Slice(foreignKeyIDs, func(i, j int) bool { return foreignKeyIDs[i] < foreignKeyIDs[j] })
		for _, id := range foreignKeyIDs {
			group := byID[id]
			sort.Slice(group, func(i, j int) bool { return group[i].seq < group[j].seq })
			count, err := migrationForeignKeyViolationCountTx(ctx, tx, tableName, group)
			if err != nil {
				return nil, err
			}
			if count == 0 {
				continue
			}
			violations = append(violations, fmt.Sprintf("table=%s parent=%s fk=%d count=%d", tableName, group[0].parentTable, id, count))
		}
	}
	return violations, nil
}

func migrationForeignKeyTablesTx(ctx context.Context, tx *sql.Tx) ([]string, error) {
	rows, err := tx.QueryContext(ctx, `SELECT name FROM sqlite_schema WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	tables := make([]string, 0)
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, err
		}
		tables = append(tables, tableName)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return tables, nil
}

func migrationForeignKeyReferencesTx(ctx context.Context, tx *sql.Tx, tableName string) ([]migrationForeignKeyReference, error) {
	rows, err := tx.QueryContext(ctx, fmt.Sprintf(`PRAGMA foreign_key_list(%s)`, quoteSQLiteStringLiteral(tableName)))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	references := make([]migrationForeignKeyReference, 0)
	for rows.Next() {
		var reference migrationForeignKeyReference
		var onUpdate string
		var onDelete string
		var match string
		if err := rows.Scan(&reference.id, &reference.seq, &reference.parentTable, &reference.childColumn, &reference.parentCol, &onUpdate, &onDelete, &match); err != nil {
			return nil, err
		}
		references = append(references, reference)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return references, nil
}

func migrationForeignKeyViolationCountTx(ctx context.Context, tx *sql.Tx, tableName string, references []migrationForeignKeyReference) (int, error) {
	if len(references) == 0 {
		return 0, nil
	}
	nonNullChecks := make([]string, 0, len(references))
	parentMatches := make([]string, 0, len(references))
	for _, reference := range references {
		childColumn := quoteSQLiteIdentifier(reference.childColumn)
		parentColumn := quoteSQLiteIdentifier(reference.parentCol)
		nonNullChecks = append(nonNullChecks, fmt.Sprintf("child.%s IS NOT NULL", childColumn))
		parentMatches = append(parentMatches, fmt.Sprintf("parent.%s = child.%s", parentColumn, childColumn))
	}
	query := fmt.Sprintf(`SELECT COUNT(1)
		FROM %s AS child
		WHERE %s
		  AND NOT EXISTS (
		    SELECT 1
		    FROM %s AS parent
		    WHERE %s
		  )`, quoteSQLiteIdentifier(tableName), strings.Join(nonNullChecks, " AND "), quoteSQLiteIdentifier(references[0].parentTable), strings.Join(parentMatches, " AND "))
	var count int
	if err := tx.QueryRowContext(ctx, query).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func quoteSQLiteIdentifier(identifier string) string {
	return `"` + strings.ReplaceAll(strings.TrimSpace(identifier), `"`, `""`) + `"`
}

func quoteSQLiteStringLiteral(value string) string {
	return `'` + strings.ReplaceAll(strings.TrimSpace(value), `'`, `''`) + `'`
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
