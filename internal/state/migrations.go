package state

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const migrationForeignKeysOffMarker = "-- kingdom: foreign_keys=off"

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

		if migrationRequiresForeignKeysOff(migration.SQL) {
			if err := applyMigrationWithForeignKeysOff(ctx, db, migration, now); err != nil {
				return err
			}
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
	if _, err := tx.ExecContext(ctx, `INSERT INTO schema_migrations (name, applied_at) VALUES (?, ?)`, migration.Name, now.UTC().Format(time.RFC3339)); err != nil {
		return fmt.Errorf("record migration %s: %w", migration.Name, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit migration %s: %w", migration.Name, err)
	}
	committed = true
	return nil
}

func applyMigrationWithForeignKeysOff(ctx context.Context, db *sql.DB, migration SQLMigration, now time.Time) error {
	if err := validateForeignKeysOffMigrationShadow(ctx, db, migration, now); err != nil {
		return err
	}
	return applyMigrationWithForeignKeysOffOnce(ctx, db, migration, now)
}

func applyMigrationWithForeignKeysOffOnce(ctx context.Context, db *sql.DB, migration SQLMigration, now time.Time) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquire dedicated connection for migration %s: %w", migration.Name, err)
	}
	defer conn.Close()

	if _, err := conn.ExecContext(ctx, `PRAGMA foreign_keys = OFF`); err != nil {
		return fmt.Errorf("disable foreign keys for migration %s: %w", migration.Name, err)
	}
	defer func() {
		_, _ = conn.ExecContext(context.Background(), `PRAGMA foreign_keys = ON`)
	}()

	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin migration %s on dedicated connection: %w", migration.Name, err)
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
	if _, err := tx.ExecContext(ctx, `INSERT INTO schema_migrations (name, applied_at) VALUES (?, ?)`, migration.Name, now.UTC().Format(time.RFC3339)); err != nil {
		return fmt.Errorf("record migration %s: %w", migration.Name, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit migration %s: %w", migration.Name, err)
	}
	committed = true

	if _, err := conn.ExecContext(ctx, `PRAGMA foreign_keys = ON`); err != nil {
		return fmt.Errorf("re-enable foreign keys for migration %s: %w", migration.Name, err)
	}
	return nil
}

func validateForeignKeysOffMigrationShadow(ctx context.Context, db *sql.DB, migration SQLMigration, now time.Time) error {
	shadowRoot, err := os.MkdirTemp("", "kingdom-migration-*")
	if err != nil {
		return fmt.Errorf("create shadow migration directory for %s: %w", migration.Name, err)
	}
	defer os.RemoveAll(shadowRoot)

	shadowPath := filepath.Join(shadowRoot, "shadow.sqlite")
	if err := vacuumIntoSQLiteFile(ctx, db, shadowPath); err != nil {
		return fmt.Errorf("snapshot database for migration %s: %w", migration.Name, err)
	}

	shadowDB, err := sql.Open("sqlite", shadowPath)
	if err != nil {
		return fmt.Errorf("open shadow database for migration %s: %w", migration.Name, err)
	}
	defer shadowDB.Close()

	if err := applyMigrationWithForeignKeysOffOnce(ctx, shadowDB, migration, now); err != nil {
		return err
	}
	violations, err := foreignKeyViolationsDB(ctx, shadowDB)
	if err != nil {
		return fmt.Errorf("validate foreign keys for migration %s: %w", migration.Name, err)
	}
	if len(violations) > 0 {
		return fmt.Errorf("validate foreign keys for migration %s: %s", migration.Name, strings.Join(violations, "; "))
	}
	return nil
}

func vacuumIntoSQLiteFile(ctx context.Context, db *sql.DB, destPath string) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	_, err = conn.ExecContext(ctx, fmt.Sprintf(`VACUUM INTO %s`, quoteSQLiteStringLiteral(destPath)))
	return err
}

func migrationRequiresForeignKeysOff(raw []byte) bool {
	for _, line := range strings.Split(string(raw), "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		return trimmed == migrationForeignKeysOffMarker
	}
	return false
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

func foreignKeyViolationsDB(ctx context.Context, db *sql.DB) ([]string, error) {
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	return foreignKeyViolationsTx(ctx, tx)
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
