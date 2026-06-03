package state

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// updateBuilder constructs partial-UPDATE statements safely without the
// CASE WHEN ? = 0 ... tri-state placeholder pattern. Columns are only touched
// when explicitly Set; raw column-relative expressions go through SetExpr.
type updateBuilder struct {
	table     string
	sets      []string
	args      []any
	wheres    []string
	whereArgs []any
}

func newUpdateBuilder(table string) *updateBuilder {
	return &updateBuilder{table: strings.TrimSpace(table)}
}

// Set assigns a literal value to a column. The value is parameterised.
func (b *updateBuilder) Set(column string, value any) *updateBuilder {
	b.sets = append(b.sets, fmt.Sprintf("%s = ?", column))
	b.args = append(b.args, value)
	return b
}

// SetExpr assigns a raw expression to a column (e.g. "lease_version + 1").
// Use sparingly and only with trusted column references — the expression is
// not parameterised.
func (b *updateBuilder) SetExpr(column, expr string, args ...any) *updateBuilder {
	b.sets = append(b.sets, fmt.Sprintf("%s = %s", column, expr))
	b.args = append(b.args, args...)
	return b
}

// Where adds a parameterised predicate joined with AND.
func (b *updateBuilder) Where(predicate string, args ...any) *updateBuilder {
	b.wheres = append(b.wheres, predicate)
	b.whereArgs = append(b.whereArgs, args...)
	return b
}

// HasUpdates reports whether any column has been queued.
func (b *updateBuilder) HasUpdates() bool {
	return len(b.sets) > 0
}

// Exec runs the UPDATE and returns the result. It returns a non-nil error if
// no SET clauses or no WHERE predicates were supplied — silent UPDATEs without
// a WHERE are always a bug in this codebase.
func (b *updateBuilder) Exec(ctx context.Context, exec sqlExecutor) (sql.Result, error) {
	if b.table == "" {
		return nil, fmt.Errorf("update builder requires table")
	}
	if len(b.sets) == 0 {
		return nil, fmt.Errorf("update builder requires at least one SET clause")
	}
	if len(b.wheres) == 0 {
		return nil, fmt.Errorf("update builder requires at least one WHERE clause")
	}
	query := fmt.Sprintf(
		"UPDATE %s SET %s WHERE %s",
		b.table,
		strings.Join(b.sets, ", "),
		strings.Join(b.wheres, " AND "),
	)
	args := make([]any, 0, len(b.args)+len(b.whereArgs))
	args = append(args, b.args...)
	args = append(args, b.whereArgs...)
	return exec.ExecContext(ctx, query, args...)
}
