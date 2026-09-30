// Package nodeattempt defines dependency-neutral SQL fragments for selecting
// durable node-attempt lineage.
package nodeattempt

import (
	"fmt"
	"strings"
)

// CanonicalNodeAttemptOrderSQL returns the shared ordering used where the
// runtime needs the latest attempt in a node lineage while honoring
// superseded-before-launch semantics.
func CanonicalNodeAttemptOrderSQL(tableAlias string) string {
	trimmedAlias := strings.TrimSpace(tableAlias)
	if trimmedAlias == "" {
		trimmedAlias = "node_runs"
	}
	return fmt.Sprintf(`CASE
		WHEN COALESCE(TRIM(%[1]s.superseded_at), '') = '' THEN 0
		ELSE 1
	END ASC,
		%[1]s.attempt DESC,
		%[1]s.node_run_id DESC`, trimmedAlias)
}

// VisibleNodeAttemptOrderSQL returns the ordering for operator-facing node
// surfaces. A live lineage remains visible while its replacement is queued.
func VisibleNodeAttemptOrderSQL(tableAlias string) string {
	trimmedAlias := strings.TrimSpace(tableAlias)
	if trimmedAlias == "" {
		trimmedAlias = "node_runs"
	}
	return fmt.Sprintf(`CASE
		WHEN COALESCE(TRIM(%[1]s.node_status), 'pending') IN ('starting', 'running', 'blocked') THEN 0
		ELSE 1
	END ASC,
	CASE
		WHEN COALESCE(TRIM(%[1]s.superseded_at), '') = '' THEN 0
		ELSE 1
	END ASC,
		%[1]s.attempt DESC,
		%[1]s.node_run_id DESC`, trimmedAlias)
}

// LiveNodeAttemptStatusConditionSQL returns the status predicate used for
// attempts that still prevent another wave from being queued.
func LiveNodeAttemptStatusConditionSQL(tableAlias string) string {
	trimmedAlias := strings.TrimSpace(tableAlias)
	if trimmedAlias == "" {
		trimmedAlias = "node_runs"
	}
	return fmt.Sprintf(`COALESCE(TRIM(%[1]s.node_status), 'pending') IN ('pending', 'ready', 'starting', 'running', 'blocked')`, trimmedAlias)
}
