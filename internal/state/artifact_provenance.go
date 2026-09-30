package state

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
)

type RunArtifactProvenance struct {
	ArtifactRecord
	NodeID          string
	Attempt         int
	ProducerAgentID string
	Lineage         string
}

// ArtifactPageCursor pins an insertion window independently of canonical lineage.
// Content and metadata are atomically inserted; a later artifact cannot enter a
// prior window even when its timestamp sorts between already returned records.
type ArtifactPageCursor struct {
	HighWater          int64  `json:"high_water"`
	LineageFingerprint string `json:"lineage"`
	KindRank           int    `json:"kind_rank"`
	CreatedAt          string `json:"created_at"`
	ArtifactID         string `json:"artifact_id"`
}

type RunArtifactPage struct {
	Items      []RunArtifactProvenance
	SnapshotID string
	Next       *ArtifactPageCursor
}

var ErrArtifactSnapshotChanged = errors.New("artifact lineage changed; refresh the listing")

const successfulBuildersCTE = `WITH successful_builders AS (
 SELECT ae.attempt_id, ae.node_run_id, nr.node_id, nr.required_role, ROW_NUMBER() OVER (
  PARTITION BY nr.node_id ORDER BY nr.attempt DESC,
  COALESCE(ae.committed_at, ae.terminal_observed_at, ae.updated_at, ae.created_at) DESC,
  ae.attempt_id DESC) AS rank
 FROM attempt_executions ae
 JOIN node_runs nr ON nr.node_run_id=ae.node_run_id AND nr.run_id=ae.run_id
 JOIN workflow_runs wr ON wr.run_id=ae.run_id
 JOIN workflow_nodes wn ON wn.workflow_definition_id=wr.workflow_definition_id AND wn.node_id=nr.node_id
 JOIN provider_completion_settlements pcs ON pcs.settlement_id=ae.canonical_settlement_id
  AND pcs.attempt_id=ae.attempt_id AND pcs.run_id=ae.run_id AND pcs.node_run_id=ae.node_run_id
 WHERE ae.run_id=? AND ae.execution_mode='provider_backed' AND ae.state='committed_success'
  AND pcs.outcome='success' AND wn.node_class='delegated_execution'
  AND COALESCE(TRIM(nr.superseded_at),'')=''
)`

const artifactKindRankSQL = `CASE a.artifact_kind WHEN 'deliverable' THEN 0 WHEN 'verification' THEN 1 ELSE 2 END`

// PageRunArtifactProvenance reads a bounded page and its classification from one
// SQLite snapshot. Only successful canonical builder settlements establish
// deliverable lineage. Publication and run approval remain independent.
func (s *ArtifactService) PageRunArtifactProvenance(ctx context.Context, runID, attemptID string, limit int, cursor *ArtifactPageCursor) (RunArtifactPage, error) {
	if limit < 1 || limit > 200 {
		return RunArtifactPage{}, fmt.Errorf("artifact page limit must be between 1 and 200")
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return RunArtifactPage{}, err
	}
	defer tx.Rollback()
	lineage, err := artifactLineageFingerprint(ctx, tx, runID)
	if err != nil {
		return RunArtifactPage{}, err
	}
	pinned := ArtifactPageCursor{LineageFingerprint: lineage}
	if cursor != nil {
		pinned = *cursor
		if pinned.LineageFingerprint != lineage {
			return RunArtifactPage{}, ErrArtifactSnapshotChanged
		}
	} else {
		if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(artifact_seq),0) FROM artifacts WHERE run_id=? AND (?='' OR attempt_id=?)`, runID, attemptID, attemptID).Scan(&pinned.HighWater); err != nil {
			return RunArtifactPage{}, err
		}
	}
	snapshotBytes, _ := json.Marshal([]any{runID, attemptID, pinned.HighWater, lineage})
	snapshotDigest := sha256.Sum256(snapshotBytes)
	page := RunArtifactPage{Items: make([]RunArtifactProvenance, 0, limit), SnapshotID: hex.EncodeToString(snapshotDigest[:])}
	query := successfulBuildersCTE + `
 SELECT a.artifact_id,a.run_id,a.node_run_id,a.attempt_id,a.artifact_kind,a.artifact_action,
  a.name,a.mime,a.path,a.sha256,a.size_bytes,a.producer_role,a.created_at,
  nr.node_id,nr.attempt,nr.agent_id,
  CASE WHEN sb.rank=1 AND a.artifact_kind='deliverable' AND a.producer_role=nr.required_role
   THEN 'canonical_deliverable' ELSE 'audit' END
 FROM artifacts a
 JOIN node_runs nr ON nr.node_run_id=a.node_run_id AND nr.run_id=a.run_id
 LEFT JOIN successful_builders sb ON sb.attempt_id=a.attempt_id AND sb.node_run_id=a.node_run_id
 WHERE a.run_id=? AND a.artifact_seq<=?`
	args := []any{runID, runID, pinned.HighWater}
	if attemptID != "" {
		query += ` AND a.attempt_id=?`
		args = append(args, attemptID)
	}
	if cursor != nil {
		query += ` AND (` + artifactKindRankSQL + `>? OR (` + artifactKindRankSQL + `=? AND (a.created_at<? OR (a.created_at=? AND a.artifact_id>?))))`
		args = append(args, pinned.KindRank, pinned.KindRank, pinned.CreatedAt, pinned.CreatedAt, pinned.ArtifactID)
	}
	query += ` ORDER BY ` + artifactKindRankSQL + `,a.created_at DESC,a.artifact_id LIMIT ?`
	args = append(args, limit+1)
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return RunArtifactPage{}, err
	}
	defer rows.Close()
	for rows.Next() {
		var item RunArtifactProvenance
		if err := rows.Scan(&item.ArtifactID, &item.RunID, &item.NodeRunID, &item.AttemptID, &item.ArtifactKind, &item.ArtifactAction,
			&item.Name, &item.MIME, &item.Path, &item.SHA256, &item.SizeBytes, &item.ProducerRole, &item.CreatedAt,
			&item.NodeID, &item.Attempt, &item.ProducerAgentID, &item.Lineage); err != nil {
			return RunArtifactPage{}, err
		}
		if len(page.Items) == limit {
			last := page.Items[len(page.Items)-1]
			pinned.KindRank = artifactKindRank(last.ArtifactKind)
			pinned.CreatedAt = last.CreatedAt
			pinned.ArtifactID = last.ArtifactID
			page.Next = &pinned
			break
		}
		page.Items = append(page.Items, item)
	}
	if err := rows.Err(); err != nil {
		return RunArtifactPage{}, err
	}
	if err := rows.Close(); err != nil {
		return RunArtifactPage{}, err
	}
	if err := tx.Commit(); err != nil {
		return RunArtifactPage{}, err
	}
	return page, nil
}

// Fingerprint only the canonical winners, not the artifact collection. Stream
// the result so memory does not grow with the number of workflow nodes.
func artifactLineageFingerprint(ctx context.Context, tx *sql.Tx, runID string) (string, error) {
	rows, err := tx.QueryContext(ctx, successfulBuildersCTE+` SELECT node_id,attempt_id,node_run_id,required_role FROM successful_builders WHERE rank=1 ORDER BY node_id`, runID)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	hash := sha256.New()
	encoder := json.NewEncoder(hash)
	for rows.Next() {
		var node, attempt, nodeRun, role string
		if err := rows.Scan(&node, &attempt, &nodeRun, &role); err != nil {
			return "", err
		}
		if err := encoder.Encode([]string{node, attempt, nodeRun, role}); err != nil {
			return "", err
		}
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func artifactKindRank(kind string) int {
	switch kind {
	case "deliverable":
		return 0
	case "verification":
		return 1
	default:
		return 2
	}
}
