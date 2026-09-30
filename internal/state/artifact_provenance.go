package state

import "context"

type RunArtifactProvenance struct {
	ArtifactRecord
	NodeID          string
	Attempt         int
	ProducerAgentID string
	Lineage         string
}

// ListRunArtifactProvenance reads metadata and its current lineage in one SQLite
// snapshot. Artifacts never establish success: canonical deliverables require a
// non-superseded builder attempt with its own successful canonical settlement.
// Publication and final run approval are independent of this classification.
func (s *ArtifactService) ListRunArtifactProvenance(ctx context.Context, runID string) ([]RunArtifactProvenance, error) {
	rows, err := s.db.QueryContext(ctx, `WITH successful_builders AS (
		SELECT ae.attempt_id, ae.node_run_id, ROW_NUMBER() OVER (
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
	)
	SELECT a.artifact_id,a.run_id,a.node_run_id,a.attempt_id,a.artifact_kind,a.artifact_action,
		a.name,a.mime,a.path,a.sha256,a.size_bytes,a.producer_role,a.created_at,
		nr.node_id,nr.attempt,nr.agent_id,
		CASE WHEN sb.rank=1 AND a.artifact_kind='deliverable' AND a.producer_role=nr.required_role
			THEN 'canonical_deliverable' ELSE 'audit' END
	FROM artifacts a
	JOIN node_runs nr ON nr.node_run_id=a.node_run_id AND nr.run_id=a.run_id
	LEFT JOIN successful_builders sb ON sb.attempt_id=a.attempt_id AND sb.node_run_id=a.node_run_id
	WHERE a.run_id=?
	ORDER BY CASE a.artifact_kind WHEN 'deliverable' THEN 0 WHEN 'verification' THEN 1 ELSE 2 END,
		a.created_at DESC,a.artifact_id`, runID, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]RunArtifactProvenance, 0)
	for rows.Next() {
		var r RunArtifactProvenance
		if err := rows.Scan(&r.ArtifactID, &r.RunID, &r.NodeRunID, &r.AttemptID, &r.ArtifactKind, &r.ArtifactAction,
			&r.Name, &r.MIME, &r.Path, &r.SHA256, &r.SizeBytes, &r.ProducerRole, &r.CreatedAt,
			&r.NodeID, &r.Attempt, &r.ProducerAgentID, &r.Lineage); err != nil {
			return nil, err
		}
		items = append(items, r)
	}
	return items, rows.Err()
}
