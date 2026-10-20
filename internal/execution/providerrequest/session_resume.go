package providerrequest

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// resumePredecessorBindings grants reuse only along immutable, checkpointed
// recovery edges for this exact workflow node. An absent edge keeps the normal
// successful same-generation continuation rule unchanged.
func resumePredecessorBindings(ctx context.Context, q Reader, binding Binding, current canonicalOwner) (map[string]struct{}, error) {
	if binding.Owner.Kind != OwnerRun {
		return nil, nil
	}
	var nodeID, baseSnapshotID string
	if err := q.QueryRowContext(ctx, `SELECT nr.node_id,ae.base_snapshot_id FROM attempt_executions ae JOIN node_runs nr ON nr.node_run_id=ae.node_run_id AND nr.run_id=ae.run_id WHERE ae.attempt_id=?`, current.id).Scan(&nodeID, &baseSnapshotID); err != nil {
		return nil, err
	}
	child := current
	var allowed map[string]struct{}
	seen := map[string]struct{}{current.id: {}}
	for {
		var sourceAttemptID, sourceNodeRunID, runID, recoveryNodeID, checkpointSource string
		err := q.QueryRowContext(ctx, `SELECT source_attempt_id,source_node_run_id,run_id,node_id,checkpoint_source_attempt_id FROM node_attempt_recoveries WHERE target_node_run_id=? AND trigger='operator_resume'`, child.node).Scan(&sourceAttemptID, &sourceNodeRunID, &runID, &recoveryNodeID, &checkpointSource)
		if errors.Is(err, sql.ErrNoRows) {
			return allowed, nil
		}
		if err != nil {
			return nil, err
		}
		if _, duplicate := seen[sourceAttemptID]; duplicate {
			return nil, fmt.Errorf("resume recovery lineage contains a cycle")
		}
		seen[sourceAttemptID] = struct{}{}
		if runID != current.parent || recoveryNodeID != nodeID || checkpointSource != sourceAttemptID {
			return nil, fmt.Errorf("resume checkpoint belongs to another workflow node")
		}
		var sourceBindingID, sourceBaseSnapshotID, sourceResultSnapshotID, sourceState, activeInvocationID string
		err = q.QueryRowContext(ctx, `SELECT cp.binding_id,ae.base_snapshot_id,ae.result_snapshot_id,ae.state,COALESCE(ae.active_provider_invocation_id,'')
			FROM attempt_resume_checkpoints cp JOIN attempt_executions ae ON ae.attempt_id=cp.source_attempt_id
			WHERE cp.source_attempt_id=? AND cp.unavailable_reason='' AND cp.workspace_snapshot_id=ae.result_snapshot_id AND cp.conversation_content_id IS NOT NULL`, sourceAttemptID).
			Scan(&sourceBindingID, &sourceBaseSnapshotID, &sourceResultSnapshotID, &sourceState, &activeInvocationID)
		if err != nil {
			return nil, fmt.Errorf("resume predecessor lacks a ready checkpoint: %w", err)
		}
		if (sourceState != "committed_failure" && sourceState != "committed_cancelled") || sourceBaseSnapshotID == "" || sourceBaseSnapshotID != baseSnapshotID || sourceResultSnapshotID == "" {
			return nil, fmt.Errorf("resume predecessor is not an interrupted attempt with the original trusted base")
		}
		source, err := Get(ctx, q, sourceBindingID)
		if err != nil {
			return nil, err
		}
		previous, err := loadOwner(ctx, q, source.Owner)
		if err != nil {
			return nil, err
		}
		if source.Owner.Kind != OwnerRun || previous.id != sourceAttemptID || previous.node != sourceNodeRunID || previous.parent != current.parent || source.Owner.InvocationID != activeInvocationID || source.Identity.SessionID != binding.Identity.SessionID || source.LogicalSessionID != binding.LogicalSessionID {
			return nil, fmt.Errorf("resume predecessor provider ownership does not match the checkpoint")
		}
		var sourceNodeID string
		if err := q.QueryRowContext(ctx, `SELECT node_id FROM node_runs WHERE node_run_id=? AND run_id=?`, sourceNodeRunID, current.parent).Scan(&sourceNodeID); err != nil {
			return nil, err
		}
		if sourceNodeID != nodeID {
			return nil, fmt.Errorf("resume predecessor belongs to another workflow node")
		}
		if err := RequireResourcesClosedTx(ctx, q, sourceBindingID); err != nil {
			return nil, err
		}
		if allowed == nil {
			allowed = map[string]struct{}{}
		}
		allowed[sourceBindingID] = struct{}{}
		child = previous
	}
}
