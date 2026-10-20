package state

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"kingdom/internal/core/workflow"
	"kingdom/internal/execution/providerrequest"
)

// AttemptResumeInput is the immutable input pinned by a target's recovery row.
// Binding retains the old conversation identity, never authority for new work.
type AttemptResumeInput struct {
	Checkpoint AttemptResumeCheckpoint
	Boundary   AttemptResumeBoundary
	Binding    providerrequest.Binding
}

// LoadAttemptResumeInput verifies the exact selected recovery and its stored
// conversation content. Fresh attempts and ordinary Reruns return sql.ErrNoRows.
func LoadAttemptResumeInput(ctx context.Context, db *sql.DB, targetNodeRunID string) (AttemptResumeInput, error) {
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return AttemptResumeInput{}, err
	}
	defer tx.Rollback()
	var sourceAttemptID string
	if err := tx.QueryRowContext(ctx, `SELECT r.checkpoint_source_attempt_id FROM node_attempt_recoveries r
		JOIN node_runs target ON target.node_run_id=r.target_node_run_id AND target.run_id=r.run_id AND target.node_id=r.node_id
		JOIN attempt_executions source ON source.attempt_id=r.source_attempt_id AND source.node_run_id=r.source_node_run_id AND source.run_id=r.run_id
		JOIN node_runs predecessor ON predecessor.node_run_id=source.node_run_id AND predecessor.run_id=r.run_id AND predecessor.node_id=r.node_id
		WHERE r.target_node_run_id=? AND r.trigger='operator_resume' AND r.checkpoint_source_attempt_id=r.source_attempt_id`, targetNodeRunID).Scan(&sourceAttemptID); err != nil {
		return AttemptResumeInput{}, err
	}
	return loadResumeSourceInput(ctx, tx, sourceAttemptID)
}

func loadResumeSourceInput(ctx context.Context, q providerrequest.Reader, sourceAttemptID string) (AttemptResumeInput, error) {
	var input AttemptResumeInput
	checkpoint, err := loadAttemptResumeCheckpoint(ctx, q, sourceAttemptID)
	if err != nil {
		return input, err
	}
	if checkpoint.UnavailableReason != "" {
		return input, fmt.Errorf("resume is unavailable: %s", checkpoint.UnavailableReason)
	}
	if checkpoint.BindingID == "" || checkpoint.BaseSnapshotID == "" || checkpoint.WorkspaceSnapshotID == "" || checkpoint.ConversationBoundary.ContentID == "" {
		return input, fmt.Errorf("resume checkpoint is incomplete")
	}
	binding, err := providerrequest.Get(ctx, q, checkpoint.BindingID)
	if err != nil {
		return input, err
	}
	var validSource, pendingHandoff bool
	if err := q.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM attempt_executions ae JOIN provider_invocations pi ON pi.provider_invocation_id=ae.active_provider_invocation_id AND pi.attempt_id=ae.attempt_id AND pi.run_id=ae.run_id AND pi.node_run_id=ae.node_run_id
		JOIN filesystem_snapshots base ON base.snapshot_id=ae.base_snapshot_id AND base.run_id=ae.run_id
		JOIN filesystem_snapshots seed ON seed.snapshot_id=ae.result_snapshot_id AND seed.run_id=ae.run_id
		WHERE ae.attempt_id=? AND ae.state IN ('committed_failure','committed_cancelled') AND ae.execution_mode='provider_backed' AND ae.result_snapshot_id=? AND pi.provider_invocation_id=?),
		EXISTS(SELECT 1 FROM provider_terminal_handoffs pth WHERE pth.provider_invocation_id=? AND NOT EXISTS(SELECT 1 FROM provider_completion_settlements pcs WHERE pcs.provider_invocation_id=pth.provider_invocation_id))
		OR EXISTS(SELECT 1 FROM provider_invocations WHERE provider_invocation_id=? AND invocation_state NOT LIKE 'settled_%' AND (terminal_handoff_pending_at IS NOT NULL OR terminal_recovery_required_at IS NOT NULL))`, sourceAttemptID, checkpoint.WorkspaceSnapshotID, binding.Owner.InvocationID, binding.Owner.InvocationID, binding.Owner.InvocationID).Scan(&validSource, &pendingHandoff); err != nil {
		return input, err
	}
	if binding.Owner.Kind != providerrequest.OwnerRun || !validSource {
		return input, fmt.Errorf("resume checkpoint source lineage is no longer valid")
	}
	// Settlement keeps the pre-publication marker as history; only an
	// unsettled handoff still carries evidence that must be processed.
	if pendingHandoff {
		return input, fmt.Errorf("resume source has an unresolved provider handoff")
	}
	if err := providerrequest.RequireResourcesClosedTx(ctx, q, binding.ID); err != nil {
		return input, err
	}
	// The envelope is a few identifiers and a hash, not a transcript. Bound its
	// decode independently of the potentially large canonical content store.
	if checkpoint.ConversationBoundary.SizeBytes > requestContentChunkBytes {
		return input, fmt.Errorf("resume conversation boundary exceeds its metadata limit")
	}
	var content bytes.Buffer
	actual, err := copyRequestContent(ctx, q, checkpoint.ConversationBoundary.ContentID, &content)
	if err != nil {
		return input, err
	}
	if actual != checkpoint.ConversationBoundary {
		return input, fmt.Errorf("resume conversation boundary reference changed")
	}
	var envelope resumeBoundaryEnvelope
	decoder := json.NewDecoder(&content)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&envelope); err != nil {
		return input, fmt.Errorf("decode resume conversation boundary: %w", err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return input, fmt.Errorf("resume conversation boundary contains trailing data")
	}
	if envelope.InvocationID != binding.Owner.InvocationID || envelope.SessionID != binding.Identity.SessionID || envelope.RequestID != binding.Identity.RequestID || envelope.MessageID == "" {
		return input, fmt.Errorf("resume conversation boundary does not match source request")
	}
	digest, err := hex.DecodeString(envelope.MessagesSHA256)
	if err != nil || len(digest) != sha256.Size || hex.EncodeToString(digest) != envelope.MessagesSHA256 {
		return input, fmt.Errorf("resume conversation boundary has an invalid history digest")
	}
	input = AttemptResumeInput{Checkpoint: checkpoint, Boundary: envelope.AttemptResumeBoundary, Binding: binding}
	return input, nil
}

// RunResumeStep is one interrupted step on the run's frontier. A step without
// a usable checkpoint carries the reason Resume is unavailable for it.
type RunResumeStep struct {
	NodeID, SourceAttemptID, CheckpointCapturedAt, UnavailableReason string
}

// RunResumePlan is the server-derived recovery scope. Resume is available only
// when every frontier step can continue; it never degrades into a Rerun.
type RunResumePlan struct {
	Steps             []RunResumeStep
	TargetNodeIDs     []string
	UnavailableReason string
}

func (p RunResumePlan) Available() bool {
	if p.UnavailableReason != "" || len(p.Steps) == 0 {
		return false
	}
	for _, step := range p.Steps {
		if step.UnavailableReason != "" {
			return false
		}
	}
	return true
}

// Sources returns the exact source attempts a Resume request must confirm.
func (p RunResumePlan) Sources() map[string]string {
	sources := make(map[string]string, len(p.Steps))
	for _, step := range p.Steps {
		sources[step.NodeID] = step.SourceAttemptID
	}
	return sources
}

// LoadRunResumePlan derives the interrupted frontier and each step's
// eligibility from durable records. It never chooses a historical attempt.
func LoadRunResumePlan(ctx context.Context, db *sql.DB, runID string) (RunResumePlan, error) {
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return RunResumePlan{}, err
	}
	defer tx.Rollback()
	var runStatus string
	if err := tx.QueryRowContext(ctx, `SELECT run_status FROM workflow_runs WHERE run_id=?`, runID).Scan(&runStatus); err != nil {
		return RunResumePlan{}, err
	}
	if runStatus != "failed" && runStatus != "cancelled" {
		return RunResumePlan{UnavailableReason: "Only a failed or cancelled run can be resumed."}, nil
	}
	frontier, err := loadResumeFrontierTx(ctx, tx, runID)
	if err != nil {
		return RunResumePlan{}, err
	}
	plan := RunResumePlan{TargetNodeIDs: frontier.targets(frontier.interrupted)}
	for _, nodeID := range frontier.interrupted {
		step := RunResumeStep{NodeID: nodeID}
		step.SourceAttemptID, step.CheckpointCapturedAt, step.UnavailableReason, err = resumeStepEligibilityTx(ctx, tx, runID, nodeID)
		if err != nil {
			return RunResumePlan{}, err
		}
		plan.Steps = append(plan.Steps, step)
	}
	if len(plan.Steps) == 0 {
		plan.UnavailableReason = "No interrupted step was found to resume."
		return plan, nil
	}
	blockers, err := cancelledRunReplayBlockersTx(ctx, tx, runID)
	if err != nil {
		return RunResumePlan{}, err
	}
	if len(blockers) != 0 {
		plan.UnavailableReason = "Kingdom is still settling the interrupted work: " + strings.Join(blockers, ", ") + "."
		return plan, nil
	}
	var pinned bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM run_agent_configuration_snapshots WHERE run_id=?)`, runID).Scan(&pinned); err != nil {
		return RunResumePlan{}, err
	}
	if err := tx.Commit(); err != nil {
		return RunResumePlan{}, err
	}
	if !pinned {
		plan.UnavailableReason = "This run has no pinned agent configuration to resume with."
		return plan, nil
	}
	snapshot, err := LoadRunAgentConfigurationSnapshot(ctx, db, runID)
	if err != nil {
		return RunResumePlan{}, err
	}
	if _, err := snapshot.PromptCatalog.ResumeContinuationSection(); err != nil {
		plan.UnavailableReason = "This run was created before Resume was available."
	}
	return plan, nil
}

// resumeSourceAttemptTx returns the attempt whose checkpoint the step would
// continue. A Resume attempt that ended without reaching the provider changed
// neither the conversation nor the files, so it hands back the checkpoint it
// pinned; any provider invocation consumes the checkpoint.
func resumeSourceAttemptTx(ctx context.Context, tx *sql.Tx, runID, nodeID string) (string, error) {
	latestID, err := latestPhysicalAttemptIDForRunNodeTx(ctx, tx, runID, nodeID)
	if err != nil || latestID == "" {
		return "", err
	}
	var latest bool
	if err := tx.QueryRowContext(ctx, `SELECT ae.node_run_id=(SELECT node_run_id FROM node_runs WHERE run_id=? AND node_id=? ORDER BY attempt DESC,node_run_id DESC LIMIT 1) FROM attempt_executions ae WHERE ae.attempt_id=?`, runID, nodeID, latestID).Scan(&latest); err != nil {
		return "", err
	}
	if !latest {
		return "", nil
	}
	sourceID := latestID
	for seen := map[string]bool{}; !seen[sourceID]; {
		seen[sourceID] = true
		var pinned string
		err := tx.QueryRowContext(ctx, `SELECT r.checkpoint_source_attempt_id FROM attempt_executions ae
			JOIN node_attempt_recoveries r ON r.target_node_run_id=ae.node_run_id AND r.run_id=ae.run_id AND r.trigger='operator_resume'
			WHERE ae.attempt_id=? AND ae.state IN ('committed_failure','committed_cancelled')
			AND NOT EXISTS(SELECT 1 FROM provider_invocations pi WHERE pi.attempt_id=ae.attempt_id)`, sourceID).Scan(&pinned)
		if errors.Is(err, sql.ErrNoRows) {
			return sourceID, nil
		}
		if err != nil {
			return "", err
		}
		sourceID = pinned
	}
	return "", fmt.Errorf("resume recovery lineage contains a cycle")
}

func resumeStepEligibilityTx(ctx context.Context, tx *sql.Tx, runID, nodeID string) (string, string, string, error) {
	latestID, err := latestPhysicalAttemptIDForRunNodeTx(ctx, tx, runID, nodeID)
	if err != nil || latestID == "" {
		return "", "", "This step stopped before Kingdom started an attempt.", err
	}
	sourceID, err := resumeSourceAttemptTx(ctx, tx, runID, nodeID)
	if err != nil {
		return "", "", "", err
	}
	if sourceID == "" {
		return latestID, "", "This step stopped before Kingdom started its latest attempt.", nil
	}
	var mode string
	if err := tx.QueryRowContext(ctx, `SELECT execution_mode FROM attempt_executions WHERE attempt_id=?`, sourceID).Scan(&mode); err != nil {
		return "", "", "", err
	}
	if mode != "provider_backed" {
		return sourceID, "", "This step runs inside Kingdom and has no conversation to resume.", nil
	}
	checkpoint, err := loadAttemptResumeCheckpoint(ctx, tx, sourceID)
	if errors.Is(err, sql.ErrNoRows) {
		return sourceID, "", "No checkpoint was saved when this step stopped.", nil
	}
	if err != nil {
		return "", "", "", err
	}
	if checkpoint.UnavailableReason != "" {
		return sourceID, checkpoint.CapturedAt, checkpoint.UnavailableReason, nil
	}
	if _, err := loadResumeSourceInput(ctx, tx, sourceID); err != nil {
		return sourceID, checkpoint.CapturedAt, "The saved checkpoint can no longer be used: " + err.Error() + ".", nil
	}
	return sourceID, checkpoint.CapturedAt, "", nil
}

// resumeFrontier is the latest status of each workflow step and its forward
// dependencies. Rework edges are loops back, never dependencies.
type resumeFrontier struct {
	graph       *workflow.Graph
	statuses    map[string]string
	parents     map[string][]string
	interrupted []string
}

func loadResumeFrontierTx(ctx context.Context, tx *sql.Tx, runID string) (resumeFrontier, error) {
	graph, err := workflowGraphForRunTx(ctx, tx, runID)
	if err != nil {
		return resumeFrontier{}, err
	}
	frontier := resumeFrontier{graph: graph, statuses: map[string]string{}, parents: map[string][]string{}}
	rows, err := tx.QueryContext(ctx, `SELECT nr.node_id,nr.node_status FROM node_runs nr WHERE nr.run_id=? AND COALESCE(TRIM(nr.superseded_at),'')='' AND nr.node_run_id=(SELECT latest.node_run_id FROM node_runs latest WHERE latest.run_id=nr.run_id AND latest.node_id=nr.node_id AND COALESCE(TRIM(latest.superseded_at),'')='' ORDER BY latest.attempt DESC,latest.node_run_id DESC LIMIT 1)`, runID)
	if err != nil {
		return resumeFrontier{}, err
	}
	for rows.Next() {
		var node, status string
		if err := rows.Scan(&node, &status); err != nil {
			rows.Close()
			return resumeFrontier{}, err
		}
		frontier.statuses[node] = status
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return resumeFrontier{}, err
	}
	for _, edges := range graph.Outgoing {
		for _, edge := range edges {
			if !strings.EqualFold(strings.TrimSpace(edge.On), "needs_rework") {
				frontier.parents[edge.To] = append(frontier.parents[edge.To], edge.From)
			}
		}
	}
	for _, nodeID := range graph.Topological {
		if frontier.isInterrupted(nodeID) && !frontier.hasInterruptedAncestor(nodeID) {
			frontier.interrupted = append(frontier.interrupted, nodeID)
		}
	}
	return frontier, nil
}

func (f resumeFrontier) isInterrupted(nodeID string) bool {
	return f.statuses[nodeID] == "failed" || f.statuses[nodeID] == "cancelled"
}

func (f resumeFrontier) hasInterruptedAncestor(nodeID string) bool {
	seen := map[string]bool{nodeID: true}
	pending := append([]string{}, f.parents[nodeID]...)
	for len(pending) > 0 {
		ancestor := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		if seen[ancestor] {
			continue
		}
		seen[ancestor] = true
		if f.isInterrupted(ancestor) {
			return true
		}
		pending = append(pending, f.parents[ancestor]...)
	}
	return false
}

// targets returns the anchors and every step they release, in workflow order.
func (f resumeFrontier) targets(anchors []string) []string {
	reachable := map[string]bool{}
	pending := append([]string{}, anchors...)
	for len(pending) > 0 {
		current := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		if reachable[current] {
			continue
		}
		reachable[current] = true
		for _, edge := range f.graph.Outgoing[current] {
			if !strings.EqualFold(strings.TrimSpace(edge.On), "needs_rework") {
				pending = append(pending, edge.To)
			}
		}
	}
	var targets []string
	for _, nodeID := range f.graph.Topological {
		if reachable[nodeID] {
			targets = append(targets, nodeID)
		}
	}
	return targets
}

func validateResumeReplayTx(ctx context.Context, tx *sql.Tx, req ReplayTerminalRunRequest) error {
	if _, ok := req.ResumeSources[req.AnchorNodeID]; !ok || req.TargetNodeIDs[0] != req.AnchorNodeID {
		return fmt.Errorf("resume anchor must be a selected frontier source")
	}
	frontier, err := loadResumeFrontierTx(ctx, tx, req.RunID)
	if err != nil {
		return err
	}
	// Every interrupted frontier step continues together. Leaving one behind
	// would reopen the run around work that can never settle.
	if len(req.ResumeSources) != len(frontier.interrupted) {
		return fmt.Errorf("resume must include every interrupted frontier step")
	}
	targets := map[string]bool{}
	for _, nodeID := range req.TargetNodeIDs {
		if targets[nodeID] {
			return fmt.Errorf("duplicate resume replay target")
		}
		targets[nodeID] = true
	}
	for nodeID, sourceID := range req.ResumeSources {
		if !targets[nodeID] || sourceID == "" {
			return fmt.Errorf("resume source must identify a replay target")
		}
		if !frontier.isInterrupted(nodeID) {
			return fmt.Errorf("resume source %q is not currently interrupted", nodeID)
		}
		if frontier.hasInterruptedAncestor(nodeID) {
			return fmt.Errorf("resume source %q is behind an interrupted ancestor", nodeID)
		}
		expected, err := resumeSourceAttemptTx(ctx, tx, req.RunID, nodeID)
		if err != nil {
			return err
		}
		if expected == "" || sourceID != expected {
			return fmt.Errorf("resume source %q is stale", nodeID)
		}
		// Every earlier pin of this checkpoint must have ended before reaching
		// the provider; otherwise the conversation has moved on from it.
		var consumed bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM node_attempt_recoveries r
			LEFT JOIN attempt_executions ae ON ae.node_run_id=r.target_node_run_id AND ae.run_id=r.run_id
			WHERE r.checkpoint_source_attempt_id=? AND (ae.attempt_id IS NULL OR ae.state NOT IN ('committed_failure','committed_cancelled')
				OR EXISTS(SELECT 1 FROM provider_invocations pi WHERE pi.attempt_id=ae.attempt_id)))`, sourceID).Scan(&consumed); err != nil {
			return err
		}
		if consumed {
			return fmt.Errorf("resume checkpoint for %q was already used", nodeID)
		}
		if _, err := loadResumeSourceInput(ctx, tx, sourceID); err != nil {
			return fmt.Errorf("validate resume source %q: %w", nodeID, err)
		}
	}
	anchors := make([]string, 0, len(req.ResumeSources))
	for nodeID := range req.ResumeSources {
		anchors = append(anchors, nodeID)
	}
	reachable := map[string]bool{}
	for _, nodeID := range frontier.targets(anchors) {
		reachable[nodeID] = true
	}
	for _, nodeID := range req.TargetNodeIDs {
		if _, ok := frontier.graph.Nodes[nodeID]; !ok || !reachable[nodeID] {
			return fmt.Errorf("resume replay target %q is outside the selected frontier", nodeID)
		}
		if _, selected := req.ResumeSources[nodeID]; selected {
			continue
		}
		if frontier.statuses[nodeID] == "succeeded" {
			return fmt.Errorf("resume cannot repeat completed work %q", nodeID)
		}
		// A downstream step that already reached the provider needs its own
		// continuation decision; releasing dependencies cannot silently rerun it.
		var dispatched bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM attempt_executions ae JOIN node_runs nr ON nr.node_run_id=ae.node_run_id WHERE ae.run_id=? AND nr.node_id=? AND nr.node_run_id=(SELECT node_run_id FROM node_runs WHERE run_id=? AND node_id=? ORDER BY attempt DESC,node_run_id DESC LIMIT 1) AND ae.execution_mode='provider_backed' AND (ae.launch_started_at IS NOT NULL OR ae.started_at IS NOT NULL OR ae.provider_session_id<>'' OR ae.active_provider_invocation_id IS NOT NULL))`, req.RunID, nodeID, req.RunID, nodeID).Scan(&dispatched); err != nil {
			return err
		}
		if dispatched {
			return fmt.Errorf("downstream step %q already has provider work and cannot be silently rerun", nodeID)
		}
	}
	return nil
}
