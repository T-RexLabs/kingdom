//go:build darwin || linux

package processguardian

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"kingdom/internal/core/providerexecution"
	"kingdom/internal/dbmigrations"
	"kingdom/internal/execution/providerrequest"
	executionstore "kingdom/internal/execution/store"
	"kingdom/internal/testutil"
)

func TestRunnerKeepsGateClosedAndPublishesEvidenceAfterConfirmedCommitError(t *testing.T) {
	connector := testutil.NewSQLiteCommitFaultConnector(":memory:")
	db := connector.OpenDB()
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(`PRAGMA foreign_keys = ON`); err != nil {
		t.Fatalf("enable foreign keys: %v", err)
	}
	store, acknowledgement := processGuardianCommitOutcomeFixture(t, db)

	runtimeRoot := t.TempDir()
	marker := filepath.Join(t.TempDir(), "provider-ran")
	runner, err := NewRunner(Config{RuntimeRoot: runtimeRoot, CancelGrace: 100 * time.Millisecond, CancelForce: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	connector.FailNextDurableCommit(cancel)
	var persistedOwnership Ownership
	_, err = runner.Start(ctx, Command{Path: "/bin/sh", Args: []string{"-c", "echo provider > " + shellQuote(marker)}, Env: os.Environ()}, func(callbackCtx context.Context, ownership Ownership) error {
		persistedOwnership = ownership
		acknowledgement.GuardID = ownership.GuardID
		acknowledgement.Platform = ownership.Platform
		acknowledgement.GuardianPID = int64(ownership.GuardianPID)
		acknowledgement.GuardianIdentityToken = ownership.GuardianIdentityToken
		acknowledgement.ChildPID = int64(ownership.ChildPID)
		acknowledgement.ChildIdentityToken = ownership.ChildIdentityToken
		acknowledgement.TreeID = fmt.Sprintf("%d", ownership.TreeID)
		acknowledgement.TreeIdentityToken = ownership.TreeIdentityToken
		acknowledgement.QuiescenceCapability = ownership.QuiescenceCapability
		acknowledgement.StartedAt = time.Now().UTC().Format(time.RFC3339Nano)
		acknowledgement.AcknowledgedAt = time.Now().UTC().Format(time.RFC3339Nano)
		_, callbackErr := store.AcknowledgeProviderProcessGuard(callbackCtx, acknowledgement)
		return callbackErr
	})
	var launchErr *LaunchError
	if !errors.As(err, &launchErr) || !errors.Is(err, context.Canceled) {
		t.Fatalf("expected canceled launch with durable abort result, got %v", err)
	}
	if !launchErr.OwnershipPersisted || !launchErr.Result.Quiescent || launchErr.Result.EvidenceSHA256 == "" || launchErr.Result.EvidenceJSON == "" {
		t.Fatalf("confirmed persisted acknowledgement did not produce durable abort evidence: %#v", launchErr.Result)
	}
	if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("provider escaped the gated launch after cancellation: %v", err)
	}
	if persistedOwnership.GuardID == "" || persistedOwnership.QuiescenceCapability == "" {
		t.Fatalf("ownership callback did not receive guardian lineage: %+v", persistedOwnership)
	}
	evidence, digest, raw, err := ReadQuiescenceEvidence(runtimeRoot, persistedOwnership.GuardID)
	if err != nil || digest != launchErr.Result.EvidenceSHA256 || raw != launchErr.Result.EvidenceJSON || evidence.QuiescenceCapability != persistedOwnership.QuiescenceCapability {
		t.Fatalf("read durable abort evidence: digest=%q result=%+v err=%v", digest, launchErr.Result, err)
	}
	quiescentAt := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := store.IngestProviderProcessGuardQuiescence(context.Background(), executionstore.IngestProviderProcessGuardQuiescenceRequest{
		GuardID: persistedOwnership.GuardID, ProviderInvocationID: acknowledgement.ProviderInvocationID,
		AttemptID: acknowledgement.AttemptID, RunID: acknowledgement.RunID, NodeRunID: acknowledgement.NodeRunID,
		Platform: persistedOwnership.Platform, GuardianPID: int64(persistedOwnership.GuardianPID), GuardianIdentityToken: persistedOwnership.GuardianIdentityToken,
		ChildPID: int64(persistedOwnership.ChildPID), ChildIdentityToken: persistedOwnership.ChildIdentityToken,
		TreeID: fmt.Sprintf("%d", persistedOwnership.TreeID), TreeIdentityToken: persistedOwnership.TreeIdentityToken,
		QuiescenceCapability: persistedOwnership.QuiescenceCapability, ExitedAt: quiescentAt,
		QuiescentAt: quiescentAt, ExitCode: int64(evidence.ExitCode), ExitSignal: int64(evidence.ExitSignal),
		TerminationForced: evidence.Forced, FailureCode: evidence.FailureCode, FailureText: evidence.FailureText,
		EvidenceSHA256: digest, EvidenceJSON: raw,
	}); err != nil {
		t.Fatalf("ingest durable aborted launch evidence: %v", err)
	}
	var lifecycle string
	if err := db.QueryRow(`SELECT lifecycle FROM provider_process_guards WHERE guard_id = ?`, persistedOwnership.GuardID).Scan(&lifecycle); err != nil || lifecycle != string(executionstore.ProviderProcessGuardLifecycleQuiescent) {
		t.Fatalf("durable abort evidence did not quiesce the persisted guard: lifecycle=%q err=%v", lifecycle, err)
	}
}

func processGuardianCommitOutcomeFixture(t *testing.T, db *sql.DB) (*executionstore.Store, executionstore.AcknowledgeProviderProcessGuardRequest) {
	t.Helper()
	names, err := dbmigrations.List()
	if err != nil {
		t.Fatalf("list database migrations: %v", err)
	}
	for _, name := range names {
		raw, err := dbmigrations.Read(name)
		if err != nil {
			t.Fatalf("read migration %q: %v", name, err)
		}
		if _, err := db.Exec(string(raw)); err != nil {
			t.Fatalf("apply migration %q: %v", name, err)
		}
	}
	for _, statement := range []string{
		`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('governance', 'Governance', 1, '2026-03-01T12:00:00Z')`,
		`INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_governance', 'governance', 'idle', NULL, 0, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`,
		`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_1', 'D1', 'text', '{}', '2026-03-01T12:00:00Z')`,
		`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_1', 'work-order', 'v1', '{}', '2026-03-01T12:00:00Z')`,
		`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_1', 'wfdef_1', 'work_intake', 'governance', 'work_intake', '2026-03-01T12:00:00Z')`,
		`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_2', 'wfdef_1', 'completion', 'governance', 'completion', '2026-03-01T12:00:00Z')`,
		`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_1', 'wfdef_1', 'work_intake', 'completion', 'always', '2026-03-01T12:00:00Z')`,
		`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_1', NULL, 'work_order_1', 'wfdef_1', 'running', 'draft', 1, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`,
		`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_1', 'run_1', 'work_intake', 1, 1, 'running', 'governance', 'agent_governance', 1, NULL, NULL)`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatalf("seed provider attempt fixture: %v", err)
		}
	}

	store := executionstore.New(db)
	ctx := context.Background()
	startedAt := time.Now().UTC()
	startedAtText := startedAt.Format(time.RFC3339Nano)
	if err := store.EnqueueAttempt(ctx, executionstore.EnqueueAttemptRequest{
		AttemptID: "run_1:work_intake:1", RunID: "run_1", NodeRunID: "nr_1",
		ExecutionMode: executionstore.ExecutionModeProviderBacked, CompletionMode: executionstore.ProviderCompletionModeServerResponse,
		CreatedAt: startedAtText,
	}); err != nil {
		t.Fatalf("enqueue provider attempt: %v", err)
	}
	leaseExpiresAt := startedAt.Add(5 * time.Minute).Format(time.RFC3339Nano)
	claim, acquired, err := store.ClaimQueuedProviderAttempt(ctx, executionstore.ClaimQueuedProviderAttemptRequest{
		AttemptID: "run_1:work_intake:1", LeaseOwner: "worker-commit-test", LeaseUntil: leaseExpiresAt, ClaimedAt: startedAtText,
	})
	if err != nil || !acquired || claim.LeaseVersion != 1 {
		t.Fatalf("claim provider attempt: claim=%#v acquired=%t err=%v", claim, acquired, err)
	}
	if _, err := db.Exec(`UPDATE attempt_executions SET state = 'running', started_at = ? WHERE attempt_id = ?`, startedAtText, claim.AttemptID); err != nil {
		t.Fatalf("mark provider attempt running: %v", err)
	}
	invocationID := "invocation_commit_outcome"
	identity := providerexecution.RequestIdentity{InstanceID: "instance", Generation: "generation", Location: "/project", SessionID: "session_commit_outcome", ProfileID: "profile", RequestID: "request"}
	if _, err := store.ActivateProviderInvocation(ctx, executionstore.ActivateProviderInvocationRequest{
		RequestIdentity: identity, ProviderInvocationID: invocationID, AttemptID: claim.AttemptID, RunID: claim.RunID, NodeRunID: claim.NodeRunID,
		LogicalSessionID: "logical_commit_outcome", ProviderSessionID: "session_commit_outcome",
		CompletionMode: executionstore.ProviderCompletionModeServerResponse,
		IssuedAt:       startedAtText, CreatedAt: startedAtText, OpenedAt: startedAtText,
		ExpectedLeaseOwner: claim.LeaseOwner, ExpectedLeaseVersion: claim.LeaseVersion,
	}); err != nil {
		t.Fatalf("activate provider invocation: %v", err)
	}
	// The server dispatches the request and admits the command that the
	// guardian will launch.
	binding, err := providerrequest.Get(ctx, db, invocationID)
	if err != nil {
		t.Fatalf("load request binding: %v", err)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	now := time.Now()
	if err := providerrequest.IssueDispatchTx(ctx, tx, binding.ID, binding.Identity, binding.Lease, now); err != nil {
		t.Fatalf("issue dispatch: %v", err)
	}
	if _, err := providerrequest.ClaimCommandTx(ctx, tx, providerrequest.NativeCall{BindingID: binding.ID, Identity: binding.Identity, MessageID: "message", CallID: "call", Tool: "shell", Input: []byte(`{"command":"true"}`)}, now); err != nil {
		t.Fatalf("claim command: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	return store, executionstore.AcknowledgeProviderProcessGuardRequest{
		ProviderInvocationID: invocationID, AttemptID: claim.AttemptID, RunID: claim.RunID, NodeRunID: claim.NodeRunID,
		CommandOrdinal: 1, ExpectedLeaseOwner: claim.LeaseOwner, ExpectedLeaseVersion: claim.LeaseVersion,
	}
}
