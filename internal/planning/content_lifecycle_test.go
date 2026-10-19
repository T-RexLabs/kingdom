package planning

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"kingdom/internal/core/providerexecution"
	"kingdom/internal/providers/opencode"
	"kingdom/internal/state"
)

type acknowledgingPlannerAdapter struct {
	stagedPlannerAdapter
	store        *state.PlanningStore
	released     []providerexecution.ContentReference
	afterExecute func(opencode.ExecuteRequest)
}

type timedOutPlannerAdapter struct {
	*acknowledgingPlannerAdapter
	cleanupErr error
}

func (a *timedOutPlannerAdapter) Execute(ctx context.Context, req opencode.ExecuteRequest) (opencode.ExecuteResponse, error) {
	response, err := a.acknowledgingPlannerAdapter.Execute(ctx, req)
	if err != nil {
		return response, err
	}
	response.Outcome = opencode.ExecutionOutcome{State: "failed", ReasonCode: "timeout"}
	response.StreamObservation.TimedOut = true
	if a.cleanupErr != nil {
		response.StreamObservation.ReleaseContent = func() error { return a.cleanupErr }
	}
	return response, errors.New("provider stream_inactivity_timeout exceeded after 2s")
}

func TestPlanningCloseDistinguishesSettledTimeoutFromCleanupFailure(t *testing.T) {
	unproven := errors.New("provider attempt is still running; quiescence is unproven")
	for _, tc := range []struct {
		name       string
		cleanupErr error
	}{
		{name: "settled timeout"},
		{name: "unproven quiescence", cleanupErr: unproven},
		{name: "cancelled cleanup", cleanupErr: fmt.Errorf("cancel guarded process: %w", context.Canceled)},
		{name: "joined cancellation and quiescence failure", cleanupErr: errors.Join(context.Canceled, unproven)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := state.NewPlanningStore(stateTestDB(t))
			adapter := &timedOutPlannerAdapter{acknowledgingPlannerAdapter: &acknowledgingPlannerAdapter{stagedPlannerAdapter: stagedPlannerAdapter{root: t.TempDir(), text: "Terminal timeout diagnostics."}, store: store}, cleanupErr: tc.cleanupErr}
			service := NewService(store, NewOpenCodeAgent(adapter, deterministicID()), t.TempDir(), func() string { return "2026-04-21T17:10:00Z" }, deterministicID())
			defer adapter.Close()
			settled, release := make(chan struct{}), make(chan struct{})
			service.runner.afterExecuteClaimed = func() { close(settled); <-release }
			session, err := service.CreateSession(context.Background(), "Timeout lifecycle", "{}")
			if err != nil {
				t.Fatal(err)
			}
			created, err := service.CreateOperation(context.Background(), session.PlanningSessionID, CreateOperationInput{Type: state.PlanningOperationTypeReply, ReplyMarkdown: "Input"})
			if err != nil {
				t.Fatal(err)
			}
			<-settled
			op, found, err := store.GetOperation(context.Background(), created.Operation.OperationID)
			if err != nil || !found || op.Status != state.PlanningOperationStatusFailed || op.CanonicalSettlementID == nil {
				close(release)
				t.Fatalf("timeout was not canonically settled: %#v %v", op, err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- service.Close(ctx) }()
			<-service.recoveryCtx.Done() // Shutdown wins the race with runner return.
			close(release)
			closeErr := <-done
			if tc.cleanupErr == nil {
				if closeErr != nil {
					t.Fatalf("settled provider timeout became a shutdown failure: %v", closeErr)
				}
			} else {
				if closeErr == nil || !errors.Is(closeErr, tc.cleanupErr) {
					t.Fatalf("real cleanup failure was suppressed: %v", closeErr)
				}
				if strings.Contains(closeErr.Error(), "stream_inactivity_timeout") {
					t.Fatalf("shutdown mixed the settled outcome into cleanup failure: %v", closeErr)
				}
			}
		})
	}
}

func (a *acknowledgingPlannerAdapter) Execute(ctx context.Context, req opencode.ExecuteRequest) (opencode.ExecuteResponse, error) {
	response, err := a.stagedPlannerAdapter.Execute(ctx, req)
	if response.StreamObservation.Request == nil {
		return response, err
	}
	refs := append([]providerexecution.ContentReference(nil), response.StreamObservation.DiagnosticContent...)
	paths := []string{response.StreamObservation.StagedDiagnosticContent[0].Path}
	ref := a.terminal
	if response.StreamObservation.CanonicalTerminalTextRef != nil {
		refs = append([]providerexecution.ContentReference{ref}, refs...)
	}
	response.StreamObservation.ReleaseContent = func() error {
		op, found, err := a.store.GetOperation(context.Background(), req.AttemptID)
		if err != nil || !found || op.CanonicalSettlementID == nil {
			return fmt.Errorf("release before canonical settlement: %v", err)
		}
		for _, content := range refs {
			if err := a.store.CopyPlanningProviderContent(context.Background(), content, io.Discard); err != nil {
				return fmt.Errorf("release before content ingestion: %w", err)
			}
		}
		for _, released := range a.released {
			if released == ref {
				return fmt.Errorf("duplicate release")
			}
		}
		for _, path := range paths {
			if err := os.Remove(path); err != nil {
				return err
			}
		}
		a.released = append(a.released, ref)
		return nil
	}
	if a.afterExecute != nil {
		a.afterExecute(req)
	}
	return response, err
}

func TestPlanningRetiresFailedAndCancelledProviderContent(t *testing.T) {
	for _, cancelled := range []bool{false, true} {
		t.Run(fmt.Sprintf("cancelled-%v", cancelled), func(t *testing.T) {
			store := state.NewPlanningStore(stateTestDB(t))
			adapter := &acknowledgingPlannerAdapter{stagedPlannerAdapter: stagedPlannerAdapter{root: t.TempDir(), text: "Not a formalized plan."}, store: store}
			service := NewService(store, NewOpenCodeAgent(adapter, deterministicID()), t.TempDir(), func() string { return "2026-04-21T17:10:00Z" }, deterministicID())
			defer service.Close(context.Background())
			if cancelled {
				adapter.interrupted = true
				adapter.afterExecute = func(req opencode.ExecuteRequest) { service.runner.RequestCancel(req.AttemptID, "operator cancelled") }
			}
			session, err := service.CreateSession(context.Background(), "Failure lifecycle", "{}")
			if err != nil {
				t.Fatal(err)
			}
			created, err := service.CreateOperation(context.Background(), session.PlanningSessionID, CreateOperationInput{Type: state.PlanningOperationTypeFormalize})
			if err != nil {
				t.Fatal(err)
			}
			if err := service.runner.Wait(context.Background()); err != nil {
				t.Fatal(err)
			}
			op, _, err := store.GetOperation(context.Background(), created.Operation.OperationID)
			if err != nil {
				t.Fatal(err)
			}
			want := state.PlanningOperationStatusFailed
			if cancelled {
				want = state.PlanningOperationStatusCancelled
			}
			if op.Status != want || len(adapter.released) != 1 {
				t.Fatalf("terminal content not retired: status=%s releases=%d", op.Status, len(adapter.released))
			}
		})
	}
}

type commitFaultContentAgent struct {
	Agent
	beforePublish func()
}

func (a *commitFaultContentAgent) ContinueTurnStream(ctx context.Context, input AgentContext, onChunk func(string)) (AgentTurn, error) {
	turn, err := a.Agent.ContinueTurnStream(ctx, input, onChunk)
	if err != nil {
		return turn, err
	}
	open := turn.ContentSource.Open
	var once sync.Once
	turn.ContentSource.Open = func(ctx context.Context) (io.ReadCloser, error) { once.Do(a.beforePublish); return open(ctx) }
	return turn, nil
}

func TestPlanningRetiresContentAfterAmbiguousCanonicalCommit(t *testing.T) {
	connector, db := planningCommitOutcomeTestDB(t)
	store := state.NewPlanningStore(db)
	adapter := &acknowledgingPlannerAdapter{stagedPlannerAdapter: stagedPlannerAdapter{root: t.TempDir(), text: "Durable reply after uncertain commit."}, store: store}
	agent := &commitFaultContentAgent{Agent: NewOpenCodeAgent(adapter, deterministicID()), beforePublish: func() { connector.FailNextDurableCommit(nil) }}
	service := NewService(store, agent, t.TempDir(), func() string { return "2026-04-21T17:10:00Z" }, deterministicID())
	defer service.Close(context.Background())
	session, err := service.CreateSession(context.Background(), "Ambiguous finalization", "{}")
	if err != nil {
		t.Fatal(err)
	}
	created, err := service.CreateOperation(context.Background(), session.PlanningSessionID, CreateOperationInput{Type: state.PlanningOperationTypeReply, ReplyMarkdown: "Input"})
	if err != nil {
		t.Fatal(err)
	}
	if err := service.runner.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	op, _, err := store.GetOperation(context.Background(), created.Operation.OperationID)
	if err != nil {
		t.Fatal(err)
	}
	if op.Status != state.PlanningOperationStatusSucceeded || len(adapter.released) != 1 {
		t.Fatalf("ambiguous commit not resolved before retirement: status=%s releases=%d", op.Status, len(adapter.released))
	}
	events, err := store.ListOperationEventMetadataAfterSeqPage(context.Background(), op.OperationID, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	completed, failed := 0, 0
	for _, event := range events {
		if event.EventType == "run.completed" {
			completed++
		}
		if event.EventType == "run.failed" {
			failed++
		}
	}
	if completed != 1 || failed != 0 {
		t.Fatalf("ambiguous commit changed canonical events: completed=%d failed=%d", completed, failed)
	}
}

func TestPlanningRetainsStagingWhenCanonicalIngestionRollsBack(t *testing.T) {
	db := stateTestDB(t)
	store := state.NewPlanningStore(db)
	adapter := &acknowledgingPlannerAdapter{stagedPlannerAdapter: stagedPlannerAdapter{root: t.TempDir(), text: "Uncommitted reply."}, store: store}
	service := NewService(store, NewOpenCodeAgent(adapter, deterministicID()), t.TempDir(), func() string { return "2026-04-21T17:10:00Z" }, deterministicID())
	defer service.Close(context.Background())
	if _, err := db.Exec(`CREATE TRIGGER reject_content_settlement BEFORE INSERT ON planning_provider_completion_settlements BEGIN SELECT RAISE(ABORT,'settlement unavailable'); END`); err != nil {
		t.Fatal(err)
	}
	session, err := service.CreateSession(context.Background(), "Retain unresolved staging", "{}")
	if err != nil {
		t.Fatal(err)
	}
	created, err := service.CreateOperation(context.Background(), session.PlanningSessionID, CreateOperationInput{Type: state.PlanningOperationTypeReply, ReplyMarkdown: "Input"})
	if err != nil {
		t.Fatal(err)
	}
	if err := service.runner.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	op, _, err := store.GetOperation(context.Background(), created.Operation.OperationID)
	if err != nil {
		t.Fatal(err)
	}
	if op.CanonicalSettlementID != nil || len(adapter.released) != 0 {
		t.Fatal("uncommitted content was acknowledged")
	}
	for _, path := range adapter.paths {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("uncommitted staging was retired: %v", err)
		}
	}
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM planning_provider_contents`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("partial content ingestion committed: count=%d err=%v", count, err)
	}
}

func TestPlanningRetiresEachOperationWhileServiceRemainsOpen(t *testing.T) {
	store := state.NewPlanningStore(stateTestDB(t))
	adapter := &acknowledgingPlannerAdapter{stagedPlannerAdapter: stagedPlannerAdapter{root: t.TempDir()}, store: store}
	service := NewService(store, NewOpenCodeAgent(adapter, deterministicID()), t.TempDir(), func() string { return "2026-04-21T17:10:00Z" }, deterministicID())
	defer service.Close(context.Background())
	for round := 0; round < 2; round++ {
		for _, kind := range []state.PlanningOperationType{state.PlanningOperationTypeReply, state.PlanningOperationTypeFormalize, state.PlanningOperationTypeGenerateTitle} {
			title := "Content lifecycle"
			adapter.text = "Reply content.\n\n```structured_question\n{\"prompt\":\"Proceed?\",\"type\":\"yes_no\"}\n```"
			if kind == state.PlanningOperationTypeFormalize {
				adapter.text = "# Complete Plan\n\n## Objective\nFull content."
			}
			if kind == state.PlanningOperationTypeGenerateTitle {
				title = ""
				adapter.text = "Complete Title"
			}
			session, err := service.CreateSession(context.Background(), title, "{}")
			if err != nil {
				t.Fatal(err)
			}
			before := len(adapter.released)
			var operation state.PlanningOperation
			if kind == state.PlanningOperationTypeGenerateTitle {
				_, operation, err = service.CreateGenerateTitleOperation(context.Background(), session.PlanningSessionID)
			} else {
				var created CreateOperationResult
				created, err = service.CreateOperation(context.Background(), session.PlanningSessionID, CreateOperationInput{Type: kind, ReplyMarkdown: "Input"})
				operation = created.Operation
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := service.runner.Wait(context.Background()); err != nil {
				t.Fatal(err)
			}
			op, found, err := store.GetOperation(context.Background(), operation.OperationID)
			if err != nil || !found || op.Status != state.PlanningOperationStatusSucceeded {
				t.Fatalf("operation=%#v err=%v", op, err)
			}
			if len(adapter.released) != before+1 {
				t.Fatalf("%s retained staging after canonical ingestion: releases=%d want=%d", kind, len(adapter.released), before+1)
			}
			files, err := os.ReadDir(adapter.root)
			if err != nil || len(files) != 0 {
				t.Fatalf("staging retained while service is open: %d files %v", len(files), err)
			}
			var body strings.Builder
			if err := store.CopyPlanningProviderContent(context.Background(), adapter.terminal, &body); err != nil || body.String() != adapter.text {
				t.Fatalf("canonical replay after release: %v", err)
			}
		}
	}
}
