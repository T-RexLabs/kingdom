package planning

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"kingdom/internal/acp"
	"kingdom/internal/dbmigrations"
	"kingdom/internal/state"
	"kingdom/internal/testutil"
)

func TestServiceReconcilesDurableCommitOutcomeForEveryOperationBundle(t *testing.T) {
	tests := []struct {
		name        string
		typeOf      state.PlanningOperationType
		title       string
		create      func(*Service, state.PlanningSession) (state.PlanningOperation, error)
		verifyAgain bool
	}{
		{
			name:   "reply",
			typeOf: state.PlanningOperationTypeReply,
			title:  "Reply commit outcome",
			create: func(service *Service, session state.PlanningSession) (state.PlanningOperation, error) {
				result, err := service.CreateOperation(context.Background(), session.PlanningSessionID, CreateOperationInput{
					Type: state.PlanningOperationTypeReply, ReplyMarkdown: "A reply after an uncertain commit",
					AnswerPayloadJSON: `{"text":"A reply after an uncertain commit"}`,
				})
				return result.Operation, err
			},
		},
		{
			name:   "regenerate",
			typeOf: state.PlanningOperationTypeRegenerate,
			title:  "Regenerate commit outcome",
			create: func(service *Service, session state.PlanningSession) (state.PlanningOperation, error) {
				result, err := service.CreateOperation(context.Background(), session.PlanningSessionID, CreateOperationInput{
					Type: state.PlanningOperationTypeRegenerate, TargetMessageID: *session.ActiveLeafMessageID,
				})
				return result.Operation, err
			},
		},
		{
			name:   "formalize",
			typeOf: state.PlanningOperationTypeFormalize,
			title:  "Formalize commit outcome",
			create: func(service *Service, session state.PlanningSession) (state.PlanningOperation, error) {
				result, err := service.CreateOperation(context.Background(), session.PlanningSessionID, CreateOperationInput{Type: state.PlanningOperationTypeFormalize})
				return result.Operation, err
			},
		},
		{
			name:        "generate title",
			typeOf:      state.PlanningOperationTypeGenerateTitle,
			title:       "",
			verifyAgain: true,
			create: func(service *Service, session state.PlanningSession) (state.PlanningOperation, error) {
				_, operation, err := service.CreateGenerateTitleOperation(context.Background(), session.PlanningSessionID)
				return operation, err
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			connector, db := planningCommitOutcomeTestDB(t)
			store := state.NewPlanningStore(db)
			projectRoot := t.TempDir()
			writeTestFile(t, projectRoot, "README.md", "# Commit outcome\n")

			agent := &commitOutcomeBlockingPlanningAgent{started: make(chan struct{}), release: make(chan struct{})}
			var releaseOnce sync.Once
			service := NewService(store, agent, projectRoot, func() string { return "2026-09-25T12:00:00Z" }, deterministicID())
			t.Cleanup(func() {
				closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if err := service.Close(closeCtx); err != nil {
					t.Errorf("close planning service: %v", err)
				}
			})
			t.Cleanup(func() {
				releaseOnce.Do(func() { close(agent.release) })
			})

			session, err := service.CreateSession(context.Background(), test.title, `{"model":"test-model"}`)
			if err != nil {
				t.Fatalf("create planning session: %v", err)
			}
			connector.FailNextDurableCommit(nil)

			operation, err := test.create(service, session)
			if err != nil {
				t.Fatalf("create %s operation after durable commit error: %v", test.typeOf, err)
			}
			if strings.TrimSpace(operation.OperationID) == "" || operation.Type != test.typeOf {
				t.Fatalf("reconstructed operation=%+v", operation)
			}
			select {
			case <-agent.started:
			case <-time.After(5 * time.Second):
				t.Fatalf("durably reconstructed %s operation was not dispatched", test.typeOf)
			}

			if test.verifyAgain {
				_, existing, err := service.CreateGenerateTitleOperation(context.Background(), session.PlanningSessionID)
				if err != nil {
					t.Fatalf("get already-active title operation: %v", err)
				}
				if existing.OperationID != operation.OperationID {
					t.Fatalf("active title operation id=%q, want existing %q", existing.OperationID, operation.OperationID)
				}
			} else {
				_, err := service.CreateOperation(context.Background(), session.PlanningSessionID, CreateOperationInput{
					Type: state.PlanningOperationTypeReply, ReplyMarkdown: "This must not create a second interactive operation",
				})
				if !errors.Is(err, ErrInteractiveOperationActive) {
					t.Fatalf("second active interactive operation error=%v, want ErrInteractiveOperationActive", err)
				}
			}

			var operationCount, runCreatedCount, userMessageCount int
			if err := db.QueryRow(`SELECT COUNT(*) FROM planning_operations WHERE planning_session_id = ?`, session.PlanningSessionID).Scan(&operationCount); err != nil {
				t.Fatalf("count session operations: %v", err)
			}
			if err := db.QueryRow(`SELECT COUNT(*) FROM planning_messages WHERE planning_session_id = ? AND role = 'user'`, session.PlanningSessionID).Scan(&userMessageCount); err != nil {
				t.Fatalf("count committed user messages: %v", err)
			}
			if err := db.QueryRow(`SELECT COUNT(*) FROM planning_operation_events WHERE operation_id = ? AND event_type = ?`, operation.OperationID, acp.RunCreatedEventType).Scan(&runCreatedCount); err != nil {
				t.Fatalf("count initial ACP run events: %v", err)
			}
			wantUserMessages := 0
			if test.typeOf == state.PlanningOperationTypeReply {
				wantUserMessages = 1
			}
			if operationCount != 1 || userMessageCount != wantUserMessages || runCreatedCount != 1 || agent.calls.Load() != 1 {
				t.Fatalf("operation_count=%d user_message_count=%d run_created_count=%d provider_calls=%d, want 1/%d/1/1", operationCount, userMessageCount, runCreatedCount, agent.calls.Load(), wantUserMessages)
			}

			releaseOnce.Do(func() { close(agent.release) })
			testutil.Eventually(t, 5*time.Second, 10*time.Millisecond, func() error {
				persisted, found, err := store.GetOperation(context.Background(), operation.OperationID)
				if err != nil {
					return err
				}
				if !found {
					return fmt.Errorf("operation %q is missing", operation.OperationID)
				}
				if persisted.Status != state.PlanningOperationStatusSucceeded {
					return fmt.Errorf("operation status is %q", persisted.Status)
				}
				return nil
			})
			if calls := agent.calls.Load(); calls != 1 {
				t.Fatalf("provider calls=%d after completion, want exactly 1", calls)
			}
		})
	}
}

type commitOutcomeBlockingPlanningAgent struct {
	mockAgent
	started     chan struct{}
	release     chan struct{}
	startedOnce sync.Once
	calls       atomic.Int32
}

func (a *commitOutcomeBlockingPlanningAgent) wait(ctx context.Context) error {
	a.calls.Add(1)
	a.startedOnce.Do(func() { close(a.started) })
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-a.release:
		return nil
	}
}

func (a *commitOutcomeBlockingPlanningAgent) ContinueTurnStream(ctx context.Context, input AgentContext, onChunk func(string)) (AgentTurn, error) {
	if err := a.wait(ctx); err != nil {
		return AgentTurn{}, err
	}
	return a.mockAgent.ContinueTurnStream(ctx, input, onChunk)
}

func (a *commitOutcomeBlockingPlanningAgent) FormalizePlanStream(ctx context.Context, input AgentContext, onChunk func(string)) (FormalizedPlan, error) {
	if err := a.wait(ctx); err != nil {
		return FormalizedPlan{}, err
	}
	return a.mockAgent.FormalizePlanStream(ctx, input, onChunk)
}

func (a *commitOutcomeBlockingPlanningAgent) GenerateTitle(ctx context.Context, input AgentContext, onChunk func(string)) (GeneratedTitle, error) {
	if err := a.wait(ctx); err != nil {
		return GeneratedTitle{}, err
	}
	return a.mockAgent.GenerateTitle(ctx, input, onChunk)
}

func planningCommitOutcomeTestDB(t *testing.T) (*testutil.SQLiteCommitFaultConnector, *sql.DB) {
	t.Helper()
	connector := testutil.NewSQLiteCommitFaultConnector(":memory:")
	db := connector.OpenDB()
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(`PRAGMA foreign_keys = ON`); err != nil {
		t.Fatalf("enable foreign keys: %v", err)
	}
	names, err := dbmigrations.List()
	if err != nil {
		t.Fatalf("list migrations: %v", err)
	}
	sort.Strings(names)
	for _, name := range names {
		if !strings.HasSuffix(name, ".up.sql") {
			continue
		}
		migration, err := dbmigrations.Read(name)
		if err != nil {
			t.Fatalf("read migration %s: %v", name, err)
		}
		if _, err := db.Exec(string(migration)); err != nil {
			t.Fatalf("apply migration %s: %v", name, err)
		}
	}
	return connector, db
}
