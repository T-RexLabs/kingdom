package state

import (
	"context"
	"errors"
	"strings"
	"testing"

	"kingdom/internal/acp"
	"kingdom/internal/testutil"
)

func TestPlanningOperationBundleCommitErrorsIdentifyUnknownOutcome(t *testing.T) {
	tests := []struct {
		name   string
		typeOf PlanningOperationType
		create func(*PlanningStore, CreatePlanningOperationRequest, *AppendPlanningOperationEventRequest) error
	}{
		{
			name:   "reply",
			typeOf: PlanningOperationTypeReply,
			create: func(store *PlanningStore, operation CreatePlanningOperationRequest, event *AppendPlanningOperationEventRequest) error {
				_, err := store.CreateReplyBundle(context.Background(), CreatePlanningReplyBundleRequest{
					SessionID: "ps_bundle_commit",
					UserMessage: AppendPlanningMessageRequest{
						PlanningMessageID:   "pm_bundle_reply",
						PlanningSessionID:   "ps_bundle_commit",
						ParentMessageID:     "pm_bundle_root",
						Role:                PlanningMessageRoleUser,
						ContentMarkdown:     "reply",
						QuestionPayloadJSON: `{}`,
						AnswerPayloadJSON:   `{}`,
						CreatedAt:           "2026-09-25T12:00:01Z",
					},
					Operation: operation, InitialEvent: event,
					ConversationState: PlanningSessionConversationStateProcessing,
					UpdatedAt:         "2026-09-25T12:00:01Z",
				})
				return err
			},
		},
		{
			name:   "regenerate",
			typeOf: PlanningOperationTypeRegenerate,
			create: func(store *PlanningStore, operation CreatePlanningOperationRequest, event *AppendPlanningOperationEventRequest) error {
				_, err := store.CreateRegenerateBundle(context.Background(), CreateRegenerateBundleRequest{
					SessionID: "ps_bundle_commit", ActiveLeafMessageID: "pm_bundle_root",
					Operation: operation, InitialEvent: event,
					ConversationState: PlanningSessionConversationStateProcessing,
					UpdatedAt:         "2026-09-25T12:00:01Z",
				})
				return err
			},
		},
		{
			name:   "formalize",
			typeOf: PlanningOperationTypeFormalize,
			create: func(store *PlanningStore, operation CreatePlanningOperationRequest, event *AppendPlanningOperationEventRequest) error {
				_, err := store.CreateFormalizeBundle(context.Background(), CreatePlanningFormalizeBundleRequest{
					SessionID: "ps_bundle_commit", Operation: operation, InitialEvent: event,
					ConversationState:  PlanningSessionConversationStateProcessing,
					CurrentOperationID: operation.OperationID,
					UpdatedAt:          "2026-09-25T12:00:01Z",
				})
				return err
			},
		},
		{
			name:   "generate title",
			typeOf: PlanningOperationTypeGenerateTitle,
			create: func(store *PlanningStore, operation CreatePlanningOperationRequest, event *AppendPlanningOperationEventRequest) error {
				_, err := store.CreateGenerateTitleBundle(context.Background(), CreatePlanningGenerateTitleBundleRequest{
					SessionID: "ps_bundle_commit", Operation: operation, InitialEvent: event,
					CurrentTitleOperationID: operation.OperationID,
					UpdatedAt:               "2026-09-25T12:00:01Z",
				})
				return err
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			connector := testutil.NewSQLiteCommitFaultConnector(":memory:")
			db := connector.OpenDB()
			db.SetMaxOpenConns(1)
			t.Cleanup(func() { _ = db.Close() })
			if _, err := db.Exec(`PRAGMA foreign_keys = ON`); err != nil {
				t.Fatalf("enable foreign keys: %v", err)
			}
			applyMigration(t, db, "0001_initial_schema.up.sql")

			store := NewPlanningStore(db)
			ctx := context.Background()
			if _, err := store.CreateSession(ctx, CreatePlanningSessionRequest{
				PlanningSessionID:  "ps_bundle_commit",
				Title:              "Bundle commit",
				TitleState:         "custom",
				LifecycleState:     PlanningSessionLifecycleStateActive,
				ConversationState:  PlanningSessionConversationStateReady,
				ProjectRoot:        "/workspace/project",
				ModelSelectionJSON: `{"model":"test-model"}`,
				CreatedAt:          "2026-09-25T12:00:00Z",
				UpdatedAt:          "2026-09-25T12:00:00Z",
			}); err != nil {
				t.Fatalf("create session: %v", err)
			}
			if _, err := store.AppendMessage(ctx, AppendPlanningMessageRequest{
				PlanningMessageID: "pm_bundle_root", PlanningSessionID: "ps_bundle_commit",
				Role: PlanningMessageRoleAssistant, ContentMarkdown: "Start",
				QuestionPayloadJSON: `{}`, AnswerPayloadJSON: `{}`,
				CreatedAt: "2026-09-25T12:00:00Z",
			}); err != nil {
				t.Fatalf("create root message: %v", err)
			}
			bindTestPlanningContext(t, db, "ps_bundle_commit")

			operationID := "po_bundle_commit_" + strings.ReplaceAll(test.name, " ", "_")
			operation := CreatePlanningOperationRequest{
				OperationID: operationID, PlanningSessionID: "ps_bundle_commit", Type: test.typeOf,
				Status: PlanningOperationStatusQueued, RequestPayloadJSON: `{"request":"expected"}`,
				CreatedAt: "2026-09-25T12:00:01Z",
			}
			event := &AppendPlanningOperationEventRequest{
				EventID: operationID + ":1", OperationID: operationID, Seq: 1,
				EventType: acp.RunCreatedEventType, PayloadJSON: `{"status":"created"}`,
				CreatedAt: "2026-09-25T12:00:01Z",
			}
			connector.FailNextDurableCommit(nil)

			err := test.create(store, operation, event)
			var unknown *CommitOutcomeUnknownError
			if !errors.As(err, &unknown) {
				t.Fatalf("bundle error=%T %v, want CommitOutcomeUnknownError", err, err)
			}
			if unknown.ResourceID != operationID {
				t.Fatalf("unknown outcome resource id=%q, want %q", unknown.ResourceID, operationID)
			}
			if !errors.Is(err, testutil.ErrDurableCommit) {
				t.Fatalf("unknown outcome does not retain commit cause: %v", err)
			}

			persisted, found, err := store.GetOperation(ctx, operationID)
			if err != nil || !found {
				t.Fatalf("read durable operation: found=%t err=%v", found, err)
			}
			if persisted.Type != test.typeOf || persisted.Status != PlanningOperationStatusQueued || persisted.RequestPayloadJSON != operation.RequestPayloadJSON {
				t.Fatalf("persisted operation does not match queued bundle: %+v", persisted)
			}
			storedEvents, err := store.ListOperationEventsAfterSeqPage(ctx, operationID, 0, 10)
			if err != nil || len(storedEvents) != 1 || storedEvents[0].EventID != event.EventID {
				t.Fatalf("persisted initial event bundle=%+v err=%v", storedEvents, err)
			}
			if test.typeOf == PlanningOperationTypeReply {
				if _, err := store.GetMessageInSession(ctx, "ps_bundle_commit", "pm_bundle_reply"); err != nil {
					t.Fatalf("read durable reply user message: %v", err)
				}
			}
		})
	}
}
