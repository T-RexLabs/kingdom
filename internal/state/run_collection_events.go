package state

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
)

const (
	RunCollectionEventTypeCreated          = "run.created"
	RunCollectionEventTypeApprovalRecorded = "approval.recorded"
)

type RunCollectionEventAppendRequest struct {
	EventID     string
	RunID       string
	EventType   string
	PayloadJSON string
	CreatedAt   string
}

func AppendRunCollectionEventTx(ctx context.Context, tx *sql.Tx, req RunCollectionEventAppendRequest) error {
	if tx == nil {
		return fmt.Errorf("run collection event transaction is required")
	}
	if strings.TrimSpace(req.EventID) == "" {
		return fmt.Errorf("run collection event id is required")
	}
	if strings.TrimSpace(req.RunID) == "" {
		return fmt.Errorf("run collection event run id is required")
	}
	if strings.TrimSpace(req.PayloadJSON) == "" {
		return fmt.Errorf("run collection event payload is required")
	}
	if strings.TrimSpace(req.CreatedAt) == "" {
		return fmt.Errorf("run collection event created_at is required")
	}
	if !isSupportedRunCollectionEventType(req.EventType) {
		return fmt.Errorf("unsupported run collection event type %q", req.EventType)
	}

	projectedPayload, err := projectRunCollectionPayloadTx(ctx, tx, req)
	if err != nil {
		return err
	}

	_, err = tx.ExecContext(ctx,
		`INSERT INTO run_collection_events (event_id, run_id, event_type, payload_json, created_at)
		 VALUES (?, ?, ?, ?, ?)`,
		strings.TrimSpace(req.EventID),
		strings.TrimSpace(req.RunID),
		strings.TrimSpace(req.EventType),
		projectedPayload,
		strings.TrimSpace(req.CreatedAt),
	)
	if err != nil {
		return fmt.Errorf("insert run collection event %q: %w", req.EventID, err)
	}
	return nil
}

func isSupportedRunCollectionEventType(eventType string) bool {
	trimmed := strings.TrimSpace(eventType)
	if trimmed == RunCollectionEventTypeApprovalRecorded {
		return true
	}
	if strings.HasPrefix(trimmed, "run.") {
		return true
	}
	return false
}

func projectRunCollectionPayloadTx(ctx context.Context, tx *sql.Tx, req RunCollectionEventAppendRequest) (string, error) {
	payload := map[string]any{}
	trimmedPayload := strings.TrimSpace(req.PayloadJSON)
	if trimmedPayload != "" {
		if err := json.Unmarshal([]byte(trimmedPayload), &payload); err != nil {
			return "", fmt.Errorf("decode run collection payload %q: %w", req.EventID, err)
		}
	}

	projection, err := loadRunProjectionTx(ctx, tx, req.RunID)
	if err != nil {
		return "", fmt.Errorf("project run collection payload for %q: %w", req.RunID, err)
	}

	payload["run_id"] = projection.RunID
	payload["workflow_id"] = projection.WorkflowID
	payload["decree_id"] = projection.DecreeID
	payload["decree_title"] = projection.DecreeTitle
	payload["status"] = projection.Status
	payload["plan_state"] = projection.PlanState
	payload["display_state"] = projection.DisplayState
	payload["created_at"] = projection.CreatedAt
	payload["updated_at"] = projection.UpdatedAt
	payload["last_activity_at"] = projection.LastActivityAt
	payload["autonomous_approvals"] = projection.AutonomousApprovals
	if projection.PendingApprovalKey != "" {
		payload["pending_approval_key"] = projection.PendingApprovalKey
	}
	payload["publish_mode"] = projection.PublishMode
	if len(projection.ModelSelection) > 0 {
		payload["model_selection"] = projection.ModelSelection
	}

	encoded, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("encode projected run collection payload %q: %w", req.EventID, err)
	}
	return string(encoded), nil
}
