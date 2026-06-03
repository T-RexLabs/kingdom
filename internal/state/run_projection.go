package state

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
)

type runProjection struct {
	AutonomousApprovals bool
	CreatedAt           string
	DecreeID            string
	DecreeTitle         string
	DisplayState        string
	LastActivityAt      string
	ModelSelection      map[string]any
	PendingApprovalKey  string
	PlanState           string
	PublishMode         string
	RunID               string
	Status              string
	UpdatedAt           string
	WorkflowID          string
}

func loadRunProjectionTx(ctx context.Context, tx *sql.Tx, runID string) (runProjection, error) {
	trimmedRunID := strings.TrimSpace(runID)
	if tx == nil {
		return runProjection{}, fmt.Errorf("run projection transaction is required")
	}
	if trimmedRunID == "" {
		return runProjection{}, fmt.Errorf("run projection run id is required")
	}

	var projection runProjection
	var autonomousApprovals int
	var modelSelectionJSON string
	var pendingApprovalKey sql.NullString
	err := tx.QueryRowContext(ctx,
		`SELECT wd.workflow_name,
		        wr.decree_id,
		        d.decree_title,
		        wr.run_status,
		        wr.plan_state,
		        wr.created_at,
		        wr.updated_at,
		        ra.last_activity_at,
		        COALESCE(wr.autonomous_approvals, 1),
		        COALESCE(wr.publish_mode, 'direct'),
		        COALESCE(wr.model_selection_json, '{}'),
		        `+PendingApprovalKeySelectExpr("wr")+`
		 FROM workflow_runs wr
		 JOIN run_activity ra ON ra.run_id = wr.run_id
		 JOIN workflow_definitions wd ON wd.workflow_definition_id = wr.workflow_definition_id
		 JOIN decrees d ON d.decree_id = wr.decree_id
		 WHERE wr.run_id = ?`,
		trimmedRunID,
	).Scan(&projection.WorkflowID, &projection.DecreeID, &projection.DecreeTitle, &projection.Status, &projection.PlanState, &projection.CreatedAt, &projection.UpdatedAt, &projection.LastActivityAt, &autonomousApprovals, &projection.PublishMode, &modelSelectionJSON, &pendingApprovalKey)
	if err != nil {
		return runProjection{}, fmt.Errorf("load run projection for %q: %w", trimmedRunID, err)
	}

	projection.RunID = trimmedRunID
	projection.AutonomousApprovals = autonomousApprovals != 0
	projection.DisplayState = DeriveRunDisplayState(projection.Status, projection.PlanState)
	if pendingApprovalKey.Valid {
		projection.PendingApprovalKey = strings.TrimSpace(pendingApprovalKey.String)
	}

	decodedModelSelection, err := decodeProjectedModelSelection(modelSelectionJSON)
	if err != nil {
		return runProjection{}, fmt.Errorf("decode run model selection for %q: %w", trimmedRunID, err)
	}
	projection.ModelSelection = decodedModelSelection
	return projection, nil
}

func decodeProjectedModelSelection(raw string) (map[string]any, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil, nil
	}
	var decoded any
	if err := json.Unmarshal([]byte(trimmed), &decoded); err != nil {
		return nil, err
	}
	modelSelectionMap, ok := decoded.(map[string]any)
	if !ok || len(modelSelectionMap) == 0 {
		return nil, nil
	}
	return modelSelectionMap, nil
}

func projectRunStateChangedPayloadTx(ctx context.Context, tx *sql.Tx, runID string, payloadJSON string) (string, error) {
	projection, err := loadRunProjectionTx(ctx, tx, runID)
	if err != nil {
		return "", err
	}

	payload := map[string]any{}
	trimmedPayload := strings.TrimSpace(payloadJSON)
	if trimmedPayload != "" {
		if err := json.Unmarshal([]byte(trimmedPayload), &payload); err != nil {
			return "", fmt.Errorf("decode run.state_changed payload for %q: %w", strings.TrimSpace(runID), err)
		}
	}

	payload["run_id"] = projection.RunID
	payload["status"] = projection.Status
	payload["plan_state"] = projection.PlanState
	payload["display_state"] = projection.DisplayState
	payload["updated_at"] = projection.UpdatedAt
	payload["last_activity_at"] = projection.LastActivityAt

	encoded, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("encode run.state_changed payload for %q: %w", strings.TrimSpace(runID), err)
	}
	return string(encoded), nil
}
