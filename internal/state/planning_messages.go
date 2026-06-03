package state

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

func (s *PlanningStore) AppendMessage(ctx context.Context, req AppendPlanningMessageRequest) (PlanningMessage, error) {
	if err := validatePlanningMessageAppend(req); err != nil {
		return PlanningMessage{}, err
	}
	return insertPlanningMessage(ctx, s.db, req)
}

func insertPlanningMessage(ctx context.Context, exec sqlExecutor, req AppendPlanningMessageRequest) (PlanningMessage, error) {
	row := PlanningMessage{
		PlanningMessageID:   strings.TrimSpace(req.PlanningMessageID),
		PlanningSessionID:   strings.TrimSpace(req.PlanningSessionID),
		TurnIndex:           req.TurnIndex,
		Role:                req.Role,
		ContentMarkdown:     req.ContentMarkdown,
		QuestionPayloadJSON: strings.TrimSpace(req.QuestionPayloadJSON),
		AnswerPayloadJSON:   strings.TrimSpace(req.AnswerPayloadJSON),
		VariantIndex:        req.VariantIndex,
		Active:              req.Active,
		CreatedAt:           strings.TrimSpace(req.CreatedAt),
	}
	_, err := exec.ExecContext(ctx,
		`INSERT INTO planning_messages (planning_message_id, planning_session_id, turn_index, role, content_markdown, question_payload_json, answer_payload_json, variant_index, active, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		row.PlanningMessageID,
		row.PlanningSessionID,
		row.TurnIndex,
		row.Role,
		row.ContentMarkdown,
		row.QuestionPayloadJSON,
		row.AnswerPayloadJSON,
		row.VariantIndex,
		row.Active,
		row.CreatedAt,
	)
	if err != nil {
		return PlanningMessage{}, fmt.Errorf("insert planning message: %w", err)
	}
	return row, nil
}

// GetMessageInSession returns the planning message identified by messageID
// only if it belongs to sessionID. Looking up a message by id alone is a
// session-integrity hazard: an attacker who knows another tenant's message id
// would otherwise be able to read or act on it through endpoints that derive
// the active session from the URL path.
func (s *PlanningStore) GetMessageInSession(ctx context.Context, planningSessionID, planningMessageID string) (PlanningMessage, error) {
	trimmedSession := strings.TrimSpace(planningSessionID)
	trimmedID := strings.TrimSpace(planningMessageID)
	if trimmedSession == "" {
		return PlanningMessage{}, fmt.Errorf("planning session id is required")
	}
	if trimmedID == "" {
		return PlanningMessage{}, fmt.Errorf("planning message id is required")
	}
	var row PlanningMessage
	err := s.db.QueryRowContext(ctx,
		`SELECT planning_message_id, planning_session_id, turn_index, role, content_markdown, question_payload_json, answer_payload_json, variant_index, active, created_at
		 FROM planning_messages WHERE planning_message_id = ? AND planning_session_id = ?`,
		trimmedID, trimmedSession,
	).Scan(&row.PlanningMessageID, &row.PlanningSessionID, &row.TurnIndex, &row.Role,
		&row.ContentMarkdown, &row.QuestionPayloadJSON, &row.AnswerPayloadJSON,
		&row.VariantIndex, &row.Active, &row.CreatedAt)
	if err == sql.ErrNoRows {
		return PlanningMessage{}, ErrPlanningMessageNotFound
	}
	if err != nil {
		return PlanningMessage{}, fmt.Errorf("get planning message: %w", err)
	}
	return row, nil
}

func (s *PlanningStore) DeleteMessage(ctx context.Context, planningMessageID string) (bool, error) {
	trimmedID := strings.TrimSpace(planningMessageID)
	if trimmedID == "" {
		return false, fmt.Errorf("planning message id is required")
	}
	result, err := s.db.ExecContext(ctx, `DELETE FROM planning_messages WHERE planning_message_id = ?`, trimmedID)
	if err != nil {
		return false, fmt.Errorf("delete planning message: %w", err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("delete planning message rows affected: %w", err)
	}
	return rowsAffected > 0, nil
}

func (s *PlanningStore) ListMessages(ctx context.Context, planningSessionID string) ([]PlanningMessage, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT planning_message_id, planning_session_id, turn_index, role, content_markdown, question_payload_json, answer_payload_json, variant_index, active, created_at
		 FROM planning_messages
		 WHERE planning_session_id = ? AND active = 1
		 ORDER BY turn_index ASC, CASE role WHEN 'assistant' THEN 1 ELSE 0 END ASC, created_at ASC`,
		strings.TrimSpace(planningSessionID),
	)
	if err != nil {
		return nil, fmt.Errorf("list planning messages: %w", err)
	}
	defer rows.Close()

	items := make([]PlanningMessage, 0)
	for rows.Next() {
		var row PlanningMessage
		if err := scanPlanningMessage(rows, &row); err != nil {
			return nil, fmt.Errorf("scan planning message: %w", err)
		}
		items = append(items, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate planning messages: %w", err)
	}
	return items, nil
}

func (s *PlanningStore) ListMessagesPage(ctx context.Context, planningSessionID string, limit int, before *int) (PlanningMessagePage, error) {
	trimmedSessionID := strings.TrimSpace(planningSessionID)
	if trimmedSessionID == "" {
		return PlanningMessagePage{}, fmt.Errorf("planning session id is required")
	}
	if limit <= 0 {
		limit = 50
	}

	var totalCount int
	if err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(1)
		 FROM planning_messages
		 WHERE planning_session_id = ? AND active = 1`,
		trimmedSessionID,
	).Scan(&totalCount); err != nil {
		return PlanningMessagePage{}, fmt.Errorf("count planning messages: %w", err)
	}

	page := PlanningMessagePage{PlanningSessionID: trimmedSessionID, Items: []PlanningMessage{}, TotalCount: totalCount, FirstItemIndex: 0}
	if totalCount == 0 {
		return page, nil
	}

	endIndex := totalCount
	if before != nil {
		endIndex = *before
		if endIndex < 0 {
			endIndex = 0
		}
		if endIndex > totalCount {
			endIndex = totalCount
		}
	}
	startIndex := endIndex - limit
	if startIndex < 0 {
		startIndex = 0
	}
	page.FirstItemIndex = startIndex
	if endIndex <= startIndex {
		return page, nil
	}

	rows, err := s.db.QueryContext(ctx,
		`SELECT planning_message_id, planning_session_id, turn_index, role, content_markdown, question_payload_json, answer_payload_json, variant_index, active, created_at
		 FROM planning_messages
		 WHERE planning_session_id = ? AND active = 1
		 ORDER BY turn_index ASC, CASE role WHEN 'assistant' THEN 1 ELSE 0 END ASC, created_at ASC
		 LIMIT ? OFFSET ?`,
		trimmedSessionID,
		endIndex-startIndex,
		startIndex,
	)
	if err != nil {
		return PlanningMessagePage{}, fmt.Errorf("list planning messages page: %w", err)
	}
	defer rows.Close()

	items := make([]PlanningMessage, 0, endIndex-startIndex)
	for rows.Next() {
		var row PlanningMessage
		if err := scanPlanningMessage(rows, &row); err != nil {
			return PlanningMessagePage{}, fmt.Errorf("scan planning message page row: %w", err)
		}
		items = append(items, row)
	}
	if err := rows.Err(); err != nil {
		return PlanningMessagePage{}, fmt.Errorf("iterate planning message page rows: %w", err)
	}
	page.Items = items
	return page, nil
}

// ActivateVariant promotes the variant identified by planningMessageID within
// the supplied planningSessionID. Both ids must match the same row — looking
// the message up by id alone would let any caller activate variants in
// sessions they do not own.
func (s *PlanningStore) ActivateVariant(ctx context.Context, planningSessionID, planningMessageID string) (PlanningMessage, error) {
	trimmedSession := strings.TrimSpace(planningSessionID)
	trimmedID := strings.TrimSpace(planningMessageID)
	if trimmedSession == "" {
		return PlanningMessage{}, fmt.Errorf("planning session id is required")
	}
	if trimmedID == "" {
		return PlanningMessage{}, fmt.Errorf("planning message id is required")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return PlanningMessage{}, fmt.Errorf("begin activate variant: %w", err)
	}

	var target PlanningMessage
	err = tx.QueryRowContext(ctx,
		`SELECT planning_message_id, planning_session_id, turn_index, role, content_markdown, question_payload_json, answer_payload_json, variant_index, active, created_at
		 FROM planning_messages WHERE planning_message_id = ? AND planning_session_id = ?`,
		trimmedID, trimmedSession,
	).Scan(&target.PlanningMessageID, &target.PlanningSessionID, &target.TurnIndex, &target.Role,
		&target.ContentMarkdown, &target.QuestionPayloadJSON, &target.AnswerPayloadJSON,
		&target.VariantIndex, &target.Active, &target.CreatedAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return PlanningMessage{}, rollbackPlanningTx(tx, "activate variant", ErrPlanningMessageNotFound)
		}
		return PlanningMessage{}, rollbackPlanningTx(tx, "activate variant", fmt.Errorf("get planning message for activation: %w", err))
	}

	if _, err := tx.ExecContext(ctx,
		`UPDATE planning_messages SET active = 0
		 WHERE planning_session_id = ? AND turn_index = ? AND role = ? AND planning_message_id != ?`,
		target.PlanningSessionID, target.TurnIndex, target.Role, trimmedID,
	); err != nil {
		return PlanningMessage{}, rollbackPlanningTx(tx, "activate variant", fmt.Errorf("deactivate sibling variants: %w", err))
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE planning_messages SET active = 1
		 WHERE planning_message_id = ? AND planning_session_id = ?`,
		trimmedID, trimmedSession,
	); err != nil {
		return PlanningMessage{}, rollbackPlanningTx(tx, "activate variant", fmt.Errorf("activate variant: %w", err))
	}

	if err := deleteMessagesAfterTurn(ctx, tx, target.PlanningSessionID, target.TurnIndex); err != nil {
		return PlanningMessage{}, rollbackPlanningTx(tx, "activate variant", err)
	}

	questionMessageID := ""
	if target.Role == PlanningMessageRoleAssistant && hasPlanningPayload(target.QuestionPayloadJSON) {
		questionMessageID = target.PlanningMessageID
	}
	if err := updatePlanningSession(ctx, tx, UpdatePlanningSessionRequest{
		PlanningSessionID:        target.PlanningSessionID,
		CurrentQuestionMessageID: stringPtr(questionMessageID),
		UpdatedAt:                target.CreatedAt,
	}); err != nil {
		return PlanningMessage{}, rollbackPlanningTx(tx, "activate variant", fmt.Errorf("update session after variant activation: %w", err))
	}

	if err := tx.Commit(); err != nil {
		return PlanningMessage{}, fmt.Errorf("commit activate variant: %w", err)
	}
	target.Active = true
	return target, nil
}

func (s *PlanningStore) ListVariantsAtTurn(ctx context.Context, planningSessionID string, turnIndex int, role PlanningMessageRole) ([]PlanningMessage, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT planning_message_id, planning_session_id, turn_index, role, content_markdown, question_payload_json, answer_payload_json, variant_index, active, created_at
		 FROM planning_messages
		 WHERE planning_session_id = ? AND turn_index = ? AND role = ?
		 ORDER BY variant_index ASC`,
		strings.TrimSpace(planningSessionID), turnIndex, role,
	)
	if err != nil {
		return nil, fmt.Errorf("list variants at turn: %w", err)
	}
	defer rows.Close()

	items := make([]PlanningMessage, 0)
	for rows.Next() {
		var row PlanningMessage
		if err := scanPlanningMessage(rows, &row); err != nil {
			return nil, fmt.Errorf("scan variant: %w", err)
		}
		items = append(items, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate variants: %w", err)
	}
	return items, nil
}

// GetMaxTurnIndex returns the highest turn_index among all (active or not)
// planning messages in the session, or 0 if the session has no messages
// yet. Used to validate `truncate_from_turn` and `target_turn_index` inputs
// against the actual transcript so an out-of-range value is rejected with a
// 400 instead of silently creating gapful turns.
func (s *PlanningStore) GetMaxTurnIndex(ctx context.Context, planningSessionID string) (int, error) {
	var maxTurn int
	err := s.db.QueryRowContext(ctx,
		`SELECT COALESCE(MAX(turn_index), 0) FROM planning_messages
		 WHERE planning_session_id = ?`,
		strings.TrimSpace(planningSessionID),
	).Scan(&maxTurn)
	if err != nil {
		return 0, fmt.Errorf("get max turn index: %w", err)
	}
	return maxTurn, nil
}

func (s *PlanningStore) GetMaxVariantIndex(ctx context.Context, planningSessionID string, turnIndex int, role PlanningMessageRole) (int, error) {
	var maxIndex int
	err := s.db.QueryRowContext(ctx,
		`SELECT COALESCE(MAX(variant_index), -1) FROM planning_messages
		 WHERE planning_session_id = ? AND turn_index = ? AND role = ?`,
		strings.TrimSpace(planningSessionID), turnIndex, role,
	).Scan(&maxIndex)
	if err != nil {
		return -1, fmt.Errorf("get max variant index: %w", err)
	}
	return maxIndex, nil
}

func (s *PlanningStore) VariantCountAtTurn(ctx context.Context, planningSessionID string, turnIndex int, role PlanningMessageRole) (int, error) {
	var count int
	err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(1) FROM planning_messages
		 WHERE planning_session_id = ? AND turn_index = ? AND role = ?`,
		strings.TrimSpace(planningSessionID), turnIndex, role,
	).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("variant count at turn: %w", err)
	}
	return count, nil
}

func (s *PlanningStore) VariantCountsBySession(ctx context.Context, planningSessionID string) (map[int]int, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT turn_index, COUNT(1) FROM planning_messages
		 WHERE planning_session_id = ? AND role = 'assistant'
		 GROUP BY turn_index
		 HAVING COUNT(1) > 1`,
		strings.TrimSpace(planningSessionID),
	)
	if err != nil {
		return nil, fmt.Errorf("variant counts by session: %w", err)
	}
	defer rows.Close()
	counts := make(map[int]int)
	for rows.Next() {
		var turnIndex, count int
		if err := rows.Scan(&turnIndex, &count); err != nil {
			return nil, fmt.Errorf("scan variant count: %w", err)
		}
		counts[turnIndex] = count
	}
	return counts, rows.Err()
}

func deleteMessagesFromTurn(ctx context.Context, exec sqlExecutor, planningSessionID string, fromTurnIndex int) error {
	_, err := exec.ExecContext(ctx,
		`DELETE FROM planning_messages WHERE planning_session_id = ? AND turn_index >= ?`,
		strings.TrimSpace(planningSessionID), fromTurnIndex,
	)
	if err != nil {
		return fmt.Errorf("delete messages from turn %d: %w", fromTurnIndex, err)
	}
	return nil
}

func deleteMessagesAfterTurn(ctx context.Context, exec sqlExecutor, planningSessionID string, afterTurnIndex int) error {
	_, err := exec.ExecContext(ctx,
		`DELETE FROM planning_messages WHERE planning_session_id = ? AND turn_index > ?`,
		strings.TrimSpace(planningSessionID), afterTurnIndex,
	)
	if err != nil {
		return fmt.Errorf("delete messages after turn %d: %w", afterTurnIndex, err)
	}
	return nil
}

func deactivateVariantsAtTurn(ctx context.Context, exec sqlExecutor, planningSessionID string, turnIndex int, role PlanningMessageRole) error {
	_, err := exec.ExecContext(ctx,
		`UPDATE planning_messages SET active = 0
		 WHERE planning_session_id = ? AND turn_index = ? AND role = ?`,
		strings.TrimSpace(planningSessionID), turnIndex, role,
	)
	if err != nil {
		return fmt.Errorf("deactivate variants at turn %d: %w", turnIndex, err)
	}
	return nil
}

type scannable interface {
	Scan(dest ...any) error
}

func scanPlanningMessage(row scannable, dest *PlanningMessage) error {
	return row.Scan(
		&dest.PlanningMessageID,
		&dest.PlanningSessionID,
		&dest.TurnIndex,
		&dest.Role,
		&dest.ContentMarkdown,
		&dest.QuestionPayloadJSON,
		&dest.AnswerPayloadJSON,
		&dest.VariantIndex,
		&dest.Active,
		&dest.CreatedAt,
	)
}
