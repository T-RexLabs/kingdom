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
	trimmedParent := strings.TrimSpace(req.ParentMessageID)
	var parentPtr *string
	if trimmedParent != "" {
		parentPtr = &trimmedParent
	}
	row := PlanningMessage{
		PlanningMessageID:   strings.TrimSpace(req.PlanningMessageID),
		PlanningSessionID:   strings.TrimSpace(req.PlanningSessionID),
		ParentMessageID:     parentPtr,
		Role:                req.Role,
		ContentMarkdown:     req.ContentMarkdown,
		QuestionPayloadJSON: strings.TrimSpace(req.QuestionPayloadJSON),
		AnswerPayloadJSON:   strings.TrimSpace(req.AnswerPayloadJSON),
		CreatedAt:           strings.TrimSpace(req.CreatedAt),
	}
	_, err := exec.ExecContext(ctx,
		`INSERT INTO planning_messages (planning_message_id, planning_session_id, parent_message_id, role, content_markdown, question_payload_json, answer_payload_json, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		row.PlanningMessageID,
		row.PlanningSessionID,
		nullableMessageID(trimmedParent),
		row.Role,
		row.ContentMarkdown,
		row.QuestionPayloadJSON,
		row.AnswerPayloadJSON,
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
	if err := scanPlanningMessage(s.db.QueryRowContext(ctx,
		`SELECT planning_message_id, planning_session_id, parent_message_id, role, content_markdown, question_payload_json, answer_payload_json, created_at
		 FROM planning_messages WHERE planning_message_id = ? AND planning_session_id = ?`,
		trimmedID, trimmedSession,
	), &row); err != nil {
		if err == sql.ErrNoRows {
			return PlanningMessage{}, ErrPlanningMessageNotFound
		}
		return PlanningMessage{}, fmt.Errorf("get planning message: %w", err)
	}
	return row, nil
}

// ListMessages returns the session's active conversation path: the messages
// from the conversation root down to the session's active leaf, in order. This
// is the canonical transcript the planner is prompted with and the operator
// sees; sibling branches off the active path are excluded.
func (s *PlanningStore) ListMessages(ctx context.Context, planningSessionID string) ([]PlanningMessage, error) {
	return activePathMessages(ctx, s.db, strings.TrimSpace(planningSessionID))
}

func (s *PlanningStore) CountMessagesByRole(ctx context.Context, planningSessionID string, role PlanningMessageRole) (int, error) {
	trimmedSessionID := strings.TrimSpace(planningSessionID)
	if trimmedSessionID == "" {
		return 0, fmt.Errorf("planning session id is required")
	}
	if !isValidPlanningMessageRole(role) {
		return 0, fmt.Errorf("invalid planning message role: %q", role)
	}
	path, err := activePathMessages(ctx, s.db, trimmedSessionID)
	if err != nil {
		return 0, err
	}
	count := 0
	for _, message := range path {
		if message.Role == role {
			count++
		}
	}
	return count, nil
}

// ListMessagesPage paginates the active conversation path (see ListMessages).
// The page window is index-based over that derived path so the API's
// total_count / first_item_index contract is unchanged.
func (s *PlanningStore) ListMessagesPage(ctx context.Context, planningSessionID string, limit int, before *int) (PlanningMessagePage, error) {
	trimmedSessionID := strings.TrimSpace(planningSessionID)
	if trimmedSessionID == "" {
		return PlanningMessagePage{}, fmt.Errorf("planning session id is required")
	}
	if limit <= 0 {
		limit = 50
	}

	path, err := activePathMessages(ctx, s.db, trimmedSessionID)
	if err != nil {
		return PlanningMessagePage{}, err
	}
	totalCount := len(path)
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
	page.Items = append([]PlanningMessage{}, path[startIndex:endIndex]...)
	return page, nil
}

// SetActiveBranch moves the session's active leaf onto the branch that runs
// through planningMessageID, descending to that branch's most recent leaf so
// the full continuation of the chosen branch becomes visible. Non-destructive:
// no message is deleted or deactivated, so the previously active branch and its
// downstream remain available to switch back to.
func (s *PlanningStore) SetActiveBranch(ctx context.Context, planningSessionID, planningMessageID string) (PlanningMessage, error) {
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
		return PlanningMessage{}, fmt.Errorf("begin set active branch: %w", err)
	}

	var target PlanningMessage
	if err := scanPlanningMessage(tx.QueryRowContext(ctx,
		`SELECT planning_message_id, planning_session_id, parent_message_id, role, content_markdown, question_payload_json, answer_payload_json, created_at
		 FROM planning_messages WHERE planning_message_id = ? AND planning_session_id = ?`,
		trimmedID, trimmedSession,
	), &target); err != nil {
		if err == sql.ErrNoRows {
			return PlanningMessage{}, rollbackPlanningTx(tx, "set active branch", ErrPlanningMessageNotFound)
		}
		return PlanningMessage{}, rollbackPlanningTx(tx, "set active branch", fmt.Errorf("get planning message for branch switch: %w", err))
	}

	leafID, err := descendToLeaf(ctx, tx, trimmedSession, target.PlanningMessageID)
	if err != nil {
		return PlanningMessage{}, rollbackPlanningTx(tx, "set active branch", err)
	}
	if err := updatePlanningSession(ctx, tx, UpdatePlanningSessionRequest{
		PlanningSessionID:   trimmedSession,
		ActiveLeafMessageID: stringPtr(leafID),
		UpdatedAt:           target.CreatedAt,
	}); err != nil {
		return PlanningMessage{}, rollbackPlanningTx(tx, "set active branch", fmt.Errorf("update session after branch switch: %w", err))
	}
	if err := tx.Commit(); err != nil {
		return PlanningMessage{}, fmt.Errorf("commit set active branch: %w", err)
	}
	return target, nil
}

// SiblingsOf returns the branch alternatives at planningMessageID — every
// message sharing its parent (including itself), ordered oldest-first. With a
// single sibling there is no branch; the navigator appears only when len > 1.
func (s *PlanningStore) SiblingsOf(ctx context.Context, planningSessionID, planningMessageID string) ([]PlanningMessage, error) {
	target, err := s.GetMessageInSession(ctx, planningSessionID, planningMessageID)
	if err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT planning_message_id, planning_session_id, parent_message_id, role, content_markdown, question_payload_json, answer_payload_json, created_at
		 FROM planning_messages
		 WHERE planning_session_id = ? AND parent_message_id IS ?
		 ORDER BY created_at ASC, planning_message_id ASC`,
		strings.TrimSpace(planningSessionID), nullStringFromPtr(target.ParentMessageID),
	)
	if err != nil {
		return nil, fmt.Errorf("list siblings: %w", err)
	}
	defer rows.Close()
	items := make([]PlanningMessage, 0)
	for rows.Next() {
		var row PlanningMessage
		if err := scanPlanningMessage(rows, &row); err != nil {
			return nil, fmt.Errorf("scan sibling: %w", err)
		}
		items = append(items, row)
	}
	return items, rows.Err()
}

// BranchInfoBySession returns, for every message in the session, its position
// among its siblings (index) and the sibling count. The API attaches this to
// active-path messages so the UI can render a branch navigator at any point
// where the conversation forked.
func (s *PlanningStore) BranchInfoBySession(ctx context.Context, planningSessionID string) (map[string]PlanningMessageBranchInfo, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT planning_message_id,
		        ROW_NUMBER() OVER (PARTITION BY parent_message_id ORDER BY created_at ASC, planning_message_id ASC) - 1 AS branch_index,
		        COUNT(1) OVER (PARTITION BY parent_message_id) AS branch_count
		 FROM planning_messages
		 WHERE planning_session_id = ?`,
		strings.TrimSpace(planningSessionID),
	)
	if err != nil {
		return nil, fmt.Errorf("branch info by session: %w", err)
	}
	defer rows.Close()
	info := make(map[string]PlanningMessageBranchInfo)
	for rows.Next() {
		var id string
		var branch PlanningMessageBranchInfo
		if err := rows.Scan(&id, &branch.Index, &branch.Count); err != nil {
			return nil, fmt.Errorf("scan branch info: %w", err)
		}
		info[id] = branch
	}
	return info, rows.Err()
}

// activePathMessages walks from the session's active leaf up to the root via
// parent_message_id and returns the path root-first. A nil/missing leaf yields
// an empty path (a fresh session before its first message).
func activePathMessages(ctx context.Context, exec sqlQuerier, planningSessionID string) ([]PlanningMessage, error) {
	if planningSessionID == "" {
		return nil, fmt.Errorf("planning session id is required")
	}
	var leaf sql.NullString
	if err := exec.QueryRowContext(ctx,
		`SELECT active_leaf_message_id FROM planning_sessions WHERE planning_session_id = ?`,
		planningSessionID,
	).Scan(&leaf); err != nil {
		if err == sql.ErrNoRows {
			return []PlanningMessage{}, nil
		}
		return nil, fmt.Errorf("read active leaf: %w", err)
	}
	if !leaf.Valid || strings.TrimSpace(leaf.String) == "" {
		return []PlanningMessage{}, nil
	}

	byID, err := allMessagesByID(ctx, exec, planningSessionID)
	if err != nil {
		return nil, err
	}
	reversed := make([]PlanningMessage, 0, len(byID))
	seen := make(map[string]struct{}, len(byID))
	for id := leaf.String; id != ""; {
		message, ok := byID[id]
		if !ok {
			break
		}
		if _, looped := seen[id]; looped {
			break // defensive: never spin on a cycle
		}
		seen[id] = struct{}{}
		reversed = append(reversed, message)
		if message.ParentMessageID == nil {
			break
		}
		id = *message.ParentMessageID
	}
	path := make([]PlanningMessage, len(reversed))
	for i, message := range reversed {
		path[len(reversed)-1-i] = message
	}
	return path, nil
}

func allMessagesByID(ctx context.Context, exec sqlQuerier, planningSessionID string) (map[string]PlanningMessage, error) {
	rows, err := exec.QueryContext(ctx,
		`SELECT planning_message_id, planning_session_id, parent_message_id, role, content_markdown, question_payload_json, answer_payload_json, created_at
		 FROM planning_messages WHERE planning_session_id = ?`,
		planningSessionID,
	)
	if err != nil {
		return nil, fmt.Errorf("load planning messages: %w", err)
	}
	defer rows.Close()
	byID := make(map[string]PlanningMessage)
	for rows.Next() {
		var row PlanningMessage
		if err := scanPlanningMessage(rows, &row); err != nil {
			return nil, fmt.Errorf("scan planning message: %w", err)
		}
		byID[row.PlanningMessageID] = row
	}
	return byID, rows.Err()
}

// descendToLeaf follows the most-recently-created child at each step until it
// reaches a leaf, so switching to a branch lands on that branch's latest tip.
func descendToLeaf(ctx context.Context, exec sqlQuerier, planningSessionID, messageID string) (string, error) {
	current := messageID
	for i := 0; i < maxPlanningTreeDepth; i++ {
		var childID sql.NullString
		err := exec.QueryRowContext(ctx,
			`SELECT planning_message_id FROM planning_messages
			 WHERE planning_session_id = ? AND parent_message_id = ?
			 ORDER BY created_at DESC, planning_message_id DESC
			 LIMIT 1`,
			planningSessionID, current,
		).Scan(&childID)
		if err == sql.ErrNoRows {
			return current, nil
		}
		if err != nil {
			return "", fmt.Errorf("descend to leaf: %w", err)
		}
		if !childID.Valid || childID.String == "" {
			return current, nil
		}
		current = childID.String
	}
	return current, nil
}

// maxPlanningTreeDepth bounds the descent walk defensively; planner
// conversations never approach this, but it guarantees termination.
const maxPlanningTreeDepth = 100000

type scannable interface {
	Scan(dest ...any) error
}

func scanPlanningMessage(row scannable, dest *PlanningMessage) error {
	var parent sql.NullString
	if err := row.Scan(
		&dest.PlanningMessageID,
		&dest.PlanningSessionID,
		&parent,
		&dest.Role,
		&dest.ContentMarkdown,
		&dest.QuestionPayloadJSON,
		&dest.AnswerPayloadJSON,
		&dest.CreatedAt,
	); err != nil {
		return err
	}
	dest.ParentMessageID = nullStringToPtr(parent)
	return nil
}

// nullableMessageID maps an empty parent id to a SQL NULL (conversation root).
func nullableMessageID(parentMessageID string) any {
	trimmed := strings.TrimSpace(parentMessageID)
	if trimmed == "" {
		return nil
	}
	return trimmed
}

func nullStringToPtr(value sql.NullString) *string {
	if !value.Valid {
		return nil
	}
	trimmed := value.String
	return &trimmed
}

// nullStringFromPtr renders a *string as a comparison value usable with
// `parent_message_id IS ?` (matches NULL roots and concrete parents alike).
func nullStringFromPtr(value *string) any {
	if value == nil {
		return nil
	}
	return *value
}
