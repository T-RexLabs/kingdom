package state

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

func (s *PlanningStore) CreateSession(ctx context.Context, req CreatePlanningSessionRequest) (PlanningSession, error) {
	if err := validatePlanningSessionCreate(req); err != nil {
		return PlanningSession{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return PlanningSession{}, err
	}
	defer tx.Rollback()
	row, err := insertPlanningSession(ctx, tx, req)
	if err != nil {
		return PlanningSession{}, err
	}
	if err := CapturePlanningAgentConfigurationSnapshotTx(ctx, tx, row.PlanningSessionID, req.CreatedAt); err != nil {
		return PlanningSession{}, rollbackPlanningTx(tx, "create planning session", err)
	}
	if err := tx.Commit(); err != nil {
		return PlanningSession{}, &CommitOutcomeUnknownError{Operation: "create planning session", ResourceID: req.PlanningSessionID, Cause: err}
	}
	if req.TitleSource != nil {
		if err := s.hydrateSessionTitle(ctx, &row); err != nil {
			return PlanningSession{}, &PostCommitProjectionError{Operation: "create planning session", ResourceID: row.PlanningSessionID, Cause: err}
		}
	}
	return row, nil
}

func insertPlanningSession(ctx context.Context, exec sqlExecutor, req CreatePlanningSessionRequest) (PlanningSession, error) {
	if err := validatePlanningTextContent(ctx, req.TitleSource, true); err != nil {
		return PlanningSession{}, err
	}
	ref, err := persistPlanningText(ctx, exec, req.PlanningSessionID, "planning-session:"+req.PlanningSessionID+":title", req.TitleSource, req.CreatedAt)
	if err != nil {
		return PlanningSession{}, err
	}
	row := PlanningSession{
		PlanningSessionID:       strings.TrimSpace(req.PlanningSessionID),
		Title:                   strings.TrimSpace(req.Title),
		TitleState:              strings.TrimSpace(req.TitleState),
		LifecycleState:          req.LifecycleState,
		ConversationState:       req.ConversationState,
		ActiveLeafMessageID:     nullableString(req.ActiveLeafMessageID),
		CurrentOperationID:      nullableString(req.CurrentOperationID),
		CurrentTitleOperationID: nullableString(req.CurrentTitleOperationID),
		CurrentDraftID:          nullableString(req.CurrentDraftID),
		ContextSnapshotID:       nullableString(req.ContextSnapshotID),
		ProjectRoot:             strings.TrimSpace(req.ProjectRoot),
		ModelSelectionJSON:      strings.TrimSpace(req.ModelSelectionJSON),
		CreatedAt:               strings.TrimSpace(req.CreatedAt),
		UpdatedAt:               strings.TrimSpace(req.UpdatedAt),
	}
	if ref != nil {
		row.Title = ""
	}
	_, err = exec.ExecContext(ctx,
		`INSERT INTO planning_sessions (
			planning_session_id, title, title_state, lifecycle_state, conversation_state,
			active_leaf_message_id, current_operation_id, current_title_operation_id,
			current_draft_id, context_snapshot_id, project_root, model_selection_json,
			created_at, updated_at
		 ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		row.PlanningSessionID,
		row.Title,
		row.TitleState,
		row.LifecycleState,
		row.ConversationState,
		nullableStringValue(row.ActiveLeafMessageID),
		nullableStringValue(row.CurrentOperationID),
		nullableStringValue(row.CurrentTitleOperationID),
		nullableStringValue(row.CurrentDraftID),
		nullableStringValue(row.ContextSnapshotID),
		row.ProjectRoot,
		row.ModelSelectionJSON,
		row.CreatedAt,
		row.UpdatedAt,
	)
	if err != nil {
		return PlanningSession{}, fmt.Errorf("insert planning session: %w", err)
	}
	if ref != nil {
		if _, err := exec.ExecContext(ctx, `UPDATE planning_sessions SET title_content_id=? WHERE planning_session_id=?`, ref.ContentID, row.PlanningSessionID); err != nil {
			return PlanningSession{}, err
		}
	}
	return row, nil
}

// CreateSessionBundle creates a planning session, its opening message, and its
// first context snapshot in one transaction. The snapshot is part of the
// bundle rather than a follow-up operation because it is a precondition for
// every planner operation the session can ever run: a committed session is
// therefore always immediately usable.
func (s *PlanningStore) CreateSessionBundle(ctx context.Context, req CreatePlanningSessionBundleRequest) (PlanningSession, error) {
	if req.Session.PlanningSessionID != req.ContextSnapshot.PlanningSessionID || req.Session.PlanningSessionID != req.OpeningMessage.PlanningSessionID {
		return PlanningSession{}, fmt.Errorf("planning bundle session mismatch")
	}
	if err := validatePlanningSessionCreate(req.Session); err != nil {
		return PlanningSession{}, err
	}
	if err := validatePlanningMessageAppend(req.OpeningMessage); err != nil {
		return PlanningSession{}, err
	}
	if err := validatePlanningContextSnapshotCreate(req.ContextSnapshot); err != nil {
		return PlanningSession{}, err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return PlanningSession{}, fmt.Errorf("begin create planning session bundle: %w", err)
	}

	session, err := insertPlanningSession(ctx, tx, req.Session)
	if err != nil {
		return PlanningSession{}, rollbackPlanningTx(tx, "create planning session bundle", err)
	}
	if _, err := insertPlanningMessage(ctx, tx, req.OpeningMessage); err != nil {
		return PlanningSession{}, rollbackPlanningTx(tx, "create planning session bundle", err)
	}
	// The snapshot row references the session, and the session's adopted
	// pointer references the snapshot, so the pointer is written last. Reusing
	// publishContextSnapshot keeps the ordering fence in one implementation.
	snapshot, err := insertPlanningContextSnapshot(ctx, tx, req.ContextSnapshot)
	if err != nil {
		return PlanningSession{}, rollbackPlanningTx(tx, "create planning session bundle", err)
	}
	adopted, err := publishContextSnapshot(ctx, tx, session.PlanningSessionID, snapshot.PlanningContextSnapshotID, req.Session.UpdatedAt)
	if err != nil {
		return PlanningSession{}, rollbackPlanningTx(tx, "create planning session bundle", err)
	}
	if err := CapturePlanningAgentConfigurationSnapshotTx(ctx, tx, session.PlanningSessionID, req.Session.CreatedAt); err != nil {
		return PlanningSession{}, rollbackPlanningTx(tx, "create planning session bundle", err)
	}
	if err := tx.Commit(); err != nil {
		return PlanningSession{}, &CommitOutcomeUnknownError{Operation: "create planning session bundle", ResourceID: strings.TrimSpace(req.Session.PlanningSessionID), Cause: err}
	}
	session.ContextSnapshotID = &adopted
	if req.Session.TitleSource != nil {
		if err := s.hydrateSessionTitle(ctx, &session); err != nil {
			return PlanningSession{}, &PostCommitProjectionError{Operation: "create planning session bundle", ResourceID: session.PlanningSessionID, Cause: err}
		}
	}
	return session, nil
}

// LoadAgentConfiguration returns the immutable Plan Author settings captured
// when the planning session was created.
func (s *PlanningStore) LoadAgentConfiguration(ctx context.Context, planningSessionID string) (PlanningPromptConfigurationSnapshot, error) {
	if s == nil || s.db == nil {
		return PlanningPromptConfigurationSnapshot{}, fmt.Errorf("planning store is required")
	}
	return LoadPlanningAgentConfiguration(ctx, s.db, planningSessionID)
}

func (s *PlanningStore) UpdateSession(ctx context.Context, req UpdatePlanningSessionRequest) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := updatePlanningSession(ctx, tx, req); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return &CommitOutcomeUnknownError{Operation: "update planning session", ResourceID: req.PlanningSessionID, Cause: err}
	}
	return nil
}

func updatePlanningSession(ctx context.Context, exec sqlExecutor, req UpdatePlanningSessionRequest) error {
	if strings.TrimSpace(req.PlanningSessionID) == "" {
		return fmt.Errorf("planning session id is required")
	}
	if strings.TrimSpace(req.UpdatedAt) == "" {
		return fmt.Errorf("updated_at is required")
	}
	if req.Title != nil && strings.TrimSpace(*req.Title) == "" {
		return fmt.Errorf("planning session title is required")
	}
	if req.TitleState != nil && !isValidPlanningSessionTitleState(strings.TrimSpace(*req.TitleState)) {
		return fmt.Errorf("invalid planning session title state: %q", strings.TrimSpace(*req.TitleState))
	}
	if req.LifecycleState != nil && !isValidPlanningSessionLifecycleState(*req.LifecycleState) {
		return fmt.Errorf("invalid planning session lifecycle state: %q", *req.LifecycleState)
	}
	if req.ConversationState != nil && !isValidPlanningSessionConversationState(*req.ConversationState) {
		return fmt.Errorf("invalid planning session conversation state: %q", *req.ConversationState)
	}
	if req.ModelSelectionJSON != nil && strings.TrimSpace(*req.ModelSelectionJSON) == "" {
		return fmt.Errorf("planning session model selection json is required")
	}

	builder := newUpdateBuilder("planning_sessions")
	if req.TitleSource != nil {
		if err := validatePlanningTextContent(ctx, req.TitleSource, true); err != nil {
			return err
		}
		if req.TitleSource.SizeBytes <= 0 {
			return fmt.Errorf("planning session title is required")
		}
		ref, err := persistPlanningText(ctx, exec, req.PlanningSessionID, req.TitleContentID, req.TitleSource, req.UpdatedAt)
		if err != nil {
			return err
		}
		builder.Set("title", "")
		builder.Set("title_content_id", ref.ContentID)
	} else if title := trimOptionalString(req.Title); title != nil {
		builder.Set("title", *title)
		builder.Set("title_content_id", nil)
	}
	if titleState := trimOptionalString(req.TitleState); titleState != nil {
		builder.Set("title_state", *titleState)
	}
	if lifecycleState := lifecycleStatePtrValue(req.LifecycleState); lifecycleState != nil {
		builder.Set("lifecycle_state", *lifecycleState)
	}
	if conversationState := conversationStatePtrValue(req.ConversationState); conversationState != nil {
		builder.Set("conversation_state", *conversationState)
	}
	if req.ActiveLeafMessageID != nil {
		builder.Set("active_leaf_message_id", finalizeNullable(*req.ActiveLeafMessageID))
	}
	if req.CurrentOperationID != nil {
		builder.Set("current_operation_id", finalizeNullable(*req.CurrentOperationID))
	}
	if req.CurrentTitleOperationID != nil {
		builder.Set("current_title_operation_id", finalizeNullable(*req.CurrentTitleOperationID))
	}
	if req.CurrentDraftID != nil {
		builder.Set("current_draft_id", finalizeNullable(*req.CurrentDraftID))
	}
	if req.ContextSnapshotID != nil {
		builder.Set("context_snapshot_id", finalizeNullable(*req.ContextSnapshotID))
	}
	if modelSelectionJSON := trimOptionalString(req.ModelSelectionJSON); modelSelectionJSON != nil {
		builder.Set("model_selection_json", *modelSelectionJSON)
	}
	builder.Set("updated_at", strings.TrimSpace(req.UpdatedAt))
	builder.Where("planning_session_id = ?", strings.TrimSpace(req.PlanningSessionID))

	_, err := builder.Exec(ctx, exec)
	if err != nil {
		return fmt.Errorf("update planning session: %w", err)
	}
	return nil
}

func (s *PlanningStore) GetSession(ctx context.Context, planningSessionID string) (PlanningSession, bool, error) {
	var row PlanningSession
	err := scanPlanningSession(s.db.QueryRowContext(ctx,
		`SELECT planning_session_id, title, title_state, lifecycle_state, conversation_state,
		        active_leaf_message_id, current_operation_id, current_title_operation_id,
		        current_draft_id, context_snapshot_id,
		        project_root, model_selection_json, created_at, updated_at
		 FROM planning_sessions
		 WHERE planning_session_id = ?`,
		strings.TrimSpace(planningSessionID),
	), &row)
	if err == sql.ErrNoRows {
		return PlanningSession{}, false, nil
	}
	if err != nil {
		return PlanningSession{}, false, fmt.Errorf("get planning session: %w", err)
	}
	if err := s.hydrateSessionTitle(ctx, &row); err != nil {
		return PlanningSession{}, false, err
	}
	return row, true, nil
}

func (s *PlanningStore) ListSessions(ctx context.Context, limit int) ([]PlanningSession, error) {
	return s.ListSessionsPage(ctx, limit, "", "", "")
}

func (s *PlanningStore) ListSessionsPage(ctx context.Context, limit int, beforeUpdatedAt, beforeID, search string) ([]PlanningSession, error) {
	if limit <= 0 {
		limit = 100
	}
	search = strings.ToLower(strings.TrimSpace(search))
	if search == "" {
		return s.listSessionsPage(ctx, limit, beforeUpdatedAt, beforeID)
	}
	// Titles can be chunk-backed content. Match the canonical text rather than
	// the inline column, scanning bounded pages without limiting searchable history.
	const scanSize = 64
	matches := make([]PlanningSession, 0)
	for {
		items, err := s.listSessionsPage(ctx, scanSize, beforeUpdatedAt, beforeID)
		if err != nil {
			return nil, err
		}
		for _, item := range items {
			if strings.Contains(strings.ToLower(item.Title), search) {
				matches = append(matches, item)
				if len(matches) == limit {
					return matches, nil
				}
			}
		}
		if len(items) < scanSize {
			return matches, nil
		}
		last := items[len(items)-1]
		beforeUpdatedAt, beforeID = last.UpdatedAt, last.PlanningSessionID
	}
}

func (s *PlanningStore) listSessionsPage(ctx context.Context, limit int, beforeUpdatedAt, beforeID string) ([]PlanningSession, error) {
	query := `SELECT planning_session_id, title, title_state, lifecycle_state, conversation_state,
		        active_leaf_message_id, current_operation_id, current_title_operation_id,
		        current_draft_id, context_snapshot_id,
		        project_root, model_selection_json, created_at, updated_at
		 FROM planning_sessions`
	args := make([]any, 0, 3)
	if beforeID != "" {
		query += ` WHERE (updated_at, planning_session_id) < (?, ?)`
		args = append(args, beforeUpdatedAt, beforeID)
	}
	query += ` ORDER BY updated_at DESC, planning_session_id DESC LIMIT ?`
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list planning sessions: %w", err)
	}
	defer rows.Close()

	items := make([]PlanningSession, 0)
	for rows.Next() {
		var row PlanningSession
		if err := scanPlanningSession(rows, &row); err != nil {
			return nil, fmt.Errorf("scan planning session: %w", err)
		}
		items = append(items, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate planning sessions: %w", err)
	}
	rows.Close()
	for i := range items {
		if err := s.hydrateSessionTitle(ctx, &items[i]); err != nil {
			return nil, err
		}
	}
	return items, nil
}

func (s *PlanningStore) ListAutoTitleEligibleSessionsPage(ctx context.Context, afterPlanningSessionID string, limit int, userMessageThreshold int) ([]PlanningSession, error) {
	if limit <= 0 {
		limit = DefaultPlanningOperationListLimit
	}
	if userMessageThreshold <= 0 {
		return nil, fmt.Errorf("user message threshold must be positive")
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT s.planning_session_id, s.title, s.title_state, s.lifecycle_state, s.conversation_state,
		        s.active_leaf_message_id, s.current_operation_id, s.current_title_operation_id,
		        s.current_draft_id, s.context_snapshot_id,
		        s.project_root, s.model_selection_json, s.created_at, s.updated_at
		 FROM planning_sessions s
		 WHERE s.planning_session_id > ?
		   AND s.title_state = ?
		   AND s.lifecycle_state = ?
		   AND COALESCE(NULLIF(TRIM(s.current_title_operation_id), ''), '') = ''
		   AND (
		     SELECT COUNT(1)
		     FROM planning_messages m
		     WHERE m.planning_session_id = s.planning_session_id
		       AND m.role = ?
		   ) >= ?
		 ORDER BY s.planning_session_id ASC
		 LIMIT ?`,
		strings.TrimSpace(afterPlanningSessionID),
		"placeholder",
		PlanningSessionLifecycleStateActive,
		PlanningMessageRoleUser,
		userMessageThreshold,
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("list auto title eligible sessions: %w", err)
	}
	defer rows.Close()

	items := make([]PlanningSession, 0, limit)
	for rows.Next() {
		var row PlanningSession
		if err := scanPlanningSession(rows, &row); err != nil {
			return nil, fmt.Errorf("scan auto title eligible session: %w", err)
		}
		items = append(items, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate auto title eligible sessions: %w", err)
	}
	return items, nil
}

func scanPlanningSession(row scannable, dest *PlanningSession) error {
	var activeLeafMessageID sql.NullString
	var currentOperationID sql.NullString
	var currentTitleOperationID sql.NullString
	var currentDraftID sql.NullString
	var contextSnapshotID sql.NullString

	if err := row.Scan(
		&dest.PlanningSessionID,
		&dest.Title,
		&dest.TitleState,
		&dest.LifecycleState,
		&dest.ConversationState,
		&activeLeafMessageID,
		&currentOperationID,
		&currentTitleOperationID,
		&currentDraftID,
		&contextSnapshotID,
		&dest.ProjectRoot,
		&dest.ModelSelectionJSON,
		&dest.CreatedAt,
		&dest.UpdatedAt,
	); err != nil {
		return err
	}

	dest.ActiveLeafMessageID = nullableStringFromSQL(activeLeafMessageID)
	dest.CurrentOperationID = nullableStringFromSQL(currentOperationID)
	dest.CurrentTitleOperationID = nullableStringFromSQL(currentTitleOperationID)
	dest.CurrentDraftID = nullableStringFromSQL(currentDraftID)
	dest.ContextSnapshotID = nullableStringFromSQL(contextSnapshotID)
	return nil
}

func (s *PlanningStore) DeleteSession(ctx context.Context, planningSessionID string) (bool, error) {
	trimmedID := strings.TrimSpace(planningSessionID)
	if trimmedID == "" {
		return false, fmt.Errorf("planning session id is required")
	}
	result, err := s.db.ExecContext(ctx, `DELETE FROM planning_sessions WHERE planning_session_id = ?`, trimmedID)
	if err != nil {
		return false, fmt.Errorf("delete planning session: %w", err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("delete planning session rows affected: %w", err)
	}
	return rowsAffected > 0, nil
}

// CountSessionsByLifecycleAndConversation returns lifecycle and conversation
// state breakdowns in two grouped scans rather than pulling the full
// `ListSessions(ctx, 1000)` result and counting in Go.
func (s *PlanningStore) CountSessionsByLifecycleAndConversation(ctx context.Context) (
	map[PlanningSessionLifecycleState]int,
	map[PlanningSessionConversationState]int,
	error,
) {
	lifecycle := make(map[PlanningSessionLifecycleState]int)
	conv := make(map[PlanningSessionConversationState]int)

	rows, err := s.db.QueryContext(ctx,
		`SELECT lifecycle_state, COUNT(*) FROM planning_sessions GROUP BY lifecycle_state`)
	if err != nil {
		return nil, nil, fmt.Errorf("count planning sessions by lifecycle: %w", err)
	}
	for rows.Next() {
		var state PlanningSessionLifecycleState
		var count int
		if err := rows.Scan(&state, &count); err != nil {
			rows.Close()
			return nil, nil, fmt.Errorf("scan planning session lifecycle count: %w", err)
		}
		lifecycle[state] = count
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, nil, err
	}
	rows.Close()

	rows, err = s.db.QueryContext(ctx,
		`SELECT conversation_state, COUNT(*) FROM planning_sessions GROUP BY conversation_state`)
	if err != nil {
		return nil, nil, fmt.Errorf("count planning sessions by conversation state: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var state PlanningSessionConversationState
		var count int
		if err := rows.Scan(&state, &count); err != nil {
			return nil, nil, fmt.Errorf("scan planning session conversation count: %w", err)
		}
		conv[state] = count
	}
	return lifecycle, conv, rows.Err()
}
