package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	gen "kingdom/internal/apigen"
	executionstore "kingdom/internal/execution/store"
	"kingdom/internal/state"
)

func TestServerTimelineUsesCursorPagination(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunWithLinearExecutionWorkflow(t, handler)

	var reviewNodeID string
	if err := db.QueryRow(`
SELECT wn.node_id
FROM workflow_runs wr
JOIN workflow_nodes wn ON wn.workflow_definition_id = wr.workflow_definition_id
WHERE wr.run_id = ? AND wn.node_class = 'execution_review'
LIMIT 1`, runID).Scan(&reviewNodeID); err != nil {
		t.Fatalf("lookup execution_review node for run: %v", err)
	}
	seedTimelineEvents(t, db, runID)

	first := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/runs/" + runID + "/timeline?limit=2",
		actorID: "actor-1",
		roleID:  "chancellor",
	})
	if first.Code != http.StatusOK {
		t.Fatalf("expected first page 200, got %d", first.Code)
	}

	var firstPage gen.RunTimelineResponse
	if err := json.Unmarshal(first.Body.Bytes(), &firstPage); err != nil {
		t.Fatalf("decode first page: %v", err)
	}
	if len(firstPage.Items) != 2 {
		t.Fatalf("expected 2 timeline items, got %d", len(firstPage.Items))
	}
	if firstPage.Items[0].RunEventSeq != 2 || firstPage.Items[1].RunEventSeq != 3 {
		t.Fatalf("expected latest timeline page to return run_event_seq 2 and 3, got %#v", firstPage.Items)
	}
	if firstPage.Items[0].AttemptSeq != 2 || firstPage.Items[1].AttemptSeq != 3 {
		t.Fatalf("expected latest timeline page to preserve attempt_seq 2 and 3, got %#v", firstPage.Items)
	}
	if firstPage.Items[0].ReasonCode == nil || *firstPage.Items[0].ReasonCode != "internal_error" {
		t.Fatalf("expected first item reason_code=internal_error, got %#v", firstPage.Items[0].ReasonCode)
	}
	if firstPage.NextCursor == nil || *firstPage.NextCursor == "" {
		t.Fatal("expected next_cursor for first page")
	}

	second := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/runs/" + runID + "/timeline?limit=2&cursor=" + *firstPage.NextCursor,
		actorID: "actor-1",
		roleID:  "chancellor",
	})
	if second.Code != http.StatusOK {
		t.Fatalf("expected second page 200, got %d", second.Code)
	}

	var secondPage gen.RunTimelineResponse
	if err := json.Unmarshal(second.Body.Bytes(), &secondPage); err != nil {
		t.Fatalf("decode second page: %v", err)
	}
	if len(secondPage.Items) != 1 {
		t.Fatalf("expected 1 timeline item on second page, got %d", len(secondPage.Items))
	}
	if secondPage.Items[0].RunEventSeq != 1 || secondPage.Items[0].AttemptSeq != 1 {
		t.Fatalf("expected oldest event on second page, got %#v", secondPage.Items[0])
	}
}

func TestServerTimelinePreservesRunEventOrderingAcrossAttempts(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunWithLinearExecutionWorkflow(t, handler)

	ingestor := state.NewAttemptEventIngestor(db, nil)
	seedEvent := func(eventID, attemptID, eventType string, attemptSeq int, createdAt string) {
		t.Helper()
		payload, _ := json.Marshal(map[string]any{"node_id": "plan_drafting"})
		if _, err := ingestor.Ingest(context.Background(), state.AttemptEventIngestionRequest{
			EventID:         eventID,
			RunID:           runID,
			NodeRunID:       sql.NullString{},
			AttemptID:       attemptID,
			SchemaVersion:   "v1",
			EventType:       eventType,
			Seq:             attemptSeq,
			PayloadJSON:     string(payload),
			ActorID:         "actor-1",
			RoleID:          "chancellor",
			AuthoritySource: "test.seed",
			ReasonCode:      "internal_error",
			ReasonText:      "seed",
			CorrelationID:   "corr-" + eventID,
			CausationID:     "caus-" + eventID,
			CreatedAt:       createdAt,
		}); err != nil {
			t.Fatalf("seed timeline event %q: %v", eventID, err)
		}
	}

	seedEvent("evt-attempt-1-start", "attempt-1", "attempt.started", 1, "2026-03-06T10:00:00Z")
	seedEvent("evt-attempt-1-progress", "attempt-1", "attempt.progress", 2, "2026-03-06T10:01:00Z")
	seedEvent("evt-attempt-2-start", "attempt-2", "attempt.started", 1, "2026-03-06T09:59:00Z")
	seedEvent("evt-attempt-2-progress", "attempt-2", "attempt.progress", 2, "2026-03-06T10:02:00Z")

	res := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/runs/" + runID + "/timeline?limit=10",
		actorID: "actor-1",
		roleID:  "chancellor",
	})
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", res.Code)
	}

	var payload gen.RunTimelineResponse
	if err := json.Unmarshal(res.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode timeline response: %v", err)
	}
	if len(payload.Items) != 4 {
		t.Fatalf("expected 4 timeline items, got %d", len(payload.Items))
	}

	if got := []int{payload.Items[0].RunEventSeq, payload.Items[1].RunEventSeq, payload.Items[2].RunEventSeq, payload.Items[3].RunEventSeq}; !slices.Equal(got, []int{1, 2, 3, 4}) {
		t.Fatalf("expected authoritative run_event_seq ordering, got %v", got)
	}
	if got := []int{payload.Items[0].AttemptSeq, payload.Items[1].AttemptSeq, payload.Items[2].AttemptSeq, payload.Items[3].AttemptSeq}; !slices.Equal(got, []int{1, 2, 1, 2}) {
		t.Fatalf("expected attempt-local sequence metadata to reset by attempt, got %v", got)
	}
}

func TestServerTimelineIncludesMailboxRevisionsForSameLogicalMessage(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunWithLinearExecutionWorkflow(t, handler)
	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_mailbox_revision_from', 'council-member', 'Council', 'idle', NULL, 0, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_mailbox_revision_to', 'king', 'King', 'idle', NULL, 0, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)

	mailbox := state.NewMailboxStore(db, nil)
	messageID := "msg_mailbox_timeline_revision"
	if _, err := mailbox.SendMessage(context.Background(), state.SendMailboxMessageRequest{
		MessageID:   messageID,
		RunID:       runID,
		FromAgentID: "agent_mailbox_revision_from",
		ToAgentID:   "agent_mailbox_revision_to",
		ThreadID:    "thread_mailbox_revision",
		MessageType: state.MailboxMessageTypeStatus,
		Priority:    state.MailboxPriorityNormal,
		PayloadJSON: `{"message_source":"communication","communication_type":"response_received","node_id":"plan_drafting","reason_code":"execution_completed","response_full":"Original mailbox body"}`,
		CreatedAt:   "2026-03-01T12:00:00Z",
	}); err != nil {
		t.Fatalf("send original mailbox revision message: %v", err)
	}
	if _, err := mailbox.SendMessage(context.Background(), state.SendMailboxMessageRequest{
		MessageID:   messageID,
		RunID:       runID,
		FromAgentID: "agent_mailbox_revision_from",
		ToAgentID:   "agent_mailbox_revision_to",
		ThreadID:    "thread_mailbox_revision",
		MessageType: state.MailboxMessageTypeStatus,
		Priority:    state.MailboxPriorityNormal,
		PayloadJSON: `{"message_source":"communication","communication_type":"response_received","node_id":"plan_drafting","reason_code":"plan_rework","response_full":"Updated mailbox body"}`,
		CreatedAt:   "2026-03-01T12:01:00Z",
	}); err != nil {
		t.Fatalf("send updated mailbox revision message: %v", err)
	}

	res := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/runs/" + runID + "/timeline?limit=10",
		actorID: "actor-1",
		roleID:  "chancellor",
	})
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", res.Code)
	}

	var payload gen.RunTimelineResponse
	if err := json.Unmarshal(res.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode timeline response: %v", err)
	}

	revisionItems := make([]gen.TimelineEvent, 0, 2)
	for _, item := range payload.Items {
		if item.EventType != "mailbox.message_recorded" {
			continue
		}
		if gotMessageID, _ := item.PayloadJson["message_id"].(string); gotMessageID == messageID {
			revisionItems = append(revisionItems, item)
		}
	}
	if len(revisionItems) != 2 {
		t.Fatalf("expected two mailbox timeline revisions for the logical message, got %d (%#v)", len(revisionItems), revisionItems)
	}
	if revisionItems[0].RunEventSeq >= revisionItems[1].RunEventSeq {
		t.Fatalf("expected mailbox timeline revisions in ascending run_event_seq order, got %#v", revisionItems)
	}
	if gotVersion, _ := revisionItems[0].PayloadJson["message_version"].(float64); gotVersion != 1 {
		t.Fatalf("expected first mailbox revision version 1, got %#v", revisionItems[0].PayloadJson["message_version"])
	}
	if gotVersion, _ := revisionItems[1].PayloadJson["message_version"].(float64); gotVersion != 2 {
		t.Fatalf("expected second mailbox revision version 2, got %#v", revisionItems[1].PayloadJson["message_version"])
	}
}

func TestServerListRunPlanReviewsReturnsLatestFirst(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunWithPlanningWaveWorkflow(t, handler, withCreateRunBody(map[string]any{"autonomous_approvals": false}))
	mustExecSQL(t, db, `UPDATE workflow_runs SET run_status = 'running' WHERE run_id = ?`, runID)
	currentPlanRevisionID := seedReadyPlanReworkNodes(t, db, runID)
	olderPlanMessageID := "msg_plan_review_older_" + runID
	olderPlanRevisionID := "planrev_plan_review_older_" + runID
	mustExecSQL(t, db, `INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at) VALUES (?, ?, ?, ?, ?, 'status_update', 'normal', ?, ?)`,
		olderPlanMessageID,
		runID,
		"agent_council_"+runID,
		"agent_king_"+runID,
		"thread_"+runID+"_plan_old",
		`{"message_source":"communication","communication_type":"response_received","node_id":"plan_drafting","response_full":"Older plan revision"}`,
		"2026-03-01T12:00:00Z",
	)
	seedAttemptExecution(t, db, runID+":plan_drafting:historic", runID, "nr_plan_rework_plan_"+runID, "2026-03-01T12:00:00Z")
	mustExecSQL(t, db, `INSERT INTO run_planning_outputs (planning_output_id, run_id, node_id, node_run_id, attempt_id, agent_id, source_message_id, kind, content_markdown, created_at) VALUES (?, ?, 'plan_drafting', ?, ?, ?, ?, 'plan_revision', 'Older plan revision', '2026-03-01T12:00:00Z')`, olderPlanRevisionID, runID, "nr_plan_rework_plan_"+runID, runID+":plan_drafting:historic", "agent_council_"+runID, olderPlanMessageID)
	olderRunPlanRevisionID := mustRefreshRunPlanRevision(t, db, runID).PlanRevisionID
	mustExecSQL(t, db, `INSERT INTO run_plan_reviews (plan_review_id, run_id, reviewed_plan_revision_id, decision, feedback_markdown, actor_id, role_id, queued_advisory_attempt, queued_plan_attempt, created_at) VALUES ('plan_review_old', ?, ?, 'approved', 'Approved baseline direction.', 'god-operator', 'allfather', NULL, NULL, '2026-03-01T12:01:00Z')`, runID, olderRunPlanRevisionID)
	mustExecSQL(t, db, `INSERT INTO run_plan_reviews (plan_review_id, run_id, reviewed_plan_revision_id, decision, feedback_markdown, actor_id, role_id, queued_advisory_attempt, queued_plan_attempt, created_at) VALUES ('plan_review_new', ?, ?, 'needs_rework', 'Tighten acceptance criteria.', 'god-operator', 'allfather', 2, 2, '2026-03-01T12:02:00Z')`, runID, currentPlanRevisionID)

	res := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/runs/" + runID + "/plan-reviews",
		actorID: "actor-1",
		roleID:  "chancellor",
	})
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (%s)", res.Code, res.Body.String())
	}

	var payload gen.RunPlanReviewsResponse
	if err := json.Unmarshal(res.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode run plan reviews response: %v", err)
	}
	if len(payload.Items) != 2 {
		t.Fatalf("expected 2 plan reviews, got %d", len(payload.Items))
	}
	if payload.Items[0].PlanReviewId != "plan_review_new" {
		t.Fatalf("expected newest plan review first, got %q", payload.Items[0].PlanReviewId)
	}
	if payload.Items[0].ReviewedPlanRevisionId != currentPlanRevisionID {
		t.Fatalf("expected current reviewed plan revision id %q, got %q", currentPlanRevisionID, payload.Items[0].ReviewedPlanRevisionId)
	}
	if payload.Items[0].QueuedAdvisoryAttempt == nil || *payload.Items[0].QueuedAdvisoryAttempt != 2 {
		t.Fatalf("expected queued advisory attempt 2, got %#v", payload.Items[0].QueuedAdvisoryAttempt)
	}
}

func TestServerListRunGovernanceDecisionsReturnsUnifiedNewestFirstHistory(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunWithLinearExecutionWorkflow(t, handler)
	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_hist_from', 'council-member', 'Council', 'idle', NULL, 0, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_hist_to', 'king', 'King', 'idle', NULL, 0, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExecSQL(t, db, `INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at) VALUES ('msg_plan_hist', ?, 'agent_hist_from', 'agent_hist_to', 'thread_hist', 'status_update', 'normal', '{"message_source":"communication","communication_type":"response_received","node_id":"plan_drafting","response_full":"Historic plan"}', '2026-03-01T12:00:00Z')`, runID)
	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_plan_hist', ?, 'plan_drafting', 1,1, 'succeeded', 'council-member', 'agent_hist_from', 1, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`, runID)
	seedAttemptExecution(t, db, runID+":plan_drafting:hist", runID, "nr_plan_hist", "2026-03-01T12:00:00Z")
	mustExecSQL(t, db, `INSERT INTO run_planning_outputs (planning_output_id, run_id, node_id, node_run_id, attempt_id, agent_id, source_message_id, kind, content_markdown, created_at) VALUES ('planrev_hist', ?, 'plan_drafting', 'nr_plan_hist', ?, 'agent_hist_from', 'msg_plan_hist', 'plan_revision', 'Historic plan', '2026-03-01T12:00:00Z')`, runID, runID+":plan_drafting:hist")
	historicPlanRevisionID := mustRefreshRunPlanRevision(t, db, runID).PlanRevisionID
	mustExecSQL(t, db, `INSERT INTO run_plan_reviews (plan_review_id, run_id, reviewed_plan_revision_id, decision, feedback_markdown, actor_id, role_id, queued_advisory_attempt, queued_plan_attempt, created_at) VALUES ('plan_review_hist', ?, ?, 'needs_rework', 'Narrow the scope.', 'god-operator', 'allfather', 2, 2, '2026-03-01T12:01:00Z')`, runID, historicPlanRevisionID)
	mustExecSQL(t, db, `INSERT INTO run_final_reviews (final_review_id, run_id, decision, feedback_markdown, actor_id, role_id, queued_plan_attempt, created_at) VALUES ('final_review_hist', ?, 'confirmed', '', 'god-operator', 'allfather', NULL, '2026-03-01T12:02:00Z')`, runID)

	res := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/runs/" + runID + "/governance-decisions",
		actorID: "actor-1",
		roleID:  "chancellor",
	})
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (%s)", res.Code, res.Body.String())
	}

	var payload struct {
		RunID string `json:"run_id"`
		Items []struct {
			DecisionID       string    `json:"decision_id"`
			Stage            string    `json:"stage"`
			Decision         string    `json:"decision"`
			FeedbackMarkdown string    `json:"feedback_markdown"`
			CreatedAt        time.Time `json:"created_at"`
		} `json:"items"`
	}
	if err := json.Unmarshal(res.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode governance decisions payload: %v", err)
	}
	if len(payload.Items) != 2 {
		t.Fatalf("expected 2 governance decisions, got %d", len(payload.Items))
	}
	if payload.Items[0].DecisionID != "final_review_hist" || payload.Items[0].Stage != "final" || payload.Items[0].Decision != "confirmed" {
		t.Fatalf("expected newest final decision first, got %#v", payload.Items[0])
	}
	if payload.Items[1].DecisionID != "plan_review_hist" || payload.Items[1].Stage != "plan" || payload.Items[1].Decision != "needs_rework" {
		t.Fatalf("expected plan review second, got %#v", payload.Items[1])
	}
}

func TestServerListRunsOrdersNewestFirstAndHonorsLimit(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)

	mustExecSQL(t, db, `INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree-list-1', 'First', 'First', '{}', '2026-03-02T09:00:00Z')`)
	mustExecSQL(t, db, `INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree-list-2', 'Second', 'Second', '{}', '2026-03-02T10:00:00Z')`)
	mustExecSQL(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef-list-1', 'royal-decree-v1', 'v1', '{"version":"v1","workflow_id":"royal-decree-v1","nodes":[{"node_id":"kickoff","required_role":"council-member","node_class":"execution_kickoff"}],"edges":[]}', '2026-03-02T09:00:00Z')`)
	mustExecSQL(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef-list-2', 'royal-decree-v2', 'v2', '{"version":"v2","workflow_id":"royal-decree-v2","nodes":[{"node_id":"kickoff","required_role":"council-member","node_class":"execution_kickoff"}],"edges":[]}', '2026-03-02T10:00:00Z')`)

	mustExecSQL(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, rework_cycle_count, row_version, created_at, updated_at) VALUES ('run-old', NULL, 'decree-list-1', 'wfdef-list-1', 'succeeded', 'approved', 0, 1, '2026-03-02T09:10:00Z', '2026-03-02T09:20:00Z')`)
	mustExecSQL(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, rework_cycle_count, row_version, created_at, updated_at) VALUES ('run-new', NULL, 'decree-list-2', 'wfdef-list-2', 'running', 'draft', 0, 1, '2026-03-02T10:10:00Z', '2026-03-02T10:20:00Z')`)

	res := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/runs?limit=1",
		actorID: "actor-1",
		roleID:  "chancellor",
	})

	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (%s)", res.Code, res.Body.String())
	}

	var payload struct {
		Items []gen.RunSummary `json:"items"`
	}
	if err := json.Unmarshal(res.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode list runs payload: %v", err)
	}

	if len(payload.Items) != 1 {
		t.Fatalf("expected 1 run item, got %d", len(payload.Items))
	}

	if payload.Items[0].RunId != "run-new" {
		t.Fatalf("expected newest run first, got %s", payload.Items[0].RunId)
	}
	if payload.Items[0].DecreeTitle != "Second" {
		t.Fatalf("expected decree title Second, got %q", payload.Items[0].DecreeTitle)
	}
}

func TestServerListRunsSupportsCursorPaginationAndSearch(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)

	mustExecSQL(t, db, `INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree-list-search-1', 'Alpha decree', 'Alpha decree', '{}', '2026-03-02T09:00:00Z')`)
	mustExecSQL(t, db, `INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree-list-search-2', 'Bravo decree', 'Bravo decree', '{}', '2026-03-02T10:00:00Z')`)
	mustExecSQL(t, db, `INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree-list-search-3', 'Charlie decree', 'Charlie decree', '{}', '2026-03-02T11:00:00Z')`)
	mustExecSQL(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef-list-search-1', 'royal-decree-v1', 'v1', '{"version":"v1","workflow_id":"royal-decree-v1","nodes":[{"node_id":"kickoff","required_role":"council-member","node_class":"execution_kickoff"}],"edges":[]}', '2026-03-02T09:00:00Z')`)

	mustExecSQL(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, rework_cycle_count, row_version, created_at, updated_at) VALUES ('run-alpha', NULL, 'decree-list-search-1', 'wfdef-list-search-1', 'succeeded', 'approved', 0, 1, '2026-03-02T09:10:00Z', '2026-03-02T09:20:00Z')`)
	mustExecSQL(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, rework_cycle_count, row_version, created_at, updated_at) VALUES ('run-bravo', NULL, 'decree-list-search-2', 'wfdef-list-search-1', 'running', 'draft', 0, 1, '2026-03-02T10:10:00Z', '2026-03-02T10:20:00Z')`)
	mustExecSQL(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, rework_cycle_count, row_version, created_at, updated_at) VALUES ('run-charlie', NULL, 'decree-list-search-3', 'wfdef-list-search-1', 'failed', 'approved', 0, 1, '2026-03-02T11:10:00Z', '2026-03-02T11:20:00Z')`)

	first := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/runs?limit=1",
		actorID: "actor-1",
		roleID:  "chancellor",
	})
	if first.Code != http.StatusOK {
		t.Fatalf("expected first page 200, got %d (%s)", first.Code, first.Body.String())
	}

	var firstPage struct {
		Items      []gen.RunSummary `json:"items"`
		NextCursor *string          `json:"next_cursor"`
	}
	if err := json.Unmarshal(first.Body.Bytes(), &firstPage); err != nil {
		t.Fatalf("decode first page: %v", err)
	}
	if len(firstPage.Items) != 1 || firstPage.Items[0].RunId != "run-charlie" {
		t.Fatalf("expected newest run on first page, got %#v", firstPage.Items)
	}
	if firstPage.NextCursor == nil || *firstPage.NextCursor == "" {
		t.Fatal("expected next_cursor for paginated runs list")
	}

	second := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/runs?limit=1&cursor=" + *firstPage.NextCursor,
		actorID: "actor-1",
		roleID:  "chancellor",
	})
	if second.Code != http.StatusOK {
		t.Fatalf("expected second page 200, got %d (%s)", second.Code, second.Body.String())
	}

	var secondPage struct {
		Items      []gen.RunSummary `json:"items"`
		NextCursor *string          `json:"next_cursor"`
	}
	if err := json.Unmarshal(second.Body.Bytes(), &secondPage); err != nil {
		t.Fatalf("decode second page: %v", err)
	}
	if len(secondPage.Items) != 1 || secondPage.Items[0].RunId != "run-bravo" {
		t.Fatalf("expected second newest run on second page, got %#v", secondPage.Items)
	}

	searchRes := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/runs?search=bravo",
		actorID: "actor-1",
		roleID:  "chancellor",
	})
	if searchRes.Code != http.StatusOK {
		t.Fatalf("expected search page 200, got %d (%s)", searchRes.Code, searchRes.Body.String())
	}

	var searchPage struct {
		Items []gen.RunSummary `json:"items"`
	}
	if err := json.Unmarshal(searchRes.Body.Bytes(), &searchPage); err != nil {
		t.Fatalf("decode search page: %v", err)
	}
	if len(searchPage.Items) != 1 || searchPage.Items[0].RunId != "run-bravo" {
		t.Fatalf("expected search to return only run-bravo, got %#v", searchPage.Items)
	}
}

func TestServerListRunsSearchRanksRunIDMatchesAheadOfTitleMatches(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)

	mustExecSQL(t, db, `INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree-search-rank-1', 'Archived exact run', 'Archived exact run', '{}', '2026-03-01T09:00:00Z')`)
	mustExecSQL(t, db, `INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree-search-rank-2', 'Recent prefix run', 'Recent prefix run', '{}', '2026-03-02T09:00:00Z')`)
	mustExecSQL(t, db, `INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree-search-rank-3', 'run-target incident review', 'run-target incident review', '{}', '2026-03-03T09:00:00Z')`)
	mustExecSQL(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef-search-rank', 'royal-decree-v1', 'v1', '{"version":"v1","workflow_id":"royal-decree-v1","nodes":[{"node_id":"kickoff","required_role":"council-member","node_class":"execution_kickoff"}],"edges":[]}', '2026-03-01T09:00:00Z')`)

	mustExecSQL(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, rework_cycle_count, row_version, created_at, updated_at) VALUES ('run-target', NULL, 'decree-search-rank-1', 'wfdef-search-rank', 'succeeded', 'approved', 0, 1, '2026-03-01T09:10:00Z', '2026-03-01T09:20:00Z')`)
	mustExecSQL(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, rework_cycle_count, row_version, created_at, updated_at) VALUES ('run-target-archive', NULL, 'decree-search-rank-2', 'wfdef-search-rank', 'running', 'draft', 0, 1, '2026-03-02T09:10:00Z', '2026-03-02T09:20:00Z')`)
	mustExecSQL(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, rework_cycle_count, row_version, created_at, updated_at) VALUES ('run-unrelated', NULL, 'decree-search-rank-3', 'wfdef-search-rank', 'failed', 'approved', 0, 1, '2026-03-03T09:10:00Z', '2026-03-03T09:20:00Z')`)

	first := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/runs?search=run-target&limit=1",
		actorID: "actor-1",
		roleID:  "chancellor",
	})
	if first.Code != http.StatusOK {
		t.Fatalf("expected first search page 200, got %d (%s)", first.Code, first.Body.String())
	}

	var firstPage struct {
		Items      []gen.RunSummary `json:"items"`
		NextCursor *string          `json:"next_cursor"`
	}
	if err := json.Unmarshal(first.Body.Bytes(), &firstPage); err != nil {
		t.Fatalf("decode ranked search first page: %v", err)
	}
	if len(firstPage.Items) != 1 || firstPage.Items[0].RunId != "run-target" {
		t.Fatalf("expected exact run id match first, got %#v", firstPage.Items)
	}
	if firstPage.NextCursor == nil || *firstPage.NextCursor == "" {
		t.Fatal("expected next cursor for ranked search")
	}

	second := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/runs?search=run-target&limit=1&cursor=" + *firstPage.NextCursor,
		actorID: "actor-1",
		roleID:  "chancellor",
	})
	if second.Code != http.StatusOK {
		t.Fatalf("expected second search page 200, got %d (%s)", second.Code, second.Body.String())
	}

	var secondPage struct {
		Items []gen.RunSummary `json:"items"`
	}
	if err := json.Unmarshal(second.Body.Bytes(), &secondPage); err != nil {
		t.Fatalf("decode ranked search second page: %v", err)
	}
	if len(secondPage.Items) != 1 || secondPage.Items[0].RunId != "run-target-archive" {
		t.Fatalf("expected run id prefix match second, got %#v", secondPage.Items)
	}

	third := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/runs?search=run-target",
		actorID: "actor-1",
		roleID:  "chancellor",
	})
	if third.Code != http.StatusOK {
		t.Fatalf("expected full ranked search page 200, got %d (%s)", third.Code, third.Body.String())
	}

	var thirdPage struct {
		Items []gen.RunSummary `json:"items"`
	}
	if err := json.Unmarshal(third.Body.Bytes(), &thirdPage); err != nil {
		t.Fatalf("decode ranked search page: %v", err)
	}
	if len(thirdPage.Items) != 3 || thirdPage.Items[2].RunId != "run-unrelated" {
		t.Fatalf("expected title token match after run id matches, got %#v", thirdPage.Items)
	}
}

func TestServerListRunsSearchUsesTokenPrefixAndRejectsMiddleSubstrings(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)

	mustExecSQL(t, db, `INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree-search-prefix-1', 'Older historical run', 'Older historical run', '{}', '2026-03-02T09:00:00Z')`)
	mustExecSQL(t, db, `INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree-search-prefix-2', 'Older archival run', 'Older archival run', '{}', '2026-03-03T09:00:00Z')`)
	mustExecSQL(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef-search-prefix', 'royal-decree-v1', 'v1', '{"version":"v1","workflow_id":"royal-decree-v1","nodes":[{"node_id":"kickoff","required_role":"council-member","node_class":"execution_kickoff"}],"edges":[]}', '2026-03-01T09:00:00Z')`)

	mustExecSQL(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, rework_cycle_count, row_version, created_at, updated_at) VALUES ('run-historical', NULL, 'decree-search-prefix-1', 'wfdef-search-prefix', 'succeeded', 'approved', 0, 1, '2026-03-02T09:10:00Z', '2026-03-02T09:20:00Z')`)
	mustExecSQL(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, rework_cycle_count, row_version, created_at, updated_at) VALUES ('run-archival', NULL, 'decree-search-prefix-2', 'wfdef-search-prefix', 'running', 'draft', 0, 1, '2026-03-03T09:10:00Z', '2026-03-03T09:20:00Z')`)

	prefixRes := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/runs?search=older%20histor",
		actorID: "actor-1",
		roleID:  "chancellor",
	})
	if prefixRes.Code != http.StatusOK {
		t.Fatalf("expected token-prefix search 200, got %d (%s)", prefixRes.Code, prefixRes.Body.String())
	}

	var prefixPage struct {
		Items []gen.RunSummary `json:"items"`
	}
	if err := json.Unmarshal(prefixRes.Body.Bytes(), &prefixPage); err != nil {
		t.Fatalf("decode token-prefix search page: %v", err)
	}
	if len(prefixPage.Items) != 1 || prefixPage.Items[0].RunId != "run-historical" {
		t.Fatalf("expected token-prefix search to return only historical run, got %#v", prefixPage.Items)
	}

	substringRes := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/runs?search=stor",
		actorID: "actor-1",
		roleID:  "chancellor",
	})
	if substringRes.Code != http.StatusOK {
		t.Fatalf("expected middle-substring search 200, got %d (%s)", substringRes.Code, substringRes.Body.String())
	}

	var substringPage struct {
		Items []gen.RunSummary `json:"items"`
	}
	if err := json.Unmarshal(substringRes.Body.Bytes(), &substringPage); err != nil {
		t.Fatalf("decode middle-substring search page: %v", err)
	}
	if len(substringPage.Items) != 0 {
		t.Fatalf("expected middle-of-word substring search to return no matches, got %#v", substringPage.Items)
	}
}

func TestServerListRunsCanFilterPendingManualGovernanceByRecentActivity(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)

	mustExecSQL(t, db, `INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree-pending-old', 'Pending Old', 'Pending Old', '{}', '2026-03-01T09:00:00Z')`)
	mustExecSQL(t, db, `INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree-pending-new', 'Pending New', 'Pending New', '{}', '2026-03-02T09:00:00Z')`)
	mustExecSQL(t, db, `INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree-ignored', 'Ignored', 'Ignored', '{}', '2026-03-03T09:00:00Z')`)
	mustExecSQL(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef-pending', 'royal-decree-v1', 'v1', '{"version":"v1","workflow_id":"royal-decree-v1","nodes":[{"node_id":"kickoff","required_role":"council-member","node_class":"execution_kickoff"}],"edges":[]}', '2026-03-01T09:00:00Z')`)

	mustExecSQL(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, autonomous_approvals, pending_approval_key, rework_cycle_count, row_version, created_at, updated_at) VALUES ('run-pending-old', NULL, 'decree-pending-old', 'wfdef-pending', 'awaiting_human_confirmation', 'approved', 0, 'final:gate-pending-old', 0, 1, '2026-03-01T09:10:00Z', '2026-03-05T11:00:00Z')`)
	mustExecSQL(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, autonomous_approvals, pending_approval_key, rework_cycle_count, row_version, created_at, updated_at) VALUES ('run-pending-new', NULL, 'decree-pending-new', 'wfdef-pending', 'running', 'awaiting_king_decision', 0, 'plan:gate-pending-new', 0, 1, '2026-03-02T09:10:00Z', '2026-03-05T12:00:00Z')`)
	mustExecSQL(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, autonomous_approvals, rework_cycle_count, row_version, created_at, updated_at) VALUES ('run-ignored', NULL, 'decree-ignored', 'wfdef-pending', 'awaiting_human_confirmation', 'approved', 1, 0, 1, '2026-03-03T09:10:00Z', '2026-03-05T13:00:00Z')`)

	res := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/runs?state_filter=awaiting_human_decision",
		actorID: "actor-1",
		roleID:  "chancellor",
	})

	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (%s)", res.Code, res.Body.String())
	}

	var payload struct {
		Items []gen.RunSummary `json:"items"`
	}
	if err := json.Unmarshal(res.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode filtered list runs payload: %v", err)
	}

	if len(payload.Items) != 2 {
		t.Fatalf("expected 2 pending manual governance runs, got %d", len(payload.Items))
	}
	if payload.Items[0].RunId != "run-pending-new" {
		t.Fatalf("expected most recently updated pending run first, got %s", payload.Items[0].RunId)
	}
	if payload.Items[1].RunId != "run-pending-old" {
		t.Fatalf("expected older pending run second, got %s", payload.Items[1].RunId)
	}
	if payload.Items[0].DisplayState != gen.RunDisplayStateAwaitingPlanDecision {
		t.Fatalf("expected newest pending run display_state awaiting_plan_decision, got %q", payload.Items[0].DisplayState)
	}
	if payload.Items[0].PendingApprovalKey == nil || *payload.Items[0].PendingApprovalKey != "plan:gate-pending-new" {
		t.Fatalf("expected newest pending run pending_approval_key plan:gate-pending-new, got %#v", payload.Items[0].PendingApprovalKey)
	}
	if payload.Items[1].DisplayState != gen.RunDisplayStateAwaitingFinalDecision {
		t.Fatalf("expected older pending run display_state awaiting_final_decision, got %q", payload.Items[1].DisplayState)
	}
	if payload.Items[1].PendingApprovalKey == nil || *payload.Items[1].PendingApprovalKey != "final:gate-pending-old" {
		t.Fatalf("expected older pending run pending_approval_key final:gate-pending-old, got %#v", payload.Items[1].PendingApprovalKey)
	}
}

func TestServerListRunsKeepsPlanPendingApprovalKeyStableAcrossProjectionCatchUp(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)

	mustExecSQL(t, db, `INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree-plan-stable', 'Plan Stable', 'Plan Stable', '{}', '2026-03-01T09:00:00Z')`)
	mustExecSQL(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef-plan-stable', 'royal-decree-v1', 'v1', '{"version":"v1","workflow_id":"royal-decree-v1","nodes":[],"edges":[]}', '2026-03-01T09:00:00Z')`)
	mustExecSQL(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, autonomous_approvals, pending_approval_key, rework_cycle_count, row_version, created_at, updated_at) VALUES ('run-plan-stable', NULL, 'decree-plan-stable', 'wfdef-plan-stable', 'running', 'awaiting_king_decision', 0, 'plan:gate-stable', 0, 1, '2026-03-01T09:10:00Z', '2026-03-05T12:00:00Z')`)

	assertPendingApprovalKey := func() {
		t.Helper()
		res := performJSONRequest(t, handler, requestSpec{
			method:  http.MethodGet,
			path:    "/api/v1/runs?state_filter=awaiting_human_decision",
			actorID: "actor-1",
			roleID:  "chancellor",
		})
		if res.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d (%s)", res.Code, res.Body.String())
		}

		var payload struct {
			Items []gen.RunSummary `json:"items"`
		}
		if err := json.Unmarshal(res.Body.Bytes(), &payload); err != nil {
			t.Fatalf("decode stable pending approval payload: %v", err)
		}
		if len(payload.Items) != 1 {
			t.Fatalf("expected 1 pending manual governance run, got %d", len(payload.Items))
		}
		if payload.Items[0].PendingApprovalKey == nil || *payload.Items[0].PendingApprovalKey != "plan:gate-stable" {
			t.Fatalf("expected stable pending approval key plan:gate-stable, got %#v", payload.Items[0].PendingApprovalKey)
		}
	}

	assertPendingApprovalKey()

	mustExecSQL(t, db, `INSERT INTO run_plan_revisions (plan_revision_id, run_id, content_markdown, components_json, created_at) VALUES ('planrev_stable', 'run-plan-stable', 'Plan stable', '[]', '2026-03-05T12:01:00Z')`)

	assertPendingApprovalKey()
}

func TestServerListRunsStateFilterKeepsOperatorBucketsNonOverlapping(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)

	mustExecSQL(t, db, `INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree-running', 'Running', 'Running', '{}', '2026-03-01T09:00:00Z')`)
	mustExecSQL(t, db, `INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree-plan', 'Plan Wait', 'Plan Wait', '{}', '2026-03-01T09:01:00Z')`)
	mustExecSQL(t, db, `INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree-final', 'Final Wait', 'Final Wait', '{}', '2026-03-01T09:02:00Z')`)
	mustExecSQL(t, db, `INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree-auto-final', 'Autonomous Final', 'Autonomous Final', '{}', '2026-03-01T09:03:00Z')`)
	mustExecSQL(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef-filter', 'royal-decree-v1', 'v1', '{"version":"v1","workflow_id":"royal-decree-v1","nodes":[{"node_id":"kickoff","required_role":"council-member","node_class":"execution_kickoff"}],"edges":[]}', '2026-03-01T09:00:00Z')`)

	mustExecSQL(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, autonomous_approvals, rework_cycle_count, row_version, created_at, updated_at) VALUES ('run-running', NULL, 'decree-running', 'wfdef-filter', 'running', 'approved', 1, 0, 1, '2026-03-01T09:10:00Z', '2026-03-01T09:10:00Z')`)
	mustExecSQL(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, autonomous_approvals, rework_cycle_count, row_version, created_at, updated_at) VALUES ('run-plan', NULL, 'decree-plan', 'wfdef-filter', 'running', 'awaiting_king_decision', 0, 0, 1, '2026-03-01T09:11:00Z', '2026-03-01T09:11:00Z')`)
	mustExecSQL(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, autonomous_approvals, rework_cycle_count, row_version, created_at, updated_at) VALUES ('run-final', NULL, 'decree-final', 'wfdef-filter', 'awaiting_human_confirmation', 'approved', 0, 0, 1, '2026-03-01T09:12:00Z', '2026-03-01T09:12:00Z')`)
	mustExecSQL(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, autonomous_approvals, rework_cycle_count, row_version, created_at, updated_at) VALUES ('run-auto-final', NULL, 'decree-auto-final', 'wfdef-filter', 'awaiting_human_confirmation', 'approved', 1, 0, 1, '2026-03-01T09:13:00Z', '2026-03-01T09:13:00Z')`)

	runningRes := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/runs?state_filter=running",
		actorID: "actor-1",
		roleID:  "chancellor",
	})
	if runningRes.Code != http.StatusOK {
		t.Fatalf("expected running filter 200, got %d (%s)", runningRes.Code, runningRes.Body.String())
	}

	var runningPayload struct {
		Items []gen.RunSummary `json:"items"`
	}
	if err := json.Unmarshal(runningRes.Body.Bytes(), &runningPayload); err != nil {
		t.Fatalf("decode running filter payload: %v", err)
	}
	if len(runningPayload.Items) != 1 || runningPayload.Items[0].RunId != "run-running" {
		t.Fatalf("expected only active running run, got %#v", runningPayload.Items)
	}
	if runningPayload.Items[0].DisplayState != gen.RunDisplayStateRunning {
		t.Fatalf("expected running display_state, got %q", runningPayload.Items[0].DisplayState)
	}

	decisionRes := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/runs?state_filter=awaiting_human_decision",
		actorID: "actor-1",
		roleID:  "chancellor",
	})
	if decisionRes.Code != http.StatusOK {
		t.Fatalf("expected awaiting_human_decision filter 200, got %d (%s)", decisionRes.Code, decisionRes.Body.String())
	}

	var decisionPayload struct {
		Items []gen.RunSummary `json:"items"`
	}
	if err := json.Unmarshal(decisionRes.Body.Bytes(), &decisionPayload); err != nil {
		t.Fatalf("decode awaiting_human_decision payload: %v", err)
	}
	if len(decisionPayload.Items) != 2 {
		t.Fatalf("expected two manual decision runs, got %d", len(decisionPayload.Items))
	}
	if decisionPayload.Items[0].RunId != "run-final" || decisionPayload.Items[0].DisplayState != gen.RunDisplayStateAwaitingFinalDecision {
		t.Fatalf("expected final decision run first, got %#v", decisionPayload.Items[0])
	}
	if decisionPayload.Items[1].RunId != "run-plan" || decisionPayload.Items[1].DisplayState != gen.RunDisplayStateAwaitingPlanDecision {
		t.Fatalf("expected plan decision run second, got %#v", decisionPayload.Items[1])
	}
}

func TestServerGetRunReturnsDerivedDisplayState(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunWithLinearExecutionWorkflow(t, handler)
	mustExecSQL(t, db, `UPDATE workflow_runs SET run_status = 'awaiting_human_confirmation', plan_state = 'approved', autonomous_approvals = 0, pending_approval_key = 'final:gate-run-detail', row_version = 7 WHERE run_id = ?`, runID)

	res := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/runs/" + runID,
		actorID: "actor-1",
		roleID:  "chancellor",
	})
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (%s)", res.Code, res.Body.String())
	}

	var summary gen.RunDetailSummary
	if err := json.Unmarshal(res.Body.Bytes(), &summary); err != nil {
		t.Fatalf("decode get run payload: %v", err)
	}
	if summary.DisplayState != gen.RunDisplayStateAwaitingFinalDecision {
		t.Fatalf("expected derived display_state awaiting_final_decision, got %q", summary.DisplayState)
	}
	if summary.PendingApprovalKey == nil || *summary.PendingApprovalKey != "final:gate-run-detail" {
		t.Fatalf("expected stable final pending approval key, got %#v", summary.PendingApprovalKey)
	}
}

func TestServerGetRunOmitsStalePlanPendingApprovalKeyForTerminalRun(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunWithLinearExecutionWorkflow(t, handler)
	mustExecSQL(t, db, `UPDATE workflow_runs SET run_status = 'cancelled', plan_state = 'awaiting_king_decision', autonomous_approvals = 0, pending_approval_key = 'plan:gate-run-detail', row_version = 7 WHERE run_id = ?`, runID)

	res := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/runs/" + runID,
		actorID: "actor-1",
		roleID:  "chancellor",
	})
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (%s)", res.Code, res.Body.String())
	}

	var summary gen.RunDetailSummary
	if err := json.Unmarshal(res.Body.Bytes(), &summary); err != nil {
		t.Fatalf("decode get run payload: %v", err)
	}
	if summary.DisplayState != gen.RunDisplayStateCancelled {
		t.Fatalf("expected derived display_state cancelled, got %q", summary.DisplayState)
	}
	if summary.PendingApprovalKey != nil {
		t.Fatalf("expected stale plan pending approval key to be omitted, got %#v", summary.PendingApprovalKey)
	}
}

func TestServerGetRunIncludesPendingSettlementNodeInFailedNodes(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunWithLinearExecutionWorkflow(t, handler)
	now := "2026-03-06T10:01:00Z"

	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent-run-recovery-pending', 'builder-berserker', 'Builder', 'busy', NULL, 0, ?, ?)`, now, now)
	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr-run-recovery-pending', ?, 'delegated_execution_berserker', 1,1, 'failed', 'builder-berserker', 'agent-run-recovery-pending', 1, ?, ?)`, runID, now, now)
	mustExecSQL(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, lease_owner, lease_expires_at, lease_version, session_ref, provider_session_id, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('attempt-run-recovery-pending', ?, 'nr-run-recovery-pending', 'terminal_observed', 'provider_backed', '', NULL, 1, 'session-run-recovery-pending', 'ses-run-recovery-pending', ?, ?, NULL, 'provider_incomplete_turn', 'provider exited without an accepted kingdom_complete_attempt receipt', ?, ?)`, runID, now, now, now, now)

	res := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/runs/" + runID,
		actorID: "actor-1",
		roleID:  "chancellor",
	})
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (%s)", res.Code, res.Body.String())
	}

	var summary gen.RunDetailSummary
	if err := json.Unmarshal(res.Body.Bytes(), &summary); err != nil {
		t.Fatalf("decode get run payload: %v", err)
	}
	if len(summary.FailedNodes) != 1 {
		t.Fatalf("expected pending-settlement node to remain in failed_nodes, got %#v", summary.FailedNodes)
	}
	if summary.FailedNodes[0].NodeId != "delegated_execution_berserker" {
		t.Fatalf("unexpected failed node payload: %#v", summary.FailedNodes[0])
	}
}

func TestServerGetRunFailsWhenStoredModelSelectionJSONIsInvalid(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunWithLinearExecutionWorkflow(t, handler)
	mustExecSQL(t, db, `UPDATE workflow_runs SET model_selection_json = '{' WHERE run_id = ?`, runID)

	res := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/runs/" + runID,
		actorID: "actor-1",
		roleID:  "chancellor",
	})
	if res.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 for corrupt run model selection, got %d (%s)", res.Code, res.Body.String())
	}
}

func TestServerListRunsFailsWhenStoredModelSelectionJSONIsInvalid(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunWithLinearExecutionWorkflow(t, handler)
	mustExecSQL(t, db, `UPDATE workflow_runs SET model_selection_json = '{' WHERE run_id = ?`, runID)

	res := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/runs",
		actorID: "actor-1",
		roleID:  "chancellor",
	})
	if res.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 for corrupt run list model selection, got %d (%s)", res.Code, res.Body.String())
	}
}

func TestServerGetRunUpdatedAtReflectsNodeTransitionActivity(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunWithLinearExecutionWorkflow(t, handler)
	setRunStatus(t, db, runID, "running")

	now := "2026-03-03T11:20:00Z"
	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent-updated-at', 'council-member', 'idle', NULL, 0, ?, ?)`, now, now)
	nodeRunID := "nr-updated-at"
	fromNodeState := "pending"
	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES (?, ?, 'kickoff', 1,1, ?, 'council-member', 'agent-updated-at', 1, NULL, NULL)`, nodeRunID, runID, fromNodeState)

	targetState := "ready"

	occurredAt := "2026-03-03T11:22:00Z"
	svc := state.NewTransitionService(db, nil)
	if err := svc.Apply(context.Background(), state.TransitionRequest{
		TransitionID:        "tr-api-run-updated-at",
		EventID:             "evt-api-run-updated-at",
		RunID:               runID,
		NodeRunID:           sql.NullString{String: nodeRunID, Valid: true},
		Entity:              state.EntityTypeNode,
		FromState:           fromNodeState,
		ToState:             targetState,
		ActorID:             "actor-1",
		RoleID:              "chancellor",
		AuthoritySource:     "api.test",
		ReasonCode:          "internal_error",
		ReasonText:          "run freshness test",
		CorrelationID:       "corr-api-run-updated-at",
		CausationID:         "caus-api-run-updated-at",
		OccurredAt:          occurredAt,
		TransitionPrevHash:  "prev",
		TransitionEntryHash: "entry",
		EventPrevHash:       "prev-event",
		EventEntryHash:      "entry-event",
		AttemptID:           "attempt-api-run-updated-at",
		SchemaVersion:       "v1",
		EventType:           "state.transitioned",
		EventSeq:            1,
		PayloadJSON:         `{"scope":"api"}`,
	}); err != nil {
		t.Fatalf("apply node transition: %v", err)
	}

	res := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/runs/" + runID,
		actorID: "actor-1",
		roleID:  "chancellor",
	})
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (%s)", res.Code, res.Body.String())
	}

	var run gen.RunDetailSummary
	if err := json.Unmarshal(res.Body.Bytes(), &run); err != nil {
		t.Fatalf("decode run summary: %v", err)
	}
	occurredAtTime, err := time.Parse(time.RFC3339, occurredAt)
	if err != nil {
		t.Fatalf("parse occurred_at: %v", err)
	}
	if run.UpdatedAt.Before(occurredAtTime) {
		t.Fatalf("expected run updated_at >= %s, got %s", occurredAt, run.UpdatedAt.Format(time.RFC3339))
	}
	if run.DecreeTitle != "Linear replay decree" {
		t.Fatalf("expected decree title Linear replay decree, got %q", run.DecreeTitle)
	}
}

func TestServerGetRunIncludesTabCountsForRunDetailChrome(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunWithLinearExecutionWorkflow(t, handler)
	now := "2026-03-06T10:01:00Z"

	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent-run-counts-root', 'king', 'King', 'idle', NULL, 0, ?, ?)`, now, now)
	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent-run-counts-council', 'council-member', 'Council', 'idle', 'agent-run-counts-root', 1, ?, ?)`, now, now)
	mustExecSQL(t, db, `INSERT INTO run_participants (run_id, agent_id, role_id, display_name, parent_agent_id, spawn_depth, first_seen_at) VALUES (?, 'agent-run-counts-root', 'king', 'King', NULL, 0, ?)`, runID, now)
	mustExecSQL(t, db, `INSERT INTO run_participants (run_id, agent_id, role_id, display_name, parent_agent_id, spawn_depth, first_seen_at) VALUES (?, 'agent-run-counts-council', 'council-member', 'Council', 'agent-run-counts-root', 1, ?)`, runID, now)
	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr-run-counts-root', ?, 'plan_drafting', 1,1, 'running', 'king', 'agent-run-counts-root', 1, ?, NULL)`, runID, now)
	mustExecSQL(t, db, `INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at) VALUES ('msg-run-count-visible', ?, 'agent-run-counts-root', 'agent-run-counts-council', 'thread-1', 'status_update', 'normal', '{"message_source":"communication","communication_type":"instruction_dispatched"}', ?)`, runID, now)
	mustExecSQL(t, db, `INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at) VALUES ('msg-run-count-self', ?, 'agent-run-counts-root', 'agent-run-counts-root', 'thread-2', 'status_update', 'normal', '{"message_source":"communication","communication_type":"instruction_dispatched"}', ?)`, runID, now)
	mustExecSQL(t, db, `INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('evt-run-count-1', ?, 1, 'nr-run-counts-root', 'attempt-run-count-1', 'v1', 'attempt.started', 1, '{"step":1}', 'actor-1', 'chancellor', 'test', 'internal_error', 'seed', 'corr-run-count', 'caus-run-count', 'prev-run-count', 'entry-run-count', ?)`, runID, now)
	mustExecSQL(t, db, `INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('evt-run-count-2', ?, 2, 'nr-run-counts-root', 'attempt-run-count-2', 'v1', 'attempt.progress', 2, '{"step":2}', 'actor-1', 'chancellor', 'test', 'internal_error', 'seed', 'corr-run-count', 'caus-run-count', 'prev-run-count-2', 'entry-run-count-2', ?)`, runID, now)

	res := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/runs/" + runID,
		actorID: "actor-1",
		roleID:  "chancellor",
	})
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (%s)", res.Code, res.Body.String())
	}

	var run gen.RunDetailSummary
	if err := json.Unmarshal(res.Body.Bytes(), &run); err != nil {
		t.Fatalf("decode run detail summary: %v", err)
	}
	if run.TabCounts.DagNodes != 6 {
		t.Fatalf("expected dag_nodes=6, got %d", run.TabCounts.DagNodes)
	}
	if run.TabCounts.Agents != 2 {
		t.Fatalf("expected agents=2, got %d", run.TabCounts.Agents)
	}
	if run.TabCounts.ActivityEvents != 2 {
		t.Fatalf("expected activity_events=2, got %d", run.TabCounts.ActivityEvents)
	}
	if run.TabCounts.Messages != 1 {
		t.Fatalf("expected messages=1, got %d", run.TabCounts.Messages)
	}
}

func TestServerGetRunLastActivityReflectsMailboxMessage(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunWithLinearExecutionWorkflow(t, handler)
	baseline := "2026-03-03T11:20:00Z"
	messageAt := "2026-03-03T11:25:00Z"
	mustExecSQL(t, db, `UPDATE workflow_runs SET updated_at = ? WHERE run_id = ?`, baseline, runID)
	mustExecSQL(t, db, `UPDATE run_activity SET last_activity_at = ? WHERE run_id = ?`, baseline, runID)
	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent-mailbox-last-activity-a', 'chancellor', 'idle', NULL, 0, ?, ?)`, baseline, baseline)
	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent-mailbox-last-activity-b', 'council-member', 'idle', NULL, 0, ?, ?)`, baseline, baseline)

	mailbox := state.NewMailboxStore(db, nil)
	if _, err := mailbox.SendMessage(context.Background(), state.SendMailboxMessageRequest{
		RunID:       runID,
		FromAgentID: "agent-mailbox-last-activity-a",
		ToAgentID:   "agent-mailbox-last-activity-b",
		ThreadID:    "thread-last-activity",
		MessageType: state.MailboxMessageTypeStatus,
		Priority:    state.MailboxPriorityNormal,
		PayloadJSON: `{"message_source":"communication","communication_type":"response_received","response_full":"Execution reply"}`,
		CreatedAt:   messageAt,
	}); err != nil {
		t.Fatalf("send mailbox message: %v", err)
	}

	res := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/runs/" + runID,
		actorID: "actor-1",
		roleID:  "chancellor",
	})
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (%s)", res.Code, res.Body.String())
	}

	var run gen.RunDetailSummary
	if err := json.Unmarshal(res.Body.Bytes(), &run); err != nil {
		t.Fatalf("decode run detail summary: %v", err)
	}
	if run.LastActivityAt.Format(time.RFC3339) != messageAt {
		t.Fatalf("expected last_activity_at=%s, got %s", messageAt, run.LastActivityAt.Format(time.RFC3339))
	}
	if run.UpdatedAt.Format(time.RFC3339) != baseline {
		t.Fatalf("expected updated_at to remain %s, got %s", baseline, run.UpdatedAt.Format(time.RFC3339))
	}
}

func TestServerListRunsReturnsLastActivityFromAttemptRetryEvent(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunWithLinearExecutionWorkflow(t, handler)
	baseline := "2026-03-06T10:00:00Z"
	retryAt := "2026-03-06T10:08:00Z"
	mustExecSQL(t, db, `UPDATE workflow_runs SET run_status = 'running', updated_at = ? WHERE run_id = ?`, baseline, runID)
	mustExecSQL(t, db, `UPDATE run_activity SET last_activity_at = ? WHERE run_id = ?`, baseline, runID)
	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent-list-last-activity', 'builder-berserker', 'Builder', 'busy', NULL, 0, ?, ?)`, baseline, baseline)
	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr-list-last-activity', ?, 'delegated_execution_berserker', 1,1, 'running', 'builder-berserker', 'agent-list-last-activity', 1, ?, NULL)`, runID, baseline)

	ingestor := state.NewAttemptEventIngestor(db, nil)
	if _, err := ingestor.Ingest(context.Background(), state.AttemptEventIngestionRequest{
		EventID:         "evt-list-last-activity",
		RunID:           runID,
		NodeRunID:       sql.NullString{String: "nr-list-last-activity", Valid: true},
		AttemptID:       "attempt-list-last-activity",
		SchemaVersion:   "v1",
		EventType:       "attempt.retrying",
		Seq:             1,
		PayloadJSON:     `{"node_id":"delegated_execution_berserker","node_run_id":"nr-list-last-activity","retry_index":2,"retry_max":3,"delay_seconds":10,"reason_code":"provider_empty_output_retry"}`,
		ActorID:         "runtime_orchestrator",
		RoleID:          "builder-berserker",
		AuthoritySource: "runtime.orchestrator",
		ReasonCode:      "provider_empty_output_retry",
		ReasonText:      "retrying empty provider output after 10s",
		CorrelationID:   "corr-list-last-activity",
		CausationID:     "caus-list-last-activity",
		CreatedAt:       retryAt,
	}); err != nil {
		t.Fatalf("ingest retry event: %v", err)
	}

	res := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/runs",
		actorID: "actor-1",
		roleID:  "chancellor",
	})
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (%s)", res.Code, res.Body.String())
	}

	var payload struct {
		Items []gen.RunSummary `json:"items"`
	}
	if err := json.Unmarshal(res.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode run list payload: %v", err)
	}
	if len(payload.Items) == 0 {
		t.Fatal("expected at least one run summary")
	}
	summary := payload.Items[0]
	if summary.RunId != runID {
		t.Fatalf("expected run id %s, got %s", runID, summary.RunId)
	}
	if summary.LastActivityAt.Format(time.RFC3339) != retryAt {
		t.Fatalf("expected last_activity_at=%s, got %s", retryAt, summary.LastActivityAt.Format(time.RFC3339))
	}
	if summary.UpdatedAt.Format(time.RFC3339) != baseline {
		t.Fatalf("expected updated_at to remain %s, got %s", baseline, summary.UpdatedAt.Format(time.RFC3339))
	}
}

func TestServerGetRunIncludesAuthoritativeStreamCursor(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunWithLinearExecutionWorkflow(t, handler)
	now := "2026-03-06T10:01:00Z"

	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent-run-cursor', 'builder-berserker', 'Builder', 'busy', NULL, 0, ?, ?)`, now, now)
	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr-run-cursor', ?, 'delegated_execution_berserker', 1,1, 'running', 'builder-berserker', 'agent-run-cursor', 1, ?, NULL)`, runID, now)
	mustExecSQL(t, db, `INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('evt-run-cursor-1', ?, 1, 'nr-run-cursor', 'attempt-run-cursor', 'v1', 'attempt.started', 1, '{"step":1}', 'actor-1', 'chancellor', 'test', 'internal_error', 'seed', 'corr-run-cursor-1', 'caus-run-cursor-1', 'prev-run-cursor-1', 'entry-run-cursor-1', ?)`, runID, now)
	mustExecSQL(t, db, `INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('evt-run-cursor-2', ?, 2, 'nr-run-cursor', 'attempt-run-cursor', 'v1', 'attempt.retrying', 2, '{"step":2}', 'actor-1', 'chancellor', 'test', 'internal_error', 'seed', 'corr-run-cursor-2', 'caus-run-cursor-2', 'prev-run-cursor-2', 'entry-run-cursor-2', ?)`, runID, now)

	res := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/runs/" + runID,
		actorID: "actor-1",
		roleID:  "chancellor",
	})
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (%s)", res.Code, res.Body.String())
	}

	var run gen.RunDetailSummary
	if err := json.Unmarshal(res.Body.Bytes(), &run); err != nil {
		t.Fatalf("decode run detail summary: %v", err)
	}
	if run.StreamCursor == nil {
		t.Fatal("expected authoritative stream cursor")
	}
	if run.StreamCursor.EventId != "evt-run-cursor-2" {
		t.Fatalf("expected event cursor evt-run-cursor-2, got %q", run.StreamCursor.EventId)
	}
	if run.StreamCursor.RunEventSeq != 2 {
		t.Fatalf("expected run_event_seq=2, got %d", run.StreamCursor.RunEventSeq)
	}
}

func TestServerGetRunIncludesActiveRuntimeRetries(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunWithLinearExecutionWorkflow(t, handler)
	now := "2026-03-06T10:01:00Z"

	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent-run-retry', 'builder-berserker', 'Builder', 'busy', NULL, 0, ?, ?)`, now, now)
	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr-run-retry', ?, 'delegated_execution_berserker', 1,1, 'running', 'builder-berserker', 'agent-run-retry', 1, ?, NULL)`, runID, now)
	mustExecSQL(t, db, `INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('evt-run-retry', ?, 1, 'nr-run-retry', 'attempt-run-retry', 'v1', 'attempt.retrying', 1, '{"node_id":"delegated_execution_berserker","node_run_id":"nr-run-retry","retry_index":2,"retry_max":3,"delay_seconds":10,"reason_code":"provider_empty_output_retry"}', 'runtime_orchestrator', 'builder-berserker', 'runtime.orchestrator', 'provider_empty_output_retry', 'retrying empty provider output after 10s', 'corr-run-retry', 'caus-run-retry', 'prev-run-retry', 'entry-run-retry', ?)`, runID, now)

	res := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/runs/" + runID,
		actorID: "actor-1",
		roleID:  "chancellor",
	})
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (%s)", res.Code, res.Body.String())
	}

	var run gen.RunDetailSummary
	if err := json.Unmarshal(res.Body.Bytes(), &run); err != nil {
		t.Fatalf("decode run detail summary: %v", err)
	}
	if len(run.RuntimeRetries) != 1 {
		t.Fatalf("expected one active runtime retry, got %#v", run.RuntimeRetries)
	}
	retry := run.RuntimeRetries[0]
	if retry.NodeId != "delegated_execution_berserker" {
		t.Fatalf("expected runtime retry node_id delegated_execution_berserker, got %q", retry.NodeId)
	}
	if retry.NodeRunId != "nr-run-retry" {
		t.Fatalf("expected runtime retry node_run_id nr-run-retry, got %q", retry.NodeRunId)
	}
	if retry.RetryIndex != 2 || retry.RetryMax != 3 || retry.DelaySeconds != 10 {
		t.Fatalf("unexpected runtime retry payload: %#v", retry)
	}
}

func TestServerGetRunOmitsRecoveredRuntimeRetries(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunWithLinearExecutionWorkflow(t, handler)
	now := "2026-03-06T10:01:00Z"

	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent-run-retry-recovered', 'builder-berserker', 'Builder', 'busy', NULL, 0, ?, ?)`, now, now)
	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr-run-retry-recovered', ?, 'delegated_execution_berserker', 1,1, 'running', 'builder-berserker', 'agent-run-retry-recovered', 1, ?, NULL)`, runID, now)
	mustExecSQL(t, db, `INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('evt-run-retry-recovered-1', ?, 1, 'nr-run-retry-recovered', 'attempt-run-retry-recovered', 'v1', 'attempt.retrying', 1, '{"node_id":"delegated_execution_berserker","node_run_id":"nr-run-retry-recovered","retry_index":2,"retry_max":3,"delay_seconds":10,"reason_code":"provider_empty_output_retry"}', 'runtime_orchestrator', 'builder-berserker', 'runtime.orchestrator', 'provider_empty_output_retry', 'retrying empty provider output after 10s', 'corr-run-retry-recovered', 'caus-run-retry-recovered-1', 'prev-run-retry-recovered-1', 'entry-run-retry-recovered-1', '2026-03-06T10:01:00Z')`, runID)
	mustExecSQL(t, db, `INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('evt-run-retry-recovered-2', ?, 2, 'nr-run-retry-recovered', 'attempt-run-retry-recovered', 'v1', 'attempt.retry_recovered', 2, '{"node_id":"delegated_execution_berserker","node_run_id":"nr-run-retry-recovered","retry_index":2,"retry_max":3,"reason_code":"provider_output_recovered"}', 'runtime_orchestrator', 'builder-berserker', 'runtime.orchestrator', 'provider_output_recovered', 'provider output recovered after empty-output retry', 'corr-run-retry-recovered', 'caus-run-retry-recovered-2', 'prev-run-retry-recovered-2', 'entry-run-retry-recovered-2', '2026-03-06T10:02:00Z')`, runID)

	res := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/runs/" + runID,
		actorID: "actor-1",
		roleID:  "chancellor",
	})
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (%s)", res.Code, res.Body.String())
	}

	var run gen.RunDetailSummary
	if err := json.Unmarshal(res.Body.Bytes(), &run); err != nil {
		t.Fatalf("decode run detail summary: %v", err)
	}
	if len(run.RuntimeRetries) != 0 {
		t.Fatalf("expected recovered retry to be omitted, got %#v", run.RuntimeRetries)
	}
}

func TestServerGetRunUsesCanonicalSeqForRuntimeRetrySelection(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunWithLinearExecutionWorkflow(t, handler)
	now := "2026-03-06T10:01:00Z"

	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent-run-retry-seq', 'builder-berserker', 'Builder', 'busy', NULL, 0, ?, ?)`, now, now)
	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr-run-retry-seq', ?, 'delegated_execution_berserker', 1,1, 'running', 'builder-berserker', 'agent-run-retry-seq', 1, ?, NULL)`, runID, now)
	mustExecSQL(t, db, `INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('evt-run-retry-seq-recovered', ?, 1, 'nr-run-retry-seq', 'attempt-run-retry-seq', 'v1', 'attempt.retry_recovered', 1, '{"node_id":"delegated_execution_berserker","node_run_id":"nr-run-retry-seq","retry_index":1,"retry_max":3,"reason_code":"provider_output_recovered"}', 'runtime_orchestrator', 'builder-berserker', 'runtime.orchestrator', 'provider_output_recovered', 'provider output recovered', 'corr-run-retry-seq', 'caus-run-retry-seq-1', 'prev-run-retry-seq-1', 'entry-run-retry-seq-1', '2026-03-06T10:03:00Z')`, runID)
	mustExecSQL(t, db, `INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('evt-run-retry-seq-active', ?, 2, 'nr-run-retry-seq', 'attempt-run-retry-seq', 'v1', 'attempt.retrying', 2, '{"node_id":"delegated_execution_berserker","node_run_id":"nr-run-retry-seq","retry_index":2,"retry_max":3,"delay_seconds":10,"reason_code":"provider_empty_output_retry"}', 'runtime_orchestrator', 'builder-berserker', 'runtime.orchestrator', 'provider_empty_output_retry', 'retrying empty provider output after 10s', 'corr-run-retry-seq', 'caus-run-retry-seq-2', 'prev-run-retry-seq-2', 'entry-run-retry-seq-2', '2026-03-06T10:02:00Z')`, runID)

	res := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/runs/" + runID,
		actorID: "actor-1",
		roleID:  "chancellor",
	})
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (%s)", res.Code, res.Body.String())
	}

	var run gen.RunDetailSummary
	if err := json.Unmarshal(res.Body.Bytes(), &run); err != nil {
		t.Fatalf("decode run detail summary: %v", err)
	}
	if len(run.RuntimeRetries) != 1 {
		t.Fatal("expected canonical latest seq to keep runtime retry active")
	}
	if run.RuntimeRetries[0].RetryIndex != 2 {
		t.Fatalf("expected retry index from highest seq event, got %#v", run.RuntimeRetries)
	}
}

func TestServerGetRunIncludesConcurrentRuntimeRetries(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunWithLinearExecutionWorkflow(t, handler)
	now := "2026-03-06T10:01:00Z"

	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent-run-retry-builder', 'builder-berserker', 'Builder', 'busy', NULL, 0, ?, ?)`, now, now)
	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent-run-retry-validator', 'intelligence-qa-scout', 'Validator', 'busy', NULL, 0, ?, ?)`, now, now)
	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr-run-retry-builder', ?, 'delegated_execution_berserker', 1,1, 'running', 'builder-berserker', 'agent-run-retry-builder', 1, ?, NULL)`, runID, now)
	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr-run-retry-validator', ?, 'validation_gates_qa_scout', 1,1, 'running', 'intelligence-qa-scout', 'agent-run-retry-validator', 1, ?, NULL)`, runID, now)
	mustExecSQL(t, db, `INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('evt-run-retry-builder', ?, 1, 'nr-run-retry-builder', 'attempt-run-retry-builder', 'v1', 'attempt.retrying', 1, '{"node_id":"delegated_execution_berserker","node_run_id":"nr-run-retry-builder","retry_index":1,"retry_max":3,"delay_seconds":5,"reason_code":"provider_empty_output_retry"}', 'runtime_orchestrator', 'builder-berserker', 'runtime.orchestrator', 'provider_empty_output_retry', 'retrying empty provider output after 5s', 'corr-run-retry', 'caus-run-retry-builder', 'prev-run-retry-builder', 'entry-run-retry-builder', ?)`, runID, now)
	mustExecSQL(t, db, `INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('evt-run-retry-validator', ?, 2, 'nr-run-retry-validator', 'attempt-run-retry-validator', 'v1', 'attempt.retrying', 1, '{"node_id":"validation_gates_qa_scout","node_run_id":"nr-run-retry-validator","retry_index":2,"retry_max":4,"delay_seconds":8,"reason_code":"provider_empty_output_retry"}', 'runtime_orchestrator', 'intelligence-qa-scout', 'runtime.orchestrator', 'provider_empty_output_retry', 'retrying empty provider output after 8s', 'corr-run-retry', 'caus-run-retry-validator', 'prev-run-retry-validator', 'entry-run-retry-validator', ?)`, runID, now)

	res := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/runs/" + runID,
		actorID: "actor-1",
		roleID:  "chancellor",
	})
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (%s)", res.Code, res.Body.String())
	}

	var run gen.RunDetailSummary
	if err := json.Unmarshal(res.Body.Bytes(), &run); err != nil {
		t.Fatalf("decode run detail summary: %v", err)
	}
	if len(run.RuntimeRetries) != 2 {
		t.Fatalf("expected two active runtime retries, got %#v", run.RuntimeRetries)
	}
	if run.RuntimeRetries[0].NodeRunId != "nr-run-retry-validator" || run.RuntimeRetries[1].NodeRunId != "nr-run-retry-builder" {
		t.Fatalf("expected retries ordered by latest run_event_seq DESC, got %#v", run.RuntimeRetries)
	}
}

func TestServerGetRunIgnoresLaterMailboxEventsWhenRetryIsStillActive(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunWithLinearExecutionWorkflow(t, handler)
	now := "2026-03-06T10:01:00Z"

	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent-run-retry-mailbox', 'builder-berserker', 'Builder', 'busy', NULL, 0, ?, ?)`, now, now)
	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr-run-retry-mailbox', ?, 'delegated_execution_berserker', 1,1, 'running', 'builder-berserker', 'agent-run-retry-mailbox', 1, ?, NULL)`, runID, now)
	mustExecSQL(t, db, `INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('evt-run-retry-mailbox-active', ?, 1, 'nr-run-retry-mailbox', 'attempt-run-retry-mailbox', 'v1', 'attempt.retrying', 1, '{"node_id":"delegated_execution_berserker","node_run_id":"nr-run-retry-mailbox","retry_index":2,"retry_max":3,"delay_seconds":10,"reason_code":"provider_empty_output_retry"}', 'runtime_orchestrator', 'builder-berserker', 'runtime.orchestrator', 'provider_empty_output_retry', 'retrying empty provider output after 10s', 'corr-run-retry-mailbox', 'caus-run-retry-mailbox-1', 'prev-run-retry-mailbox-1', 'entry-run-retry-mailbox-1', '2026-03-06T10:01:00Z')`, runID)
	mustExecSQL(t, db, `INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('evt-run-retry-mailbox-message', ?, 2, 'nr-run-retry-mailbox', 'run:mailbox', 'v1', 'mailbox.message_recorded', 1, '{"message_id":"msg-mailbox","node_id":"delegated_execution_berserker","node_run_id":"nr-run-retry-mailbox","message_source":"communication","communication_type":"response_received","visible_to_operator":true}', 'agent-run-retry-mailbox', 'builder-berserker', 'state.mailbox', 'execution_completed', 'mailbox message recorded', 'corr-run-retry-mailbox-2', 'caus-run-retry-mailbox-2', 'prev-run-retry-mailbox-2', 'entry-run-retry-mailbox-2', '2026-03-06T10:02:00Z')`, runID)

	res := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/runs/" + runID,
		actorID: "actor-1",
		roleID:  "chancellor",
	})
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (%s)", res.Code, res.Body.String())
	}

	var run gen.RunDetailSummary
	if err := json.Unmarshal(res.Body.Bytes(), &run); err != nil {
		t.Fatalf("decode run detail summary: %v", err)
	}
	if len(run.RuntimeRetries) != 1 {
		t.Fatal("expected active runtime retry to survive later mailbox event")
	}
	if run.RuntimeRetries[0].NodeRunId != "nr-run-retry-mailbox" || run.RuntimeRetries[0].RetryIndex != 2 {
		t.Fatalf("expected active retry payload from attempt.retrying event, got %#v", run.RuntimeRetries)
	}
}

func TestServerGetRunIgnoresMalformedRuntimeRetryPayload(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunWithLinearExecutionWorkflow(t, handler)
	now := "2026-03-06T10:01:00Z"

	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent-run-retry-malformed', 'builder-berserker', 'Builder', 'busy', NULL, 0, ?, ?)`, now, now)
	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr-run-retry-malformed', ?, 'delegated_execution_berserker', 1,1, 'running', 'builder-berserker', 'agent-run-retry-malformed', 1, ?, NULL)`, runID, now)
	mustExecSQL(t, db, `INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('evt-run-retry-malformed', ?, 1, 'nr-run-retry-malformed', 'attempt-run-retry-malformed', 'v1', 'attempt.retrying', 1, '{"node_id":"delegated_execution_berserker","node_run_id":"nr-run-retry-malformed","retry_index":2,"retry_max":3}', 'runtime_orchestrator', 'builder-berserker', 'runtime.orchestrator', 'provider_empty_output_retry', 'retrying empty provider output', 'corr-run-retry-malformed', 'caus-run-retry-malformed', 'prev-run-retry-malformed', 'entry-run-retry-malformed', ?)`, runID, now)

	res := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/runs/" + runID,
		actorID: "actor-1",
		roleID:  "chancellor",
	})
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (%s)", res.Code, res.Body.String())
	}

	var run gen.RunDetailSummary
	if err := json.Unmarshal(res.Body.Bytes(), &run); err != nil {
		t.Fatalf("decode run detail summary: %v", err)
	}
	if len(run.RuntimeRetries) != 0 {
		t.Fatalf("expected malformed runtime retry payload to be ignored, got %#v", run.RuntimeRetries)
	}
}

func TestServerGetRunGraphIncludesFailedNodeReason(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunWithLinearExecutionWorkflow(t, handler)
	now := "2026-03-06T10:01:00Z"

	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent-graph-failed', 'builder-berserker', 'Builder', 'failed', NULL, 0, ?, ?)`, now, now)
	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr-graph-failed', ?, 'delegated_execution_berserker', 1,1, 'failed', 'builder-berserker', 'agent-graph-failed', 1, ?, ?)`, runID, now, now)
	mustExecSQL(t, db, `INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('evt-graph-failed', ?, 1, 'nr-graph-failed', 'attempt-graph-failed', 'v1', 'attempt.failed', 1, '{"error":"Builder crashed while preparing workspace."}', 'runtime_orchestrator', 'builder-berserker', 'runtime.orchestrator', 'execution_setup_failed', 'builder failed', 'corr-graph-failed', 'caus-graph-failed', 'prev-graph-failed', 'entry-graph-failed', ?)`, runID, now)

	res := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/runs/" + runID + "/graph",
		actorID: "actor-1",
		roleID:  "chancellor",
	})
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (%s)", res.Code, res.Body.String())
	}

	var payload gen.RunGraphResponse
	if err := json.Unmarshal(res.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode run graph response: %v", err)
	}

	for _, node := range payload.Nodes {
		if node.NodeId != "delegated_execution_berserker" {
			continue
		}
		if node.FailureReason == nil || *node.FailureReason != "Builder crashed while preparing workspace." {
			t.Fatalf("expected failed node reason, got %#v", node.FailureReason)
		}
		return
	}
	t.Fatal("expected delegated_execution_berserker node in graph response")
}

func TestServerGetRunGraphUsesCanonicalSeqForFailureReason(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunWithLinearExecutionWorkflow(t, handler)
	now := "2026-03-06T10:01:00Z"

	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent-graph-failed-seq', 'builder-berserker', 'Builder', 'failed', NULL, 0, ?, ?)`, now, now)
	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr-graph-failed-seq', ?, 'delegated_execution_berserker', 1,1, 'failed', 'builder-berserker', 'agent-graph-failed-seq', 1, ?, ?)`, runID, now, now)
	mustExecSQL(t, db, `INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('evt-graph-failed-seq-old', ?, 1, 'nr-graph-failed-seq', 'attempt-graph-failed-seq', 'v1', 'attempt.failed', 1, '{"error":"Stale timestamp failure."}', 'runtime_orchestrator', 'builder-berserker', 'runtime.orchestrator', 'execution_setup_failed', 'builder failed', 'corr-graph-failed-seq', 'caus-graph-failed-seq-1', 'prev-graph-failed-seq-1', 'entry-graph-failed-seq-1', '2026-03-06T10:03:00Z')`, runID)
	mustExecSQL(t, db, `INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('evt-graph-failed-seq-new', ?, 2, 'nr-graph-failed-seq', 'attempt-graph-failed-seq', 'v1', 'attempt.failed', 2, '{"error":"Canonical seq failure."}', 'runtime_orchestrator', 'builder-berserker', 'runtime.orchestrator', 'execution_setup_failed', 'builder failed', 'corr-graph-failed-seq', 'caus-graph-failed-seq-2', 'prev-graph-failed-seq-2', 'entry-graph-failed-seq-2', '2026-03-06T10:02:00Z')`, runID)

	res := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/runs/" + runID + "/graph",
		actorID: "actor-1",
		roleID:  "chancellor",
	})
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (%s)", res.Code, res.Body.String())
	}

	var payload gen.RunGraphResponse
	if err := json.Unmarshal(res.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode run graph response: %v", err)
	}

	for _, node := range payload.Nodes {
		if node.NodeId != "delegated_execution_berserker" {
			continue
		}
		if node.FailureReason == nil || *node.FailureReason != "Canonical seq failure." {
			t.Fatalf("expected failure reason from highest seq event, got %#v", node.FailureReason)
		}
		return
	}
	t.Fatal("expected delegated_execution_berserker node in graph response")
}

func TestServerGetRunGraphFallsBackToDurableFailureText(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunWithLinearExecutionWorkflow(t, handler)
	now := "2026-03-06T10:01:00Z"

	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent-graph-failed-durable', 'builder-berserker', 'Builder', 'failed', NULL, 0, ?, ?)`, now, now)
	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr-graph-failed-durable', ?, 'delegated_execution_berserker', 1,1, 'failed', 'builder-berserker', 'agent-graph-failed-durable', 1, ?, ?)`, runID, now, now)
	mustExecSQL(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, lease_owner, lease_expires_at, lease_version, session_ref, provider_session_id, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('attempt-graph-failed-durable', ?, 'nr-graph-failed-durable', 'committed_failure', 'provider_backed', '', NULL, 1, 'session-graph-failed-durable', 'ses-graph-failed-durable', ?, ?, ?, 'agent_unresponsive', 'Agent became unresponsive before completion could be confirmed.', ?, ?)`, runID, now, now, now, now, now)
	mustExecSQL(t, db, `INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('evt-graph-failed-durable', ?, 1, 'nr-graph-failed-durable', 'attempt-graph-failed-durable', 'v1', 'attempt.failed', 1, '{"reason":"stale_attempt"}', 'runtime_orchestrator', 'builder-berserker', 'runtime.orchestrator', 'agent_unresponsive', 'stale running attempt recovered as failed', 'corr-graph-failed-durable', 'caus-graph-failed-durable', 'prev-graph-failed-durable', 'entry-graph-failed-durable', ?)`, runID, now)

	res := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/runs/" + runID + "/graph",
		actorID: "actor-1",
		roleID:  "chancellor",
	})
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (%s)", res.Code, res.Body.String())
	}

	var payload gen.RunGraphResponse
	if err := json.Unmarshal(res.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode run graph response: %v", err)
	}

	for _, node := range payload.Nodes {
		if node.NodeId != "delegated_execution_berserker" {
			continue
		}
		if node.FailureReason == nil || *node.FailureReason != "Agent became unresponsive before completion could be confirmed." {
			t.Fatalf("expected graph failure reason to fall back to durable failure text, got %#v", node.FailureReason)
		}
		return
	}
	t.Fatal("expected delegated_execution_berserker node in graph response")
}

func TestServerGetRunGraphUsesFriendlyProviderIncompleteTurnReason(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunWithLinearExecutionWorkflow(t, handler)
	now := "2026-03-06T10:01:00Z"

	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent-graph-incomplete-turn', 'intelligence-nightraven', 'Validator', 'failed', NULL, 0, ?, ?)`, now, now)
	mustExecSQL(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) SELECT 'wn-graph-incomplete-turn', workflow_definition_id, 'validation_gates_nightraven', 'intelligence-nightraven', 'validation_gates', ? FROM workflow_runs WHERE run_id = ? AND NOT EXISTS (SELECT 1 FROM workflow_nodes WHERE workflow_definition_id = workflow_runs.workflow_definition_id AND node_id = 'validation_gates_nightraven')`, now, runID)
	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr-graph-incomplete-turn', ?, 'validation_gates_nightraven', 1,1, 'failed', 'intelligence-nightraven', 'agent-graph-incomplete-turn', 1, ?, ?)`, runID, now, now)
	mustExecSQL(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, lease_owner, lease_expires_at, lease_version, session_ref, provider_session_id, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('attempt-graph-incomplete-turn', ?, 'nr-graph-incomplete-turn', 'committed_failure', 'provider_backed', '', NULL, 1, 'session-graph-incomplete-turn', 'ses-graph-incomplete-turn', ?, ?, ?, 'provider_incomplete_turn', 'decode opencode export: unexpected end of JSON input', ?, ?)`, runID, now, now, now, now, now)
	mustExecSQL(t, db, `INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('evt-graph-incomplete-turn', ?, 1, 'nr-graph-incomplete-turn', 'attempt-graph-incomplete-turn', 'v1', 'attempt.failed', 1, '{"error":"incomplete terminal state (reason=\"provider_incomplete_turn\")"}', 'runtime_orchestrator', 'intelligence-nightraven', 'runtime.orchestrator', 'provider_incomplete_turn', 'node execution completed', 'corr-graph-incomplete-turn', 'caus-graph-incomplete-turn', 'prev-graph-incomplete-turn', 'entry-graph-incomplete-turn', ?)`, runID, now)

	res := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/runs/" + runID + "/graph",
		actorID: "actor-1",
		roleID:  "chancellor",
	})
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (%s)", res.Code, res.Body.String())
	}

	var payload gen.RunGraphResponse
	if err := json.Unmarshal(res.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode run graph response: %v", err)
	}

	for _, node := range payload.Nodes {
		if node.NodeId != "validation_gates_nightraven" {
			continue
		}
		if node.FailureReason == nil || *node.FailureReason != "Provider completion could not be confirmed before the recovery window expired." {
			t.Fatalf("expected friendly provider incomplete turn reason, got %#v", node.FailureReason)
		}
		return
	}
	t.Fatal("expected validation_gates_nightraven node in graph response")
}

func TestServerGetRunGraphTreatsPendingSettlementAsFailed(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunWithLinearExecutionWorkflow(t, handler)
	now := "2026-03-06T10:01:00Z"

	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent-graph-recovery-pending', 'builder-berserker', 'Builder', 'busy', NULL, 0, ?, ?)`, now, now)
	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr-graph-recovery-pending', ?, 'delegated_execution_berserker', 1,1, 'failed', 'builder-berserker', 'agent-graph-recovery-pending', 1, ?, ?)`, runID, now, now)
	mustExecSQL(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, lease_owner, lease_expires_at, lease_version, session_ref, provider_session_id, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('attempt-graph-recovery-pending', ?, 'nr-graph-recovery-pending', 'terminal_observed', 'provider_backed', '', NULL, 1, 'session-graph-recovery-pending', 'ses-graph-recovery-pending', ?, ?, NULL, 'provider_incomplete_turn', 'provider exited without an accepted kingdom_complete_attempt receipt', ?, ?)`, runID, now, now, now, now)

	res := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/runs/" + runID + "/graph",
		actorID: "actor-1",
		roleID:  "chancellor",
	})
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (%s)", res.Code, res.Body.String())
	}

	var payload gen.RunGraphResponse
	if err := json.Unmarshal(res.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode run graph response: %v", err)
	}

	for _, node := range payload.Nodes {
		if node.NodeId != "delegated_execution_berserker" {
			continue
		}
		if node.State != gen.RunGraphNodeStateFailed {
			t.Fatalf("expected pending settlement node to stay failed, got %q", node.State)
		}
		if node.FailureReason == nil || *node.FailureReason != "Provider completion could not be confirmed before the recovery window expired." {
			t.Fatalf("expected provider incomplete turn copy, got %#v", node.FailureReason)
		}
		return
	}
	t.Fatal("expected delegated_execution_berserker node in graph response")
}

func TestServerGetRunGraphReturnsLatestNodeLineage(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunWithLinearExecutionWorkflow(t, handler)
	now := "2026-03-06T10:01:00Z"
	later := "2026-03-06T10:05:00Z"

	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent-graph-lineage', 'builder-berserker', 'Builder', 'busy', NULL, 0, ?, ?)`, now, later)
	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr-graph-lineage-1', ?, 'delegated_execution_berserker', 1,1, 'failed', 'builder-berserker', 'agent-graph-lineage', 1, ?, ?)`, runID, now, now)
	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr-graph-lineage-2', ?, 'delegated_execution_berserker', 2,2, 'running', 'builder-berserker', 'agent-graph-lineage', 1, ?, NULL)`, runID, later)
	mustExecSQL(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, lease_owner, lease_expires_at, lease_version, session_ref, provider_session_id, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('attempt-graph-lineage-1', ?, 'nr-graph-lineage-1', 'committed_failure', 'provider_backed', '', NULL, 1, 'session-graph-lineage-1', 'ses-graph-lineage-1', ?, ?, ?, 'provider_incomplete_turn', 'older failed attempt', ?, ?)`, runID, now, now, now, later, later)
	mustExecSQL(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, lease_owner, lease_expires_at, lease_version, session_ref, provider_session_id, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('attempt-graph-lineage-2', ?, 'nr-graph-lineage-2', 'running', 'provider_backed', '', NULL, 1, 'session-graph-lineage-2', 'ses-graph-lineage-2', ?, NULL, NULL, '', '', ?, ?)`, runID, later, later, later)

	res := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/runs/" + runID + "/graph",
		actorID: "actor-1",
		roleID:  "chancellor",
	})
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (%s)", res.Code, res.Body.String())
	}

	var payload struct {
		Nodes []struct {
			NodeID    string  `json:"node_id"`
			State     string  `json:"state"`
			NodeRunID *string `json:"node_run_id"`
			Attempt   *int    `json:"attempt"`
		} `json:"nodes"`
	}
	if err := json.Unmarshal(res.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode run graph response: %v", err)
	}

	for _, node := range payload.Nodes {
		if node.NodeID != "delegated_execution_berserker" {
			continue
		}
		if node.State != "running" {
			t.Fatalf("expected latest node lineage to remain running, got %#v", node)
		}
		if node.NodeRunID == nil || *node.NodeRunID != "nr-graph-lineage-2" {
			t.Fatalf("expected current node_run_id, got %#v", node)
		}
		if node.Attempt == nil || *node.Attempt != 2 {
			t.Fatalf("expected current attempt=2, got %#v", node)
		}
		return
	}
	t.Fatal("expected delegated_execution_berserker node in graph response")
}

func TestServerGetRunGraphKeepsBlockedLineageVisibleUntilRetryLaunches(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunWithLinearExecutionWorkflow(t, handler)
	now := "2026-03-06T10:01:00Z"
	later := "2026-03-06T10:05:00Z"

	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent-graph-blocked-visible', 'builder-berserker', 'Builder', 'busy', NULL, 0, ?, ?)`, now, later)
	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr-graph-blocked-visible-1', ?, 'delegated_execution_berserker', 1,1, 'blocked', 'builder-berserker', 'agent-graph-blocked-visible', 1, ?, NULL)`, runID, now)
	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr-graph-blocked-visible-2', ?, 'delegated_execution_berserker', 2,2, 'ready', 'builder-berserker', 'agent-graph-blocked-visible', 1, NULL, NULL)`, runID)
	mustExecSQL(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, lease_owner, lease_expires_at, lease_version, session_ref, provider_session_id, launch_started_at, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('attempt-graph-blocked-visible-1', ?, 'nr-graph-blocked-visible-1', 'starting', 'provider_backed', '', NULL, 1, 'session-graph-blocked-visible-1', 'ses-graph-blocked-visible-1', ?, NULL, NULL, NULL, 'agent_unresponsive', 'blocked while retry is queued', ?, ?)`, runID, now, later, later)
	mustExecSQL(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, lease_owner, lease_expires_at, lease_version, session_ref, provider_session_id, launch_started_at, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('attempt-graph-blocked-visible-2', ?, 'nr-graph-blocked-visible-2', 'queued', 'provider_backed', '', NULL, 1, 'session-graph-blocked-visible-2', '', NULL, NULL, NULL, NULL, '', '', ?, ?)`, runID, later, later)

	res := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/runs/" + runID + "/graph",
		actorID: "actor-1",
		roleID:  "chancellor",
	})
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (%s)", res.Code, res.Body.String())
	}

	var payload struct {
		Nodes []struct {
			NodeID    string  `json:"node_id"`
			State     string  `json:"state"`
			NodeRunID *string `json:"node_run_id"`
			Attempt   *int    `json:"attempt"`
		} `json:"nodes"`
	}
	if err := json.Unmarshal(res.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode run graph response: %v", err)
	}

	for _, node := range payload.Nodes {
		if node.NodeID != "delegated_execution_berserker" {
			continue
		}
		if node.State != "blocked" {
			t.Fatalf("expected blocked lineage to remain visible while retry is only queued, got %#v", node)
		}
		if node.NodeRunID == nil || *node.NodeRunID != "nr-graph-blocked-visible-1" {
			t.Fatalf("expected blocked node_run_id to stay primary until retry launches, got %#v", node)
		}
		if node.Attempt == nil || *node.Attempt != 1 {
			t.Fatalf("expected blocked attempt=1 to remain primary, got %#v", node)
		}
		return
	}
	t.Fatal("expected delegated_execution_berserker node in graph response")
}

func TestServerGetRunGraphKeepsSupersededBlockedLineageVisibleUntilCancelDrain(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunWithLinearExecutionWorkflow(t, handler)
	now := "2026-03-06T10:01:00Z"
	later := "2026-03-06T10:05:00Z"

	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent-graph-superseded-visible', 'builder-berserker', 'Builder', 'busy', NULL, 0, ?, ?)`, now, later)
	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr-graph-superseded-visible-1', ?, 'delegated_execution_berserker', 1,1, 'succeeded', 'builder-berserker', 'agent-graph-superseded-visible', 1, ?, ?)`, runID, now, now)
	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at, superseded_at) VALUES ('nr-graph-superseded-visible-2', ?, 'delegated_execution_berserker', 2,2, 'blocked', 'builder-berserker', 'agent-graph-superseded-visible', 1, ?, ?, ?)`, runID, later, later, later)
	mustExecSQL(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, lease_owner, lease_expires_at, lease_version, session_ref, provider_session_id, launch_started_at, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('attempt-graph-superseded-visible-1', ?, 'nr-graph-superseded-visible-1', 'committed_success', 'provider_backed', '', NULL, 1, 'session-graph-superseded-visible-1', 'ses-graph-superseded-visible-1', ?, ?, ?, ?, '', '', ?, ?)`, runID, now, now, now, now, now, now)
	mustExecSQL(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, lease_owner, lease_expires_at, lease_version, session_ref, provider_session_id, launch_started_at, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('attempt-graph-superseded-visible-2', ?, 'nr-graph-superseded-visible-2', 'committed_cancelled', 'provider_backed', '', NULL, 1, 'session-graph-superseded-visible-2', 'ses-graph-superseded-visible-2', ?, NULL, ?, ?, 'superseded_after_launch_handoff', 'retry attempt superseded after launch handoff by an earlier successful attempt', ?, ?)`, runID, later, later, later, later, later, later)

	res := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/runs/" + runID + "/graph",
		actorID: "actor-1",
		roleID:  "chancellor",
	})
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (%s)", res.Code, res.Body.String())
	}

	var payload struct {
		Nodes []struct {
			NodeID    string  `json:"node_id"`
			State     string  `json:"state"`
			NodeRunID *string `json:"node_run_id"`
			Attempt   *int    `json:"attempt"`
		} `json:"nodes"`
	}
	if err := json.Unmarshal(res.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode run graph response: %v", err)
	}

	for _, node := range payload.Nodes {
		if node.NodeID != "delegated_execution_berserker" {
			continue
		}
		if node.State != "blocked" {
			t.Fatalf("expected superseded blocked lineage to remain visible until cancel drain, got %#v", node)
		}
		if node.NodeRunID == nil || *node.NodeRunID != "nr-graph-superseded-visible-2" {
			t.Fatalf("expected superseded blocked node_run_id to remain visible, got %#v", node)
		}
		if node.Attempt == nil || *node.Attempt != 2 {
			t.Fatalf("expected superseded blocked attempt=2 to remain visible, got %#v", node)
		}
		return
	}
	t.Fatal("expected delegated_execution_berserker node in graph response")
}

func TestServerRunPlanSnapshotReturnsLatestPlanDraftingResponse(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunWithLinearExecutionWorkflow(t, handler)

	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_plan_chancellor', 'chancellor', 'Chancellor', 'idle', NULL, 0, '2026-03-02T09:00:00Z', '2026-03-02T09:00:00Z')`)
	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_plan_council', 'council-member', 'Council Member', 'idle', NULL, 0, '2026-03-02T09:00:00Z', '2026-03-02T09:00:00Z')`)
	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_plan_snapshot', ?, 'plan_drafting', 1,1, 'succeeded', 'council-member', 'agent_plan_council', 1, '2026-03-02T09:00:00Z', '2026-03-02T09:00:00Z')`, runID)

	mustExecSQL(t, db, `INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at)
	VALUES ('msg_plan_old', ?, 'agent_plan_council', 'agent_plan_chancellor', 'thread_plan', 'status_update', 'normal', '{"communication_type":"response_received","message_source":"communication","node_id":"plan_drafting","response_full":"Old plan"}', '2026-03-02T09:01:00Z')`, runID)
	mustExecSQL(t, db, `INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at)
	VALUES ('msg_plan_new', ?, 'agent_plan_council', 'agent_plan_chancellor', 'thread_plan', 'status_update', 'normal', '{"communication_type":"response_received","message_source":"communication","node_id":"plan_drafting","response_full":"Latest plan\n- Step one\n- Step two"}', '2026-03-02T09:02:00Z')`, runID)
	seedAttemptExecution(t, db, runID+":plan_drafting:old", runID, "nr_plan_snapshot", "2026-03-02T09:01:00Z")
	seedAttemptExecution(t, db, runID+":plan_drafting:new", runID, "nr_plan_snapshot", "2026-03-02T09:02:00Z")
	mustExecSQL(t, db, `INSERT INTO run_planning_outputs (planning_output_id, run_id, node_id, node_run_id, attempt_id, agent_id, source_message_id, kind, content_markdown, created_at) VALUES ('planrev_old', ?, 'plan_drafting', 'nr_plan_snapshot', ?, 'agent_plan_council', 'msg_plan_old', 'plan_revision', 'Old plan', '2026-03-02T09:01:00Z')`, runID, runID+":plan_drafting:old")
	mustExecSQL(t, db, `INSERT INTO run_planning_outputs (planning_output_id, run_id, node_id, node_run_id, attempt_id, agent_id, source_message_id, kind, content_markdown, created_at) VALUES ('planrev_new', ?, 'plan_drafting', 'nr_plan_snapshot', ?, 'agent_plan_council', 'msg_plan_new', 'plan_revision', 'Latest plan
- Step one
- Step two', '2026-03-02T09:02:00Z')`, runID, runID+":plan_drafting:new")
	revision := mustRefreshRunPlanRevision(t, db, runID)

	res := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/runs/" + runID + "/plan",
		actorID: "actor-allfather",
		roleID:  "allfather",
	})

	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (%s)", res.Code, res.Body.String())
	}

	var payload struct {
		RunID          string  `json:"run_id"`
		PlanState      string  `json:"plan_state"`
		RevisionStatus string  `json:"revision_status"`
		PlanRevisionId *string `json:"plan_revision_id"`
		Components     []struct {
			NodeID string `json:"node_id"`
		} `json:"components"`
		Content string `json:"content"`
	}
	if err := json.Unmarshal(res.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode plan snapshot payload: %v", err)
	}
	if payload.RunID != runID {
		t.Fatalf("expected run id %q, got %q", runID, payload.RunID)
	}
	if len(payload.Components) != 1 || payload.Components[0].NodeID != "plan_drafting" {
		t.Fatalf("expected single plan_drafting component, got %#v", payload.Components)
	}
	if payload.RevisionStatus != "ready" {
		t.Fatalf("expected ready revision_status, got %q", payload.RevisionStatus)
	}
	if payload.PlanRevisionId == nil || *payload.PlanRevisionId != revision.PlanRevisionID {
		t.Fatalf("expected newest plan revision, got %#v", payload.PlanRevisionId)
	}
	if !strings.Contains(payload.Content, "Step two") {
		t.Fatalf("expected latest plan content, got %q", payload.Content)
	}
}

func TestServerRunPlanSnapshotUsesCanonicalRevisionIndependentOfMailboxVisibility(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunWithLinearExecutionWorkflow(t, handler)

	createdAt := "2026-03-02T09:02:00Z"
	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_plan_visible_king', 'king', 'King', 'idle', NULL, 0, ?, ?)`, createdAt, createdAt)
	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_plan_visible_council', 'council-member', 'Council Member', 'idle', NULL, 0, ?, ?)`, createdAt, createdAt)
	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_plan_hidden_council', 'council-member', 'Council Member', 'idle', NULL, 0, ?, ?)`, createdAt, createdAt)
	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_plan_visible', ?, 'plan_drafting', 1,1, 'succeeded', 'council-member', 'agent_plan_visible_council', 1, ?, ?)`, runID, createdAt, createdAt)

	mustExecSQL(t, db, `INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at)
	VALUES ('msg_plan_visible', ?, 'agent_plan_visible_council', 'agent_plan_visible_king', 'thread_plan_visible', 'status_update', 'normal', '{"communication_type":"response_received","message_source":"communication","node_id":"plan_drafting","response_full":"Visible plan revision"}', '2026-03-02T09:01:00Z')`, runID)
	mustExecSQL(t, db, `INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at)
	VALUES ('msg_plan_hidden_self', ?, 'agent_plan_hidden_council', 'agent_plan_hidden_council', 'thread_plan_hidden', 'status_update', 'normal', '{"communication_type":"response_received","message_source":"communication","node_id":"plan_drafting","response_full":"Hidden internal note"}', ?)`, runID, createdAt)
	seedAttemptExecution(t, db, runID+":plan_drafting:visible", runID, "nr_plan_visible", "2026-03-02T09:01:00Z")
	mustExecSQL(t, db, `INSERT INTO run_planning_outputs (planning_output_id, run_id, node_id, node_run_id, attempt_id, agent_id, source_message_id, kind, content_markdown, created_at) VALUES ('planrev_visible', ?, 'plan_drafting', 'nr_plan_visible', ?, 'agent_plan_visible_council', 'msg_plan_visible', 'plan_revision', 'Visible plan revision', '2026-03-02T09:01:00Z')`, runID, runID+":plan_drafting:visible")
	revision := mustRefreshRunPlanRevision(t, db, runID)

	res := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/runs/" + runID + "/plan",
		actorID: "actor-allfather",
		roleID:  "allfather",
	})

	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (%s)", res.Code, res.Body.String())
	}

	var payload struct {
		PlanRevisionId *string `json:"plan_revision_id"`
		Content        string  `json:"content"`
	}
	if err := json.Unmarshal(res.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode plan snapshot payload: %v", err)
	}
	if payload.PlanRevisionId == nil || *payload.PlanRevisionId != revision.PlanRevisionID {
		t.Fatalf("expected latest canonical plan revision, got %#v", payload.PlanRevisionId)
	}
	if payload.Content != "Visible plan revision" {
		t.Fatalf("expected visible plan content, got %q", payload.Content)
	}
}

func TestServerRunPlanSnapshotUsesLatestCanonicalRevision(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunWithLinearExecutionWorkflow(t, handler)

	createdAt := "2026-03-02T09:02:00Z"
	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_plan_suppressed_king', 'king', 'King', 'idle', NULL, 0, ?, ?)`, createdAt, createdAt)
	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_plan_suppressed_council', 'council-member', 'Council Member', 'idle', NULL, 0, ?, ?)`, createdAt, createdAt)
	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_plan_latest', ?, 'plan_drafting', 1,1, 'succeeded', 'council-member', 'agent_plan_suppressed_council', 1, ?, ?)`, runID, createdAt, createdAt)

	mustExecSQL(t, db, `INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at)
	VALUES ('msg_plan_visible', ?, 'agent_plan_suppressed_council', 'agent_plan_suppressed_king', 'thread_plan_visible', 'status_update', 'normal', '{"communication_type":"response_received","message_source":"communication","node_id":"plan_drafting","response_full":"Changed files plan: tighten auth handling and expand tests before implementation."}', '2026-03-02T09:01:00Z')`, runID)
	mustExecSQL(t, db, `INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at)
	VALUES ('msg_plan_latest', ?, 'agent_plan_suppressed_council', 'agent_plan_suppressed_king', 'thread_plan_latest', 'status_update', 'normal', '{"communication_type":"response_received","message_source":"communication","node_id":"plan_drafting","response_full":"Latest canonical plan drift text"}', ?)`, runID, createdAt)
	seedAttemptExecution(t, db, runID+":plan_drafting:visible", runID, "nr_plan_latest", "2026-03-02T09:01:00Z")
	seedAttemptExecution(t, db, runID+":plan_drafting:latest", runID, "nr_plan_latest", createdAt)
	mustExecSQL(t, db, `INSERT INTO run_planning_outputs (planning_output_id, run_id, node_id, node_run_id, attempt_id, agent_id, source_message_id, kind, content_markdown, created_at) VALUES ('planrev_visible', ?, 'plan_drafting', 'nr_plan_latest', ?, 'agent_plan_suppressed_council', 'msg_plan_visible', 'plan_revision', 'Changed files plan: tighten auth handling and expand tests before implementation.', '2026-03-02T09:01:00Z')`, runID, runID+":plan_drafting:visible")
	mustExecSQL(t, db, `INSERT INTO run_planning_outputs (planning_output_id, run_id, node_id, node_run_id, attempt_id, agent_id, source_message_id, kind, content_markdown, created_at) VALUES ('planrev_latest', ?, 'plan_drafting', 'nr_plan_latest', ?, 'agent_plan_suppressed_council', 'msg_plan_latest', 'plan_revision', 'Latest canonical plan drift text', ?)`, runID, runID+":plan_drafting:latest", createdAt)
	revision := mustRefreshRunPlanRevision(t, db, runID)

	res := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/runs/" + runID + "/plan",
		actorID: "actor-allfather",
		roleID:  "allfather",
	})

	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (%s)", res.Code, res.Body.String())
	}

	var payload struct {
		PlanRevisionId *string `json:"plan_revision_id"`
		Content        string  `json:"content"`
	}
	if err := json.Unmarshal(res.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode plan snapshot payload: %v", err)
	}
	if payload.PlanRevisionId == nil || *payload.PlanRevisionId != revision.PlanRevisionID {
		t.Fatalf("expected latest canonical plan revision, got %#v", payload.PlanRevisionId)
	}
	if payload.Content != "Latest canonical plan drift text" {
		t.Fatalf("expected latest canonical plan content, got %q", payload.Content)
	}
}

func TestServerRunPlanSnapshotUsesCanonicalCurrentPlanWhenProjectionRowIsMissing(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunWithLinearExecutionWorkflow(t, handler)
	createdAt := "2026-03-02T09:02:00Z"

	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_plan_missing_projection_king', 'king', 'King', 'idle', NULL, 0, ?, ?)`, createdAt, createdAt)
	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_plan_missing_projection_council', 'council-member', 'Council Member', 'idle', NULL, 0, ?, ?)`, createdAt, createdAt)
	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_plan_missing_projection', ?, 'plan_drafting', 1,1, 'succeeded', 'council-member', 'agent_plan_missing_projection_council', 1, ?, ?)`, runID, createdAt, createdAt)
	mustExecSQL(t, db, `INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at)
	VALUES ('msg_plan_missing_projection', ?, 'agent_plan_missing_projection_council', 'agent_plan_missing_projection_king', 'thread_plan_missing_projection', 'status_update', 'normal', '{"communication_type":"response_received","message_source":"communication","node_id":"plan_drafting","response_full":"Canonical plan without persisted projection"}', ?)`, runID, createdAt)
	seedAttemptExecution(t, db, runID+":plan_drafting:missing_projection", runID, "nr_plan_missing_projection", createdAt)
	mustExecSQL(t, db, `INSERT INTO run_planning_outputs (planning_output_id, run_id, node_id, node_run_id, attempt_id, agent_id, source_message_id, kind, content_markdown, created_at) VALUES ('planrev_missing_projection', ?, 'plan_drafting', 'nr_plan_missing_projection', ?, 'agent_plan_missing_projection_council', 'msg_plan_missing_projection', 'plan_revision', 'Canonical plan without persisted projection', ?)`, runID, runID+":plan_drafting:missing_projection", createdAt)

	currentRevision, ok, err := state.NewRunPlanRevisionStore(db).CurrentRunPlanRevision(context.Background(), runID)
	if err != nil {
		t.Fatalf("load canonical current run plan revision: %v", err)
	}
	if !ok {
		t.Fatal("expected canonical current run plan revision")
	}
	var projectedCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM run_plan_revisions WHERE run_id = ?`, runID).Scan(&projectedCount); err != nil {
		t.Fatalf("count projected run plan revisions: %v", err)
	}
	if projectedCount != 0 {
		t.Fatalf("expected no persisted run_plan_revisions rows before snapshot read, got %d", projectedCount)
	}

	res := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/runs/" + runID + "/plan",
		actorID: "actor-allfather",
		roleID:  "allfather",
	})
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (%s)", res.Code, res.Body.String())
	}

	var payload struct {
		PlanRevisionId *string `json:"plan_revision_id"`
		RevisionStatus string  `json:"revision_status"`
		Content        string  `json:"content"`
	}
	if err := json.Unmarshal(res.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode plan snapshot payload: %v", err)
	}
	if payload.RevisionStatus != "ready" {
		t.Fatalf("expected ready revision_status, got %q", payload.RevisionStatus)
	}
	if payload.PlanRevisionId == nil || *payload.PlanRevisionId != currentRevision.PlanRevisionID {
		t.Fatalf("expected canonical plan revision id %q, got %#v", currentRevision.PlanRevisionID, payload.PlanRevisionId)
	}
	if payload.Content != "Canonical plan without persisted projection" {
		t.Fatalf("expected canonical plan content, got %q", payload.Content)
	}
}

func TestServerRunPlanSnapshotReturnsPendingAfterFreshPlanningWaveQueued(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunWithPlanningWaveWorkflow(t, handler, withCreateRunBody(map[string]any{"autonomous_approvals": false}))
	mustExecSQL(t, db, `UPDATE workflow_runs SET run_status = 'running' WHERE run_id = ?`, runID)
	_ = seedReadyPlanReworkNodes(t, db, runID)
	mustExecSQL(t, db, `UPDATE workflow_runs SET plan_state = 'needs_rework' WHERE run_id = ?`, runID)
	now := "2026-03-02T09:10:00Z"
	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES (?, ?, 'advisory_synthesis', 2,2, 'pending', 'chancellor', ?, 1, NULL, NULL)`, "nr_plan_rework_advisory_2_"+runID, runID, "agent_chancellor_"+runID)
	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES (?, ?, 'plan_drafting', 2,2, 'pending', 'council-member', ?, 1, NULL, NULL)`, "nr_plan_rework_plan_2_"+runID, runID, "agent_council_"+runID)
	mustExecSQL(t, db, `UPDATE workflow_runs SET updated_at = ? WHERE run_id = ?`, now, runID)

	res := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/runs/" + runID + "/plan",
		actorID: "actor-allfather",
		roleID:  "allfather",
	})
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (%s)", res.Code, res.Body.String())
	}

	var payload struct {
		PlanState      string     `json:"plan_state"`
		RevisionStatus string     `json:"revision_status"`
		PlanRevisionId *string    `json:"plan_revision_id"`
		Components     []struct{} `json:"components"`
		Content        string     `json:"content"`
	}
	if err := json.Unmarshal(res.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode queued-wave plan snapshot payload: %v", err)
	}
	if payload.PlanState != "needs_rework" {
		t.Fatalf("expected needs_rework plan_state, got %q", payload.PlanState)
	}
	if payload.RevisionStatus != "pending" {
		t.Fatalf("expected pending revision_status after fresh planning wave queued, got %q", payload.RevisionStatus)
	}
	if payload.PlanRevisionId != nil {
		t.Fatalf("expected no current plan revision id after fresh planning wave queued, got %#v", payload.PlanRevisionId)
	}
	if len(payload.Components) != 0 {
		t.Fatalf("expected no current plan components after fresh planning wave queued, got %#v", payload.Components)
	}
	if payload.Content != "" {
		t.Fatalf("expected empty current plan content after fresh planning wave queued, got %q", payload.Content)
	}
}

func TestServerRunTabCountsSuppressStructuredPlanningMessages(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunWithLinearExecutionWorkflow(t, handler)
	createdAt := "2026-03-02T09:02:00Z"

	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_plan_counts_king', 'king', 'King', 'idle', NULL, 0, ?, ?)`, createdAt, createdAt)
	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_plan_counts_council', 'council-member', 'Council Member', 'idle', NULL, 0, ?, ?)`, createdAt, createdAt)

	mustExecSQL(t, db, `INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at)
	VALUES ('msg_plan_counts_visible', ?, 'agent_plan_counts_council', 'agent_plan_counts_king', 'thread_plan_counts_visible', 'status_update', 'normal', '{"message_source":"communication","communication_type":"response_received","node_id":"plan_drafting","response_full":"Visible approval revision"}', '2026-03-02T09:01:00Z')`, runID)
	mustExecSQL(t, db, `INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at)
	VALUES ('msg_plan_counts_suppressed', ?, 'agent_plan_counts_council', 'agent_plan_counts_king', 'thread_plan_counts_suppressed', 'status_update', 'normal', '{"message_source":"communication","communication_type":"response_received","node_id":"plan_drafting","response_full":"Latest planning response"}', ?)`, runID, createdAt)

	res := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/runs/" + runID,
		actorID: "actor-1",
		roleID:  "chancellor",
	})
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (%s)", res.Code, res.Body.String())
	}

	var payload gen.RunDetailSummary
	if err := json.Unmarshal(res.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode run detail response: %v", err)
	}
	if payload.TabCounts.Messages != 2 {
		t.Fatalf("expected tab_counts.messages=2 with planning responses visible, got %d", payload.TabCounts.Messages)
	}
}

func TestServerRunPlanSnapshotPrefersLatestPersistedRevisionWhenTimestampsTie(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunWithLinearExecutionWorkflow(t, handler)
	createdAt := "2026-03-02T09:02:00Z"

	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_plan_same_ts_chancellor', 'chancellor', 'Chancellor', 'idle', NULL, 0, ?, ?)`, createdAt, createdAt)
	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_plan_same_ts_council', 'council-member', 'Council Member', 'idle', NULL, 0, ?, ?)`, createdAt, createdAt)
	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_plan_same_ts', ?, 'plan_drafting', 1,1, 'succeeded', 'council-member', 'agent_plan_same_ts_council', 1, ?, ?)`, runID, createdAt, createdAt)

	mustExecSQL(t, db, `INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at)
	VALUES ('msg_plan_z_same_ts', ?, 'agent_plan_same_ts_council', 'agent_plan_same_ts_chancellor', 'thread_plan_same_ts', 'status_update', 'normal', '{"communication_type":"response_received","message_source":"communication","node_id":"plan_drafting","response_full":"Plan revision one"}', ?)`, runID, createdAt)
	mustExecSQL(t, db, `INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at)
	VALUES ('msg_plan_a_same_ts', ?, 'agent_plan_same_ts_council', 'agent_plan_same_ts_chancellor', 'thread_plan_same_ts', 'status_update', 'normal', '{"communication_type":"response_received","message_source":"communication","node_id":"plan_drafting","response_full":"Plan revision two"}', ?)`, runID, createdAt)
	seedAttemptExecution(t, db, runID+":plan_drafting:one", runID, "nr_plan_same_ts", createdAt)
	seedAttemptExecution(t, db, runID+":plan_drafting:two", runID, "nr_plan_same_ts", createdAt)
	mustExecSQL(t, db, `INSERT INTO run_planning_outputs (planning_output_id, run_id, node_id, node_run_id, attempt_id, agent_id, source_message_id, kind, content_markdown, created_at) VALUES ('planrev_z_same_ts', ?, 'plan_drafting', 'nr_plan_same_ts', ?, 'agent_plan_same_ts_council', 'msg_plan_z_same_ts', 'plan_revision', 'Plan revision one', ?)`, runID, runID+":plan_drafting:one", createdAt)
	mustExecSQL(t, db, `INSERT INTO run_planning_outputs (planning_output_id, run_id, node_id, node_run_id, attempt_id, agent_id, source_message_id, kind, content_markdown, created_at) VALUES ('planrev_a_same_ts', ?, 'plan_drafting', 'nr_plan_same_ts', ?, 'agent_plan_same_ts_council', 'msg_plan_a_same_ts', 'plan_revision', 'Plan revision two', ?)`, runID, runID+":plan_drafting:two", createdAt)
	revision := mustRefreshRunPlanRevision(t, db, runID)

	res := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/runs/" + runID + "/plan",
		actorID: "actor-allfather",
		roleID:  "allfather",
	})

	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (%s)", res.Code, res.Body.String())
	}

	var payload struct {
		PlanRevisionId *string `json:"plan_revision_id"`
		Content        string  `json:"content"`
	}
	if err := json.Unmarshal(res.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode plan snapshot payload: %v", err)
	}
	if payload.PlanRevisionId == nil || *payload.PlanRevisionId != revision.PlanRevisionID {
		t.Fatalf("expected latest appended plan revision, got %#v", payload.PlanRevisionId)
	}
	if payload.Content != "Plan revision two" {
		t.Fatalf("expected latest appended plan content, got %q", payload.Content)
	}
}

func TestServerRunPlanSnapshotSupportsCustomPlanDraftingNodeID(t *testing.T) {
	t.Parallel()

	const planNodeID = "plan_drafting_scope"
	handler, db := newAPIServerForTest(t)
	runID := seedRunWithCustomPlanDraftingWorkflow(t, handler, planNodeID)
	revisionID := seedCurrentPlanRevisionForNode(t, db, runID, planNodeID, "Custom scope plan\n- Rework acceptance criteria")

	res := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/runs/" + runID + "/plan",
		actorID: "actor-allfather",
		roleID:  "allfather",
	})
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (%s)", res.Code, res.Body.String())
	}

	var payload struct {
		RevisionStatus string  `json:"revision_status"`
		PlanRevisionId *string `json:"plan_revision_id"`
		Components     []struct {
			NodeID string `json:"node_id"`
		} `json:"components"`
		Content string `json:"content"`
	}
	if err := json.Unmarshal(res.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode custom plan snapshot payload: %v", err)
	}
	if len(payload.Components) != 1 || payload.Components[0].NodeID != planNodeID {
		t.Fatalf("expected single component node_id %q, got %#v", planNodeID, payload.Components)
	}
	if payload.RevisionStatus != "ready" {
		t.Fatalf("expected ready revision_status, got %q", payload.RevisionStatus)
	}
	if payload.PlanRevisionId == nil || *payload.PlanRevisionId != revisionID {
		t.Fatalf("expected plan_revision_id %q, got %#v", revisionID, payload.PlanRevisionId)
	}
	if !strings.Contains(payload.Content, "Rework acceptance criteria") {
		t.Fatalf("expected custom plan content, got %q", payload.Content)
	}
}

func TestServerRunPlanSnapshotAggregatesMultiplePlanDraftingNodes(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunWithMultiPlanDraftingWorkflow(t, handler)
	seedPlanOutputForNode(t, db, runID, "plan_drafting_scope", "Scope plan\n- Narrow rollout")
	seedPlanOutputForNode(t, db, runID, "plan_drafting_delivery", "Delivery plan\n- Stage deployment")
	revision := mustRefreshRunPlanRevision(t, db, runID)

	res := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/runs/" + runID + "/plan",
		actorID: "actor-allfather",
		roleID:  "allfather",
	})
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (%s)", res.Code, res.Body.String())
	}

	var payload struct {
		RevisionStatus string  `json:"revision_status"`
		PlanRevisionId *string `json:"plan_revision_id"`
		Components     []struct {
			NodeID  string `json:"node_id"`
			Content string `json:"content"`
		} `json:"components"`
		Content string `json:"content"`
	}
	if err := json.Unmarshal(res.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode multi-plan snapshot payload: %v", err)
	}
	if payload.RevisionStatus != "ready" {
		t.Fatalf("expected ready revision_status, got %q", payload.RevisionStatus)
	}
	if payload.PlanRevisionId == nil || *payload.PlanRevisionId != revision.PlanRevisionID {
		t.Fatalf("expected aggregated plan revision id %q, got %#v", revision.PlanRevisionID, payload.PlanRevisionId)
	}
	if len(payload.Components) != 2 {
		t.Fatalf("expected two aggregated plan components, got %#v", payload.Components)
	}
	componentNodeIDs := []string{payload.Components[0].NodeID, payload.Components[1].NodeID}
	slices.Sort(componentNodeIDs)
	if !slices.Equal(componentNodeIDs, []string{"plan_drafting_delivery", "plan_drafting_scope"}) {
		t.Fatalf("expected both multi-plan components, got %#v", payload.Components)
	}
	if !strings.Contains(payload.Content, "## plan_drafting_scope") || !strings.Contains(payload.Content, "## plan_drafting_delivery") {
		t.Fatalf("expected aggregated plan content to include both plan sections, got %q", payload.Content)
	}
	if !strings.Contains(payload.Content, "Narrow rollout") || !strings.Contains(payload.Content, "Stage deployment") {
		t.Fatalf("expected aggregated plan content to include both plan bodies, got %q", payload.Content)
	}
}

func TestServerRunPlanSnapshotFallbackUsesResolvedCustomPlanDraftingNodeID(t *testing.T) {
	t.Parallel()

	const planNodeID = "plan_drafting_scope"
	handler, _ := newAPIServerForTest(t)
	runID := seedRunWithCustomPlanDraftingWorkflow(t, handler, planNodeID)

	res := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/runs/" + runID + "/plan",
		actorID: "actor-allfather",
		roleID:  "allfather",
	})
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (%s)", res.Code, res.Body.String())
	}

	var payload struct {
		RevisionStatus string  `json:"revision_status"`
		PlanRevisionId *string `json:"plan_revision_id"`
		Components     []struct {
			NodeID string `json:"node_id"`
		} `json:"components"`
		Content string `json:"content"`
	}
	if err := json.Unmarshal(res.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode custom empty plan payload: %v", err)
	}
	if payload.RevisionStatus != "pending" {
		t.Fatalf("expected pending revision_status, got %q", payload.RevisionStatus)
	}
	if len(payload.Components) != 0 {
		t.Fatalf("expected no components for pending custom plan snapshot, got %#v", payload.Components)
	}
	if payload.PlanRevisionId != nil {
		t.Fatalf("expected no plan_revision_id for pending plan snapshot, got %#v", payload.PlanRevisionId)
	}
	if payload.Content != "" {
		t.Fatalf("expected empty content for missing custom plan, got %q", payload.Content)
	}
}

func TestServerRunPlanSnapshotReturnsEmptyDraftWhenPlanMissing(t *testing.T) {
	t.Parallel()

	handler, _ := newAPIServerForTest(t)
	runID := seedRunWithLinearExecutionWorkflow(t, handler)

	res := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/runs/" + runID + "/plan",
		actorID: "actor-allfather",
		roleID:  "allfather",
	})

	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (%s)", res.Code, res.Body.String())
	}

	var payload struct {
		RunID          string  `json:"run_id"`
		PlanState      string  `json:"plan_state"`
		RevisionStatus string  `json:"revision_status"`
		PlanRevisionId *string `json:"plan_revision_id"`
		Components     []struct {
			NodeID string `json:"node_id"`
		} `json:"components"`
		Content string `json:"content"`
	}
	if err := json.Unmarshal(res.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode empty plan payload: %v", err)
	}
	if payload.RunID != runID {
		t.Fatalf("expected run id %q, got %q", runID, payload.RunID)
	}
	if payload.Content != "" {
		t.Fatalf("expected empty content for missing plan, got %q", payload.Content)
	}
	if payload.RevisionStatus != "pending" {
		t.Fatalf("expected pending revision_status, got %q", payload.RevisionStatus)
	}
	if payload.PlanRevisionId != nil {
		t.Fatalf("expected no plan_revision_id for pending plan snapshot, got %#v", payload.PlanRevisionId)
	}
	if len(payload.Components) != 0 {
		t.Fatalf("expected no components for missing plan snapshot, got %#v", payload.Components)
	}
}

func TestServerListRunGovernanceDecisionsSupportsPagination(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunWithLinearExecutionWorkflow(t, handler)
	planRevisionID := seedCurrentPlanRevisionForNode(t, db, runID, "plan_drafting", "Historic plan")
	mustExecSQL(t, db, `INSERT INTO run_plan_reviews (plan_review_id, run_id, reviewed_plan_revision_id, decision, feedback_markdown, actor_id, role_id, queued_advisory_attempt, queued_plan_attempt, created_at) VALUES ('plan_review_1', ?, ?, 'approved', 'Approved v1', 'god-operator', 'allfather', NULL, NULL, '2026-03-01T12:01:00Z')`, runID, planRevisionID)
	mustExecSQL(t, db, `INSERT INTO run_final_reviews (final_review_id, run_id, decision, feedback_markdown, actor_id, role_id, queued_plan_attempt, created_at) VALUES ('final_review_1', ?, 'needs_rework', 'Final rework v1', 'god-operator', 'allfather', 1, '2026-03-01T12:02:00Z')`, runID)
	mustExecSQL(t, db, `INSERT INTO run_plan_reviews (plan_review_id, run_id, reviewed_plan_revision_id, decision, feedback_markdown, actor_id, role_id, queued_advisory_attempt, queued_plan_attempt, created_at) VALUES ('plan_review_2', ?, ?, 'needs_rework', 'Plan rework v2', 'god-operator', 'allfather', 2, 2, '2026-03-01T12:03:00Z')`, runID, planRevisionID)

	first := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/runs/" + runID + "/governance-decisions?limit=2",
		actorID: "actor-1",
		roleID:  "chancellor",
	})
	if first.Code != http.StatusOK {
		t.Fatalf("expected first page 200, got %d (%s)", first.Code, first.Body.String())
	}

	var firstPage struct {
		Items []struct {
			DecisionID string `json:"decision_id"`
		} `json:"items"`
		NextCursor *string `json:"next_cursor"`
	}
	if err := json.Unmarshal(first.Body.Bytes(), &firstPage); err != nil {
		t.Fatalf("decode first governance page: %v", err)
	}
	if len(firstPage.Items) != 2 {
		t.Fatalf("expected 2 governance decisions on first page, got %d", len(firstPage.Items))
	}
	if firstPage.Items[0].DecisionID != "plan_review_2" || firstPage.Items[1].DecisionID != "final_review_1" {
		t.Fatalf("expected newest decisions first on page one, got %#v", firstPage.Items)
	}
	if firstPage.NextCursor == nil || *firstPage.NextCursor == "" {
		t.Fatal("expected next_cursor for paginated governance history")
	}

	second := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/runs/" + runID + "/governance-decisions?limit=2&cursor=" + *firstPage.NextCursor,
		actorID: "actor-1",
		roleID:  "chancellor",
	})
	if second.Code != http.StatusOK {
		t.Fatalf("expected second page 200, got %d (%s)", second.Code, second.Body.String())
	}

	var secondPage struct {
		Items []struct {
			DecisionID string `json:"decision_id"`
		} `json:"items"`
		NextCursor *string `json:"next_cursor"`
	}
	if err := json.Unmarshal(second.Body.Bytes(), &secondPage); err != nil {
		t.Fatalf("decode second governance page: %v", err)
	}
	if len(secondPage.Items) != 1 {
		t.Fatalf("expected 1 governance decision on second page, got %d", len(secondPage.Items))
	}
	if secondPage.Items[0].DecisionID != "plan_review_1" {
		t.Fatalf("expected oldest decision on second page, got %#v", secondPage.Items)
	}
	if secondPage.NextCursor != nil {
		t.Fatalf("expected no next_cursor on final governance page, got %q", *secondPage.NextCursor)
	}

	invalid := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/runs/" + runID + "/governance-decisions?cursor=invalid-token",
		actorID: "actor-1",
		roleID:  "chancellor",
	})
	if invalid.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for invalid governance cursor, got %d (%s)", invalid.Code, invalid.Body.String())
	}
}

func TestServerGetRunReturnsCurrentAndReworkLoopCycleMetrics(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunViaAPI(t, handler)

	var workflowDefinitionID string
	if err := db.QueryRow(`SELECT workflow_definition_id FROM workflow_runs WHERE run_id = ?`, runID).Scan(&workflowDefinitionID); err != nil {
		t.Fatalf("query workflow_definition_id: %v", err)
	}

	now := time.Now().UTC().Format(time.RFC3339)
	mustExecSQL(t, db, `INSERT OR IGNORE INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer-shieldmaiden', 'Reviewer Shieldmaiden', 0, ?)`, now)
	mustExecSQL(t, db, `INSERT OR IGNORE INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('intelligence-qa-scout', 'Intelligence QA Scout', 0, ?)`, now)
	mustExecSQL(t, db, `INSERT OR IGNORE INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('intelligence-nightraven', 'Intelligence Nightraven', 0, ?)`, now)
	mustExecSQL(t, db, `INSERT OR IGNORE INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_reviewer_shieldmaiden', 'reviewer-shieldmaiden', 'Reviewer Shieldmaiden', 'idle', NULL, 0, ?, ?)`, now, now)
	mustExecSQL(t, db, `INSERT OR IGNORE INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_intelligence_qa_scout', 'intelligence-qa-scout', 'Intelligence QA Scout', 'idle', NULL, 0, ?, ?)`, now, now)
	mustExecSQL(t, db, `INSERT OR IGNORE INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_intelligence_nightraven', 'intelligence-nightraven', 'Intelligence Nightraven', 'idle', NULL, 0, ?, ?)`, now, now)

	mustExecSQL(t, db, `INSERT OR IGNORE INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_cycle_metrics_review', ?, 'execution_review_shieldmaiden', 'reviewer-shieldmaiden', 'execution_review', ?)`, workflowDefinitionID, now)
	mustExecSQL(t, db, `INSERT OR IGNORE INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_cycle_metrics_validation_a', ?, 'validation_gates_qa_scout', 'intelligence-qa-scout', 'validation_gates', ?)`, workflowDefinitionID, now)
	mustExecSQL(t, db, `INSERT OR IGNORE INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_cycle_metrics_validation_b', ?, 'validation_gates_nightraven', 'intelligence-nightraven', 'validation_gates', ?)`, workflowDefinitionID, now)

	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_cycle_review_1', ?, 'execution_review_shieldmaiden', 1,1, 'failed', 'reviewer-shieldmaiden', 'agent_reviewer_shieldmaiden', 1, ?, ?)`, runID, now, now)
	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_cycle_review_2', ?, 'execution_review_shieldmaiden', 2,2, 'succeeded', 'reviewer-shieldmaiden', 'agent_reviewer_shieldmaiden', 1, ?, ?)`, runID, now, now)
	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_cycle_validation_a_1', ?, 'validation_gates_qa_scout', 1,1, 'failed', 'intelligence-qa-scout', 'agent_intelligence_qa_scout', 1, ?, ?)`, runID, now, now)
	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_cycle_validation_b_1', ?, 'validation_gates_nightraven', 1,1, 'succeeded', 'intelligence-nightraven', 'agent_intelligence_nightraven', 1, ?, ?)`, runID, now, now)
	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_cycle_validation_a_2', ?, 'validation_gates_qa_scout', 2,2, 'succeeded', 'intelligence-qa-scout', 'agent_intelligence_qa_scout', 1, ?, ?)`, runID, now, now)
	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_cycle_validation_b_2', ?, 'validation_gates_nightraven', 2,2, 'succeeded', 'intelligence-nightraven', 'agent_intelligence_nightraven', 1, ?, ?)`, runID, now, now)
	mustExecSQL(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('review_attempt_1', ?, 'nr_cycle_review_1', 'committed_failure', 'execution_review_rework', 'review requested rework', ?, ?, ?, ?, ?)`, runID, now, now, now, now, now)
	mustExecSQL(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('validation_attempt_qa_1', ?, 'nr_cycle_validation_a_1', 'committed_failure', 'validation_gate_rework', 'validator requested rework', ?, ?, ?, ?, ?)`, runID, now, now, now, now, now)

	res := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/runs/" + runID,
		actorID: "actor-1",
		roleID:  "chancellor",
	})
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200 get run response, got %d (%s)", res.Code, res.Body.String())
	}

	var run gen.RunDetailSummary
	if err := json.Unmarshal(res.Body.Bytes(), &run); err != nil {
		t.Fatalf("decode run summary: %v", err)
	}
	if run.ReviewCurrentWave != 2 {
		t.Fatalf("expected review_current_wave=2, got %#v", run.ReviewCurrentWave)
	}
	if run.ReviewReworkCycles != 1 {
		t.Fatalf("expected review_rework_cycles=1, got %#v", run.ReviewReworkCycles)
	}
	if run.ValidationCurrentWave != 2 {
		t.Fatalf("expected validation_current_wave=2, got %#v", run.ValidationCurrentWave)
	}
	if run.ValidationReworkWaves != 1 {
		t.Fatalf("expected validation_rework_waves=1, got %#v", run.ValidationReworkWaves)
	}
}

func TestServerGetRunUsesWaveLineageInsteadOfLatestAttemptNumber(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunViaAPI(t, handler)

	var workflowDefinitionID string
	if err := db.QueryRow(`SELECT workflow_definition_id FROM workflow_runs WHERE run_id = ?`, runID).Scan(&workflowDefinitionID); err != nil {
		t.Fatalf("query workflow_definition_id: %v", err)
	}

	now := time.Now().UTC().Format(time.RFC3339)
	mustExecSQL(t, db, `INSERT OR IGNORE INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer-shieldmaiden', 'Reviewer Shieldmaiden', 0, ?)`, now)
	mustExecSQL(t, db, `INSERT OR IGNORE INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer-sentinel', 'Reviewer Sentinel', 0, ?)`, now)
	mustExecSQL(t, db, `INSERT OR IGNORE INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_wave_primary', 'reviewer-shieldmaiden', 'Reviewer Shieldmaiden', 'idle', NULL, 0, ?, ?)`, now, now)
	mustExecSQL(t, db, `INSERT OR IGNORE INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_wave_secondary', 'reviewer-sentinel', 'Reviewer Sentinel', 'idle', NULL, 0, ?, ?)`, now, now)
	mustExecSQL(t, db, `INSERT OR IGNORE INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_review_wave_lineage_primary', ?, 'execution_review_shieldmaiden', 'reviewer-shieldmaiden', 'execution_review', ?)`, workflowDefinitionID, now)
	mustExecSQL(t, db, `INSERT OR IGNORE INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_review_wave_lineage_secondary', ?, 'execution_review_sentinel', 'reviewer-sentinel', 'execution_review', ?)`, workflowDefinitionID, now)
	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_wave_lineage_primary_1', ?, 'execution_review_shieldmaiden', 1, 1, 'blocked', 'reviewer-shieldmaiden', 'agent_wave_primary', 1, ?, ?)`, runID, now, now)
	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_wave_lineage_primary_2', ?, 'execution_review_shieldmaiden', 2, 1, 'failed', 'reviewer-shieldmaiden', 'agent_wave_primary', 1, ?, ?)`, runID, now, now)
	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_wave_lineage_secondary_1', ?, 'execution_review_sentinel', 1, 1, 'failed', 'reviewer-sentinel', 'agent_wave_secondary', 1, ?, ?)`, runID, now, now)
	mustExecSQL(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('review_wave_lineage_primary_attempt_2', ?, 'nr_review_wave_lineage_primary_2', 'committed_failure', 'execution_review_rework', 'review requested rework', ?, ?, ?, ?, ?)`, runID, now, now, now, now, now)
	mustExecSQL(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('review_wave_lineage_secondary_attempt_1', ?, 'nr_review_wave_lineage_secondary_1', 'committed_failure', 'execution_review_rework', 'review requested rework', ?, ?, ?, ?, ?)`, runID, now, now, now, now, now)

	res := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: "/api/v1/runs/" + runID, actorID: "actor-1", roleID: "chancellor"})
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200 get run response, got %d (%s)", res.Code, res.Body.String())
	}

	var run gen.RunDetailSummary
	if err := json.Unmarshal(res.Body.Bytes(), &run); err != nil {
		t.Fatalf("decode run summary: %v", err)
	}
	if run.ReviewCurrentWave != 1 {
		t.Fatalf("expected review_current_wave=1 for staggered same-wave retry, got %#v", run.ReviewCurrentWave)
	}
	if run.ReviewReworkCycles != 1 {
		t.Fatalf("expected review_rework_cycles=1 for staggered same-wave retry, got %#v", run.ReviewReworkCycles)
	}
}

func TestServerGetRunIgnoresCancelledValidationWaveInReworkMetrics(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunViaAPI(t, handler)

	var workflowDefinitionID string
	if err := db.QueryRow(`SELECT workflow_definition_id FROM workflow_runs WHERE run_id = ?`, runID).Scan(&workflowDefinitionID); err != nil {
		t.Fatalf("query workflow_definition_id: %v", err)
	}

	now := time.Now().UTC().Format(time.RFC3339)
	mustExecSQL(t, db, `INSERT OR IGNORE INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('intelligence-qa-scout', 'Intelligence QA Scout', 0, ?)`, now)
	mustExecSQL(t, db, `INSERT OR IGNORE INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('intelligence-nightraven', 'Intelligence Nightraven', 0, ?)`, now)
	mustExecSQL(t, db, `INSERT OR IGNORE INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_intelligence_qa_scout', 'intelligence-qa-scout', 'Intelligence QA Scout', 'idle', NULL, 0, ?, ?)`, now, now)
	mustExecSQL(t, db, `INSERT OR IGNORE INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_intelligence_nightraven', 'intelligence-nightraven', 'Intelligence Nightraven', 'idle', NULL, 0, ?, ?)`, now, now)
	mustExecSQL(t, db, `INSERT OR IGNORE INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_cycle_metrics_validation_a_cancelled', ?, 'validation_gates_qa_scout', 'intelligence-qa-scout', 'validation_gates', ?)`, workflowDefinitionID, now)
	mustExecSQL(t, db, `INSERT OR IGNORE INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_cycle_metrics_validation_b_cancelled', ?, 'validation_gates_nightraven', 'intelligence-nightraven', 'validation_gates', ?)`, workflowDefinitionID, now)
	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_cycle_validation_cancelled_a_1', ?, 'validation_gates_qa_scout', 1,1, 'failed', 'intelligence-qa-scout', 'agent_intelligence_qa_scout', 1, ?, ?)`, runID, now, now)
	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_cycle_validation_cancelled_b_1', ?, 'validation_gates_nightraven', 1,1, 'cancelled', 'intelligence-nightraven', 'agent_intelligence_nightraven', 1, ?, ?)`, runID, now, now)
	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_cycle_validation_cancelled_a_2', ?, 'validation_gates_qa_scout', 2,2, 'ready', 'intelligence-qa-scout', 'agent_intelligence_qa_scout', 1, NULL, NULL)`, runID)
	mustExecSQL(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('validation_attempt_cancelled_qa_1', ?, 'nr_cycle_validation_cancelled_a_1', 'committed_failure', 'validation_gate_rework', 'validator requested rework', ?, ?, ?, ?, ?)`, runID, now, now, now, now, now)
	mustExecSQL(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('validation_attempt_cancelled_night_1', ?, 'nr_cycle_validation_cancelled_b_1', 'committed_cancelled', 'provider_cancelled', 'validator cancelled', ?, ?, ?, ?, ?)`, runID, now, now, now, now, now)

	res := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/runs/" + runID,
		actorID: "actor-1",
		roleID:  "chancellor",
	})
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200 get run response, got %d (%s)", res.Code, res.Body.String())
	}

	var run gen.RunDetailSummary
	if err := json.Unmarshal(res.Body.Bytes(), &run); err != nil {
		t.Fatalf("decode run summary: %v", err)
	}
	if run.ValidationCurrentWave != 2 {
		t.Fatalf("expected validation_current_wave=2, got %#v", run.ValidationCurrentWave)
	}
	if run.ValidationReworkWaves != 0 {
		t.Fatalf("expected validation_rework_waves=0 for cancelled mixed wave, got %#v", run.ValidationReworkWaves)
	}
}

func TestServerGetRunUsesCanonicalReviewWaveCounting(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunViaAPI(t, handler)

	var workflowDefinitionID string
	if err := db.QueryRow(`SELECT workflow_definition_id FROM workflow_runs WHERE run_id = ?`, runID).Scan(&workflowDefinitionID); err != nil {
		t.Fatalf("query workflow_definition_id: %v", err)
	}

	now := time.Now().UTC().Format(time.RFC3339)
	mustExecSQL(t, db, `INSERT OR IGNORE INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer-shieldmaiden', 'Reviewer Shieldmaiden', 0, ?)`, now)
	mustExecSQL(t, db, `INSERT OR IGNORE INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer-sentinel', 'Reviewer Sentinel', 0, ?)`, now)
	mustExecSQL(t, db, `INSERT OR IGNORE INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_reviewer_primary', 'reviewer-shieldmaiden', 'Reviewer Shieldmaiden', 'idle', NULL, 0, ?, ?)`, now, now)
	mustExecSQL(t, db, `INSERT OR IGNORE INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_reviewer_secondary', 'reviewer-sentinel', 'Reviewer Sentinel', 'idle', NULL, 0, ?, ?)`, now, now)
	mustExecSQL(t, db, `INSERT OR IGNORE INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_review_wave_primary', ?, 'execution_review_shieldmaiden', 'reviewer-shieldmaiden', 'execution_review', ?)`, workflowDefinitionID, now)
	mustExecSQL(t, db, `INSERT OR IGNORE INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_review_wave_secondary', ?, 'execution_review_sentinel', 'reviewer-sentinel', 'execution_review', ?)`, workflowDefinitionID, now)
	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_wave_primary_1', ?, 'execution_review_shieldmaiden', 1,1, 'failed', 'reviewer-shieldmaiden', 'agent_reviewer_primary', 1, ?, ?)`, runID, now, now)
	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_wave_secondary_1', ?, 'execution_review_sentinel', 1,1, 'failed', 'reviewer-sentinel', 'agent_reviewer_secondary', 1, ?, ?)`, runID, now, now)
	mustExecSQL(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('review_wave_primary_attempt_1', ?, 'nr_review_wave_primary_1', 'committed_failure', 'execution_review_rework', 'review requested rework', ?, ?, ?, ?, ?)`, runID, now, now, now, now, now)
	mustExecSQL(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('review_wave_secondary_attempt_1', ?, 'nr_review_wave_secondary_1', 'committed_failure', 'provider_process_failed', 'review process crashed', ?, ?, ?, ?, ?)`, runID, now, now, now, now, now)

	res := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: "/api/v1/runs/" + runID, actorID: "actor-1", roleID: "chancellor"})
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200 get run response, got %d (%s)", res.Code, res.Body.String())
	}

	var run gen.RunDetailSummary
	if err := json.Unmarshal(res.Body.Bytes(), &run); err != nil {
		t.Fatalf("decode run summary: %v", err)
	}
	if run.ReviewCurrentWave != 1 {
		t.Fatalf("expected review_current_wave=1, got %#v", run.ReviewCurrentWave)
	}
	if run.ReviewReworkCycles != 0 {
		t.Fatalf("expected review_rework_cycles=0 for mixed review wave, got %#v", run.ReviewReworkCycles)
	}
}

func TestServerGetRunCountsAutoSucceededLoopsFromCanonicalAttemptEventsOnly(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunViaAPI(t, handler)

	var workflowDefinitionID string
	if err := db.QueryRow(`SELECT workflow_definition_id FROM workflow_runs WHERE run_id = ?`, runID).Scan(&workflowDefinitionID); err != nil {
		t.Fatalf("query workflow_definition_id: %v", err)
	}

	now := time.Now().UTC().Format(time.RFC3339)
	mustExecSQL(t, db, `INSERT OR IGNORE INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer-shieldmaiden', 'Reviewer Shieldmaiden', 0, ?)`, now)
	mustExecSQL(t, db, `INSERT OR IGNORE INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('intelligence-qa-scout', 'Intelligence QA Scout', 0, ?)`, now)
	mustExecSQL(t, db, `INSERT OR IGNORE INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_review_auto_success', 'reviewer-shieldmaiden', 'Reviewer Shieldmaiden', 'idle', NULL, 0, ?, ?)`, now, now)
	mustExecSQL(t, db, `INSERT OR IGNORE INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_validation_auto_success', 'intelligence-qa-scout', 'Intelligence QA Scout', 'idle', NULL, 0, ?, ?)`, now, now)
	mustExecSQL(t, db, `INSERT OR IGNORE INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_auto_success_review', ?, 'execution_review_shieldmaiden', 'reviewer-shieldmaiden', 'execution_review', ?)`, workflowDefinitionID, now)
	mustExecSQL(t, db, `INSERT OR IGNORE INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_auto_success_validation', ?, 'validation_gates_qa_scout', 'intelligence-qa-scout', 'validation_gates', ?)`, workflowDefinitionID, now)
	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_auto_success_review', ?, 'execution_review_shieldmaiden', 2,2, 'succeeded', 'reviewer-shieldmaiden', 'agent_review_auto_success', 1, ?, ?)`, runID, now, now)
	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_auto_success_validation', ?, 'validation_gates_qa_scout', 3,3, 'succeeded', 'intelligence-qa-scout', 'agent_validation_auto_success', 1, ?, ?)`, runID, now, now)

	mustExecSQL(t, db, `INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('evt_auto_success_review_attempt', ?, 1, 'nr_auto_success_review', 'attempt_auto_success_review', 'v1', 'attempt.succeeded', 1, '{"status":"ok"}', 'runtime_orchestrator', 'reviewer-shieldmaiden', 'runtime.orchestrator', 'execution_review_cap_auto_succeeded', 'review auto-succeeded at cap', 'corr_auto_success_review', 'caus_auto_success_review_attempt', 'prev_auto_success_review_attempt', 'entry_auto_success_review_attempt', ?)`, runID, now)
	mustExecSQL(t, db, `INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('evt_auto_success_review_mailbox', ?, 2, 'nr_auto_success_review', 'run:mailbox', 'v1', 'mailbox.message_recorded', 1, '{"message_id":"msg_auto_success_review","node_id":"execution_review_shieldmaiden","node_run_id":"nr_auto_success_review","message_source":"communication","communication_type":"response_received","visible_to_operator":true}', 'agent_review_auto_success', 'reviewer-shieldmaiden', 'state.mailbox', 'execution_review_cap_auto_succeeded', 'mailbox message recorded', 'corr_auto_success_review_mailbox', 'caus_auto_success_review_mailbox', 'prev_auto_success_review_mailbox', 'entry_auto_success_review_mailbox', ?)`, runID, now)
	mustExecSQL(t, db, `INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('evt_auto_success_validation_attempt', ?, 3, 'nr_auto_success_validation', 'attempt_auto_success_validation', 'v1', 'attempt.succeeded', 1, '{"status":"ok"}', 'runtime_orchestrator', 'intelligence-qa-scout', 'runtime.orchestrator', 'validation_gate_cap_auto_succeeded', 'validation auto-succeeded at cap', 'corr_auto_success_validation', 'caus_auto_success_validation_attempt', 'prev_auto_success_validation_attempt', 'entry_auto_success_validation_attempt', ?)`, runID, now)
	mustExecSQL(t, db, `INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('evt_auto_success_validation_mailbox', ?, 4, 'nr_auto_success_validation', 'run:mailbox', 'v1', 'mailbox.message_recorded', 2, '{"message_id":"msg_auto_success_validation","node_id":"validation_gates_qa_scout","node_run_id":"nr_auto_success_validation","message_source":"communication","communication_type":"response_received","visible_to_operator":true}', 'agent_validation_auto_success', 'intelligence-qa-scout', 'state.mailbox', 'validation_gate_cap_auto_succeeded', 'mailbox message recorded', 'corr_auto_success_validation_mailbox', 'caus_auto_success_validation_mailbox', 'prev_auto_success_validation_mailbox', 'entry_auto_success_validation_mailbox', ?)`, runID, now)

	res := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/runs/" + runID,
		actorID: "actor-1",
		roleID:  "chancellor",
	})
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200 get run response, got %d (%s)", res.Code, res.Body.String())
	}

	var run gen.RunDetailSummary
	if err := json.Unmarshal(res.Body.Bytes(), &run); err != nil {
		t.Fatalf("decode run summary: %v", err)
	}
	if run.ReviewAutoSucceededCount != 1 {
		t.Fatalf("expected review_auto_succeeded_count=1, got %#v", run.ReviewAutoSucceededCount)
	}
	if run.ValidationAutoSucceededCount != 1 {
		t.Fatalf("expected validation_auto_succeeded_count=1, got %#v", run.ValidationAutoSucceededCount)
	}
}

func TestServerGetRunCountsValidationAutoSucceededByWaveNotPerValidator(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunViaAPI(t, handler)

	var workflowDefinitionID string
	if err := db.QueryRow(`SELECT workflow_definition_id FROM workflow_runs WHERE run_id = ?`, runID).Scan(&workflowDefinitionID); err != nil {
		t.Fatalf("query workflow_definition_id: %v", err)
	}

	now := time.Now().UTC().Format(time.RFC3339)
	mustExecSQL(t, db, `INSERT OR IGNORE INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('intelligence-qa-scout', 'Intelligence QA Scout', 0, ?)`, now)
	mustExecSQL(t, db, `INSERT OR IGNORE INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('intelligence-nightraven', 'Intelligence Nightraven', 0, ?)`, now)
	mustExecSQL(t, db, `INSERT OR IGNORE INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_validation_auto_wave_qa', 'intelligence-qa-scout', 'Intelligence QA Scout', 'idle', NULL, 0, ?, ?)`, now, now)
	mustExecSQL(t, db, `INSERT OR IGNORE INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_validation_auto_wave_night', 'intelligence-nightraven', 'Intelligence Nightraven', 'idle', NULL, 0, ?, ?)`, now, now)
	mustExecSQL(t, db, `INSERT OR IGNORE INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_auto_success_validation_wave_qa', ?, 'validation_gates_qa_scout', 'intelligence-qa-scout', 'validation_gates', ?)`, workflowDefinitionID, now)
	mustExecSQL(t, db, `INSERT OR IGNORE INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_auto_success_validation_wave_night', ?, 'validation_gates_nightraven', 'intelligence-nightraven', 'validation_gates', ?)`, workflowDefinitionID, now)
	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_auto_success_validation_wave_qa', ?, 'validation_gates_qa_scout', 4,4, 'succeeded', 'intelligence-qa-scout', 'agent_validation_auto_wave_qa', 1, ?, ?)`, runID, now, now)
	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_auto_success_validation_wave_night', ?, 'validation_gates_nightraven', 4,4, 'succeeded', 'intelligence-nightraven', 'agent_validation_auto_wave_night', 1, ?, ?)`, runID, now, now)
	mustExecSQL(t, db, `INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('evt_auto_success_validation_wave_qa', ?, 1, 'nr_auto_success_validation_wave_qa', 'attempt_auto_success_validation_wave_qa', 'v1', 'attempt.succeeded', 1, '{"status":"ok"}', 'runtime_orchestrator', 'intelligence-qa-scout', 'runtime.orchestrator', 'validation_gate_cap_auto_succeeded', 'validation auto-succeeded at cap', 'corr_auto_success_validation_wave_qa', 'caus_auto_success_validation_wave_qa', 'prev_auto_success_validation_wave_qa', 'entry_auto_success_validation_wave_qa', ?)`, runID, now)
	mustExecSQL(t, db, `INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('evt_auto_success_validation_wave_night', ?, 2, 'nr_auto_success_validation_wave_night', 'attempt_auto_success_validation_wave_night', 'v1', 'attempt.succeeded', 1, '{"status":"ok"}', 'runtime_orchestrator', 'intelligence-nightraven', 'runtime.orchestrator', 'validation_gate_cap_auto_succeeded', 'validation auto-succeeded at cap', 'corr_auto_success_validation_wave_night', 'caus_auto_success_validation_wave_night', 'prev_auto_success_validation_wave_night', 'entry_auto_success_validation_wave_night', ?)`, runID, now)

	res := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/runs/" + runID,
		actorID: "actor-1",
		roleID:  "chancellor",
	})
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200 get run response, got %d (%s)", res.Code, res.Body.String())
	}

	var run gen.RunDetailSummary
	if err := json.Unmarshal(res.Body.Bytes(), &run); err != nil {
		t.Fatalf("decode run summary: %v", err)
	}
	if run.ValidationAutoSucceededCount != 1 {
		t.Fatalf("expected validation_auto_succeeded_count=1 for one validator wave, got %#v", run.ValidationAutoSucceededCount)
	}
}

func TestServerRunDetailIncludesFailedNodeSummaries(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunWithLinearExecutionWorkflow(t, handler)
	now := time.Now().UTC().Format(time.RFC3339)

	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent-failed-builder', 'builder-berserker', 'Builder', 'failed', NULL, 0, ?, ?)`, now, now)
	mustExecSQL(t, db, `UPDATE workflow_runs SET run_status = 'failed', updated_at = ? WHERE run_id = ?`, now, runID)
	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr-failed-builder', ?, 'delegated_execution_berserker', 1,1, 'failed', 'builder-berserker', 'agent-failed-builder', 1, ?, ?)`, runID, now, now)
	mustExecSQL(t, db, `INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('evt-failed-builder', ?, 1, 'nr-failed-builder', 'attempt-failed-builder', 'v1', 'attempt.failed', 1, '{"error":"Builder crashed while preparing workspace."}', 'runtime_orchestrator', 'builder-berserker', 'runtime.orchestrator', 'execution_setup_failed', 'builder failed', 'corr-failed-builder', 'caus-failed-builder', 'prev-failed-builder', 'entry-failed-builder', ?)`, runID, now)

	res := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/runs/" + runID,
		actorID: "actor-1",
		roleID:  "chancellor",
	})
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200 get run response, got %d (%s)", res.Code, res.Body.String())
	}

	var run gen.RunDetailSummary
	if err := json.Unmarshal(res.Body.Bytes(), &run); err != nil {
		t.Fatalf("decode run summary: %v", err)
	}
	if len(run.FailedNodes) != 1 {
		t.Fatalf("expected one failed node summary, got %#v", run.FailedNodes)
	}
	if run.FailedNodes[0].NodeId != "delegated_execution_berserker" {
		t.Fatalf("expected delegated_execution_berserker failed node, got %#v", run.FailedNodes[0].NodeId)
	}
	if run.FailedNodes[0].FailureReason == nil || *run.FailedNodes[0].FailureReason != "Builder crashed while preparing workspace." {
		t.Fatalf("expected failed node reason, got %#v", run.FailedNodes[0].FailureReason)
	}
}

func TestServerRunDetailFailedNodeSummariesFallBackToReasonText(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunWithLinearExecutionWorkflow(t, handler)
	now := time.Now().UTC().Format(time.RFC3339)

	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent-failed-builder-reason-text', 'builder-berserker', 'Builder', 'failed', NULL, 0, ?, ?)`, now, now)
	mustExecSQL(t, db, `UPDATE workflow_runs SET run_status = 'failed', updated_at = ? WHERE run_id = ?`, now, runID)
	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr-failed-builder-reason-text', ?, 'delegated_execution_berserker', 1,1, 'failed', 'builder-berserker', 'agent-failed-builder-reason-text', 1, ?, ?)`, runID, now, now)
	mustExecSQL(t, db, `INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('evt-failed-builder-reason-text', ?, 1, 'nr-failed-builder-reason-text', 'attempt-failed-builder-reason-text', 'v1', 'attempt.failed', 1, '{"reason":"stale_attempt"}', 'runtime_orchestrator', 'builder-berserker', 'runtime.orchestrator', 'agent_unresponsive', 'Agent became unresponsive before completion could be confirmed.', 'corr-failed-builder-reason-text', 'caus-failed-builder-reason-text', 'prev-failed-builder-reason-text', 'entry-failed-builder-reason-text', ?)`, runID, now)

	res := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/runs/" + runID,
		actorID: "actor-1",
		roleID:  "chancellor",
	})
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200 get run response, got %d (%s)", res.Code, res.Body.String())
	}

	var run gen.RunDetailSummary
	if err := json.Unmarshal(res.Body.Bytes(), &run); err != nil {
		t.Fatalf("decode run summary: %v", err)
	}
	if len(run.FailedNodes) != 1 {
		t.Fatalf("expected one failed node summary, got %#v", run.FailedNodes)
	}
	if run.FailedNodes[0].FailureReason == nil || *run.FailedNodes[0].FailureReason != "Agent became unresponsive before completion could be confirmed." {
		t.Fatalf("expected failed node reason to fall back to reason_text, got %#v", run.FailedNodes[0].FailureReason)
	}
}

func TestServerRunDetailFailedNodeSummariesUseFriendlyProviderIncompleteTurnReason(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunWithLinearExecutionWorkflow(t, handler)
	now := time.Now().UTC().Format(time.RFC3339)

	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent-failed-validator-incomplete-turn', 'intelligence-nightraven', 'Validator', 'failed', NULL, 0, ?, ?)`, now, now)
	mustExecSQL(t, db, `UPDATE workflow_runs SET run_status = 'failed', updated_at = ? WHERE run_id = ?`, now, runID)
	mustExecSQL(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) SELECT 'wn-failed-validator-incomplete-turn', workflow_definition_id, 'validation_gates_nightraven', 'intelligence-nightraven', 'validation_gates', ? FROM workflow_runs WHERE run_id = ? AND NOT EXISTS (SELECT 1 FROM workflow_nodes WHERE workflow_definition_id = workflow_runs.workflow_definition_id AND node_id = 'validation_gates_nightraven')`, now, runID)
	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr-failed-validator-incomplete-turn', ?, 'validation_gates_nightraven', 1,1, 'failed', 'intelligence-nightraven', 'agent-failed-validator-incomplete-turn', 1, ?, ?)`, runID, now, now)
	mustExecSQL(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, lease_owner, lease_expires_at, lease_version, session_ref, provider_session_id, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('attempt-failed-validator-incomplete-turn', ?, 'nr-failed-validator-incomplete-turn', 'committed_failure', 'provider_backed', '', NULL, 1, 'session-failed-validator-incomplete-turn', 'ses-failed-validator-incomplete-turn', ?, ?, ?, 'provider_incomplete_turn', 'decode opencode export: unexpected end of JSON input', ?, ?)`, runID, now, now, now, now, now)
	mustExecSQL(t, db, `INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('evt-failed-validator-incomplete-turn', ?, 1, 'nr-failed-validator-incomplete-turn', 'attempt-failed-validator-incomplete-turn', 'v1', 'attempt.failed', 1, '{"error":"incomplete terminal state (reason=\"provider_incomplete_turn\")"}', 'runtime_orchestrator', 'intelligence-nightraven', 'runtime.orchestrator', 'provider_incomplete_turn', 'node execution completed', 'corr-failed-validator-incomplete-turn', 'caus-failed-validator-incomplete-turn', 'prev-failed-validator-incomplete-turn', 'entry-failed-validator-incomplete-turn', ?)`, runID, now)

	res := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/runs/" + runID,
		actorID: "actor-1",
		roleID:  "chancellor",
	})
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200 get run response, got %d (%s)", res.Code, res.Body.String())
	}

	var run gen.RunDetailSummary
	if err := json.Unmarshal(res.Body.Bytes(), &run); err != nil {
		t.Fatalf("decode run summary: %v", err)
	}
	if len(run.FailedNodes) != 1 {
		t.Fatalf("expected one failed node summary, got %#v", run.FailedNodes)
	}
	if run.FailedNodes[0].FailureReason == nil || *run.FailedNodes[0].FailureReason != "Provider completion could not be confirmed before the recovery window expired." {
		t.Fatalf("expected friendly failed node reason, got %#v", run.FailedNodes[0].FailureReason)
	}
}

func TestServerListRunAgentsReturnsParticipantsOrdered(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunViaAPI(t, handler)

	now := time.Now().UTC().Format(time.RFC3339)
	builderCreatedAt := time.Now().UTC().Add(-2 * time.Hour).Format(time.RFC3339)
	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent-root', 'king', 'Sovereign', 'busy', NULL, 0, ?, ?)`, now, now)
	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent-council', 'council-member', 'Council', 'idle', 'agent-root', 1, ?, ?)`, now, now)
	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent-builder', 'council-member', 'Builder', 'busy', 'agent-council', 2, ?, ?)`, builderCreatedAt, now)
	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent-ignore', 'council-member', 'Ignore', 'idle', NULL, 0, ?, ?)`, now, now)
	mustExecSQL(t, db, `INSERT INTO run_participants (run_id, agent_id, role_id, display_name, parent_agent_id, spawn_depth, first_seen_at) VALUES (?, 'agent-root', 'king', 'Sovereign', NULL, 0, ?)`, runID, now)
	mustExecSQL(t, db, `INSERT INTO run_participants (run_id, agent_id, role_id, display_name, parent_agent_id, spawn_depth, first_seen_at) VALUES (?, 'agent-council', 'council-member', 'Council', 'agent-root', 1, ?)`, runID, now)
	mustExecSQL(t, db, `INSERT INTO run_participants (run_id, agent_id, role_id, display_name, parent_agent_id, spawn_depth, first_seen_at) VALUES (?, 'agent-builder', 'council-member', 'Builder', 'agent-council', 2, ?)`, runID, now)

	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr-agent-root', ?, 'kickoff', 1,1, 'running', 'king', 'agent-root', 1, ?, NULL)`, runID, now)
	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr-agent-builder', ?, 'implementation', 1,1, 'running', 'council-member', 'agent-builder', 1, ?, NULL)`, runID, now)
	mustExecSQL(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, lease_owner, lease_expires_at, lease_version, session_ref, provider_session_id, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('`+runID+`:implementation:1', ?, 'nr-agent-builder', 'running', 'worker-1', '2026-03-01T14:00:00Z', 1, 'session-agent-builder', 'ses-agent-builder', ?, NULL, NULL, '', '', ?, ?)`, runID, now, now, now)
	mustExecSQL(t, db, `INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at) VALUES ('msg-run-agents-1', ?, 'agent-root', 'agent-council', 'thread-agents', 'status_update', 'normal', '{}', ?)`, runID, now)

	res := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: "/api/v1/runs/" + runID + "/agents", actorID: "actor-1", roleID: "chancellor"})
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (%s)", res.Code, res.Body.String())
	}

	var payload struct {
		RunID string `json:"run_id"`
		Items []struct {
			AgentID       string  `json:"agent_id"`
			ParentAgentID *string `json:"parent_agent_id"`
			FirstSeenAt   string  `json:"first_seen_at"`
		} `json:"items"`
	}
	if err := json.Unmarshal(res.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode run agents payload: %v", err)
	}
	var rawPayload struct {
		Items []map[string]any `json:"items"`
	}
	if err := json.Unmarshal(res.Body.Bytes(), &rawPayload); err != nil {
		t.Fatalf("decode raw run agents payload: %v", err)
	}

	if payload.RunID != runID {
		t.Fatalf("expected run id %q, got %q", runID, payload.RunID)
	}
	if len(payload.Items) != 3 {
		t.Fatalf("expected 3 participating agents, got %d", len(payload.Items))
	}
	if payload.Items[0].AgentID != "agent-root" || payload.Items[1].AgentID != "agent-council" || payload.Items[2].AgentID != "agent-builder" {
		t.Fatalf("unexpected run-agent ordering: %#v", payload.Items)
	}
	if payload.Items[1].ParentAgentID == nil || *payload.Items[1].ParentAgentID != "agent-root" {
		t.Fatalf("expected council parent relationship to be preserved")
	}
	if _, ok := rawPayload.Items[2]["session_id"]; ok {
		t.Fatalf("expected run agents payload to omit session_id, got %#v", rawPayload.Items[2]["session_id"])
	}
	if _, ok := rawPayload.Items[2]["provider_session_id"]; ok {
		t.Fatalf("expected run agents payload to omit provider_session_id, got %#v", rawPayload.Items[2]["provider_session_id"])
	}
	if payload.Items[2].FirstSeenAt != now {
		t.Fatalf("expected run-scoped first_seen_at for agent-builder, got %#v", payload.Items[2].FirstSeenAt)
	}
}

func TestServerListRunExecutionsReturnsExecutionScopedSessionIdentity(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunViaAPI(t, handler)

	now := time.Now().UTC().Format(time.RFC3339)
	earlier := time.Now().UTC().Add(-time.Hour).Format(time.RFC3339)
	mustExecSQL(t, db, `INSERT OR IGNORE INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder-berserker', 'Builder Berserker', 1, ?)`, now)
	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent-root-exec', 'king', 'Sovereign', 'busy', NULL, 0, ?, ?)`, now, now)
	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent-builder-exec', 'builder-berserker', 'Builder', 'busy', 'agent-root-exec', 1, ?, ?)`, now, now)
	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr-runtime-exec', ?, 'execution_kickoff', 1,1, 'failed', 'king', 'agent-root-exec', 1, ?, ?)`, runID, earlier, earlier)
	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr-provider-exec', ?, 'delegated_execution_berserker', 1,1, 'running', 'builder-berserker', 'agent-builder-exec', 1, ?, NULL)`, runID, now)
	mustExecSQL(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, lease_owner, lease_expires_at, lease_version, session_ref, provider_session_id, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('`+runID+`:execution_kickoff:1', ?, 'nr-runtime-exec', 'committed_failure', 'runtime_owned', '', NULL, 0, '', '', ?, ?, ?, 'execution_setup_failed', 'kickoff failed', ?, ?)`, runID, earlier, earlier, earlier, earlier, earlier)
	mustExecSQL(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, lease_owner, lease_expires_at, lease_version, session_ref, provider_session_id, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('`+runID+`:delegated_execution_berserker:1', ?, 'nr-provider-exec', 'running', 'provider_backed', 'worker-1', '2026-03-01T14:00:00Z', 1, 'session-exec-builder', 'ses-exec-builder', ?, NULL, NULL, '', '', ?, ?)`, runID, now, now, now)

	res := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: "/api/v1/runs/" + runID + "/executions", actorID: "actor-1", roleID: "chancellor"})
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (%s)", res.Code, res.Body.String())
	}

	var payload struct {
		RunID string `json:"run_id"`
		Items []struct {
			AttemptID         string  `json:"attempt_id"`
			NodeRunID         string  `json:"node_run_id"`
			NodeID            string  `json:"node_id"`
			Attempt           int     `json:"attempt"`
			AgentID           string  `json:"agent_id"`
			RequiredRole      string  `json:"required_role"`
			ExecutionMode     string  `json:"execution_mode"`
			Status            string  `json:"status"`
			LogicalSessionID  *string `json:"logical_session_id"`
			ProviderSessionID *string `json:"provider_session_id"`
			QueuedAt          string  `json:"queued_at"`
			ActivityAt        string  `json:"activity_at"`
			FailureCode       *string `json:"failure_code"`
			FailureText       *string `json:"failure_text"`
		} `json:"items"`
	}
	if err := json.Unmarshal(res.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode run executions payload: %v", err)
	}

	if payload.RunID != runID {
		t.Fatalf("expected run id %q, got %q", runID, payload.RunID)
	}
	if len(payload.Items) != 2 {
		t.Fatalf("expected 2 executions, got %#v", payload.Items)
	}

	if payload.Items[0].AttemptID != runID+`:delegated_execution_berserker:1` {
		t.Fatalf("expected provider-backed execution first, got %#v", payload.Items[0])
	}
	if payload.Items[0].ExecutionMode != "provider_backed" || payload.Items[0].Status != "running" {
		t.Fatalf("unexpected provider-backed execution payload: %#v", payload.Items[0])
	}
	if payload.Items[0].LogicalSessionID == nil || *payload.Items[0].LogicalSessionID != "session-exec-builder" {
		t.Fatalf("expected logical session id, got %#v", payload.Items[0].LogicalSessionID)
	}
	if payload.Items[0].ProviderSessionID == nil || *payload.Items[0].ProviderSessionID != "ses-exec-builder" {
		t.Fatalf("expected provider session id, got %#v", payload.Items[0].ProviderSessionID)
	}
	if payload.Items[0].ActivityAt != now {
		t.Fatalf("expected provider-backed activity_at to match latest execution activity, got %#v", payload.Items[0].ActivityAt)
	}
	if payload.Items[0].QueuedAt != now {
		t.Fatalf("expected provider-backed queued_at to match attempt creation, got %#v", payload.Items[0].QueuedAt)
	}

	if payload.Items[1].AttemptID != runID+`:execution_kickoff:1` {
		t.Fatalf("expected runtime-owned execution second, got %#v", payload.Items[1])
	}
	if payload.Items[1].ExecutionMode != "runtime_owned" || payload.Items[1].Status != "failed" {
		t.Fatalf("unexpected runtime-owned execution payload: %#v", payload.Items[1])
	}
	if payload.Items[1].LogicalSessionID != nil || payload.Items[1].ProviderSessionID != nil {
		t.Fatalf("expected runtime-owned execution to omit session identity fields, got %#v", payload.Items[1])
	}
	if payload.Items[1].ActivityAt != earlier {
		t.Fatalf("expected runtime-owned activity_at to match durable row activity, got %#v", payload.Items[1].ActivityAt)
	}
	if payload.Items[1].QueuedAt != earlier {
		t.Fatalf("expected runtime-owned queued_at to match attempt creation, got %#v", payload.Items[1].QueuedAt)
	}
	if payload.Items[1].FailureCode == nil || *payload.Items[1].FailureCode != "execution_setup_failed" {
		t.Fatalf("expected failure code, got %#v", payload.Items[1].FailureCode)
	}
	if payload.Items[1].FailureText == nil || *payload.Items[1].FailureText != "kickoff failed" {
		t.Fatalf("expected failure text, got %#v", payload.Items[1].FailureText)
	}

	runRes := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: "/api/v1/runs/" + runID, actorID: "actor-1", roleID: "chancellor"})
	if runRes.Code != http.StatusOK {
		t.Fatalf("expected run detail 200, got %d (%s)", runRes.Code, runRes.Body.String())
	}
	var runPayload map[string]any
	if err := json.Unmarshal(runRes.Body.Bytes(), &runPayload); err != nil {
		t.Fatalf("decode run detail response: %v", err)
	}
	tabCounts, ok := runPayload["tab_counts"].(map[string]any)
	if !ok {
		t.Fatalf("expected tab_counts object, got %#v", runPayload["tab_counts"])
	}
	executions, ok := tabCounts["executions"].(float64)
	if !ok {
		t.Fatalf("expected tab_counts.executions to be present, got %#v", tabCounts)
	}
	if executions != 2 {
		t.Fatalf("expected tab_counts.executions=2, got %#v", executions)
	}
}

func TestServerListRunExecutionsUsesVerifyingStatusForPendingSettlement(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunWithLinearExecutionWorkflow(t, handler)
	now := "2026-03-01T13:00:00Z"

	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent-builder-recovery', 'builder-berserker', 'Builder', 'busy', NULL, 0, ?, ?)`, now, now)
	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr-builder-recovery', ?, 'delegated_execution_berserker', 1,1, 'failed', 'builder-berserker', 'agent-builder-recovery', 1, ?, ?)`, runID, now, now)
	mustExecSQL(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, lease_owner, lease_expires_at, lease_version, session_ref, provider_session_id, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('`+runID+`:delegated_execution_berserker:1', ?, 'nr-builder-recovery', 'terminal_observed', 'provider_backed', '', NULL, 1, 'session-builder-recovery', 'ses-builder-recovery', ?, ?, NULL, '', '', ?, ?)`, runID, now, now, now, now)

	res := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: "/api/v1/runs/" + runID + "/executions", actorID: "actor-1", roleID: "chancellor"})
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (%s)", res.Code, res.Body.String())
	}

	var payload struct {
		Items []struct {
			AttemptID string `json:"attempt_id"`
			Status    string `json:"status"`
		} `json:"items"`
	}
	if err := json.Unmarshal(res.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode run executions payload: %v", err)
	}
	if len(payload.Items) != 1 {
		t.Fatalf("expected 1 execution, got %#v", payload.Items)
	}
	if payload.Items[0].AttemptID != runID+`:delegated_execution_berserker:1` {
		t.Fatalf("unexpected execution payload: %#v", payload.Items[0])
	}
	if payload.Items[0].Status != "verifying" {
		t.Fatalf("expected verifying status, got %#v", payload.Items[0].Status)
	}
}

func TestServerListRunExecutionsUsesBlockedStatusFromNodeState(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunWithLinearExecutionWorkflow(t, handler)
	now := "2026-03-01T13:00:00Z"

	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent-builder-blocked', 'builder-berserker', 'Builder', 'blocked', NULL, 0, ?, ?)`, now, now)
	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr-builder-blocked', ?, 'delegated_execution_berserker', 1, 1, 'blocked', 'builder-berserker', 'agent-builder-blocked', 1, ?, ?)`, runID, now, now)
	mustExecSQL(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, lease_owner, lease_expires_at, lease_version, session_ref, provider_session_id, launch_started_at, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('`+runID+`:delegated_execution_berserker:1', ?, 'nr-builder-blocked', 'committed_failure', 'provider_backed', '', NULL, 1, 'session-builder-blocked', 'ses-builder-blocked', ?, ?, ?, ?, 'dependency_blocked', 'validator blocked', ?, ?)`, runID, now, now, now, now, now, now)

	res := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: "/api/v1/runs/" + runID + "/executions", actorID: "actor-1", roleID: "chancellor"})
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (%s)", res.Code, res.Body.String())
	}

	var payload struct {
		Items []struct {
			Status string `json:"status"`
		} `json:"items"`
	}
	if err := json.Unmarshal(res.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode blocked run executions payload: %v", err)
	}
	if len(payload.Items) != 1 || payload.Items[0].Status != "blocked" {
		t.Fatalf("expected blocked execution status, got %#v", payload.Items)
	}
}

func TestServerGetRunExecutionReturnsObservedOutputDetail(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunWithLinearExecutionWorkflow(t, handler)
	now := "2026-03-01T13:00:00Z"
	attemptID := runID + `:execution_review_shieldmaiden:1`
	execStore := executionstore.New(db)

	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent-review-detail', 'reviewer-shieldmaiden', 'Reviewer', 'busy', NULL, 0, ?, ?)`, now, now)
	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr-review-detail', ?, 'execution_review_shieldmaiden', 1,1, 'failed', 'reviewer-shieldmaiden', 'agent-review-detail', 1, ?, ?)`, runID, now, now)
	if err := execStore.EnqueueAttempt(context.Background(), executionstore.EnqueueAttemptRequest{
		AttemptID:         attemptID,
		RunID:             runID,
		NodeRunID:         "nr-review-detail",
		ExecutionMode:     executionstore.ExecutionModeProviderBacked,
		SessionRef:        "session-review-detail",
		ProviderSessionID: "ses-review-detail",
		CreatedAt:         now,
	}); err != nil {
		t.Fatalf("enqueue attempt: %v", err)
	}
	if _, err := execStore.CreateProviderInvocation(context.Background(), executionstore.CreateProviderInvocationRequest{
		ProviderInvocationID: "inv-review-detail",
		AttemptID:            attemptID,
		RunID:                runID,
		NodeRunID:            "nr-review-detail",
		LogicalSessionID:     "session-review-detail",
		ProviderSessionID:    "ses-review-detail",
		CompletionCapability: "cap-review-detail",
		IssuedAt:             "2026-03-01T12:59:59Z",
		CreatedAt:            "2026-03-01T12:59:59Z",
	}); err != nil {
		t.Fatalf("create provider invocation: %v", err)
	}
	if _, err := execStore.MarkProviderInvocationOpen(context.Background(), executionstore.MarkProviderInvocationOpenRequest{
		ProviderInvocationID: "inv-review-detail",
		AttemptID:            attemptID,
		ProviderSessionID:    "ses-review-detail",
		OpenedAt:             now,
	}); err != nil {
		t.Fatalf("mark provider invocation open: %v", err)
	}
	if _, err := execStore.AcceptProviderCompletionReceipt(context.Background(), executionstore.AcceptProviderCompletionReceiptRequest{
		EvidenceID:            "receipt-review-detail",
		ProviderInvocationID:  "inv-review-detail",
		AttemptID:             attemptID,
		RunID:                 runID,
		NodeRunID:             "nr-review-detail",
		ProtocolVersion:       "2026-05-01",
		CompletionCapability:  "cap-review-detail",
		FinalText:             "VERDICT: REWORK\n\nCanonical reviewer response.",
		StructuredPayloadJSON: `{"verdict":"REWORK"}`,
		ReceivedAt:            now,
	}); err != nil {
		t.Fatalf("accept provider completion receipt: %v", err)
	}
	if _, err := execStore.FinalizeProviderCompletionSettlement(context.Background(), executionstore.FinalizeProviderCompletionSettlementRequest{
		SettlementID:          "settlement-review-detail",
		ProviderInvocationID:  "inv-review-detail",
		AttemptID:             attemptID,
		RunID:                 runID,
		NodeRunID:             "nr-review-detail",
		Outcome:               executionstore.ProviderCompletionOutcomeFailed,
		PrimaryEvidenceID:     "receipt-review-detail",
		FinalText:             "VERDICT: REWORK\n\nCanonical reviewer response.",
		StructuredPayloadJSON: `{"verdict":"REWORK"}`,
		FailureReasonCode:     "execution_review_rework",
		FailureReasonText:     "review requested rework",
		SettledAt:             now,
	}); err != nil {
		t.Fatalf("finalize provider completion settlement: %v", err)
	}
	mustExecSQL(t, db, `UPDATE attempt_executions SET state = 'committed_failure', terminal_observed_at = ?, committed_at = ?, failure_code = 'execution_review_rework', failure_text = 'review requested rework', created_at = ?, updated_at = ? WHERE attempt_id = ?`, now, now, now, now, attemptID)
	mustExecSQL(t, db, `INSERT INTO attempt_outbox (outbox_id, attempt_id, run_id, node_run_id, terminal_seq, terminal_state, payload_json, status, publish_attempts, last_error, created_at, updated_at, published_at) VALUES ('outbox-review-detail', ?, ?, 'nr-review-detail', 1, 'committed_failure', ?, 'published', 1, '', ?, ?, ?)`, attemptID, runID, `{"provider_invocation_id":"inv-review-stale","provider_invocation_at":"2026-03-01T12:58:00Z","output_text":"VERDICT: PASS\n\nStale reviewer response."}`, now, now, now)

	res := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: "/api/v1/runs/" + runID + "/executions/" + attemptID, actorID: "actor-1", roleID: "chancellor"})
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (%s)", res.Code, res.Body.String())
	}

	var payload struct {
		AttemptID            string  `json:"attempt_id"`
		NodeID               string  `json:"node_id"`
		ProviderInvocationID *string `json:"provider_invocation_id"`
		ProviderInvocationAt *string `json:"provider_invocation_at"`
		SettlementStatus     string  `json:"settlement_status"`
		SettledAt            *string `json:"settled_at"`
		OutputStatus         string  `json:"output_status"`
		OutputText           *string `json:"output_text"`
		OutputVerdict        *string `json:"output_verdict"`
		OutputVerdictStatus  *string `json:"output_verdict_status"`
		FailureCode          *string `json:"failure_code"`
	}
	if err := json.Unmarshal(res.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode run execution detail payload: %v", err)
	}
	if payload.AttemptID != attemptID || payload.NodeID != "execution_review_shieldmaiden" {
		t.Fatalf("unexpected execution detail payload: %#v", payload)
	}
	if payload.ProviderInvocationID == nil || *payload.ProviderInvocationID != "inv-review-detail" {
		t.Fatalf("expected provider invocation id, got %#v", payload.ProviderInvocationID)
	}
	if payload.ProviderInvocationAt == nil || *payload.ProviderInvocationAt != "2026-03-01T12:59:59Z" {
		t.Fatalf("expected provider invocation timestamp, got %#v", payload.ProviderInvocationAt)
	}
	if payload.SettlementStatus != "failed" {
		t.Fatalf("expected failed settlement status, got %#v", payload.SettlementStatus)
	}
	if payload.SettledAt == nil || *payload.SettledAt != now {
		t.Fatalf("expected settled_at, got %#v", payload.SettledAt)
	}
	if payload.OutputStatus != "settled" {
		t.Fatalf("expected settled output status, got %#v", payload.OutputStatus)
	}
	if payload.OutputText == nil || *payload.OutputText != "VERDICT: REWORK\n\nCanonical reviewer response." {
		t.Fatalf("expected output text, got %#v", payload.OutputText)
	}
	if payload.OutputVerdict == nil || *payload.OutputVerdict != "REWORK" {
		t.Fatalf("expected settled verdict REWORK, got %#v", payload.OutputVerdict)
	}
	if payload.OutputVerdictStatus == nil || *payload.OutputVerdictStatus != "settled" {
		t.Fatalf("expected settled verdict status, got %#v", payload.OutputVerdictStatus)
	}
	if payload.FailureCode == nil || *payload.FailureCode != "execution_review_rework" {
		t.Fatalf("expected durable failure code, got %#v", payload.FailureCode)
	}
	if payload.OutputText != nil && strings.Contains(*payload.OutputText, "Stale reviewer response") {
		t.Fatalf("expected canonical settlement text to override stale outbox text, got %#v", payload.OutputText)
	}
}

func TestServerGetRunExecutionReturnsPendingObservedOutputDetail(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunWithLinearExecutionWorkflow(t, handler)
	now := "2026-03-01T13:00:00Z"

	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent-review-pending-detail', 'reviewer-shieldmaiden', 'Reviewer', 'busy', NULL, 0, ?, ?)`, now, now)
	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr-review-pending-detail', ?, 'execution_review_shieldmaiden', 1,1, 'running', 'reviewer-shieldmaiden', 'agent-review-pending-detail', 1, ?, NULL)`, runID, now)
	mustExecSQL(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, lease_owner, lease_expires_at, lease_version, session_ref, provider_session_id, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('`+runID+`:execution_review_shieldmaiden:1', ?, 'nr-review-pending-detail', 'terminal_observed', 'provider_backed', '', NULL, 1, 'session-review-pending-detail', 'ses-review-pending-detail', ?, ?, NULL, '', '', ?, ?)`, runID, now, now, now, now)
	mustExecSQL(t, db, `INSERT INTO attempt_outbox (outbox_id, attempt_id, run_id, node_run_id, terminal_seq, terminal_state, payload_json, status, publish_attempts, last_error, created_at, updated_at, published_at) VALUES ('outbox-review-pending-detail', ?, ?, 'nr-review-pending-detail', 1, 'committed_success', ?, 'pending', 0, '', ?, ?, NULL)`, runID+`:execution_review_shieldmaiden:1`, runID, `{"provider_invocation_id":"inv-review-pending-detail","provider_invocation_at":"2026-03-01T12:59:59Z","output_text":"Context summary\nVERDICT: PASS\n\nCaptured reviewer response."}`, now, now)

	res := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: "/api/v1/runs/" + runID + "/executions/" + runID + `:execution_review_shieldmaiden:1`, actorID: "actor-1", roleID: "chancellor"})
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (%s)", res.Code, res.Body.String())
	}

	var payload struct {
		OutputStatus        string  `json:"output_status"`
		OutputText          *string `json:"output_text"`
		OutputVerdict       *string `json:"output_verdict"`
		OutputVerdictStatus *string `json:"output_verdict_status"`
		FailureCode         *string `json:"failure_code"`
		SettlementStatus    string  `json:"settlement_status"`
		SettlementDetail    *struct {
			Reason *string `json:"reason"`
		} `json:"settlement_detail"`
	}
	if err := json.Unmarshal(res.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode pending run execution detail payload: %v", err)
	}
	if payload.OutputStatus != "observed" {
		t.Fatalf("expected observed output status, got %#v", payload.OutputStatus)
	}
	if payload.OutputText == nil || *payload.OutputText != "Context summary\nVERDICT: PASS\n\nCaptured reviewer response." {
		t.Fatalf("expected observed output text, got %#v", payload.OutputText)
	}
	if payload.OutputVerdict == nil || *payload.OutputVerdict != "PASS" {
		t.Fatalf("expected observed verdict PASS, got %#v", payload.OutputVerdict)
	}
	if payload.OutputVerdictStatus == nil || *payload.OutputVerdictStatus != "observed" {
		t.Fatalf("expected observed verdict status, got %#v", payload.OutputVerdictStatus)
	}
	if payload.FailureCode != nil {
		t.Fatalf("expected no durable failure code while settlement is pending, got %#v", payload.FailureCode)
	}
	if payload.SettlementStatus != "pending" {
		t.Fatalf("expected pending settlement status, got %#v", payload.SettlementStatus)
	}
	if payload.SettlementDetail != nil {
		t.Fatalf("expected pending settlement detail to be omitted when no reason exists, got %#v", payload.SettlementDetail)
	}
}

func TestServerGetRunExecutionReturnsDurableProgressFields(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunWithLinearExecutionWorkflow(t, handler)
	now := "2026-03-01T13:00:00Z"
	attemptID := runID + `:execution_review_shieldmaiden:1`

	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent-review-progress-detail', 'reviewer-shieldmaiden', 'Reviewer', 'busy', NULL, 0, ?, ?)`, now, now)
	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr-review-progress-detail', ?, 'execution_review_shieldmaiden', 1,1, 'starting', 'reviewer-shieldmaiden', 'agent-review-progress-detail', 1, NULL, NULL)`, runID)
	mustExecSQL(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, lease_owner, lease_expires_at, lease_version, session_ref, provider_session_id, launch_started_at, started_at, terminal_observed_at, committed_at, failure_code, failure_text, progress_event_type, progress_reason_code, progress_at, created_at, updated_at) VALUES (?, ?, 'nr-review-progress-detail', 'starting', 'provider_backed', '', NULL, 1, 'session-review-progress-detail', '', ?, NULL, NULL, NULL, '', '', 'attempt.progress', 'provider_invoked', ?, ?, ?)`, attemptID, runID, now, now, now, now)

	res := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: "/api/v1/runs/" + runID + "/executions/" + attemptID, actorID: "actor-1", roleID: "chancellor"})
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (%s)", res.Code, res.Body.String())
	}

	var payload struct {
		Status             string  `json:"status"`
		ProgressEventType  *string `json:"progress_event_type"`
		ProgressReasonCode *string `json:"progress_reason_code"`
		ProgressAt         *string `json:"progress_at"`
		LaunchStartedAt    *string `json:"launch_started_at"`
		QueuedAt           string  `json:"queued_at"`
	}
	if err := json.Unmarshal(res.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode run execution progress detail payload: %v", err)
	}
	if payload.ProgressEventType == nil || *payload.ProgressEventType != "attempt.progress" {
		t.Fatalf("expected progress_event_type attempt.progress, got %#v", payload.ProgressEventType)
	}
	if payload.Status != "running" {
		t.Fatalf("expected provider_invoked progress to surface running status, got %q", payload.Status)
	}
	if payload.ProgressReasonCode == nil || *payload.ProgressReasonCode != "provider_invoked" {
		t.Fatalf("expected progress_reason_code provider_invoked, got %#v", payload.ProgressReasonCode)
	}
	if payload.ProgressAt == nil || *payload.ProgressAt != now {
		t.Fatalf("expected progress_at %q, got %#v", now, payload.ProgressAt)
	}
	if payload.LaunchStartedAt == nil || *payload.LaunchStartedAt != now {
		t.Fatalf("expected launch_started_at %q, got %#v", now, payload.LaunchStartedAt)
	}
	if payload.QueuedAt != now {
		t.Fatalf("expected queued_at %q, got %#v", now, payload.QueuedAt)
	}
}

func TestServerGetRunExecutionTreatsCommittedProviderAttemptsWithoutInvocationAsNotStarted(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunWithLinearExecutionWorkflow(t, handler)
	now := "2026-03-01T13:00:00Z"

	tests := []struct {
		name  string
		state string
	}{
		{name: "committed success", state: "committed_success"},
		{name: "committed failure", state: "committed_failure"},
		{name: "committed cancelled", state: "committed_cancelled"},
	}

	for idx, tt := range tests {
		attemptID := fmt.Sprintf("attempt-provider-settlement-pending-%d", idx+1)
		nodeRunID := fmt.Sprintf("nr-provider-settlement-pending-%d", idx+1)
		agentID := fmt.Sprintf("agent-provider-settlement-pending-%d", idx+1)
		mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES (?, 'reviewer-shieldmaiden', 'Reviewer', 'idle', NULL, 0, ?, ?)`, agentID, now, now)
		mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES (?, ?, 'execution_review_shieldmaiden', ?, 1, 'running', 'reviewer-shieldmaiden', ?, 1, ?, ?)`, nodeRunID, runID, idx+1, agentID, now, now)
		mustExecSQL(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, lease_owner, lease_expires_at, lease_version, session_ref, provider_session_id, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES (?, ?, ?, ?, 'provider_backed', '', NULL, 1, ?, ?, ?, ?, ?, '', '', ?, ?)`, attemptID, runID, nodeRunID, tt.state, fmt.Sprintf("session-%d", idx+1), fmt.Sprintf("ses-%d", idx+1), now, now, now, now, now)

		res := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: "/api/v1/runs/" + runID + "/executions/" + attemptID, actorID: "actor-1", roleID: "chancellor"})
		if res.Code != http.StatusOK {
			t.Fatalf("%s: expected 200, got %d (%s)", tt.name, res.Code, res.Body.String())
		}

		var payload struct {
			SettlementStatus string `json:"settlement_status"`
			SettlementDetail *struct {
				Reason *string `json:"reason"`
			} `json:"settlement_detail"`
		}
		if err := json.Unmarshal(res.Body.Bytes(), &payload); err != nil {
			t.Fatalf("%s: decode run execution detail payload: %v", tt.name, err)
		}
		if payload.SettlementStatus != "not_started" {
			t.Fatalf("%s: expected not_started settlement status, got %#v", tt.name, payload.SettlementStatus)
		}
		if payload.SettlementDetail != nil {
			t.Fatalf("%s: expected no settlement detail without invocation-backed settlement, got %#v", tt.name, payload.SettlementDetail)
		}
	}
}

func TestServerGetRunExecutionTreatsCommittedProviderFailuresWithInvocationAsTerminal(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunWithLinearExecutionWorkflow(t, handler)
	now := "2026-03-01T13:00:00Z"

	tests := []struct {
		name              string
		state             string
		expectedStatus    string
		expectedReason    string
		expectedNodeState string
	}{
		{name: "committed failure", state: "committed_failure", expectedStatus: "failed", expectedReason: "provider_incomplete_turn", expectedNodeState: "failed"},
		{name: "committed cancelled", state: "committed_cancelled", expectedStatus: "cancelled", expectedReason: "provider_incomplete_turn", expectedNodeState: "cancelled"},
	}

	for idx, tt := range tests {
		attemptID := fmt.Sprintf("attempt-provider-terminal-with-invocation-%d", idx+1)
		nodeRunID := fmt.Sprintf("nr-provider-terminal-with-invocation-%d", idx+1)
		agentID := fmt.Sprintf("agent-provider-terminal-with-invocation-%d", idx+1)
		mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES (?, 'reviewer-shieldmaiden', 'Reviewer', 'idle', NULL, 0, ?, ?)`, agentID, now, now)
		mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES (?, ?, 'execution_review_shieldmaiden', ?, 1, ?, 'reviewer-shieldmaiden', ?, 1, ?, ?)`, nodeRunID, runID, idx+1, tt.expectedNodeState, agentID, now, now)
		mustExecSQL(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, lease_owner, lease_expires_at, lease_version, session_ref, provider_session_id, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES (?, ?, ?, ?, 'provider_backed', '', NULL, 1, ?, ?, ?, ?, ?, 'provider_incomplete_turn', 'terminalized before settlement linked', ?, ?)`, attemptID, runID, nodeRunID, tt.state, fmt.Sprintf("session-%d", idx+1), fmt.Sprintf("ses-%d", idx+1), now, now, now, now, now)
		execStore := executionstore.New(db)
		if _, err := execStore.CreateProviderInvocation(context.Background(), executionstore.CreateProviderInvocationRequest{
			ProviderInvocationID: fmt.Sprintf("inv-%d", idx+1),
			AttemptID:            attemptID,
			RunID:                runID,
			NodeRunID:            nodeRunID,
			LogicalSessionID:     fmt.Sprintf("session-%d", idx+1),
			CompletionCapability: fmt.Sprintf("capability-%d", idx+1),
			IssuedAt:             now,
			CreatedAt:            now,
		}); err != nil {
			t.Fatalf("%s: create provider invocation: %v", tt.name, err)
		}
		if _, err := execStore.MarkProviderInvocationOpen(context.Background(), executionstore.MarkProviderInvocationOpenRequest{
			ProviderInvocationID: fmt.Sprintf("inv-%d", idx+1),
			AttemptID:            attemptID,
			ProviderSessionID:    fmt.Sprintf("ses-%d", idx+1),
			OpenedAt:             now,
		}); err != nil {
			t.Fatalf("%s: mark provider invocation open: %v", tt.name, err)
		}

		res := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: "/api/v1/runs/" + runID + "/executions/" + attemptID, actorID: "actor-1", roleID: "chancellor"})
		if res.Code != http.StatusOK {
			t.Fatalf("%s: expected 200, got %d (%s)", tt.name, res.Code, res.Body.String())
		}

		var payload struct {
			SettlementStatus string `json:"settlement_status"`
			SettlementDetail *struct {
				Reason *string `json:"reason"`
			} `json:"settlement_detail"`
		}
		if err := json.Unmarshal(res.Body.Bytes(), &payload); err != nil {
			t.Fatalf("%s: decode run execution detail payload: %v", tt.name, err)
		}
		if payload.SettlementStatus != tt.expectedStatus {
			t.Fatalf("%s: expected %s settlement status, got %#v", tt.name, tt.expectedStatus, payload.SettlementStatus)
		}
		if payload.SettlementDetail == nil || payload.SettlementDetail.Reason == nil || *payload.SettlementDetail.Reason != tt.expectedReason {
			t.Fatalf("%s: expected settlement detail reason %s, got %#v", tt.name, tt.expectedReason, payload.SettlementDetail)
		}
	}
}

func TestServerGetRunExecutionReturnsNotFoundForUnknownAttempt(t *testing.T) {
	t.Parallel()

	handler, _ := newAPIServerForTest(t)
	runID := seedRunViaAPI(t, handler)

	res := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: "/api/v1/runs/" + runID + "/executions/" + runID + `:missing:1`, actorID: "actor-1", roleID: "chancellor"})
	if res.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d (%s)", res.Code, res.Body.String())
	}
	assertErrorCode(t, res.Body.Bytes(), "dependency_blocked")
}

func TestServerListRunExecutionsRequiresAuthHeaders(t *testing.T) {
	t.Parallel()

	handler, _ := newAPIServerForTest(t)
	runID := seedRunViaAPI(t, handler)

	res := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: "/api/v1/runs/" + runID + "/executions"})
	if res.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d (%s)", res.Code, res.Body.String())
	}
	assertErrorCode(t, res.Body.Bytes(), "policy_violation")
}

func TestServerListRunExecutionsRejectsUnknownRole(t *testing.T) {
	t.Parallel()

	handler, _ := newAPIServerForTest(t)
	runID := seedRunViaAPI(t, handler)

	res := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: "/api/v1/runs/" + runID + "/executions", actorID: "actor-1", roleID: "role-missing"})
	if res.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d (%s)", res.Code, res.Body.String())
	}
	assertErrorCode(t, res.Body.Bytes(), "policy_violation")
}

func TestServerListRunExecutionsOrdersByActivityAt(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunViaAPI(t, handler)

	now := time.Now().UTC().Format(time.RFC3339)
	mustExecSQL(t, db, `INSERT OR IGNORE INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder-berserker', 'Builder Berserker', 1, ?)`, now)
	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent-order-builder', 'builder-berserker', 'Builder', 'busy', NULL, 0, ?, ?)`, now, now)
	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr-order-old', ?, 'delegated_execution_berserker', 1,1, 'failed', 'builder-berserker', 'agent-order-builder', 1, '2026-03-01T12:00:00Z', '2026-03-01T12:05:00Z')`, runID)
	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr-order-newer-start', ?, 'delegated_execution_berserker', 2,2, 'running', 'builder-berserker', 'agent-order-builder', 1, '2026-03-01T12:01:00Z', NULL)`, runID)
	mustExecSQL(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, lease_owner, lease_expires_at, lease_version, session_ref, provider_session_id, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('`+runID+`:delegated_execution_berserker:1', ?, 'nr-order-old', 'committed_failure', 'provider_backed', '', NULL, 0, 'session-order-old', 'ses-order-old', '2026-03-01T12:00:00Z', '2026-03-01T12:05:00Z', '2026-03-01T12:05:00Z', 'provider_process_failed', 'older start, latest activity', '2026-03-01T12:00:00Z', '2026-03-01T12:05:00Z')`, runID)
	mustExecSQL(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, lease_owner, lease_expires_at, lease_version, session_ref, provider_session_id, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('`+runID+`:delegated_execution_berserker:2', ?, 'nr-order-newer-start', 'running', 'provider_backed', 'worker-1', '2026-03-01T12:20:00Z', 1, 'session-order-new', 'ses-order-new', '2026-03-01T12:01:00Z', NULL, NULL, '', '', '2026-03-01T12:01:00Z', '2026-03-01T12:04:00Z')`, runID)

	res := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: "/api/v1/runs/" + runID + "/executions", actorID: "actor-1", roleID: "chancellor"})
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (%s)", res.Code, res.Body.String())
	}

	var payload struct {
		Items []struct {
			AttemptID  string `json:"attempt_id"`
			ActivityAt string `json:"activity_at"`
		} `json:"items"`
	}
	if err := json.Unmarshal(res.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode run executions payload: %v", err)
	}
	if len(payload.Items) < 2 {
		t.Fatalf("expected at least 2 executions, got %#v", payload.Items)
	}
	if payload.Items[0].AttemptID != runID+`:delegated_execution_berserker:1` {
		t.Fatalf("expected latest activity attempt first, got %#v", payload.Items)
	}
	if payload.Items[0].ActivityAt != "2026-03-01T12:05:00Z" {
		t.Fatalf("expected first activity_at to reflect latest durable activity, got %#v", payload.Items[0].ActivityAt)
	}
}

func TestServerListRunAgentsPreservesUnavailableStatuses(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunViaAPI(t, handler)
	now := time.Now().UTC().Format(time.RFC3339)

	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, spawn_depth, created_at, updated_at) VALUES ('agent-offline', 'council-member', 'Offline Scout', 'offline', 0, ?, ?)`, now, now)
	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, spawn_depth, created_at, updated_at) VALUES ('agent-failed', 'council-member', 'Failed Scout', 'failed', 0, ?, ?)`, now, now)
	mustExecSQL(t, db, `INSERT INTO run_participants (run_id, agent_id, role_id, display_name, parent_agent_id, spawn_depth, first_seen_at) VALUES (?, 'agent-offline', 'council-member', 'Offline Scout', NULL, 0, ?)`, runID, now)
	mustExecSQL(t, db, `INSERT INTO run_participants (run_id, agent_id, role_id, display_name, parent_agent_id, spawn_depth, first_seen_at) VALUES (?, 'agent-failed', 'council-member', 'Failed Scout', NULL, 0, ?)`, runID, now)

	res := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: "/api/v1/runs/" + runID + "/agents", actorID: "actor-1", roleID: "chancellor"})
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (%s)", res.Code, res.Body.String())
	}

	var payload struct {
		Items []struct {
			AgentID string `json:"agent_id"`
			Status  string `json:"status"`
		} `json:"items"`
	}
	if err := json.Unmarshal(res.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode run agents payload: %v", err)
	}

	statuses := map[string]string{}
	for _, item := range payload.Items {
		statuses[item.AgentID] = item.Status
	}
	if statuses["agent-offline"] != "offline" {
		t.Fatalf("expected offline API status, got %q", statuses["agent-offline"])
	}
	if statuses["agent-failed"] != "failed" {
		t.Fatalf("expected failed API status, got %q", statuses["agent-failed"])
	}
}

func TestServerRunAgentApisPreserveHistoricalMembershipSnapshots(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunWithLinearExecutionWorkflow(t, handler)
	now := time.Now().UTC().Format(time.RFC3339)

	mustExecSQL(t, db, `INSERT OR IGNORE INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('allfather', 'Allfather', 0, ?)`, now)
	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, spawn_depth, created_at, updated_at) VALUES ('agent-role-transition-api', 'allfather', 'Transition Agent', 'idle', 0, ?, ?)`, now, now)
	mustExecSQL(t, db, `INSERT INTO run_participants (run_id, agent_id, role_id, display_name, parent_agent_id, spawn_depth, first_seen_at) VALUES (?, 'agent-role-transition-api', 'allfather', 'Transition Agent', NULL, 0, ?)`, runID, now)
	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr-role-transition-api', ?, 'plan_drafting', 1,1, 'running', 'council-member', 'agent-role-transition-api', 1, ?, NULL)`, runID, now)
	mustExecSQL(t, db, `INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at) VALUES ('msg-role-transition-api', ?, 'agent-role-transition-api', 'agent-role-transition-api', 'thread-role-transition-api', 'status_update', 'normal', '{}', ?)`, runID, now)

	assertRunAgentAPIState := func(expectedCount int) {
		res := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: "/api/v1/runs/" + runID + "/agents", actorID: "actor-1", roleID: "chancellor"})
		if res.Code != http.StatusOK {
			t.Fatalf("expected agents 200, got %d (%s)", res.Code, res.Body.String())
		}

		var agentsPayload struct {
			Items []struct {
				AgentID string `json:"agent_id"`
			} `json:"items"`
		}
		if err := json.Unmarshal(res.Body.Bytes(), &agentsPayload); err != nil {
			t.Fatalf("decode agents payload: %v", err)
		}
		if len(agentsPayload.Items) != expectedCount {
			t.Fatalf("expected %d run agents, got %#v", expectedCount, agentsPayload.Items)
		}

		runRes := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: "/api/v1/runs/" + runID, actorID: "actor-1", roleID: "chancellor"})
		if runRes.Code != http.StatusOK {
			t.Fatalf("expected run detail 200, got %d (%s)", runRes.Code, runRes.Body.String())
		}

		var run gen.RunDetailSummary
		if err := json.Unmarshal(runRes.Body.Bytes(), &run); err != nil {
			t.Fatalf("decode run summary: %v", err)
		}
		if run.TabCounts.Agents != expectedCount {
			t.Fatalf("expected tab_counts.agents=%d, got %d", expectedCount, run.TabCounts.Agents)
		}
	}

	assertRunAgentAPIState(0)

	mustExecSQL(t, db, `UPDATE agents SET role_id = 'council-member', updated_at = ? WHERE agent_id = 'agent-role-transition-api'`, time.Now().UTC().Add(time.Second).Format(time.RFC3339))
	assertRunAgentAPIState(0)

	mustExecSQL(t, db, `UPDATE agents SET role_id = 'allfather', updated_at = ? WHERE agent_id = 'agent-role-transition-api'`, time.Now().UTC().Add(2*time.Second).Format(time.RFC3339))
	assertRunAgentAPIState(0)
}

func TestServerListRunAgentsReturnsNotFoundForUnknownRun(t *testing.T) {
	t.Parallel()

	handler, _ := newAPIServerForTest(t)
	res := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: "/api/v1/runs/run-missing/agents", actorID: "actor-1", roleID: "chancellor"})
	if res.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", res.Code)
	}
	assertErrorCode(t, res.Body.Bytes(), "dependency_blocked")
}

func TestServerListRunExecutionsReturnsNotFoundForUnknownRun(t *testing.T) {
	t.Parallel()

	handler, _ := newAPIServerForTest(t)
	res := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: "/api/v1/runs/run-missing/executions", actorID: "actor-1", roleID: "chancellor"})
	if res.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", res.Code)
	}
	assertErrorCode(t, res.Body.Bytes(), "dependency_blocked")
}

func TestServerRunExecutionActivityReturnsParsedDedupedItems(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunWithLinearExecutionWorkflow(t, handler)

	const attemptID = "attempt-activity-1"
	const otherAttemptID = "attempt-activity-2"
	now := "2026-06-14T10:00:00Z"
	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent-attempt-activity-1', 'builder-berserker', 'Builder', 'idle', NULL, 0, ?, ?)`, now, now)
	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_attempt_activity_1', ?, 'delegated_execution_berserker', 1,1, 'succeeded', 'builder-berserker', 'agent-attempt-activity-1', 1, ?, ?)`, runID, now, now)
	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_attempt_activity_2', ?, 'delegated_execution_berserker', 2,1, 'succeeded', 'builder-berserker', 'agent-attempt-activity-1', 1, ?, ?)`, runID, now, now)
	seedAttemptExecution(t, db, attemptID, runID, "nr_attempt_activity_1", "2026-06-14T10:00:00Z")
	seedAttemptExecution(t, db, otherAttemptID, runID, "nr_attempt_activity_2", "2026-06-14T10:00:00Z")
	insertActivityEvent := func(eventID, attempt string, runEventSeq, seq int, payloadJSON string) {
		t.Helper()
		mustExecSQL(t, db, `INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES (?, ?, ?, NULL, ?, 'v1', 'attempt.activity', ?, ?, 'runtime_orchestrator', 'builder-berserker', 'runtime.orchestrator', 'provider_activity', 'provider activity telemetry', 'corr-activity', ?, ?, ?, '2026-06-14T10:00:00Z')`,
			eventID, runID, runEventSeq, attempt, seq, payloadJSON, "caus-"+eventID, "prev-"+eventID, "entry-"+eventID)
	}

	insertActivityEvent("evt-act-r1", attemptID, 9001, 1, `{"kind":"kingdom.reasoning","activity_id":"part-r1","detail":"Considering layout","occurred_at":"2026-06-14T10:00:01Z"}`)
	insertActivityEvent("evt-act-t1", attemptID, 9002, 2, `{"kind":"kingdom.tool","activity_id":"part-t1","tool_name":"read","tool_status":"completed","tool_title":"foo.go","occurred_at":"2026-06-14T10:00:02Z"}`)
	// Same provider part streamed again — must collapse to one item on read.
	insertActivityEvent("evt-act-r1-dup", attemptID, 9003, 3, `{"kind":"kingdom.reasoning","activity_id":"part-r1","detail":"Considering layout","occurred_at":"2026-06-14T10:00:01Z"}`)
	// A different attempt's activity must not leak into this attempt's feed.
	insertActivityEvent("evt-act-other", otherAttemptID, 9004, 1, `{"kind":"kingdom.reasoning","activity_id":"part-other","detail":"Other","occurred_at":"2026-06-14T10:00:03Z"}`)
	// A coarse lifecycle event for the same attempt must be ignored by the activity feed.
	mustExecSQL(t, db, `INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('evt-act-progress', ?, 9005, NULL, ?, 'v1', 'attempt.progress', 4, '{"step":1}', 'runtime_orchestrator', 'builder-berserker', 'runtime.orchestrator', 'provider_invoked', 'progress', 'corr-activity', 'caus-act-progress', 'prev-act-progress', 'entry-act-progress', '2026-06-14T10:00:04Z')`, runID, attemptID)

	res := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/runs/" + runID + "/executions/" + attemptID + "/activity",
		actorID: "actor-1",
		roleID:  "chancellor",
	})
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", res.Code, res.Body.String())
	}

	var response gen.AcpRunActivityResponse
	if err := json.Unmarshal(res.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode activity response: %v", err)
	}
	if len(response.Items) != 2 {
		t.Fatalf("expected 2 deduped activity items for the attempt, got %d: %#v", len(response.Items), response.Items)
	}
	if response.Items[0].Id != "part-r1" || response.Items[0].Kind != gen.Thinking {
		t.Fatalf("expected first item to be reasoning part-r1, got %#v", response.Items[0])
	}
	if response.Items[1].Id != "part-t1" || response.Items[1].Kind != gen.Tool {
		t.Fatalf("expected second item to be tool part-t1, got %#v", response.Items[1])
	}

	// The coarse timeline must never surface activity events.
	timeline := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/runs/" + runID + "/timeline?limit=200",
		actorID: "actor-1",
		roleID:  "chancellor",
	})
	if timeline.Code != http.StatusOK {
		t.Fatalf("expected timeline 200, got %d", timeline.Code)
	}
	var timelinePage gen.RunTimelineResponse
	if err := json.Unmarshal(timeline.Body.Bytes(), &timelinePage); err != nil {
		t.Fatalf("decode timeline: %v", err)
	}
	for _, item := range timelinePage.Items {
		if item.EventType == "attempt.activity" {
			t.Fatalf("attempt.activity must be excluded from the timeline, got %#v", item)
		}
	}
}

func TestServerRunExecutionActivityReturnsNotFoundForUnknownAttempt(t *testing.T) {
	t.Parallel()

	handler, _ := newAPIServerForTest(t)
	runID := seedRunWithLinearExecutionWorkflow(t, handler)

	res := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/runs/" + runID + "/executions/attempt-missing/activity",
		actorID: "actor-1",
		roleID:  "chancellor",
	})
	if res.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", res.Code, res.Body.String())
	}
	assertErrorCode(t, res.Body.Bytes(), "dependency_blocked")
}

func TestServerRunExecutionActivityReturnsNotFoundForMismatchedRunAttempt(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunWithLinearExecutionWorkflow(t, handler)
	otherRunID := seedRunWithCustomPlanDraftingWorkflow(t, handler, "plan_drafting_cross_run")
	const attemptID = "attempt-activity-cross-run"
	now := "2026-06-14T10:00:00Z"
	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent-attempt-activity-cross-run', 'builder-berserker', 'Builder', 'idle', NULL, 0, ?, ?)`, now, now)
	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_attempt_activity_cross_run', ?, 'delegated_execution_berserker', 1,1, 'succeeded', 'builder-berserker', 'agent-attempt-activity-cross-run', 1, ?, ?)`, otherRunID, now, now)
	seedAttemptExecution(t, db, attemptID, otherRunID, "nr_attempt_activity_cross_run", "2026-06-14T10:00:00Z")
	mustExecSQL(t, db, `INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('evt-act-cross-run', ?, 9001, NULL, ?, 'v1', 'attempt.activity', 1, '{"kind":"kingdom.reasoning","activity_id":"part-cross-run","detail":"Other run","occurred_at":"2026-06-14T10:00:01Z"}', 'runtime_orchestrator', 'builder-berserker', 'runtime.orchestrator', 'provider_activity', 'provider activity telemetry', 'corr-activity', 'caus-cross-run', 'prev-cross-run', 'entry-cross-run', '2026-06-14T10:00:00Z')`, otherRunID, attemptID)

	res := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/runs/" + runID + "/executions/" + attemptID + "/activity",
		actorID: "actor-1",
		roleID:  "chancellor",
	})
	if res.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", res.Code, res.Body.String())
	}
	assertErrorCode(t, res.Body.Bytes(), "dependency_blocked")
}
