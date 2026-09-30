package planning

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"kingdom/internal/state"
)

const (
	planningSessionTitleStatePlaceholder = "placeholder"
	planningSessionTitleStateGenerated   = "generated"
	planningSessionTitleStateCustom      = "custom"
)

const (
	defaultMaxSnapshotFiles    = 200
	defaultMaxSnapshotFileSize = 64 * 1024
	// maxEnumeratedFiles bounds how many paths a scan looks at at all. It is
	// far above the capture budget because counting a path costs nothing but a
	// string, and the project shape is only honest if it saw the whole project.
	maxEnumeratedFiles = 20000
	// maxProjectPathLength bounds one entry read from the enumerator, so a
	// hostile or corrupt listing cannot make the scan allocate without limit.
	maxProjectPathLength      = 4096
	planningOperationLeaseTTL = 30 * time.Second
)

const openingQuestionPrompt = "How would you like to start?"

type UpdateSessionInput struct {
	Title              *string
	ModelSelectionJSON *string
}

type CreateOperationInput struct {
	Type              state.PlanningOperationType
	ReplyMarkdown     string
	AnswerPayloadJSON string
	// ParentMessageID branches a reply off a specific message. Nil = continue
	// from the session's active leaf (a normal reply); set = edit, i.e. append
	// a sibling under the edited message's parent. No truncation either way.
	ParentMessageID *string
	// TargetMessageID is the assistant message a regenerate forks a new sibling
	// branch from.
	TargetMessageID string
}

type CreateOperationResult struct {
	Operation state.PlanningOperation
	Session   state.PlanningSession
}

type SnapshotSummary struct {
	SummaryMarkdown string              `json:"summary_markdown"`
	IncludedFiles   []SnapshotFileEntry `json:"included_files"`
	ExcludedFiles   []SnapshotFileEntry `json:"excluded_files"`
}

type SnapshotFileEntry struct {
	Path   string `json:"path"`
	Reason string `json:"reason,omitempty"`
	Size   int64  `json:"size_bytes,omitempty"`
}

type StructuredQuestionOption struct {
	Label string `json:"label"`
	Value string `json:"value"`
}

type StructuredQuestion struct {
	Prompt  string                     `json:"prompt"`
	Type    string                     `json:"type"`
	Options []StructuredQuestionOption `json:"options,omitempty"`
}

type DraftSummary struct {
	Summary string `json:"summary"`
}

type PlannerModelSelection struct {
	Model          string `json:"model"`
	ThinkingEffort string `json:"thinking_effort,omitempty"`
}

type operationRequest struct {
	ContentMarkdown    string          `json:"content_markdown,omitempty"`
	AnswerPayload      json.RawMessage `json:"answer_payload,omitempty"`
	TriggerSource      string          `json:"trigger_source,omitempty"`
	UserMessageID      string          `json:"user_message_id,omitempty"`
	UserCreatedAt      string          `json:"user_created_at,omitempty"`
	AssistantMessageID string          `json:"assistant_message_id,omitempty"`
	AssistantCreatedAt string          `json:"assistant_created_at,omitempty"`
	DraftID            string          `json:"draft_id,omitempty"`
	DraftCreatedAt     string          `json:"draft_created_at,omitempty"`
	// ParentMessageID is where the produced branch attaches: the user message's
	// parent for reply/edit, or the regenerated assistant's parent for
	// regenerate. Empty means the conversation root.
	ParentMessageID string `json:"parent_message_id,omitempty"`
	// TargetMessageID records the assistant message a regenerate forks from.
	TargetMessageID string `json:"target_message_id,omitempty"`
	// PreviousActiveLeafMessageID preserves the exact transcript tip that was
	// canonical before regenerate temporarily rewound the session to the fork
	// point for prompting.
	PreviousActiveLeafMessageID string `json:"previous_active_leaf_message_id,omitempty"`
	// ForceNewSession is true when the branch does not continue from the prior
	// active leaf (edit or regenerate), so the provider session must restart.
	ForceNewSession bool `json:"force_new_session,omitempty"`
}

type clockFunc func() string
type idFunc func(prefix string) string

type plannerMetrics struct {
	recoveryCount     atomic.Uint64
	failureCount      atomic.Uint64
	streamReplayCount atomic.Uint64
	streamResumeCount atomic.Uint64
	foregroundStarts  atomic.Uint64
	recoveryStarts    atomic.Uint64
	foregroundClaims  atomic.Uint64
	recoveryClaims    atomic.Uint64
	claimFailures     atomic.Uint64
	claimRetries      atomic.Uint64
	startFailures     atomic.Uint64
	mu                sync.Mutex
	lastStartError    string
	lastStartErrorAt  string
}

type PlannerDispatchMetrics struct {
	ForegroundStartRequests int    `json:"foreground_start_requests"`
	RecoveryStartRequests   int    `json:"recovery_start_requests"`
	ForegroundClaimSuccess  int    `json:"foreground_claim_successes"`
	RecoveryClaimSuccess    int    `json:"recovery_claim_successes"`
	ClaimFailureCount       int    `json:"claim_failure_count"`
	ClaimRetryCount         int    `json:"claim_retry_count"`
	StartFailureCount       int    `json:"start_failure_count"`
	LastStartFailure        string `json:"last_start_failure,omitempty"`
	LastStartFailureAt      string `json:"last_start_failure_at,omitempty"`
}

type PlannerLatencyMetric struct {
	Samples int   `json:"samples"`
	AvgMs   int64 `json:"avg_ms"`
}

type GeneratedTitleLatencyMetrics struct {
	RecentSampleSize int                  `json:"recent_sample_size"`
	Queue            PlannerLatencyMetric `json:"queue"`
	Launch           PlannerLatencyMetric `json:"launch"`
	FirstOutput      PlannerLatencyMetric `json:"first_output"`
	Finalize         PlannerLatencyMetric `json:"finalize"`
	Total            PlannerLatencyMetric `json:"total"`
}

type GeneratedTitleRuntimeMetrics struct {
	QueuedOperations       int                          `json:"queued_operations"`
	RunningOperations      int                          `json:"running_operations"`
	FailedOperations       int                          `json:"failed_operations"`
	OldestQueuedCreatedAt  string                       `json:"oldest_queued_created_at,omitempty"`
	OldestRunningStartedAt string                       `json:"oldest_running_started_at,omitempty"`
	Latency                GeneratedTitleLatencyMetrics `json:"latency"`
}

type PlannerRuntimeMetrics struct {
	QueuedOperations       int                           `json:"queued_operations"`
	RunningOperations      int                           `json:"running_operations"`
	FailedOperations       int                           `json:"failed_operations"`
	ActiveSessions         int                           `json:"active_sessions"`
	ProcessingSessions     int                           `json:"processing_sessions"`
	ReplayCount            uint64                        `json:"planning_stream_replay_count"`
	ResumeCount            uint64                        `json:"planning_stream_resume_count"`
	RecoveryCount          uint64                        `json:"planning_operation_recovery_count"`
	FailureCount           uint64                        `json:"planning_operation_failure_count"`
	OldestQueuedCreatedAt  string                        `json:"oldest_queued_created_at,omitempty"`
	OldestRunningStartedAt string                        `json:"oldest_running_started_at,omitempty"`
	Dispatch               PlannerDispatchMetrics        `json:"dispatch"`
	GeneratedTitle         *GeneratedTitleRuntimeMetrics `json:"generated_title,omitempty"`
}

func (m *plannerMetrics) snapshotDispatchMetrics() PlannerDispatchMetrics {
	if m == nil {
		return PlannerDispatchMetrics{}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return PlannerDispatchMetrics{
		ForegroundStartRequests: int(m.foregroundStarts.Load()),
		RecoveryStartRequests:   int(m.recoveryStarts.Load()),
		ForegroundClaimSuccess:  int(m.foregroundClaims.Load()),
		RecoveryClaimSuccess:    int(m.recoveryClaims.Load()),
		ClaimFailureCount:       int(m.claimFailures.Load()),
		ClaimRetryCount:         int(m.claimRetries.Load()),
		StartFailureCount:       int(m.startFailures.Load()),
		LastStartFailure:        strings.TrimSpace(m.lastStartError),
		LastStartFailureAt:      strings.TrimSpace(m.lastStartErrorAt),
	}
}

func (m *plannerMetrics) recordStartFailure(now string, err error) {
	if m == nil || err == nil {
		return
	}
	m.startFailures.Add(1)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lastStartError = strings.TrimSpace(err.Error())
	m.lastStartErrorAt = strings.TrimSpace(now)
}

func defaultPlanningSessionTitleState(title string) string {
	if strings.TrimSpace(title) == "" {
		return planningSessionTitleStatePlaceholder
	}
	return planningSessionTitleStateCustom
}

func effectiveSessionTitle(session state.PlanningSession) string {
	if session.TitleState == planningSessionTitleStatePlaceholder {
		return ""
	}
	return strings.TrimSpace(session.Title)
}

func stringPtr(value string) *string {
	return &value
}

func decodeModelSelection(raw string) PlannerModelSelection {
	selection := PlannerModelSelection{}
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return selection
	}
	_ = json.Unmarshal([]byte(trimmed), &selection)
	selection.Model = strings.TrimSpace(selection.Model)
	selection.ThinkingEffort = strings.TrimSpace(selection.ThinkingEffort)
	return selection
}

// marshalQuestion serialises a StructuredQuestion for storage in the
// planning_messages.question_payload_json column. A nil question (or one
// missing required fields) intentionally renders as an empty JSON object —
// `{}` is the schema's "no question" sentinel. A genuine marshal failure
// propagates up so the runner can fail the operation cleanly rather than
// quietly persisting an empty payload that looks indistinguishable from an
// intentionally absent question.
func marshalQuestion(question *StructuredQuestion) (string, error) {
	if question == nil || strings.TrimSpace(question.Prompt) == "" || strings.TrimSpace(question.Type) == "" {
		return `{}`, nil
	}
	raw, err := json.Marshal(question)
	if err != nil {
		return "", fmt.Errorf("marshal planning question payload: %w", err)
	}
	return string(raw), nil
}

func openingQuestion() *StructuredQuestion {
	return &StructuredQuestion{
		Prompt: openingQuestionPrompt,
		Type:   "single_select",
		Options: []StructuredQuestionOption{
			{Label: "Explore the topic", Value: "explore"},
			{Label: "Review current context", Value: "review_context"},
			{Label: "Shape a plan", Value: "shape_plan"},
		},
	}
}

func decodeOperationRequest(raw string) (operationRequest, error) {
	request := operationRequest{}
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return request, nil
	}
	if err := json.Unmarshal([]byte(trimmed), &request); err != nil {
		return operationRequest{}, fmt.Errorf("decode planning operation request: %w", err)
	}
	request.ContentMarkdown = strings.TrimSpace(request.ContentMarkdown)
	return request, nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

// plannerLeaseOwner returns a process-unique owner string used to fence
// planning-operation leases. The hostname/pid pair makes the owner trivially
// auditable; the random suffix ensures two processes on the same host can
// never collide even if pids are recycled.
func plannerLeaseOwner() string {
	host, _ := os.Hostname()
	if strings.TrimSpace(host) == "" {
		host = "unknown"
	}
	var b [8]byte
	_, _ = rand.Read(b[:])
	return fmt.Sprintf("kingdomd-%s-%d-%x", host, os.Getpid(), b)
}
