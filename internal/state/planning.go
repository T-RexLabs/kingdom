package state

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

const (
	// These limits bound both the context scan and its durable representation.
	// Keep them here so direct store callers cannot persist a larger payload.
	MaxPlanningContextSnapshotEntries      = 200
	MaxPlanningContextSnapshotJSONBytes    = 1 << 20
	MaxPlanningContextSnapshotSummaryBytes = 32 << 10
	MaxPlanningContextSnapshotPathBytes    = 4096
	MaxPlanningContextSnapshotReasonBytes  = 128
)

var ErrPlanningOperationEventNotFound = errors.New("planning operation event not found")

type PlanningSessionLifecycleState string

const (
	PlanningSessionLifecycleStateActive   PlanningSessionLifecycleState = "active"
	PlanningSessionLifecycleStateArchived PlanningSessionLifecycleState = "archived"
)

type PlanningSessionConversationState string

const (
	PlanningSessionConversationStateReady      PlanningSessionConversationState = "ready"
	PlanningSessionConversationStateProcessing PlanningSessionConversationState = "processing"
)

type PlanningOperationType string

const (
	PlanningOperationTypeReply         PlanningOperationType = "reply"
	PlanningOperationTypeFormalize     PlanningOperationType = "formalize"
	PlanningOperationTypeRegenerate    PlanningOperationType = "regenerate"
	PlanningOperationTypeGenerateTitle PlanningOperationType = "generate_title"
)

type PlanningOperationStatus string

const (
	PlanningOperationStatusQueued    PlanningOperationStatus = "queued"
	PlanningOperationStatusRunning   PlanningOperationStatus = "running"
	PlanningOperationStatusSucceeded PlanningOperationStatus = "succeeded"
	PlanningOperationStatusFailed    PlanningOperationStatus = "failed"
	PlanningOperationStatusCancelled PlanningOperationStatus = "cancelled"
)

const (
	DefaultPlanningOperationListLimit      = 100
	DefaultPlanningOperationEventListLimit = 100
)

type PlanningMessageRole string

const (
	PlanningMessageRoleUser      PlanningMessageRole = "user"
	PlanningMessageRoleAssistant PlanningMessageRole = "assistant"
)

type PlanningSession struct {
	PlanningSessionID       string
	Title                   string
	TitleState              string
	LifecycleState          PlanningSessionLifecycleState
	ConversationState       PlanningSessionConversationState
	ActiveLeafMessageID     *string
	CurrentOperationID      *string
	CurrentTitleOperationID *string
	CurrentDraftID          *string
	ContextSnapshotID       *string
	ProjectRoot             string
	ModelSelectionJSON      string
	CreatedAt               string
	UpdatedAt               string
}

type PlanningMessage struct {
	PlanningMessageID   string
	PlanningSessionID   string
	ParentMessageID     *string
	Role                PlanningMessageRole
	ContentMarkdown     string
	QuestionPayloadJSON string
	AnswerPayloadJSON   string
	CreatedAt           string
}

// PlanningMessageBranchInfo positions a message among its siblings (messages
// sharing a parent, ordered by created_at). Derived, never stored; drives the
// `‹ k/n ›` branch navigator wherever Count > 1.
type PlanningMessageBranchInfo struct {
	Index int
	Count int
}

type PlanningMessagePage struct {
	PlanningSessionID string
	Items             []PlanningMessage
	TotalCount        int
	FirstItemIndex    int
}

type PlanningDraft struct {
	PlanningDraftID   string
	PlanningSessionID string
	DecreeTitle       string
	DecreeText        string
	SummaryJSON       string
	CreatedAt         string
}

type PlanningContextSnapshot struct {
	PlanningContextSnapshotID string
	PlanningSessionID         string
	SummaryMarkdown           string
	IncludedFilesJSON         string
	ExcludedFilesJSON         string
	CreatedAt                 string
}

type PlanningOperation struct {
	OperationID                string
	AcpRunID                   string
	PlanningSessionID          string
	PlanningContextSnapshotID  *string
	Type                       PlanningOperationType
	Status                     PlanningOperationStatus
	RequestPayloadJSON         string
	LeaseOwner                 string
	LeaseExpiresAt             *string
	LeaseVersion               int
	LastHeartbeatAt            *string
	ProviderSessionID          *string
	ActiveProviderInvocationID *string
	ObservedOutputText         string
	Error                      *string
	ResultRef                  *string
	CanonicalSettlementID      *string
	CreatedAt                  string
	StartedAt                  *string
	FinishedAt                 *string
	// AutomaticRetry is populated only by a due retry claim. It is not stored
	// on planning_operations; retry_decisions remains durable authority.
	AutomaticRetry bool
}

type PlanningOperationEvent struct {
	EventID     string
	OperationID string
	Seq         int
	EventType   string
	PayloadJSON string
	CreatedAt   string
}

type PlanningActivityReconciliationStatus string

const (
	PlanningActivityReconciliationStatusPending   PlanningActivityReconciliationStatus = "pending"
	PlanningActivityReconciliationStatusCompleted PlanningActivityReconciliationStatus = "completed"
)

// PlanningActivityReconciliation is a post-settlement diagnostic activity
// delivery lifecycle. It is deliberately independent of the operation lease.
type PlanningActivityReconciliation struct {
	PlanningOperationID          string
	PlanningProviderInvocationID string
	Status                       PlanningActivityReconciliationStatus
	LeaseOwner                   string
	LeaseExpiresAt               *string
	LeaseVersion                 int
	CreatedAt                    string
	CompletedAt                  *string
}

type CompletePlanningActivityReconciliationRequest struct {
	PlanningOperationID string
	LeaseOwner          string
	LeaseVersion        int
	CompletedAt         string
	ActivityEvents      []AppendPlanningOperationEventRequest
	MarkerEvent         AppendPlanningOperationEventRequest
}

type PlanningOperationTelemetrySample struct {
	OperationID           string
	CreatedAt             string
	StartedAt             *string
	FinishedAt            *string
	ProviderSessionOpenAt *string
	FirstTextDeltaAt      *string
	FinalizingAt          *string
}

type CreatePlanningSessionRequest struct {
	PlanningSessionID       string
	Title                   string
	TitleState              string
	LifecycleState          PlanningSessionLifecycleState
	ConversationState       PlanningSessionConversationState
	ActiveLeafMessageID     string
	CurrentOperationID      string
	CurrentTitleOperationID string
	CurrentDraftID          string
	ContextSnapshotID       string
	ProjectRoot             string
	ModelSelectionJSON      string
	CreatedAt               string
	UpdatedAt               string
}

type UpdatePlanningSessionRequest struct {
	PlanningSessionID       string
	Title                   *string
	TitleState              *string
	LifecycleState          *PlanningSessionLifecycleState
	ConversationState       *PlanningSessionConversationState
	ActiveLeafMessageID     *string
	CurrentOperationID      *string
	CurrentTitleOperationID *string
	CurrentDraftID          *string
	ContextSnapshotID       *string
	ModelSelectionJSON      *string
	UpdatedAt               string
}

type AppendPlanningMessageRequest struct {
	PlanningMessageID   string
	PlanningSessionID   string
	ParentMessageID     string
	Role                PlanningMessageRole
	ContentMarkdown     string
	QuestionPayloadJSON string
	AnswerPayloadJSON   string
	CreatedAt           string
}

type CreatePlanningDraftRequest struct {
	PlanningDraftID   string
	PlanningSessionID string
	DecreeTitle       string
	DecreeText        string
	SummaryJSON       string
	CreatedAt         string
}

type CreatePlanningContextSnapshotRequest struct {
	PlanningContextSnapshotID string
	PlanningSessionID         string
	SummaryMarkdown           string
	IncludedFilesJSON         string
	ExcludedFilesJSON         string
	CreatedAt                 string
}

type CreatePlanningOperationRequest struct {
	OperationID                string
	PlanningSessionID          string
	Type                       PlanningOperationType
	Status                     PlanningOperationStatus
	RequestPayloadJSON         string
	LeaseOwner                 string
	LeaseExpiresAt             string
	LeaseVersion               int
	LastHeartbeatAt            string
	ProviderSessionID          string
	ActiveProviderInvocationID string
	ObservedOutputText         string
	Error                      string
	ResultRef                  string
	CanonicalSettlementID      string
	CreatedAt                  string
	StartedAt                  string
	FinishedAt                 string
}

type UpdatePlanningOperationRequest struct {
	OperationID                string
	Status                     *PlanningOperationStatus
	LeaseOwner                 *string
	LeaseExpiresAt             *string
	LeaseVersion               *int
	LastHeartbeatAt            *string
	ProviderSessionID          *string
	ActiveProviderInvocationID *string
	ObservedOutputText         *string
	Error                      *string
	ResultRef                  *string
	CanonicalSettlementID      *string
	StartedAt                  *string
	FinishedAt                 *string
}

type AppendPlanningOperationEventRequest struct {
	EventID     string
	OperationID string
	Seq         int
	EventType   string
	PayloadJSON string
	CreatedAt   string
}

type CancelQueuedPlanningOperationRequest struct {
	OperationID   string
	ErrorMessage  string
	UpdatedAt     string
	FinishedAt    string
	TerminalEvent *AppendPlanningOperationEventRequest
}

type FailQueuedPlanningOperationRequest struct {
	OperationID   string
	ErrorMessage  string
	UpdatedAt     string
	FinishedAt    string
	TerminalEvent AppendPlanningOperationEventRequest
}

type CreatePlanningSessionBundleRequest struct {
	Session        CreatePlanningSessionRequest
	OpeningMessage AppendPlanningMessageRequest
	// ContextSnapshot is captured with the session, not after it. Every
	// provider-backed planner operation binds the session's active context
	// snapshot at enqueue, so a session that exists without one cannot chat.
	ContextSnapshot CreatePlanningContextSnapshotRequest
}

type CreatePlanningReplyBundleRequest struct {
	SessionID         string
	UserMessage       AppendPlanningMessageRequest
	Operation         CreatePlanningOperationRequest
	InitialEvent      *AppendPlanningOperationEventRequest
	ConversationState PlanningSessionConversationState
	UpdatedAt         string
}

type CreateRegenerateBundleRequest struct {
	SessionID string
	// ActiveLeafMessageID is the regenerate target's parent: the active leaf
	// moves here for the duration of the run so the planner is prompted with the
	// conversation up to the fork point (empty = the conversation root).
	ActiveLeafMessageID string
	Operation           CreatePlanningOperationRequest
	InitialEvent        *AppendPlanningOperationEventRequest
	ConversationState   PlanningSessionConversationState
	UpdatedAt           string
}

type CreatePlanningGenerateTitleBundleRequest struct {
	SessionID               string
	Operation               CreatePlanningOperationRequest
	InitialEvent            *AppendPlanningOperationEventRequest
	CurrentTitleOperationID string
	UpdatedAt               string
}

type CreatePlanningFormalizeBundleRequest struct {
	SessionID          string
	Operation          CreatePlanningOperationRequest
	InitialEvent       *AppendPlanningOperationEventRequest
	ConversationState  PlanningSessionConversationState
	CurrentOperationID string
	UpdatedAt          string
}

// FinalizeOperationKind identifies the per-operation post-conditions that
// FinalizeOperation must apply atomically with the lease-fenced terminal write.
type FinalizeOperationKind int

const (
	FinalizeReply FinalizeOperationKind = iota + 1
	FinalizeRegenerate
	FinalizeFormalize
	FinalizeFail
	// FinalizeCancel terminates an in-flight operation as cancelled
	// (status = cancelled), distinct from FinalizeFail. Used when the
	// operation is interrupted by a system event such as service
	// shutdown — the runner observed the cancellation, so this is not
	// a planner-side failure and must not be conflated with one in
	// metrics, alerts, or UI copy. The request carries the cancellation
	// reason in ErrorMessage so consumers have human-readable context.
	FinalizeCancel
)

// FinalizeOperationRequest collapses the interactive planning operation
// terminal paths (reply, regenerate, formalize, fail, cancel) into one
// parametric shape. Generated title runs on its own background lane and uses a
// dedicated finalize path so it cannot contend with the session's interactive
// slot.
type FinalizeOperationRequest struct {
	Kind         FinalizeOperationKind
	SessionID    string
	OperationID  string
	LeaseOwner   string // owner from claim — required to fence the terminal write
	LeaseVersion int    // version from claim — required to fence the terminal write
	// AllowExpiredLease is reserved for the stale-operation recovery authority.
	// Normal worker finalization must only settle an unexpired lease.
	AllowExpiredLease bool

	ConversationState PlanningSessionConversationState
	UpdatedAt         string
	FinishedAt        string

	// Reply / Regenerate — append-only into the message tree; the active leaf
	// pointer selects the newly produced branch.
	UserMessage         *AppendPlanningMessageRequest
	AssistantMessage    *AppendPlanningMessageRequest
	ActiveLeafMessageID string

	// Formalize
	Draft          *CreatePlanningDraftRequest
	CurrentDraftID string

	// Common
	ResultRef          string
	ErrorMessage       string // FinalizeFail / FinalizeCancel only
	ObservedOutputText string
	ProviderSettlement *FinalizePlanningProviderSettlementRequest
	// MessageCompletedEvent is committed atomically with the canonical message,
	// draft, or title, provider settlement, and terminal run event. Streamed
	// message.part events remain provisional until this boundary commits.
	MessageCompletedEvent *AppendPlanningOperationEventRequest
	TerminalEvent         *AppendPlanningOperationEventRequest
}

// FinalizeGenerateTitleRequest settles a successful generated-title operation
// on the title lane. Unlike interactive finalization, it never touches
// conversation_state or current_operation_id.
type FinalizeGenerateTitleRequest struct {
	SessionID    string
	OperationID  string
	LeaseOwner   string
	LeaseVersion int

	UpdatedAt             string
	FinishedAt            string
	GeneratedTitle        string
	ObservedOutputText    string
	ProviderSettlement    *FinalizePlanningProviderSettlementRequest
	MessageCompletedEvent *AppendPlanningOperationEventRequest
	TerminalEvent         *AppendPlanningOperationEventRequest
}

// FinalizeGenerateTitleAbortRequest terminates a generated-title operation as
// failed or cancelled without touching the interactive session lane.
type FinalizeGenerateTitleAbortRequest struct {
	SessionID         string
	OperationID       string
	LeaseOwner        string
	LeaseVersion      int
	AllowExpiredLease bool

	Status             PlanningOperationStatus
	UpdatedAt          string
	FinishedAt         string
	ErrorMessage       string
	ObservedOutputText string
	ProviderSettlement *FinalizePlanningProviderSettlementRequest
	TerminalEvent      *AppendPlanningOperationEventRequest
}

type PlanningStore struct {
	db                      *sql.DB
	processGuardRuntimeRoot string
}

func (s *PlanningStore) SetProcessGuardRuntimeRoot(root string) {
	if s != nil {
		s.processGuardRuntimeRoot = strings.TrimSpace(root)
	}
}

type sqlExecutor interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

type sqlQueryRower interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// sqlQuerier is satisfied by both *sql.DB and *sql.Tx, so read/walk helpers can
// run against the pool or inside a transaction.
type sqlQuerier interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func NewPlanningStore(db *sql.DB) *PlanningStore {
	return &PlanningStore{db: db}
}

func validatePlanningSessionCreate(req CreatePlanningSessionRequest) error {
	if strings.TrimSpace(req.PlanningSessionID) == "" {
		return fmt.Errorf("planning session id is required")
	}
	if strings.TrimSpace(req.Title) == "" {
		return fmt.Errorf("planning session title is required")
	}
	if !isValidPlanningSessionTitleState(req.TitleState) {
		return fmt.Errorf("planning session title state is required")
	}
	if !isValidPlanningSessionLifecycleState(req.LifecycleState) {
		return fmt.Errorf("invalid planning session lifecycle state: %q", req.LifecycleState)
	}
	if !isValidPlanningSessionConversationState(req.ConversationState) {
		return fmt.Errorf("invalid planning session conversation state: %q", req.ConversationState)
	}
	if strings.TrimSpace(req.ProjectRoot) == "" {
		return fmt.Errorf("planning session project root is required")
	}
	if strings.TrimSpace(req.ModelSelectionJSON) == "" {
		return fmt.Errorf("planning session model selection json is required")
	}
	if strings.TrimSpace(req.CreatedAt) == "" || strings.TrimSpace(req.UpdatedAt) == "" {
		return fmt.Errorf("planning session timestamps are required")
	}
	return nil
}

func isValidPlanningSessionTitleState(value string) bool {
	switch strings.TrimSpace(value) {
	case "placeholder", "generated", "custom":
		return true
	default:
		return false
	}
}

func isValidPlanningSessionLifecycleState(state PlanningSessionLifecycleState) bool {
	switch state {
	case PlanningSessionLifecycleStateActive, PlanningSessionLifecycleStateArchived:
		return true
	default:
		return false
	}
}

func isValidPlanningSessionConversationState(state PlanningSessionConversationState) bool {
	switch state {
	case PlanningSessionConversationStateReady, PlanningSessionConversationStateProcessing:
		return true
	default:
		return false
	}
}

func trimOptionalString(value *string) *string {
	if value == nil {
		return nil
	}
	trimmed := strings.TrimSpace(*value)
	return &trimmed
}

func stringPtr(value string) *string {
	return &value
}

func nullableString(value string) *string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return nil
	}
	return &trimmed
}

func nullableStringFromSQL(value sql.NullString) *string {
	if !value.Valid {
		return nil
	}
	return nullableString(value.String)
}

func nullableStringValue(value *string) any {
	if value == nil || strings.TrimSpace(*value) == "" {
		return nil
	}
	return strings.TrimSpace(*value)
}

func normalizedNullableString(value *string) string {
	if value == nil {
		return ""
	}
	return strings.TrimSpace(*value)
}

func lifecycleStatePtrValue(value *PlanningSessionLifecycleState) *string {
	if value == nil {
		return nil
	}
	trimmed := strings.TrimSpace(string(*value))
	return &trimmed
}

func conversationStatePtrValue(value *PlanningSessionConversationState) *string {
	if value == nil {
		return nil
	}
	trimmed := strings.TrimSpace(string(*value))
	return &trimmed
}

func operationStatusPtrValue(value *PlanningOperationStatus) *string {
	if value == nil {
		return nil
	}
	trimmed := strings.TrimSpace(string(*value))
	return &trimmed
}

func intPtrValue(value *int) *int {
	if value == nil {
		return nil
	}
	copyValue := *value
	return &copyValue
}

func validatePlanningMessageAppend(req AppendPlanningMessageRequest) error {
	if strings.TrimSpace(req.PlanningMessageID) == "" {
		return fmt.Errorf("planning message id is required")
	}
	if strings.TrimSpace(req.PlanningSessionID) == "" {
		return fmt.Errorf("planning session id is required")
	}
	if !isValidPlanningMessageRole(req.Role) {
		return fmt.Errorf("invalid planning message role: %q", req.Role)
	}
	if strings.TrimSpace(req.ContentMarkdown) == "" && !allowsEmptyPlanningMessageContent(req.Role, req.QuestionPayloadJSON) {
		return fmt.Errorf("planning message content is required")
	}
	if strings.TrimSpace(req.QuestionPayloadJSON) == "" || strings.TrimSpace(req.AnswerPayloadJSON) == "" {
		return fmt.Errorf("planning message payload json fields are required")
	}
	if strings.TrimSpace(req.CreatedAt) == "" {
		return fmt.Errorf("planning message created_at is required")
	}
	return nil
}

func allowsEmptyPlanningMessageContent(role PlanningMessageRole, questionPayloadJSON string) bool {
	return role == PlanningMessageRoleAssistant && hasPlanningPayload(questionPayloadJSON)
}

func hasPlanningPayload(raw string) bool {
	trimmed := strings.TrimSpace(raw)
	return trimmed != "" && trimmed != "{}"
}

func validatePlanningDraftCreate(req CreatePlanningDraftRequest) error {
	if strings.TrimSpace(req.PlanningDraftID) == "" {
		return fmt.Errorf("planning draft id is required")
	}
	if strings.TrimSpace(req.PlanningSessionID) == "" {
		return fmt.Errorf("planning session id is required")
	}
	if strings.TrimSpace(req.DecreeTitle) == "" {
		return fmt.Errorf("planning draft decree title is required")
	}
	if strings.TrimSpace(req.DecreeText) == "" {
		return fmt.Errorf("planning draft decree text is required")
	}
	if strings.TrimSpace(req.SummaryJSON) == "" {
		return fmt.Errorf("planning draft summary json is required")
	}
	if strings.TrimSpace(req.CreatedAt) == "" {
		return fmt.Errorf("planning draft created_at is required")
	}
	return nil
}

func validatePlanningContextSnapshotCreate(req CreatePlanningContextSnapshotRequest) error {
	if strings.TrimSpace(req.PlanningContextSnapshotID) == "" {
		return fmt.Errorf("planning context snapshot id is required")
	}
	if strings.TrimSpace(req.PlanningSessionID) == "" {
		return fmt.Errorf("planning session id is required")
	}
	if strings.TrimSpace(req.SummaryMarkdown) == "" {
		return fmt.Errorf("planning context summary is required")
	}
	if len(req.SummaryMarkdown) > MaxPlanningContextSnapshotSummaryBytes {
		return fmt.Errorf("planning context summary exceeds %d bytes", MaxPlanningContextSnapshotSummaryBytes)
	}
	if strings.TrimSpace(req.IncludedFilesJSON) == "" || strings.TrimSpace(req.ExcludedFilesJSON) == "" {
		return fmt.Errorf("planning context file json fields are required")
	}
	if err := validatePlanningContextSnapshotFilesJSON("included", req.IncludedFilesJSON); err != nil {
		return err
	}
	if err := validatePlanningContextSnapshotFilesJSON("excluded", req.ExcludedFilesJSON); err != nil {
		return err
	}
	if strings.TrimSpace(req.CreatedAt) == "" {
		return fmt.Errorf("planning context created_at is required")
	}
	return nil
}

type planningContextSnapshotFileEntry struct {
	Path   string `json:"path"`
	Reason string `json:"reason,omitempty"`
	Size   int64  `json:"size_bytes,omitempty"`
}

func validatePlanningContextSnapshotFilesJSON(kind, raw string) error {
	trimmed := strings.TrimSpace(raw)
	if len(trimmed) > MaxPlanningContextSnapshotJSONBytes {
		return fmt.Errorf("%s planning context file json exceeds %d bytes", kind, MaxPlanningContextSnapshotJSONBytes)
	}
	if !strings.HasPrefix(trimmed, "[") {
		return fmt.Errorf("%s planning context file json must be an array", kind)
	}
	var entries []planningContextSnapshotFileEntry
	if err := json.Unmarshal([]byte(trimmed), &entries); err != nil {
		return fmt.Errorf("decode %s planning context file json: %w", kind, err)
	}
	if len(entries) > MaxPlanningContextSnapshotEntries {
		return fmt.Errorf("%s planning context files exceed %d entries", kind, MaxPlanningContextSnapshotEntries)
	}
	for _, entry := range entries {
		if len(entry.Path) == 0 || len(entry.Path) > MaxPlanningContextSnapshotPathBytes {
			return fmt.Errorf("%s planning context file path exceeds %d bytes", kind, MaxPlanningContextSnapshotPathBytes)
		}
		if len(entry.Reason) > MaxPlanningContextSnapshotReasonBytes {
			return fmt.Errorf("%s planning context file reason exceeds %d bytes", kind, MaxPlanningContextSnapshotReasonBytes)
		}
		if entry.Size < 0 {
			return fmt.Errorf("%s planning context file size must not be negative", kind)
		}
	}
	return nil
}

func validatePlanningOperationCreate(req CreatePlanningOperationRequest) error {
	if strings.TrimSpace(req.OperationID) == "" {
		return fmt.Errorf("planning operation id is required")
	}
	if strings.TrimSpace(req.PlanningSessionID) == "" {
		return fmt.Errorf("planning session id is required")
	}
	if !isValidPlanningOperationType(req.Type) {
		return fmt.Errorf("invalid planning operation type: %q", req.Type)
	}
	if !isValidPlanningOperationStatus(req.Status) {
		return fmt.Errorf("invalid planning operation status: %q", req.Status)
	}
	if strings.TrimSpace(req.RequestPayloadJSON) == "" {
		return fmt.Errorf("planning operation request payload json is required")
	}
	if strings.TrimSpace(req.CreatedAt) == "" {
		return fmt.Errorf("planning operation created_at is required")
	}
	return nil
}

func validatePlanningOperationEventAppend(req AppendPlanningOperationEventRequest) error {
	if strings.TrimSpace(req.EventID) == "" {
		return fmt.Errorf("planning operation event id is required")
	}
	if strings.TrimSpace(req.OperationID) == "" {
		return fmt.Errorf("planning operation id is required")
	}
	if req.Seq < 1 {
		return fmt.Errorf("planning operation event seq must be >= 1")
	}
	if strings.TrimSpace(req.EventType) == "" {
		return fmt.Errorf("planning operation event type is required")
	}
	if strings.TrimSpace(req.PayloadJSON) == "" {
		return fmt.Errorf("planning operation event payload json is required")
	}
	if strings.TrimSpace(req.CreatedAt) == "" {
		return fmt.Errorf("planning operation event created_at is required")
	}
	return nil
}

func isValidPlanningOperationType(operationType PlanningOperationType) bool {
	switch operationType {
	case PlanningOperationTypeReply, PlanningOperationTypeFormalize, PlanningOperationTypeRegenerate, PlanningOperationTypeGenerateTitle:
		return true
	default:
		return false
	}
}

func isValidPlanningOperationStatus(status PlanningOperationStatus) bool {
	switch status {
	case PlanningOperationStatusQueued, PlanningOperationStatusRunning, PlanningOperationStatusSucceeded, PlanningOperationStatusFailed, PlanningOperationStatusCancelled:
		return true
	default:
		return false
	}
}

func isValidPlanningMessageRole(role PlanningMessageRole) bool {
	switch role {
	case PlanningMessageRoleUser, PlanningMessageRoleAssistant:
		return true
	default:
		return false
	}
}
