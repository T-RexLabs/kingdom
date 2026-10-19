package state

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"kingdom/internal/core/providerexecution"
	"kingdom/internal/execution/providerrequest"
	"kingdom/internal/sqlitex"
	"kingdom/internal/timestamporder"
)

type PlanningProviderInvocationState string

const (
	PlanningProviderInvocationStateIssued           PlanningProviderInvocationState = "issued"
	PlanningProviderInvocationStateOpen             PlanningProviderInvocationState = "open"
	PlanningProviderInvocationStateSettledSuccess   PlanningProviderInvocationState = "settled_success"
	PlanningProviderInvocationStateSettledFailed    PlanningProviderInvocationState = "settled_failed"
	PlanningProviderInvocationStateSettledCancelled PlanningProviderInvocationState = "settled_cancelled"
	PlanningProviderInvocationStateSuperseded       PlanningProviderInvocationState = "superseded"
)

type PlanningProviderCompletionMode string

const PlanningProviderCompletionModeServerResponse PlanningProviderCompletionMode = "server_response"

type PlanningProviderCompletionEvidenceKind string

const (
	PlanningProviderCompletionEvidenceKindStreamObservation PlanningProviderCompletionEvidenceKind = "stream_observation"
	PlanningProviderCompletionEvidenceKindExportDiagnostic  PlanningProviderCompletionEvidenceKind = "export_diagnostic"
)

type PlanningProviderCompletionOutcome string

const (
	PlanningProviderCompletionOutcomeSuccess   PlanningProviderCompletionOutcome = "success"
	PlanningProviderCompletionOutcomeFailed    PlanningProviderCompletionOutcome = "failed"
	PlanningProviderCompletionOutcomeCancelled PlanningProviderCompletionOutcome = "cancelled"
)

type PlanningProviderInvocation struct {
	PlanningProviderInvocationID string
	PlanningOperationID          string
	PlanningSessionID            string
	LogicalSessionID             string
	ProviderSessionID            string
	InvocationState              PlanningProviderInvocationState
	CompletionMode               PlanningProviderCompletionMode
	OpenedAt                     string
	SettledAt                    string
	FailureReasonCode            string
	FailureReasonText            string
	CreatedAt                    string
	UpdatedAt                    string
}

type PlanningProviderCompletionEvidence struct {
	PlanningProviderEvidenceSeq  int64
	EvidenceID                   string
	PlanningProviderInvocationID string
	PlanningOperationID          string
	PlanningSessionID            string
	EvidenceKind                 PlanningProviderCompletionEvidenceKind
	PayloadJSON                  string
	CreatedAt                    string
}

type PlanningProviderCompletionSettlement struct {
	FinalTextRef                 *providerexecution.ContentReference
	SettlementID                 string
	PlanningProviderInvocationID string
	PlanningOperationID          string
	PlanningSessionID            string
	Outcome                      PlanningProviderCompletionOutcome
	PrimaryEvidenceID            string
	FinalText                    string
	StructuredPayloadJSON        string
	FailureReasonCode            string
	FailureReasonText            string
	SettledAt                    string
	CreatedAt                    string
}

type CreatePlanningProviderInvocationRequest struct {
	PlanningProviderInvocationID string
	PlanningOperationID          string
	PlanningSessionID            string
	LogicalSessionID             string
	CreatedAt                    string
	LeaseOwner                   string
	LeaseVersion                 int
}

type MarkPlanningProviderInvocationOpenRequest struct {
	PlanningProviderInvocationID string
	PlanningOperationID          string
	ProviderSessionID            string
	OpenedAt                     string
	LeaseOwner                   string
	LeaseVersion                 int
}

type ActivatePlanningProviderInvocationRequest struct {
	RequestIdentity              *providerexecution.RequestIdentity
	PlanningProviderInvocationID string
	PlanningOperationID          string
	PlanningSessionID            string
	LogicalSessionID             string
	ProviderSessionID            string
	CreatedAt                    string
	OpenedAt                     string
	LeaseOwner                   string
	LeaseVersion                 int
}

type FinalizePlanningProviderSettlementRequest struct {
	FinalTextRef                 *providerexecution.ContentReference
	StagedContent                []providerexecution.StagedContent `json:"-"`
	SettlementID                 string
	PlanningProviderInvocationID string
	PrimaryEvidenceID            string
	PrimaryEvidenceKind          PlanningProviderCompletionEvidenceKind
	PrimaryEvidencePayloadJSON   string
	PrimaryEvidenceCreatedAt     string
	Outcome                      PlanningProviderCompletionOutcome
	FinalText                    string
	StructuredPayloadJSON        string
	FailureReasonCode            string
	FailureReasonText            string
	SettledAt                    string
	RequiresProcessGuard         bool
}

type planningProviderSettlementError struct {
	Operation                    string
	Reason                       string
	PlanningProviderInvocationID string
	PlanningOperationID          string
	State                        string
}

func (e *planningProviderSettlementError) Error() string {
	if e == nil {
		return "planning provider settlement error"
	}
	message := strings.TrimSpace(e.Operation)
	if reason := strings.TrimSpace(e.Reason); reason != "" {
		if message != "" {
			message += ": "
		}
		message += reason
	}
	if message == "" {
		message = "planning provider settlement error"
	}
	if operationID := strings.TrimSpace(e.PlanningOperationID); operationID != "" {
		message += fmt.Sprintf(" (operation=%q", operationID)
		if invocationID := strings.TrimSpace(e.PlanningProviderInvocationID); invocationID != "" {
			message += fmt.Sprintf(", invocation=%q", invocationID)
		}
		if state := strings.TrimSpace(e.State); state != "" {
			message += fmt.Sprintf(", state=%q", state)
		}
		message += ")"
	}
	return message
}

func (s *PlanningStore) CreatePlanningProviderInvocation(ctx context.Context, req CreatePlanningProviderInvocationRequest) (PlanningProviderInvocation, error) {
	if s == nil || s.db == nil {
		return PlanningProviderInvocation{}, fmt.Errorf("planning store database is required")
	}
	normalized, err := normalizeCreatePlanningProviderInvocationRequest(req)
	if err != nil {
		return PlanningProviderInvocation{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return PlanningProviderInvocation{}, fmt.Errorf("begin create planning provider invocation: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	invocation, err := createPlanningProviderInvocationTx(ctx, tx, normalized)
	if err != nil {
		return PlanningProviderInvocation{}, err
	}
	if _, bound, err := providerrequest.ForOwner(ctx, tx, providerrequest.Owner{Kind: providerrequest.OwnerPlanning, InvocationID: invocation.PlanningProviderInvocationID}); err != nil {
		return PlanningProviderInvocation{}, err
	} else if bound {
		return PlanningProviderInvocation{}, fmt.Errorf("bound planning invocation requires atomic request activation")
	}
	query := fmt.Sprintf(`UPDATE planning_operations
		SET active_provider_invocation_id = CASE
			WHEN COALESCE(TRIM(active_provider_invocation_id), '') = '' THEN ?
			ELSE active_provider_invocation_id
		END
		WHERE operation_id = ?
		  AND planning_session_id = ?
		  AND (COALESCE(TRIM(active_provider_invocation_id), '') = '' OR active_provider_invocation_id = ?)
		  AND status = 'running'
		  AND lease_owner = ?
		  AND lease_version = ?
		  AND %s`, timestamporder.ComparisonSQL("lease_expires_at", "?", ">"))
	result, err := tx.ExecContext(ctx, query,
		invocation.PlanningProviderInvocationID,
		invocation.PlanningOperationID,
		invocation.PlanningSessionID,
		invocation.PlanningProviderInvocationID,
		normalized.LeaseOwner,
		normalized.LeaseVersion,
		normalized.CreatedAt,
	)
	if err != nil {
		return PlanningProviderInvocation{}, fmt.Errorf("link planning operation %q active invocation: %w", invocation.PlanningOperationID, err)
	}
	linkedRows, err := result.RowsAffected()
	if err != nil {
		return PlanningProviderInvocation{}, fmt.Errorf("read planning operation active invocation rows affected for %q: %w", invocation.PlanningOperationID, err)
	}
	if linkedRows == 0 {
		return PlanningProviderInvocation{}, planningProviderError("create_planning_provider_invocation", "active_invocation_conflict", invocation.PlanningProviderInvocationID, invocation.PlanningOperationID, string(invocation.InvocationState))
	}
	if err := tx.Commit(); err != nil {
		return PlanningProviderInvocation{}, fmt.Errorf("commit create planning provider invocation: %w", err)
	}
	committed = true
	return invocation, nil
}

func (s *PlanningStore) ActivatePlanningProviderInvocation(ctx context.Context, req ActivatePlanningProviderInvocationRequest) (PlanningProviderInvocation, error) {
	if s == nil || s.db == nil {
		return PlanningProviderInvocation{}, fmt.Errorf("planning store database is required")
	}
	createReq, openReq, err := normalizeActivatePlanningProviderInvocationRequest(req)
	if err != nil {
		return PlanningProviderInvocation{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return PlanningProviderInvocation{}, fmt.Errorf("begin activate planning provider invocation: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	invocation, err := createPlanningProviderInvocationTx(ctx, tx, createReq)
	if err != nil {
		return PlanningProviderInvocation{}, err
	}
	owner := providerrequest.Owner{Kind: providerrequest.OwnerPlanning, InvocationID: invocation.PlanningProviderInvocationID}
	binding, bound, err := providerrequest.ForOwner(ctx, tx, owner)
	if err != nil {
		return PlanningProviderInvocation{}, err
	}
	if req.RequestIdentity == nil {
		if bound {
			return PlanningProviderInvocation{}, fmt.Errorf("bound planning invocation requires its exact provider request identity")
		}
		if err := requireUnboundPlanningProviderSessionTx(ctx, tx, openReq.ProviderSessionID); err != nil {
			return PlanningProviderInvocation{}, err
		}
	} else {
		if err := req.RequestIdentity.Validate(); err != nil {
			return PlanningProviderInvocation{}, err
		}
		if req.RequestIdentity.SessionID != openReq.ProviderSessionID {
			return PlanningProviderInvocation{}, fmt.Errorf("planning provider request session differs from activation")
		}
		if !bound && invocation.InvocationState == PlanningProviderInvocationStateOpen {
			return PlanningProviderInvocation{}, fmt.Errorf("cannot attach provider request ownership to an open planning invocation")
		}
		if bound && (binding.Identity != *req.RequestIdentity || binding.Lease != (providerrequest.Lease{Owner: openReq.LeaseOwner, Version: int64(openReq.LeaseVersion)})) {
			return PlanningProviderInvocation{}, fmt.Errorf("planning provider request identity or lease conflicts with activation")
		}
	}
	if err := activatePlanningProviderInvocationTx(ctx, tx, invocation, openReq); err != nil {
		return PlanningProviderInvocation{}, err
	}
	if req.RequestIdentity != nil {
		if err := providerrequest.BindTx(ctx, tx, providerrequest.Binding{
			ID: invocation.PlanningProviderInvocationID, Identity: *req.RequestIdentity, Owner: owner,
			Lease: providerrequest.Lease{Owner: openReq.LeaseOwner, Version: int64(openReq.LeaseVersion)},
		}, s.leaseNow()); err != nil {
			return PlanningProviderInvocation{}, err
		}
	}
	invocation, found, err := loadPlanningProviderInvocationTx(ctx, tx, invocation.PlanningProviderInvocationID)
	if err != nil {
		return PlanningProviderInvocation{}, err
	}
	if !found {
		return PlanningProviderInvocation{}, planningProviderError("activate_planning_provider_invocation", "invocation_not_found", createReq.PlanningProviderInvocationID, createReq.PlanningOperationID, "")
	}
	if err := tx.Commit(); err != nil {
		return PlanningProviderInvocation{}, fmt.Errorf("commit activate planning provider invocation: %w", err)
	}
	committed = true
	return invocation, nil
}

func (s *PlanningStore) MarkPlanningProviderInvocationOpen(ctx context.Context, req MarkPlanningProviderInvocationOpenRequest) (PlanningProviderInvocation, error) {
	if s == nil || s.db == nil {
		return PlanningProviderInvocation{}, fmt.Errorf("planning store database is required")
	}
	normalized, err := normalizeMarkPlanningProviderInvocationOpenRequest(req)
	if err != nil {
		return PlanningProviderInvocation{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return PlanningProviderInvocation{}, fmt.Errorf("begin mark planning provider invocation open: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	invocation, found, err := loadPlanningProviderInvocationTx(ctx, tx, normalized.PlanningProviderInvocationID)
	if err != nil {
		return PlanningProviderInvocation{}, err
	}
	if !found {
		return PlanningProviderInvocation{}, planningProviderError("mark_planning_provider_invocation_open", "invocation_not_found", normalized.PlanningProviderInvocationID, normalized.PlanningOperationID, "")
	}
	if invocation.PlanningOperationID != normalized.PlanningOperationID {
		return PlanningProviderInvocation{}, planningProviderError("mark_planning_provider_invocation_open", "operation_lineage_mismatch", invocation.PlanningProviderInvocationID, normalized.PlanningOperationID, string(invocation.InvocationState))
	}
	if err := ensurePlanningProviderInvocationStateForOpen(invocation); err != nil {
		return PlanningProviderInvocation{}, err
	}
	if err := ensurePlanningProviderSessionLineage(invocation.PlanningProviderInvocationID, invocation.PlanningOperationID, invocation.ProviderSessionID, normalized.ProviderSessionID, string(invocation.InvocationState), "mark_planning_provider_invocation_open"); err != nil {
		return PlanningProviderInvocation{}, err
	}
	if _, bound, err := providerrequest.ForOwner(ctx, tx, providerrequest.Owner{Kind: providerrequest.OwnerPlanning, InvocationID: invocation.PlanningProviderInvocationID}); err != nil {
		return PlanningProviderInvocation{}, err
	} else if bound {
		return PlanningProviderInvocation{}, fmt.Errorf("bound planning provider request requires atomic activation")
	}
	if err := requireUnboundPlanningProviderSessionTx(ctx, tx, normalized.ProviderSessionID); err != nil {
		return PlanningProviderInvocation{}, err
	}
	if err := fencePlanningOperationProviderSessionTx(ctx, tx, invocation, normalized); err != nil {
		return PlanningProviderInvocation{}, err
	}
	if invocation.InvocationState != PlanningProviderInvocationStateOpen || invocation.OpenedAt == "" {
		result, err := tx.ExecContext(ctx, `UPDATE planning_provider_invocations
			SET invocation_state = 'open',
			    provider_session_id = CASE WHEN TRIM(?) <> '' THEN TRIM(?) ELSE provider_session_id END,
			    opened_at = COALESCE(opened_at, ?),
			    updated_at = ?
			WHERE planning_provider_invocation_id = ? AND invocation_state = 'issued'`,
			normalized.ProviderSessionID,
			normalized.ProviderSessionID,
			normalized.OpenedAt,
			normalized.OpenedAt,
			invocation.PlanningProviderInvocationID,
		)
		if err != nil {
			return PlanningProviderInvocation{}, fmt.Errorf("mark planning provider invocation %q open: %w", invocation.PlanningProviderInvocationID, err)
		}
		if affected, err := result.RowsAffected(); err != nil {
			return PlanningProviderInvocation{}, fmt.Errorf("read planning provider invocation %q open rows affected: %w", invocation.PlanningProviderInvocationID, err)
		} else if affected != 1 {
			return PlanningProviderInvocation{}, planningProviderError("mark_planning_provider_invocation_open", "active_invocation_conflict", invocation.PlanningProviderInvocationID, invocation.PlanningOperationID, string(invocation.InvocationState))
		}
	}
	invocation, found, err = loadPlanningProviderInvocationTx(ctx, tx, invocation.PlanningProviderInvocationID)
	if err != nil {
		return PlanningProviderInvocation{}, err
	}
	if !found {
		return PlanningProviderInvocation{}, planningProviderError("mark_planning_provider_invocation_open", "invocation_not_found", normalized.PlanningProviderInvocationID, normalized.PlanningOperationID, "")
	}
	if err := tx.Commit(); err != nil {
		return PlanningProviderInvocation{}, fmt.Errorf("commit mark planning provider invocation open: %w", err)
	}
	committed = true
	return invocation, nil
}

func (s *PlanningStore) GetPlanningProviderInvocation(ctx context.Context, planningProviderInvocationID string) (PlanningProviderInvocation, bool, error) {
	if s == nil || s.db == nil {
		return PlanningProviderInvocation{}, false, fmt.Errorf("planning store database is required")
	}
	return getPlanningProviderInvocation(ctx, s.db, planningProviderInvocationID)
}

func (s *PlanningStore) RequirePlanningProviderProcessGuard(ctx context.Context, invocationID, operationID string) error {
	result, err := s.db.ExecContext(ctx, `UPDATE planning_provider_invocations SET process_guard_required = 1 WHERE planning_provider_invocation_id = ? AND planning_operation_id = ? AND invocation_state IN ('issued','open')`, strings.TrimSpace(invocationID), strings.TrimSpace(operationID))
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows != 1 {
		return fmt.Errorf("require planning provider process guard: invocation authority conflict")
	}
	return nil
}

func (s *PlanningStore) GetLatestReusablePlanningProviderInvocation(ctx context.Context, logicalSessionID string) (PlanningProviderInvocation, bool, error) {
	if s == nil || s.db == nil {
		return PlanningProviderInvocation{}, false, fmt.Errorf("planning store database is required")
	}
	trimmedLogicalSessionID := strings.TrimSpace(logicalSessionID)
	if trimmedLogicalSessionID == "" {
		return PlanningProviderInvocation{}, false, fmt.Errorf("logical_session_id is required")
	}
	row := s.db.QueryRowContext(ctx, planningProviderInvocationSelect+`
		 WHERE logical_session_id = ?
		   AND TRIM(provider_session_id) <> ''
		 ORDER BY created_at DESC, planning_provider_invocation_id DESC
		 LIMIT 1`,
		trimmedLogicalSessionID,
	)
	invocation, found, err := scanPlanningProviderInvocation(row)
	if err != nil {
		return PlanningProviderInvocation{}, false, fmt.Errorf("get latest reusable planning provider invocation %q: %w", trimmedLogicalSessionID, err)
	}
	// The conversation continues only from a request that settled successfully.
	// Any later request, including a cancelled or failed one, may have changed
	// the provider's history in ways the canonical transcript does not show.
	if !found || invocation.InvocationState != PlanningProviderInvocationStateSettledSuccess {
		return PlanningProviderInvocation{}, false, nil
	}
	return invocation, true, nil
}

func (s *PlanningStore) GetPlanningProviderCompletionSettlement(ctx context.Context, settlementID string) (PlanningProviderCompletionSettlement, bool, error) {
	if s == nil || s.db == nil {
		return PlanningProviderCompletionSettlement{}, false, fmt.Errorf("planning store database is required")
	}
	return getPlanningProviderCompletionSettlement(ctx, s.db, settlementID)
}

func finalizePlanningProviderSettlementTx(ctx context.Context, tx *sql.Tx, operationID, sessionID string, req *FinalizePlanningProviderSettlementRequest) (string, error) {
	if req == nil {
		return "", nil
	}
	normalized, err := normalizeFinalizePlanningProviderSettlementRequest(*req)
	if err != nil {
		return "", err
	}
	invocation, found, err := loadPlanningProviderInvocationTx(ctx, tx, normalized.PlanningProviderInvocationID)
	if err != nil {
		return "", err
	}
	if !found {
		return "", planningProviderError("finalize_planning_provider_settlement", "invocation_not_found", normalized.PlanningProviderInvocationID, operationID, "")
	}
	if invocation.PlanningOperationID != strings.TrimSpace(operationID) || invocation.PlanningSessionID != strings.TrimSpace(sessionID) {
		return "", planningProviderError("finalize_planning_provider_settlement", "operation_lineage_mismatch", invocation.PlanningProviderInvocationID, operationID, string(invocation.InvocationState))
	}
	observation, observationErr := planningProviderTerminalStreamObservation(normalized.PrimaryEvidencePayloadJSON)
	owner := providerrequest.Owner{Kind: providerrequest.OwnerPlanning, InvocationID: invocation.PlanningProviderInvocationID}
	binding, requestBound, err := providerrequest.ForOwner(ctx, tx, owner)
	if err != nil {
		return "", err
	}
	if requestBound {
		if observationErr != nil {
			return "", fmt.Errorf("bound planning provider request requires terminal observation")
		}
		if err := providerrequest.RequireCompletionTx(ctx, tx, owner, binding.Lease, observation, string(normalized.Outcome)); err != nil {
			return "", err
		}
	} else if observation.Request != nil {
		return "", fmt.Errorf("unbound planning invocation cannot claim provider request completion")
	}
	if observationErr != nil && (normalized.Outcome == PlanningProviderCompletionOutcomeSuccess || len(normalized.StagedContent) > 0) {
		return "", planningProviderError("finalize_planning_provider_settlement", "invalid_stream_success", invocation.PlanningProviderInvocationID, invocation.PlanningOperationID, string(invocation.InvocationState))
	}
	if observationErr == nil {
		refs := append([]providerexecution.ContentReference(nil), observation.DiagnosticContent...)
		if observation.CanonicalTerminalTextRef != nil {
			refs = append(refs, *observation.CanonicalTerminalTextRef)
		}
		for _, ref := range refs {
			if err := importPlanningProviderContent(ctx, tx, invocation.PlanningProviderInvocationID, ref, normalized.StagedContent); err != nil {
				return "", err
			}
		}
		if observation.CanonicalTerminalTextRef != nil {
			ref := *observation.CanonicalTerminalTextRef
			observation.TerminalTextOpener = func(context.Context) (io.ReadCloser, error) {
				return providerexecution.OpenContentCopy(ctx, func(ctx context.Context, out io.Writer) error { return copyPlanningProviderContent(ctx, tx, ref, out) }), nil
			}
		}
	}
	if normalized.Outcome == PlanningProviderCompletionOutcomeSuccess {
		if invocation.InvocationState != PlanningProviderInvocationStateOpen || invocation.OpenedAt == "" {
			return "", planningProviderError("finalize_planning_provider_settlement", "success_requires_open_active_invocation", invocation.PlanningProviderInvocationID, invocation.PlanningOperationID, string(invocation.InvocationState))
		}
		if normalized.FinalText == "" && normalized.FinalTextRef == nil {
			return "", planningProviderError("finalize_planning_provider_settlement", "success_requires_final_text", invocation.PlanningProviderInvocationID, invocation.PlanningOperationID, string(invocation.InvocationState))
		}
		var activeInvocationID string
		if err := tx.QueryRowContext(ctx, `SELECT COALESCE(active_provider_invocation_id, '') FROM planning_operations WHERE operation_id = ? AND planning_session_id = ? AND status = 'running'`, invocation.PlanningOperationID, invocation.PlanningSessionID).Scan(&activeInvocationID); err != nil {
			if err == sql.ErrNoRows {
				return "", planningProviderError("finalize_planning_provider_settlement", "success_requires_open_active_invocation", invocation.PlanningProviderInvocationID, invocation.PlanningOperationID, string(invocation.InvocationState))
			}
			return "", err
		}
		if strings.TrimSpace(activeInvocationID) != invocation.PlanningProviderInvocationID {
			return "", planningProviderError("finalize_planning_provider_settlement", "success_requires_open_active_invocation", invocation.PlanningProviderInvocationID, invocation.PlanningOperationID, string(invocation.InvocationState))
		}
		textMatches := strings.TrimSpace(observation.CanonicalTerminalText) == normalized.FinalText
		if observation.CanonicalTerminalTextRef != nil {
			if normalized.FinalTextRef != nil {
				textMatches = *normalized.FinalTextRef == *observation.CanonicalTerminalTextRef
			} else {
				textMatches = providerexecution.ReferenceText(invocation.PlanningProviderInvocationID, normalized.FinalText) == *observation.CanonicalTerminalTextRef
			}
		}
		if observationErr != nil || providerexecution.ValidateServerResponseSuccess(observation) != nil || !textMatches {
			return "", planningProviderError("finalize_planning_provider_settlement", "invalid_stream_success", invocation.PlanningProviderInvocationID, invocation.PlanningOperationID, string(invocation.InvocationState))
		}
	}
	var invocationGuardRequired bool
	if err := tx.QueryRowContext(ctx, `SELECT process_guard_required FROM planning_provider_invocations WHERE planning_provider_invocation_id = ?`, invocation.PlanningProviderInvocationID).Scan(&invocationGuardRequired); err != nil {
		return "", err
	}
	var guardTotal, guardUnresolved int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(1), COALESCE(SUM(CASE WHEN lifecycle <> 'quiescent' THEN 1 ELSE 0 END), 0) FROM provider_process_guards WHERE owner_kind = 'planning_operation' AND provider_invocation_id = ? AND planning_operation_id = ?`, invocation.PlanningProviderInvocationID, invocation.PlanningOperationID).Scan(&guardTotal, &guardUnresolved); err != nil {
		return "", err
	}
	if guardUnresolved != 0 || (!requestBound && (invocationGuardRequired || normalized.RequiresProcessGuard) && guardTotal == 0 && normalized.Outcome == PlanningProviderCompletionOutcomeSuccess) {
		return "", planningProviderError("finalize_planning_provider_settlement", "provider_process_guard_not_quiescent", invocation.PlanningProviderInvocationID, invocation.PlanningOperationID, string(invocation.InvocationState))
	}
	primaryEvidenceID := normalized.PrimaryEvidenceID
	if normalized.PrimaryEvidencePayloadJSON != "" {
		if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO planning_provider_completion_evidence (
			evidence_id,
			planning_provider_invocation_id,
			planning_operation_id,
			planning_session_id,
			evidence_kind,
			payload_json,
			created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?)`,
			normalized.PrimaryEvidenceID,
			invocation.PlanningProviderInvocationID,
			invocation.PlanningOperationID,
			invocation.PlanningSessionID,
			string(normalized.PrimaryEvidenceKind),
			normalized.PrimaryEvidencePayloadJSON,
			normalized.PrimaryEvidenceCreatedAt,
		); err != nil {
			return "", fmt.Errorf("insert planning provider settlement evidence %q: %w", normalized.PrimaryEvidenceID, err)
		}
	}
	evidence, evidenceFound, err := loadPlanningProviderCompletionEvidenceTx(ctx, tx, primaryEvidenceID)
	if err != nil {
		return "", err
	}
	if !evidenceFound {
		return "", planningProviderError("finalize_planning_provider_settlement", "primary_evidence_not_found", invocation.PlanningProviderInvocationID, invocation.PlanningOperationID, string(invocation.InvocationState))
	}
	if evidence.PlanningProviderInvocationID != invocation.PlanningProviderInvocationID {
		return "", planningProviderError("finalize_planning_provider_settlement", "primary_evidence_invocation_mismatch", invocation.PlanningProviderInvocationID, invocation.PlanningOperationID, string(invocation.InvocationState))
	}
	candidateSettlement := PlanningProviderCompletionSettlement{
		FinalTextRef:                 normalized.FinalTextRef,
		SettlementID:                 normalized.SettlementID,
		PlanningProviderInvocationID: invocation.PlanningProviderInvocationID,
		PlanningOperationID:          invocation.PlanningOperationID,
		PlanningSessionID:            invocation.PlanningSessionID,
		Outcome:                      normalized.Outcome,
		PrimaryEvidenceID:            primaryEvidenceID,
		FinalText:                    normalized.FinalText,
		StructuredPayloadJSON:        normalized.StructuredPayloadJSON,
		FailureReasonCode:            normalized.FailureReasonCode,
		FailureReasonText:            normalized.FailureReasonText,
		SettledAt:                    normalized.SettledAt,
		CreatedAt:                    normalized.SettledAt,
	}
	if normalized.Outcome == PlanningProviderCompletionOutcomeSuccess {
		// Planner success is settled server-side from the provider's terminal
		// stop signal. The durable proof is the runtime-owned stream observation
		// and its assembled final text.
		if evidence.EvidenceKind != PlanningProviderCompletionEvidenceKindStreamObservation {
			return "", planningProviderError("finalize_planning_provider_settlement", "success_requires_stream_observation_evidence", invocation.PlanningProviderInvocationID, invocation.PlanningOperationID, string(invocation.InvocationState))
		}
		if candidateSettlement.FinalText == "" && candidateSettlement.FinalTextRef == nil {
			return "", planningProviderError("finalize_planning_provider_settlement", "success_requires_final_text", invocation.PlanningProviderInvocationID, invocation.PlanningOperationID, string(invocation.InvocationState))
		}
		candidateSettlement.FailureReasonCode = ""
		candidateSettlement.FailureReasonText = ""
	}
	res, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO planning_provider_completion_settlements (
		settlement_id,
		planning_provider_invocation_id,
		planning_operation_id,
		planning_session_id,
		outcome,
		primary_evidence_id,
		final_text,
		structured_payload_json,
		failure_reason_code,
		failure_reason_text,
		settled_at,
		created_at
		,final_text_ref_json
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?)`,
		candidateSettlement.SettlementID,
		candidateSettlement.PlanningProviderInvocationID,
		candidateSettlement.PlanningOperationID,
		candidateSettlement.PlanningSessionID,
		string(candidateSettlement.Outcome),
		candidateSettlement.PrimaryEvidenceID,
		candidateSettlement.FinalText,
		candidateSettlement.StructuredPayloadJSON,
		candidateSettlement.FailureReasonCode,
		candidateSettlement.FailureReasonText,
		candidateSettlement.SettledAt,
		candidateSettlement.CreatedAt,
		planningProviderReferenceJSON(candidateSettlement.FinalTextRef),
	)
	if err != nil {
		return "", fmt.Errorf("insert planning provider settlement %q: %w", normalized.SettlementID, err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return "", fmt.Errorf("read planning provider settlement rows affected for %q: %w", normalized.SettlementID, err)
	}
	settlement, found, err := loadPlanningProviderCompletionSettlementTx(ctx, tx, normalized.SettlementID)
	if err != nil {
		return "", err
	}
	if !found {
		return "", planningProviderError("finalize_planning_provider_settlement", "settlement_not_found", invocation.PlanningProviderInvocationID, invocation.PlanningOperationID, string(invocation.InvocationState))
	}
	if rows == 0 && !samePlanningProviderSettlement(settlement, candidateSettlement) {
		return "", planningProviderError("finalize_planning_provider_settlement", "settlement_conflict", invocation.PlanningProviderInvocationID, invocation.PlanningOperationID, string(invocation.InvocationState))
	}
	if _, err := tx.ExecContext(ctx, `UPDATE planning_provider_invocations
		SET invocation_state = ?, settled_at = ?, updated_at = ?, failure_reason_code = ?, failure_reason_text = ?
		WHERE planning_provider_invocation_id = ?`,
		string(planningProviderInvocationStateForOutcome(settlement.Outcome)),
		settlement.SettledAt,
		settlement.SettledAt,
		settlement.FailureReasonCode,
		settlement.FailureReasonText,
		invocation.PlanningProviderInvocationID,
	); err != nil {
		return "", fmt.Errorf("update planning provider invocation %q settlement state: %w", invocation.PlanningProviderInvocationID, err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE planning_operations
		SET canonical_settlement_id = CASE
			WHEN COALESCE(TRIM(canonical_settlement_id), '') = '' THEN ?
			ELSE canonical_settlement_id
		END
		WHERE operation_id = ?
		  AND (COALESCE(TRIM(canonical_settlement_id), '') = '' OR canonical_settlement_id = ?)`,
		settlement.SettlementID,
		invocation.PlanningOperationID,
		settlement.SettlementID,
	); err != nil {
		return "", fmt.Errorf("link planning operation %q canonical settlement: %w", invocation.PlanningOperationID, err)
	}
	return settlement.SettlementID, nil
}

func planningProviderTerminalStreamObservation(payloadJSON string) (providerexecution.InvocationStreamObservation, error) {
	var payload struct {
		StreamObservation *providerexecution.InvocationStreamObservation `json:"stream_observation"`
	}
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		return providerexecution.InvocationStreamObservation{}, err
	}
	if payload.StreamObservation == nil {
		return providerexecution.InvocationStreamObservation{}, fmt.Errorf("stream observation is required")
	}
	return *payload.StreamObservation, nil
}

func planningProviderError(operation, reason, invocationID, operationID, state string) error {
	return &planningProviderSettlementError{
		Operation:                    operation,
		Reason:                       reason,
		PlanningProviderInvocationID: invocationID,
		PlanningOperationID:          operationID,
		State:                        state,
	}
}

func normalizeCreatePlanningProviderInvocationRequest(req CreatePlanningProviderInvocationRequest) (CreatePlanningProviderInvocationRequest, error) {
	normalized := CreatePlanningProviderInvocationRequest{
		PlanningProviderInvocationID: strings.TrimSpace(req.PlanningProviderInvocationID),
		PlanningOperationID:          strings.TrimSpace(req.PlanningOperationID),
		PlanningSessionID:            strings.TrimSpace(req.PlanningSessionID),
		LogicalSessionID:             strings.TrimSpace(req.LogicalSessionID),
		CreatedAt:                    strings.TrimSpace(req.CreatedAt),
		LeaseOwner:                   strings.TrimSpace(req.LeaseOwner),
		LeaseVersion:                 req.LeaseVersion,
	}
	if normalized.PlanningProviderInvocationID == "" || normalized.PlanningOperationID == "" || normalized.PlanningSessionID == "" {
		return CreatePlanningProviderInvocationRequest{}, fmt.Errorf("planning provider invocation, operation, and session ids are required")
	}
	if normalized.CreatedAt == "" {
		return CreatePlanningProviderInvocationRequest{}, fmt.Errorf("planning provider invocation created_at is required")
	}
	if normalized.LeaseOwner == "" || normalized.LeaseVersion < 1 {
		return CreatePlanningProviderInvocationRequest{}, fmt.Errorf("planning provider invocation requires an active operation lease")
	}
	return normalized, nil
}

func normalizeMarkPlanningProviderInvocationOpenRequest(req MarkPlanningProviderInvocationOpenRequest) (MarkPlanningProviderInvocationOpenRequest, error) {
	normalized := MarkPlanningProviderInvocationOpenRequest{
		PlanningProviderInvocationID: strings.TrimSpace(req.PlanningProviderInvocationID),
		PlanningOperationID:          strings.TrimSpace(req.PlanningOperationID),
		ProviderSessionID:            strings.TrimSpace(req.ProviderSessionID),
		OpenedAt:                     strings.TrimSpace(req.OpenedAt),
		LeaseOwner:                   strings.TrimSpace(req.LeaseOwner),
		LeaseVersion:                 req.LeaseVersion,
	}
	if normalized.PlanningProviderInvocationID == "" || normalized.PlanningOperationID == "" {
		return MarkPlanningProviderInvocationOpenRequest{}, fmt.Errorf("planning provider invocation id and planning operation id are required")
	}
	if normalized.OpenedAt == "" {
		return MarkPlanningProviderInvocationOpenRequest{}, fmt.Errorf("planning provider invocation opened_at is required")
	}
	if normalized.LeaseOwner == "" || normalized.LeaseVersion < 1 {
		return MarkPlanningProviderInvocationOpenRequest{}, fmt.Errorf("planning provider invocation open requires an active operation lease")
	}
	return normalized, nil
}

func normalizeActivatePlanningProviderInvocationRequest(req ActivatePlanningProviderInvocationRequest) (CreatePlanningProviderInvocationRequest, MarkPlanningProviderInvocationOpenRequest, error) {
	createReq, err := normalizeCreatePlanningProviderInvocationRequest(CreatePlanningProviderInvocationRequest{
		PlanningProviderInvocationID: req.PlanningProviderInvocationID,
		PlanningOperationID:          req.PlanningOperationID,
		PlanningSessionID:            req.PlanningSessionID,
		LogicalSessionID:             req.LogicalSessionID,
		CreatedAt:                    req.CreatedAt,
		LeaseOwner:                   req.LeaseOwner,
		LeaseVersion:                 req.LeaseVersion,
	})
	if err != nil {
		return CreatePlanningProviderInvocationRequest{}, MarkPlanningProviderInvocationOpenRequest{}, err
	}
	openReq, err := normalizeMarkPlanningProviderInvocationOpenRequest(MarkPlanningProviderInvocationOpenRequest{
		PlanningProviderInvocationID: createReq.PlanningProviderInvocationID,
		PlanningOperationID:          createReq.PlanningOperationID,
		ProviderSessionID:            req.ProviderSessionID,
		OpenedAt:                     firstNonEmptyPlanningProvider(req.OpenedAt, createReq.CreatedAt),
		LeaseOwner:                   req.LeaseOwner,
		LeaseVersion:                 req.LeaseVersion,
	})
	if err != nil {
		return CreatePlanningProviderInvocationRequest{}, MarkPlanningProviderInvocationOpenRequest{}, err
	}
	return createReq, openReq, nil
}

func createPlanningProviderInvocationTx(ctx context.Context, tx *sql.Tx, req CreatePlanningProviderInvocationRequest) (PlanningProviderInvocation, error) {
	res, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO planning_provider_invocations (
		planning_provider_invocation_id,
		planning_operation_id,
		planning_session_id,
		logical_session_id,
		provider_session_id,
		invocation_state,
		completion_mode,
		created_at,
		updated_at
	) VALUES (?, ?, ?, ?, '', ?, ?, ?, ?)`,
		req.PlanningProviderInvocationID,
		req.PlanningOperationID,
		req.PlanningSessionID,
		req.LogicalSessionID,
		string(PlanningProviderInvocationStateIssued),
		string(PlanningProviderCompletionModeServerResponse),
		req.CreatedAt,
		req.CreatedAt,
	)
	if err != nil {
		return PlanningProviderInvocation{}, fmt.Errorf("create planning provider invocation %q: %w", req.PlanningProviderInvocationID, err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return PlanningProviderInvocation{}, fmt.Errorf("read planning provider invocation rows affected for %q: %w", req.PlanningProviderInvocationID, err)
	}
	invocation, found, err := loadPlanningProviderInvocationTx(ctx, tx, req.PlanningProviderInvocationID)
	if err != nil {
		return PlanningProviderInvocation{}, err
	}
	if !found {
		return PlanningProviderInvocation{}, planningProviderError("create_planning_provider_invocation", "invocation_not_found", req.PlanningProviderInvocationID, req.PlanningOperationID, "")
	}
	if rows == 0 && !samePlanningProviderInvocationCreate(invocation, req) {
		return PlanningProviderInvocation{}, planningProviderError("create_planning_provider_invocation", "invocation_identity_conflict", req.PlanningProviderInvocationID, req.PlanningOperationID, string(invocation.InvocationState))
	}
	return invocation, nil
}

// Low-level V1 session callbacks cannot open an invocation on a session that
// has canonical request ownership. That path must bind identity atomically.
func requireUnboundPlanningProviderSessionTx(ctx context.Context, tx *sql.Tx, sessionID string) error {
	if sessionID == "" {
		return nil
	}
	var count int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM provider_requests WHERE provider_session_id=?`, sessionID).Scan(&count); err != nil {
		return err
	}
	if count != 0 {
		return fmt.Errorf("planning provider session requires atomic request ownership")
	}
	return nil
}

func activatePlanningProviderInvocationTx(ctx context.Context, tx *sql.Tx, invocation PlanningProviderInvocation, openReq MarkPlanningProviderInvocationOpenRequest) error {
	if err := ensurePlanningProviderInvocationStateForOpen(invocation); err != nil {
		return err
	}
	if err := ensurePlanningProviderSessionLineage(invocation.PlanningProviderInvocationID, invocation.PlanningOperationID, invocation.ProviderSessionID, openReq.ProviderSessionID, string(invocation.InvocationState), "activate_planning_provider_invocation"); err != nil {
		return err
	}
	if err := linkPlanningOperationActiveInvocationTx(ctx, tx, invocation, openReq); err != nil {
		return err
	}
	if err := fencePlanningOperationProviderSessionTx(ctx, tx, invocation, openReq); err != nil {
		return err
	}
	if invocation.InvocationState != PlanningProviderInvocationStateOpen || invocation.OpenedAt == "" {
		result, err := tx.ExecContext(ctx, `UPDATE planning_provider_invocations
			SET invocation_state = 'open',
			    provider_session_id = CASE WHEN TRIM(?) <> '' THEN TRIM(?) ELSE provider_session_id END,
			    opened_at = COALESCE(opened_at, ?),
			    updated_at = ?
			WHERE planning_provider_invocation_id = ? AND invocation_state = 'issued'`,
			openReq.ProviderSessionID,
			openReq.ProviderSessionID,
			openReq.OpenedAt,
			openReq.OpenedAt,
			invocation.PlanningProviderInvocationID,
		)
		if err != nil {
			return fmt.Errorf("mark planning provider invocation %q open during activation: %w", invocation.PlanningProviderInvocationID, err)
		}
		if affected, err := result.RowsAffected(); err != nil {
			return fmt.Errorf("read planning provider invocation %q activation rows affected: %w", invocation.PlanningProviderInvocationID, err)
		} else if affected != 1 {
			return planningProviderError("activate_planning_provider_invocation", "active_invocation_conflict", invocation.PlanningProviderInvocationID, invocation.PlanningOperationID, string(invocation.InvocationState))
		}
	}
	return nil
}

func linkPlanningOperationActiveInvocationTx(ctx context.Context, tx *sql.Tx, invocation PlanningProviderInvocation, req MarkPlanningProviderInvocationOpenRequest) error {
	query := fmt.Sprintf(`UPDATE planning_operations
		SET active_provider_invocation_id = ?
		WHERE operation_id = ?
		  AND COALESCE(TRIM(active_provider_invocation_id), '') = ''
		  AND status = 'running'
		  AND lease_owner = ?
		  AND lease_version = ?
		  AND %s`, timestamporder.ComparisonSQL("lease_expires_at", "?", ">"))
	result, err := tx.ExecContext(ctx, query,
		invocation.PlanningProviderInvocationID,
		invocation.PlanningOperationID,
		req.LeaseOwner,
		req.LeaseVersion,
		req.OpenedAt,
	)
	if err != nil {
		return fmt.Errorf("link planning operation %q active invocation: %w", invocation.PlanningOperationID, err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read planning operation active invocation rows affected for %q: %w", invocation.PlanningOperationID, err)
	}
	if rows != 1 {
		return planningProviderError("activate_planning_provider_invocation", "active_invocation_or_lease_conflict", invocation.PlanningProviderInvocationID, invocation.PlanningOperationID, string(invocation.InvocationState))
	}
	return nil
}

func fencePlanningOperationProviderSessionTx(ctx context.Context, tx *sql.Tx, invocation PlanningProviderInvocation, req MarkPlanningProviderInvocationOpenRequest) error {
	query := fmt.Sprintf(`UPDATE planning_operations
		SET provider_session_id = CASE WHEN TRIM(?) <> '' THEN TRIM(?) ELSE provider_session_id END
		WHERE operation_id = ?
		  AND active_provider_invocation_id = ?
		  AND status = 'running'
		  AND lease_owner = ?
		  AND lease_version = ?
		  AND %s`, timestamporder.ComparisonSQL("lease_expires_at", "?", ">"))
	result, err := tx.ExecContext(ctx, query,
		req.ProviderSessionID,
		req.ProviderSessionID,
		invocation.PlanningOperationID,
		invocation.PlanningProviderInvocationID,
		req.LeaseOwner,
		req.LeaseVersion,
		req.OpenedAt,
	)
	if err != nil {
		return fmt.Errorf("fence planning operation %q provider session: %w", invocation.PlanningOperationID, err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read planning operation provider session rows affected for %q: %w", invocation.PlanningOperationID, err)
	}
	if rows != 1 {
		return planningProviderError("fence_planning_operation_provider_session", "active_invocation_or_lease_conflict", invocation.PlanningProviderInvocationID, invocation.PlanningOperationID, string(invocation.InvocationState))
	}
	return nil
}

func normalizeFinalizePlanningProviderSettlementRequest(req FinalizePlanningProviderSettlementRequest) (FinalizePlanningProviderSettlementRequest, error) {
	normalized := FinalizePlanningProviderSettlementRequest{
		FinalTextRef:                 req.FinalTextRef,
		StagedContent:                req.StagedContent,
		SettlementID:                 strings.TrimSpace(req.SettlementID),
		PlanningProviderInvocationID: strings.TrimSpace(req.PlanningProviderInvocationID),
		PrimaryEvidenceID:            strings.TrimSpace(req.PrimaryEvidenceID),
		PrimaryEvidenceKind:          req.PrimaryEvidenceKind,
		PrimaryEvidencePayloadJSON:   strings.TrimSpace(req.PrimaryEvidencePayloadJSON),
		PrimaryEvidenceCreatedAt:     strings.TrimSpace(req.PrimaryEvidenceCreatedAt),
		Outcome:                      req.Outcome,
		FinalText:                    strings.TrimSpace(req.FinalText),
		StructuredPayloadJSON:        firstNonEmptyPlanningProvider(req.StructuredPayloadJSON, "{}"),
		FailureReasonCode:            strings.TrimSpace(req.FailureReasonCode),
		FailureReasonText:            strings.TrimSpace(req.FailureReasonText),
		SettledAt:                    strings.TrimSpace(req.SettledAt),
		RequiresProcessGuard:         req.RequiresProcessGuard,
	}
	if normalized.SettlementID == "" || normalized.PlanningProviderInvocationID == "" || normalized.PrimaryEvidenceID == "" {
		return FinalizePlanningProviderSettlementRequest{}, fmt.Errorf("planning provider settlement id, invocation id, and primary evidence id are required")
	}
	if !isValidPlanningProviderCompletionOutcome(normalized.Outcome) {
		return FinalizePlanningProviderSettlementRequest{}, fmt.Errorf("invalid planning provider completion outcome: %q", normalized.Outcome)
	}
	if normalized.SettledAt == "" {
		return FinalizePlanningProviderSettlementRequest{}, fmt.Errorf("planning provider settlement settled_at is required")
	}
	if normalized.FinalTextRef != nil {
		if err := normalized.FinalTextRef.Validate(normalized.PlanningProviderInvocationID); err != nil {
			return FinalizePlanningProviderSettlementRequest{}, err
		}
		if normalized.FinalText != "" && providerexecution.ReferenceText(normalized.PlanningProviderInvocationID, normalized.FinalText) != *normalized.FinalTextRef {
			return FinalizePlanningProviderSettlementRequest{}, fmt.Errorf("planning terminal text conflicts with its content reference")
		}
		normalized.FinalText = ""
	}
	structuredPayloadJSON, err := canonicalizePlanningProviderJSONDocument(normalized.StructuredPayloadJSON, "{}")
	if err != nil {
		return FinalizePlanningProviderSettlementRequest{}, fmt.Errorf("canonicalize planning provider settlement structured payload: %w", err)
	}
	normalized.StructuredPayloadJSON = structuredPayloadJSON
	if normalized.PrimaryEvidencePayloadJSON != "" {
		if !isValidPlanningProviderCompletionEvidenceKind(normalized.PrimaryEvidenceKind) {
			return FinalizePlanningProviderSettlementRequest{}, fmt.Errorf("invalid planning provider evidence kind: %q", normalized.PrimaryEvidenceKind)
		}
		if normalized.PrimaryEvidenceCreatedAt == "" {
			return FinalizePlanningProviderSettlementRequest{}, fmt.Errorf("planning provider evidence created_at is required when evidence payload is supplied")
		}
	}
	return normalized, nil
}

func planningProviderInvocationStateForOutcome(outcome PlanningProviderCompletionOutcome) PlanningProviderInvocationState {
	switch outcome {
	case PlanningProviderCompletionOutcomeSuccess:
		return PlanningProviderInvocationStateSettledSuccess
	case PlanningProviderCompletionOutcomeCancelled:
		return PlanningProviderInvocationStateSettledCancelled
	default:
		return PlanningProviderInvocationStateSettledFailed
	}
}

func ensurePlanningProviderInvocationStateForOpen(invocation PlanningProviderInvocation) error {
	switch invocation.InvocationState {
	case PlanningProviderInvocationStateIssued, PlanningProviderInvocationStateOpen:
		return nil
	default:
		return planningProviderError("mark_planning_provider_invocation_open", "invalid_invocation_state", invocation.PlanningProviderInvocationID, invocation.PlanningOperationID, string(invocation.InvocationState))
	}
}

func ensurePlanningProviderSessionLineage(planningProviderInvocationID, planningOperationID, existingProviderSessionID, observedProviderSessionID, state, operation string) error {
	existingProviderSessionID = strings.TrimSpace(existingProviderSessionID)
	observedProviderSessionID = strings.TrimSpace(observedProviderSessionID)
	if existingProviderSessionID == "" || observedProviderSessionID == "" || existingProviderSessionID == observedProviderSessionID {
		return nil
	}
	return planningProviderError(operation, "provider_session_lineage_conflict", strings.TrimSpace(planningProviderInvocationID), strings.TrimSpace(planningOperationID), strings.TrimSpace(state))
}

func getPlanningProviderInvocation(ctx context.Context, query sqlQueryRower, planningProviderInvocationID string) (PlanningProviderInvocation, bool, error) {
	return loadPlanningProviderInvocationTx(ctx, query, strings.TrimSpace(planningProviderInvocationID))
}

func loadPlanningProviderInvocationTx(ctx context.Context, query sqlQueryRower, planningProviderInvocationID string) (PlanningProviderInvocation, bool, error) {
	if strings.TrimSpace(planningProviderInvocationID) == "" {
		return PlanningProviderInvocation{}, false, fmt.Errorf("planning_provider_invocation_id is required")
	}
	row := query.QueryRowContext(ctx, planningProviderInvocationSelect+` WHERE planning_provider_invocation_id = ?`, strings.TrimSpace(planningProviderInvocationID))
	invocation, found, err := scanPlanningProviderInvocation(row)
	if err != nil {
		return PlanningProviderInvocation{}, false, fmt.Errorf("get planning provider invocation %q: %w", planningProviderInvocationID, err)
	}
	return invocation, found, nil
}

func getPlanningProviderCompletionSettlement(ctx context.Context, query sqlQueryRower, settlementID string) (PlanningProviderCompletionSettlement, bool, error) {
	return loadPlanningProviderCompletionSettlementTx(ctx, query, strings.TrimSpace(settlementID))
}

func loadPlanningProviderCompletionSettlementTx(ctx context.Context, query sqlQueryRower, settlementID string) (PlanningProviderCompletionSettlement, bool, error) {
	if strings.TrimSpace(settlementID) == "" {
		return PlanningProviderCompletionSettlement{}, false, fmt.Errorf("planning provider settlement_id is required")
	}
	row := query.QueryRowContext(ctx, planningProviderCompletionSettlementSelect+` WHERE settlement_id = ?`, strings.TrimSpace(settlementID))
	settlement, found, err := scanPlanningProviderCompletionSettlement(row)
	if err != nil {
		return PlanningProviderCompletionSettlement{}, false, fmt.Errorf("get planning provider settlement %q: %w", settlementID, err)
	}
	return settlement, found, nil
}

func loadPlanningProviderCompletionEvidenceTx(ctx context.Context, query sqlQueryRower, evidenceID string) (PlanningProviderCompletionEvidence, bool, error) {
	if strings.TrimSpace(evidenceID) == "" {
		return PlanningProviderCompletionEvidence{}, false, fmt.Errorf("planning provider evidence_id is required")
	}
	row := query.QueryRowContext(ctx, planningProviderCompletionEvidenceSelect+` WHERE evidence_id = ?`, strings.TrimSpace(evidenceID))
	evidence, found, err := scanPlanningProviderCompletionEvidence(row)
	if err != nil {
		return PlanningProviderCompletionEvidence{}, false, fmt.Errorf("get planning provider evidence %q: %w", evidenceID, err)
	}
	return evidence, found, nil
}

func scanPlanningProviderInvocation(row scannable) (PlanningProviderInvocation, bool, error) {
	var item PlanningProviderInvocation
	var providerSessionID sql.NullString
	var openedAt sql.NullString
	var settledAt sql.NullString
	err := row.Scan(
		&item.PlanningProviderInvocationID,
		&item.PlanningOperationID,
		&item.PlanningSessionID,
		&item.LogicalSessionID,
		&providerSessionID,
		&item.InvocationState,
		&item.CompletionMode,
		&openedAt,
		&settledAt,
		&item.FailureReasonCode,
		&item.FailureReasonText,
		&item.CreatedAt,
		&item.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		return PlanningProviderInvocation{}, false, nil
	}
	if err != nil {
		return PlanningProviderInvocation{}, false, err
	}
	item.ProviderSessionID = strings.TrimSpace(providerSessionID.String)
	item.OpenedAt = strings.TrimSpace(openedAt.String)
	item.SettledAt = strings.TrimSpace(settledAt.String)
	return item, true, nil
}

func scanPlanningProviderCompletionEvidence(row scannable) (PlanningProviderCompletionEvidence, bool, error) {
	var item PlanningProviderCompletionEvidence
	err := row.Scan(
		&item.PlanningProviderEvidenceSeq,
		&item.EvidenceID,
		&item.PlanningProviderInvocationID,
		&item.PlanningOperationID,
		&item.PlanningSessionID,
		&item.EvidenceKind,
		&item.PayloadJSON,
		&item.CreatedAt,
	)
	if err == sql.ErrNoRows {
		return PlanningProviderCompletionEvidence{}, false, nil
	}
	if err != nil {
		return PlanningProviderCompletionEvidence{}, false, err
	}
	return item, true, nil
}

func scanPlanningProviderCompletionSettlement(row scannable) (PlanningProviderCompletionSettlement, bool, error) {
	var item PlanningProviderCompletionSettlement
	var referenceJSON string
	err := row.Scan(
		&item.SettlementID,
		&item.PlanningProviderInvocationID,
		&item.PlanningOperationID,
		&item.PlanningSessionID,
		&item.Outcome,
		&item.PrimaryEvidenceID,
		&item.FinalText,
		&item.StructuredPayloadJSON,
		&item.FailureReasonCode,
		&item.FailureReasonText,
		&item.SettledAt,
		&item.CreatedAt,
		&referenceJSON,
	)
	if err == sql.ErrNoRows {
		return PlanningProviderCompletionSettlement{}, false, nil
	}
	if err != nil {
		return PlanningProviderCompletionSettlement{}, false, err
	}
	if referenceJSON != "" {
		if err := json.Unmarshal([]byte(referenceJSON), &item.FinalTextRef); err != nil {
			return PlanningProviderCompletionSettlement{}, false, err
		}
		if item.FinalTextRef == nil {
			return PlanningProviderCompletionSettlement{}, false, fmt.Errorf("planning terminal content reference is missing")
		}
		if err := item.FinalTextRef.Validate(item.PlanningProviderInvocationID); err != nil {
			return PlanningProviderCompletionSettlement{}, false, err
		}
	}
	return item, true, nil
}

func samePlanningProviderInvocationCreate(invocation PlanningProviderInvocation, req CreatePlanningProviderInvocationRequest) bool {
	return invocation.PlanningProviderInvocationID == req.PlanningProviderInvocationID &&
		invocation.PlanningOperationID == req.PlanningOperationID &&
		invocation.PlanningSessionID == req.PlanningSessionID &&
		invocation.LogicalSessionID == req.LogicalSessionID
}

func samePlanningProviderSettlement(existing, candidate PlanningProviderCompletionSettlement) bool {
	return existing.SettlementID == candidate.SettlementID &&
		existing.PlanningProviderInvocationID == candidate.PlanningProviderInvocationID &&
		existing.Outcome == candidate.Outcome &&
		existing.PrimaryEvidenceID == candidate.PrimaryEvidenceID &&
		existing.FinalText == candidate.FinalText &&
		planningProviderReferenceJSON(existing.FinalTextRef) == planningProviderReferenceJSON(candidate.FinalTextRef) &&
		existing.StructuredPayloadJSON == candidate.StructuredPayloadJSON &&
		existing.FailureReasonCode == candidate.FailureReasonCode &&
		existing.FailureReasonText == candidate.FailureReasonText
}

func isValidPlanningProviderCompletionEvidenceKind(kind PlanningProviderCompletionEvidenceKind) bool {
	switch kind {
	case PlanningProviderCompletionEvidenceKindStreamObservation,
		PlanningProviderCompletionEvidenceKindExportDiagnostic:
		return true
	default:
		return false
	}
}

func isValidPlanningProviderCompletionOutcome(outcome PlanningProviderCompletionOutcome) bool {
	switch outcome {
	case PlanningProviderCompletionOutcomeSuccess, PlanningProviderCompletionOutcomeFailed, PlanningProviderCompletionOutcomeCancelled:
		return true
	default:
		return false
	}
}

func canonicalizePlanningProviderJSONDocument(raw string, defaultJSON string) (string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		trimmed = strings.TrimSpace(defaultJSON)
	}
	if trimmed == "" {
		return "", fmt.Errorf("json document is required")
	}
	var value any
	if err := json.Unmarshal([]byte(trimmed), &value); err != nil {
		return "", err
	}
	normalized, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return string(normalized), nil
}

const planningProviderInvocationSelect = `SELECT
	planning_provider_invocation_id,
	planning_operation_id,
	planning_session_id,
	logical_session_id,
	provider_session_id,
	invocation_state,
	completion_mode,
	opened_at,
	settled_at,
	failure_reason_code,
	failure_reason_text,
	created_at,
	updated_at
FROM planning_provider_invocations`

const planningProviderCompletionEvidenceSelect = `SELECT
	planning_provider_evidence_seq,
	evidence_id,
	planning_provider_invocation_id,
	planning_operation_id,
	planning_session_id,
	evidence_kind,
	payload_json,
	created_at
FROM planning_provider_completion_evidence`

const planningProviderCompletionSettlementSelect = `SELECT
	settlement_id,
	planning_provider_invocation_id,
	planning_operation_id,
	planning_session_id,
	outcome,
	primary_evidence_id,
	final_text,
	structured_payload_json,
	failure_reason_code,
	failure_reason_text,
	settled_at,
	created_at,final_text_ref_json
	FROM planning_provider_completion_settlements`

func firstNonEmptyPlanningProvider(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

// AbandonPlanningProviderRequest closes the provider request of a planning
// invocation whose server can no longer prove it, and returns its identity. It
// fails with providerrequest.ErrResourcesOpen when the request admitted a tool,
// because the missing provider evidence could then hide a workspace effect.
func (s *PlanningStore) AbandonPlanningProviderRequest(ctx context.Context, invocationID string) (providerexecution.RequestIdentity, bool, error) {
	if s == nil || s.db == nil {
		return providerexecution.RequestIdentity{}, false, fmt.Errorf("planning store database is required")
	}
	var identity providerexecution.RequestIdentity
	var bound bool
	_, err := sqlitex.Transaction(ctx, s.db, func(ctx context.Context, tx *sql.Tx) (err error) {
		identity, bound, err = providerrequest.AbandonOwnerTx(ctx, tx, providerrequest.Owner{Kind: providerrequest.OwnerPlanning, InvocationID: strings.TrimSpace(invocationID)}, s.leaseNow())
		return err
	}, sqlitex.Options{})
	return identity, bound, err
}
