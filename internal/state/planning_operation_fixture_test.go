package state

import (
	"context"
	"fmt"
	"strings"
)

// Generic operation mutation is fixture setup only. Runtime code must use
// canonical lease-fenced claim, finalization, and recovery operations.
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

func updatePlanningOperationFixture(ctx context.Context, store *PlanningStore, req UpdatePlanningOperationRequest) error {
	if strings.TrimSpace(req.OperationID) == "" {
		return fmt.Errorf("operation id is required")
	}
	if req.Status != nil && !isValidPlanningOperationStatus(*req.Status) {
		return fmt.Errorf("invalid planning operation status: %q", *req.Status)
	}
	builder := newUpdateBuilder("planning_operations")
	if status := operationStatusPtrValue(req.Status); status != nil {
		builder.Set("status", *status)
	}
	if req.LeaseOwner != nil {
		builder.Set("lease_owner", strings.TrimSpace(*req.LeaseOwner))
	}
	if req.LeaseExpiresAt != nil {
		builder.Set("lease_expires_at", finalizeNullable(*req.LeaseExpiresAt))
	}
	if leaseVersion := intPtrValue(req.LeaseVersion); leaseVersion != nil {
		builder.Set("lease_version", *leaseVersion)
	}
	if req.LastHeartbeatAt != nil {
		builder.Set("last_heartbeat_at", finalizeNullable(*req.LastHeartbeatAt))
	}
	if req.ProviderSessionID != nil {
		builder.Set("provider_session_id", finalizeNullable(*req.ProviderSessionID))
	}
	if req.ActiveProviderInvocationID != nil {
		builder.Set("active_provider_invocation_id", finalizeNullable(*req.ActiveProviderInvocationID))
	}
	if req.ObservedOutputText != nil {
		builder.Set("observed_output_text", strings.TrimSpace(*req.ObservedOutputText))
	}
	if req.Error != nil {
		builder.Set("error", finalizeNullable(*req.Error))
	}
	if req.ResultRef != nil {
		builder.Set("result_ref", finalizeNullable(*req.ResultRef))
	}
	if req.CanonicalSettlementID != nil {
		builder.Set("canonical_settlement_id", finalizeNullable(*req.CanonicalSettlementID))
	}
	if req.StartedAt != nil {
		builder.Set("started_at", finalizeNullable(*req.StartedAt))
	}
	if req.FinishedAt != nil {
		builder.Set("finished_at", finalizeNullable(*req.FinishedAt))
	}
	if !builder.HasUpdates() {
		return nil
	}
	builder.Where("operation_id = ?", strings.TrimSpace(req.OperationID))

	_, err := builder.Exec(ctx, store.db)
	if err != nil {
		return fmt.Errorf("update planning operation: %w", err)
	}
	return nil
}

func operationStatusPtrValue(value *PlanningOperationStatus) *string {
	if value == nil {
		return nil
	}
	trimmed := strings.TrimSpace(string(*value))
	return &trimmed
}
