package planning

import (
	"context"
	"fmt"

	"kingdom/internal/state"
)

func (r *operationRunner) finishProviderContent(operation state.PlanningOperation, metadata ProviderExecutionMetadata, closeParsed func() error) error {
	if closeParsed != nil {
		if err := closeParsed(); err != nil {
			return fmt.Errorf("close parsed planner content before retirement: %w", err)
		}
	}
	return r.retireProviderContent(operation, metadata)
}

// Called after the operation's parsers, finalizer, and diagnostic consumers
// return. Never infer ingestion from a nil execution error or terminal status
// alone: the exact invocation, evidence, and sealed manifests must agree.
func (r *operationRunner) retireProviderContent(operation state.PlanningOperation, metadata ProviderExecutionMetadata) error {
	if metadata.StreamObservation.ReleaseContent == nil {
		return nil
	}
	// Forced/deadline shutdown revoked DB handoff access. Leave staging with
	// its owner for the later, fully-drained adapter close; do not acknowledge it.
	if r.handoffCtx.Err() != nil {
		return nil
	}
	if operation.ActiveProviderInvocationID == nil {
		return fmt.Errorf("planner content retirement requires invocation lineage")
	}
	ctx, cancel := context.WithTimeout(r.handoffCtx, terminalCancelWriteTimeout)
	defer cancel()
	_, confirmed, err := r.deps.store.ConfirmPlanningProviderContentIngestion(ctx, operation.OperationID, *operation.ActiveProviderInvocationID, metadata.StreamObservation)
	if err != nil {
		if r.handoffCtx.Err() != nil && onlyContextCancellation(err) {
			return nil
		}
		return fmt.Errorf("confirm planner content ingestion: %w", err)
	}
	if !confirmed {
		return fmt.Errorf("planner content ingestion is not confirmed; staging retained")
	}
	if err := metadata.StreamObservation.ReleaseContent(); err != nil {
		return fmt.Errorf("release ingested planner provider content: %w", err)
	}
	return nil
}
