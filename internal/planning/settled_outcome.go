package planning

import (
	"context"
	"errors"

	"kingdom/internal/state"
)

// settledPlanningFailure retains the provider/operation failure for reporting,
// but is minted only after its terminal transaction has completed. The failure
// is a durable outcome, not evidence that shutdown failed to drain a consumer.
type settledPlanningFailure struct{ cause error }

func (e *settledPlanningFailure) Error() string { return e.cause.Error() }
func (e *settledPlanningFailure) Unwrap() error { return e.cause }

// Keep cleanup branches separate from settled outcomes. errors.As/Is over the
// whole joined error would let a benign outcome hide an independent cleanup or
// quiescence error. Only bare sentinels represent the runner's clean-cancel and
// lease-race return paths; a wrapped cancellation can be a real cleanup failure.
func planningDrainFailure(err error) error {
	if err == nil || err == context.Canceled || err == state.ErrPlanningLeaseConflict {
		return nil
	}
	if _, ok := err.(*settledPlanningFailure); ok {
		return nil
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		var failures []error
		for _, cause := range joined.Unwrap() {
			if failure := planningDrainFailure(cause); failure != nil {
				failures = append(failures, failure)
			}
		}
		return errors.Join(failures...)
	}
	return err
}

// Used only at explicitly cancellation-safe lifecycle boundaries, never to
// classify a provider failure, finalization error, or release callback broadly.
func onlyContextCancellation(err error) bool {
	if err == context.Canceled {
		return true
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		causes := joined.Unwrap()
		if len(causes) == 0 {
			return false
		}
		for _, cause := range causes {
			if !onlyContextCancellation(cause) {
				return false
			}
		}
		return true
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		return onlyContextCancellation(wrapped.Unwrap())
	}
	return false
}
