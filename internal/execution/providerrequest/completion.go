package providerrequest

import (
	"context"
	"fmt"
	"time"

	"kingdom/internal/core/providerexecution"
)

// RequireCompletionTx checks stored request facts within the canonical
// settlement transaction. It cannot create terminal evidence or close effects.
func RequireCompletionTx(ctx context.Context, q Reader, owner Owner, lease Lease, observation providerexecution.InvocationStreamObservation, outcome string) error {
	binding, found, err := ForOwner(ctx, q, owner)
	if err != nil {
		return err
	}
	if !found || observation.Request == nil {
		return fmt.Errorf("canonical provider request and observation are required")
	}
	request := observation.Request
	if err := request.Identity.Validate(); err != nil {
		return err
	}
	if binding.Owner != owner || binding.Lease != lease || binding.Identity != request.Identity || observation.ProviderSessionID != binding.Identity.SessionID {
		return fmt.Errorf("provider completion does not match its bound identity and lease")
	}
	canonical, err := loadOwner(ctx, q, owner)
	if err != nil {
		return err
	}
	if canonical.settlement == "" {
		// A completed handoff may be consumed after lease expiry. It must
		// still belong to the current owner; expiry grants no new authority.
		if err := canonical.requireActive(binding, false, time.Time{}); err != nil {
			return err
		}
	} else {
		terminalState, invocationState := "committed_failure", "settled_failed"
		if outcome == "success" {
			terminalState, invocationState = "committed_success", "settled_success"
		}
		if outcome == "cancelled" {
			terminalState, invocationState = "committed_cancelled", "settled_cancelled"
		}
		stateMatches := canonical.state == terminalState
		activeMatches := canonical.activeInvocation == owner.InvocationID
		if owner.Kind == OwnerPlanning {
			stateMatches = canonical.state == map[string]string{"success": "succeeded", "failed": "failed", "cancelled": "cancelled"}[outcome]
			activeMatches = canonical.activeInvocation == "" || activeMatches
		} else if outcome == "success" && canonical.state == "committed_failure" {
			// A valid review/validation REWORK succeeds at the provider layer
			// and is mapped separately by Kingdom's workflow validator.
			stateMatches = true
		}
		if canonical.settlementInvocation != owner.InvocationID || canonical.settlementOutcome != outcome || canonical.invocationState != invocationState || !stateMatches || !activeMatches || canonical.session != binding.Identity.SessionID {
			return fmt.Errorf("provider request lacks its exact canonical settled outcome")
		}
	}
	if binding.DispatchIssuedAt == "" || binding.TerminalReference == nil {
		return fmt.Errorf("provider completion requires issued dispatch and stored terminal evidence")
	}
	terminalRef, err := request.Terminal.Reference()
	if err != nil {
		return err
	}
	if terminalRef.InvocationID != owner.InvocationID || terminalRef != *binding.TerminalReference {
		return fmt.Errorf("provider completion evidence conflicts with the immutable terminal")
	}
	if err := providerexecution.RequireRequestTerminalMatch(observation); err != nil {
		return err
	}
	switch outcome {
	case "success":
		if request.Terminal.Outcome != providerexecution.RequestOutcomeSucceeded || observation.Cancelled || observation.TimedOut {
			return fmt.Errorf("successful completion requires the successful provider request")
		}
	case "cancelled":
		// A request cancelled before anything was sent, or whose server is gone,
		// was abandoned rather than interrupted.
		if (request.Terminal.Outcome != providerexecution.RequestOutcomeInterrupted && !request.Terminal.Abandoned()) || !observation.Cancelled {
			return fmt.Errorf("cancellation requires the interrupted or abandoned provider request")
		}
	case "failed":
		// Kingdom may derive failure from a successful raw response, for
		// example when exact guardian facts show forced descendant cleanup.
		// The existing failure derivation and settlement rules still apply.
		if request.Terminal.Outcome != providerexecution.RequestOutcomeSucceeded && request.Terminal.Outcome != providerexecution.RequestOutcomeFailed && request.Terminal.Outcome != providerexecution.RequestOutcomeInterrupted {
			return fmt.Errorf("unknown provider request outcome")
		}
	default:
		return fmt.Errorf("unknown canonical provider completion outcome")
	}
	return RequireResourcesClosedTx(ctx, q, binding.ID)
}
