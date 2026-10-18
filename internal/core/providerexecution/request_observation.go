package providerexecution

import "fmt"

// RequestObservation describes one terminated request on a provider service
// that can remain alive. Its verified terminal record is the immutable provider
// evidence; settlement checks the stored reference and resource closure.
// These facts do not assert that the provider server process exited.
type RequestObservation struct {
	Identity RequestIdentity `json:"identity"`
	Terminal RequestTerminal `json:"terminal"`
}

func validateRequestSuccess(observation InvocationStreamObservation) error {
	request := observation.Request
	if err := request.Identity.Validate(); err != nil {
		return err
	}
	if request.Terminal.Outcome != RequestOutcomeSucceeded || observation.ProviderSessionID != request.Identity.SessionID {
		return fmt.Errorf("provider request did not succeed in its bound session")
	}
	if err := RequireRequestTerminalMatch(observation); err != nil {
		return err
	}
	if observation.ProcessExited || observation.ProcessExitCode != nil || observation.ProcessExitSignal != nil ||
		observation.ProcessQuiescent || observation.StdoutDrained || observation.StderrDrained {
		return fmt.Errorf("provider request cannot assert server process completion")
	}
	return nil
}
