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
	if request == nil {
		return fmt.Errorf("provider request observation is required")
	}
	if err := request.Identity.Validate(); err != nil {
		return err
	}
	if request.Terminal.Outcome != RequestOutcomeSucceeded || observation.ProviderSessionID != request.Identity.SessionID {
		return fmt.Errorf("provider request did not succeed in its bound session")
	}
	return RequireRequestTerminalMatch(observation)
}
