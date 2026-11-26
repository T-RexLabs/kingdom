package opencode

import (
	"context"
	"kingdom/internal/core/providerexecution"
)

// Backend owns the lifecycle of one execution implementation. Model discovery
// is a separate capability so hosted assembly cannot inherit host accounts.
type Backend interface {
	Provider
	CloseContext(context.Context) error
	ForceClose() error
	ExecutionBackend() providerexecution.Backend
}

func (s *Service) ExecutionBackend() providerexecution.Backend { return providerexecution.BackendLocal }
