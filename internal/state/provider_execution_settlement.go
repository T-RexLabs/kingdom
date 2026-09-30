package state

import (
	"context"
	"fmt"

	executionstore "kingdom/internal/execution/store"
)

// SettleProviderExecution is the TransitionService boundary for canonical
// provider terminal truth. The execution store owns the focused transaction.
func (s *TransitionService) SettleProviderExecution(ctx context.Context, req executionstore.SettleProviderCompletionAndTerminalRequest) (executionstore.ProviderCompletionSettlement, error) {
	if s == nil || s.db == nil {
		return executionstore.ProviderCompletionSettlement{}, fmt.Errorf("transition service database is required")
	}
	return executionstore.New(s.db).SettleProviderCompletionAndTerminal(ctx, req)
}
