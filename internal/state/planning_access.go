package state

import (
	"context"
	"database/sql"
	"fmt"
	"kingdom/internal/execution/access"
)

// PlanningAccessResolver is injected by the trusted API boundary. It must use
// only bounded metadata and cached model validation: it runs inside the project
// writer transaction and must not query this database through another connection.
type PlanningAccessResolver func(context.Context, string) (access.Snapshot, error)
type PlanningStoreOption func(*PlanningStore)

func WithPlanningAccessResolver(resolve PlanningAccessResolver) PlanningStoreOption {
	return func(s *PlanningStore) { s.resolveAccess = resolve }
}
func (s *PlanningStore) captureOperationAccess(ctx context.Context, tx *sql.Tx, row PlanningOperation) error {
	project, err := access.Project(ctx, tx)
	if err != nil {
		return err
	}
	if project == "" {
		return nil
	}
	if row.ModelSelectionSourceOperationID != "" {
		return access.CopyPlanningTx(ctx, tx, row.OperationID, row.ModelSelectionSourceOperationID)
	}
	if s.resolveAccess == nil {
		return fmt.Errorf("hosted planning access resolver is required")
	}
	snapshot, err := s.resolveAccess(ctx, row.ModelSelectionJSON)
	if err != nil {
		return err
	}
	if snapshot.ProjectID != project {
		return access.ErrInvalid
	}
	if err := access.MatchesSelection(row.ModelSelectionJSON, snapshot.Selection); err != nil {
		return err
	}
	return access.CapturePlanningTx(ctx, tx, row.OperationID, snapshot)
}
