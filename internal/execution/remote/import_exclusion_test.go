package remote

import (
	"database/sql"
	"github.com/google/uuid"
	"testing"
)

func TestWorkspaceImportRejectsProviderLaunchIntent(t *testing.T) {
	for _, stage := range []string{"prepare", "dispatch"} {
		t.Run(stage, func(t *testing.T) {
			db := openLedger(t, ":memory:")
			r := bound(t, db)
			o, request, _ := managedRequest(t, r)
			if stage == "dispatch" {
				prepareManaged(t, db, r, WorkspaceImport, o, request)
			}
			transaction(t, db, func(tx *sql.Tx) error {
				_, e := tx.Exec(`INSERT INTO remote_provider_launches(allocation,operation_id)VALUES(?,?)`, r.Reference.Allocation, uuid.NewString())
				return e
			})
			reject(t, db, nil, func(tx *sql.Tx) error {
				if stage == "dispatch" {
					_, e := ClaimDispatchTx(ctx, tx, r.Owner, r.Reference, request.Execution.RequestId)
					return e
				}
				_, e := PrepareManagedExecutionTx(ctx, tx, r.Owner, r.Reference, WorkspaceImport, o, request)
				return e
			})
		})
	}
}
