package providerrequest

import (
	"context"
	"database/sql"
	"testing"
)

func TestRemoteDisposalCandidatesRequireTerminalOwnerOrFencedUnboundPreparation(t *testing.T) {
	for _, kind := range []OwnerKind{OwnerRun, OwnerPlanning} {
		t.Run(string(kind), func(t *testing.T) {
			db, b, ref, now := remoteBindingFixture(t, kind)
			ctx := context.Background()
			if e := transaction(t, db, func(tx *sql.Tx) error { return BindRemoteTx(ctx, tx, b, ref, now) }); e != nil {
				t.Fatal(e)
			}
			execSQL(t, db, `UPDATE remote_runtimes SET admission_closed=1`)
			execSQL(t, db, `UPDATE provider_preparations SET admission_closed=1`)
			execSQL(t, db, `UPDATE provider_requests SET admission_closed_at='closed'`)
			rows, e := ListRemoteDisposalCandidates(ctx, db, 0, 64)
			if e != nil || len(rows) != 0 {
				t.Fatal("active bound owner selected", rows, e)
			}
			setInvocationState(t, db, kind, "settled_success")
			rows, e = ListRemoteDisposalCandidates(ctx, db, 0, 64)
			if e != nil || len(rows) != 1 || rows[0].Owner != b.Owner || rows[0].Allocation != ref.Allocation {
				t.Fatal("normal settlement debt missing", rows, e)
			}
			if yes, e := RemoteDisposalStillEligible(ctx, db, rows[0]); e != nil || !yes {
				t.Fatal(e)
			}
			page, e := ListRemoteDisposalCandidates(ctx, db, ref.Allocation, 64)
			if e != nil || len(page) != 0 {
				t.Fatal("cursor repeated allocation", e)
			}
		})
	}
}
func TestRemoteDisposalUnboundFencedIssuedPreparationRemainsRecoverable(t *testing.T) {
	db, b, _, _ := remoteBindingFixture(t, OwnerPlanning)
	setInvocationState(t, db, OwnerPlanning, "issued")
	execSQL(t, db, `UPDATE remote_runtimes SET admission_closed=1`)
	execSQL(t, db, `UPDATE provider_preparations SET admission_closed=1`)
	rows, e := ListRemoteDisposalCandidates(context.Background(), db, 0, 64)
	if e != nil || len(rows) != 1 || rows[0].Owner != b.Owner {
		t.Fatal(rows, e)
	}
}
