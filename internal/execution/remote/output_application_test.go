package remote

import "testing"

func TestMissingOutputApplicationCannotCertifySuccess(t *testing.T) {
	db := openLedger(t, ":memory:")
	r := bound(t, db)
	if e := RequireWorkspaceOutputApplied(ctx, db, r.Owner, r.Reference); e == nil {
		t.Fatal("missing output application accepted")
	}
}
