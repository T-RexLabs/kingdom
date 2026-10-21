package remote

import (
	"database/sql"
	"testing"
)

func TestReadManagedIdentityRequiresRetainedExactOwner(t *testing.T) {
	db := openLedger(t, ":memory:")
	r := bound(t, db)
	expected, start, identity := managedRequest(t, r)
	prepareManaged(t, db, r, ProviderServer, expected, start)
	dispatchManaged(t, db, r, identity.RequestId)
	if _, _, err := ReadManagedIdentity(ctx, db, r.Owner, r.Reference, identity.RequestId); err == nil {
		t.Fatal("missing returned identity accepted")
	}
	transaction(t, db, func(tx *sql.Tx) error { return RecordManagedIdentityTx(ctx, tx, r.Owner, r.Reference, identity) })
	got, purpose, err := ReadManagedIdentity(ctx, db, r.Owner, r.Reference, identity.RequestId)
	if err != nil || purpose != ProviderServer || got.RequestId != identity.RequestId {
		t.Fatal(got, purpose, err)
	}
	got.Runtime.MainProcessInstanceId = "modified copy"
	again, _, err := ReadManagedIdentity(ctx, db, r.Owner, r.Reference, identity.RequestId)
	if err != nil || again.Runtime.MainProcessInstanceId != identity.Runtime.MainProcessInstanceId {
		t.Fatal("caller modified retained identity", err)
	}
	owner := r.Owner
	owner.LeaseVersion++
	if _, _, err := ReadManagedIdentity(ctx, db, owner, r.Reference, identity.RequestId); err == nil {
		t.Fatal("foreign lease accepted")
	}
	ref := r.Reference
	ref.Generation = "replacement"
	if _, _, err := ReadManagedIdentity(ctx, db, r.Owner, ref, identity.RequestId); err == nil {
		t.Fatal("replacement runtime accepted")
	}
}
