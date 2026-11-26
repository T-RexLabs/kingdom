package providerrequest

import (
	"context"
	"database/sql"
	"kingdom/internal/core/providerexecution"
	"testing"
)

func TestBackendBindingUsesRetainedRemoteOwnerAndRejectsMixedIdentity(t *testing.T) {
	for _, kind := range []OwnerKind{OwnerRun, OwnerPlanning} {
		for _, change := range []string{"none", "local", "foreign-owner", "foreign-runtime"} {
			t.Run(string(kind)+"/"+change, func(t *testing.T) {
				db, b, ref, now := remoteBindingFixture(t, kind)
				expected := b.ID
				b.ID = "activation-default"
				b.RemoteAllocation = 0
				switch change {
				case "local":
					b.Backend = providerexecution.BackendLocal
				case "foreign-owner":
					b.Owner.InvocationID = "foreign"
				case "foreign-runtime":
					ref.Generation = "foreign"
				}
				err := transaction(t, db, func(tx *sql.Tx) error { return BindBackendTx(context.Background(), tx, b, ref, now) })
				if change != "none" {
					if err == nil {
						t.Fatal("mixed identity accepted")
					}
					var n int
					if e := db.QueryRow("SELECT count(*) FROM provider_requests").Scan(&n); e != nil || n != 0 {
						t.Fatal("partial binding survived", n, e)
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				saved, e := Get(context.Background(), db, expected)
				if e != nil || saved.ID != expected || saved.RemoteAllocation != ref.Allocation {
					t.Fatal(saved, e)
				}
			})
		}
	}
}
