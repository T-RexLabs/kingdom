package providerrequest

import (
	"bytes"
	"context"
	"database/sql"
	dm "github.com/NVIDIA/OpenShell/sdk/go/proto/datamodelv1"
	nativeapi "github.com/NVIDIA/OpenShell/sdk/go/proto/openshellv1"
	"github.com/google/uuid"
	"kingdom/internal/execution/remote"
	"testing"
	"time"
)

func TestRecoverableReservationUsesCanonicalGate(t *testing.T) {
	for _, kind := range []OwnerKind{OwnerRun, OwnerPlanning} {
		for _, scenario := range []string{"valid", "expiry", "replacement", "closed", "foreign-name", "foreign-workspace"} {
			t.Run(string(kind)+"/"+scenario, func(t *testing.T) {
				db, b, now := fixture(t, kind)
				requests, _ := remote.NewRequestStore(bytes.Repeat([]byte{1}, 32))
				template := &nativeapi.CreateSandboxRequest{WorkspaceScope: &dm.WorkspaceSelector{Selection: &dm.WorkspaceSelector_Workspace{Workspace: "trusted"}}, Spec: &nativeapi.SandboxSpec{Command: []string{"/bin/sleep", "60"}}}
				switch scenario {
				case "expiry":
					now = now.Add(2 * time.Hour)
				case "replacement":
					b.Lease.Version++
				case "closed":
					execSQL(t, db, `INSERT INTO provider_preparations(owner_kind,invocation_id,lease_owner,lease_version,admission_closed) VALUES(?,?,?,?,1)`, kind, b.Owner.InvocationID, b.Lease.Owner, b.Lease.Version)
				case "foreign-name":
					template.Name = "foreign"
				case "foreign-workspace":
					template.WorkspaceScope.Selection = &dm.WorkspaceSelector_Workspace{Workspace: "foreign"}
				}
				var r remote.Reservation
				var request *nativeapi.CreateSandboxRequest
				err := transaction(t, db, func(tx *sql.Tx) error {
					var e error
					r, request, e = ReserveRecoverableRemoteRuntimeTx(context.Background(), tx, b.Owner, b.Lease, b.ID, remote.Target{WorkerID: "worker", GatewayID: "gateway", WorkspaceID: "opaque-workspace"}, "trusted", uuid.NewString(), template, requests, now)
					return e
				})
				if scenario == "valid" {
					if err != nil || r.Reference.Allocation == 0 || request.Name == "" {
						t.Fatal(err)
					}
					return
				}
				if err == nil {
					t.Fatal("invalid reservation accepted")
				}
				var count int
				if e := db.QueryRow(`SELECT count(*) FROM remote_runtimes`).Scan(&count); e != nil || count != 0 {
					t.Fatal("partial reservation retained", count, e)
				}
			})
		}
	}
}
