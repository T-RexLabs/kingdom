package providerrequest

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestCheckpointClosureRequiresExactIdentity(t *testing.T) {
	db, binding, now := fixture(t, OwnerRun)
	bind(t, db, binding, now)
	dispatch(t, db, binding, now)
	authority := NewAuthority(db, func() time.Time { return now })
	if err := authority.RequireSessionClosed(context.Background(), binding.Identity); !errors.Is(err, ErrResourcesOpen) {
		t.Fatalf("open admission accepted: %v", err)
	}
	closeAdmission(t, db, binding, now)
	if err := authority.RequireSessionClosed(context.Background(), binding.Identity); err != nil {
		t.Fatal(err)
	}
	wrong := binding.Identity
	wrong.Location = "/other"
	if err := authority.RequireSessionClosed(context.Background(), wrong); err == nil {
		t.Fatal("partial binding identity accepted")
	}
}

func TestCheckpointClosureRejectsStaleSourceWhenAnotherSessionRequestIsOpen(t *testing.T) {
	db, binding, now := fixture(t, OwnerRun)
	bind(t, db, binding, now)
	dispatch(t, db, binding, now)
	closeAdmission(t, db, binding, now)
	// Fixture the later binding directly: current continuation admission is
	// intentionally stricter than the future checkpoint-authorized Resume path.
	execSQL(t, db, `INSERT INTO provider_invocations(provider_invocation_id,attempt_id,run_id,node_run_id,logical_session_id,provider_session_id,invocation_state,completion_mode,created_at,updated_at) SELECT 'later-inv',attempt_id,run_id,node_run_id,logical_session_id,provider_session_id,'open',completion_mode,created_at,updated_at FROM provider_invocations WHERE provider_invocation_id='inv'`)
	execSQL(t, db, `INSERT INTO provider_requests(binding_id,run_invocation_id,instance_id,generation,location,provider_session_id,profile_id,request_id,logical_session_id,lease_owner,lease_version,created_at) SELECT 'later-binding','later-inv',instance_id,generation,location,provider_session_id,'later-profile','later-request',logical_session_id,lease_owner,lease_version,created_at FROM provider_requests WHERE binding_id='binding'`)
	authority := NewAuthority(db, func() time.Time { return now })
	if err := authority.RequireSessionClosed(context.Background(), binding.Identity); !errors.Is(err, ErrResourcesOpen) {
		t.Fatalf("stale source could interrupt a newer open request: %v", err)
	}
}
