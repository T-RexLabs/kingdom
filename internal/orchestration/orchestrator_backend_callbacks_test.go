package orchestration

import (
	"context"
	"kingdom/internal/providers/opencode"
	"testing"
)

func TestGuardianCallbacksFollowCanonicalBackend(t *testing.T) {
	for _, backend := range []string{"local", "openshell", "unknown", "missing", "stale"} {
		t.Run(backend, func(t *testing.T) {
			h := newTerminalHandoffHarness(t)
			inv := h.addInvocation(t, 1)
			job := h.runningHandoffJob(t, inv, 1)
			// Corrupt only the provenance column to exercise the dispatch guard without
			// manufacturing remote process receipts or enabling remote dispatch.
			if backend == "openshell" || backend == "unknown" {
				mustExec(t, h.db, `DROP TRIGGER provider_request_runtime_immutable`)
				mustExec(t, h.db, `PRAGMA ignore_check_constraints=ON`)
				if _, err := h.db.Exec(`UPDATE provider_requests SET execution_backend=?`, backend); err != nil {
					t.Fatal(err)
				}
			}
			if backend == "missing" {
				job.providerInvocationID = "missing"
			}
			if backend == "stale" {
				job.expectedLeaseVersion++
			}
			request := opencode.ExecuteRequest{}
			err := h.loop.configureProviderGuardianCallbacks(context.Background(), job, &request)
			if backend == "unknown" || backend == "missing" || backend == "stale" {
				if err == nil {
					t.Fatal("invalid canonical backend admitted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			local := backend == "local"
			if (request.OnCommandGuardianOwnership != nil) != local || (request.OnCommandGuardianQuiescence != nil) != local {
				t.Fatal("guardian callbacks do not match canonical backend")
			}
		})
	}
}
