package providerrequest

import (
	"context"
	"testing"
)

func TestHostLocalReversionRequiresClosedManagedSources(t *testing.T) {
	for _, kind := range []OwnerKind{OwnerRun, OwnerPlanning} {
		t.Run(string(kind), func(t *testing.T) {
			db, _, _ := fixture(t, kind)
			if kind == OwnerRun {
				execSQL(t, db, `INSERT INTO execution_run_backends(run_id,backend) VALUES('run','openshell')`)
			} else {
				execSQL(t, db, `INSERT INTO execution_planning_backends(operation_id,backend) VALUES('operation','openshell')`)
			}
			if err := RequireHostLocalMode(context.Background(), db); err == nil {
				t.Fatal("active managed source allowed local reversion")
			}
			if kind == OwnerRun {
				execSQL(t, db, `UPDATE workflow_runs SET run_status='succeeded'`)
			} else {
				execSQL(t, db, `UPDATE planning_operations SET status='succeeded'`)
			}
			if err := RequireHostLocalMode(context.Background(), db); err == nil {
				t.Fatal("open managed invocation ignored")
			}
			if kind == OwnerRun {
				execSQL(t, db, `UPDATE provider_invocations SET invocation_state='settled_success'`)
			} else {
				execSQL(t, db, `UPDATE planning_provider_invocations SET invocation_state='settled_success'`)
			}
			if err := RequireHostLocalMode(context.Background(), db); err != nil {
				t.Fatal("closed history must not prevent local reversion", err)
			}
		})
	}
}
