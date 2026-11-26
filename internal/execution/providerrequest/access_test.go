package providerrequest

import (
	"context"
	"database/sql"
	"testing"

	"github.com/google/uuid"
	"kingdom/internal/execution/access"
)

func accessSnapshot() access.Snapshot {
	actor := uuid.NewString()
	return access.Snapshot{Version: 1, ActorID: actor, ProjectID: uuid.NewString(), Selection: access.Selection{Model: "openai/model", ThinkingEffort: "medium", Connection: access.Connection{ID: uuid.NewString(), Revision: 1, OwnerID: actor, Scope: "personal", Provider: "openai"}}}
}

func TestHostedPreparationPinsCanonicalAccessOrRollsBack(t *testing.T) {
	for _, kind := range []OwnerKind{OwnerRun, OwnerPlanning} {
		t.Run(string(kind), func(t *testing.T) {
			db, b, now := fixture(t, kind)
			s := accessSnapshot()
			execSQL(t, db, `INSERT INTO execution_access_project(slot,project_id) VALUES(1,?)`, s.ProjectID)
			if kind == OwnerRun {
				execSQL(t, db, `UPDATE provider_invocations SET invocation_state='issued',opened_at=NULL,provider_session_id=''`)
			} else {
				execSQL(t, db, `UPDATE planning_provider_invocations SET invocation_state='issued',opened_at=NULL,provider_session_id=''`)
			}
			reserve := func() error {
				return transaction(t, db, func(tx *sql.Tx) error { return ReservePreparationTx(context.Background(), tx, b.Owner, b.Lease, now) })
			}
			if err := reserve(); err == nil {
				t.Fatal("hosted preparation accepted missing access")
			}
			var count int
			if err := db.QueryRow(`SELECT count(*) FROM provider_preparations`).Scan(&count); err != nil || count != 0 {
				t.Fatalf("partial reservation: %d %v", count, err)
			}
			wanted := s
			if err := transaction(t, db, func(tx *sql.Tx) error {
				if kind == OwnerRun {
					override := s.Selection
					override.Model = "openai/role-model"
					override.Connection.ID = uuid.NewString()
					override.Connection.Revision = 7
					wanted.Selection = override
					return access.CaptureRunTx(context.Background(), tx, "run", access.RunSnapshot{Version: 1, ActorID: s.ActorID, ProjectID: s.ProjectID, Global: s.Selection, Roles: map[string]access.Selection{"role": override}})
				}
				return access.CapturePlanningTx(context.Background(), tx, "operation", s)
			}); err != nil {
				t.Fatal(err)
			}
			if err := reserve(); err != nil {
				t.Fatal(err)
			}
			if err := reserve(); err != nil {
				t.Fatalf("same reservation replay: %v", err)
			}
			saved, err := access.Invocation(context.Background(), db, string(kind), b.Owner.InvocationID)
			if err != nil || saved != wanted {
				t.Fatalf("wrong canonical selection: %+v %v", saved, err)
			}
			if _, err := db.Exec(`UPDATE execution_access_invocations SET snapshot='{}'`); err == nil {
				t.Fatal("invocation selection mutated")
			}
			if _, err := db.Exec(`DELETE FROM execution_access_project`); err == nil {
				t.Fatal("hosted policy removed")
			}
			if _, err := db.Exec(`DELETE FROM execution_access_invocations`); err == nil {
				t.Fatal("invocation selection deleted")
			}
			if kind == OwnerPlanning {
				changed := s
				changed.Selection.Connection.Revision++
				if err := transaction(t, db, func(tx *sql.Tx) error {
					return access.CapturePlanningTx(context.Background(), tx, "operation", changed)
				}); err == nil {
					t.Fatal("source selection replaced")
				}
			}
		})
	}
}

func TestExecutionProjectBindingRejectsExistingLocalWork(t *testing.T) {
	db, _, _ := fixture(t, OwnerRun)
	if err := transaction(t, db, func(tx *sql.Tx) error { return access.BindProjectTx(context.Background(), tx, uuid.NewString()) }); err == nil {
		t.Fatal("existing local work silently acquired hosted authority")
	}
	fresh := database(t)
	project := uuid.NewString()
	for range 2 {
		if err := transaction(t, fresh, func(tx *sql.Tx) error { return access.BindProjectTx(context.Background(), tx, project) }); err != nil {
			t.Fatal(err)
		}
	}
	if err := transaction(t, fresh, func(tx *sql.Tx) error { return access.BindProjectTx(context.Background(), tx, uuid.NewString()) }); err == nil {
		t.Fatal("project identity changed")
	}
}

func TestExecutionAccessRejectsPersonalOwnerAndIncompleteRoleTuple(t *testing.T) {
	s := accessSnapshot()
	s.Selection.Connection.OwnerID = uuid.NewString()
	if err := s.Validate(); err == nil {
		t.Fatal("another user's personal connection accepted")
	}
	s = accessSnapshot()
	run := access.RunSnapshot{Version: 1, ActorID: s.ActorID, ProjectID: s.ProjectID, Global: s.Selection, Roles: map[string]access.Selection{"role": {Model: "openai/override"}}}
	if _, err := run.ForRole("role"); err == nil {
		t.Fatal("role borrowed global credential")
	}
}
