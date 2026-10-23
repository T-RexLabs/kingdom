package providerrequest

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestPreparationWorkspaceRejectsReplacementAndForeignOwner(t *testing.T) {
	for _, kind := range []OwnerKind{OwnerRun, OwnerPlanning} {
		t.Run(string(kind), func(t *testing.T) {
			db, b, now := fixture(t, kind)
			ctx := context.Background()
			setInvocationState(t, db, kind, "issued")
			dir := filepath.Join(t.TempDir(), "root")
			if err := os.Mkdir(dir, 0700); err != nil {
				t.Fatal(err)
			}
			pin, err := PinWorkspace(dir)
			if err != nil {
				t.Fatal(err)
			}
			if err = transaction(t, db, func(tx *sql.Tx) error {
				if err := ReservePreparationTx(ctx, tx, b.Owner, b.Lease, now); err != nil {
					return err
				}
				return RecordPreparationWorkspaceTx(ctx, tx, b.Owner, b.Lease, pin)
			}); err != nil {
				t.Fatal(err)
			}
			p, err := NewAuthority(db, func() time.Time { return now }).Preparation(ctx, b.Owner.InvocationID)
			if err != nil {
				t.Fatal(err)
			}
			root, err := OpenPreparationWorkspace(ctx, db, p)
			if err != nil {
				t.Fatal(err)
			}
			root.Close()
			var sequence int
			var name, path string
			if err = db.QueryRow(`PRAGMA database_list`).Scan(&sequence, &name, &path); err != nil {
				t.Fatal(err)
			}
			reopened, err := sql.Open("sqlite", path)
			if err != nil {
				t.Fatal(err)
			}
			root, err = OpenPreparationWorkspace(ctx, reopened, p)
			if err != nil {
				t.Fatal("restart lost pin", err)
			}
			root.Close()
			reopened.Close()
			otherPin, err := PinWorkspace(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			if err = transaction(t, db, func(tx *sql.Tx) error { return RecordPreparationWorkspaceTx(ctx, tx, b.Owner, b.Lease, otherPin) }); err == nil {
				t.Fatal("pin replacement accepted")
			}
			bad := p
			bad.Lease.Version++
			if root, err = OpenPreparationWorkspace(ctx, db, bad); err == nil {
				root.Close()
				t.Fatal("foreign lease accepted")
			}
			execSQL(t, db, `UPDATE provider_preparations SET admission_closed=1`)
			root, err = OpenPreparationWorkspace(ctx, db, p)
			if err != nil {
				t.Fatal("historical root lost after fence", err)
			}
			root.Close()
			if err = os.Rename(dir, dir+"-old"); err != nil {
				t.Fatal(err)
			}
			if err = os.Mkdir(dir, 0700); err != nil {
				t.Fatal(err)
			}
			if root, err = OpenPreparationWorkspace(ctx, db, p); err == nil {
				root.Close()
				t.Fatal("replacement root accepted")
			}
			os.Remove(dir)
			if err = os.Symlink(dir+"-old", dir); err != nil {
				t.Fatal(err)
			}
			if _, err = PinWorkspace(dir); err == nil {
				t.Fatal("symlink pin accepted")
			}
			if root, err = OpenPreparationWorkspace(ctx, db, p); err == nil {
				root.Close()
				t.Fatal("symlink root accepted")
			}
		})
	}
}

func TestOutputWorkspaceRequiresCurrentUnpublishedTarget(t *testing.T) {
	db, b, now := fixture(t, OwnerRun)
	ctx := context.Background()
	setInvocationState(t, db, OwnerRun, "issued")
	dir := t.TempDir()
	pin, err := PinWorkspace(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err = transaction(t, db, func(tx *sql.Tx) error {
		if err := ReservePreparationTx(ctx, tx, b.Owner, b.Lease, now); err != nil {
			return err
		}
		return RecordPreparationWorkspaceTx(ctx, tx, b.Owner, b.Lease, pin)
	}); err != nil {
		t.Fatal(err)
	}
	p, err := NewAuthority(db, func() time.Time { return now }).Preparation(ctx, b.Owner.InvocationID)
	if err != nil {
		t.Fatal(err)
	}
	setInvocationState(t, db, OwnerRun, "open")
	execSQL(t, db, `INSERT INTO run_execution_targets(run_id,target_mode,source_root,execution_root,publish_root,state,created_at,updated_at) VALUES('run','direct',?,?,?,'attempt_dirty','now','now')`, t.TempDir(), dir, t.TempDir())
	execSQL(t, db, `UPDATE provider_preparations SET admission_closed=1`)
	if err = ValidateOutputTx(ctx, db, p, now); err != nil {
		t.Fatal(err)
	}
	root, err := OpenOutputWorkspace(ctx, db, p)
	if err != nil {
		t.Fatal(err)
	}
	root.Close()
	alias := filepath.Join(t.TempDir(), "alias")
	if err = os.Symlink(dir, alias); err != nil {
		t.Fatal(err)
	}
	var previous string
	if err = db.QueryRow(`SELECT publish_root FROM run_execution_targets`).Scan(&previous); err != nil {
		t.Fatal(err)
	}
	execSQL(t, db, `UPDATE run_execution_targets SET publish_root=?`, alias)
	if root, err = OpenOutputWorkspace(ctx, db, p); err == nil {
		root.Close()
		t.Fatal("published symlink alias accepted")
	}
	execSQL(t, db, `UPDATE run_execution_targets SET publish_root=?`, previous)
	for _, mutation := range []string{`UPDATE run_execution_targets SET state='trusted'`, `UPDATE run_execution_targets SET publish_root=execution_root`, `UPDATE attempt_executions SET lease_version=2`} {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = tx.Exec(mutation); err != nil {
			t.Fatal(err)
		}
		if err = ValidateOutputTx(ctx, tx, p, now); err == nil {
			t.Fatal("unsafe output accepted", mutation)
		}
		tx.Rollback()
	}
	if err = ValidateOutputTx(ctx, db, p, now.Add(2*time.Hour)); err == nil {
		t.Fatal("expired output accepted")
	}
}
