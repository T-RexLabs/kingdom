package commands

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"github.com/google/uuid"
	"kingdom/internal/dbmigrations"
	"strings"
	"testing"

	pb "kingdom/internal/execution/openshell/managedpb"
	pr "kingdom/internal/execution/providerrequest"
	"kingdom/internal/execution/remote"
	"kingdom/internal/filetoolhelper"
)

func nativeCall(t *testing.T, e *Executor, in pr.NativeCall, p *peer) pr.NativeCall {
	t.Helper()
	e.config.HelperSHA256 = strings.Repeat("a", 64)
	in.MessageID = "native-message"
	in.CallID = "native-call"
	in.Tool = "write"
	in.Input = json.RawMessage(`{"path":"file","content":"hello"}`)
	payload, _ := json.Marshal(filetoolhelper.Result{Version: 1, Tool: "write", OK: true, Result: &filetoolhelper.ToolResult{Output: map[string]any{"operation": "write", "target": "/workspace/project/file", "resource": "file", "existed": false}, Content: "Created file successfully: file"}})
	p.chunks = []*pb.OutputChunk{{Sequence: 1, Stream: pb.OutputStream_OUTPUT_STREAM_STDOUT, Data: payload}}
	return in
}
func TestNativeHelperCompletionRequiresRetainedResult(t *testing.T) {
	e, shell, p := executorFixture(t)
	call := nativeCall(t, e, pr.NativeCall{Identity: shell.Identity}, p)
	result, err := e.InvokeNative(context.Background(), call)
	if err != nil || !result.OK {
		t.Fatalf("%+v %v", result, err)
	}
	p.offline = true
	result, err = e.InvokeNative(context.Background(), call)
	if err != nil || !result.OK || p.starts != 1 {
		t.Fatalf("replay %+v %v", result, err)
	}
	var outcome, effect string
	var started bool
	if err = e.db.QueryRow(`SELECT g.outcome,g.native_started,c.effect FROM provider_native_grants g JOIN provider_remote_native_completions c ON c.grant_id=g.grant_id`).Scan(&outcome, &started, &effect); err != nil {
		t.Fatal(err)
	}
	if outcome != "completed" || !started || effect != "complete" {
		t.Fatal(outcome, started, effect)
	}
}
func TestNativeCallbackCannotForgeHelperCompletion(t *testing.T) {
	e, shell, p := executorFixture(t)
	call := nativeCall(t, e, pr.NativeCall{Identity: shell.Identity}, p)
	p.lost = true
	if _, err := e.InvokeNative(context.Background(), call); !errors.Is(err, ErrUnresolved) {
		t.Fatal(err)
	}
	var id, digest string
	e.db.QueryRow(`SELECT grant_id,identity_digest FROM provider_native_grants`).Scan(&id, &digest)
	started := true
	err := e.transaction(context.Background(), func(tx *sql.Tx) error {
		return pr.FinishNativeTx(context.Background(), tx, pr.NativeReceipt{GrantID: id, Digest: digest, Outcome: "completed", Started: &started}, e.config.Now())
	})
	if err == nil {
		t.Fatal("callback forged helper result")
	}
	p.lost = false
	if _, err = e.InvokeNative(context.Background(), call); err != nil {
		t.Fatal(err)
	}
}
func TestNativeAndShellShareDurableSlot(t *testing.T) {
	e, shell, p := executorFixture(t)
	call := nativeCall(t, e, pr.NativeCall{Identity: shell.Identity}, p)
	p.lost = true
	p.pending = true
	if _, err := e.InvokeNative(context.Background(), call); !errors.Is(err, ErrUnresolved) {
		t.Fatal(err)
	}
	if _, err := e.Invoke(context.Background(), shell); !errors.Is(err, remote.ErrToolBusy) {
		t.Fatal("shell overlapped native", err)
	}
	var count int
	e.db.QueryRow(`SELECT count(*) FROM provider_command_calls`).Scan(&count)
	if count != 0 {
		t.Fatal("busy shell left claim")
	}
	p.mu.Lock()
	p.finish(false)
	p.mu.Unlock()
	p.pending = false
	p.lost = false
	if _, err := e.InvokeNative(context.Background(), call); err != nil {
		t.Fatal(err)
	}
	p.chunks = nil
	if _, err := e.Invoke(context.Background(), shell); err != nil {
		t.Fatal(err)
	}
}
func TestMalformedNativeResultClosesAsUnknown(t *testing.T) {
	e, shell, p := executorFixture(t)
	call := nativeCall(t, e, pr.NativeCall{Identity: shell.Identity}, p)
	p.chunks[0].Data = []byte(`{"ok":true}`)
	result, err := e.InvokeNative(context.Background(), call)
	if err != nil || result.OK || result.Error == nil || result.Error.Effect != "unknown" {
		t.Fatalf("%+v %v", result, err)
	}
	var effect string
	if err = e.db.QueryRow(`SELECT effect FROM provider_remote_native_completions`).Scan(&effect); err != nil || effect != "unknown" {
		t.Fatal(effect, err)
	}
}

func TestNativeReservationRejectsArbitraryCommand(t *testing.T) {
	e, shell, p := executorFixture(t)
	call := nativeCall(t, e, pr.NativeCall{Identity: shell.Identity}, p)
	ctx := context.Background()
	err := e.transaction(ctx, func(tx *sql.Tx) error {
		b, r, err := pr.ActiveRemoteNativeTx(ctx, tx, call.Identity, e.config.Now())
		if err != nil {
			return err
		}
		call.BindingID = b.ID
		grant, err := pr.AdmitNativeTx(ctx, tx, call, e.config.Now())
		if err != nil {
			return err
		}
		spec, err := filetoolhelper.Command(filetoolhelper.Job{Version: 1, HelperSHA256: e.config.HelperSHA256, Tool: call.Tool, Input: call.Input})
		if err != nil {
			return err
		}
		spec.Argv = []string{"/bin/sh", "-c", "printf forged-result"}
		_, err = remote.PrepareNativeToolTx(ctx, tx, r.Owner, r.Reference, grant.ID, p.owner, &pb.StartExecutionRequest{Execution: &pb.ExecutionSelector{Workspace: "workspace", Sandbox: "k-11", RequestId: uuid.NewString(), Runtime: p.owner.Runtime}, Command: spec})
		return err
	})
	if err == nil {
		t.Fatal("arbitrary command substituted for trusted file helper")
	}
	var count int
	e.db.QueryRow(`SELECT count(*) FROM provider_native_grants`).Scan(&count)
	if count != 0 {
		t.Fatal("rejected reservation left grant")
	}
}
func TestShellDebtBlocksNativeGrant(t *testing.T) {
	e, shell, p := executorFixture(t)
	p.lost = true
	p.pending = true
	if _, err := e.Invoke(context.Background(), shell); !errors.Is(err, ErrUnresolved) {
		t.Fatal(err)
	}
	call := nativeCall(t, e, pr.NativeCall{Identity: shell.Identity}, p)
	if _, err := e.InvokeNative(context.Background(), call); !errors.Is(err, remote.ErrToolBusy) {
		t.Fatal("native helper overlapped shell", err)
	}
	var count int
	e.db.QueryRow(`SELECT count(*) FROM provider_native_grants`).Scan(&count)
	if count != 0 {
		t.Fatal("busy native call left grant")
	}
}
func TestNativeCancellationRetainsUnknownEffect(t *testing.T) {
	e, shell, p := executorFixture(t)
	call := nativeCall(t, e, pr.NativeCall{Identity: shell.Identity}, p)
	p.lost = true
	p.pending = true
	if _, err := e.InvokeNative(context.Background(), call); !errors.Is(err, ErrUnresolved) {
		t.Fatal(err)
	}
	if err := e.Cancel(context.Background(), call.Identity); err != nil {
		t.Fatal(err)
	}
	var effect, outcome string
	e.db.QueryRow(`SELECT c.effect,g.outcome FROM provider_remote_native_completions c JOIN provider_native_grants g ON g.grant_id=c.grant_id`).Scan(&effect, &outcome)
	if effect != "unknown" || outcome != "failed" || p.cancels != 1 {
		t.Fatal(effect, outcome, p.cancels)
	}
}
func TestNativePurposeMigrationPreservesRetainedEvidence(t *testing.T) {
	e, _, _ := executorFixture(t)
	var before, after int
	if err := e.db.QueryRow(`SELECT count(*) FROM managed_execution_identities`).Scan(&before); err != nil {
		t.Fatal(err)
	}
	raw, err := dbmigrations.Read("0022_remote_native_tools.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	// Remove only the new, empty0022 tables to replay the upgrade over retained
	// server and import evidence. The old purpose constraint does not affect these rows.
	err = e.transaction(context.Background(), func(tx *sql.Tx) error {
		if _, err := tx.Exec(`DROP TABLE provider_remote_native_completions; DROP TABLE remote_native_tools`); err != nil {
			return err
		}
		_, err := tx.Exec(string(raw))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = e.db.QueryRow(`SELECT count(*) FROM managed_execution_identities`).Scan(&after); err != nil || after != before {
		t.Fatal(before, after, err)
	}
	rows, err := e.db.Query(`PRAGMA foreign_key_check`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	if rows.Next() {
		t.Fatal("migration lost retained foreign key")
	}
}
