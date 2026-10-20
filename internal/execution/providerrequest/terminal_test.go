package providerrequest

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"kingdom/internal/core/providerexecution"
)

func terminalFor(t *testing.T, b Binding, text string, tools ...providerexecution.RequestTool) providerexecution.RequestTerminal {
	t.Helper()
	id := b.Owner.InvocationID
	textRef := providerexecution.ReferenceText(id, text)
	return providerexecution.RequestTerminal{
		Outcome: providerexecution.RequestOutcomeSucceeded, AssistantMessageID: "assistant", RawFinishReason: "stop", FinishReason: "stop",
		Text: &textRef, Tools: tools, Events: providerexecution.ReferenceText(id, "events"), Messages: providerexecution.ReferenceText(id, "messages"),
	}
}

func nativeTool(t *testing.T, call NativeCall, status string) providerexecution.RequestTool {
	t.Helper()
	input, err := providerexecution.ToolInputDigests(call.Input)
	if err != nil {
		t.Fatal(err)
	}
	return providerexecution.RequestTool{ID: call.CallID, MessageID: call.MessageID, Name: call.Tool, Status: status, Input: input}
}

func inputDigests(input string) map[string]string {
	digests, err := providerexecution.ToolInputDigests(json.RawMessage(input))
	if err != nil {
		panic(err)
	}
	return digests
}

func recordTerminal(db *sql.DB, b Binding, terminal providerexecution.RequestTerminal, now time.Time) error {
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := RecordTerminalTx(context.Background(), tx, b.ID, b.Lease, terminal); err != nil {
		return err
	}
	return tx.Commit()
}

func admitted(t *testing.T, kind OwnerKind) (*sql.DB, Binding, time.Time, NativeCall) {
	t.Helper()
	db, b, now := fixture(t, kind)
	bind(t, db, b, now)
	dispatch(t, db, b, now)
	call := native(b)
	if err := transaction(t, db, func(tx *sql.Tx) error { _, err := AdmitNativeTx(context.Background(), tx, call, now); return err }); err != nil {
		t.Fatal(err)
	}
	return db, b, now, call
}

func TestRecordTerminalStoresExactVerifiedRecord(t *testing.T) {
	for _, kind := range []OwnerKind{OwnerRun, OwnerPlanning} {
		t.Run(string(kind), func(t *testing.T) {
			db, b, now, call := admitted(t, kind)
			closeAdmission(t, db, b, now)
			terminal := terminalFor(t, b, "response", nativeTool(t, call, "completed"))
			if err := recordTerminal(db, b, terminal, now); err != nil {
				t.Fatal(err)
			}
			want, _ := terminal.Reference()
			stored, err := Get(context.Background(), db, b.ID)
			if err != nil || stored.TerminalReference == nil || *stored.TerminalReference != want {
				t.Fatalf("stored=%#v want=%#v error=%v", stored.TerminalReference, want, err)
			}
			if err := recordTerminal(db, b, terminal, now); err != nil {
				t.Fatal("exact replay", err)
			}
			if err := recordTerminal(db, b, terminalFor(t, b, "different", nativeTool(t, call, "completed")), now); err == nil {
				t.Fatal("replaced immutable terminal evidence")
			}
			if err := RequireResourcesClosedTx(context.Background(), db, b.ID); err == nil {
				t.Fatal("recording a terminal closed the pending native receipt")
			}
		})
	}
}

func TestRecordTerminalRequiresDispatchAndClosedAdmission(t *testing.T) {
	db, b, now := fixture(t, OwnerRun)
	bind(t, db, b, now)
	terminal := terminalFor(t, b, "response")
	if err := recordTerminal(db, b, terminal, now); err == nil {
		t.Fatal("recorded a terminal for an undispatched request")
	}
	dispatch(t, db, b, now)
	if err := recordTerminal(db, b, terminal, now); err == nil {
		t.Fatal("recorded a terminal while tool admission was open")
	}
	closeAdmission(t, db, b, now)
	if err := recordTerminal(db, b, terminal, now); err != nil {
		t.Fatal(err)
	}
}

func TestRecordTerminalRejectsForeignInvocationAndStaleLease(t *testing.T) {
	db, b, now := fixture(t, OwnerRun)
	bind(t, db, b, now)
	dispatch(t, db, b, now)
	closeAdmission(t, db, b, now)
	foreign := b
	foreign.Owner.InvocationID = "other"
	if err := recordTerminal(db, b, terminalFor(t, foreign, "response"), now); err == nil {
		t.Fatal("recorded evidence from another invocation")
	}
	stale := b
	stale.Lease.Version++
	if err := recordTerminal(db, stale, terminalFor(t, b, "response"), now); err == nil {
		t.Fatal("recorded under a stale lease")
	}
	execSQL(t, db, `UPDATE attempt_executions SET lease_version=2`)
	if err := recordTerminal(db, b, terminalFor(t, b, "response"), now); err == nil {
		t.Fatal("recorded after the canonical lease moved")
	}
}

func TestRecordTerminalMatchesProjectedToolsToDurableAdmissions(t *testing.T) {
	for name, mutate := range map[string]func(NativeCall, *[]providerexecution.RequestTool){
		"missing projected tool": func(_ NativeCall, tools *[]providerexecution.RequestTool) { *tools = nil },
		"other input": func(_ NativeCall, tools *[]providerexecution.RequestTool) {
			(*tools)[0].Input = inputDigests(`{"value":1}`)
		},
		"disjoint input": func(_ NativeCall, tools *[]providerexecution.RequestTool) {
			(*tools)[0].Input = inputDigests(`{"other":1}`)
		},
		"other message": func(_ NativeCall, tools *[]providerexecution.RequestTool) { (*tools)[0].MessageID = "other" },
		"other tool":    func(_ NativeCall, tools *[]providerexecution.RequestTool) { (*tools)[0].Name = "edit" },
		"completed unowned tool": func(call NativeCall, tools *[]providerexecution.RequestTool) {
			extra := (*tools)[0]
			extra.ID, extra.Name = "call-2", "webfetch"
			*tools = append(*tools, extra)
		},
		"completed tool without admission": func(_ NativeCall, tools *[]providerexecution.RequestTool) {
			extra := (*tools)[0]
			extra.ID = "call-2"
			*tools = append(*tools, extra)
		},
	} {
		t.Run(name, func(t *testing.T) {
			db, b, now, call := admitted(t, OwnerRun)
			closeAdmission(t, db, b, now)
			tools := []providerexecution.RequestTool{nativeTool(t, call, "completed")}
			mutate(call, &tools)
			if err := recordTerminal(db, b, terminalFor(t, b, "response", tools...), now); err == nil {
				t.Fatal("recorded a terminal that conflicts with durable admissions")
			}
		})
	}
	t.Run("schema-stripped arguments do not conflict", func(t *testing.T) {
		db, b, now, call := admitted(t, OwnerRun)
		closeAdmission(t, db, b, now)
		projected := nativeTool(t, call, "completed")
		projected.Input = inputDigests(`{"value":9007199254740993,"description":"ignored by the tool schema"}`)
		if err := recordTerminal(db, b, terminalFor(t, b, "response", projected), now); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("refused unowned tool is harmless", func(t *testing.T) {
		db, b, now, call := admitted(t, OwnerRun)
		closeAdmission(t, db, b, now)
		refused := nativeTool(t, call, "error")
		refused.ID, refused.Name = "call-refused", "worktree_remove"
		if err := recordTerminal(db, b, terminalFor(t, b, "response", nativeTool(t, call, "completed"), refused), now); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("refused call needs no admission", func(t *testing.T) {
		db, b, now, call := admitted(t, OwnerRun)
		closeAdmission(t, db, b, now)
		refused := nativeTool(t, call, "error")
		refused.ID = "call-refused"
		if err := recordTerminal(db, b, terminalFor(t, b, "response", nativeTool(t, call, "completed"), refused), now); err != nil {
			t.Fatal(err)
		}
	})
}

func TestRecordTerminalMatchesCommandClaims(t *testing.T) {
	db, b, now := fixture(t, OwnerRun)
	bind(t, db, b, now)
	dispatch(t, db, b, now)
	call := native(b)
	call.Tool, call.CallID = "shell", "command-call"
	call.Input = []byte(`{"command":"go test ./..."}`)
	if err := transaction(t, db, func(tx *sql.Tx) error { _, err := ClaimCommandTx(context.Background(), tx, call, now); return err }); err != nil {
		t.Fatal(err)
	}
	closeAdmission(t, db, b, now)
	if err := recordTerminal(db, b, terminalFor(t, b, "response"), now); err == nil {
		t.Fatal("recorded a terminal that omits a claimed command")
	}
	if err := recordTerminal(db, b, terminalFor(t, b, "response", nativeTool(t, call, "completed")), now); err != nil {
		t.Fatal(err)
	}
}

func TestByIdentityRequiresCompleteMatch(t *testing.T) {
	db, b, now := fixture(t, OwnerPlanning)
	bind(t, db, b, now)
	got, err := ByIdentity(context.Background(), db, b.Identity)
	if err != nil || got.ID != b.ID || got.Lease != b.Lease {
		t.Fatalf("binding=%#v error=%v", got, err)
	}
	for name, mutate := range map[string]func(*providerexecution.RequestIdentity){
		"request":  func(i *providerexecution.RequestIdentity) { i.RequestID = "other" },
		"session":  func(i *providerexecution.RequestIdentity) { i.SessionID = "other" },
		"location": func(i *providerexecution.RequestIdentity) { i.Location = "/other" },
		"profile":  func(i *providerexecution.RequestIdentity) { i.ProfileID = "other" },
		"blank":    func(i *providerexecution.RequestIdentity) { i.Generation = "" },
	} {
		t.Run(name, func(t *testing.T) {
			identity := b.Identity
			mutate(&identity)
			if _, err := ByIdentity(context.Background(), db, identity); err == nil {
				t.Fatal("resolved a binding from a non-matching identity")
			}
		})
	}
}

func TestAuthorityCommitsEachFactByIdentity(t *testing.T) {
	db, b, now := fixture(t, OwnerRun)
	bind(t, db, b, now)
	ctx := context.Background()
	authority := NewAuthority(db, func() time.Time { return now })
	call := native(b)
	call.BindingID = ""
	if _, err := authority.AdmitNative(ctx, call); err == nil {
		t.Fatal("admitted a tool before dispatch")
	}
	if err := authority.IssueDispatch(ctx, b.Identity); err != nil {
		t.Fatal(err)
	}
	grant, err := authority.AdmitNative(ctx, call)
	if err != nil || !grant.Execute {
		t.Fatalf("grant=%#v error=%v", grant, err)
	}
	replay, err := authority.AdmitNative(ctx, call)
	if err != nil || replay.Execute || replay.ID != grant.ID {
		t.Fatalf("replay=%#v error=%v", replay, err)
	}
	started := true
	receipt := NativeReceipt{GrantID: grant.ID, Digest: grant.Digest, Outcome: "completed", Started: &started}
	foreign := b.Identity
	foreign.RequestID = "other"
	if err := authority.FinishNative(ctx, foreign, receipt); err == nil {
		t.Fatal("accepted a receipt for another request identity")
	}
	if err := authority.FinishNative(ctx, b.Identity, receipt); err != nil {
		t.Fatal(err)
	}
	if err := authority.CloseAdmission(ctx, b.Identity); err != nil {
		t.Fatal(err)
	}
	if err := authority.CheckTerminal(ctx, b.Identity, terminalFor(t, b, "response", nativeTool(t, call, "completed"))); err != nil {
		t.Fatal(err)
	}
	if err := RequireResourcesClosedTx(ctx, db, b.ID); err != nil {
		t.Fatal(err)
	}
}
