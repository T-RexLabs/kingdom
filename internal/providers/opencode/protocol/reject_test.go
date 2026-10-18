package protocol

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"

	"kingdom/internal/core/providerexecution"
)

type rejection struct {
	name   string
	mutate func(f *fixture)
	tweak  func(t *testing.T, ev *Evidence, id *providerexecution.RequestIdentity)
	// want is the rule that must reject the evidence, so a case cannot pass by
	// tripping an unrelated check.
	want string
}

func runRejections(t *testing.T, name string, cases []rejection) {
	t.Helper()
	for _, tt := range cases {
		t.Run(name+"/"+tt.name, func(t *testing.T) {
			f := loadFixture(t, name)
			if tt.mutate != nil {
				tt.mutate(f)
			}
			ev, id := f.evidence(), f.Identity
			if tt.tweak != nil {
				tt.tweak(t, &ev, &id)
			}
			var text bytes.Buffer
			_, err := Verify(context.Background(), id, ev, &text)
			if err == nil {
				t.Fatal("invalid evidence accepted")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("rejected by the wrong rule: %v (want %q)", err, tt.want)
			}
			if text.Len() != 0 {
				t.Fatalf("rejected evidence still wrote %d text bytes", text.Len())
			}
		})
	}
}

func sourceMutation(pick func(*Evidence) *Source, mutate func(raw []byte) []byte) func(*testing.T, *Evidence, *providerexecution.RequestIdentity) {
	return func(t *testing.T, ev *Evidence, _ *providerexecution.RequestIdentity) {
		src := pick(ev)
		*src = rawSource(mutate(readSource(t, *src)))
	}
}

func eventsSource(ev *Evidence) *Source   { return &ev.Events }
func messagesSource(ev *Evidence) *Source { return &ev.Messages }

func TestVerifyRejectsOrdinaryEvidence(t *testing.T) {
	zeros := strings.Repeat("0", 64)
	runRejections(t, "success", []rejection{
		{name: "missing-request-projection", mutate: func(f *fixture) { f.removeMessage(0) }, want: "must start with user request"},
		{name: "request-projection-not-user", mutate: func(f *fixture) { f.Messages[0]["type"] = "synthetic" }, want: "must start with user request"},
		{name: "duplicate-request-projection", mutate: func(f *fixture) { f.insertMessage(1, clone(f.t, f.Messages[0])) }, want: "duplicate request projection"},
		{name: "missing-delivery", mutate: func(f *fixture) { f.removeEvent(0) }, want: "must start with the located delivery"},
		{name: "delivery-without-location", mutate: func(f *fixture) { delete(f.Events[0], "location") }, want: "must start with the located delivery"},
		{name: "delivery-of-another-request", mutate: func(f *fixture) { data(f.Events[0])["inboxID"] = "msg_other" }, want: "must start with the located delivery"},
		{name: "duplicate-delivery", mutate: func(f *fixture) {
			duplicate := clone(f.t, f.Events[0])
			duplicate["id"] = "evt_duplicate_delivery"
			f.insertEvent(1, duplicate)
		}, want: "additional inbox input"},
		{name: "additional-inbox-input", mutate: func(f *fixture) {
			f.insertEvent(1, f.newEvent("evt_extra_input", "session.inbox.enqueued", map[string]any{"inboxID": "msg_other"}))
		}, want: "additional inbox input"},
		{name: "agent-selected", mutate: func(f *fixture) {
			f.insertEvent(1, f.newEvent("evt_change", "session.agent.selected", map[string]any{"agent": "other"}))
		}, want: "session identity changed"},
		{name: "model-selected", mutate: func(f *fixture) {
			f.insertEvent(1, f.newEvent("evt_change", "session.model.selected", nil))
		}, want: "session identity changed"},
		{name: "location-switched", mutate: func(f *fixture) {
			f.insertEvent(1, f.newEvent("evt_change", "session.location.switched", nil))
		}, want: "session identity changed"},
		{name: "session-moved", mutate: func(f *fixture) {
			f.insertEvent(1, f.newEvent("evt_change", "session.moved", nil))
		}, want: "session identity changed"},
		{name: "unclassified-execution-event", mutate: func(f *fixture) {
			f.insertEvent(1, f.newEvent("evt_restart", "session.execution.started", nil))
		}, want: "unclassified execution event"},
		{name: "wrong-location", mutate: func(f *fixture) {
			f.event("session.step.started")["location"].(map[string]any)["directory"] = "/other/location"
		}, want: "another location"},
		{name: "wrong-session", mutate: func(f *fixture) { data(f.event("session.step.started"))["sessionID"] = "ses_other" }, want: "another session"},
		{name: "wrong-aggregate", mutate: func(f *fixture) {
			f.event("session.step.started")["durable"].(map[string]any)["aggregateID"] = "ses_other"
		}, want: "another session"},
		{name: "event-without-session", mutate: func(f *fixture) {
			delete(data(f.event("session.step.ended")), "sessionID")
			delete(f.event("session.step.ended")["durable"].(map[string]any), "aggregateID")
		}, want: "another session"},
		{name: "wrong-profile-step", mutate: func(f *fixture) { data(f.event("session.step.started"))["agent"] = "other-profile" }, want: "does not belong to profile"},
		{name: "wrong-profile-assistant", mutate: func(f *fixture) { f.message("assistant")["agent"] = "other-profile" }, want: "matches no step of profile"},
		{name: "additional-user-message", mutate: func(f *fixture) {
			f.insertMessage(1, map[string]any{"id": "msg_other_input", "type": "user"})
		}, want: `unclassified "user" message`},
		{name: "agent-switched", mutate: func(f *fixture) {
			f.insertMessage(1, map[string]any{"id": "msg_switch", "type": "agent-switched"})
		}, want: `unclassified "agent-switched" message`},
		{name: "unknown-message-kind", mutate: func(f *fixture) {
			f.insertMessage(1, map[string]any{"id": "msg_unknown", "type": "background-job"})
		}, want: `unclassified "background-job" message`},
		{name: "event-after-terminal", mutate: func(f *fixture) {
			f.Events = append(f.Events, f.newEvent("evt_late", "session.text.started", nil))
			f.reseq()
		}, want: "follows the terminal execution event"},
		{name: "duplicate-outcome", mutate: func(f *fixture) {
			duplicate := clone(f.t, f.lastEvent("session.execution.succeeded"))
			duplicate["id"] = "evt_second_outcome"
			f.Events = append(f.Events, duplicate)
			f.reseq()
		}, want: "follows the terminal execution event"},
		{name: "conflicting-outcome", mutate: func(f *fixture) {
			f.Events = append(f.Events, f.newEvent("evt_second_outcome", "session.execution.failed", nil))
			f.reseq()
		}, want: "follows the terminal execution event"},
		{name: "terminal-missing", mutate: func(f *fixture) { f.removeEvent(len(f.Events) - 1) }, want: "must end with the terminal execution event"},
		{name: "sequence-repeats", mutate: func(f *fixture) {
			f.Events[3]["durable"].(map[string]any)["seq"] = f.Events[2]["durable"].(map[string]any)["seq"]
		}, want: "strictly increasing"},
		{name: "sequence-decreases", mutate: func(f *fixture) {
			f.Events[3]["durable"].(map[string]any)["seq"] = f.Events[0]["durable"].(map[string]any)["seq"]
		}, want: "strictly increasing"},
		{name: "event-not-public", mutate: func(f *fixture) { delete(f.Events[3], "durable") }, want: "strictly increasing"},
		{name: "duplicate-event-id", mutate: func(f *fixture) { f.Events[3]["id"] = f.Events[2]["id"] }, want: "duplicate event"},
		{name: "empty-event-id", mutate: func(f *fixture) { f.Events[3]["id"] = "" }, want: "event identity is required"},
		{name: "wrong-watermark", mutate: func(f *fixture) { f.Watermark++ }, want: "at watermark"},
		{name: "unhealthy-capture", tweak: func(_ *testing.T, ev *Evidence, _ *providerexecution.RequestIdentity) { ev.CaptureHealthy = false }, want: "not continuous"},
		{name: "wait-not-completed", tweak: func(_ *testing.T, ev *Evidence, _ *providerexecution.RequestIdentity) { ev.WaitCompleted = false }, want: "wait for idle"},
		{name: "incomplete-identity", tweak: func(_ *testing.T, _ *Evidence, id *providerexecution.RequestIdentity) { id.Generation = "" }, want: "identity is required"},
		{name: "relative-location", tweak: func(_ *testing.T, _ *Evidence, id *providerexecution.RequestIdentity) { id.Location = "relative" }, want: "must be absolute"},
		{name: "another-location", tweak: func(_ *testing.T, _ *Evidence, id *providerexecution.RequestIdentity) {
			id.Location = "/other/location"
		}, want: "another location"},
		{name: "another-session-identity", tweak: func(_ *testing.T, _ *Evidence, id *providerexecution.RequestIdentity) { id.SessionID = "ses_other" }, want: "another session"},
		{name: "another-profile-identity", tweak: func(_ *testing.T, _ *Evidence, id *providerexecution.RequestIdentity) { id.ProfileID = "other-profile" }, want: "does not belong to profile"},
		{name: "another-request-identity", tweak: func(_ *testing.T, _ *Evidence, id *providerexecution.RequestIdentity) { id.RequestID = "msg_other" }, want: "must start with the located delivery"},
		{name: "missing-invocation", tweak: func(_ *testing.T, ev *Evidence, _ *providerexecution.RequestIdentity) { ev.InvocationID = "" }, want: "invocation"},
		{name: "foreign-invocation", tweak: func(_ *testing.T, ev *Evidence, _ *providerexecution.RequestIdentity) {
			ev.InvocationID = "other-invocation"
		}, want: "lineage"},
		{name: "events-hash-mismatch", tweak: func(_ *testing.T, ev *Evidence, _ *providerexecution.RequestIdentity) { ev.Events.Ref.SHA256 = zeros }, want: "hash or size mismatch"},
		{name: "messages-hash-mismatch", tweak: func(_ *testing.T, ev *Evidence, _ *providerexecution.RequestIdentity) { ev.Messages.Ref.SHA256 = zeros }, want: "hash or size mismatch"},
		{name: "events-size-mismatch", tweak: func(_ *testing.T, ev *Evidence, _ *providerexecution.RequestIdentity) { ev.Events.Ref.SizeBytes-- }, want: "exceeds declared size"},
		{name: "events-truncated", tweak: sourceMutation(eventsSource, func(raw []byte) []byte { return raw[:len(raw)-10] }), want: "events"},
		{name: "messages-truncated", tweak: sourceMutation(messagesSource, func(raw []byte) []byte { return raw[:len(raw)-10] }), want: "messages"},
		{name: "events-trailing-json", tweak: sourceMutation(eventsSource, func(raw []byte) []byte { return append(raw, " []"...) }), want: "trailing JSON"},
		{name: "messages-trailing-json", tweak: sourceMutation(messagesSource, func(raw []byte) []byte { return append(raw, " {}"...) }), want: "trailing JSON"},
		{name: "events-not-an-array", tweak: sourceMutation(eventsSource, func([]byte) []byte { return []byte(`{"events":[]}`) }), want: "JSON array is required"},
		{name: "unavailable-opener", tweak: func(_ *testing.T, ev *Evidence, _ *providerexecution.RequestIdentity) { ev.Messages.Open = nil }, want: "content opener is required"},
		{name: "opener-failure", tweak: func(_ *testing.T, ev *Evidence, _ *providerexecution.RequestIdentity) {
			ev.Events.Open = func(context.Context) (io.ReadCloser, error) { return nil, io.ErrClosedPipe }
		}, want: io.ErrClosedPipe.Error()},
		{name: "duplicate-step", mutate: func(f *fixture) {
			duplicate := clone(f.t, f.event("session.step.started"))
			duplicate["id"] = "evt_duplicate_step"
			f.insertEvent(f.eventIndex("session.step.ended")+1, duplicate)
		}, want: "duplicate step"},
		{name: "duplicate-step-completion", mutate: func(f *fixture) {
			duplicate := clone(f.t, f.event("session.step.ended"))
			duplicate["id"] = "evt_duplicate_end"
			f.insertEvent(f.eventIndex("session.step.ended")+1, duplicate)
		}, want: "matches no open step"},
		{name: "step-completion-without-step", mutate: func(f *fixture) { data(f.event("session.step.ended"))["assistantMessageID"] = "msg_other" }, want: "matches no open step"},
		{name: "step-completion-without-finish", mutate: func(f *fixture) { delete(data(f.event("session.step.ended")), "finish") }, want: "without a finish reason"},
		{name: "step-never-completes", mutate: func(f *fixture) { f.removeEvent(f.eventIndex("session.step.ended")) }, want: "lacks a completed final step"},
		{name: "step-failed-at-success", mutate: func(f *fixture) { f.event("session.step.ended")["type"] = "session.step.failed" }, want: "lacks a completed final step"},
		{name: "duplicate-idle", mutate: func(f *fixture) { f.Messages = append(f.Messages, clone(f.t, f.lastMessage("idle"))) }, want: "follows the idle boundary"},
		{name: "work-after-idle", mutate: func(f *fixture) { f.Messages = append(f.Messages, clone(f.t, f.lastMessage("assistant"))) }, want: "follows the idle boundary"},
		{name: "missing-idle", mutate: func(f *fixture) { f.removeMessage(f.messageIndex("idle")) }, want: "must end with the idle boundary"},
		{name: "idle-outcome-conflict", mutate: func(f *fixture) { f.message("idle")["outcome"] = "interrupted" }, want: "idle outcome"},
		{name: "assistant-finish-conflict", mutate: func(f *fixture) { f.message("assistant")["finish"] = "length" }, want: "finish conflicts"},
		{name: "assistant-raw-finish-conflict", mutate: func(f *fixture) { f.message("assistant")["rawFinish"] = "length" }, want: "finish conflicts"},
		{name: "assistant-identity-conflict", mutate: func(f *fixture) { f.message("assistant")["id"] = "msg_other" }, want: "matches no step"},
		{name: "summary-without-assistant", mutate: func(f *fixture) { f.removeMessage(f.messageIndex("assistant")) }, want: "have no assistant projection"},
		{name: "assistant-incomplete", mutate: func(f *fixture) { delete(f.message("assistant")["time"].(map[string]any), "completed") }, want: "completed assistant"},
		{name: "final-finish-not-stop", mutate: func(f *fixture) {
			data(f.event("session.step.ended"))["finish"] = "tool-calls"
			f.message("assistant")["finish"] = "tool-calls"
		}, want: "stopped normally"},
		{name: "blank-final-text", mutate: func(f *fixture) {
			f.message("assistant")["content"] = []any{map[string]any{"type": "text", "text": " \n\t"}}
		}, want: "no final assistant text"},
		{name: "empty-final-content", mutate: func(f *fixture) { f.message("assistant")["content"] = []any{} }, want: "no final assistant text"},
	})
}

func TestVerifyRequiresCaptureAndWaitForEveryOutcome(t *testing.T) {
	for _, name := range []string{"success", "tools", "length", "compaction", "compaction-cancelled", "interrupted", "failed", "failed-content-filter"} {
		runRejections(t, name, []rejection{
			{name: "unhealthy-capture", tweak: func(_ *testing.T, ev *Evidence, _ *providerexecution.RequestIdentity) { ev.CaptureHealthy = false }, want: "not continuous"},
			{name: "wait-not-completed", tweak: func(_ *testing.T, ev *Evidence, _ *providerexecution.RequestIdentity) { ev.WaitCompleted = false }, want: "wait for idle"},
		})
	}
}

func TestVerifyRejectsToolEvidence(t *testing.T) {
	appendTool := func(f *fixture, tool map[string]any) {
		last := f.lastMessage("assistant")
		last["content"] = append(last["content"].([]any), tool)
	}
	firstTool := func(f *fixture) map[string]any { return f.message("assistant")["content"].([]any)[0].(map[string]any) }
	runRejections(t, "tools", []rejection{
		{name: "duplicate-call-id", mutate: func(f *fixture) { appendTool(f, clone(f.t, firstTool(f))) }, want: "incomplete or repeated"},
		{name: "pending-tool-at-success", mutate: func(f *fixture) { firstTool(f)["state"].(map[string]any)["status"] = "pending" }, want: "at success"},
		{name: "running-tool-at-success", mutate: func(f *fixture) { firstTool(f)["state"].(map[string]any)["status"] = "running" }, want: "at success"},
		{name: "tool-without-status", mutate: func(f *fixture) { delete(firstTool(f)["state"].(map[string]any), "status") }, want: "incomplete or repeated"},
		{name: "tool-without-name", mutate: func(f *fixture) { delete(firstTool(f), "name") }, want: "incomplete or repeated"},
		{name: "tool-input-not-an-object", mutate: func(f *fixture) { firstTool(f)["state"].(map[string]any)["input"] = "text" }, want: "JSON object"},
		{name: "tool-input-missing", mutate: func(f *fixture) { delete(firstTool(f)["state"].(map[string]any), "input") }, want: "JSON object"},
	})
}

func TestVerifyRejectsLengthContinuationWithoutCausalProof(t *testing.T) {
	lastEnded := func(f *fixture) map[string]any { return data(f.events("session.step.ended")[1]) }
	runRejections(t, "length", []rejection{
		{name: "watermark-beyond-events", mutate: func(f *fixture) { f.Watermark++ }, want: "at watermark"},
		{name: "delivery-missing", mutate: func(f *fixture) { f.removeEvent(0) }, want: "must start with the located delivery"},
		{name: "synthetic-event-missing", mutate: func(f *fixture) { f.removeEvent(f.eventIndex("session.synthetic")) }, want: "matches no length continuation event"},
		{name: "sequence-jumps-back", mutate: func(f *fixture) { f.Events[len(f.Events)/2]["durable"].(map[string]any)["seq"] = json.Number("999") }, want: "strictly increasing"},
		{name: "synthetic-location", mutate: func(f *fixture) {
			f.event("session.synthetic")["location"].(map[string]any)["directory"] = "/other/location"
		}, want: "another location"},
		{name: "first-event-session", mutate: func(f *fixture) { data(f.Events[0])["sessionID"] = "ses_wrong" }, want: "another session"},
		{name: "first-event-aggregate", mutate: func(f *fixture) { f.Events[0]["durable"].(map[string]any)["aggregateID"] = "ses_wrong" }, want: "another session"},
		{name: "synthetic-event-identity", mutate: func(f *fixture) { f.event("session.synthetic")["id"] = "evt_wrong" }, want: "matches no length continuation event"},
		{name: "synthetic-after-ordinary-step", mutate: func(f *fixture) { data(f.event("session.step.ended"))["finish"] = "stop" }, want: "does not follow a length-limited step"},
		{name: "synthetic-after-another-event", mutate: func(f *fixture) {
			f.insertEvent(f.eventIndex("session.synthetic"), f.newEvent("evt_gap", "session.text.started", nil))
		}, want: "does not follow a length-limited step"},
		{name: "synthetic-without-resumed-step", mutate: func(f *fixture) {
			f.insertEvent(f.eventIndex("session.synthetic")+1, f.newEvent("evt_gap", "session.text.started", nil))
		}, want: "not followed by a step"},
		{name: "resumed-step-of-another-profile", mutate: func(f *fixture) { data(f.events("session.step.started")[1])["agent"] = "wrong-profile" }, want: "does not belong to profile"},
		{name: "extra-inbox-input", mutate: func(f *fixture) {
			e := f.event("session.text.started")
			e["type"] = "session.inbox.enqueued"
			data(e)["inboxID"] = "msg_other"
		}, want: "additional inbox input"},
		{name: "final-step-finish", mutate: func(f *fixture) { lastEnded(f)["finish"] = "length" }, want: "finish conflicts"},
		{name: "final-step-raw-finish", mutate: func(f *fixture) { lastEnded(f)["rawFinish"] = "length" }, want: "finish conflicts"},
		{name: "final-step-identity", mutate: func(f *fixture) { lastEnded(f)["assistantMessageID"] = "msg_other" }, want: "matches no open step"},
		{name: "intervening-failure", mutate: func(f *fixture) { f.event("session.text.started")["type"] = "session.execution.failed" }, want: "follows the terminal execution event"},
		{name: "intervening-interruption", mutate: func(f *fixture) { f.event("session.text.started")["type"] = "session.execution.interrupted" }, want: "follows the terminal execution event"},
		{name: "intervening-compaction", mutate: func(f *fixture) { f.event("session.text.started")["type"] = "session.compaction.started" }, want: "single automatic compaction"},
		{name: "synthetic-projection-identity", mutate: func(f *fixture) { f.message("synthetic")["id"] = "msg_wrong" }, want: "matches no length continuation event"},
		{name: "synthetic-projection-missing", mutate: func(f *fixture) { f.removeMessage(f.messageIndex("synthetic")) }, want: "have no synthetic projection"},
		{name: "synthetic-projection-repeated", mutate: func(f *fixture) {
			f.insertMessage(f.messageIndex("synthetic")+1, clone(f.t, f.message("synthetic")))
		}, want: "does not resume with assistant"},
		{name: "synthetic-projection-misplaced", mutate: func(f *fixture) {
			synthetic := f.message("synthetic")
			f.removeMessage(f.messageIndex("synthetic"))
			f.insertMessage(f.messageIndex("idle"), synthetic)
		}, want: "does not follow the length-limited assistant"},
		{name: "assistant-projection-profile", mutate: func(f *fixture) { f.message("assistant")["agent"] = "wrong-profile" }, want: "matches no step of profile"},
		{name: "premature-idle", mutate: func(f *fixture) {
			synthetic := f.message("synthetic")
			synthetic["type"] = "idle"
			synthetic["outcome"] = "succeeded"
		}, want: "follows the idle boundary"},
		{name: "length-with-compaction", mutate: func(f *fixture) {
			f.insertEvent(1, f.newEvent("evt_compaction_start", "session.compaction.started", map[string]any{"reason": "auto"}))
			f.insertEvent(2, f.newEvent("evt_compaction_end", "session.compaction.ended", map[string]any{"reason": "auto", "text": "summary", "recent": "recent"}))
			f.insertMessage(1, map[string]any{"id": "msg_compaction_start", "type": "compaction", "status": "completed", "reason": "auto", "summary": "summary", "recent": "recent"})
		}, want: "length continuation combined with compaction"},
	})
}

func TestVerifyRejectsConflictingCompactionCheckpoint(t *testing.T) {
	runRejections(t, "compaction", []rejection{
		{name: "resumed-profile", mutate: func(f *fixture) { data(f.event("session.step.started"))["agent"] = "another-profile" }, want: "does not belong to profile"},
		{name: "resumed-finish", mutate: func(f *fixture) { data(f.event("session.step.ended"))["finish"] = "length" }, want: "finish conflicts"},
		{name: "checkpoint-summary", mutate: func(f *fixture) { f.message("compaction")["summary"] = "changed summary" }, want: "conflicts with its checkpoint event"},
		{name: "checkpoint-recent", mutate: func(f *fixture) { f.message("compaction")["recent"] = "changed recent" }, want: "conflicts with its checkpoint event"},
		{name: "checkpoint-not-completed", mutate: func(f *fixture) { f.message("compaction")["status"] = "failed" }, want: "conflicts with its checkpoint event"},
		{name: "manual-reason", mutate: func(f *fixture) { data(f.event("session.compaction.started"))["reason"] = "manual" }, want: "single automatic compaction"},
		{name: "owned-input", mutate: func(f *fixture) { data(f.event("session.compaction.started"))["inputID"] = "msg_input" }, want: "single automatic compaction"},
		{name: "end-reason", mutate: func(f *fixture) { data(f.event("session.compaction.ended"))["reason"] = "manual" }, want: "no matching automatic start"},
		{name: "missing-end", mutate: func(f *fixture) { f.event("session.compaction.ended")["type"] = "session.unclassified" }, want: "starts before compaction completed"},
		{name: "end-without-summary", mutate: func(f *fixture) { data(f.event("session.compaction.ended"))["text"] = " " }, want: "has no summary"},
		{name: "duplicate-start", mutate: func(f *fixture) {
			duplicate := clone(f.t, f.event("session.compaction.started"))
			duplicate["id"] = "evt_second_start"
			f.insertEvent(f.eventIndex("session.compaction.ended"), duplicate)
		}, want: "single automatic compaction"},
		{name: "duplicate-end", mutate: func(f *fixture) {
			duplicate := clone(f.t, f.event("session.compaction.ended"))
			duplicate["id"] = "evt_second_end"
			f.insertEvent(f.eventIndex("session.compaction.ended")+1, duplicate)
		}, want: "no matching automatic start"},
		{name: "intermediate-terminal", mutate: func(f *fixture) { f.event("session.text.started")["type"] = "session.execution.failed" }, want: "follows the terminal execution event"},
		{name: "projection-identity", mutate: func(f *fixture) { f.message("compaction")["id"] = "msg_other" }, want: "matches no automatic compaction event"},
		{name: "projection-reason", mutate: func(f *fixture) { f.message("compaction")["reason"] = "manual" }, want: "matches no automatic compaction event"},
		{name: "projection-missing", mutate: func(f *fixture) { f.removeMessage(f.messageIndex("compaction")) }, want: "have no compaction projection"},
		{name: "projection-repeated", mutate: func(f *fixture) {
			f.insertMessage(f.messageIndex("compaction")+1, clone(f.t, f.message("compaction")))
		}, want: "matches no automatic compaction event"},
		{name: "projection-after-assistant", mutate: func(f *fixture) {
			compaction := f.message("compaction")
			f.removeMessage(f.messageIndex("compaction"))
			f.insertMessage(f.messageIndex("idle"), compaction)
		}, want: "matches no automatic compaction event"},
		{name: "summary-as-final-output", mutate: func(f *fixture) {
			for _, eventType := range []string{"session.step.started", "session.text.started", "session.text.ended", "session.step.streamed", "session.step.ended"} {
				f.removeEvent(f.eventIndex(eventType))
			}
			f.removeMessage(f.messageIndex("assistant"))
		}, want: "lacks a completed final step"},
	})
}

func TestVerifyRejectsConflictingCompactionCancellation(t *testing.T) {
	runRejections(t, "compaction-cancelled", []rejection{
		{name: "duplicate-request", mutate: func(f *fixture) { f.insertMessage(1, clone(f.t, f.Messages[0])) }, want: "duplicate request projection"},
		{name: "non-user-request", mutate: func(f *fixture) { f.Messages[0]["type"] = "synthetic" }, want: "must start with user request"},
		{name: "missing-delivery-location", mutate: func(f *fixture) { delete(f.Events[0], "location") }, want: "must start with the located delivery"},
		{name: "missing-delivery", mutate: func(f *fixture) { f.removeEvent(0) }, want: "must start with the located delivery"},
		{name: "missing-wait", tweak: func(_ *testing.T, ev *Evidence, _ *providerexecution.RequestIdentity) { ev.WaitCompleted = false }, want: "wait for idle"},
		{name: "invented-assistant", mutate: func(f *fixture) {
			f.insertMessage(f.messageIndex("idle"), map[string]any{"id": "msg_invented", "type": "assistant", "agent": f.Identity.ProfileID, "finish": "stop", "rawFinish": "stop", "content": []any{}})
		}, want: "matches no step"},
		{name: "invented-final", mutate: func(f *fixture) {
			f.insertEvent(f.eventIndex("session.execution.interrupted"), f.newEvent("evt_invented_step", "session.step.started", map[string]any{"agent": f.Identity.ProfileID, "assistantMessageID": "msg_invented"}))
		}, want: "starts before compaction completed"},
		{name: "wrong-idle", mutate: func(f *fixture) { f.message("idle")["outcome"] = "succeeded" }, want: "idle outcome"},
		{name: "capture-lost", tweak: func(_ *testing.T, ev *Evidence, _ *providerexecution.RequestIdentity) { ev.CaptureHealthy = false }, want: "not continuous"},
		{name: "events-hash", tweak: func(_ *testing.T, ev *Evidence, _ *providerexecution.RequestIdentity) {
			ev.Events.Ref.SHA256 = strings.Repeat("a", 64)
		}, want: "hash or size mismatch"},
		{name: "dropped-compaction-start", mutate: func(f *fixture) { f.removeEvent(f.eventIndex("session.compaction.started")) }, want: "no matching automatic start"},
		{name: "wrong-location", tweak: func(_ *testing.T, _ *Evidence, id *providerexecution.RequestIdentity) {
			id.Location = "/another/location"
		}, want: "another location"},
		{name: "missing-generation", tweak: func(_ *testing.T, _ *Evidence, id *providerexecution.RequestIdentity) { id.Generation = "" }, want: "identity is required"},
		{name: "non-increasing-sequence", mutate: func(f *fixture) {
			f.Events[1]["durable"].(map[string]any)["seq"] = f.Events[0]["durable"].(map[string]any)["seq"]
		}, want: "strictly increasing"},
		{name: "failure-without-interruption", mutate: func(f *fixture) {
			data(f.event("session.compaction.failed"))["error"].(map[string]any)["type"] = "compaction.model"
		}, want: "failed without interruption evidence"},
		{name: "failure-without-error", mutate: func(f *fixture) { delete(data(f.event("session.compaction.failed")), "error") }, want: "failed without interruption evidence"},
		{name: "projection-error-conflict", mutate: func(f *fixture) {
			f.message("compaction")["error"].(map[string]any)["type"] = "compaction.model"
		}, want: "conflicts with its interruption event"},
		{name: "projection-completed", mutate: func(f *fixture) { f.message("compaction")["status"] = "completed" }, want: "conflicts with its interruption event"},
		{name: "projection-missing", mutate: func(f *fixture) { f.removeMessage(f.messageIndex("compaction")) }, want: "have no compaction projection"},
		{name: "failed-outcome", mutate: func(f *fixture) {
			f.event("session.execution.interrupted")["type"] = "session.execution.failed"
			f.message("idle")["outcome"] = "failed"
		}, want: "does not match the failed outcome"},
		{name: "compaction-never-finished", mutate: func(f *fixture) { f.removeEvent(f.eventIndex("session.compaction.failed")) }, want: "does not match the interrupted outcome"},
	})
}

func TestVerifyRejectsInconsistentFailureEvidence(t *testing.T) {
	runRejections(t, "interrupted", []rejection{
		{name: "fabricated-finish", mutate: func(f *fixture) { f.lastMessage("assistant")["finish"] = "stop" }, want: "finish conflicts"},
		{name: "idle-succeeded", mutate: func(f *fixture) { f.message("idle")["outcome"] = "succeeded" }, want: "idle outcome"},
		{name: "step-after-failed-step", mutate: func(f *fixture) {
			f.insertEvent(f.eventIndex("session.step.failed")+1, f.newEvent("evt_next_step", "session.step.started", map[string]any{"agent": f.Identity.ProfileID, "assistantMessageID": "msg_next"}))
		}, want: "starts before step"},
	})
	runRejections(t, "failed", []rejection{
		{name: "idle-error-conflict", mutate: func(f *fixture) { f.message("idle")["error"].(map[string]any)["type"] = "provider.timeout" }, want: "idle error"},
		{name: "idle-without-error", mutate: func(f *fixture) { delete(f.message("idle"), "error") }, want: "idle error"},
		{name: "event-without-error", mutate: func(f *fixture) { delete(data(f.event("session.execution.failed")), "error") }, want: "idle error"},
	})
}
