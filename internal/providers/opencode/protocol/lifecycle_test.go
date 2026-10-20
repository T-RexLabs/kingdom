package protocol

import (
	"kingdom/internal/core/providerexecution"
	"testing"
)

// These regressions model the native 2.0.29 producers: SessionInstructions.load,
// SessionStep.attempt retry/Continue, and SessionRunner.runStep compaction.
func lifecycleFixture(t *testing.T) *fixture {
	f := loadFixture(t, "success")
	f.Events = f.Events[:1]
	f.Messages = f.Messages[:1]
	return f
}
func (f *fixture) addEvent(id, kind string, fields map[string]any) {
	f.Events = append(f.Events, f.newEvent(id, kind, fields))
	f.reseq()
}
func (f *fixture) start(id string) {
	f.addEvent("evt_start_"+id, "session.step.started", map[string]any{"assistantMessageID": id, "agent": f.Identity.ProfileID})
}
func (f *fixture) end(id string, failed bool) {
	fields := map[string]any{"assistantMessageID": id, "finish": "stop", "rawFinish": "stop"}
	kind := "session.step.ended"
	if failed {
		kind = "session.step.failed"
		delete(fields, "finish")
		delete(fields, "rawFinish")
		fields["error"] = map[string]any{"type": "provider.transport", "message": "stream interrupted"}
	}
	f.addEvent("evt_end_"+id, kind, fields)
	finish, raw := "stop", "stop"
	if failed {
		finish = "error"
		raw = ""
	}
	f.Messages = append(f.Messages, map[string]any{"id": id, "type": "assistant", "agent": f.Identity.ProfileID, "finish": finish, "rawFinish": raw, "time": map[string]any{"completed": 1}, "content": []any{map[string]any{"type": "text", "text": "answer"}}})
}
func (f *fixture) terminal(outcome string) {
	f.addEvent("evt_idle", "session.execution."+outcome, nil)
	f.Messages = append(f.Messages, map[string]any{"id": "msg_idle", "type": "idle", "outcome": outcome})
}
func (f *fixture) context(id string) {
	fields := map[string]any{"text": "Instructions from: /project/nested/AGENTS.md\nUse project conventions.", "description": "Loaded nested/AGENTS.md", "metadata": map[string]any{"instruction": map[string]any{"paths": []any{"/project/nested/AGENTS.md"}}, "offset": 9007199254740993}}
	f.addEvent("evt_"+id, "session.synthetic", fields)
	m := map[string]any{"id": "msg_" + id, "type": "synthetic"}
	for k, v := range fields {
		m[k] = v
	}
	f.Messages = append(f.Messages, m)
}
func (f *fixture) compact(id string, native, failed bool) {
	f.addEvent("evt_"+id, "session.compaction.started", map[string]any{"reason": "auto"})
	fields := map[string]any{"reason": "auto", "text": "checkpoint", "recent": "recent"}
	m := map[string]any{"id": "msg_" + id, "type": "compaction", "reason": "auto", "status": "completed", "summary": "checkpoint", "recent": "recent"}
	kind := "session.compaction.ended"
	if native {
		fields["text"] = ""
		fields["providerContext"] = []any{map[string]any{"type": "compaction", "encrypted_content": "checkpoint"}}
		m["summary"] = ""
		m["providerContext"] = fields["providerContext"]
	}
	if failed {
		kind = "session.compaction.failed"
		fields = map[string]any{"reason": "auto", "error": map[string]any{"type": "compaction.failed", "message": "provider unavailable"}}
		m["status"] = "failed"
		m["error"] = fields["error"]
		delete(m, "summary")
		delete(m, "recent")
	}
	f.addEvent("evt_end_"+id, kind, fields)
	f.Messages = append(f.Messages, m)
}
func TestVerifyNativeLifecycle(t *testing.T) {
	for _, name := range []string{"instructions during step", "same assistant retry", "recovery after failure", "multiple midrequest checkpoints", "native structured checkpoint", "failed checkpoint", "checkpoint and continuation"} {
		t.Run(name, func(t *testing.T) {
			f := lifecycleFixture(t)
			switch name {
			case "instructions during step":
				f.start("msg_answer")
				f.end("msg_answer", false)
				// A read inserts context during the open assistant; projection creation
				// order places the assistant before the newly discovered instructions.
				f.Events = f.Events[:2]
				f.context("instructions")
				f.addEvent("evt_finish", "session.step.ended", map[string]any{"assistantMessageID": "msg_answer", "finish": "stop", "rawFinish": "stop"})
			case "same assistant retry":
				f.start("msg_answer")
				f.addEvent("evt_retry", "session.retry.scheduled", map[string]any{"assistantMessageID": "msg_answer"})
				f.addEvent("evt_retry_start", "session.step.started", map[string]any{"assistantMessageID": "msg_answer", "agent": f.Identity.ProfileID})
				f.end("msg_answer", false)
			case "recovery after failure":
				f.start("msg_failed")
				f.end("msg_failed", true)
				f.addEvent("evt_retry", "session.retry.scheduled", map[string]any{"assistantMessageID": "msg_failed"})
				f.context("recovery")
				f.start("msg_answer")
				f.end("msg_answer", false)
			case "multiple midrequest checkpoints":
				f.start("msg_first")
				f.end("msg_first", false)
				f.compact("checkpoint_a", false, false)
				f.compact("checkpoint_b", false, false)
				f.start("msg_answer")
				f.end("msg_answer", false)
			case "native structured checkpoint":
				f.compact("checkpoint", true, false)
				f.start("msg_answer")
				f.end("msg_answer", false)
			case "failed checkpoint":
				f.compact("checkpoint", false, true)
				f.terminal(providerexecution.RequestOutcomeFailed)
			case "checkpoint and continuation":
				f.start("msg_first")
				f.end("msg_first", false)
				f.context("continuation")
				f.compact("checkpoint", false, false)
				f.start("msg_answer")
				f.end("msg_answer", false)
			}
			if name != "failed checkpoint" {
				f.terminal(providerexecution.RequestOutcomeSucceeded)
			}
			if _, _, err := verify(t, f); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestVerifySyntheticContextMustMatchProjection(t *testing.T) {
	for _, field := range []string{"id", "text", "description", "metadata"} {
		t.Run(field, func(t *testing.T) {
			f := lifecycleFixture(t)
			f.context("context")
			f.start("msg_answer")
			f.end("msg_answer", false)
			f.terminal("succeeded")
			f.message("synthetic")[field] = "forged"
			if _, _, err := verify(t, f); err == nil {
				t.Fatal("accepted mismatched synthetic context")
			}
		})
	}
}

func TestVerifySyntheticContextDoesNotUseScheduling(t *testing.T) {
	for _, name := range []string{"ordinary finish", "intervening events", "compaction before continuation"} {
		t.Run(name, func(t *testing.T) {
			f := loadFixture(t, "length")
			switch name {
			case "ordinary finish":
				data(f.event("session.step.ended"))["finish"] = "stop"
				f.message("assistant")["finish"] = "stop"
			case "intervening events":
				f.insertEvent(f.eventIndex("session.synthetic"), f.newEvent("evt_gap_before", "session.usage.recorded", nil))
				f.insertEvent(f.eventIndex("session.synthetic")+1, f.newEvent("evt_gap_after", "session.usage.recorded", nil))
			case "compaction before continuation":
				f.insertEvent(1, f.newEvent("evt_checkpoint", "session.compaction.started", map[string]any{"reason": "auto"}))
				f.insertEvent(2, f.newEvent("evt_checkpoint_end", "session.compaction.ended", map[string]any{"reason": "auto", "text": "summary", "recent": "recent"}))
				f.insertMessage(1, map[string]any{"id": "msg_checkpoint", "type": "compaction", "status": "completed", "reason": "auto", "summary": "summary", "recent": "recent"})
			}
			if _, _, err := verify(t, f); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestVerifyRejectsUnprovenSameAssistantRestart(t *testing.T) {
	for _, name := range []string{"no retry event", "retry after output", "wrong retry assistant", "changed retry profile", "reuse after completion"} {
		t.Run(name, func(t *testing.T) {
			f := lifecycleFixture(t)
			f.start("msg_answer")
			if name != "no retry event" {
				id := "msg_answer"
				if name == "wrong retry assistant" {
					id = "msg_foreign"
				}
				f.addEvent("evt_retry", "session.retry.scheduled", map[string]any{"assistantMessageID": id})
			}
			if name == "retry after output" {
				f.addEvent("evt_text", "session.text.started", map[string]any{"assistantMessageID": "msg_answer"})
			}
			if name == "reuse after completion" {
				f.addEvent("evt_old_end", "session.step.ended", map[string]any{"assistantMessageID": "msg_answer", "finish": "stop"})
			}
			profile := f.Identity.ProfileID
			if name == "changed retry profile" {
				profile = "another_profile"
			}
			f.addEvent("evt_restart", "session.step.started", map[string]any{"assistantMessageID": "msg_answer", "agent": profile})
			f.end("msg_answer", false)
			f.terminal("succeeded")
			if _, _, err := verify(t, f); err == nil {
				t.Fatal("accepted unproven assistant restart")
			}
		})
	}
}

func TestVerifyCheckpointContextIntegrity(t *testing.T) {
	for _, name := range []string{"different context", "missing context", "empty context", "error message differs"} {
		t.Run(name, func(t *testing.T) {
			f := lifecycleFixture(t)
			failed := name == "error message differs"
			f.compact("checkpoint", true, failed)
			if failed {
				f.terminal("failed")
			} else {
				f.start("msg_answer")
				f.end("msg_answer", false)
				f.terminal("succeeded")
			}
			switch name {
			case "different context":
				f.message("compaction")["providerContext"] = map[string]any{"different": "checkpoint"}
			case "missing context":
				delete(f.message("compaction"), "providerContext")
			case "empty context":
				data(f.event("session.compaction.ended"))["providerContext"] = map[string]any{}
				f.message("compaction")["providerContext"] = map[string]any{}
			case "error message differs":
				f.message("compaction")["error"] = map[string]any{"type": "compaction.failed", "message": "invented explanation"}
			}
			if _, _, err := verify(t, f); err == nil {
				t.Fatal("accepted inconsistent checkpoint")
			}
		})
	}
}

func TestVerifySyntheticMetadataPreservesIntegersAndIgnoresObjectOrder(t *testing.T) {
	f := lifecycleFixture(t)
	f.context("context")
	f.start("msg_answer")
	f.end("msg_answer", false)
	f.terminal("succeeded")
	original := data(f.event("session.synthetic"))["metadata"].(map[string]any)
	f.message("synthetic")["metadata"] = map[string]any{"offset": original["offset"], "instruction": original["instruction"]}
	if _, _, err := verify(t, f); err != nil {
		t.Fatal(err)
	}
	f.message("synthetic")["metadata"].(map[string]any)["offset"] = 9007199254740992
	if _, _, err := verify(t, f); err == nil {
		t.Fatal("metadata integer change was rounded away")
	}
}

func systemContextFixture(t *testing.T) *fixture {
	f := lifecycleFixture(t)
	// An initial instruction snapshot has no text and creates no projection.
	f.addEvent("evt_initial_instructions", "session.instructions.updated", map[string]any{"delta": map[string]any{"core/date": "first"}})
	f.start("msg_answer")
	f.end("msg_answer", false)
	e := f.newEvent("evt_instructions", "session.instructions.updated", map[string]any{"delta": map[string]any{"core/date": "next"}, "text": "Today's date changed."})
	e["metadata"] = map[string]any{"offset": 9007199254740993}
	f.Events = append(f.Events, e)
	f.reseq()
	f.Messages = append(f.Messages, map[string]any{"id": "msg_instructions", "type": "system", "text": "Today's date changed.", "description": "Instructions updated: core/date", "metadata": map[string]any{"offset": 9007199254740993, "notice": "instructions", "instructionSources": []any{"core/date"}}})
	f.terminal("succeeded")
	return f
}

func TestVerifyNativeSystemContext(t *testing.T) {
	f := systemContextFixture(t)
	if _, _, err := verify(t, f); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"id", "text"} {
		t.Run("mismatched "+field, func(t *testing.T) {
			f := systemContextFixture(t)
			f.message("system")[field] = "forged update"
			if _, _, err := verify(t, f); err == nil {
				t.Fatal("accepted forged system context")
			}
		})
	}
	t.Run("missing context projection", func(t *testing.T) {
		f := systemContextFixture(t)
		f.removeMessage(f.messageIndex("system"))
		if _, _, err := verify(t, f); err == nil {
			t.Fatal("accepted missing system context")
		}
	})
	t.Run("unrelated input remains rejected", func(t *testing.T) {
		f := systemContextFixture(t)
		f.insertEvent(1, f.newEvent("evt_unowned", "session.inbox.enqueued", map[string]any{"inboxID": "msg_other"}))
		if _, _, err := verify(t, f); err == nil {
			t.Fatal("accepted unowned input")
		}
	})
}

func TestVerifySystemContextPresentationDoesNotGrantAuthority(t *testing.T) {
	f := systemContextFixture(t)
	f.message("system")["description"] = "Updated provider display label"
	f.message("system")["metadata"] = map[string]any{"notice": "new presentation metadata"}
	if _, _, err := verify(t, f); err != nil {
		t.Fatalf("provider presentation was mistaken for context authority: %v", err)
	}
}
