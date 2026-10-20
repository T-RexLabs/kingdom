package opencode

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func projectEvents(t *testing.T, events ...string) []ExecuteActivityObservation {
	t.Helper()
	var observed []ExecuteActivityObservation
	projector := newActivityProjector("invocation", func(o ExecuteActivityObservation) { observed = append(observed, o) })
	for _, event := range events {
		projector.project(json.RawMessage(event))
	}
	return observed
}

func TestActivityReportsOnlyWhatTheProviderEmitted(t *testing.T) {
	observed := projectEvents(t,
		`{"type":"session.step.started","created":1792359424000,"data":{"assistantMessageID":"msg_a"}}`,
		`{"type":"session.text.delta","created":1792359424001,"data":{"delta":"x"}}`,
		`{"type":"session.text.ended","created":1792359424002,"data":{"assistantMessageID":"msg_a","ordinal":0,"text":"  "}}`,
		`{"type":"session.text.ended","created":1792359424003,"data":{"assistantMessageID":"msg_a","ordinal":1,"text":"hello"}}`,
		`{"type":"session.step.ended","created":1792359424004,"data":{"assistantMessageID":"msg_a","tokens":{"input":0,"output":0,"reasoning":0,"cache":{"read":0,"write":0}}}}`,
		`{"type":"session.step.ended","created":1792359424005,"data":{"assistantMessageID":"msg_a","tokens":{"input":3,"output":1,"reasoning":0,"cache":{"read":0,"write":0}}}}`,
		`{"type":"session.usage.updated","created":1792359424006,"data":{}}`,
		`not json`,
	)
	var kinds []string
	for _, o := range observed {
		kinds = append(kinds, o.Kind)
	}
	if strings.Join(kinds, ",") != "progress,message,progress" {
		t.Fatalf("kinds %v", kinds)
	}
	message := observed[1]
	if message.ProviderPartID != "msg_a:1" || message.Message.PreviewText() != "hello" || message.InvocationID != "invocation" {
		t.Fatalf("message %#v", message)
	}
	if want := time.UnixMilli(1792359424003).UTC().Format(time.RFC3339); message.OccurredAt != want {
		t.Fatalf("occurred %q want %q", message.OccurredAt, want)
	}
	if observed[0].Label != "Provider step started" || observed[2].Label != "Provider Step Completed" {
		t.Fatalf("labels %q %q", observed[0].Label, observed[2].Label)
	}
}

func TestActivityFollowsAToolCallToItsOutcome(t *testing.T) {
	observed := projectEvents(t,
		`{"type":"session.tool.input.started","created":1792359424000,"data":{"id":"call_1","name":"shell"}}`,
		`{"type":"session.tool.called","created":1792359424001,"data":{"id":"call_1","input":{"command":"false"}}}`,
		`{"type":"session.tool.failed","created":1792359424002,"data":{"id":"call_1","content":[{"type":"text","text":"exit 1"}]}}`,
		`{"type":"session.tool.failed","created":1792359424003,"data":{"id":"call_2"}}`,
	)
	var statuses []string
	for _, o := range observed {
		if o.ToolCallID == "call_1" {
			statuses = append(statuses, o.ToolName+":"+o.ToolStatus)
		}
	}
	if strings.Join(statuses, ",") != "shell:pending,shell:running,shell:error" {
		t.Fatalf("statuses %v", statuses)
	}
	if observed[1].Input.PreviewText() != `{"command":"false"}` || observed[2].Output.PreviewText() != "exit 1" {
		t.Fatalf("content %#v", observed)
	}
	if observed[3].Output.Availability != "not_provided" {
		t.Fatalf("an outcome without output claimed output: %#v", observed[3].Output)
	}
}

func TestEventFeedDeliversInOrderAndEnds(t *testing.T) {
	feed := newEventFeed()
	feed.push(json.RawMessage(`1`))
	feed.push(json.RawMessage(`2`))
	feed.close()
	for _, want := range []string{"1", "2"} {
		if raw, ok := feed.next(context.Background()); !ok || string(raw) != want {
			t.Fatalf("event %s ok=%v", raw, ok)
		}
	}
	if _, ok := feed.next(context.Background()); ok {
		t.Fatal("a closed, drained feed produced an event")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, ok := newEventFeed().next(ctx); ok {
		t.Fatal("an empty feed ignored its context")
	}
}
