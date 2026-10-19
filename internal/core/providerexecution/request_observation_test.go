package providerexecution

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"
)

func requestTerminalFixture(invocationID string) RequestTerminal {
	text := ReferenceText(invocationID, "Complete response.")
	input := sha256.Sum256([]byte(`"a.txt"`))
	return RequestTerminal{
		Outcome: RequestOutcomeSucceeded, AssistantMessageID: "assistant-message",
		RawFinishReason: "stop", FinishReason: "stop", Text: &text,
		Tools:    []RequestTool{{ID: "call-1", MessageID: "assistant-message", Name: "write", Status: "completed", Input: map[string]string{"filePath": hex.EncodeToString(input[:])}}},
		Events:   ReferenceText(invocationID, "events"),
		Messages: ReferenceText(invocationID, "messages"),
	}
}

func successfulRequestObservation() InvocationStreamObservation {
	terminal := requestTerminalFixture("invocation")
	return InvocationStreamObservation{
		Request: &RequestObservation{
			Identity: RequestIdentity{
				InstanceID: "instance", Generation: "generation", Location: "/project",
				SessionID: "session", ProfileID: "profile", RequestID: "request",
			},
			Terminal: terminal,
		},
		ProviderSessionID: "session", FinalTurnID: "assistant-message",
		RawFinishReason: "stop", NormalizedFinishReason: "stop", CanonicalTerminalText: "Complete response.",
		ToolCalls: terminal.ToolCalls(),
	}
}

func TestRequestResponseSuccessNeedsOnlyTheRequestObservation(t *testing.T) {
	observation := successfulRequestObservation()
	if err := ValidateServerResponseSuccess(observation); err != nil {
		t.Fatalf("shared request success must hold on the request observation alone: %v", err)
	}
}

func TestRequestTerminalReferenceBindsEveryRecordedFact(t *testing.T) {
	base := requestTerminalFixture("invocation")
	want, err := base.Reference()
	if err != nil {
		t.Fatal(err)
	}
	if again, _ := base.Reference(); again != want {
		t.Fatal("reference is not deterministic")
	}
	for name, mutate := range map[string]func(*RequestTerminal){
		"assistant": func(t *RequestTerminal) { t.AssistantMessageID = "other" },
		"raw":       func(t *RequestTerminal) { t.RawFinishReason = "other" },
		"tool":      func(t *RequestTerminal) { t.Tools[0].Status = "error" },
		"events":    func(t *RequestTerminal) { t.Events = ReferenceText("invocation", "other") },
		"text":      func(t *RequestTerminal) { text := ReferenceText("invocation", "other text"); t.Text = &text },
	} {
		t.Run(name, func(t *testing.T) {
			mutated := requestTerminalFixture("invocation")
			mutate(&mutated)
			got, err := mutated.Reference()
			if err != nil || got == want {
				t.Fatalf("mutation did not change the reference: %v", err)
			}
		})
	}
}

func TestRequestTerminalRejectsInvalidRecords(t *testing.T) {
	for name, mutate := range map[string]func(*RequestTerminal){
		"unknown outcome":     func(t *RequestTerminal) { t.Outcome = "done" },
		"cross lineage":       func(t *RequestTerminal) { t.Messages = ReferenceText("other", "messages") },
		"invalid digest":      func(t *RequestTerminal) { t.Events.SHA256 = "invalid" },
		"repeated tool":       func(t *RequestTerminal) { t.Tools = append(t.Tools, t.Tools[0]) },
		"tool digest":         func(t *RequestTerminal) { t.Tools[0].Input["filePath"] = "short" },
		"blank tool identity": func(t *RequestTerminal) { t.Tools[0].MessageID = "" },
		"success unfinished":  func(t *RequestTerminal) { t.FinishReason = "tool-calls" },
		"success no text":     func(t *RequestTerminal) { t.Text = nil },
		"success no message":  func(t *RequestTerminal) { t.AssistantMessageID = "" },
	} {
		t.Run(name, func(t *testing.T) {
			terminal := requestTerminalFixture("invocation")
			mutate(&terminal)
			if _, err := terminal.Reference(); err == nil {
				t.Fatal("accepted an invalid terminal record")
			}
		})
	}
}

func TestRequestResponseRejectsFabricatedOrIncompleteFacts(t *testing.T) {
	for name, mutate := range map[string]func(*InvocationStreamObservation){
		"missing identity":  func(o *InvocationStreamObservation) { o.Request.Identity.RequestID = "" },
		"wrong session":     func(o *InvocationStreamObservation) { o.ProviderSessionID = "other" },
		"wrong outcome":     func(o *InvocationStreamObservation) { o.Request.Terminal.Outcome = RequestOutcomeInterrupted },
		"missing assistant": func(o *InvocationStreamObservation) { o.FinalTurnID = "" },
		"other assistant":   func(o *InvocationStreamObservation) { o.FinalTurnID = "other" },
		"other finish":      func(o *InvocationStreamObservation) { o.RawFinishReason = "other" },
		"other text":        func(o *InvocationStreamObservation) { o.CanonicalTerminalText = "Different response." },
		"missing text":      func(o *InvocationStreamObservation) { o.Request.Terminal.Text = nil },
		"other tools":       func(o *InvocationStreamObservation) { o.ToolCalls[0].Status = "error" },
		"dropped tools":     func(o *InvocationStreamObservation) { o.ToolCalls = nil },
		"invalid evidence":  func(o *InvocationStreamObservation) { o.Request.Terminal.Events.SHA256 = "invalid" },
		"pending tool":      func(o *InvocationStreamObservation) { o.PendingToolCount = 1 },
		"later event":       func(o *InvocationStreamObservation) { o.LaterEventAfterTerminal = true },
		"cancelled":         func(o *InvocationStreamObservation) { o.Cancelled = true },
		"timeout":           func(o *InvocationStreamObservation) { o.TimedOut = true },
		"empty response":    func(o *InvocationStreamObservation) { o.CanonicalTerminalText = " " },
	} {
		t.Run(name, func(t *testing.T) {
			observation := successfulRequestObservation()
			mutate(&observation)
			if err := ValidateServerResponseSuccess(observation); err == nil {
				t.Fatal("accepted conflicting request evidence")
			}
		})
	}
}
