package providerexecution

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
)

// Request execution outcomes as reported by the provider service.
const (
	RequestOutcomeSucceeded   = "succeeded"
	RequestOutcomeInterrupted = "interrupted"
	RequestOutcomeFailed      = "failed"
)

// RequestTool is a projected tool call reduced to the facts that must agree with
// Kingdom's durable admissions. InputSHA256 digests the canonical tool input.
type RequestTool struct {
	ID          string `json:"id"`
	MessageID   string `json:"message_id"`
	Name        string `json:"name"`
	Status      string `json:"status"`
	InputSHA256 string `json:"input_sha256"`
}

// RequestTerminal is the verified summary of one request's terminal evidence.
// Large provider content stays in immutable references; the canonical encoding
// of this record is the request's stored terminal reference.
type RequestTerminal struct {
	Outcome            string            `json:"outcome"`
	AssistantMessageID string            `json:"assistant_message_id,omitempty"`
	RawFinishReason    string            `json:"raw_finish_reason,omitempty"`
	FinishReason       string            `json:"finish_reason,omitempty"`
	Text               *ContentReference `json:"text,omitempty"`
	Tools              []RequestTool     `json:"tools,omitempty"`
	Events             ContentReference  `json:"events"`
	Messages           ContentReference  `json:"messages"`
}

func (t RequestTerminal) InvocationID() string { return t.Events.InvocationID }

func (t RequestTerminal) Validate() error {
	invocationID := t.InvocationID()
	switch t.Outcome {
	case RequestOutcomeSucceeded, RequestOutcomeInterrupted, RequestOutcomeFailed:
	default:
		return fmt.Errorf("provider request terminal outcome is invalid")
	}
	if err := t.Events.Validate(invocationID); err != nil {
		return err
	}
	if err := t.Messages.Validate(invocationID); err != nil {
		return err
	}
	if t.Text != nil {
		if err := t.Text.Validate(invocationID); err != nil {
			return err
		}
	}
	seen := make(map[string]bool, len(t.Tools))
	for _, tool := range t.Tools {
		for _, value := range []string{tool.ID, tool.MessageID, tool.Name, tool.Status} {
			if value == "" || value != strings.TrimSpace(value) {
				return fmt.Errorf("provider request terminal tool identity is incomplete")
			}
		}
		if raw, err := hex.DecodeString(tool.InputSHA256); err != nil || len(raw) != sha256.Size {
			return fmt.Errorf("provider request terminal tool input digest is invalid")
		}
		if seen[tool.ID] {
			return fmt.Errorf("provider request terminal repeats tool call %q", tool.ID)
		}
		seen[tool.ID] = true
	}
	if t.Outcome == RequestOutcomeSucceeded && (t.AssistantMessageID == "" || t.FinishReason != "stop" || t.Text == nil || t.Text.SizeBytes == 0) {
		return fmt.Errorf("successful provider request requires a completed assistant response")
	}
	return nil
}

// Reference is the immutable identity of this exact verified record.
func (t RequestTerminal) Reference() (ContentReference, error) {
	if err := t.Validate(); err != nil {
		return ContentReference{}, err
	}
	raw, err := json.Marshal(t)
	if err != nil {
		return ContentReference{}, err
	}
	sum := sha256.Sum256(raw)
	return ContentReference{InvocationID: t.InvocationID(), SHA256: hex.EncodeToString(sum[:]), SizeBytes: int64(len(raw))}, nil
}

func (t RequestTerminal) ToolCalls() []InvocationToolObservation {
	calls := make([]InvocationToolObservation, 0, len(t.Tools))
	for _, tool := range t.Tools {
		calls = append(calls, InvocationToolObservation{ID: tool.ID, Name: tool.Name, Status: tool.Status})
	}
	return calls
}

// RequireRequestTerminalMatch binds the observation's derived response facts to
// the verified terminal record, so the settlement cannot describe a different
// response than the evidence the request was closed with.
func RequireRequestTerminalMatch(o InvocationStreamObservation) error {
	if o.Request == nil {
		return fmt.Errorf("provider request observation is required")
	}
	terminal := o.Request.Terminal
	if err := terminal.Validate(); err != nil {
		return err
	}
	text := o.CanonicalTerminalTextRef
	if text == nil && o.CanonicalTerminalText != "" {
		inline := ReferenceText(terminal.InvocationID(), o.CanonicalTerminalText)
		text = &inline
	}
	if (text == nil) != (terminal.Text == nil) || (text != nil && *text != *terminal.Text) {
		return fmt.Errorf("provider request terminal text conflicts with its verified record")
	}
	if o.FinalTurnID != terminal.AssistantMessageID || o.RawFinishReason != terminal.RawFinishReason || o.NormalizedFinishReason != terminal.FinishReason {
		return fmt.Errorf("provider request response identity conflicts with its verified record")
	}
	if len(o.ToolCalls) != len(terminal.Tools) {
		return fmt.Errorf("provider request tool calls conflict with its verified record")
	}
	for i, call := range terminal.ToolCalls() {
		if o.ToolCalls[i] != call {
			return fmt.Errorf("provider request tool calls conflict with its verified record")
		}
	}
	if o.Cancelled && terminal.Outcome != RequestOutcomeInterrupted {
		return fmt.Errorf("provider request cancellation requires an interrupted terminal")
	}
	return nil
}
