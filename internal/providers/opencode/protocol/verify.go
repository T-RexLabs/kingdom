// Package protocol is the single verifier of OpenCode V2 request terminal
// evidence. It is pure: it reads immutable captured events and projected
// messages and decides whether they prove one request's terminal outcome.
// Ownership, leases, admissions and settlement are decided elsewhere.
package protocol

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"strings"
	"unicode"

	"kingdom/internal/core/providerexecution"
)

// Source is complete, immutable, invocation-scoped JSON evidence.
type Source struct {
	Ref  providerexecution.ContentReference
	Open providerexecution.ContentOpener
}

// Evidence is everything Verify may rely on.
//
// Events are the provider session's public events for one request, in capture
// order, from that request's session.inbox.delivered through the final
// captured event at Watermark, as a JSON array. Messages are the session's
// projected messages from the request's own user message through the final
// idle, as a JSON array.
type Evidence struct {
	InvocationID string
	Events       Source
	Messages     Source
	// Watermark is the aggregate sequence of the final captured public event,
	// obtained independently of the events from the finite log sync marker.
	Watermark int64
	// CaptureHealthy reports a single uninterrupted subscription opened before
	// session creation: no EOF, error, reconnect or overflow, with the live
	// capture loop still running at snapshot.
	CaptureHealthy bool
	// WaitCompleted reports that the provider wait for idle returned.
	WaitCompleted bool
}

// ProviderFailure holds the structured error fields of a failed execution. It
// is never derived from prose.
type ProviderFailure struct {
	Code    string
	Message string
	Status  int
}

type Verified struct {
	// Terminal references the evidence it was verified from. Its text
	// reference is computed from the bytes written to the text writer.
	Terminal providerexecution.RequestTerminal
	// Tokens are summed over the request's assistant messages.
	Tokens providerexecution.TokenUsage
	// Provider is set only for a failed execution with structured error fields.
	Provider *ProviderFailure
}

// Verify validates the evidence for the expected identity and streams the final
// assistant text, when the request produced any, to text. The text writer
// receives bytes only after every rule holds. A nil error is the only proof of
// the returned terminal.
func Verify(ctx context.Context, identity providerexecution.RequestIdentity, ev Evidence, text io.Writer) (Verified, error) {
	if err := identity.Validate(); err != nil {
		return Verified{}, err
	}
	if ev.InvocationID == "" || strings.TrimSpace(ev.InvocationID) != ev.InvocationID || text == nil {
		return Verified{}, fmt.Errorf("provider invocation and text destination are required")
	}
	if !ev.CaptureHealthy {
		return Verified{}, fmt.Errorf("provider event capture was not continuous")
	}
	if !ev.WaitCompleted {
		return Verified{}, fmt.Errorf("provider wait for idle did not complete")
	}
	facts, err := scanEvents(ctx, identity, ev)
	if err != nil {
		return Verified{}, err
	}
	result, err := scanMessages(ctx, identity, ev, facts)
	if err != nil {
		return Verified{}, err
	}
	if facts.outcome == providerexecution.RequestOutcomeSucceeded {
		if err := requireCompletedResponse(result); err != nil {
			return Verified{}, err
		}
	}

	terminal := providerexecution.RequestTerminal{
		Outcome:  facts.outcome,
		Tools:    result.tools,
		Events:   ev.Events.Ref,
		Messages: ev.Messages.Ref,
	}
	var texts []string
	if final := result.final; final != nil {
		terminal.AssistantMessageID, terminal.FinishReason, terminal.RawFinishReason = final.id, final.finish, final.rawFinish
		texts = final.texts
	}
	reference, hasText, err := referenceText(ev.InvocationID, texts)
	if err != nil {
		return Verified{}, err
	}
	if hasText {
		terminal.Text = &reference
	}
	if err := terminal.Validate(); err != nil {
		return Verified{}, err
	}
	if _, err := canonicalText(text, texts); err != nil {
		return Verified{}, err
	}

	verified := Verified{Terminal: terminal, Tokens: result.tokens}
	if failure := facts.failure(); failure != nil {
		verified.Provider = &ProviderFailure{Code: failure.Type, Message: failure.Message, Status: failure.Status}
	}
	return verified, nil
}

// requireCompletedResponse is the success rule: the final assistant completed,
// stopped normally and produced text, and no tool call is left unfinished.
func requireCompletedResponse(result projection) error {
	final := result.final
	if final == nil || !final.completed || final.finish != "stop" {
		return fmt.Errorf("successful request lacks a completed assistant that stopped normally")
	}
	blank := true
	for _, text := range final.texts {
		blank = blank && strings.TrimSpace(text) == ""
	}
	if blank {
		return fmt.Errorf("successful request has no final assistant text")
	}
	for _, tool := range result.tools {
		if tool.Status != "completed" && tool.Status != "error" {
			return fmt.Errorf("tool call %q is %q at success", tool.ID, tool.Status)
		}
	}
	return nil
}

// trimmedWriter forwards text without its leading and trailing Unicode white
// space, which is the canonical form that settlement requires of terminal
// text. Trailing white space is held back until later text proves it interior.
type trimmedWriter struct {
	out     io.Writer
	started bool
	pending []byte
	written int64
}

func (w *trimmedWriter) Write(raw []byte) (int, error) {
	for rest := raw; len(rest) > 0; {
		if !w.started {
			trimmed := strings.TrimLeftFunc(string(rest), unicode.IsSpace)
			rest = rest[len(rest)-len(trimmed):]
			if len(rest) == 0 {
				break
			}
			w.started = true
		}
		body := strings.TrimRightFunc(string(rest), unicode.IsSpace)
		if body != "" {
			if err := w.emit(append(w.pending, body...)); err != nil {
				return 0, err
			}
			w.pending = w.pending[:0]
		}
		w.pending = append(w.pending, rest[len(body):]...)
		break
	}
	return len(raw), nil
}

func (w *trimmedWriter) emit(raw []byte) error {
	n, err := w.out.Write(raw)
	w.written += int64(n)
	return err
}

// canonicalText streams the parts' trimmed concatenation to out.
func canonicalText(out io.Writer, parts []string) (int64, error) {
	writer := &trimmedWriter{out: out}
	for _, part := range parts {
		if _, err := io.WriteString(writer, part); err != nil {
			return 0, err
		}
	}
	return writer.written, nil
}

// referenceText references the canonical text without materializing it. ok is
// false when the request produced no text.
func referenceText(invocationID string, parts []string) (providerexecution.ContentReference, bool, error) {
	digest := sha256.New()
	size, err := canonicalText(digest, parts)
	if err != nil || size == 0 {
		return providerexecution.ContentReference{}, false, err
	}
	return providerexecution.ContentReference{InvocationID: invocationID, SHA256: hex.EncodeToString(digest.Sum(nil)), SizeBytes: size}, true, nil
}
