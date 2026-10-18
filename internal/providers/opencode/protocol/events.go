package protocol

import (
	"context"
	"fmt"
	"strings"

	"kingdom/internal/core/providerexecution"
)

const (
	stepOpen = iota
	stepEnded
	stepFailed
)

const (
	compactionStarted = iota
	compactionEnded
	compactionFailed
)

const compactionInterrupted = "compaction.interrupted"

type providerError struct {
	Type    string `json:"type"`
	Message string `json:"message"`
	Status  int    `json:"status"`
}

// event is the compact view of one public session event. Payloads that are not
// part of a classifier are never retained.
type event struct {
	ID       string `json:"id"`
	Type     string `json:"type"`
	Location *struct {
		Directory string `json:"directory"`
	} `json:"location"`
	Data struct {
		SessionID          string         `json:"sessionID"`
		InboxID            string         `json:"inboxID"`
		AssistantMessageID string         `json:"assistantMessageID"`
		Agent              string         `json:"agent"`
		Finish             string         `json:"finish"`
		RawFinish          string         `json:"rawFinish"`
		Reason             string         `json:"reason"`
		InputID            string         `json:"inputID"`
		Text               string         `json:"text"`
		Recent             string         `json:"recent"`
		Error              *providerError `json:"error"`
	} `json:"data"`
	Durable *struct {
		AggregateID string `json:"aggregateID"`
		Seq         int64  `json:"seq"`
	} `json:"durable"`
}

type step struct {
	id        string
	state     int
	finish    string
	rawFinish string
}

// synthetic is a bounded length continuation: a synthetic input between a
// step that ended with finish length and the step that resumes the work.
type synthetic struct {
	messageID string
	before    string
	after     string
}

type compaction struct {
	messageID string
	state     int
	summary   string
	recent    string
}

// eventFacts is everything the message classifier needs from the events.
type eventFacts struct {
	outcome        string
	steps          []step
	synthetics     []synthetic
	compaction     *compaction
	executionError *providerError
	// stepError is the structured error of the last failed step.
	stepError *providerError
}

type eventScanner struct {
	identity providerexecution.RequestIdentity
	facts    eventFacts
	count    int
	lastSeq  int64
	ids      map[string]struct{}
	stepByID map[string]int
	// justEnded and pending carry the strict adjacency of a synthetic input
	// across consecutive events.
	justEnded *step
	pending   *synthetic
	done      bool
}

func scanEvents(ctx context.Context, identity providerexecution.RequestIdentity, ev Evidence) (eventFacts, error) {
	scanner := &eventScanner{identity: identity, lastSeq: -1, ids: map[string]struct{}{}, stepByID: map[string]int{}}
	if err := readArray(ctx, "provider events", ev.Events, ev.InvocationID, scanner.observe); err != nil {
		return eventFacts{}, err
	}
	return scanner.finish(ev.Watermark)
}

func (s *eventScanner) observe(e *event) error {
	if err := s.checkEnvelope(e); err != nil {
		return err
	}
	if s.done {
		return fmt.Errorf("event %q follows the terminal execution event", e.ID)
	}
	justEnded, pending := s.justEnded, s.pending
	s.justEnded, s.pending = nil, nil
	if pending != nil && e.Type != "session.step.started" {
		return fmt.Errorf("synthetic input %q is not followed by a step", pending.messageID)
	}
	first := s.count == 0
	s.count++
	if first {
		return s.checkDelivery(e)
	}
	if strings.HasPrefix(e.Type, "session.inbox.") {
		return fmt.Errorf("additional inbox input %q during request", e.Type)
	}
	switch e.Type {
	case "session.agent.selected", "session.model.selected", "session.location.switched", "session.moved":
		return fmt.Errorf("session identity changed during request: %s", e.Type)
	case "session.step.started":
		return s.stepStarted(e, pending)
	case "session.step.ended":
		return s.stepEnded(e)
	case "session.step.failed":
		return s.stepFailed(e)
	case "session.synthetic":
		if justEnded == nil || justEnded.finish != "length" {
			return fmt.Errorf("synthetic input %q does not follow a length-limited step", e.ID)
		}
		s.pending = &synthetic{messageID: derivedMessageID(e.ID), before: justEnded.id}
	case "session.compaction.started":
		return s.compactionStarted(e)
	case "session.compaction.ended", "session.compaction.failed":
		return s.compactionFinished(e)
	case "session.execution.succeeded", "session.execution.interrupted", "session.execution.failed":
		s.done = true
		s.facts.outcome = strings.TrimPrefix(e.Type, "session.execution.")
		if e.Type == "session.execution.failed" && e.Data.Error != nil {
			s.facts.executionError = e.Data.Error
		}
	default:
		if strings.HasPrefix(e.Type, "session.execution.") {
			return fmt.Errorf("unclassified execution event %q during request", e.Type)
		}
	}
	return nil
}

func (s *eventScanner) checkEnvelope(e *event) error {
	if e.ID == "" {
		return fmt.Errorf("event identity is required")
	}
	if _, duplicate := s.ids[e.ID]; duplicate {
		return fmt.Errorf("duplicate event %q", e.ID)
	}
	s.ids[e.ID] = struct{}{}
	if e.Durable == nil || e.Durable.Seq <= s.lastSeq {
		return fmt.Errorf("event %q lacks a strictly increasing public sequence", e.ID)
	}
	s.lastSeq = e.Durable.Seq
	sessionID, aggregateID := e.Data.SessionID, e.Durable.AggregateID
	if (sessionID == "" && aggregateID == "") || (sessionID != "" && sessionID != s.identity.SessionID) || (aggregateID != "" && aggregateID != s.identity.SessionID) {
		return fmt.Errorf("event %q belongs to another session", e.ID)
	}
	if e.Location != nil && e.Location.Directory != s.identity.Location {
		return fmt.Errorf("event %q belongs to another location", e.ID)
	}
	return nil
}

// checkDelivery requires the evidence to open with the delivery of this exact
// request; every later inbox event is rejected as additional input.
func (s *eventScanner) checkDelivery(e *event) error {
	if e.Type != "session.inbox.delivered" || e.Data.InboxID != s.identity.RequestID || e.Location == nil {
		return fmt.Errorf("events must start with the located delivery of request %q", s.identity.RequestID)
	}
	return nil
}

func (s *eventScanner) stepStarted(e *event, pending *synthetic) error {
	id := e.Data.AssistantMessageID
	if e.Data.Agent != s.identity.ProfileID || id == "" {
		return fmt.Errorf("step %q does not belong to profile %q", id, s.identity.ProfileID)
	}
	if _, duplicate := s.stepByID[id]; duplicate {
		return fmt.Errorf("duplicate step %q", id)
	}
	if n := len(s.facts.steps); n > 0 && s.facts.steps[n-1].state != stepEnded {
		return fmt.Errorf("step %q starts before step %q ended", id, s.facts.steps[n-1].id)
	}
	if c := s.facts.compaction; c != nil && c.state != compactionEnded {
		return fmt.Errorf("step %q starts before compaction completed", id)
	}
	if pending != nil {
		if id == pending.before {
			return fmt.Errorf("synthetic input %q resumes with the step it follows", pending.messageID)
		}
		pending.after = id
		s.facts.synthetics = append(s.facts.synthetics, *pending)
	}
	s.stepByID[id] = len(s.facts.steps)
	s.facts.steps = append(s.facts.steps, step{id: id})
	return nil
}

func (s *eventScanner) openStep(e *event) (*step, error) {
	index, ok := s.stepByID[e.Data.AssistantMessageID]
	if !ok || s.facts.steps[index].state != stepOpen {
		return nil, fmt.Errorf("step completion %q matches no open step", e.Data.AssistantMessageID)
	}
	return &s.facts.steps[index], nil
}

func (s *eventScanner) stepEnded(e *event) error {
	current, err := s.openStep(e)
	if err != nil {
		return err
	}
	if e.Data.Finish == "" {
		return fmt.Errorf("step %q ended without a finish reason", current.id)
	}
	current.state, current.finish, current.rawFinish = stepEnded, e.Data.Finish, e.Data.RawFinish
	s.justEnded = current
	return nil
}

func (s *eventScanner) stepFailed(e *event) error {
	current, err := s.openStep(e)
	if err != nil {
		return err
	}
	current.state, current.finish, current.rawFinish = stepFailed, e.Data.Finish, e.Data.RawFinish
	s.facts.stepError = e.Data.Error
	return nil
}

func (s *eventScanner) compactionStarted(e *event) error {
	if s.facts.compaction != nil || len(s.facts.steps) > 0 || e.Data.Reason != "auto" || e.Data.InputID != "" {
		return fmt.Errorf("compaction %q is not the single automatic compaction before the first step", e.ID)
	}
	s.facts.compaction = &compaction{messageID: derivedMessageID(e.ID), state: compactionStarted}
	return nil
}

func (s *eventScanner) compactionFinished(e *event) error {
	c := s.facts.compaction
	if c == nil || c.state != compactionStarted || e.Data.Reason != "auto" || e.Data.InputID != "" {
		return fmt.Errorf("compaction completion %q has no matching automatic start", e.ID)
	}
	if e.Type == "session.compaction.ended" {
		if strings.TrimSpace(e.Data.Text) == "" {
			return fmt.Errorf("compaction %q has no summary", e.ID)
		}
		c.state, c.summary, c.recent = compactionEnded, e.Data.Text, e.Data.Recent
		return nil
	}
	if e.Data.Error == nil || e.Data.Error.Type != compactionInterrupted {
		return fmt.Errorf("compaction %q failed without interruption evidence", e.ID)
	}
	c.state = compactionFailed
	return nil
}

func (s *eventScanner) finish(watermark int64) (eventFacts, error) {
	facts := s.facts
	if !s.done || s.lastSeq != watermark {
		return eventFacts{}, fmt.Errorf("events must end with the terminal execution event at watermark %d", watermark)
	}
	if facts.compaction != nil && len(facts.synthetics) > 0 {
		return eventFacts{}, fmt.Errorf("length continuation combined with compaction is unsupported")
	}
	if c := facts.compaction; c != nil && (c.state == compactionStarted || (c.state == compactionFailed && facts.outcome != providerexecution.RequestOutcomeInterrupted)) {
		return eventFacts{}, fmt.Errorf("compaction does not match the %s outcome", facts.outcome)
	}
	if facts.outcome == providerexecution.RequestOutcomeSucceeded {
		if n := len(facts.steps); n == 0 || facts.steps[n-1].state != stepEnded {
			return eventFacts{}, fmt.Errorf("successful request lacks a completed final step")
		}
	}
	return facts, nil
}

// derivedMessageID is the projection identity OpenCode derives from the event
// that created a synthetic or compaction message.
func derivedMessageID(eventID string) string {
	return "msg_" + strings.TrimPrefix(eventID, "evt_")
}

// failure is the structured error of a failed request, preferring the
// execution's own error over the error of the step that failed.
func (f eventFacts) failure() *providerError {
	if f.outcome != providerexecution.RequestOutcomeFailed {
		return nil
	}
	for _, candidate := range []*providerError{f.executionError, f.stepError} {
		if candidate != nil && candidate.Type != "" {
			return candidate
		}
	}
	return nil
}
