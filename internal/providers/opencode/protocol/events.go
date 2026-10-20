package protocol

import (
	"context"
	"encoding/json"
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
		SessionID          string          `json:"sessionID"`
		InboxID            string          `json:"inboxID"`
		AssistantMessageID string          `json:"assistantMessageID"`
		Agent              string          `json:"agent"`
		Finish             string          `json:"finish"`
		RawFinish          string          `json:"rawFinish"`
		Reason             string          `json:"reason"`
		InputID            string          `json:"inputID"`
		Text               *string         `json:"text"`
		Description        *string         `json:"description"`
		Metadata           json.RawMessage `json:"metadata"`
		ProviderContext    json.RawMessage `json:"providerContext"`
		Recent             string          `json:"recent"`
		Error              *providerError  `json:"error"`
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
	retry     bool
	output    bool
}

// Context and checkpoints are correlated to projections by their creation
// identities, not by guesses about the provider's scheduling decisions.
type contextMessage struct {
	messageID   string
	text        *string
	description *string
	metadata    json.RawMessage
}
type compaction struct {
	messageID       string
	state           int
	summary         string
	recent          string
	providerContext json.RawMessage
	failure         *providerError
}
type projectionIdentity struct{ kind, id string }

// eventFacts is everything the message classifier needs from the events.
type eventFacts struct {
	outcome        string
	steps          []step
	synthetics     []contextMessage
	systems        []contextMessage
	compactions    []*compaction
	order          []projectionIdentity
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

	done bool
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
		return s.stepStarted(e)
	case "session.step.ended":
		return s.stepEnded(e)
	case "session.step.failed":
		return s.stepFailed(e)
	case "session.synthetic":
		if e.Data.Text == nil {
			return fmt.Errorf("synthetic event %q has no context text", e.ID)
		}
		id := derivedMessageID(e.ID)
		s.facts.synthetics = append(s.facts.synthetics, contextMessage{messageID: id, text: e.Data.Text, description: e.Data.Description, metadata: e.Data.Metadata})
		s.facts.order = append(s.facts.order, projectionIdentity{"synthetic", id})
	case "session.instructions.updated":
		if e.Data.Text != nil {
			context := contextMessage{messageID: derivedMessageID(e.ID), text: e.Data.Text}
			s.facts.systems = append(s.facts.systems, context)
			s.facts.order = append(s.facts.order, projectionIdentity{"system", context.messageID})
		}
	case "session.retry.scheduled":
		index, ok := s.stepByID[e.Data.AssistantMessageID]
		if !ok {
			return fmt.Errorf("retry matches no assistant step %q", e.Data.AssistantMessageID)
		}
		s.facts.steps[index].retry = true
	case "session.text.started", "session.reasoning.started", "session.tool.input.started", "session.tool.called":
		if index, ok := s.stepByID[e.Data.AssistantMessageID]; ok {
			s.facts.steps[index].output = true
		}
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

func (s *eventScanner) stepStarted(e *event) error {
	id := e.Data.AssistantMessageID
	if e.Data.Agent != s.identity.ProfileID || id == "" {
		return fmt.Errorf("step %q does not belong to profile %q", id, s.identity.ProfileID)
	}
	if index, exists := s.stepByID[id]; exists {
		current := &s.facts.steps[index]
		// OpenCode retries before output under the same assistant identity. The
		// durable retry event must prove that restart; an arbitrary duplicate does not.
		if index != len(s.facts.steps)-1 || !current.retry || current.output || current.state != stepOpen {
			return fmt.Errorf("duplicate step %q without a pre-output retry", id)
		}
		current.retry = false
		return nil
	}
	if n := len(s.facts.steps); n > 0 && s.facts.steps[n-1].state == stepOpen {
		return fmt.Errorf("step %q starts before step %q ended", id, s.facts.steps[n-1].id)
	}
	if c := s.activeCompaction(); c != nil {
		return fmt.Errorf("step %q starts before compaction completed", id)
	}
	s.stepByID[id] = len(s.facts.steps)
	s.facts.steps = append(s.facts.steps, step{id: id})
	s.facts.order = append(s.facts.order, projectionIdentity{"assistant", id})
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

func (s *eventScanner) activeCompaction() *compaction {
	if n := len(s.facts.compactions); n > 0 && s.facts.compactions[n-1].state == compactionStarted {
		return s.facts.compactions[n-1]
	}
	return nil
}

func (s *eventScanner) compactionStarted(e *event) error {
	if s.activeCompaction() != nil || e.Data.Reason != "auto" || e.Data.InputID != "" {
		return fmt.Errorf("compaction %q is not an owned automatic compaction", e.ID)
	}
	c := &compaction{messageID: derivedMessageID(e.ID), state: compactionStarted}
	s.facts.compactions = append(s.facts.compactions, c)
	s.facts.order = append(s.facts.order, projectionIdentity{"compaction", c.messageID})
	return nil
}

func (s *eventScanner) compactionFinished(e *event) error {
	c := s.activeCompaction()
	if c == nil || e.Data.Reason != "auto" || e.Data.InputID != "" {
		return fmt.Errorf("compaction completion %q has no matching automatic start", e.ID)
	}
	if e.Type == "session.compaction.ended" {
		if (e.Data.Text == nil || strings.TrimSpace(*e.Data.Text) == "") && !hasStructuredContext(e.Data.ProviderContext) {
			return fmt.Errorf("compaction %q has no summary or structured checkpoint", e.ID)
		}
		c.state, c.recent = compactionEnded, e.Data.Recent
		if e.Data.Text != nil {
			c.summary = *e.Data.Text
		}
		c.providerContext = e.Data.ProviderContext
		return nil
	}
	if e.Data.Error == nil || e.Data.Error.Type == "" {
		return fmt.Errorf("compaction %q failed without error evidence", e.ID)
	}
	c.state, c.failure = compactionFailed, e.Data.Error
	return nil
}

func hasStructuredContext(raw json.RawMessage) bool {
	var v any
	if json.Unmarshal(raw, &v) != nil {
		return false
	}
	switch v := v.(type) {
	case []any:
		return len(v) > 0
	case map[string]any:
		return len(v) > 0
	}
	return false
}

func (s *eventScanner) finish(watermark int64) (eventFacts, error) {
	facts := s.facts
	if !s.done || s.lastSeq != watermark {
		return eventFacts{}, fmt.Errorf("events must end with the terminal execution event at watermark %d", watermark)
	}
	if s.activeCompaction() != nil {
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
