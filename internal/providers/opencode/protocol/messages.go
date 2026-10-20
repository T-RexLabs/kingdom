package protocol

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	"kingdom/internal/core/providerexecution"
)

// message is the compact view of one projected session message. A single
// message is decoded at a time, so large user, tool and assistant content is
// never held beyond its own turn.
type message struct {
	ID              string          `json:"id"`
	Type            string          `json:"type"`
	Agent           string          `json:"agent"`
	Finish          string          `json:"finish"`
	RawFinish       string          `json:"rawFinish"`
	Outcome         string          `json:"outcome"`
	Status          string          `json:"status"`
	Reason          string          `json:"reason"`
	Summary         string          `json:"summary"`
	Recent          string          `json:"recent"`
	Text            *string         `json:"text"`
	Description     *string         `json:"description"`
	Metadata        json.RawMessage `json:"metadata"`
	ProviderContext json.RawMessage `json:"providerContext"`
	Error           *providerError  `json:"error"`
	Time            struct {
		Completed *int64 `json:"completed"`
	} `json:"time"`
	Tokens struct {
		Input     int64 `json:"input"`
		Output    int64 `json:"output"`
		Reasoning int64 `json:"reasoning"`
		Cache     struct {
			Read  int64 `json:"read"`
			Write int64 `json:"write"`
		} `json:"cache"`
	} `json:"tokens"`
	Content []struct {
		Type  string `json:"type"`
		ID    string `json:"id"`
		Name  string `json:"name"`
		Text  string `json:"text"`
		State struct {
			Status string          `json:"status"`
			Input  json.RawMessage `json:"input"`
		} `json:"state"`
	} `json:"content"`
}

// finalAssistant is the last assistant of the request interval.
type finalAssistant struct {
	id        string
	finish    string
	rawFinish string
	completed bool
	texts     []string
}

// projection is what the message classifier derives for the terminal record.
type projection struct {
	final  *finalAssistant
	tools  []providerexecution.RequestTool
	tokens providerexecution.TokenUsage
}

type messageScanner struct {
	identity    providerexecution.RequestIdentity
	facts       eventFacts
	count       int
	assistants  int
	synthetics  int
	systems     int
	compactions int
	projected   int
	idle        bool
	toolIDs     map[string]struct{}
	result      projection
}

func scanMessages(ctx context.Context, identity providerexecution.RequestIdentity, ev Evidence, facts eventFacts) (projection, error) {
	scanner := &messageScanner{identity: identity, facts: facts, toolIDs: map[string]struct{}{}}
	if err := readArray(ctx, "provider messages", ev.Messages, ev.InvocationID, scanner.observe); err != nil {
		return projection{}, err
	}
	return scanner.finish()
}

func (s *messageScanner) observe(m *message) error {
	if s.idle {
		return fmt.Errorf("message %q follows the idle boundary", m.ID)
	}
	if s.count == 0 {
		if m.ID != s.identity.RequestID || m.Type != "user" {
			return fmt.Errorf("messages must start with user request %q", s.identity.RequestID)
		}
	} else if m.ID == s.identity.RequestID {
		return fmt.Errorf("duplicate request projection %q", m.ID)
	}
	if s.count > 0 {
		if m.Type == "assistant" || m.Type == "synthetic" || m.Type == "compaction" || m.Type == "system" {
			if s.projected >= len(s.facts.order) || s.facts.order[s.projected] != (projectionIdentity{m.Type, m.ID}) {
				return fmt.Errorf("%s message %q matches no %s projection creation event in order", m.Type, m.ID, m.Type)
			}
			s.projected++
		}
		var err error
		switch m.Type {
		case "assistant":
			err = s.assistant(m)
		case "synthetic":
			err = s.synthetic(m)
		case "system":
			err = s.system(m)
		case "compaction":
			err = s.compaction(m)
		case "idle":
			err = s.idleBoundary(m)
		default:
			err = fmt.Errorf("unclassified %q message %q during request", m.Type, m.ID)
		}
		if err != nil {
			return err
		}
	}
	s.count++
	return nil
}

func (s *messageScanner) assistant(m *message) error {
	steps := s.facts.steps
	if s.assistants >= len(steps) || steps[s.assistants].id != m.ID || m.Agent != s.identity.ProfileID {
		return fmt.Errorf("assistant %q matches no step of profile %q", m.ID, s.identity.ProfileID)
	}
	current := steps[s.assistants]
	finish, rawFinish := current.finish, current.rawFinish
	if current.state == stepFailed && finish == "" {
		finish = "error"
	}
	if m.Finish != finish || m.RawFinish != rawFinish {
		return fmt.Errorf("assistant %q finish conflicts with its step events", m.ID)
	}
	s.assistants++

	final := &finalAssistant{id: m.ID, finish: m.Finish, rawFinish: m.RawFinish, completed: m.Time.Completed != nil}
	for _, part := range m.Content {
		switch part.Type {
		case "text":
			final.texts = append(final.texts, part.Text)
		case "tool":
			if err := s.tool(m.ID, part.ID, part.Name, part.State.Status, part.State.Input); err != nil {
				return err
			}
		}
	}
	s.result.final = final
	s.result.tokens.InputTokens += m.Tokens.Input
	s.result.tokens.OutputTokens += m.Tokens.Output
	s.result.tokens.ReasoningTokens += m.Tokens.Reasoning
	s.result.tokens.CacheReadTokens += m.Tokens.Cache.Read
	s.result.tokens.CacheWriteTokens += m.Tokens.Cache.Write
	s.result.tokens.TotalTokens += m.Tokens.Input + m.Tokens.Output + m.Tokens.Reasoning + m.Tokens.Cache.Read + m.Tokens.Cache.Write
	return nil
}

func (s *messageScanner) tool(messageID, id, name, status string, input json.RawMessage) error {
	if _, duplicate := s.toolIDs[id]; duplicate || id == "" || name == "" || status == "" {
		return fmt.Errorf("tool call %q in assistant %q is incomplete or repeated", id, messageID)
	}
	s.toolIDs[id] = struct{}{}
	arguments, err := providerexecution.ToolInputDigests(input)
	if err != nil {
		return fmt.Errorf("tool call %q: %w", id, err)
	}
	s.result.tools = append(s.result.tools, providerexecution.RequestTool{
		ID:        id,
		MessageID: messageID,
		Name:      name,
		Status:    status,
		Input:     arguments,
	})
	return nil
}

func (s *messageScanner) synthetic(m *message) error {
	if s.synthetics >= len(s.facts.synthetics) {
		return fmt.Errorf("synthetic message %q matches no synthetic event", m.ID)
	}
	c := s.facts.synthetics[s.synthetics]
	if m.ID != c.messageID || !reflect.DeepEqual(m.Text, c.text) || !reflect.DeepEqual(m.Description, c.description) || !sameJSON(m.Metadata, c.metadata) {
		return fmt.Errorf("synthetic message %q conflicts with its context event", m.ID)
	}
	s.synthetics++
	return nil
}

// System display labels and metadata are derived by OpenCode, not carried in
// the instruction event. Only its identity and authored context can be matched.
func (s *messageScanner) system(m *message) error {
	if s.systems >= len(s.facts.systems) {
		return fmt.Errorf("system message %q matches no instruction update", m.ID)
	}
	c := s.facts.systems[s.systems]
	if m.ID != c.messageID || !reflect.DeepEqual(m.Text, c.text) {
		return fmt.Errorf("system message %q conflicts with its instruction event", m.ID)
	}
	s.systems++
	return nil
}

// sameJSON compares immutable JSON values without depending on object key order
// or rounding large integer metadata through float64.
func sameJSON(a, b json.RawMessage) bool {
	decode := func(raw json.RawMessage) (any, error) {
		if len(raw) == 0 {
			return nil, nil
		}
		var value any
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.UseNumber()
		err := decoder.Decode(&value)
		return value, err
	}
	av, ae := decode(a)
	bv, be := decode(b)
	return ae == nil && be == nil && reflect.DeepEqual(av, bv)
}

func (s *messageScanner) compaction(m *message) error {
	if s.compactions >= len(s.facts.compactions) {
		return fmt.Errorf("compaction message %q matches no automatic compaction event", m.ID)
	}
	c := s.facts.compactions[s.compactions]
	if m.ID != c.messageID || m.Reason != "auto" {
		return fmt.Errorf("compaction message %q matches no automatic compaction event", m.ID)
	}
	switch c.state {
	case compactionEnded:
		if m.Status != "completed" || m.Summary != c.summary || m.Recent != c.recent || !sameJSON(m.ProviderContext, c.providerContext) {
			return fmt.Errorf("compaction message %q conflicts with its checkpoint event", m.ID)
		}
	case compactionFailed:
		if m.Status != "failed" || !reflect.DeepEqual(m.Error, c.failure) {
			return fmt.Errorf("compaction message %q conflicts with its failure event", m.ID)
		}
	}
	s.compactions++
	return nil
}

func (s *messageScanner) idleBoundary(m *message) error {
	if m.Outcome != s.facts.outcome {
		return fmt.Errorf("idle outcome %q conflicts with execution outcome %q", m.Outcome, s.facts.outcome)
	}
	if s.facts.outcome == providerexecution.RequestOutcomeFailed {
		var want, got string
		if s.facts.executionError != nil {
			want = s.facts.executionError.Type
		}
		if m.Error != nil {
			got = m.Error.Type
		}
		if got != want {
			return fmt.Errorf("idle error %q conflicts with failed execution error %q", got, want)
		}
	}
	s.idle = true
	return nil
}

func (s *messageScanner) finish() (projection, error) {
	switch {
	case !s.idle:
		return projection{}, fmt.Errorf("messages must end with the idle boundary")
	case s.assistants != len(s.facts.steps):
		return projection{}, fmt.Errorf("step events have no assistant projection")
	case s.synthetics != len(s.facts.synthetics):
		return projection{}, fmt.Errorf("synthetic events have no synthetic projection")
	case s.systems != len(s.facts.systems):
		return projection{}, fmt.Errorf("instruction events have no system projection")
	case s.compactions != len(s.facts.compactions):
		return projection{}, fmt.Errorf("compaction events have no compaction projection")
	}
	return s.result, nil
}
