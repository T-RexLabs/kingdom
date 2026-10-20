package protocol

import (
	"context"
	"encoding/json"
	"fmt"

	"kingdom/internal/core/providerexecution"
)

// message is the compact view of one projected session message. A single
// message is decoded at a time, so large user, tool and assistant content is
// never held beyond its own turn.
type message struct {
	ID        string `json:"id"`
	Type      string `json:"type"`
	Agent     string `json:"agent"`
	Finish    string `json:"finish"`
	RawFinish string `json:"rawFinish"`
	Outcome   string `json:"outcome"`
	Status    string `json:"status"`
	Reason    string `json:"reason"`
	Summary   string `json:"summary"`
	Recent    string `json:"recent"`
	Error     *struct {
		Type string `json:"type"`
	} `json:"error"`
	Time struct {
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

type previousMessage struct {
	kind   string
	id     string
	finish string
}

type messageScanner struct {
	identity providerexecution.RequestIdentity
	facts    eventFacts
	count    int
	previous previousMessage
	// resume is the assistant a synthetic message must be followed by.
	resume     string
	assistants int
	synthetics int
	compacted  bool
	idle       bool
	toolIDs    map[string]struct{}
	result     projection
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
	if s.resume != "" && (m.Type != "assistant" || m.ID != s.resume) {
		return fmt.Errorf("message %q does not resume with assistant %q", m.ID, s.resume)
	}
	s.resume = ""
	if s.count > 0 {
		var err error
		switch m.Type {
		case "assistant":
			err = s.assistant(m)
		case "synthetic":
			err = s.synthetic(m)
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
	s.previous = previousMessage{kind: m.Type, id: m.ID, finish: m.Finish}
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
	synthetics := s.facts.synthetics
	if s.synthetics >= len(synthetics) || synthetics[s.synthetics].messageID != m.ID {
		return fmt.Errorf("synthetic message %q matches no length continuation event", m.ID)
	}
	current := synthetics[s.synthetics]
	if s.previous.kind != "assistant" || s.previous.id != current.before || s.previous.finish != "length" {
		return fmt.Errorf("synthetic message %q does not follow the length-limited assistant", m.ID)
	}
	s.synthetics++
	s.resume = current.after
	return nil
}

func (s *messageScanner) compaction(m *message) error {
	c := s.facts.compaction
	if c == nil || s.compacted || s.assistants > 0 || m.ID != c.messageID || m.Reason != "auto" {
		return fmt.Errorf("compaction message %q matches no automatic compaction event", m.ID)
	}
	switch c.state {
	case compactionEnded:
		if m.Status != "completed" || m.Summary != c.summary || m.Recent != c.recent {
			return fmt.Errorf("compaction message %q conflicts with its checkpoint event", m.ID)
		}
	case compactionFailed:
		if m.Status != "failed" || m.Error == nil || m.Error.Type != compactionInterrupted {
			return fmt.Errorf("compaction message %q conflicts with its interruption event", m.ID)
		}
	}
	s.compacted = true
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
		return projection{}, fmt.Errorf("length continuation events have no synthetic projection")
	case s.facts.compaction != nil && !s.compacted:
		return projection{}, fmt.Errorf("compaction events have no compaction projection")
	}
	return s.result, nil
}
