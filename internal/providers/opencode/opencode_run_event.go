package opencode

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

const maxOpencodeRunEventLineBytes = 8 * 1024 * 1024

// opencodeRunEventWire mirrors one line of `opencode run --format json`. The
// runner formatter nests the whole payload under `part`: the part's kind,
// id, tool name, execution state and reasoning metadata all live inside
// `part`, never at the top level. Only `type`, `sessionID`, `error` and
// `message` are top-level envelope fields.
type opencodeRunEventWire struct {
	Type      string `json:"type"`
	SessionID string `json:"sessionID"`
	Part      struct {
		Type      string  `json:"type"`
		ID        string  `json:"id"`
		CallID    string  `json:"callID"`
		MessageID string  `json:"messageID"`
		Text      string  `json:"text"`
		Tool      string  `json:"tool"`
		Reason    *string `json:"reason"`
		State     struct {
			Status string          `json:"status"`
			Title  string          `json:"title"`
			Input  json.RawMessage `json:"input"`
		} `json:"state"`
		Metadata struct {
			OpenAI struct {
				ReasoningEncryptedContent string `json:"reasoningEncryptedContent"`
			} `json:"openai"`
			Redacted string `json:"redacted"`
		} `json:"metadata"`
		Time struct {
			Start *int64 `json:"start"`
			End   *int64 `json:"end"`
		} `json:"time"`
		Tokens *struct {
			Total     int64 `json:"total"`
			Input     int64 `json:"input"`
			Output    int64 `json:"output"`
			Reasoning int64 `json:"reasoning"`
			Cache     struct {
				Read  int64 `json:"read"`
				Write int64 `json:"write"`
			} `json:"cache"`
		} `json:"tokens,omitempty"`
	} `json:"part"`
	Error   json.RawMessage `json:"error"`
	Message string          `json:"message"`
}

func parseOpencodeRunEvent(line string) (opencodeRunEvent, error) {
	decoder := json.NewDecoder(strings.NewReader(line))
	event, err := decodeNextOpencodeRunEvent(decoder)
	if err != nil {
		return opencodeRunEvent{}, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return opencodeRunEvent{}, fmt.Errorf("multiple JSON values in provider event line")
		}
		return opencodeRunEvent{}, fmt.Errorf("trailing provider event data: %w", err)
	}
	return event, nil
}

func decodeOpencodeRunEventStream(stream io.Reader, onEvent func(opencodeRunEvent) error, onMalformed func(string, error)) error {
	scanner := bufio.NewScanner(stream)
	scanner.Buffer(make([]byte, 0, 64*1024), maxOpencodeRunEventLineBytes)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		event, err := parseOpencodeRunEvent(line)
		if err != nil {
			if onMalformed != nil {
				onMalformed(line, err)
			}
			continue
		}
		if err := onEvent(event); err != nil {
			return err
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return nil
}

func decodeNextOpencodeRunEvent(decoder *json.Decoder) (opencodeRunEvent, error) {
	var envelope opencodeRunEventWire
	if err := decoder.Decode(&envelope); err != nil {
		return opencodeRunEvent{}, err
	}
	event := opencodeRunEvent{
		Type:      strings.TrimSpace(envelope.Type),
		SessionID: strings.TrimSpace(envelope.SessionID),
	}
	event.Part.Type = strings.TrimSpace(envelope.Part.Type)
	event.Part.ID = strings.TrimSpace(envelope.Part.ID)
	event.Part.CallID = strings.TrimSpace(envelope.Part.CallID)
	event.Part.MessageID = strings.TrimSpace(envelope.Part.MessageID)
	event.Part.Text = envelope.Part.Text
	event.Part.Tool = strings.TrimSpace(envelope.Part.Tool)
	if envelope.Part.Reason != nil {
		event.Part.ReasonPresent = true
		event.Part.Reason = *envelope.Part.Reason
	}
	event.Part.State.Status = strings.TrimSpace(envelope.Part.State.Status)
	event.Part.State.Title = strings.TrimSpace(envelope.Part.State.Title)
	event.Part.State.Input = append(json.RawMessage(nil), envelope.Part.State.Input...)
	event.Part.Metadata.OpenAI.ReasoningEncryptedContent = strings.TrimSpace(envelope.Part.Metadata.OpenAI.ReasoningEncryptedContent)
	event.Part.Metadata.Redacted = strings.TrimSpace(envelope.Part.Metadata.Redacted)
	event.Part.Time.Start = envelope.Part.Time.Start
	event.Part.Time.End = envelope.Part.Time.End
	event.Part.Tokens = envelope.Part.Tokens
	event.ErrorText = extractRunEventErrorText(envelope.Error, envelope.Message)
	return event, nil
}

func extractRunEventErrorText(raw json.RawMessage, fallback string) string {
	if strings.TrimSpace(fallback) != "" {
		return strings.TrimSpace(fallback)
	}
	if len(raw) == 0 || string(raw) == "null" {
		return ""
	}
	var text string
	if err := json.Unmarshal(raw, &text); err == nil {
		return strings.TrimSpace(text)
	}
	var object map[string]any
	if err := json.Unmarshal(raw, &object); err == nil {
		data, _ := object["data"].(map[string]any)
		return firstNonEmpty(stringValue(object["message"]), stringValue(object["text"]), stringValue(data["message"]), "provider error event")
	}
	return "provider error event"
}
