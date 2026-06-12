package opencode

import (
	"bufio"
	"encoding/json"
	"io"
	"strings"
)

const maxOpencodeRunEventLineBytes = 8 * 1024 * 1024

type opencodeRunEventWire struct {
	Type      string `json:"type"`
	SessionID string `json:"sessionID"`
	Text      string `json:"text"`
	Tool      string `json:"tool"`
	State     struct {
		Status string          `json:"status"`
		Title  string          `json:"title"`
		Input  json.RawMessage `json:"input"`
	} `json:"state"`
	Part      struct {
		Text   string  `json:"text"`
		Reason *string `json:"reason"`
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
	return decodeNextOpencodeRunEvent(decoder)
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
		Text:      strings.TrimSpace(envelope.Text),
		Tool:      strings.TrimSpace(envelope.Tool),
	}
	event.State.Status = strings.TrimSpace(envelope.State.Status)
	event.State.Title = strings.TrimSpace(envelope.State.Title)
	event.State.Input = append(json.RawMessage(nil), envelope.State.Input...)
	event.Part.Text = envelope.Part.Text
	if envelope.Part.Reason != nil {
		event.Part.ReasonPresent = true
		event.Part.Reason = *envelope.Part.Reason
	}
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
		return firstNonEmpty(stringValue(object["message"]), stringValue(object["text"]), "provider error event")
	}
	return "provider error event"
}
