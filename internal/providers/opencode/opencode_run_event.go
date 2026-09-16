package opencode

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"kingdom/internal/core/providerexecution"
	"kingdom/internal/providerlimits"
)

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

func decodeOpencodeRunEventStream(stream io.Reader, onLine func(int) error, onEvent func(opencodeRunEvent) error, onMalformed func(string, error)) error {
	scanner := bufio.NewScanner(stream)
	// Let the split function classify every oversize line before Scanner can
	// replace it with an untyped token-too-long error.
	scanner.Buffer(make([]byte, 0, 64*1024), providerlimits.OpenCodeEventLineBytes+1)
	scanner.Split(splitBoundedOpencodeRunEventLine)
	for scanner.Scan() {
		raw := scanner.Text()
		if onLine != nil {
			if err := onLine(len(raw)); err != nil {
				return err
			}
		}
		line := strings.TrimSpace(raw)
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

func splitBoundedOpencodeRunEventLine(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if newline := bytes.IndexByte(data, '\n'); newline >= 0 {
		if newline > providerlimits.OpenCodeEventLineBytes {
			return 0, nil, &providerlimits.LimitExceededError{Resource: "event line", Limit: int64(providerlimits.OpenCodeEventLineBytes)}
		}
		return newline + 1, data[:newline], nil
	}
	if len(data) > providerlimits.OpenCodeEventLineBytes {
		return 0, nil, &providerlimits.LimitExceededError{Resource: "event line", Limit: int64(providerlimits.OpenCodeEventLineBytes)}
	}
	if atEOF && len(data) > 0 {
		return len(data), data, nil
	}
	return 0, nil, nil
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
	event.ProviderErrorCode, event.ProviderErrorCategory, event.ProviderErrorStatus = extractRunEventErrorMetadata(envelope.Error)
	event.RetryAt, event.RetryAfterMS = extractRunEventRetryHint(envelope.Error)
	return event, nil
}

func extractRunEventErrorMetadata(raw json.RawMessage) (string, string, int) {
	var envelope struct {
		Code       string          `json:"code"`
		Category   string          `json:"category"`
		Status     json.RawMessage `json:"status"`
		StatusCode json.RawMessage `json:"statusCode"`
		Data       json.RawMessage `json:"data"`
	}
	if len(raw) == 0 || json.Unmarshal(raw, &envelope) != nil {
		return "", "", 0
	}
	var data struct {
		Code       string          `json:"code"`
		Category   string          `json:"category"`
		Status     json.RawMessage `json:"status"`
		StatusCode json.RawMessage `json:"statusCode"`
	}
	_ = json.Unmarshal(envelope.Data, &data)
	return firstNonEmpty(data.Code, envelope.Code), firstNonEmpty(data.Category, envelope.Category), parseRunEventErrorStatus(data.StatusCode, data.Status, envelope.StatusCode, envelope.Status)
}

func parseRunEventErrorStatus(values ...json.RawMessage) int {
	for _, raw := range values {
		var status int
		if json.Unmarshal(raw, &status) == nil && status >= 100 && status <= 599 {
			return status
		}
		var text string
		if json.Unmarshal(raw, &text) != nil {
			continue
		}
		status, err := strconv.Atoi(strings.TrimSpace(text))
		if err == nil && status >= 100 && status <= 599 {
			return status
		}
	}
	return 0
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

// extractRunEventRetryHint admits only the canonical retry-hint shapes.
// Provider errors are untrusted, so malformed, negative, or overflowing
// values are omitted rather than risking an early retry.
func extractRunEventRetryHint(raw json.RawMessage) (string, *float64) {
	var envelope struct {
		RetryAt      json.RawMessage `json:"retry_at"`
		RetryAfterMS json.RawMessage `json:"retry_after_ms"`
		Data         json.RawMessage `json:"data"`
	}
	if len(raw) == 0 || json.Unmarshal(raw, &envelope) != nil {
		return "", nil
	}
	retryAt, retryAfterMS := parseRunEventRetryHint(envelope.RetryAt, envelope.RetryAfterMS)
	if len(envelope.Data) == 0 {
		return retryAt, retryAfterMS
	}
	nestedRetryAt, nestedRetryAfterMS := parseRunEventRetryHintFromObject(envelope.Data)
	if nestedRetryAt != "" {
		retryAt = nestedRetryAt
	}
	if nestedRetryAfterMS != nil {
		retryAfterMS = nestedRetryAfterMS
	}
	return retryAt, retryAfterMS
}

func parseRunEventRetryHintFromObject(raw json.RawMessage) (string, *float64) {
	var value struct {
		RetryAt      json.RawMessage `json:"retry_at"`
		RetryAfterMS json.RawMessage `json:"retry_after_ms"`
	}
	if json.Unmarshal(raw, &value) != nil {
		return "", nil
	}
	return parseRunEventRetryHint(value.RetryAt, value.RetryAfterMS)
}

func parseRunEventRetryHint(retryAtRaw, retryAfterRaw json.RawMessage) (string, *float64) {
	var retryAt string
	if json.Unmarshal(retryAtRaw, &retryAt) == nil {
		if parsed, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(retryAt)); err == nil {
			retryAt = parsed.UTC().Format(time.RFC3339Nano)
		} else {
			retryAt = ""
		}
	}
	var retryAfterMS float64
	if json.Unmarshal(retryAfterRaw, &retryAfterMS) != nil {
		return retryAt, nil
	}
	if _, valid := providerexecution.RetryAfterDurationFromMilliseconds(retryAfterMS); !valid {
		return retryAt, nil
	}
	return retryAt, &retryAfterMS
}
