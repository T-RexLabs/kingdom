package opencode

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"kingdom/internal/core/providerexecution"
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
	if _, err := decoder.Token(); err != io.EOF {
		if err == nil {
			return opencodeRunEvent{}, fmt.Errorf("multiple JSON values in provider event line")
		}
		return opencodeRunEvent{}, fmt.Errorf("trailing provider event data: %w", err)
	}
	fields, cleanup, err := stageActivityFields(context.Background(), strings.NewReader(line), jsonValueSpan{size: int64(len(line))}, "part.")
	if err != nil {
		return opencodeRunEvent{}, err
	}
	defer cleanup()
	event.activityContent = map[string]providerexecution.ActivityContent{}
	for key, content := range fields {
		reader, err := content.Open(context.Background())
		if err != nil {
			return opencodeRunEvent{}, err
		}
		raw, err := io.ReadAll(reader)
		_ = reader.Close()
		if err != nil {
			return opencodeRunEvent{}, err
		}
		event.activityContent[key] = providerexecution.ActivityText(string(raw))
	}
	return event, nil
}

// onMalformed receives the SHA-256 of the complete raw JSONL frame, including
// its newline when present. Provider bytes themselves stay in the full trace.
func decodeOpencodeRunEventStream(stream io.Reader, onLine func(int) error, onEvent func(opencodeRunEvent) error, onMalformed func(string, error), activity ...func()) error {
	// Frame JSONL on disk. Decoder owns at most one event, never the complete
	// stream. In particular ReadString/Scanner must not accumulate a giant line
	// in addition to encoding/json's allocation for its string values.
	file, err := os.CreateTemp("", "kingdom-provider-frame-*")
	if err != nil {
		return err
	}
	defer func() { file.Close(); os.Remove(file.Name()) }()
	noteActivity := func() {
		for _, note := range activity {
			note()
		}
	}
	reader := bufio.NewReaderSize(stream, 64*1024)
	for {
		if err := file.Truncate(0); err != nil {
			return err
		}
		if _, err := file.Seek(0, io.SeekStart); err != nil {
			return err
		}
		var size int64
		frameHasContent := false
		lineHash := sha256.New()
		var readErr error
		for {
			part, err := reader.ReadSlice('\n')
			if len(part) > 0 {
				frameHasContent = frameHasContent || len(bytes.TrimSpace(part)) > 0
				if frameHasContent {
					noteActivity()
				}
				if _, writeErr := file.Write(part); writeErr != nil {
					return writeErr
				}
				size += int64(len(part))
				_, _ = lineHash.Write(part)
			}
			if err == bufio.ErrBufferFull {
				continue
			}
			readErr = err
			break
		}
		if readErr != nil && readErr != io.EOF {
			return readErr
		}
		if size == 0 && readErr == io.EOF {
			return nil
		}
		if onLine != nil {
			if err := onLine(int(size)); err != nil {
				return err
			}
		}
		if !frameHasContent {
			continue
		}
		var source io.Reader = activityReader{io.NewSectionReader(file, 0, size), noteActivity}
		var largeStrings *eventStrings
		if size > streamPreviewBytes {
			var err error
			largeStrings, err = compactEventStrings(source)
			if err != nil {
				if !errors.Is(err, errInvalidEventString) && !errors.Is(err, errInvalidEventNumber) {
					return err
				}
				if onMalformed != nil {
					onMalformed(fmt.Sprintf("%x", lineHash.Sum(nil)), err)
				}
				continue
			}
			source = largeStrings.projected
		}
		decoder := json.NewDecoder(source)
		numberMarker := ""
		if largeStrings != nil {
			numberMarker = largeStrings.numberMarker
		}
		event, err := decodeNextOpencodeRunEvent(decoder, numberMarker)
		if err == io.EOF {
			// Empty frames were filtered above. EOF here means the token
			// projection stopped inside a nonempty, incomplete JSON value.
			err = io.ErrUnexpectedEOF
		}
		if err == nil {
			if _, trailingErr := decoder.Token(); trailingErr != io.EOF {
				err = fmt.Errorf("trailing provider event data")
			}
		}
		if err != nil {
			if largeStrings != nil {
				largeStrings.close()
			}
			if onMalformed != nil {
				onMalformed(fmt.Sprintf("%x", lineHash.Sum(nil)), err)
			}
			continue
		}
		if largeStrings != nil {
			// Identity and control fields retain their exact values. Provider
			// bodies/tool payload strings remain disk-backed or diagnostic-only.
			for _, field := range []*string{&event.Type, &event.SessionID, &event.Part.Type, &event.Part.ID, &event.Part.CallID, &event.Part.MessageID, &event.Part.Tool, &event.Part.Reason, &event.Part.State.Status, &event.ProviderErrorCode, &event.ProviderErrorCategory, &event.RetryAt} {
				if err := largeStrings.restoreMetadata(field); err != nil {
					largeStrings.close()
					return err
				}
			}
			for _, field := range []*string{&event.Part.State.Title, &event.ErrorText, &event.Part.Metadata.OpenAI.ReasoningEncryptedContent, &event.Part.Metadata.Redacted} {
				if err := largeStrings.restorePreview(field); err != nil {
					largeStrings.close()
					return err
				}
			}
			event.Part.State.Input, err = largeStrings.previewJSON(event.Part.State.Input)
			if err != nil {
				largeStrings.close()
				return err
			}
			event.textContent, err = largeStrings.resolve(event.Part.Text)
			if err != nil {
				largeStrings.close()
				return err
			}
			if event.textContent != nil {
				preview, readErr := io.ReadAll(io.NewSectionReader(event.textContent.reader, event.textContent.offset, min(event.textContent.size, int64(streamPreviewBytes))))
				if readErr != nil {
					largeStrings.close()
					return readErr
				}
				event.Part.Text = string(preview)
			}
		}
		fields, cleanup, contentErr := stageActivityFields(context.Background(), file, jsonValueSpan{size: size}, "part.")
		if contentErr != nil {
			if largeStrings != nil {
				largeStrings.close()
			}
			return contentErr
		}
		event.activityContent = fields
		err = onEvent(event)
		cleanup()
		if largeStrings != nil {
			largeStrings.close()
		}
		if err != nil {
			return err
		}
	}
}

type activityReader struct {
	io.Reader
	activity func()
}

func (r activityReader) Read(raw []byte) (int, error) {
	n, err := r.Reader.Read(raw)
	if n > 0 {
		r.activity()
	}
	return n, err
}

func decodeNextOpencodeRunEvent(decoder *json.Decoder, numberMarkers ...string) (opencodeRunEvent, error) {
	decoder.UseNumber()
	marker := ""
	if len(numberMarkers) > 0 {
		marker = numberMarkers[0]
	}
	projected, err := projectEventValue(&boundedJSONTokens{Decoder: decoder}, runEventProjection, marker)
	if err != nil {
		return opencodeRunEvent{}, err
	}
	raw, err := json.Marshal(projected)
	if err != nil {
		return opencodeRunEvent{}, err
	}
	var envelope opencodeRunEventWire
	if err := json.Unmarshal(raw, &envelope); err != nil {
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
