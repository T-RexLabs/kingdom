package modelproxy

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"unicode/utf8"
)

const maxChatGPTPreamble = 64 << 10

// chatGPTResponseStream proves a complete initial Responses SSE frame before
// deriving a missing media type. The returned reader replays all bytes, even
// bytes buffered beyond that frame. Native completion validation still owns the
// complete response, and the caller's normal response budget counts this prefix.
func chatGPTResponseStream(ctx context.Context, body io.Reader, budget int64) (io.Reader, ResponseShape, error) {
	limit := int64(maxChatGPTPreamble)
	if budget < limit {
		limit = budget
	}
	bounded := io.LimitReader(body, limit+1)
	reader := bufio.NewReader(bounded)
	prefix := make([]byte, 0, 4096)
	var line []byte
	var data []byte
	event := ""
	eventSet := false
	dataSet := false
	afterCR := false
	for {
		if err := ctx.Err(); err != nil {
			return nil, "", err
		}
		value, err := reader.ReadByte()
		if err != nil {
			if ctx.Err() != nil {
				return nil, "", ctx.Err()
			}
			if errors.Is(err, io.EOF) {
				if !dataSet && !eventSet && len(line) != 0 && !strings.HasPrefix(string(line), "data:") && !strings.HasPrefix(string(line), "event:") && !strings.HasPrefix(string(line), ":") {
					return nil, classifyResponseLine(string(line)), ErrDenied
				}
				return nil, ShapeTruncated, ErrDenied
			}
			return nil, "", err
		}
		prefix = append(prefix, value)
		if int64(len(prefix)) > limit {
			return nil, ShapePreambleLimit, ErrDenied
		}
		if afterCR && value == '\n' {
			afterCR = false
			continue
		}
		afterCR = false
		if value != '\n' && value != '\r' {
			line = append(line, value)
			continue
		}
		afterCR = value == '\r'
		if len(line) == 0 {
			if !dataSet {
				if eventSet {
					return nil, ShapeInvalidFrame, ErrDenied
				}
				continue
			}
			if !utf8.Valid(data) {
				return nil, ShapeInvalidFrame, ErrDenied
			}
			object, err := uniqueObject(data)
			if err != nil {
				return nil, ShapeInvalidFrame, ErrDenied
			}
			kind, ok := object["type"].(string)
			if !ok || !initialResponsesEvent(kind) || (eventSet && event != kind) {
				return nil, ShapeInvalidFrame, ErrDenied
			}
			return io.MultiReader(bytes.NewReader(prefix), reader, body), "", nil
		}
		if !utf8.Valid(line) {
			return nil, ShapeInvalidFrame, ErrDenied
		}
		text := string(line)
		line = line[:0]
		if strings.HasPrefix(text, ":") {
			continue
		}
		field, fieldValue, found := strings.Cut(text, ":")
		if !found {
			return nil, classifyResponseLine(text), ErrDenied
		}
		fieldValue = strings.TrimPrefix(fieldValue, " ")
		switch field {
		case "event":
			if eventSet {
				return nil, ShapeInvalidFrame, ErrDenied
			}
			event, eventSet = fieldValue, true
		case "data":
			if dataSet {
				data = append(data, '\n')
			}
			data = append(data, fieldValue...)
			dataSet = true
		default:
			return nil, classifyResponseLine(text), ErrDenied
		}
	}
}

func initialResponsesEvent(kind string) bool {
	switch kind {
	case "response.created", "response.in_progress", "response.completed", "response.failed", "response.incomplete":
		return true
	default:
		return false
	}
}

func classifyResponseLine(line string) ResponseShape {
	line = strings.TrimSpace(line)
	if strings.HasPrefix(line, "<") {
		return ShapeHTML
	}
	if strings.HasPrefix(line, "{") || strings.HasPrefix(line, "[") {
		return ShapeJSON
	}
	return ShapeOther
}
