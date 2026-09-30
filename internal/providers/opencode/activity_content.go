package opencode

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"kingdom/internal/core/providerexecution"
)

// jsonValueSpan indexes provider JSON without materializing nested values. The
// same source must remain open until the synchronous activity consumer returns.
type jsonValueSpan struct {
	offset, size int64
	kind         byte
}

type jsonSpanReader struct {
	ctx    context.Context
	r      *bufio.Reader
	offset int64
	visit  func([]string, jsonValueSpan) error
}

func walkJSONSpans(ctx context.Context, source io.ReaderAt, span jsonValueSpan, visit func([]string, jsonValueSpan) error) error {
	r := &jsonSpanReader{ctx: ctx, r: bufio.NewReaderSize(io.NewSectionReader(source, span.offset, span.size), 64<<10), offset: span.offset, visit: visit}
	if _, err := r.value(nil, 0); err != nil {
		return err
	}
	if err := r.space(); err != nil && err != io.EOF {
		return err
	}
	if _, err := r.r.Peek(1); err != io.EOF {
		return fmt.Errorf("trailing activity JSON")
	}
	return nil
}
func (r *jsonSpanReader) take() (byte, error) {
	if r.offset%(64<<10) == 0 {
		if err := r.ctx.Err(); err != nil {
			return 0, err
		}
	}
	b, e := r.r.ReadByte()
	if e == nil {
		r.offset++
	}
	return b, e
}
func (r *jsonSpanReader) space() error {
	for {
		b, e := r.r.Peek(1)
		if e != nil {
			return e
		}
		if !strings.ContainsRune(" \t\r\n", rune(b[0])) {
			return nil
		}
		if _, e = r.take(); e != nil {
			return e
		}
	}
}
func (r *jsonSpanReader) expect(want byte) error {
	b, e := r.take()
	if e != nil {
		return e
	}
	if b != want {
		return fmt.Errorf("invalid activity JSON delimiter")
	}
	return nil
}
func (r *jsonSpanReader) quoted(capture bool) (string, error) {
	if e := r.expect('"'); e != nil {
		return "", e
	}
	raw := []byte{'"'}
	bounded := true
	for {
		b, e := r.take()
		if e != nil {
			return "", e
		}
		if b < 0x20 {
			return "", fmt.Errorf("invalid activity JSON string")
		}
		if capture && bounded {
			if len(raw) < 4096 {
				raw = append(raw, b)
			} else {
				bounded = false
			}
		}
		if b == '"' {
			break
		}
		if b == '\\' {
			b, e = r.take()
			if e != nil {
				return "", e
			}
			if capture && bounded {
				raw = append(raw, b)
			}
			if !strings.ContainsRune("\"\\/bfnrtu", rune(b)) {
				return "", fmt.Errorf("invalid activity JSON escape")
			}
			if b == 'u' {
				for range 4 {
					b, e = r.take()
					if e != nil {
						return "", e
					}
					if !strings.ContainsRune("0123456789abcdefABCDEF", rune(b)) {
						return "", fmt.Errorf("invalid activity Unicode escape")
					}
					if capture && bounded {
						raw = append(raw, b)
					}
				}
			}
		}
	}
	if !capture || !bounded {
		return "", nil
	}
	var key string
	if e := json.Unmarshal(raw, &key); e != nil {
		return "", e
	}
	return key, nil
}
func (r *jsonSpanReader) value(path []string, depth int) (jsonValueSpan, error) {
	if e := r.ctx.Err(); e != nil {
		return jsonValueSpan{}, e
	}
	if depth > 10000 {
		return jsonValueSpan{}, fmt.Errorf("activity JSON exceeds standard nesting depth")
	}
	if e := r.space(); e != nil {
		return jsonValueSpan{}, e
	}
	first, e := r.r.Peek(1)
	if e != nil {
		return jsonValueSpan{}, e
	}
	span := jsonValueSpan{offset: r.offset, kind: first[0]}
	switch span.kind {
	case '{', '[':
		_, _ = r.take()
		close := byte('}')
		if span.kind == '[' {
			close = ']'
		}
		if e := r.space(); e != nil {
			return span, e
		}
		b, e := r.r.Peek(1)
		if e != nil {
			return span, e
		}
		if b[0] != close {
			for {
				key := "*"
				if span.kind == '{' {
					key, e = r.quoted(true)
					if e != nil {
						return span, e
					}
					if e = r.space(); e != nil {
						return span, e
					}
					if e = r.expect(':'); e != nil {
						return span, e
					}
				}
				if _, e = r.value(append(path, key), depth+1); e != nil {
					return span, e
				}
				if e = r.space(); e != nil {
					return span, e
				}
				b, e = r.r.Peek(1)
				if e != nil {
					return span, e
				}
				if b[0] == close {
					break
				}
				if e = r.expect(','); e != nil {
					return span, e
				}
				if e = r.space(); e != nil {
					return span, e
				}
			}
		}
		if e = r.expect(close); e != nil {
			return span, e
		}
	case '"':
		if _, e = r.quoted(false); e != nil {
			return span, e
		}
	case 't', 'f', 'n':
		literal := "true"
		if span.kind == 'f' {
			literal = "false"
		}
		if span.kind == 'n' {
			literal = "null"
		}
		for i := range literal {
			if e = r.expect(literal[i]); e != nil {
				return span, e
			}
		}
	default:
		// The complete event is validated by the standard decoder before use. Here
		// numbers are scanned as raw bytes, avoiding float conversion or truncation.
		count := 0
		for {
			b, e := r.r.Peek(1)
			if e == io.EOF {
				break
			}
			if e != nil {
				return span, e
			}
			if !strings.ContainsRune("0123456789+-.eE", rune(b[0])) {
				break
			}
			_, _ = r.take()
			count++
		}
		if count == 0 {
			return span, fmt.Errorf("invalid activity JSON scalar")
		}
	}
	span.size = r.offset - span.offset
	if r.visit != nil {
		if e = r.visit(path, span); e != nil {
			return span, e
		}
	}
	return span, nil
}

func readActivityScalar(source io.ReaderAt, span jsonValueSpan) (string, error) {
	if span.size > 4096 {
		return "", fmt.Errorf("provider activity metadata exceeds bound")
	}
	raw, err := io.ReadAll(io.NewSectionReader(source, span.offset, span.size))
	if err != nil {
		return "", err
	}
	if span.kind == '"' {
		var text string
		err = json.Unmarshal(raw, &text)
		return text, err
	}
	return string(raw), nil
}

func stageActivityValue(ctx context.Context, source io.ReaderAt, span jsonValueSpan) (providerexecution.ActivityContent, func() error, error) {
	reader := io.NewSectionReader(source, span.offset, span.size)
	if span.kind != '"' {
		return providerexecution.StageActivityContent(ctx, reader)
	}
	compacted, err := compactEventStrings(activityContextReader{ctx: ctx, reader: reader})
	if err != nil {
		return providerexecution.ActivityContent{}, nil, err
	}
	defer compacted.close()
	var text string
	if err = json.NewDecoder(compacted.projected).Decode(&text); err != nil {
		return providerexecution.ActivityContent{}, nil, err
	}
	full, err := compacted.resolve(text)
	if err != nil {
		return providerexecution.ActivityContent{}, nil, err
	}
	if full == nil {
		return providerexecution.ActivityText(text), func() error { return nil }, nil
	}
	return providerexecution.StageActivityContent(ctx, io.NewSectionReader(full.reader, full.offset, full.size))
}

func stageActivityFields(ctx context.Context, source io.ReaderAt, span jsonValueSpan, prefix string) (map[string]providerexecution.ActivityContent, func(), error) {
	fields := map[string]providerexecution.ActivityContent{}
	var closes []func() error
	cleanup := func() {
		for _, close := range closes {
			_ = close()
		}
	}
	err := walkJSONSpans(ctx, source, span, func(path []string, value jsonValueSpan) error {
		key := strings.Join(path, ".")
		name := ""
		switch key {
		case prefix + "state.input":
			name = "input"
		case prefix + "state.output":
			name = "output"
		case prefix + "text":
			name = "text"
		}
		if name == "" {
			return nil
		}
		content, close, err := stageActivityValue(ctx, source, value)
		if err != nil {
			return err
		}
		fields[name] = content
		closes = append(closes, close)
		return nil
	})
	if err != nil {
		cleanup()
		return nil, func() {}, err
	}
	return fields, cleanup, nil
}

// Diagnostic parsing observes cancellation while scanning even a single large
// scalar; the provider export's process deadline remains unchanged.
type activityContextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r activityContextReader) Read(raw []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(raw)
}
