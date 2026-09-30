package opencode

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"strconv"
	"strings"
	"unicode/utf8"

	"kingdom/internal/core/providerexecution"
)

var errInvalidEventString = errors.New("invalid provider JSON string")
var errInvalidEventNumber = errors.New("invalid provider JSON number")

// eventStrings is a lexical string compactor, not a JSON parser. It moves large
// string literals to a scratch file before encoding/json validates the event's
// grammar and types. Unescaping/Unicode semantics also stay with encoding/json,
// applied to bounded, escape- and rune-aligned fragments. This avoids Decoder's
// otherwise unavoidable whole-string allocation for large text/tool output.
type eventStrings struct {
	projected    *os.File
	content      *os.File
	prefix       string
	numberMarker string
}

type eventTextContent struct {
	reader        io.ReaderAt
	offset, size  int64
	sha256        string
	nonWhitespace bool
}

func compactEventStrings(reader io.Reader) (*eventStrings, error) {
	projected, err := os.CreateTemp("", "kingdom-provider-json-*")
	if err != nil {
		return nil, err
	}
	content, err := os.CreateTemp("", "kingdom-provider-strings-*")
	if err != nil {
		projected.Close()
		os.Remove(projected.Name())
		return nil, err
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		projected.Close()
		content.Close()
		os.Remove(projected.Name())
		os.Remove(content.Name())
		return nil, err
	}
	s := &eventStrings{projected: projected, content: content, prefix: "kingdom-string:" + hex.EncodeToString(nonce[:]) + ":"}
	s.numberMarker = "kingdom-number:" + hex.EncodeToString(nonce[:])
	r := bufio.NewReaderSize(reader, 64*1024)
	w := bufio.NewWriterSize(projected, 64*1024)
	for {
		b, err := r.ReadByte()
		if err == io.EOF {
			break
		}
		if err != nil {
			s.close()
			return nil, err
		}
		if b == '"' {
			if err := s.stringLiteral(r, w); err != nil {
				s.close()
				return nil, err
			}
		} else if b == '-' || (b >= '0' && b <= '9') {
			if err := s.numberLiteral(b, r, w); err != nil {
				s.close()
				return nil, err
			}
		} else if err := w.WriteByte(b); err != nil {
			s.close()
			return nil, err
		}
	}
	if err := w.Flush(); err != nil {
		s.close()
		return nil, err
	}
	if _, err := projected.Seek(0, io.SeekStart); err != nil {
		s.close()
		return nil, err
	}
	return s, nil
}

// Numeric tool data may also contain enormous scalar literals. Validate their
// JSON lexical form with finite state, preserving complete bytes in the trace.
// Long numeric literals become diagnostic placeholders; control fields reject
// those placeholders rather than interpreting a truncated numeric value.
func (s *eventStrings) numberLiteral(first byte, r *bufio.Reader, w *bufio.Writer) error {
	const (
		start = iota
		sign
		zero
		integer
		dot
		fraction
		exponent
		exponentSign
		exponentDigits
	)
	state := start
	raw := make([]byte, 0, 128)
	large := false
	consume := func(ch byte) bool {
		digit := ch >= '0' && ch <= '9'
		switch state {
		case start:
			if ch == '-' {
				state = sign
				return true
			}
			fallthrough
		case sign:
			if ch == '0' {
				state = zero
				return true
			}
			if ch >= '1' && ch <= '9' {
				state = integer
				return true
			}
		case zero:
			if ch == '.' {
				state = dot
				return true
			}
			if ch == 'e' || ch == 'E' {
				state = exponent
				return true
			}
		case integer:
			if digit {
				return true
			}
			if ch == '.' {
				state = dot
				return true
			}
			if ch == 'e' || ch == 'E' {
				state = exponent
				return true
			}
		case dot:
			if digit {
				state = fraction
				return true
			}
		case fraction:
			if digit {
				return true
			}
			if ch == 'e' || ch == 'E' {
				state = exponent
				return true
			}
		case exponent:
			if ch == '+' || ch == '-' {
				state = exponentSign
				return true
			}
			fallthrough
		case exponentSign:
			if digit {
				state = exponentDigits
				return true
			}
		case exponentDigits:
			if digit {
				return true
			}
		}
		return false
	}
	ch := first
	for {
		if !consume(ch) {
			return errInvalidEventNumber
		}
		if len(raw) < 1024 {
			raw = append(raw, ch)
		} else {
			large = true
		}
		next, err := r.Peek(1)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		ch = next[0]
		if !((ch >= '0' && ch <= '9') || ch == '-' || ch == '+' || ch == '.' || ch == 'e' || ch == 'E') {
			break
		}
		_, _ = r.ReadByte()
	}
	if state != zero && state != integer && state != fraction && state != exponentDigits {
		return errInvalidEventNumber
	}
	if large {
		encoded, _ := json.Marshal(s.numberMarker)
		_, err := w.Write(encoded)
		return err
	}
	_, err := w.Write(raw)
	return err
}

func (s *eventStrings) stringLiteral(r *bufio.Reader, w *bufio.Writer) error {
	fragment := make([]byte, 1, 256)
	fragment[0] = '"'
	offset := int64(-1)
	var h hash.Hash
	var size int64
	spilled, nonWhitespace := false, false
	flush := func() error {
		if offset < 0 {
			var err error
			offset, err = s.content.Seek(0, io.SeekCurrent)
			if err != nil {
				return err
			}
			h = sha256.New()
		}
		fragment = append(fragment, '"')
		var decoded string
		if err := json.Unmarshal(fragment, &decoded); err != nil {
			return fmt.Errorf("%w: %v", errInvalidEventString, err)
		}
		if strings.TrimSpace(decoded) != "" {
			nonWhitespace = true
		}
		n, err := io.WriteString(io.MultiWriter(s.content, h), decoded)
		size += int64(n)
		fragment = fragment[:1]
		return err
	}
	for {
		b, err := r.ReadByte()
		if err != nil {
			return fmt.Errorf("%w: unterminated literal", errInvalidEventString)
		}
		if b == '"' {
			if !spilled {
				fragment = append(fragment, '"')
				_, err := w.Write(fragment)
				return err
			}
			if err := flush(); err != nil {
				return err
			}
			marker := fmt.Sprintf("%s%d:%d:%x:%t", s.prefix, offset, size, h.Sum(nil), nonWhitespace)
			encoded, _ := json.Marshal(marker)
			_, err := w.Write(encoded)
			return err
		}
		if b == '\\' {
			fragment = append(fragment, b)
			escaped, err := r.ReadByte()
			if err != nil {
				return fmt.Errorf("%w: unterminated escape", errInvalidEventString)
			}
			fragment = append(fragment, escaped)
			if escaped == 'u' {
				var digits [4]byte
				if _, err := io.ReadFull(r, digits[:]); err != nil {
					return fmt.Errorf("%w: unterminated Unicode escape", errInvalidEventString)
				}
				fragment = append(fragment, digits[:]...)
				value, err := strconv.ParseUint(string(digits[:]), 16, 16)
				if err == nil && value >= 0xd800 && value <= 0xdbff {
					// Never split a potential UTF-16 surrogate pair across the
					// standard decoder's independently decoded fragments.
					if pair, _ := r.Peek(6); len(pair) == 6 && pair[0] == '\\' && pair[1] == 'u' {
						fragment = append(fragment, pair...)
						_, _ = r.Discard(6)
					}
				}
			}
		} else if b >= utf8.RuneSelf {
			if err := r.UnreadByte(); err != nil {
				return err
			}
			value, _, err := r.ReadRune()
			if err != nil {
				return err
			}
			fragment = utf8.AppendRune(fragment, value)
		} else {
			fragment = append(fragment, b)
		}
		if len(fragment) >= streamPreviewBytes {
			spilled = true
			if err := flush(); err != nil {
				return err
			}
		}
	}
}

func (s *eventStrings) resolve(text string) (*eventTextContent, error) {
	if !strings.HasPrefix(text, s.prefix) {
		return nil, nil
	}
	parts := strings.Split(strings.TrimPrefix(text, s.prefix), ":")
	if len(parts) != 4 {
		return nil, fmt.Errorf("invalid internal string reference")
	}
	offset, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return nil, err
	}
	size, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return nil, err
	}
	nonWhitespace, err := strconv.ParseBool(parts[3])
	if err != nil {
		return nil, err
	}
	return &eventTextContent{reader: s.content, offset: offset, size: size, sha256: parts[2], nonWhitespace: nonWhitespace}, nil
}

func (s *eventStrings) restoreMetadata(value *string) error {
	content, err := s.resolve(*value)
	if err != nil || content == nil {
		return err
	}
	summary, err := providerexecution.InspectText(context.Background(), io.NewSectionReader(content.reader, content.offset, content.size))
	if err != nil {
		return err
	}
	if summary.End-summary.Offset > streamPreviewBytes {
		return fmt.Errorf("provider identity/control field is not compact")
	}
	*value = summary.Preview
	return nil
}

func (s *eventStrings) restorePreview(value *string) error {
	content, err := s.resolve(*value)
	if err != nil || content == nil {
		return err
	}
	raw, err := io.ReadAll(io.NewSectionReader(content.reader, content.offset, min(content.size, int64(streamPreviewBytes))))
	if err != nil {
		return err
	}
	*value = activityPreview(string(raw))
	return nil
}

// Tool input is diagnostic-only. Keep a valid, bounded JSON preview while the
// complete original value remains in the invocation's immutable stdout trace.
func (s *eventStrings) previewJSON(raw json.RawMessage) (json.RawMessage, error) {
	number, _ := json.Marshal(s.numberMarker)
	raw = bytes.ReplaceAll(raw, number, []byte(`"Full numeric value is preserved in the provider stream diagnostic."`))
	fallback := json.RawMessage(`"Full tool input is preserved in the provider stream diagnostic."`)
	if len(raw) > streamPreviewBytes {
		return fallback, nil
	}
	prefix := []byte(`"` + s.prefix)
	var out bytes.Buffer
	for len(raw) > 0 {
		index := bytes.Index(raw, prefix)
		if index < 0 {
			out.Write(raw)
			break
		}
		out.Write(raw[:index])
		raw = raw[index:]
		end := bytes.IndexByte(raw[1:], '"') + 1
		if end <= 0 {
			return nil, fmt.Errorf("invalid internal tool-input string reference")
		}
		value := string(raw[1:end])
		if err := s.restorePreview(&value); err != nil {
			return nil, err
		}
		encoded, err := json.Marshal(value)
		if err != nil {
			return nil, err
		}
		if out.Len()+len(encoded) > streamPreviewBytes {
			return fallback, nil
		}
		out.Write(encoded)
		raw = raw[end+1:]
	}
	if out.Len() > streamPreviewBytes {
		return fallback, nil
	}
	return out.Bytes(), nil
}

func (s *eventStrings) close() {
	s.projected.Close()
	s.content.Close()
	os.Remove(s.projected.Name())
	os.Remove(s.content.Name())
}

func (e opencodeRunEvent) hasText() bool {
	if e.textContent != nil {
		return e.textContent.nonWhitespace
	}
	return strings.TrimSpace(e.Part.Text) != ""
}

func (e opencodeRunEvent) textChunks(visit func(string, bool) error) error {
	if e.textContent == nil {
		return visit(e.Part.Text, true)
	}
	r := bufio.NewReaderSize(io.NewSectionReader(e.textContent.reader, e.textContent.offset, e.textContent.size), streamPreviewBytes)
	buffer := make([]byte, streamPreviewBytes)
	first := true
	for {
		n, err := r.Read(buffer)
		if n > 0 {
			// Leave a partial UTF-8 rune for the next callback so consumers may
			// safely encode each progress chunk independently as JSON.
			for n > 0 && !utf8.Valid(buffer[:n]) && n > len(buffer)-utf8.UTFMax {
				b, readErr := r.ReadByte()
				if readErr != nil {
					return readErr
				}
				buffer = append(buffer[:n], b)
				n++
			}
			if err := visit(string(buffer[:n]), first); err != nil {
				return err
			}
			first = false
			buffer = buffer[:streamPreviewBytes]
		}
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}
