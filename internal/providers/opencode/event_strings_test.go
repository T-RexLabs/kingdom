package opencode

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestLargeJSONStringIsDecodedIncrementallyWithStandardEscapeSemantics(t *testing.T) {
	for _, text := range []string{
		strings.Repeat("界😀\\\"\n", 200000),
		strings.Repeat(" ", streamPreviewBytes*2) + "terminal after whitespace",
		strings.Repeat("界", streamPreviewBytes),
	} {
		encoded, _ := json.Marshal(text)
		line := `{"type":"text","part":{"text":` + string(encoded) + `}}` + "\n"
		count := 0
		err := decodeOpencodeRunEventStream(strings.NewReader(line), nil, func(event opencodeRunEvent) error {
			count++
			if event.textContent == nil || len(event.Part.Text) > streamPreviewBytes {
				t.Fatal("giant string materialized in event")
			}
			if !event.hasText() {
				t.Fatal("lost text after a whitespace-only prefix")
			}
			h := sha256.New()
			chunks := 0
			if err := event.textChunks(func(chunk string, first bool) error {
				if len(chunk) > streamPreviewBytes+utf8.UTFMax || !utf8.ValidString(chunk) {
					t.Fatal("unbounded or split-rune text chunk")
				}
				if first != (chunks == 0) {
					t.Fatal("text part separator moved to a fragment")
				}
				chunks++
				_, err := io.WriteString(h, chunk)
				return err
			}); err != nil {
				return err
			}
			want := sha256.Sum256([]byte(text))
			if fmt.Sprintf("%x", h.Sum(nil)) != fmt.Sprintf("%x", want) {
				t.Fatal("large JSON string changed during streaming")
			}
			return nil
		}, func(_ string, err error) { t.Fatalf("valid string rejected: %v", err) })
		if err != nil || count != 1 {
			t.Fatalf("events=%d err=%v", count, err)
		}
	}
}

func TestLargeJSONStringSurrogateBoundaryAndMalformedLaterEscape(t *testing.T) {
	for _, escape := range []string{`\ud83d\ude00`, `\ud800x`, `\ud800\u0061`, `\u0061`, `\\`, `\"`} {
		literal := `"` + strings.Repeat("x", streamPreviewBytes-4) + escape + strings.Repeat("z", streamPreviewBytes) + `"`
		var expected string
		if err := json.Unmarshal([]byte(literal), &expected); err != nil {
			t.Fatal(err)
		}
		var actual strings.Builder
		err := decodeOpencodeRunEventStream(strings.NewReader(`{"type":"text","part":{"text":`+literal+`}}`), nil, func(e opencodeRunEvent) error {
			return e.textChunks(func(text string, _ bool) error { _, err := io.WriteString(&actual, text); return err })
		}, nil)
		if err != nil || actual.String() != expected {
			t.Fatalf("escape %s was changed: %v", escape, err)
		}
	}
	malformed, events := 0, 0
	malformedLine := `{"type":"text","part":{"text":"` + strings.Repeat("x", streamPreviewBytes*2) + `\q"}}` + "\n"
	stream := malformedLine + `{"type":"step_finish","part":{"reason":"stop"}}` + "\n"
	err := decodeOpencodeRunEventStream(strings.NewReader(stream), nil, func(opencodeRunEvent) error { events++; return nil }, func(digest string, _ error) {
		malformed++
		if digest != fmt.Sprintf("%x", sha256.Sum256([]byte(malformedLine))) {
			t.Fatal("malformed diagnostics hashed only a preview")
		}
	})
	if err != nil || malformed != 1 || events != 1 {
		t.Fatalf("malformed=%d later=%d err=%v", malformed, events, err)
	}
}

func TestLargeToolInputHasReadableBoundedPreview(t *testing.T) {
	line := `{"type":"tool_use","part":{"tool":"read","state":{"input":{"content":"` + strings.Repeat("tool input ", 10000) + `"}}}}` + "\n"
	err := decodeOpencodeRunEventStream(strings.NewReader(line), nil, func(e opencodeRunEvent) error {
		if len(e.Part.State.Input) > streamPreviewBytes || !json.Valid(e.Part.State.Input) {
			t.Fatal("tool preview is not bounded valid JSON")
		}
		if !strings.Contains(string(e.Part.State.Input), "tool input") || strings.Contains(string(e.Part.State.Input), "kingdom-string:") {
			t.Fatal("tool preview exposed an internal scratch marker")
		}
		return nil
	}, func(string, error) { t.Fatal("valid tool input was rejected") })
	if err != nil {
		t.Fatal(err)
	}
}
