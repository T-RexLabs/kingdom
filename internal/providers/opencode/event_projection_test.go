package opencode

import (
	"io"
	"strings"
	"testing"
)

type repeatedJSONItems struct {
	item          string
	count, offset int
}

func (r *repeatedJSONItems) Read(dst []byte) (int, error) {
	if r.count == 0 {
		return 0, io.EOF
	}
	n := 0
	for n < len(dst) && r.count > 0 {
		copied := copy(dst[n:], r.item[r.offset:])
		n += copied
		r.offset += copied
		if r.offset == len(r.item) {
			r.offset = 0
			r.count--
		}
	}
	return n, nil
}

func TestEventProjectionStreamsLargeUnknownAndToolObjects(t *testing.T) {
	item := `{"path":"example","values":[1,2,3,4,5,6]},`
	reader := io.MultiReader(strings.NewReader(`{"type":"tool_use","ignored":[`), &repeatedJSONItems{item: item, count: 150000}, strings.NewReader(`{}],"part":{"id":"tool-1","tool":"read","state":{"input":[`), &repeatedJSONItems{item: item, count: 150000}, strings.NewReader(`{}],"status":"completed"}}}`+"\n"))
	events, bytes := 0, 0
	err := decodeOpencodeRunEventStream(reader, func(n int) error { bytes += n; return nil }, func(event opencodeRunEvent) error {
		events++
		if event.Part.State.Status != "completed" || event.Part.ID != "tool-1" {
			t.Fatal("large object hid later control fields")
		}
		if len(event.Part.State.Input) > streamPreviewBytes || !strings.Contains(string(event.Part.State.Input), "preserved") {
			t.Fatal("large tool object was retained instead of a bounded preview")
		}
		return nil
	}, func(_ string, err error) { t.Fatalf("valid large object rejected: %v", err) })
	if err != nil || events != 1 || bytes <= 8*1024*1024 {
		t.Fatalf("events=%d bytes=%d err=%v", events, bytes, err)
	}
}

func TestEventProjectionRejectsDuplicateControlFieldsAndRetainsLaterEvents(t *testing.T) {
	malformed, events := 0, 0
	err := decodeOpencodeRunEventStream(strings.NewReader(`{"type":"text","Type":"step_finish"}`+"\n"+`{"type":"step_finish","part":{"reason":"stop"}}`), nil, func(opencodeRunEvent) error { events++; return nil }, func(string, error) { malformed++ })
	if err != nil || events != 1 || malformed != 1 {
		t.Fatalf("events=%d malformed=%d err=%v", events, malformed, err)
	}
}

func TestLargeNumericToolDataIsStreamedButControlNumbersCannotBeCoerced(t *testing.T) {
	for _, field := range []string{"input", "tokens"} {
		prefix := `{"type":"tool_use","part":{"state":{"input":0.`
		suffix := `1,"status":"completed"}}}`
		if field == "tokens" {
			prefix = `{"type":"step_finish","part":{"tokens":{"total":0.`
			suffix = `1},"reason":"stop"}}`
		}
		reader := io.MultiReader(strings.NewReader(prefix), &repeatedJSONItems{item: "0", count: 9 * 1024 * 1024}, strings.NewReader(suffix))
		events, malformed := 0, 0
		err := decodeOpencodeRunEventStream(reader, nil, func(event opencodeRunEvent) error {
			events++
			if field == "input" && !strings.Contains(string(event.Part.State.Input), "numeric value") {
				t.Fatal("numeric tool preview missing")
			}
			return nil
		}, func(string, error) { malformed++ })
		if err != nil {
			t.Fatal(err)
		}
		if field == "input" && (events != 1 || malformed != 0) {
			t.Fatalf("valid numeric data events=%d malformed=%d", events, malformed)
		}
		if field == "tokens" && (events != 0 || malformed != 1) {
			t.Fatal("large numeric control value was reinterpreted as text")
		}
	}
}
