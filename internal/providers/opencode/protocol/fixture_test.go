package protocol

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"kingdom/internal/core/providerexecution"
)

const testInvocationID = "invocation-protocol-test"

// fixture is one captured request interval. The files under testdata are real
// OpenCode 2.0.29 traces reduced to the request interval; strings over 600
// bytes are shortened consistently in events and messages.
type fixture struct {
	t         *testing.T
	Identity  providerexecution.RequestIdentity `json:"identity"`
	Watermark int64                             `json:"watermark"`
	Events    []map[string]any                  `json:"events"`
	Messages  []map[string]any                  `json:"messages"`
}

func loadFixture(t *testing.T, name string) *fixture {
	t.Helper()
	file, err := os.Open(filepath.Join("testdata", name+".json"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	f := &fixture{t: t}
	decoder := json.NewDecoder(file)
	decoder.UseNumber()
	if err := decoder.Decode(f); err != nil {
		t.Fatal(err)
	}
	return f
}

func rawSource(raw []byte) Source {
	return Source{
		Ref: providerexecution.ReferenceText(testInvocationID, string(raw)),
		Open: func(context.Context) (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader(raw)), nil
		},
	}
}

func (f *fixture) source(v any) Source {
	f.t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		f.t.Fatal(err)
	}
	return rawSource(raw)
}

func (f *fixture) evidence() Evidence {
	return Evidence{
		InvocationID:   testInvocationID,
		Events:         f.source(f.Events),
		Messages:       f.source(f.Messages),
		Watermark:      f.Watermark,
		CaptureHealthy: true,
		WaitCompleted:  true,
	}
}

func readSource(t *testing.T, src Source) []byte {
	t.Helper()
	reader, err := src.Open(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	raw, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func clone(t *testing.T, value map[string]any) map[string]any {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var copied map[string]any
	if err := decoder.Decode(&copied); err != nil {
		t.Fatal(err)
	}
	return copied
}

func data(e map[string]any) map[string]any { return e["data"].(map[string]any) }

func (f *fixture) eventIndex(eventType string) int {
	f.t.Helper()
	for i, e := range f.Events {
		if e["type"] == eventType {
			return i
		}
	}
	f.t.Fatalf("fixture has no %s event", eventType)
	return -1
}

func (f *fixture) events(eventType string) []map[string]any {
	var matched []map[string]any
	for _, e := range f.Events {
		if e["type"] == eventType {
			matched = append(matched, e)
		}
	}
	return matched
}

func (f *fixture) event(eventType string) map[string]any { return f.Events[f.eventIndex(eventType)] }

func (f *fixture) lastEvent(eventType string) map[string]any {
	f.t.Helper()
	for i := len(f.Events) - 1; i >= 0; i-- {
		if f.Events[i]["type"] == eventType {
			return f.Events[i]
		}
	}
	f.t.Fatalf("fixture has no %s event", eventType)
	return nil
}

func (f *fixture) messageIndex(kind string) int {
	f.t.Helper()
	for i, m := range f.Messages {
		if m["type"] == kind {
			return i
		}
	}
	f.t.Fatalf("fixture has no %s message", kind)
	return -1
}

func (f *fixture) message(kind string) map[string]any { return f.Messages[f.messageIndex(kind)] }

func (f *fixture) lastMessage(kind string) map[string]any {
	f.t.Helper()
	for i := len(f.Messages) - 1; i >= 0; i-- {
		if f.Messages[i]["type"] == kind {
			return f.Messages[i]
		}
	}
	f.t.Fatalf("fixture has no %s message", kind)
	return nil
}

// reseq renumbers the public sequence contiguously after a structural edit and
// keeps the watermark on the final event.
func (f *fixture) reseq() {
	f.t.Helper()
	base, err := f.Events[0]["durable"].(map[string]any)["seq"].(json.Number).Int64()
	if err != nil {
		f.t.Fatal(err)
	}
	for i, e := range f.Events {
		e["durable"].(map[string]any)["seq"] = json.Number(strconv.FormatInt(base+int64(i), 10))
	}
	f.Watermark = base + int64(len(f.Events)) - 1
}

func (f *fixture) insertEvent(i int, e map[string]any) {
	f.Events = append(f.Events[:i], append([]map[string]any{e}, f.Events[i:]...)...)
	f.reseq()
}

func (f *fixture) removeEvent(i int) {
	f.Events = append(f.Events[:i], f.Events[i+1:]...)
	f.reseq()
}

func (f *fixture) insertMessage(i int, m map[string]any) {
	f.Messages = append(f.Messages[:i], append([]map[string]any{m}, f.Messages[i:]...)...)
}

func (f *fixture) removeMessage(i int) {
	f.Messages = append(f.Messages[:i], f.Messages[i+1:]...)
}

// newEvent builds a public session event of this fixture's session.
func (f *fixture) newEvent(id, eventType string, fields map[string]any) map[string]any {
	d := map[string]any{"sessionID": f.Identity.SessionID}
	for k, v := range fields {
		d[k] = v
	}
	return map[string]any{
		"id":       id,
		"type":     eventType,
		"location": map[string]any{"directory": f.Identity.Location},
		"data":     d,
		"durable":  map[string]any{"aggregateID": f.Identity.SessionID, "seq": json.Number("0"), "version": json.Number("1")},
	}
}
