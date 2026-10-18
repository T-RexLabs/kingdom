package opencode

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeModel is a deterministic OpenAI-compatible endpoint. The last user text
// chooses the reply: TEXT:<word>, WRITE:<file>, SLEEP:<seconds>, FINISH:<reason>.
type fakeModel struct {
	server *httptest.Server
	mu     sync.Mutex
	calls  []fakeModelCall
}

type fakeModelCall struct {
	System   []string
	LastUser string
	Messages int
}

var (
	fakeText   = regexp.MustCompile(`TEXT:(\S+)`)
	fakeWrite  = regexp.MustCompile(`WRITE:(\S+)`)
	fakeSleep  = regexp.MustCompile(`SLEEP:(\d+)`)
	fakeFinish = regexp.MustCompile(`FINISH:(\S+)`)
	fakeShell  = regexp.MustCompile(`SHELL64:(\S+)`)
)

func newFakeModel(t *testing.T) *fakeModel {
	t.Helper()
	m := &fakeModel{}
	m.server = httptest.NewServer(http.HandlerFunc(m.serve))
	t.Cleanup(m.server.Close)
	return m
}

func (m *fakeModel) URL() string { return m.server.URL + "/v1" }

func (m *fakeModel) Calls() []fakeModelCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]fakeModelCall(nil), m.calls...)
}

func fakeMessageText(message map[string]any) string {
	switch content := message["content"].(type) {
	case string:
		return content
	case []any:
		var parts []string
		for _, part := range content {
			if object, ok := part.(map[string]any); ok {
				if text, ok := object["text"].(string); ok {
					parts = append(parts, text)
				}
			}
		}
		return strings.Join(parts, " ")
	}
	return ""
}

func (m *fakeModel) serve(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Messages []map[string]any `json:"messages"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	call := fakeModelCall{Messages: len(body.Messages)}
	toolResults := map[string]bool{}
	for _, message := range body.Messages {
		switch message["role"] {
		case "system":
			call.System = append(call.System, fakeMessageText(message))
		case "user":
			call.LastUser = fakeMessageText(message)
		case "tool":
			if id, ok := message["tool_call_id"].(string); ok {
				toolResults[id] = true
			}
		}
	}
	m.mu.Lock()
	m.calls = append(m.calls, call)
	m.mu.Unlock()

	if sleep := fakeSleep.FindStringSubmatch(call.LastUser); sleep != nil && len(toolResults) == 0 {
		var seconds int
		fmt.Sscan(sleep[1], &seconds)
		select {
		case <-time.After(time.Duration(seconds) * time.Second):
		case <-r.Context().Done():
			return
		}
	}
	var chunks []map[string]any
	if write := fakeWrite.FindStringSubmatch(call.LastUser); write != nil && !toolResults["call_write"] {
		arguments, _ := json.Marshal(map[string]string{"path": write[1], "content": "hello\n"})
		chunks = []map[string]any{
			{"choices": []any{map[string]any{"index": 0, "delta": map[string]any{"role": "assistant", "tool_calls": []any{map[string]any{"index": 0, "id": "call_write", "type": "function", "function": map[string]any{"name": "write", "arguments": string(arguments)}}}}}}},
			{"choices": []any{map[string]any{"index": 0, "delta": map[string]any{}, "finish_reason": "tool_calls"}}},
		}
	} else if shell := fakeShell.FindStringSubmatch(call.LastUser); shell != nil && !toolResults["call_shell"] {
		command, _ := base64.StdEncoding.DecodeString(shell[1])
		arguments, _ := json.Marshal(map[string]any{"command": string(command), "timeout": 20000, "description": "fixture command"})
		chunks = []map[string]any{
			{"choices": []any{map[string]any{"index": 0, "delta": map[string]any{"role": "assistant", "tool_calls": []any{map[string]any{"index": 0, "id": "call_shell", "type": "function", "function": map[string]any{"name": "shell", "arguments": string(arguments)}}}}}}},
			{"choices": []any{map[string]any{"index": 0, "delta": map[string]any{}, "finish_reason": "tool_calls"}}},
		}
	} else {
		reply := "OK"
		if text := fakeText.FindStringSubmatch(call.LastUser); text != nil {
			reply = text[1]
		} else if len(toolResults) > 0 {
			reply = "DONE"
		}
		finish := "stop"
		if f := fakeFinish.FindStringSubmatch(call.LastUser); f != nil {
			finish = f[1]
		}
		chunks = []map[string]any{
			{"choices": []any{map[string]any{"index": 0, "delta": map[string]any{"role": "assistant", "content": reply}}}},
			{"choices": []any{map[string]any{"index": 0, "delta": map[string]any{}, "finish_reason": finish}}},
		}
	}
	w.Header().Set("Content-Type", "text/event-stream")
	for _, chunk := range chunks {
		raw, _ := json.Marshal(chunk)
		fmt.Fprintf(w, "data: %s\n\n", raw)
	}
	fmt.Fprint(w, "data: [DONE]\n\n")
}
