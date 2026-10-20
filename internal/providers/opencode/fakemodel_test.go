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
	Tools    []string
	System   []string
	LastUser string
	// ToolResults holds the content of every tool result the model was shown.
	ToolResults []string
	Messages    int
}

var (
	fakeText           = regexp.MustCompile(`TEXT:(\S+)`)
	fakeWrite          = regexp.MustCompile(`WRITE:(\S+)`)
	fakeSleep          = regexp.MustCompile(`SLEEP:(\d+)`)
	fakePauseAfterTool = regexp.MustCompile(`PAUSE_AFTER_TOOL:(\d+)`)
	fakeFinish         = regexp.MustCompile(`FINISH:(\S+)`)
	fakeShell          = regexp.MustCompile(`SHELL64:(\S+)`)
	fakeWorkdir        = regexp.MustCompile(`WORKDIR:(\S+)`)
	fakeTool           = regexp.MustCompile(`TOOL:(\w+)`)
	fakeSearch         = regexp.MustCompile(`SEARCH:(glob|grep)`)
	fakeRead           = regexp.MustCompile(`READ:(\S+)`)
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
		Tools    []struct {
			Function struct {
				Name string `json:"name"`
			} `json:"function"`
		} `json:"tools"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	call := fakeModelCall{Messages: len(body.Messages)}
	for _, tool := range body.Tools {
		call.Tools = append(call.Tools, tool.Function.Name)
	}
	toolResults := map[string]bool{}
	for _, message := range body.Messages {
		switch message["role"] {
		case "system":
			call.System = append(call.System, fakeMessageText(message))
		case "user":
			call.LastUser = fakeMessageText(message)
		case "tool":
			call.ToolResults = append(call.ToolResults, fakeMessageText(message))
			if id, ok := message["tool_call_id"].(string); ok {
				toolResults[id] = true
			}
		}
	}
	m.mu.Lock()
	m.calls = append(m.calls, call)
	m.mu.Unlock()

	sleep := fakeSleep.FindStringSubmatch(call.LastUser)
	if len(toolResults) > 0 {
		sleep = fakePauseAfterTool.FindStringSubmatch(call.LastUser)
	}
	if sleep != nil {
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
	} else if read := fakeRead.FindStringSubmatch(call.LastUser); read != nil && !toolResults["call_read"] {
		arguments, _ := json.Marshal(map[string]string{"path": read[1]})
		chunks = []map[string]any{
			{"choices": []any{map[string]any{"index": 0, "delta": map[string]any{"role": "assistant", "tool_calls": []any{map[string]any{"index": 0, "id": "call_read", "type": "function", "function": map[string]any{"name": "read", "arguments": string(arguments)}}}}}}},
			{"choices": []any{map[string]any{"index": 0, "delta": map[string]any{}, "finish_reason": "tool_calls"}}},
		}
	} else if search := fakeSearch.FindStringSubmatch(call.LastUser); search != nil && !toolResults["call_search"] {
		input := map[string]string{"pattern": "**/*.txt"}
		if search[1] == "grep" {
			input["pattern"] = "search-needle"
		}
		arguments, _ := json.Marshal(input)
		chunks = []map[string]any{
			{"choices": []any{map[string]any{"index": 0, "delta": map[string]any{"role": "assistant", "tool_calls": []any{map[string]any{"index": 0, "id": "call_search", "type": "function", "function": map[string]any{"name": search[1], "arguments": string(arguments)}}}}}}},
			{"choices": []any{map[string]any{"index": 0, "delta": map[string]any{}, "finish_reason": "tool_calls"}}},
		}
	} else if tool := fakeTool.FindStringSubmatch(call.LastUser); tool != nil && !toolResults["call_other"] {
		chunks = []map[string]any{
			{"choices": []any{map[string]any{"index": 0, "delta": map[string]any{"role": "assistant", "tool_calls": []any{map[string]any{"index": 0, "id": "call_other", "type": "function", "function": map[string]any{"name": tool[1], "arguments": `{"directory":"/nonexistent-worktree"}`}}}}}}},
			{"choices": []any{map[string]any{"index": 0, "delta": map[string]any{}, "finish_reason": "tool_calls"}}},
		}
	} else if shell := fakeShell.FindStringSubmatch(call.LastUser); shell != nil && !toolResults["call_shell"] {
		command, _ := base64.StdEncoding.DecodeString(shell[1])
		input := map[string]any{"command": string(command), "timeout": 20000, "description": "fixture command"}
		if workdir := fakeWorkdir.FindStringSubmatch(call.LastUser); workdir != nil {
			input["workdir"] = workdir[1]
		}
		arguments, _ := json.Marshal(input)
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
