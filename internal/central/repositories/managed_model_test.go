package repositories_test

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
)

// managedModel is an explicit deterministic protocol fixture. It selects real
// confined tools exposed by OpenCode and checks their returned observations.
// It does not contact a model provider or retain request bodies or credentials.
type managedModel struct{ calls atomic.Int32 }

func (s *managedModel) count() int32 { return s.calls.Load() }
func (s *managedModel) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" || r.URL.Path != "/v1/chat/completions" || r.Header.Get("Authorization") != "Bearer synthetic-disposable" {
		http.Error(w, "Fixture route rejected.", 403)
		return
	}
	var request struct {
		Model    string           `json:"model"`
		Messages []map[string]any `json:"messages"`
		Tools    []struct {
			Function struct {
				Name string `json:"name"`
			} `json:"function"`
		} `json:"tools"`
	}
	if json.NewDecoder(io.LimitReader(r.Body, 2<<20)).Decode(&request) != nil || request.Model != "gpt-6-luna" || s.calls.Add(1) > 128 {
		http.Error(w, "Fixture request rejected.", 400)
		return
	}
	available := map[string]bool{}
	for _, tool := range request.Tools {
		available[tool.Function.Name] = true
	}
	observations := map[string]string{}
	for _, message := range request.Messages {
		if message["role"] == "tool" {
			id, _ := message["tool_call_id"].(string)
			observations[id] = fmt.Sprint(message["content"])
		}
	}
	delta := map[string]any{"role": "assistant"}
	finish := "stop"
	tool := func(id, name string, args any) {
		encoded, _ := json.Marshal(args)
		delta["tool_calls"] = []any{map[string]any{"index": 0, "id": id, "type": "function", "function": map[string]any{"name": name, "arguments": string(encoded)}}}
		finish = "tool_calls"
	}
	if !available["patch"] {
		if available["read"] && observations["acceptance_read"] == "" {
			tool("acceptance_read", "read", map[string]string{"path": "/workspace/project/input.txt"})
		} else if available["read"] && !strings.Contains(observations["acceptance_read"], "trusted synthetic fixture input") {
			http.Error(w, "Fixture source read failed.", 422)
			return
		} else {
			delta["content"] = "Create result.txt with exactly managed result followed by a newline. Preserve all imported files and modes. Verify result.txt using a confined shell check. This is planning advice, not completed implementation."
		}
	} else if observations["acceptance_patch"] == "" {
		tool("acceptance_patch", "patch", map[string]string{"patchText": "*** Begin Patch\n*** Add File: result.txt\n+managed result\n*** End Patch"})
	} else if observations["acceptance_shell"] == "" {
		if !available["shell"] {
			http.Error(w, "Fixture shell unavailable.", 422)
			return
		}
		tool("acceptance_shell", "shell", map[string]any{"command": "test \"$(cat result.txt)\" = \"managed result\" && printf verified", "timeout": 20000, "description": "verify managed repository result"})
	} else if !strings.Contains(observations["acceptance_shell"], "verified") {
		http.Error(w, "Fixture result check failed.", 422)
		return
	} else {
		delta["content"] = "Managed repository result written and verified. DONE"
	}
	w.Header().Set("Content-Type", "text/event-stream")
	for _, event := range []map[string]any{{"id": "managed-repository-fixture", "object": "chat.completion.chunk", "model": request.Model, "choices": []any{map[string]any{"index": 0, "delta": delta}}}, {"id": "managed-repository-fixture", "object": "chat.completion.chunk", "choices": []any{map[string]any{"index": 0, "delta": map[string]any{}, "finish_reason": finish}}}} {
		encoded, _ := json.Marshal(event)
		fmt.Fprintf(w, "data: %s\n\n", encoded)
	}
	fmt.Fprint(w, "data: [DONE]\n\n")
}
