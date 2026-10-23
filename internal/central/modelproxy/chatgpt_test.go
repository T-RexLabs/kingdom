package modelproxy

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
)

func TestChatGPTPreviewPreservesContextAndConfinesProtocol(t *testing.T) {
	obj, e := uniqueObject([]byte(`{"model":"model","input":[{"role":"system","content":"rules"},{"role":"user","content":"question"},{"type":"function_call_output","call_id":"call","output":"result"}],"tools":[{"type":"function","name":"read","parameters":{"type":"object"}}],"store":true,"stream":false,"temperature":0.2,"max_output_tokens":123}`))
	if e != nil {
		t.Fatal(e)
	}
	body, e := chatGPTRequest("/v1/responses", obj)
	if e != nil {
		t.Fatal(e)
	}
	var got map[string]any
	json.Unmarshal(body, &got)
	if got["store"] != false || got["stream"] != true || got["temperature"] != nil || got["max_output_tokens"] != nil {
		t.Fatal("preview requirements not applied")
	}
	input := got["input"].([]any)
	if len(input) != 3 || input[0].(map[string]any)["role"] != "developer" || input[2].(map[string]any)["output"] != "result" {
		t.Fatal("context changed")
	}
	namespace := got["tools"].([]any)[0].(map[string]any)
	if namespace["type"] != "namespace" || namespace["tools"].([]any)[0].(map[string]any)["name"] != "read" {
		t.Fatal("function identity changed")
	}
}
func TestChatGPTRejectsContinuationAndUnsupportedTools(t *testing.T) {
	for _, raw := range []string{`{"input":[],"previous_response_id":"other"}`, `{"input":"short"}`, `{"input":[],"tools":[{"type":"file_search"}]}`, `{"input":[],"tools":[{"type":"programmatic_tool_calling"}]}`, `{"input":[{"type":"function_call","namespace":"foreign","name":"read"}]}`} {
		obj, _ := uniqueObject([]byte(raw))
		if _, e := chatGPTRequest("/v1/responses", obj); e == nil {
			t.Fatalf("accepted %s", raw)
		}
	}
	if _, e := chatGPTRequest("/v1/chat/completions", map[string]any{"input": []any{}}); e == nil {
		t.Fatal("chat completions accepted")
	}
}

func TestLeaseProtocolComesFromPinnedRoute(t *testing.T) {
	broker, first, _, _, resolver := proxyFixture(t, func(http.ResponseWriter, *http.Request) {}, true)
	if first.Protocol != "openai" {
		t.Fatalf("API key protocol = %q", first.Protocol)
	}
	pins, _, err := broker.cfg.Registry.Lookup(context.Background(), first.ID)
	if err != nil {
		t.Fatal(err)
	}
	pins.Binding.Preparation.Owner.InvocationID = "chatgpt-invocation"
	resolver.route = Route{Protocol: "openai_chatgpt", Endpoint: "https://api.openai.com/v1", Model: "model"}
	broker.cfg.Origins = append(broker.cfg.Origins, OriginPolicy{Origin: "https://api.openai.com"})
	lease, err := broker.Issue(context.Background(), pins.Binding)
	if err != nil {
		t.Fatal(err)
	}
	if lease.Protocol != "openai_chatgpt" {
		t.Fatalf("ChatGPT protocol = %q", lease.Protocol)
	}
	resolver.route.Endpoint = "https://other.example/v1"
	if _, err := broker.Issue(context.Background(), pins.Binding); err == nil {
		t.Fatal("ChatGPT route accepted an endpoint override")
	}
}

func TestChatGPTToolReplayRetainsNamespace(t *testing.T) {
	object, err := uniqueObject([]byte(`{"model":"model","input":[{"type":"function_call","call_id":"call","name":"read","arguments":"{}"},{"type":"function_call_output","call_id":"call","output":"result"}],"tools":[{"type":"function","name":"read","parameters":{"type":"object"}}]}`))
	if err != nil {
		t.Fatal(err)
	}
	_, err = chatGPTRequest("/v1/responses", object)
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range object["input"].([]any) {
		if item.(map[string]any)["namespace"] != "kingdom" {
			t.Fatal("tool replay lost its namespace")
		}
	}
}
