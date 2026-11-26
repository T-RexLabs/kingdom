package modelproxy

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"kingdom/internal/core/providerexecution"
)

func TestProxyPinsNativeProtocolAndPreservesMessagesStream(t *testing.T) {
	const stream = "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"tool_use\",\"id\":\"tool_1\",\"name\":\"read\",\"input\":{}}}\n\nevent: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"path\\\":\\\"input.txt\\\"}\"}}\n\n"
	for _, protocol := range []string{"anthropic", "openai_responses", "openai_chat"} {
		t.Run(protocol, func(t *testing.T) {
			var calls atomic.Int64
			wantPath := "/v1/messages"
			wrongPaths := []string{"/v1/chat/completions", "/v1/responses"}
			body := `{"model":"model","messages":[{"role":"user","content":"Read input"}],"max_tokens":32,"stream":true,"tools":[{"name":"read","input_schema":{"type":"object"}}]}`
			if protocol == "openai_responses" {
				wantPath = "/v1/responses"
				wrongPaths = []string{"/v1/chat/completions", "/v1/messages"}
				body = `{"model":"model","input":[{"role":"user","content":"Read input"}],"store":false,"stream":true,"tools":[{"type":"function","name":"read","parameters":{"type":"object"}}]}`
			}
			if protocol == "openai_chat" {
				wantPath = "/v1/chat/completions"
				wrongPaths = []string{"/v1/responses", "/v1/messages"}
				body = `{ "model":"model", "messages":[{"role":"user","content":"Read input"}], "stream":true }`
			}
			broker, first, server, authority, resolver := proxyFixture(t, func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				received, _ := io.ReadAll(r.Body)
				if r.URL.Path != wantPath || string(received) != body {
					t.Error("native request changed")
				}
				if r.Header.Get("Authorization") != "Bearer private-upstream-secret" || r.Header.Get("X-Api-Key") != "" || r.Header.Get("Cookie") != "" || r.Header.Get("Anthropic-Beta") != "" {
					t.Error("untrusted credentials or headers reached upstream")
				}
				if protocol == "anthropic" && r.Header.Get("Anthropic-Version") != "2023-06-01" {
					t.Error("native Messages version missing")
				}
				if protocol != "anthropic" && r.Header.Get("Anthropic-Version") != "" {
					t.Error("Messages headers crossed protocol")
				}
				w.Header().Set("Content-Type", "text/event-stream")
				io.WriteString(w, stream)
			}, true)
			pins, _, err := broker.cfg.Registry.Lookup(context.Background(), first.ID)
			if err != nil {
				t.Fatal(err)
			}
			pins.Binding.Preparation.Owner.InvocationID = "native-protocol-invocation"
			resolver.route.Protocol = protocol
			lease, err := broker.Issue(context.Background(), pins.Binding)
			if err != nil {
				t.Fatal(err)
			}
			if lease.Protocol != protocol {
				t.Fatal("lease protocol not retained")
			}
			identity := providerexecution.RequestIdentity{InstanceID: "instance", Generation: "generation", Location: "/sandbox", SessionID: "native-session", ProfileID: "native-profile", RequestID: "native-request"}
			if err := broker.Activate(context.Background(), lease.ID, identity); err != nil {
				t.Fatal(err)
			}
			authority.active.Store(true)
			send := func(path string) *http.Response {
				request, _ := http.NewRequest(http.MethodPost, server.URL+path, strings.NewReader(body))
				request.Header.Set("Authorization", "Bearer "+lease.Token())
				request.Header.Set("Content-Type", "application/json")
				request.Header.Set("Anthropic-Version", "untrusted")
				request.Header.Set("Anthropic-Beta", "untrusted")
				request.Header.Set("X-Api-Key", "untrusted")
				response, err := server.Client().Do(request)
				if err != nil {
					t.Fatal(err)
				}
				return response
			}
			for _, path := range wrongPaths {
				response := send(path)
				response.Body.Close()
				if response.StatusCode != http.StatusBadRequest {
					t.Fatalf("cross-protocol request returned %d", response.StatusCode)
				}
			}
			if calls.Load() != 0 {
				t.Fatal("cross-protocol request reached upstream")
			}
			response := send(wantPath)
			got, err := io.ReadAll(response.Body)
			response.Body.Close()
			if err != nil || response.StatusCode != http.StatusOK || string(got) != stream {
				t.Fatalf("native stream changed: status=%d error=%v", response.StatusCode, err)
			}
			if calls.Load() != 1 {
				t.Fatal("inference repeated")
			}
		})
	}
}

func TestCopilotCredentialOwnsHeadersWithoutChangingAPIKeyRequests(t *testing.T) {
	for _, protocol := range []string{"openai", "openai_responses", "anthropic"} {
		t.Run(protocol, func(t *testing.T) {
			headers := make(chan http.Header, 1)
			broker, first, server, authority, resolver := proxyFixture(t, func(w http.ResponseWriter, r *http.Request) {
				headers <- r.Header.Clone()
				w.Header().Set("Content-Type", "application/json")
				io.WriteString(w, `{"ok":true}`)
			}, true)
			pins, _, err := broker.cfg.Registry.Lookup(context.Background(), first.ID)
			if err != nil {
				t.Fatal(err)
			}
			pins.Binding.Preparation.Owner.InvocationID = "copilot-header-invocation"
			resolver.route.Protocol = protocol
			resolver.copilot = true
			lease, err := broker.Issue(context.Background(), pins.Binding)
			if err != nil {
				t.Fatal(err)
			}
			identity := providerexecution.RequestIdentity{InstanceID: "instance", Generation: "generation", Location: "/sandbox", SessionID: "copilot-session", ProfileID: "copilot-profile", RequestID: "copilot-request"}
			if err := broker.Activate(context.Background(), lease.ID, identity); err != nil {
				t.Fatal(err)
			}
			authority.active.Store(true)
			path := "/v1/responses"
			body := `{"model":"model","input":[{"role":"user","content":[{"type":"input_image","image_url":"https://example.test/image.png"}]}]}`
			if protocol == "anthropic" {
				path = "/v1/messages"
				body = `{"model":"model","messages":[{"role":"user","content":[{"type":"image","source":{"type":"url","url":"https://example.test/image.png"}}]}]}`
			}
			request, _ := http.NewRequest(http.MethodPost, server.URL+path, strings.NewReader(body))
			request.Header.Set("Authorization", "Bearer "+lease.Token())
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("X-Initiator", "user")
			request.Header.Set("X-Interaction-Id", "foreign-session")
			request.Header.Set("User-Agent", "untrusted")
			request.Header.Set("Editor-Version", "vscode/untrusted")
			request.Header.Set("Copilot-Integration-Id", "vscode-chat")
			response, err := server.Client().Do(request)
			if err != nil {
				t.Fatal(err)
			}
			response.Body.Close()
			if response.StatusCode != 200 {
				t.Fatal(response.StatusCode)
			}
			got := <-headers
			if got.Get("Authorization") != "Bearer private-upstream-secret" || got.Get("X-Initiator") != "agent" || got.Get("X-Interaction-Id") != identity.SessionID || got.Get("X-Interaction-Type") != "conversation-agent" || got.Get("Openai-Intent") != "conversation-edits" || got.Get("X-GitHub-Api-Version") != "2026-08-01" || got.Get("User-Agent") != "Kingdom" || got.Get("Copilot-Vision-Request") != "true" {
				t.Fatal("Copilot header policy missing or changed")
			}
			if got.Get("Editor-Version") != "" || got.Get("Copilot-Integration-Id") != "" {
				t.Fatal("editor identity copied")
			}
			if protocol == "anthropic" && got.Get("Anthropic-Beta") != "interleaved-thinking-2025-05-14" {
				t.Fatal("Copilot Messages profile missing")
			}
			if protocol != "anthropic" && got.Get("Anthropic-Beta") != "" {
				t.Fatal("Messages beta crossed protocol")
			}
		})
	}
}
