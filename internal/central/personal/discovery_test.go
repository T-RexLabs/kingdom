package personal

import (
	"bytes"
	"context"
	"database/sql"
	"io"
	"net/http"
	"strings"
	"testing"

	"kingdom/internal/central/accounts"
	"kingdom/internal/central/connections"
	_ "modernc.org/sqlite"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestCopilotCatalogUsesAdvertisedProtocolsAndExactAccount(t *testing.T) {
	ctx := context.Background()
	db, _ := sql.Open("sqlite", ":memory:")
	defer db.Close()
	calls := 0
	body := `{"data":[
 {"id":"response-model","name":"Responses Model","model_picker_enabled":true,"supported_endpoints":["/responses"],"capabilities":{"limits":{"max_output_tokens":100,"max_prompt_tokens":1000},"supports":{"tool_calls":true,"reasoning_effort":["medium"]}}},
 {"id":"message-model","name":"Messages Model","model_picker_enabled":true,"supported_endpoints":["/v1/messages","/responses"],"capabilities":{"limits":{"max_output_tokens":100,"max_prompt_tokens":1000},"supports":{"tool_calls":true}}},
 {"id":"chat-model","name":"Chat Model","model_picker_enabled":true,"supported_endpoints":["/chat/completions"],"capabilities":{"limits":{"max_output_tokens":100,"max_prompt_tokens":1000},"supports":{"tool_calls":true}}},
 {"id":"unknown-api","name":"Unknown Model","model_picker_enabled":true,"supported_endpoints":["/unknown"],"capabilities":{"limits":{"max_output_tokens":100,"max_prompt_tokens":1000},"supports":{"tool_calls":true}}},
 {"id":"disabled","name":"Disabled Model","model_picker_enabled":false,"supported_endpoints":["/responses"],"capabilities":{"limits":{"max_output_tokens":100,"max_prompt_tokens":1000},"supports":{"tool_calls":true}}}
 ]}`
	s, err := New(ctx, Config{DB: db, Key: bytes.Repeat([]byte{3}, 32), Client: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		calls++
		if r.URL.String() != "https://api.individual.githubcopilot.com/models" || r.Header.Get("Authorization") != "Bearer personal-token" || r.Header.Get("X-GitHub-Api-Version") == "" {
			t.Fatal("unexpected discovery request")
		}
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(body))}, nil
	})}})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close(ctx)
	ref, err := s.SaveCopilot(ctx, "owner", accounts.OAuthCredential{Type: "oauth", MethodID: "device", Access: "personal-token", Refresh: "personal-token", Metadata: map[string]string{"apiEndpoint": "https://api.individual.githubcopilot.com"}})
	if err != nil {
		t.Fatal(err)
	}
	d := connections.Definition{Provider: "github-copilot", AuthKind: "copilot_oauth"}
	if _, err = s.Discover(ctx, "other", d, ref); err == nil || calls != 0 {
		t.Fatal("foreign account discovery admitted")
	}
	got, err := s.Discover(ctx, "owner", d, ref)
	if err != nil || len(got) != 3 {
		t.Fatal(got, err)
	}
	protocols := map[string]string{}
	for _, m := range got {
		protocols[m.Model.ID] = m.Route.Protocol
		if m.Route.Model != m.Model.ID {
			t.Fatal("model route mismatch")
		}
	}
	if protocols["response-model"] != "openai_responses" || protocols["message-model"] != "anthropic" || protocols["chat-model"] != "openai_chat" {
		t.Fatal(protocols)
	}
	if err = s.fence(ctx, "owner", ref); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Discover(ctx, "owner", d, ref); err == nil || calls != 1 {
		t.Fatal("disconnected account discovered models")
	}
}
