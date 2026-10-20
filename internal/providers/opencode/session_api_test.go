package opencode

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func sessionTestClient(t *testing.T, handler http.HandlerFunc) *Client {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if user, password, ok := r.BasicAuth(); !ok || user != "opencode" || password != "disposable" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		handler(w, r)
	}))
	t.Cleanup(server.Close)
	client, err := NewClient(server.URL, "disposable")
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func TestSessionCreationSendsExactIdentityAndRejectsDifferentSession(t *testing.T) {
	var body map[string]any
	client := sessionTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/api/session" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL)
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		agent := body["agent"].(string)
		if agent == "wrong" {
			agent = "other"
		}
		fmt.Fprintf(w, `{"data":{"id":%q,"agent":%q,"location":{"directory":"/workspace"}}}`, body["id"], agent)
	})
	spec := SessionSpec{ID: "ses_1", Title: "Run", Agent: "profile", Model: ModelRef{ProviderID: "local", ID: "model/with/slash", Variant: "high"}}
	if err := client.CreateSession(context.Background(), "/workspace", spec); err != nil {
		t.Fatal(err)
	}
	model := body["model"].(map[string]any)
	if body["id"] != "ses_1" || body["agent"] != "profile" || model["providerID"] != "local" || model["id"] != "model/with/slash" || model["variant"] != "high" {
		t.Fatalf("unexpected session payload %v", body)
	}
	if permissions, ok := body["permissions"].([]any); !ok || len(permissions) != 0 {
		t.Fatalf("session must not add permission overlays: %v", body["permissions"])
	}
	spec.Agent = "wrong"
	if err := client.CreateSession(context.Background(), "/workspace", spec); err == nil {
		t.Fatal("accepted a session with another agent")
	}
	if err := client.CreateSession(context.Background(), "relative", spec); err == nil {
		t.Fatal("accepted a relative location")
	}
}

func TestPromptRequiresExactAdmission(t *testing.T) {
	for _, tc := range []struct {
		name, response string
		ok             bool
	}{
		{"admitted", `{"data":{"id":"msg_1","type":"user"}}`, true},
		{"other message", `{"data":{"id":"msg_2","type":"user"}}`, false},
		{"other kind", `{"data":{"id":"msg_1","type":"synthetic"}}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := sessionTestClient(t, func(w http.ResponseWriter, r *http.Request) {
				var body map[string]string
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Fatal(err)
				}
				if r.URL.Path != "/api/session/ses_1/prompt" || r.URL.Query().Get("location[directory]") != "/workspace" || body["id"] != "msg_1" || body["text"] != "go" || body["delivery"] != "steer" {
					t.Fatalf("unexpected prompt %s %v", r.URL, body)
				}
				io.WriteString(w, tc.response)
			})
			err := client.Prompt(context.Background(), "/workspace", "ses_1", "msg_1", "go")
			if (err == nil) != tc.ok {
				t.Fatalf("error=%v", err)
			}
		})
	}
}

func TestInterruptParksSessionQueue(t *testing.T) {
	client := sessionTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/session/ses_1/interrupt" || r.URL.Query().Get("resume") != "false" || r.URL.Query().Get("location[directory]") != "/workspace" {
			t.Fatalf("unexpected interrupt %s", r.URL)
		}
		io.WriteString(w, `{"interrupted":true}`)
	})
	if err := client.Interrupt(context.Background(), "/workspace", "ses_1"); err != nil {
		t.Fatal(err)
	}
}

func TestWaitIsNotBoundByTheShortRequestTimeout(t *testing.T) {
	client := sessionTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(60 * time.Millisecond)
		w.WriteHeader(http.StatusNoContent)
	})
	client.http.Timeout = 10 * time.Millisecond
	if err := client.Wait(context.Background(), "/workspace", "ses_1"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := client.Wait(ctx, "/workspace", "ses_1"); err == nil {
		t.Fatal("wait ignored its context")
	}
}

func TestMessagesPageNewestFirstAndFollowCursor(t *testing.T) {
	var queries []string
	client := sessionTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		queries = append(queries, r.URL.RawQuery)
		// OpenCode v2 cursors carry ordering. Its public API rejects a cursor
		// combined with an explicit order, even when that order is unchanged.
		if r.URL.Query().Get("cursor") != "" && r.URL.Query().Has("order") {
			http.Error(w, "cursor cannot be combined with order", http.StatusBadRequest)
			return
		}
		if r.URL.Query().Get("cursor") == "" {
			io.WriteString(w, `{"data":[{"id":"c"},{"id":"b"}],"cursor":{"next":"page-2"}}`)
			return
		}
		io.WriteString(w, `{"data":[{"id":"a"}],"cursor":{}}`)
	})
	visit := func(ids *[]string, stopAt string) func(json.RawMessage) error {
		return func(raw json.RawMessage) error {
			var message struct{ ID string }
			if err := json.Unmarshal(raw, &message); err != nil {
				return err
			}
			*ids = append(*ids, message.ID)
			if message.ID == stopAt {
				return errStopVisit
			}
			return nil
		}
	}
	var ids []string
	if err := client.Messages(context.Background(), "/workspace", "ses_1", visit(&ids, "")); err != nil || strings.Join(ids, ",") != "c,b,a" {
		t.Fatalf("ids=%v error=%v", ids, err)
	}
	if len(queries) != 2 || !strings.Contains(queries[0], "order=desc") || strings.Contains(queries[1], "order=") || !strings.Contains(queries[1], "cursor=page-2") {
		t.Fatalf("unexpected paging %v", queries)
	}
	ids, queries = nil, nil
	if err := client.Messages(context.Background(), "/workspace", "ses_1", visit(&ids, "b")); err != nil || strings.Join(ids, ",") != "c,b" || len(queries) != 1 {
		t.Fatalf("early stop ids=%v queries=%v error=%v", ids, queries, err)
	}
}

func TestPluginCallWrapsInputAndDecodesOutput(t *testing.T) {
	client := sessionTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Input map[string]string `json:"input"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if r.URL.Path != "/api/rpc/kingdom/register" || r.URL.Query().Get("location[directory]") != "/workspace" || body.Input["profileID"] != "kd_1" {
			t.Fatalf("unexpected call %s %v", r.URL, body)
		}
		io.WriteString(w, `{"output":{"generation":"g1"}}`)
	})
	var output struct{ Generation string }
	if err := client.CallPlugin(context.Background(), "/workspace", "kingdom", "register", map[string]string{"profileID": "kd_1"}, &output); err != nil || output.Generation != "g1" {
		t.Fatalf("output=%+v error=%v", output, err)
	}
}

func TestSessionLogReturnsWatermarkAndRejectsTrailingEvents(t *testing.T) {
	for _, tc := range []struct {
		name, stream string
		watermark    int64
		ok           bool
	}{
		{"synced", "data: {\"id\":\"e1\",\"durable\":{\"seq\":0}}\n\ndata: {\"id\":\"e2\",\"durable\":{\"seq\":3}}\n\ndata: {\"type\":\"log.synced\",\"aggregateID\":\"ses_1\",\"seq\":3}\n\n", 3, true},
		{"event after marker", "data: {\"type\":\"log.synced\",\"seq\":1}\n\ndata: {\"id\":\"late\"}\n\n", 0, false},
		{"missing marker", "data: {\"id\":\"e1\"}\n\n", 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := sessionTestClient(t, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Query().Get("follow") != "false" {
					t.Fatalf("log must be finite: %s", r.URL)
				}
				io.WriteString(w, tc.stream)
			})
			var seen int
			watermark, err := client.Log(context.Background(), "/workspace", "ses_1", func(json.RawMessage) error { seen++; return nil })
			if (err == nil) != tc.ok || (tc.ok && (watermark != tc.watermark || seen != 2)) {
				t.Fatalf("watermark=%d seen=%d error=%v", watermark, seen, err)
			}
		})
	}
}

func TestFollowEventsRequiresConnectionEventAndReportsEveryEnd(t *testing.T) {
	client := sessionTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "data: {\"type\":\"server.connected\"}\n\ndata: {\"id\":\"e1\"}\r\n\r\ndata: {\"id\":\ndata: \"e2\"}\n\n")
	})
	var connected int
	var ids []string
	err := client.FollowEvents(context.Background(), func() { connected++ }, func(raw json.RawMessage) error {
		ids = append(ids, strings.ReplaceAll(string(raw), "\n", ""))
		return nil
	})
	if err == nil {
		t.Fatal("a clean end of the stream must not look like continuous capture")
	}
	if connected != 1 || len(ids) != 2 || ids[1] != `{"id":"e2"}` {
		t.Fatalf("connected=%d events=%v", connected, ids)
	}
	bad := sessionTestClient(t, func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "data: {\"id\":\"e1\"}\n\n") })
	if err := bad.FollowEvents(context.Background(), func() {}, func(json.RawMessage) error { return nil }); err == nil || !errors.As(err, new(*ProtocolError)) {
		t.Fatalf("stream without a connection event was accepted: %v", err)
	}
}

func TestEventStreamRejectsOversizedEvents(t *testing.T) {
	err := readEventStream(strings.NewReader("data: "+strings.Repeat("x", maxEventLine+1)+"\n\n"), func([]byte) error { return nil })
	if err == nil {
		t.Fatal("accepted an unbounded event")
	}
}
