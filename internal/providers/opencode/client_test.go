package opencode

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
)

type clientTestTransport func(*http.Request) (*http.Response, error)

func (f clientTestTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func testV2Client(t *testing.T, route func(*http.Request) (int, string)) *Client {
	t.Helper()
	c, err := NewClient("http://127.0.0.1:32123", "disposable-service-password")
	if err != nil {
		t.Fatal(err)
	}
	c.http.Transport = clientTestTransport(func(r *http.Request) (*http.Response, error) {
		user, password, ok := r.BasicAuth()
		if !ok || user != "opencode" || password != "disposable-service-password" {
			t.Fatal("missing exact service authentication")
		}
		if r.Method != http.MethodGet || r.Header.Get("Accept") != "application/json" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL)
		}
		status, body := route(r)
		return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}, nil
	})
	return c
}

func TestV2ClientRejectsNonOwnedEndpoints(t *testing.T) {
	for _, address := range []string{
		"https://127.0.0.1:1234", "http://example.com:1234", "http://127.0.0.1", "http://127.0.0.1:1234/path",
		"http://user:secret@127.0.0.1:1234", "http://127.0.0.1:1234?query=secret", "http://127.0.0.1:1234#fragment",
	} {
		if _, err := NewClient(address, "password"); err == nil {
			t.Fatalf("accepted invalid endpoint %q", address)
		}
	}
	if _, err := NewClient("http://127.0.0.1:1234", ""); err == nil {
		t.Fatal("accepted missing service password")
	}
}

func TestV2ClientHealthRequiresV2Identity(t *testing.T) {
	for _, tc := range []struct {
		body string
		ok   bool
	}{
		{`{"version":"2.0.29","pid":42}`, true},
		{`{"version":"1.18.32","pid":42}`, false},
		{`{"version":"2.0.29","pid":0}`, false},
		{`{"healthy":true}`, false},
		{`{"version":"2.0.29","pid":42} {}`, false},
	} {
		c := testV2Client(t, func(r *http.Request) (int, string) {
			if r.URL.Path != "/api/info" {
				t.Fatalf("health used unsupported path %s", r.URL.Path)
			}
			return 200, tc.body
		})
		if err := c.Health(context.Background()); (err == nil) != tc.ok {
			t.Fatalf("health %s: %v", tc.body, err)
		}
	}
}

func TestV2ClientFormatterUsesLastDefinedDocument(t *testing.T) {
	for _, tc := range []struct {
		body string
		ok   bool
	}{
		{`[{"type":"document","info":{"formatter":{}}},{"type":"document","info":{"formatter":false}},{"type":"directory","path":"/tmp"}]`, true},
		{`[{"type":"document","info":{"formatter":false}},{"type":"document","info":{}}]`, true},
		{`[{"type":"document","info":{"formatter":false}},{"type":"document","info":{"formatter":{}}}]`, false},
		{`[]`, false},
		{`[{"type":"document","info":{"formatter":null}}]`, false},
		{`[{"type":"document"}]`, false},
		{`[{"type":"unknown","info":{"formatter":false}}]`, false},
	} {
		c := testV2Client(t, func(r *http.Request) (int, string) {
			if r.URL.Path != "/api/config" || r.URL.Query().Get("location[directory]") != "/tmp/custom project" {
				t.Fatalf("incorrect location query %s", r.URL)
			}
			return 200, tc.body
		})
		if err := c.ValidateConfiguration(context.Background(), "/tmp/custom project"); (err == nil) != tc.ok {
			t.Fatalf("configuration %s: %v", tc.body, err)
		}
	}
}

func TestV2ClientModelsPreserveConfiguredIDsAndVariants(t *testing.T) {
	var requests []string
	c := testV2Client(t, func(r *http.Request) (int, string) {
		requests = append(requests, r.URL.Path)
		if r.URL.Query().Get("location[directory]") != "/tmp/custom project" {
			t.Fatal("catalog escaped its configured location")
		}
		switch r.URL.Path {
		case "/api/config":
			return 200, `[{"type":"document","info":{"formatter":false}}]`
		case "/api/provider":
			return 200, `{"location":{"directory":"/tmp/custom project"},"data":[{"id":"my-private-server","name":"My Local Models","activation":"enabled"},{"id":"disabled","name":"Disabled","activation":"disabled"}]}`
		case "/api/model":
			return 200, `{"location":{"directory":"/tmp/custom project"},"data":[{"id":"org/model:custom","providerID":"my-private-server","name":"Custom Model","enabled":true,"variants":[{"id":"careful","body":{"reasoning":{"effort":"high"}}}]},{"id":"disabled-model","providerID":"my-private-server","name":"Disabled Model","enabled":false},{"id":"hidden","providerID":"disabled","name":"Hidden","enabled":true}]}`
		default:
			t.Fatalf("unexpected endpoint %s", r.URL.Path)
			return 500, ""
		}
	})
	models, err := c.Models(context.Background(), "/tmp/custom project")
	if err != nil || len(models) != 1 {
		t.Fatalf("models=%#v error=%v", models, err)
	}
	model := models[0]
	if model.ID != "org/model:custom" || model.ProviderID != "my-private-server" || model.ProviderName != "My Local Models" || len(model.Variants) != 1 || model.Variants[0].ID != "careful" {
		t.Fatalf("lost custom configuration: %#v", model)
	}
	if strings.Join(requests, ",") != "/api/config,/api/model,/api/provider" {
		t.Fatalf("catalog must await model/plugin readiness before provider snapshot: %v", requests)
	}
}

func TestV2ClientModelsRejectIncompleteProtocol(t *testing.T) {
	for _, body := range []string{
		`{}`, `[]`, `{"location":{"directory":"/wrong"},"data":[]}`,
		`{"location":{"directory":"/tmp"},"data":null}`,
		`{"location":{"directory":"/tmp"},"data":[{"id":"model","name":"Model","providerID":"custom"}]}`,
		`{"location":{"directory":"/tmp"},"data":[{"id":"model","name":"Model","providerID":"custom","enabled":true,"variants":[{"id":"same"},{"id":"same"}]}]}`,
		`{"location":{"directory":"/tmp"},"data":[{"id":"model","name":"Model","providerID":"custom","enabled":true},{"id":"model","name":"Model","providerID":"custom","enabled":true}]}`,
	} {
		c := testV2Client(t, func(r *http.Request) (int, string) {
			switch r.URL.Path {
			case "/api/config":
				return 200, `[{"type":"document","info":{"formatter":false}}]`
			case "/api/provider":
				return 200, `{"location":{"directory":"/tmp"},"data":[{"id":"custom","name":"Custom","activation":"enabled"}]}`
			default:
				return 200, body
			}
		})
		_, err := c.Models(context.Background(), "/tmp")
		var protocol *ProtocolError
		if !errors.As(err, &protocol) {
			t.Fatalf("accepted incomplete protocol %s: %v", body, err)
		}
	}
}

func TestV2ClientErrorsHideProviderResponseBodies(t *testing.T) {
	c := testV2Client(t, func(*http.Request) (int, string) { return 503, `{"secret":"sensitive-provider-token"}` })
	err := c.Health(context.Background())
	var service *ServiceError
	if !errors.As(err, &service) || service.StatusCode != 503 || strings.Contains(err.Error(), "sensitive-provider-token") {
		t.Fatalf("unsafe status error %v", err)
	}
	if c.http.CheckRedirect == nil || c.http.CheckRedirect(nil, nil) != http.ErrUseLastResponse {
		t.Fatal("service credentials must not follow redirects")
	}
}

func TestV2ClientCancellationAndEmptyCatalog(t *testing.T) {
	c := testV2Client(t, func(r *http.Request) (int, string) {
		if r.URL.Path == "/api/config" {
			return 200, `[{"type":"document","info":{"formatter":false}}]`
		}
		body, err := json.Marshal(map[string]any{"location": map[string]string{"directory": "/tmp"}, "data": []any{}})
		if err != nil {
			t.Fatal(err)
		}
		return 200, string(body)
	})
	models, err := c.Models(context.Background(), "/tmp")
	if err != nil || models == nil || len(models) != 0 {
		t.Fatalf("empty successful catalog: %#v %v", models, err)
	}
	c.http.Transport = clientTestTransport(func(r *http.Request) (*http.Response, error) {
		<-r.Context().Done()
		return nil, r.Context().Err()
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := c.Health(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("lost cancellation: %v", err)
	}
}
