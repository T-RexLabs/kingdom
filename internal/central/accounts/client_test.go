package accounts

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"kingdom/internal/processguardian"
)

func TestMain(m *testing.M) {
	if code, handled := processguardian.DispatchHiddenMode(os.Args); handled {
		os.Exit(code)
	}
	os.Exit(m.Run())
}

func fixtureClient(t *testing.T, handler http.HandlerFunc) *Client {
	t.Helper()
	s := httptest.NewServer(handler)
	t.Cleanup(s.Close)
	c, e := NewClient(s.URL, "private-test-password", t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(c.CloseIdleConnections)
	return c
}
func TestClientRejectsNonPrivateAddresses(t *testing.T) {
	for _, s := range []string{"https://127.0.0.1:5", "http://localhost:5", "http://example.com:5", "http://127.0.0.1:5/path", "http://127.0.0.1:5?secret=x", "http://u:p@127.0.0.1:5"} {
		if _, e := NewClient(s, "x", t.TempDir()); e == nil {
			t.Fatalf("accepted %s", s)
		}
	}
}
func TestAccountRequestsAreAuthenticatedAndLocationBound(t *testing.T) {
	var c *Client
	c = fixtureClient(t, func(w http.ResponseWriter, r *http.Request) {
		u, p, ok := r.BasicAuth()
		if !ok || u != "opencode" || p != "private-test-password" {
			t.Error("missing authentication")
		}
		if r.URL.Query().Get("location[directory]") != c.directory {
			t.Error("different location")
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"location": map[string]string{"directory": c.directory}, "data": map[string]any{"id": CopilotProvider, "connections": []Connection{{Type: "credential", ID: "credential-1", Method: "oauth", Label: "private-label"}}}})
	})
	got, e := c.Connections(context.Background(), CopilotProvider)
	if e != nil || len(got) != 1 || got[0].ID != "credential-1" {
		t.Fatalf("connections: %v %v", got, e)
	}
}
func TestClientDoesNotFollowRedirectsOrExposeProviderErrors(t *testing.T) {
	c := fixtureClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", "https://example.com/secret")
		w.WriteHeader(302)
		_, _ = fmt.Fprint(w, "secret-token")
	})
	_, e := c.Connections(context.Background(), CopilotProvider)
	if e == nil || strings.Contains(e.Error(), "secret") {
		t.Fatalf("unsafe error %v", e)
	}
}
func TestClientRejectsWrongLocationAndUnboundedResponses(t *testing.T) {
	for _, body := range []string{`{"location":{"directory":"/foreign"},"data":{"id":"github-copilot","connections":[]}}`, strings.Repeat("x", (1<<20)+1), `{} {}`} {
		t.Run(fmt.Sprint(len(body)), func(t *testing.T) {
			c := fixtureClient(t, func(w http.ResponseWriter, r *http.Request) { _, _ = fmt.Fprint(w, body) })
			if _, e := c.Connections(context.Background(), CopilotProvider); e == nil {
				t.Fatal("accepted invalid response")
			}
		})
	}
}
func TestResolveUsesExactNativeCredentialAndRedactsFormatting(t *testing.T) {
	c := fixtureClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/rpc/kingdom.accounts/resolve" {
			t.Fatal("wrong RPC route")
		}
		var in struct {
			Input map[string]string `json:"input"`
		}
		_ = json.NewDecoder(r.Body).Decode(&in)
		if in.Input["connectionID"] != "native-exact" || in.Input["provider"] != CopilotProvider {
			t.Fatal("wrong reference")
		}
		_, _ = fmt.Fprint(w, `{"output":{"type":"oauth","methodID":"device","access":"access-secret","refresh":"refresh-secret","expires":0}}`)
	})
	secret, e := c.Resolve(context.Background(), CopilotProvider, "native-exact")
	if e != nil {
		t.Fatal(e)
	}
	for _, format := range []string{"%v", "%+v", "%#v", "%s"} {
		if got := fmt.Sprintf(format, secret); strings.Contains(got, "access-secret") || strings.Contains(got, "refresh-secret") {
			t.Fatal("credential formatting exposed a secret")
		}
	}
}
func TestLegacyChatGPTCannotUseNativeAccountProcess(t *testing.T) {
	c := fixtureClient(t, func(w http.ResponseWriter, r *http.Request) { t.Fatal("unsupported provider reached process") })
	if _, e := c.Begin(context.Background(), "openai", "chatgpt-headless", "x", nil); e == nil {
		t.Fatal("accepted legacy provider")
	}
}
func TestEnvironmentDoesNotInheritHostCredentials(t *testing.T) {
	t.Setenv("OPENAI_API_KEY", "host-secret")
	t.Setenv("OPENCODE_CONFIG_CONTENT", `{"plugins":["host-plugin"]}`)
	env := accountEnvironment("/private/attempt", "server-secret")
	joined := strings.Join(env, "\n")
	if strings.Contains(joined, "host-secret") || strings.Contains(joined, "host-plugin") {
		t.Fatal("inherited host configuration")
	}
	for _, key := range []string{"HOME=/private/attempt/home", "XDG_CONFIG_HOME=/private/attempt/config", "XDG_DATA_HOME=/private/attempt/data", "XDG_STATE_HOME=/private/attempt/state", "OPENCODE_DISABLE_PROJECT_CONFIG=1"} {
		if !strings.Contains(joined, key) {
			t.Fatalf("missing %s", key)
		}
	}
}
func TestProcessRejectsExistingRuntimeAndRemovesFreshUnusedStore(t *testing.T) {
	parent := t.TempDir()
	binary, _ := os.Executable()
	if _, e := NewProcess(ProcessConfig{BinaryPath: binary, RuntimeRoot: parent}); e == nil {
		t.Fatal("reused private store")
	}
	root := filepath.Join(parent, "attempt")
	p, e := NewProcess(ProcessConfig{BinaryPath: binary, RuntimeRoot: root})
	if e != nil {
		t.Fatal(e)
	}
	for _, name := range []string{"home", "config", "data", "state", "work", "tmp"} {
		s, e := os.Stat(filepath.Join(root, name))
		if e != nil || s.Mode().Perm() != 0o700 {
			t.Fatalf("unsafe private directory %s", name)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if e = p.Close(ctx); e != nil {
		t.Fatal(e)
	}
	if _, e = os.Stat(root); !os.IsNotExist(e) {
		t.Fatal("native store remains")
	}
	if _, e = p.Client(ctx); e == nil {
		t.Fatal("closed process restarted")
	}
}
