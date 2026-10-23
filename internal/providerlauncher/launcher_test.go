package providerlauncher

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func job() Job {
	return Job{Version: JobVersion, ModelAPI: ChatCompletionsAPI, LauncherSHA256: strings.Repeat("a", 64), OpenCodeSHA256: strings.Repeat("b", 64), ControlToken: strings.Repeat("c", 64), ServerPassword: strings.Repeat("d", 64), ProxyURL: "https://worker.example/v1", ProxyToken: base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{1}, 32)), ProviderID: "openai", ModelID: "fixture", CallbackURL: "https://worker.example/callback", CallbackToken: base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{2}, 32)), InstanceID: "main:0", ContextLimit: 32000, OutputLimit: 4096, LifetimeSeconds: 300}
}
func TestStrictJobAndNoInheritedCredentials(t *testing.T) {
	j := job()
	raw, _ := json.Marshal(j)
	if _, e := DecodeJob(bytes.NewReader(raw)); e != nil {
		t.Fatal(e)
	}
	for _, bad := range []string{string(raw) + " {}", strings.Replace(string(raw), `"version":2`, `"version":2,"api_key":"real"`, 1), strings.Repeat("x", MaxJobBytes+1)} {
		if _, e := DecodeJob(strings.NewReader(bad)); e == nil {
			t.Fatal("invalid job accepted")
		}
	}
	j.ProxyURL = "http://worker.example"
	if j.Validate() == nil {
		t.Fatal("plaintext proxy accepted")
	}
	j = job()
	j.ProxyToken = "sk-real-secret"
	if j.Validate() == nil {
		t.Fatal("upstream-shaped key accepted")
	}
	j = job()
	t.Setenv("OPENAI_API_KEY", "host-secret")
	env := strings.Join(environment(j), "\n")
	if strings.Contains(env, "host-secret") || strings.Contains(env, "OPENAI_API_KEY") || !strings.Contains(env, "OPENCODE_CONFIG_PROJECT_DISABLE=1") || !strings.Contains(env, "KINGDOM_NATIVE_MODE=managed") {
		t.Fatal("unsafe environment")
	}
	spec, e := Command(j)
	if e != nil || spec.Argv[0] != LauncherPath || spec.Workdir != WorkspacePath || len(spec.Environment) != 0 {
		t.Fatal(spec, e)
	}
}
func TestFakeChild(t *testing.T) {
	if os.Getenv("KINGDOM_LAUNCHER_FIXTURE") == "" {
		return
	}
	_, _ = io.Copy(io.Discard, os.Stdin)
	if os.Getenv("KINGDOM_LAUNCHER_FIXTURE") == "fail" {
		os.Exit(3)
	}
	time.Sleep(100 * time.Millisecond)
	os.Exit(0)
}

type harness struct {
	url    string
	done   chan error
	cancel context.CancelFunc
	out    *bytes.Buffer
	child  *exec.Cmd
}

func start(t *testing.T, mode string, ready bool) *harness {
	t.Helper()
	listener, e := net.Listen("tcp4", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithCancel(context.Background())
	child := exec.Command(os.Args[0], "-test.run=^TestFakeChild$")
	child.Env = []string{"KINGDOM_LAUNCHER_FIXTURE=" + mode}
	h := &harness{url: "http://" + listener.Addr().String(), done: make(chan error, 1), cancel: cancel, out: new(bytes.Buffer), child: child}
	go func() {
		h.done <- serve(ctx, job(), child, listener, h.out, func(context.Context, Job, int) bool { return ready })
	}()
	t.Cleanup(cancel)
	for i := 0; i < 100; i++ {
		r, e := http.Get(h.url)
		if e == nil {
			r.Body.Close()
			return h
		}
		time.Sleep(time.Millisecond * 5)
	}
	t.Fatal("listener not ready")
	return nil
}
func (h *harness) call(method, path, token string) (int, error) {
	r, e := http.NewRequest(method, h.url+path, nil)
	if e != nil {
		return 0, e
	}
	r.Host = "managed.invalid"
	r.Header.Set("Authorization", "Bearer "+token)
	c := http.Client{Timeout: time.Second * 3}
	response, e := c.Do(r)
	if e != nil {
		return 0, e
	}
	response.Body.Close()
	return response.StatusCode, nil
}
func expect(t *testing.T, h *harness, method, path, token string, want int) {
	t.Helper()
	code, e := h.call(method, path, token)
	if e != nil || code != want {
		t.Fatal(code, e)
	}
}
func TestExplicitStopOwnsStdinAndCleanExit(t *testing.T) {
	h := start(t, "wait", true)
	expect(t, h, "GET", "/v1/ready", job().ControlToken, 204)
	expect(t, h, "POST", "/v1/stop", "wrong", 401)
	select {
	case <-h.done:
		t.Fatal("wrong token stopped child")
	default:
	}
	expect(t, h, "POST", "/v1/stop", job().ControlToken, 204)
	if e := <-h.done; e != nil {
		t.Fatal(e)
	}
	if h.out.String() != "{\"version\":1,\"stopped\":true}\n" {
		t.Fatal(h.out.String())
	}
	if h.child.SysProcAttr != nil {
		t.Fatal("separate process group configured")
	}
}
func TestFailedChildCannotNormalizeToSuccess(t *testing.T) {
	h := start(t, "fail", true)
	expect(t, h, "POST", "/v1/stop", job().ControlToken, 500)
	if e := <-h.done; e == nil {
		t.Fatal("failure returned clean")
	}
	if h.out.Len() != 0 {
		t.Fatal("clean fact emitted")
	}
}
func TestCancellationAndNotReadyRemainUnresolved(t *testing.T) {
	h := start(t, "wait", false)
	expect(t, h, "GET", "/v1/ready", job().ControlToken, 503)
	h.cancel()
	if e := <-h.done; e == nil {
		t.Fatal("cancel normalized")
	}
	if h.out.Len() != 0 {
		t.Fatal("clean fact emitted")
	}
}
func TestDuplicateStopRejected(t *testing.T) {
	h := start(t, "wait", true)
	first := make(chan error, 1)
	go func() {
		code, e := h.call("POST", "/v1/stop", job().ControlToken)
		if e == nil && code != 204 {
			e = io.ErrUnexpectedEOF
		}
		first <- e
	}()
	for i := 0; i < 100; i++ {
		code, e := h.call("GET", "/v1/ready", job().ControlToken)
		if e != nil {
			t.Fatal(e)
		}
		if code == 409 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	expect(t, h, "POST", "/v1/stop", job().ControlToken, 409)
	if e := <-first; e != nil {
		t.Fatal(e)
	}
	if e := <-h.done; e != nil {
		t.Fatal(e)
	}
}

func TestJobRejectsDuplicateFields(t *testing.T) {
	raw, _ := json.Marshal(job())
	duplicate := strings.Replace(string(raw), `"version":2`, `"version":1,"version":1`, 1)
	if _, e := DecodeJob(strings.NewReader(duplicate)); e == nil {
		t.Fatal("duplicate field accepted")
	}
}
func TestDeadlineDoesNotProduceCleanStop(t *testing.T) {
	listener, e := net.Listen("tcp4", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	cmd := exec.Command(os.Args[0], "-test.run=^TestFakeChild$")
	cmd.Env = []string{"KINGDOM_LAUNCHER_FIXTURE=wait"}
	var out bytes.Buffer
	e = serve(ctx, job(), cmd, listener, &out, func(context.Context, Job, int) bool { return false })
	if e == nil || out.Len() != 0 {
		t.Fatal("deadline normalized", e, out.String())
	}
}
func TestUnexpectedCleanChildExitIsFailure(t *testing.T) {
	listener, e := net.Listen("tcp4", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	cmd := exec.Command("/bin/sh", "-c", "exit 0")
	var out bytes.Buffer
	e = serve(ctx, job(), cmd, listener, &out, func(context.Context, Job, int) bool { return false })
	if e == nil || out.Len() != 0 {
		t.Fatal("natural exit normalized", e)
	}
}
func TestExecutableDigestRejectsChangedFile(t *testing.T) {
	path := t.TempDir() + "/executable"
	if e := os.WriteFile(path, []byte("trusted"), 0600); e != nil {
		t.Fatal(e)
	}
	sum := sha256.Sum256([]byte("trusted"))
	if e := verifyFile(path, hex.EncodeToString(sum[:])); e != nil {
		t.Fatal(e)
	}
	if e := os.WriteFile(path, []byte("changed"), 0600); e != nil {
		t.Fatal(e)
	}
	if e := verifyFile(path, hex.EncodeToString(sum[:])); e == nil {
		t.Fatal("changed executable accepted")
	}
}

func TestSlowConnectionsAreBoundedBeforeHeaders(t *testing.T) {
	raw, e := net.Listen("tcp4", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	defer raw.Close()
	listener := &boundedListener{Listener: raw, slots: make(chan struct{}, 1)}
	accepted := make(chan net.Conn, 2)
	go func() {
		for i := 0; i < 2; i++ {
			c, e := listener.Accept()
			if e != nil {
				return
			}
			accepted <- c
		}
	}()
	first, e := net.Dial("tcp4", raw.Addr().String())
	if e != nil {
		t.Fatal(e)
	}
	defer first.Close()
	held := <-accepted
	defer held.Close()
	extra, e := net.Dial("tcp4", raw.Addr().String())
	if e != nil {
		t.Fatal(e)
	}
	defer extra.Close()
	_ = extra.SetReadDeadline(time.Now().Add(time.Second))
	var b [1]byte
	if _, e = extra.Read(b[:]); e != io.EOF {
		t.Fatal("excess connection retained", e)
	}
	_ = held.Close()
	next, e := net.Dial("tcp4", raw.Addr().String())
	if e != nil {
		t.Fatal(e)
	}
	defer next.Close()
	released := <-accepted
	released.Close()
}

type failingCloseListener struct{ net.Listener }

func (l failingCloseListener) Close() error {
	_ = l.Listener.Close()
	return errors.New("fixture listener shutdown failure")
}
func TestControlShutdownFailureCannotEmitCleanCompletion(t *testing.T) {
	raw, e := net.Listen("tcp4", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cmd := exec.Command(os.Args[0], "-test.run=^TestFakeChild$")
	cmd.Env = []string{"KINGDOM_LAUNCHER_FIXTURE=wait"}
	h := &harness{url: "http://" + raw.Addr().String(), done: make(chan error, 1), out: new(bytes.Buffer)}
	go func() {
		h.done <- serve(ctx, job(), cmd, failingCloseListener{raw}, h.out, func(context.Context, Job, int) bool { return true })
	}()
	expect(t, h, "POST", "/v1/stop", job().ControlToken, 204)
	if e := <-h.done; e == nil {
		t.Fatal("control shutdown normalized")
	}
	if h.out.Len() != 0 {
		t.Fatal("clean completion emitted")
	}
}

func TestResponsesJobSelectsNativeResponsesProvider(t *testing.T) {
	j := job()
	j.ModelAPI = "responses"
	if err := j.Validate(); err != nil {
		t.Fatal(err)
	}
	var config map[string]any
	for _, entry := range environment(j) {
		if strings.HasPrefix(entry, "OPENCODE_CONFIG_CONTENT=") {
			if err := json.Unmarshal([]byte(strings.TrimPrefix(entry, "OPENCODE_CONFIG_CONTENT=")), &config); err != nil {
				t.Fatal(err)
			}
		}
	}
	provider := config["providers"].(map[string]any)[j.ProviderID].(map[string]any)
	if provider["package"] != "@opencode/ai/providers/openai/responses" {
		t.Fatalf("Responses connection uses %v", provider["package"])
	}
	j.ModelAPI = "arbitrary-provider"
	if j.Validate() == nil {
		t.Fatal("untrusted provider API accepted")
	}
}

func TestMessagesJobSelectsNativeAnthropicProvider(t *testing.T) {
	j := job()
	j.ModelAPI = "messages"
	if err := j.Validate(); err != nil {
		t.Fatal(err)
	}
	var config map[string]any
	for _, entry := range environment(j) {
		if strings.HasPrefix(entry, "OPENCODE_CONFIG_CONTENT=") {
			if err := json.Unmarshal([]byte(strings.TrimPrefix(entry, "OPENCODE_CONFIG_CONTENT=")), &config); err != nil {
				t.Fatal(err)
			}
		}
	}
	provider := config["providers"].(map[string]any)[j.ProviderID].(map[string]any)
	if provider["package"] != "@opencode/ai/providers/anthropic" {
		t.Fatalf("Messages connection uses %v", provider["package"])
	}
	settings := provider["settings"].(map[string]any)
	if settings["authToken"] != j.ProxyToken || settings["apiKey"] != nil || settings["baseURL"] != j.ProxyURL {
		t.Fatal("native provider changed scoped credentials")
	}
}
