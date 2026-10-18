package opencode

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"kingdom/internal/processguardian"
)

type serviceRunnerFunc func(context.Context, processguardian.Command, processguardian.PersistOwnership) (processguardian.Handle, error)

func (f serviceRunnerFunc) Start(ctx context.Context, command processguardian.Command, persist processguardian.PersistOwnership) (processguardian.Handle, error) {
	return f(ctx, command, persist)
}

type serviceTestHandle struct {
	stdout *io.PipeReader
	writer *io.PipeWriter
	input  serviceTestInput
	done   chan struct{}
	once   sync.Once
	result processguardian.Result
}

type serviceTestInput struct{ closed atomic.Bool }

func (s *serviceTestInput) Write(p []byte) (int, error) { return len(p), nil }
func (s *serviceTestInput) Close() error                { s.closed.Store(true); return nil }

func newServiceTestHandle(endpoint string) *serviceTestHandle {
	reader, writer := io.Pipe()
	handle := &serviceTestHandle{stdout: reader, writer: writer, done: make(chan struct{}), result: processguardian.Result{Quiescent: true}}
	go func() {
		_, _ = fmt.Fprintln(writer, endpoint)
	}()
	return handle
}

func (h *serviceTestHandle) GuardID() string       { return "service-test-guard" }
func (h *serviceTestHandle) Stdin() io.WriteCloser { return &h.input }
func (h *serviceTestHandle) Stdout() io.ReadCloser { return h.stdout }
func (h *serviceTestHandle) Stderr() io.ReadCloser { return io.NopCloser(strings.NewReader("")) }
func (h *serviceTestHandle) Cancel(context.Context) error {
	h.once.Do(func() { _ = h.input.Close(); _ = h.writer.Close(); close(h.done) })
	return nil
}
func (h *serviceTestHandle) Wait(ctx context.Context) (processguardian.Result, error) {
	select {
	case <-ctx.Done():
		return processguardian.Result{}, ctx.Err()
	case <-h.done:
		return h.result, nil
	}
}

func serviceFixture(t *testing.T, handler http.HandlerFunc) (*Service, string) {
	t.Helper()
	if handler == nil {
		handler = func(w http.ResponseWriter, r *http.Request) {
			user, password, ok := r.BasicAuth()
			if !ok || user != "opencode" || len(password) != 64 {
				http.Error(w, "authentication required", http.StatusUnauthorized)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			switch r.URL.Path {
			case "/api/info":
				_, _ = io.WriteString(w, `{"version":"2.0.29","pid":123,"urls":[],"paths":{"tmp":"/tmp"}}`)
			case "/api/config":
				_, _ = io.WriteString(w, `[{"type":"document","info":{"formatter":false}}]`)
			default:
				http.NotFound(w, r)
			}
		}
	}
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	service, err := NewService(ServiceConfig{BinaryPath: "fixture-opencode", WorkDir: t.TempDir(), RuntimeRoot: t.TempDir(), Env: []string{"PATH=" + os.Getenv("PATH")}})
	if err != nil {
		t.Fatal(err)
	}
	service.startLimit = time.Second
	service.stopLimit = 5 * time.Second
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := service.CloseContext(ctx); err != nil {
			t.Error(err)
		}
	})
	return service, `{"url":"` + server.URL + `"}`
}

func TestServiceStartsLazilyAndCollapsesConcurrentClients(t *testing.T) {
	service, endpoint := serviceFixture(t, nil)
	var starts atomic.Int32
	var handle *serviceTestHandle
	service.runner = serviceRunnerFunc(func(_ context.Context, command processguardian.Command, persist processguardian.PersistOwnership) (processguardian.Handle, error) {
		starts.Add(1)
		if persist != nil {
			t.Error("service launch must not claim invocation ownership")
		}
		if !reflect.DeepEqual(command.Args, []string{"serve", "--stdio", "--hostname", "127.0.0.1", "--port", "0"}) {
			t.Errorf("unexpected command args: %v", command.Args)
		}
		handle = newServiceTestHandle(endpoint)
		return handle, nil
	})
	if starts.Load() != 0 {
		t.Fatal("construction launched a server")
	}
	const callers = 24
	clients := make(chan *Client, callers)
	errs := make(chan error, callers)
	var wait sync.WaitGroup
	for range callers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			client, err := service.Client(context.Background())
			clients <- client
			errs <- err
		}()
	}
	wait.Wait()
	close(clients)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	var first *Client
	for client := range clients {
		if first == nil {
			first = client
		}
		if client != first {
			t.Fatal("concurrent callers received different clients")
		}
	}
	if starts.Load() != 1 || handle.input.closed.Load() {
		t.Fatalf("starts=%d stdin closed=%v", starts.Load(), handle.input.closed.Load())
	}
	if err := service.CloseContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !handle.input.closed.Load() {
		t.Fatal("server input lease remained open")
	}
	if _, err := service.Client(context.Background()); err == nil {
		t.Fatal("closed service resurrected")
	}
	if err := service.CloseContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	if starts.Load() != 1 {
		t.Fatalf("closed service started again: %d", starts.Load())
	}
}

func TestServiceCallerCancellationDoesNotCancelSharedStartup(t *testing.T) {
	service, endpoint := serviceFixture(t, nil)
	started, release := make(chan struct{}), make(chan struct{})
	service.runner = serviceRunnerFunc(func(ctx context.Context, _ processguardian.Command, _ processguardian.PersistOwnership) (processguardian.Handle, error) {
		close(started)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-release:
			return newServiceTestHandle(endpoint), nil
		}
	})
	ctx, cancel := context.WithCancel(context.Background())
	first := make(chan error, 1)
	go func() { _, err := service.Client(ctx); first <- err }()
	<-started
	cancel()
	if err := <-first; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	close(release)
	if _, err := service.Client(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestServiceCloseDuringStartupPreventsResurrection(t *testing.T) {
	service, _ := serviceFixture(t, nil)
	started := make(chan struct{})
	var starts atomic.Int32
	service.runner = serviceRunnerFunc(func(ctx context.Context, _ processguardian.Command, _ processguardian.PersistOwnership) (processguardian.Handle, error) {
		starts.Add(1)
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	})
	done := make(chan error, 1)
	go func() { _, err := service.Client(context.Background()); done <- err }()
	<-started
	if err := service.ForceClose(); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err == nil {
		t.Fatal("startup succeeded after close")
	}
	if _, err := service.Client(context.Background()); err == nil {
		t.Fatal("closed service resurrected")
	}
	if starts.Load() != 1 {
		t.Fatal(starts.Load())
	}
}

func TestServiceFailedStartCanRetryAfterQuiescence(t *testing.T) {
	service, endpoint := serviceFixture(t, nil)
	var starts atomic.Int32
	service.runner = serviceRunnerFunc(func(context.Context, processguardian.Command, processguardian.PersistOwnership) (processguardian.Handle, error) {
		if starts.Add(1) == 1 {
			return nil, errors.New("fixture launch failure")
		}
		return newServiceTestHandle(endpoint), nil
	})
	if _, err := service.Client(context.Background()); err == nil {
		t.Fatal("failed launch succeeded")
	}
	if _, err := service.Client(context.Background()); err != nil {
		t.Fatal(err)
	}
	if starts.Load() != 2 {
		t.Fatal(starts.Load())
	}
}

func TestServiceRejectsUnhealthyAndConflictingConfiguration(t *testing.T) {
	for _, mode := range []string{"health", "formatter"} {
		t.Run(mode, func(t *testing.T) {
			service, endpoint := serviceFixture(t, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/api/info" {
					if mode == "health" {
						http.Error(w, "unavailable", 503)
						return
					}
					_, _ = io.WriteString(w, `{"version":"2.0.29","pid":123,"urls":[],"paths":{"tmp":"/tmp"}}`)
					return
				}
				_, _ = io.WriteString(w, `[{"type":"document","info":{"formatter":false}},{"type":"document","info":{"formatter":{"prettier":{}}}}]`)
			})
			var handle *serviceTestHandle
			service.runner = serviceRunnerFunc(func(context.Context, processguardian.Command, processguardian.PersistOwnership) (processguardian.Handle, error) {
				handle = newServiceTestHandle(endpoint)
				return handle, nil
			})
			if _, err := service.Client(context.Background()); err == nil {
				t.Fatal("invalid readiness succeeded")
			}
			select {
			case <-handle.done:
			default:
				t.Fatal("failed server not stopped")
			}
		})
	}
}

func TestServiceStartupTimeoutStopsGuardedProcess(t *testing.T) {
	service, _ := serviceFixture(t, nil)
	service.startLimit = 20 * time.Millisecond
	var handle *serviceTestHandle
	service.runner = serviceRunnerFunc(func(context.Context, processguardian.Command, processguardian.PersistOwnership) (processguardian.Handle, error) {
		handle = newServiceTestHandle(`{"url":"http://0.0.0.0:8080"}`)
		return handle, nil
	})
	if _, err := service.Client(context.Background()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	select {
	case <-handle.done:
	default:
		t.Fatal("timed out process not cancelled")
	}
}

func TestServiceEnvironmentPreservesProviderSetupAndDisablesFormatter(t *testing.T) {
	inherited := []string{"PATH=/fixture/bin", "OPENCODE_DATABASE_PATH=/existing/opencode.db", "OPENCODE_CONFIG=/existing/user.json", "OPENAI_API_KEY=fixture-key", `OPENCODE_CONFIG_CONTENT={"provider":{"custom":{"options":{"baseURL":"http://127.0.0.1:1234/v1"}}},"formatter":{"prettier":{}}}`}
	env, err := serviceEnvironment(inherited, "")
	if err != nil {
		t.Fatal(err)
	}
	for _, expected := range inherited[:4] {
		if !strings.Contains(strings.Join(env, "\n"), expected) {
			t.Fatalf("lost inherited entry %s", strings.SplitN(expected, "=", 2)[0])
		}
	}
	var config map[string]json.RawMessage
	for _, entry := range env {
		if value, ok := strings.CutPrefix(entry, "OPENCODE_CONFIG_CONTENT="); ok {
			if err := json.Unmarshal([]byte(value), &config); err != nil {
				t.Fatal(err)
			}
		}
	}
	if string(config["formatter"]) != "false" || !strings.Contains(string(config["provider"]), "http://127.0.0.1:1234/v1") {
		t.Fatalf("wrong configuration %v", config)
	}
	for _, invalid := range []string{"null", "[]", "{bad", "{} trailing"} {
		if _, err := serviceEnvironment([]string{"OPENCODE_CONFIG_CONTENT=" + invalid}, ""); err == nil {
			t.Fatalf("accepted malformed inline configuration %q", invalid)
		}
	}
}

func TestServiceEndpointRejectsUnexpectedAddresses(t *testing.T) {
	for _, line := range []string{`{"url":"http://127.0.0.1:49152"}`, "startup noise"} {
		_, ok := serviceEndpoint(line)
		if ok != strings.Contains(line, "49152") {
			t.Fatalf("unexpected readiness result for %q", line)
		}
	}
	for _, address := range []string{"http://localhost:1234", "http://0.0.0.0:1234", "https://127.0.0.1:1234", "http://127.0.0.1:0", "http://127.0.0.1:1234/path", "http://user:secret@127.0.0.1:1234", "http://127.0.0.1:1234?secret=value"} {
		if _, ok := serviceEndpoint(`{"url":"` + address + `"}`); ok {
			t.Fatalf("accepted unsafe readiness address %s", address)
		}
	}
}

func TestServiceUsesActualGuardianLease(t *testing.T) {
	service, endpoint := serviceFixture(t, nil)
	path := filepath.Join(t.TempDir(), "opencode")
	script := "#!/bin/sh\nprintf '%s\\n' '" + endpoint + "'\ncat >/dev/null\n"
	if err := os.WriteFile(path, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	service.binaryPath = path
	if _, err := service.Client(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := service.CloseContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Client(context.Background()); err == nil {
		t.Fatal("guardian-backed service restarted after close")
	}
}

func TestServiceUnprovenLaunchBlocksReplacementAndReportsShutdownFailure(t *testing.T) {
	service, err := NewService(ServiceConfig{WorkDir: t.TempDir(), RuntimeRoot: t.TempDir(), Env: []string{}})
	if err != nil {
		t.Fatal(err)
	}
	defer service.cancel()
	var starts atomic.Int32
	service.runner = serviceRunnerFunc(func(context.Context, processguardian.Command, processguardian.PersistOwnership) (processguardian.Handle, error) {
		starts.Add(1)
		return nil, &processguardian.LaunchError{Cause: errors.New("fixture launch uncertainty"), Result: processguardian.Result{Quiescent: false}}
	})
	for range 2 {
		if _, err := service.Client(context.Background()); err == nil {
			t.Fatal("unproven launch became ready")
		}
	}
	if starts.Load() != 1 {
		t.Fatalf("unproven owner was replaced: %d starts", starts.Load())
	}
	if err := service.CloseContext(context.Background()); err == nil {
		t.Fatal("uncertain shutdown reported success")
	}
}

func TestServiceEnvironmentOverridesBothPasswordAliasesWithoutArguments(t *testing.T) {
	service, endpoint := serviceFixture(t, nil)
	service.env = append(service.env, "OPENCODE_PASSWORD=old-password", "OPENCODE_SERVER_PASSWORD=old-server-password")
	service.runner = serviceRunnerFunc(func(_ context.Context, command processguardian.Command, _ processguardian.PersistOwnership) (processguardian.Handle, error) {
		values := make(map[string]string)
		for _, entry := range command.Env {
			name, value, _ := strings.Cut(entry, "=")
			values[name] = value
		}
		password := values["OPENCODE_PASSWORD"]
		if len(password) != 64 || values["OPENCODE_SERVER_PASSWORD"] != password {
			t.Fatal("server password aliases do not select the same random credential")
		}
		if strings.Contains(strings.Join(command.Args, " "), password) {
			t.Fatal("server credential leaked in arguments")
		}
		return newServiceTestHandle(endpoint), nil
	})
	if _, err := service.Client(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestServiceCanonicalizesWorkingDirectorySymlinks(t *testing.T) {
	root := t.TempDir()
	actual := filepath.Join(root, "actual")
	if err := os.Mkdir(actual, 0o700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "linked")
	if err := os.Symlink(actual, link); err != nil {
		t.Fatal(err)
	}
	service, err := NewService(ServiceConfig{WorkDir: link, RuntimeRoot: t.TempDir(), Env: []string{}})
	if err != nil {
		t.Fatal(err)
	}
	defer service.cancel()
	expected, err := filepath.EvalSymlinks(actual)
	if err != nil {
		t.Fatal(err)
	}
	if service.workDir != expected {
		t.Fatalf("working directory=%q want %q", service.workDir, expected)
	}
}

func TestServiceModelsRechecksChangedFormattingConfiguration(t *testing.T) {
	var conflicting atomic.Bool
	service, endpoint := serviceFixture(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/info":
			_, _ = io.WriteString(w, `{"version":"2.0.29","pid":123}`)
		case "/api/config":
			formatter := "false"
			if conflicting.Load() {
				formatter = `{"custom":{}}`
			}
			_, _ = fmt.Fprintf(w, `[{"type":"document","info":{"formatter":%s}}]`, formatter)
		case "/api/model", "/api/provider":
			_ = json.NewEncoder(w).Encode(map[string]any{"location": map[string]string{"directory": r.URL.Query().Get("location[directory]")}, "data": []any{}})
		default:
			http.NotFound(w, r)
		}
	})
	var starts atomic.Int32
	service.runner = serviceRunnerFunc(func(context.Context, processguardian.Command, processguardian.PersistOwnership) (processguardian.Handle, error) {
		starts.Add(1)
		return newServiceTestHandle(endpoint), nil
	})
	if _, err := service.Models(context.Background()); err != nil {
		t.Fatal(err)
	}
	conflicting.Store(true)
	var configurationError *ConfigurationError
	if _, err := service.Models(context.Background()); !errors.As(err, &configurationError) {
		t.Fatalf("changed formatter was not rejected: %v", err)
	}
	if starts.Load() != 1 {
		t.Fatalf("configuration read restarted shared service: %d", starts.Load())
	}
}

func TestServiceEnvironmentAcceptsOpenCodeJSONC(t *testing.T) {
	inline := `{
		// OpenCode accepts comments in inline configuration.
		"providers": {
			"private-models": {
				"settings": {
					"baseURL": "http://127.0.0.1:1234/v1", // Preserve provider URL.
					"headers": {"X-Custom": "value",},
					"features": ["one", "two",],
				},
			},
		},
		/* Managed formatting overrides this value only. */
		"formatter": {"custom": {"command": ["formatter", "--write",],},},
	}`
	env, err := serviceEnvironment([]string{"OPENCODE_CONFIG_CONTENT=" + inline}, "")
	if err != nil {
		t.Fatalf("valid OpenCode JSONC rejected: %v", err)
	}
	var config struct {
		Providers map[string]struct {
			Settings struct {
				BaseURL  string            `json:"baseURL"`
				Headers  map[string]string `json:"headers"`
				Features []string          `json:"features"`
			} `json:"settings"`
		} `json:"providers"`
		Formatter json.RawMessage `json:"formatter"`
	}
	for _, entry := range env {
		if value, ok := strings.CutPrefix(entry, "OPENCODE_CONFIG_CONTENT="); ok {
			if err := json.Unmarshal([]byte(value), &config); err != nil {
				t.Fatal(err)
			}
		}
	}
	settings := config.Providers["private-models"].Settings
	if settings.BaseURL != "http://127.0.0.1:1234/v1" || settings.Headers["X-Custom"] != "value" || !reflect.DeepEqual(settings.Features, []string{"one", "two"}) {
		t.Fatalf("inline provider settings changed: %+v", settings)
	}
	if string(config.Formatter) != "false" {
		t.Fatalf("formatter not disabled: %s", config.Formatter)
	}
}
