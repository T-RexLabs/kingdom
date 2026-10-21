package providerlauncher

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

func verifyFile(path, expected string) error {
	f, e := os.Open(path)
	if e != nil {
		return e
	}
	defer f.Close()
	info, e := f.Stat()
	if e != nil || !info.Mode().IsRegular() {
		return errors.New("invalid provider executable")
	}
	h := sha256.New()
	if _, e = io.Copy(h, f); e != nil {
		return e
	}
	if hex.EncodeToString(h.Sum(nil)) != expected {
		return errors.New("provider executable digest mismatch")
	}
	return nil
}
func environment(j Job) []string {
	config := map[string]any{"formatter": false, "plugins": []string{PluginPath}, "providers": map[string]any{j.ProviderID: map[string]any{"name": j.ProviderID, "package": "@opencode/ai/providers/openai-compatible", "settings": map[string]any{"baseURL": j.ProxyURL, "apiKey": j.ProxyToken, "encoding": "none"}, "models": map[string]any{j.ModelID: map[string]any{"name": j.ModelID, "capabilities": map[string]any{"tools": true, "input": []string{"text"}, "output": []string{"text"}}, "limit": map[string]int{"context": j.ContextLimit, "output": j.OutputLimit}}}}}}
	raw, _ := json.Marshal(config)
	return []string{"PATH=/usr/local/bin:/usr/bin:/bin", "LANG=C.UTF-8", "HOME=" + RuntimePath + "/home", "TMPDIR=" + RuntimePath + "/tmp", "XDG_CONFIG_HOME=" + RuntimePath + "/config", "XDG_CACHE_HOME=" + RuntimePath + "/cache", "XDG_DATA_HOME=" + RuntimePath + "/data", "XDG_STATE_HOME=" + RuntimePath + "/state", "OPENCODE_CONFIG_DIR=" + RuntimePath + "/config", "OPENCODE_DB=" + RuntimePath + "/opencode.db", "OPENCODE_CONFIG_PROJECT_DISABLE=1", "OPENCODE_DISABLE_CLAUDE_CODE=1", "OPENCODE_DISABLE_CLAUDE_CODE_SKILLS=1", "OPENCODE_DISABLE_MODELS_FETCH=1", "OPENCODE_DISABLE_FFF=1", "OPENCODE_CONFIG_CONTENT=" + string(raw), "OPENCODE_PASSWORD=" + j.ServerPassword, "OPENCODE_SERVER_PASSWORD=" + j.ServerPassword, "KINGDOM_AUTHORITY_URL=" + j.CallbackURL, "KINGDOM_AUTHORITY_TOKEN=" + j.CallbackToken, "KINGDOM_INSTANCE_ID=" + j.InstanceID, "KINGDOM_OUTBOX_DIR=" + RuntimePath + "/outbox"}
}

// Run never sends process signals or creates a process group. The trusted
// managed boundary owns group cancellation and deadlines. Any non-clean stop
// returns an error; pipe closure alone does not establish workflow success.
func Run(ctx context.Context, j Job, stdout io.Writer) error {
	if e := j.Validate(); e != nil {
		return e
	}
	if e := verifyFile("/proc/self/exe", j.LauncherSHA256); e != nil {
		return e
	}
	if e := verifyFile(OpenCodePath, j.OpenCodeSHA256); e != nil {
		return e
	}
	// A fresh runtime directory prevents inherited account/configuration state.
	if e := os.Mkdir(RuntimePath, 0700); e != nil {
		return e
	}
	for _, name := range []string{"home", "tmp", "config", "cache", "data", "state", "outbox"} {
		if e := os.Mkdir(filepath.Join(RuntimePath, name), 0700); e != nil {
			return e
		}
	}
	cmd := exec.Command(OpenCodePath, "serve", "--stdio", "--hostname", "127.0.0.1", "--port", "4096")
	cmd.Dir = WorkspacePath
	cmd.Env = environment(j)
	listener, e := net.Listen("tcp4", "127.0.0.1:4098")
	if e != nil {
		return e
	}
	return serve(ctx, j, cmd, listener, stdout, providerReady)
}
func providerReady(ctx context.Context, j Job, pid int) bool {
	transport := &http.Transport{Proxy: nil, DisableKeepAlives: true}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	request, e := http.NewRequestWithContext(ctx, "GET", "http://127.0.0.1:4096/api/info", nil)
	if e != nil {
		return false
	}
	request.SetBasicAuth("opencode", j.ServerPassword)
	response, e := client.Do(request)
	if e != nil {
		return false
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		return false
	}
	var info struct {
		Version string `json:"version"`
		PID     int    `json:"pid"`
	}
	e = json.NewDecoder(io.LimitReader(response.Body, 4097)).Decode(&info)
	return e == nil && info.Version == OpenCodeVersion && info.PID == pid
}
func serve(ctx context.Context, j Job, cmd *exec.Cmd, listener net.Listener, stdout io.Writer, ready func(context.Context, Job, int) bool) error {
	listener = &boundedListener{Listener: listener, slots: make(chan struct{}, 4)}
	defer listener.Close()
	stdin, e := cmd.StdinPipe()
	if e != nil {
		return e
	}
	defer stdin.Close()
	// Child logs can contain scoped capabilities. Only fixed launcher facts enter
	// managed stdout. Kernel closure and exit status remain in the typed receipt.
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	if e = cmd.Start(); e != nil {
		return e
	}
	exited := make(chan struct{})
	var childErr error
	go func() { childErr = cmd.Wait(); close(exited) }()
	var stopping atomic.Bool
	finished := make(chan error, 1)
	slots := make(chan struct{}, 4)
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case slots <- struct{}{}:
			defer func() { <-slots }()
		default:
			http.Error(w, "busy", 429)
			return
		}
		if r.Host != "managed.invalid" || r.URL.RawQuery != "" || r.URL.RawPath != "" || r.URL.IsAbs() || len(r.Header.Values("Authorization")) != 1 || subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+j.ControlToken)) != 1 {
			http.Error(w, "unauthorized", 401)
			return
		}
		w.Header().Set("Cache-Control", "no-store")
		if r.Method == "GET" && r.URL.Path == "/v1/ready" {
			if stopping.Load() {
				http.Error(w, "stopping", 409)
				return
			}
			select {
			case <-exited:
				http.Error(w, "provider exited", 503)
				return
			default:
			}
			if !ready(r.Context(), j, cmd.Process.Pid) {
				http.Error(w, "not ready", 503)
				return
			}
			w.WriteHeader(204)
			return
		}
		if r.Method != "POST" || r.URL.Path != "/v1/stop" || r.ContentLength != 0 {
			http.Error(w, "not found", 404)
			return
		}
		select {
		case <-exited:
			http.Error(w, "provider already exited", 409)
			return
		default:
		}
		if !stopping.CompareAndSwap(false, true) {
			http.Error(w, "stop already requested", 409)
			return
		}
		if e := stdin.Close(); e != nil {
			finished <- errors.New("provider lease close failed")
			http.Error(w, "stop failed", 500)
			return
		}
		select {
		case <-exited:
			if childErr != nil {
				http.Error(w, "provider failed", 500)
				finished <- errors.New("provider child failed")
				return
			}
			w.WriteHeader(204)
			// Response delivery is a transport fact. The worker still needs the
			// exact managed receipt; losing this response cannot undo child exit.
			_ = http.NewResponseController(w).Flush()
			finished <- nil
		case <-ctx.Done():
			http.Error(w, "stop unresolved", 503)
		}
	})
	server := &http.Server{Handler: handler, ReadHeaderTimeout: 2 * time.Second, ReadTimeout: 2 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 2 * time.Second, MaxHeaderBytes: 8192}
	defer server.Close()
	serverErr := make(chan error, 1)
	go func() { serverErr <- server.Serve(listener) }()
	finish := func(err error) error {
		if err != nil {
			return err
		}
		drain, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		if err := server.Shutdown(drain); err != nil {
			return errors.New("provider control shutdown unresolved")
		}
		select {
		case err := <-serverErr:
			if !errors.Is(err, http.ErrServerClosed) {
				return errors.New("provider control shutdown failed")
			}
		case <-drain.Done():
			return errors.New("provider control shutdown unresolved")
		}
		if ctx.Err() != nil {
			return errors.New("provider ownership ended during stop")
		}
		_, err = fmt.Fprintln(stdout, `{"version":1,"stopped":true}`)
		return err
	}
	for {
		select {
		case e := <-finished:
			return finish(e)
		case <-exited:
			if !stopping.Load() {
				return errors.New("provider exited without explicit stop")
			}
			select {
			case e := <-finished:
				return finish(e)
			case <-ctx.Done():
				return errors.New("provider stop unresolved")
			}
		case <-ctx.Done():
			return errors.New("provider ownership ended without clean stop")
		case <-serverErr:
			return errors.New("provider control server stopped")
		}
	}
}

// Bound accepted connections before HTTP header parsing, including slow peers.
type boundedListener struct {
	net.Listener
	slots chan struct{}
}

func (l *boundedListener) Accept() (net.Conn, error) {
	for {
		conn, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}
		select {
		case l.slots <- struct{}{}:
			return &boundedConn{Conn: conn, release: func() { <-l.slots }}, nil
		default:
			_ = conn.Close()
		}
	}
}

type boundedConn struct {
	net.Conn
	once    sync.Once
	release func()
}

func (c *boundedConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(c.release)
	return err
}
