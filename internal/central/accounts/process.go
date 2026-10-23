package accounts

import (
	"bufio"
	"context"
	"crypto/rand"
	"embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"kingdom/internal/processguardian"
)

//go:embed plugin/index.js plugin/package.json
var pluginFiles embed.FS

type ProcessConfig struct {
	BinaryPath string
	// RuntimeRoot is a fresh private directory on protected volatile storage.
	// Native OpenCode can write unencrypted account credentials in this tree.
	// The central deployment must place it on its private tmpfs mount.
	RuntimeRoot        string
	GuardianExecutable string
}

// Process owns one temporary login process. It never inherits host account
// stores, provider environment variables, plugins, or project configuration.
type Process struct {
	mu      sync.Mutex
	config  ProcessConfig
	runner  *processguardian.Runner
	handle  processguardian.Handle
	done    chan struct{}
	result  processguardian.Result
	waitErr error
	client  *Client
	closed  bool
	started bool
}

func NewProcess(cfg ProcessConfig) (*Process, error) {
	if !filepath.IsAbs(cfg.BinaryPath) || !filepath.IsAbs(cfg.RuntimeRoot) {
		return nil, errors.New("absolute account binary and volatile runtime paths are required")
	}
	if err := os.Mkdir(cfg.RuntimeRoot, 0o700); err != nil {
		return nil, fmt.Errorf("create private account runtime: %w", err)
	}
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.RemoveAll(cfg.RuntimeRoot)
		}
	}()
	for _, name := range []string{"home", "config", "data", "state", "cache", "work", "tmp", "plugin", "guardian"} {
		if err := os.Mkdir(filepath.Join(cfg.RuntimeRoot, name), 0o700); err != nil {
			return nil, err
		}
	}
	for _, name := range []string{"index.js", "package.json"} {
		data, err := pluginFiles.ReadFile("plugin/" + name)
		if err != nil {
			return nil, err
		}
		if err = os.WriteFile(filepath.Join(cfg.RuntimeRoot, "plugin", name), data, 0o600); err != nil {
			return nil, err
		}
	}
	runner, err := processguardian.NewRunner(processguardian.Config{Executable: cfg.GuardianExecutable, RuntimeRoot: filepath.Join(cfg.RuntimeRoot, "guardian"), CancelGrace: time.Second, CancelForce: time.Second})
	if err != nil {
		return nil, err
	}
	cleanup = false
	return &Process{config: cfg, runner: runner}, nil
}

func accountEnvironment(root, password string) []string {
	config, _ := json.Marshal(map[string]any{"formatter": false, "plugins": []string{filepath.Join(root, "plugin")}, "permissions": []map[string]string{{"action": "*", "resource": "*", "effect": "deny"}}})
	return []string{
		"PATH=" + filepath.Dir(os.Args[0]) + ":/usr/local/bin:/usr/bin:/bin",
		"HOME=" + filepath.Join(root, "home"), "XDG_CONFIG_HOME=" + filepath.Join(root, "config"), "XDG_DATA_HOME=" + filepath.Join(root, "data"), "XDG_STATE_HOME=" + filepath.Join(root, "state"), "XDG_CACHE_HOME=" + filepath.Join(root, "cache"), "TMPDIR=" + filepath.Join(root, "tmp"),
		"OPENCODE_SERVER_PASSWORD=" + password, "OPENCODE_PASSWORD=" + password, "OPENCODE_CONFIG_CONTENT=" + string(config), "OPENCODE_DISABLE_PROJECT_CONFIG=1", "OPENCODE_CONFIG_PROJECT_DISABLE=1", "OPENCODE_DISABLE_CLAUDE_CODE=1", "OPENCODE_DISABLE_CLAUDE_CODE_SKILLS=1", "OPENCODE_DISABLE_MODELS_FETCH=1",
	}
}

// Client starts at most one server. A failed or closed process cannot restart.
func (p *Process) Client(ctx context.Context) (*Client, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, errors.New("account process is closed")
	}
	if p.client != nil {
		select {
		case <-p.done:
			return nil, errors.New("account process stopped")
		default:
			return p.client, nil
		}
	}
	if p.started {
		return nil, errors.New("account process startup failed")
	}
	p.started = true
	var secret [32]byte
	if _, err := rand.Read(secret[:]); err != nil {
		return nil, err
	}
	password := hex.EncodeToString(secret[:])
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	h, err := p.runner.Start(ctx, processguardian.Command{Path: p.config.BinaryPath, Args: []string{"serve", "--stdio", "--hostname", "127.0.0.1", "--port", "0"}, Dir: filepath.Join(p.config.RuntimeRoot, "work"), Env: accountEnvironment(p.config.RuntimeRoot, password)}, func(ctx context.Context, own processguardian.Ownership) error {
		own.QuiescenceCapability = ""
		raw, err := json.Marshal(own)
		if err != nil {
			return err
		}
		f, err := os.OpenFile(filepath.Join(p.config.RuntimeRoot, "ownership.json"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			return err
		}
		defer f.Close()
		if _, err = f.Write(raw); err != nil {
			return err
		}
		return f.Sync()
	})
	if err != nil {
		var launch *processguardian.LaunchError
		if errors.As(err, &launch) && !launch.Result.Quiescent {
			p.done = make(chan struct{})
			p.result = launch.Result
			p.waitErr = err
			close(p.done)
		}
		return nil, errors.New("account process could not start")
	}
	p.handle = h
	p.done = make(chan struct{})
	address := make(chan string, 1)
	var output sync.WaitGroup
	output.Add(2)
	go func() {
		defer output.Done()
		defer h.Stdout().Close()
		scanner := bufio.NewScanner(h.Stdout())
		scanner.Buffer(make([]byte, 4096), 64<<10)
		for scanner.Scan() {
			var ready struct {
				URL string `json:"url"`
			}
			if json.Unmarshal(scanner.Bytes(), &ready) == nil && ready.URL != "" {
				select {
				case address <- ready.URL:
				default:
				}
			}
		}
		_, _ = io.Copy(io.Discard, h.Stdout())
	}()
	go func() { defer output.Done(); defer h.Stderr().Close(); _, _ = io.Copy(io.Discard, h.Stderr()) }()
	go func() { p.result, p.waitErr = h.Wait(context.Background()); output.Wait(); close(p.done) }()
	select {
	case <-ctx.Done():
		_ = h.Stdin().Close()
		return nil, errors.New("account process startup timed out")
	case <-p.done:
		return nil, errors.New("account process stopped during startup")
	case endpoint := <-address:
		client, err := NewClient(endpoint, password, filepath.Join(p.config.RuntimeRoot, "work"))
		if err != nil {
			_ = h.Stdin().Close()
			return nil, err
		}
		// Loading the integration also activates the private plugin at this exact
		// isolated location before a login can start.
		var integrations []struct {
			ID string `json:"id"`
		}
		if err = client.call(ctx, "GET", "/api/integration", nil, &integrations); err == nil {
			_, err = client.Connections(ctx, CopilotProvider)
		}
		if err != nil {
			client.CloseIdleConnections()
			_ = h.Stdin().Close()
			return nil, err
		}
		p.client = client
		return client, nil
	}
}

// Close stops the process before it deletes the native credential store. A
// failed stop retains the private tree and cannot authorize a replacement.
func (p *Process) Close(ctx context.Context) error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
	if p.client != nil {
		p.client.CloseIdleConnections()
	}
	if p.done != nil {
		if p.handle != nil {
			_ = p.handle.Stdin().Close()
		}
		select {
		case <-p.done:
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
			if p.handle == nil {
				return errors.New("account process stop is unproven")
			}
			if err := p.handle.Cancel(ctx); err != nil {
				return err
			}
			select {
			case <-p.done:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		if !p.result.Quiescent {
			return errors.New("account process stop is unproven")
		}
	}
	return os.RemoveAll(p.config.RuntimeRoot)
}

// RecoverAndPurge removes a prior temporary store only after the retained exact
// guardian receipt proves its complete process tree stopped. A missing receipt
// retains the store for retry; process-name or PID absence is not a substitute.
func RecoverAndPurge(root string) error {
	info, err := os.Lstat(root)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("private account runtime is invalid")
	}
	raw, err := os.ReadFile(filepath.Join(root, "ownership.json"))
	if err != nil || len(raw) > 65536 {
		return errors.New("private account process ownership is unavailable")
	}
	var own processguardian.Ownership
	if json.Unmarshal(raw, &own) != nil || own.GuardID == "" {
		return errors.New("private account process ownership is invalid")
	}
	result, _, _, err := processguardian.ReadQuiescenceEvidence(filepath.Join(root, "guardian"), own.GuardID)
	if err != nil || !result.Quiescent || result.ChildPID != own.ChildPID || result.ChildIdentityToken != own.ChildIdentityToken || result.TreeID != own.TreeID || result.TreeIdentityToken != own.TreeIdentityToken || result.GuardianPID != own.GuardianPID {
		return errors.New("private account process stop is unproven")
	}
	return os.RemoveAll(root)
}
