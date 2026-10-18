package opencode

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"kingdom/internal/processguardian"

	"github.com/tailscale/hujson"
)

// ServiceConfig describes Kingdom's dedicated OpenCode server. Provider and
// account configuration stays in OpenCode's inherited environment and stores.
type ServiceConfig struct {
	BinaryPath  string
	WorkDir     string
	RuntimeRoot string
	// Env replaces the inherited environment when non-nil. It allows isolated
	// installations and fixtures without changing the host's account stores.
	Env []string
}

// Service owns one lazily started server. Consumers borrow its client; only
// the service assembly closes the server, after its consumers have drained.
// Stopping this process does not settle any managed model or workspace effect.
type Service struct {
	binaryPath string
	workDir    string
	env        []string
	runner     GuardianRunner
	startLimit time.Duration
	stopLimit  time.Duration
	ctx        context.Context
	cancel     context.CancelFunc

	mu      sync.Mutex
	closed  bool
	attempt *serviceStart
}

type serviceStart struct {
	done    chan struct{}
	client  *Client
	process *serviceProcess
	err     error
}

type serviceProcess struct {
	handle processguardian.Handle
	done   chan struct{}
	result processguardian.Result
	err    error
}

func NewService(cfg ServiceConfig) (*Service, error) {
	binaryPath := strings.TrimSpace(cfg.BinaryPath)
	if binaryPath == "" {
		binaryPath = "opencode"
	}
	workDir := strings.TrimSpace(cfg.WorkDir)
	if workDir == "" {
		return nil, fmt.Errorf("OpenCode service working directory is required")
	}
	workDir, err := filepath.Abs(workDir)
	if err != nil {
		return nil, fmt.Errorf("resolve OpenCode service working directory: %w", err)
	}
	if info, err := os.Stat(workDir); err != nil || !info.IsDir() {
		return nil, fmt.Errorf("OpenCode service working directory must be a directory")
	}
	workDir, err = filepath.EvalSymlinks(workDir)
	if err != nil {
		return nil, fmt.Errorf("resolve OpenCode service working directory symlinks: %w", err)
	}
	env := cfg.Env
	if env == nil {
		env = os.Environ()
	}
	env, err = serviceEnvironment(env)
	if err != nil {
		return nil, err
	}
	runner, err := processguardian.NewRunner(processguardian.Config{RuntimeRoot: cfg.RuntimeRoot, CancelGrace: time.Second, CancelForce: time.Second})
	if err != nil {
		return nil, fmt.Errorf("initialize OpenCode service guardian: %w", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Service{binaryPath: binaryPath, workDir: workDir, env: env, runner: runner, startLimit: 20 * time.Second, stopLimit: 5 * time.Second, ctx: ctx, cancel: cancel}, nil
}

// Client collapses concurrent starts. A caller's cancellation stops its wait,
// not another caller's shared startup. Failed starts can be retried only after
// the previous guarded process has demonstrably stopped.
func (s *Service) Client(ctx context.Context) (*Client, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, fmt.Errorf("OpenCode service is closed")
	}
	attempt := s.attempt
	if attempt != nil {
		select {
		case <-attempt.done:
			if attempt.err == nil && attempt.process != nil {
				select {
				case <-attempt.process.done:
					attempt.err = fmt.Errorf("OpenCode service stopped unexpectedly")
				default:
				}
			}
			if attempt.err != nil && serviceProcessStopped(attempt.process) {
				if attempt.client != nil {
					attempt.client.CloseIdleConnections()
				}
				attempt = nil
			}
		default:
		}
	}
	if attempt == nil {
		attempt = &serviceStart{done: make(chan struct{})}
		s.attempt = attempt
		go s.start(attempt)
	}
	s.mu.Unlock()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-attempt.done:
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.closed {
			return nil, fmt.Errorf("OpenCode service is closed")
		}
		return attempt.client, attempt.err
	}
}

// Models uses the same provider configuration and location as this service.
func (s *Service) Models(ctx context.Context) ([]Model, error) {
	client, err := s.Client(ctx)
	if err != nil {
		return nil, err
	}
	return client.Models(ctx, s.workDir)
}

func serviceProcessStopped(process *serviceProcess) bool {
	if process == nil {
		return true
	}
	select {
	case <-process.done:
		return process.result.Quiescent
	default:
		return false
	}
}

func (s *Service) start(attempt *serviceStart) {
	ctx, cancel := context.WithTimeout(s.ctx, s.startLimit)
	defer cancel()
	client, process, err := s.launch(ctx)
	if err != nil && process != nil {
		cleanupCtx, stop := context.WithTimeout(context.Background(), s.stopLimit)
		err = errors.Join(err, stopServiceProcess(cleanupCtx, process))
		stop()
	}
	s.mu.Lock()
	attempt.client, attempt.process, attempt.err = client, process, err
	close(attempt.done)
	s.mu.Unlock()
}

func (s *Service) launch(ctx context.Context) (*Client, *serviceProcess, error) {
	var secret [32]byte
	if _, err := rand.Read(secret[:]); err != nil {
		return nil, nil, fmt.Errorf("generate OpenCode service credential: %w", err)
	}
	password := hex.EncodeToString(secret[:])
	env := replaceServiceEnv(s.env, "OPENCODE_SERVER_PASSWORD", password)
	// Both public aliases may be inherited; give this server exactly one
	// explicit credential so OpenCode never prints a generated password.
	env = replaceServiceEnv(env, "OPENCODE_PASSWORD", password)
	handle, err := s.runner.Start(ctx, processguardian.Command{Path: s.binaryPath, Args: []string{"serve", "--stdio", "--hostname", "127.0.0.1", "--port", "0"}, Dir: s.workDir, Env: env}, nil)
	if err != nil {
		var launchErr *processguardian.LaunchError
		if errors.As(err, &launchErr) && !launchErr.Result.Quiescent {
			// A failed launch with uncertain process shutdown must not be
			// replaced merely because no live handle was returned.
			process := &serviceProcess{done: make(chan struct{}), result: launchErr.Result, err: err}
			close(process.done)
			return nil, process, fmt.Errorf("start OpenCode service: %w", err)
		}
		return nil, nil, fmt.Errorf("start OpenCode service: %w", err)
	}
	process := &serviceProcess{handle: handle, done: make(chan struct{})}
	endpoint := make(chan string, 1)
	var output sync.WaitGroup
	output.Add(2)
	go func() {
		defer output.Done()
		defer handle.Stdout().Close()
		scanner := bufio.NewScanner(handle.Stdout())
		scanner.Buffer(make([]byte, 4096), 64<<10)
		for scanner.Scan() {
			if address, ok := serviceEndpoint(scanner.Text()); ok {
				select {
				case endpoint <- address:
				default:
				}
			}
		}
		// Keep draining even if a diagnostic exceeded the scanner bound.
		_, _ = io.Copy(io.Discard, handle.Stdout())
	}()
	go func() {
		defer output.Done()
		defer handle.Stderr().Close()
		_, _ = io.Copy(io.Discard, handle.Stderr())
	}()
	go func() {
		process.result, process.err = handle.Wait(context.Background())
		output.Wait()
		close(process.done)
	}()
	var address string
	select {
	case <-ctx.Done():
		return nil, process, fmt.Errorf("wait for OpenCode service address: %w", ctx.Err())
	case <-process.done:
		return nil, process, fmt.Errorf("OpenCode service exited before readiness")
	case address = <-endpoint:
	}
	client, err := NewClient(address, password)
	if err != nil {
		return nil, process, err
	}
	ready := false
	defer func() {
		if !ready {
			client.CloseIdleConnections()
		}
	}()
	if err := client.Health(ctx); err != nil {
		return nil, process, fmt.Errorf("check OpenCode service health: %w", err)
	}
	if err := client.ValidateConfiguration(ctx, s.workDir); err != nil {
		return nil, process, fmt.Errorf("validate OpenCode service configuration: %w", err)
	}
	select {
	case <-process.done:
		return nil, process, fmt.Errorf("OpenCode service exited during readiness")
	default:
	}
	ready = true
	return client, process, nil
}

// CloseContext permanently stops admission and cancels startup. A timed-out
// close can be retried; it never permits a replacement server to start.
func (s *Service) CloseContext(ctx context.Context) error {
	if s == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, s.stopLimit)
	defer cancel()
	s.mu.Lock()
	s.closed = true
	s.cancel()
	attempt := s.attempt
	s.mu.Unlock()
	if attempt == nil {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-attempt.done:
		if attempt.client != nil {
			defer attempt.client.CloseIdleConnections()
		}
		return stopServiceProcess(ctx, attempt.process)
	}
}

func (s *Service) ForceClose() error {
	if s == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), s.stopLimit)
	defer cancel()
	return s.CloseContext(ctx)
}

func stopServiceProcess(ctx context.Context, process *serviceProcess) error {
	if process == nil {
		return nil
	}
	select {
	case <-process.done:
		if !process.result.Quiescent {
			return errors.Join(process.err, fmt.Errorf("OpenCode service guardian did not prove process shutdown"))
		}
		return process.err
	default:
	}
	cancelErr := process.handle.Cancel(ctx)
	select {
	case <-ctx.Done():
		return errors.Join(cancelErr, ctx.Err())
	case <-process.done:
		if !process.result.Quiescent {
			return errors.Join(cancelErr, process.err, fmt.Errorf("OpenCode service guardian did not prove process shutdown"))
		}
		return errors.Join(cancelErr, process.err)
	}
}

func serviceEndpoint(line string) (string, bool) {
	var ready struct {
		URL string `json:"url"`
	}
	if err := json.Unmarshal([]byte(line), &ready); err != nil {
		return "", false
	}
	address := ready.URL
	parsed, err := url.Parse(address)
	if err != nil || parsed.Scheme != "http" || parsed.User != nil || parsed.Path != "" || parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", false
	}
	host, port, err := net.SplitHostPort(parsed.Host)
	portNumber, portErr := strconv.Atoi(port)
	if err != nil || host != "127.0.0.1" || portErr != nil || portNumber <= 0 || portNumber > 65535 {
		return "", false
	}
	return address, true
}

func serviceEnvironment(inherited []string) ([]string, error) {
	var inline string
	for _, variable := range inherited {
		if value, ok := strings.CutPrefix(variable, "OPENCODE_CONFIG_CONTENT="); ok {
			inline = value
		}
	}
	config := make(map[string]json.RawMessage)
	if strings.TrimSpace(inline) != "" {
		standard, err := hujson.Standardize([]byte(inline))
		if err != nil {
			return nil, fmt.Errorf("OpenCode inline configuration must be a valid JSONC object")
		}
		if err := json.Unmarshal(standard, &config); err != nil || config == nil {
			return nil, fmt.Errorf("OpenCode inline configuration must be a valid JSONC object")
		}
	}
	config["formatter"] = json.RawMessage("false")
	encoded, err := json.Marshal(config)
	if err != nil {
		return nil, fmt.Errorf("encode OpenCode service configuration: %w", err)
	}
	return replaceServiceEnv(inherited, "OPENCODE_CONFIG_CONTENT", string(encoded)), nil
}

func replaceServiceEnv(env []string, name, value string) []string {
	result := make([]string, 0, len(env)+1)
	prefix := name + "="
	for _, variable := range env {
		if !strings.HasPrefix(variable, prefix) {
			result = append(result, variable)
		}
	}
	return append(result, prefix+value)
}
