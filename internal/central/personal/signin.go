package personal

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"kingdom/internal/central/accounts"
	"kingdom/internal/central/catalog"
	"kingdom/internal/central/connections"
	"kingdom/internal/central/mutation"
)

type SignInsConfig struct {
	DB           *sql.DB
	Accounts     *Service
	Connections  *connections.Store
	Catalog      *catalog.Store
	BinaryPath   string
	RuntimeRoot  string
	CheckSession func(context.Context, string, string) error
}
type SignIn struct {
	ID           string                `json:"id"`
	Provider     string                `json:"provider"`
	Status       string                `json:"status"`
	URL          string                `json:"url,omitempty"`
	Instructions string                `json:"instructions,omitempty"`
	PairingCode  string                `json:"pairing_code,omitempty"`
	ExpiresAt    time.Time             `json:"expires_at"`
	Connection   *connections.Metadata `json:"connection,omitempty"`
}
type loginProcess interface {
	Client(context.Context) (*accounts.Client, error)
	Close(context.Context) error
}
type signInAttempt struct {
	mu                         sync.Mutex
	view                       SignIn
	owner, session, label, key string
	ref                        connections.AccountReference
	process                    loginProcess
	cancel                     context.CancelFunc
	ready                      chan struct{}
	readyOnce                  sync.Once
}
type SignIns struct {
	mu         sync.Mutex
	cfg        SignInsConfig
	life       context.Context
	cancel     context.CancelFunc
	closed     bool
	attempts   map[string]*signInAttempt
	active     sync.WaitGroup
	done       chan struct{}
	newProcess func(accounts.ProcessConfig) (loginProcess, error)
}

func NewSignIns(ctx context.Context, c SignInsConfig) (*SignIns, error) {
	if c.DB == nil || c.Accounts == nil || c.Connections == nil || c.Catalog == nil || c.CheckSession == nil || !filepath.IsAbs(c.BinaryPath) || !filepath.IsAbs(c.RuntimeRoot) {
		return nil, ErrDenied
	}
	if err := initializePortalRequests(ctx, c.DB); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(c.RuntimeRoot, 0o700); err != nil {
		return nil, err
	}
	info, err := os.Lstat(c.RuntimeRoot)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0o077 != 0 {
		return nil, ErrDenied
	}
	_, err = c.DB.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS central_personal_signins (
 id TEXT PRIMARY KEY, owner_id TEXT NOT NULL, session_id TEXT NOT NULL, label TEXT NOT NULL, request_key TEXT NOT NULL,
 status TEXT NOT NULL CHECK(status IN ('starting','pending','publishing','cleanup','canceling','complete','failed','canceled')),
 expires_at INTEGER NOT NULL, account_id TEXT NOT NULL DEFAULT '', connection_json BLOB,
 UNIQUE(owner_id,request_key));
 CREATE TRIGGER IF NOT EXISTS central_personal_signin_identity BEFORE UPDATE OF id,owner_id,session_id,label,request_key ON central_personal_signins BEGIN SELECT RAISE(ABORT,'sign-in identity is immutable'); END;
 CREATE TRIGGER IF NOT EXISTS central_personal_signin_retained BEFORE DELETE ON central_personal_signins BEGIN SELECT RAISE(ABORT,'sign-in is retained'); END;`)
	if err != nil {
		return nil, err
	}
	life, cancel := context.WithCancel(context.Background())
	s := &SignIns{cfg: c, life: life, cancel: cancel, attempts: map[string]*signInAttempt{}, done: make(chan struct{}), newProcess: func(c accounts.ProcessConfig) (loginProcess, error) { return accounts.NewProcess(c) }}
	if err = s.recoverPublished(ctx); err != nil {
		cancel()
		return nil, err
	}
	return s, nil
}

func (s *SignIns) authorize(ctx context.Context, owner, session string) error {
	if owner == "" || session == "" || len(session) > 512 {
		return ErrDenied
	}
	_, err := s.cfg.Catalog.ActiveUser(ctx, owner)
	if err != nil {
		return ErrDenied
	}
	if s.cfg.CheckSession(ctx, owner, session) != nil {
		return ErrDenied
	}
	return nil
}
func (s *SignIns) load(ctx context.Context, owner, session, id string) (*signInAttempt, error) {
	a := &signInAttempt{owner: owner, session: session, ready: make(chan struct{})}
	var expires int64
	var connection []byte
	err := s.cfg.DB.QueryRowContext(ctx, `SELECT id,label,request_key,status,expires_at,account_id,connection_json FROM central_personal_signins WHERE id=? AND owner_id=? AND session_id=?`, id, owner, session).Scan(&a.view.ID, &a.label, &a.key, &a.view.Status, &expires, &a.ref.ID, &connection)
	if err != nil {
		return nil, ErrDenied
	}
	a.view.Provider = accounts.CopilotProvider
	a.view.ExpiresAt = time.UnixMilli(expires).UTC()
	if a.ref.ID != "" {
		a.ref.Revision = 1
	}
	if len(connection) > 0 {
		var m connections.Metadata
		if json.Unmarshal(connection, &m) != nil {
			return nil, ErrDenied
		}
		a.view.Connection = &m
	}
	a.readyOnce.Do(func() { close(a.ready) })
	return a, nil
}
func snapshot(a *signInAttempt) SignIn {
	a.mu.Lock()
	defer a.mu.Unlock()
	v := a.view
	if v.Connection != nil {
		m := *v.Connection
		v.Connection = &m
	}
	return v
}
func (s *SignIns) Get(ctx context.Context, owner, session, id string) (SignIn, error) {
	if err := s.authorize(ctx, owner, session); err != nil {
		return SignIn{}, err
	}
	s.mu.Lock()
	a := s.attempts[id]
	s.mu.Unlock()
	if a != nil {
		if a.owner != owner || a.session != session {
			return SignIn{}, ErrDenied
		}
		a.mu.Lock()
		if a.view.Status == "cleanup" && a.process != nil {
			_ = s.retryCleanup(ctx, a)
		}
		a.mu.Unlock()
		return snapshot(a), nil
	}
	a, err := s.load(ctx, owner, session, id)
	if err != nil {
		return SignIn{}, err
	}
	return snapshot(a), nil
}

func (s *SignIns) Begin(ctx context.Context, owner, session, label, key string) (SignIn, error) {
	if err := s.authorize(ctx, owner, session); err != nil {
		return SignIn{}, err
	}
	if !mutation.ValidKey(key) || strings.TrimSpace(label) != label || label == "" || len(label) > 128 || strings.ContainsAny(label, "\x00\r\n") {
		return SignIn{}, ErrDenied
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return SignIn{}, ErrDenied
	}
	var priorID, priorSession, priorLabel string
	err := s.cfg.DB.QueryRowContext(ctx, `SELECT id,session_id,label FROM central_personal_signins WHERE owner_id=? AND request_key=?`, owner, key).Scan(&priorID, &priorSession, &priorLabel)
	if err == nil {
		if priorSession != session || priorLabel != label {
			s.mu.Unlock()
			return SignIn{}, mutation.ErrConflict
		}
		a := s.attempts[priorID]
		s.mu.Unlock()
		if a != nil {
			select {
			case <-a.ready:
				return s.Get(ctx, owner, session, priorID)
			case <-ctx.Done():
				return SignIn{}, ctx.Err()
			}
		}
		return s.Get(ctx, owner, session, priorID)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		s.mu.Unlock()
		return SignIn{}, err
	}
	var count int
	if err = s.cfg.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM central_personal_signins WHERE status IN ('starting','pending','publishing','cleanup','canceling')`).Scan(&count); err != nil {
		s.mu.Unlock()
		return SignIn{}, err
	}
	if count >= 8 {
		s.mu.Unlock()
		return SignIn{}, ErrDenied
	}
	a := &signInAttempt{view: SignIn{ID: uuid.NewString(), Provider: accounts.CopilotProvider, Status: "starting", ExpiresAt: time.Now().Add(10 * time.Minute).UTC()}, owner: owner, session: session, label: label, key: key, ready: make(chan struct{})}
	_, err = s.cfg.DB.ExecContext(ctx, `INSERT INTO central_personal_signins(id,owner_id,session_id,label,request_key,status,expires_at) VALUES(?,?,?,?,?,'starting',?)`, a.view.ID, owner, session, label, key, a.view.ExpiresAt.UnixMilli())
	if err != nil {
		s.mu.Unlock()
		return SignIn{}, err
	}
	life, cancel := context.WithDeadline(s.life, a.view.ExpiresAt)
	a.cancel = cancel
	s.attempts[a.view.ID] = a
	s.active.Add(1)
	s.mu.Unlock()
	go func() {
		defer s.active.Done()
		defer cancel()
		defer a.readyOnce.Do(func() { close(a.ready) })
		s.run(life, a)
	}()
	select {
	case <-a.ready:
		return s.Get(ctx, owner, session, a.view.ID)
	case <-ctx.Done():
		return SignIn{}, ctx.Err()
	}
}

func (s *SignIns) update(a *signInAttempt, status string) error {
	var connection []byte
	var err error
	if a.view.Connection != nil {
		connection, err = json.Marshal(a.view.Connection)
		if err != nil {
			return err
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = s.cfg.DB.ExecContext(ctx, `UPDATE central_personal_signins SET status=?,expires_at=?,account_id=?,connection_json=? WHERE id=? AND owner_id=? AND session_id=?`, status, a.view.ExpiresAt.UnixMilli(), a.ref.ID, connection, a.view.ID, a.owner, a.session)
	if err == nil {
		a.view.Status = status
	}
	return err
}
func (s *SignIns) run(ctx context.Context, a *signInAttempt) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	watchDone := make(chan struct{})
	stopWatch := make(chan struct{})
	go func() {
		defer close(watchDone)
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-stopWatch:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				if s.authorize(ctx, a.owner, a.session) != nil {
					cancel()
					return
				}
			}
		}
	}()
	defer func() { close(stopWatch); <-watchDone }()
	defer func() {
		a.mu.Lock()
		terminal := a.view.Status == "complete" || a.view.Status == "failed" || a.view.Status == "canceled"
		a.mu.Unlock()
		if terminal {
			s.mu.Lock()
			delete(s.attempts, a.view.ID)
			s.mu.Unlock()
		}
	}()
	p, err := s.newProcess(accounts.ProcessConfig{BinaryPath: s.cfg.BinaryPath, RuntimeRoot: filepath.Join(s.cfg.RuntimeRoot, a.view.ID)})
	if err != nil {
		a.mu.Lock()
		_ = s.update(a, "failed")
		a.mu.Unlock()
		return
	}
	a.mu.Lock()
	a.process = p
	a.mu.Unlock()
	final := "failed"
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err := p.Close(cleanup)
		cancel()
		a.mu.Lock()
		defer a.mu.Unlock()
		if err != nil {
			_ = s.update(a, "cleanup")
			return
		}
		if a.view.Status == "canceling" {
			final = "canceled"
		}
		if a.view.Status == "complete" {
			return
		}
		if a.ref.ID != "" && final != "complete" {
			fence, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = s.cfg.Accounts.fence(fence, a.owner, a.ref)
			cancel()
		}
		a.view.URL = ""
		a.view.Instructions = ""
		_ = s.update(a, final)
	}()
	c, err := p.Client(ctx)
	if err != nil {
		return
	}
	native, err := c.Begin(ctx, accounts.CopilotProvider, "device", a.view.ID, map[string]any{"deploymentType": "github.com"})
	if err != nil {
		return
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = c.Cancel(cleanup, accounts.CopilotProvider, native.AttemptID)
	}()
	a.mu.Lock()
	a.view.URL = native.URL
	a.view.Instructions = native.Instructions
	if expiry := time.UnixMilli(native.Time.Expires); expiry.Before(a.view.ExpiresAt) {
		a.view.ExpiresAt = expiry.UTC()
	}
	err = s.update(a, "pending")
	a.mu.Unlock()
	if err != nil {
		return
	}
	a.readyOnce.Do(func() { close(a.ready) })
	for {
		if err = s.authorize(ctx, a.owner, a.session); err != nil {
			return
		}
		state, e := c.Status(ctx, accounts.CopilotProvider, native.AttemptID)
		if e != nil {
			return
		}
		if state.Status == "complete" {
			break
		}
		if state.Status != "pending" {
			return
		}
		select {
		case <-ctx.Done():
			a.mu.Lock()
			if a.view.Status == "canceling" {
				final = "canceled"
			}
			a.mu.Unlock()
			return
		case <-time.After(time.Second):
		}
		if time.Now().After(a.view.ExpiresAt) {
			return
		}
	}
	if ctx.Err() != nil {
		return
	}
	connectionsList, err := c.Connections(ctx, accounts.CopilotProvider)
	if err != nil || len(connectionsList) != 1 || connectionsList[0].Label != a.view.ID {
		return
	}
	if s.authorize(ctx, a.owner, a.session) != nil {
		return
	}
	credential, err := c.Resolve(ctx, accounts.CopilotProvider, connectionsList[0].ID)
	if err != nil {
		return
	}
	if s.authorize(ctx, a.owner, a.session) != nil {
		return
	}
	a.mu.Lock()
	a.ref = connections.AccountReference{ID: uuid.NewString(), Revision: 1}
	err = s.update(a, "publishing")
	a.mu.Unlock()
	if err != nil {
		return
	}
	if err = s.cfg.Accounts.SaveCopilotReference(ctx, a.owner, a.ref, credential); err != nil {
		return
	}
	credential = accounts.OAuthCredential{}
	if err = s.authorize(ctx, a.owner, a.session); err != nil {
		return
	}
	m, err := s.cfg.Connections.CreateAccountRequest(ctx, a.owner, connections.Definition{Label: a.label, Provider: accounts.CopilotProvider, AuthKind: "copilot_oauth"}, a.ref, a.view.ID)
	if err != nil {
		return
	}
	a.mu.Lock()
	a.view.Connection = &m
	err = s.update(a, "cleanup")
	a.mu.Unlock()
	if err != nil {
		return
	}
	if err = p.Close(ctx); err != nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if ctx.Err() != nil {
		return
	}
	a.view.URL = ""
	a.view.Instructions = ""
	if s.update(a, "complete") == nil {
		final = "complete"
	}
}

func (s *SignIns) Cancel(ctx context.Context, owner, session, id string) error {
	if err := s.authorize(ctx, owner, session); err != nil {
		return err
	}
	s.mu.Lock()
	a := s.attempts[id]
	s.mu.Unlock()
	if a == nil {
		_, err := s.load(ctx, owner, session, id)
		return err
	}
	if a.owner != owner || a.session != session {
		return ErrDenied
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.view.Status == "complete" || a.view.Status == "failed" || a.view.Status == "canceled" {
		return nil
	}
	if a.view.Status == "publishing" || a.view.Status == "cleanup" {
		return ErrDenied
	}
	if err := s.update(a, "canceling"); err != nil {
		return err
	}
	a.cancel()
	return nil
}

func (s *SignIns) recoverPublished(ctx context.Context) error {
	rows, err := s.cfg.DB.QueryContext(ctx, `SELECT id,owner_id,session_id FROM central_personal_signins WHERE status IN ('starting','pending','publishing','cleanup','canceling')`)
	if err != nil {
		return err
	}
	type identity struct{ id, owner, session string }
	var pending []identity
	for rows.Next() {
		var v identity
		if err = rows.Scan(&v.id, &v.owner, &v.session); err != nil {
			rows.Close()
			return err
		}
		pending = append(pending, v)
	}
	if err = rows.Close(); err != nil {
		return err
	}
	for _, v := range pending {
		a, e := s.load(ctx, v.owner, v.session, v.id)
		if e != nil {
			return e
		}
		final := "failed"
		if a.view.Status == "canceling" {
			final = "canceled"
		}
		// Never restart an ambiguous native Begin. Only an already encrypted
		// account can recover the exact idempotent central connection.
		if a.ref.ID != "" {
			if _, readErr := s.cfg.Accounts.read(ctx, a.owner, a.ref); readErr == nil && s.authorize(ctx, a.owner, a.session) == nil {
				m, createErr := s.cfg.Connections.CreateAccountRequest(ctx, a.owner, connections.Definition{Label: a.label, Provider: accounts.CopilotProvider, AuthKind: "copilot_oauth"}, a.ref, a.view.ID)
				if createErr != nil {
					return createErr
				}
				a.view.Connection = &m
				final = "complete"
			} else {
				_ = s.cfg.Accounts.fence(ctx, a.owner, a.ref)
			}
		}
		if accounts.RecoverAndPurge(filepath.Join(s.cfg.RuntimeRoot, a.view.ID)) != nil {
			final = "cleanup"
		}
		a.mu.Lock()
		e = s.update(a, final)
		a.mu.Unlock()
		if e != nil {
			return e
		}
	}
	return nil
}

func (s *SignIns) Close(ctx context.Context) error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	if !s.closed {
		s.closed = true
		s.cancel()
		go func() { s.active.Wait(); close(s.done) }()
	}
	s.mu.Unlock()
	select {
	case <-s.done:
		s.mu.Lock()
		var attempts []*signInAttempt
		for _, a := range s.attempts {
			attempts = append(attempts, a)
		}
		s.mu.Unlock()
		var failures []error
		for _, a := range attempts {
			a.mu.Lock()
			if a.view.Status == "cleanup" && a.process != nil {
				if err := s.retryCleanup(ctx, a); err != nil {
					failures = append(failures, err)
				}
			}
			a.mu.Unlock()
		}
		return errors.Join(failures...)
	case <-ctx.Done():
		return ctx.Err()
	}
}

// retryCleanup is called with the attempt lock held. A receipt failure retains
// cleanup state. Only a proven closed store can produce a final public result.
func (s *SignIns) retryCleanup(ctx context.Context, a *signInAttempt) error {
	if err := a.process.Close(ctx); err != nil {
		return err
	}
	final := "failed"
	if a.view.Connection != nil && s.authorize(ctx, a.owner, a.session) == nil {
		if _, err := s.cfg.Accounts.read(ctx, a.owner, a.ref); err == nil {
			final = "complete"
		}
	}
	if final != "complete" && a.ref.ID != "" {
		_ = s.cfg.Accounts.fence(ctx, a.owner, a.ref)
	}
	a.view.URL = ""
	a.view.Instructions = ""
	return s.update(a, final)
}
