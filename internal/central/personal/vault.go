// Package personal owns credentials for one central user's personal accounts.
// Account credentials never enter a project, worker, or public JSON response.
package personal

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/subtle"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"sync"

	"github.com/google/uuid"
	"kingdom/internal/central/accounts"
	"kingdom/internal/central/connections"
	"kingdom/internal/central/modelproxy"
	"kingdom/internal/subscription/chatgpt"
)

var ErrDenied = errors.New("personal account unavailable")

type Config struct {
	DB  *sql.DB
	Key []byte
	// Client is trusted transport configuration for controlled acceptance only.
	// All request URLs are still selected from fixed provider origins.
	Client  *http.Client
	ChatGPT *chatgpt.Service
}
type Service struct {
	db       *sql.DB
	aead     cipher.AEAD
	client   *http.Client
	chatgpt  *chatgpt.Service
	mu       sync.Mutex
	closed   bool
	lifetime context.Context
	cancel   context.CancelFunc
	active   sync.WaitGroup
	done     chan struct{}
}

func New(ctx context.Context, c Config) (*Service, error) {
	if c.DB == nil || len(c.Key) != 32 {
		return nil, ErrDenied
	}
	b, err := aes.NewCipher(c.Key)
	if err != nil {
		return nil, err
	}
	a, err := cipher.NewGCM(b)
	if err != nil {
		return nil, err
	}
	client, err := modelproxy.DiscoveryClient(CopilotOrigins(), nil)
	if err != nil {
		return nil, err
	}
	if c.Client != nil {
		clone := *c.Client
		clone.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
		client = &clone
	}
	_, err = c.DB.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS central_personal_accounts (
 id TEXT PRIMARY KEY, owner_id TEXT NOT NULL, provider TEXT NOT NULL CHECK(provider='github-copilot'),
 revision INTEGER NOT NULL CHECK(revision=1), state TEXT NOT NULL CHECK(state IN ('ready','signed_out')), credential BLOB,
 CHECK((state='ready' AND credential IS NOT NULL) OR (state='signed_out' AND credential IS NULL)));
 CREATE TRIGGER IF NOT EXISTS central_personal_identity_immutable BEFORE UPDATE OF id,owner_id,provider,revision ON central_personal_accounts BEGIN SELECT RAISE(ABORT,'personal account identity is immutable'); END;
 CREATE TRIGGER IF NOT EXISTS central_personal_retained BEFORE DELETE ON central_personal_accounts BEGIN SELECT RAISE(ABORT,'personal account is retained'); END;`)
	if err != nil {
		client.CloseIdleConnections()
		return nil, err
	}
	life, cancel := context.WithCancel(context.Background())
	return &Service{db: c.DB, aead: a, client: client, chatgpt: c.ChatGPT, lifetime: life, cancel: cancel, done: make(chan struct{})}, nil
}

func CopilotOrigins() []modelproxy.OriginPolicy {
	return []modelproxy.OriginPolicy{{Origin: "https://api.githubcopilot.com"}, {Origin: "https://api.individual.githubcopilot.com"}, {Origin: "https://api.business.githubcopilot.com"}, {Origin: "https://api.enterprise.githubcopilot.com"}}
}
func copilotEndpoint(c accounts.OAuthCredential) (string, error) {
	if c.Type != "oauth" || c.MethodID != "device" || c.Access == "" || c.Access != c.Refresh || c.Expires != 0 {
		return "", ErrDenied
	}
	if _, err := modelproxy.NewBearerCredential(c.Access); err != nil {
		return "", ErrDenied
	}
	for k := range c.Metadata {
		if k != "apiEndpoint" {
			return "", ErrDenied
		}
	}
	u := strings.TrimRight(c.Metadata["apiEndpoint"], "/")
	if u == "" {
		u = "https://api.githubcopilot.com"
	}
	for _, origin := range CopilotOrigins() {
		if u == origin.Origin {
			return u, nil
		}
	}
	return "", ErrDenied
}
func aad(owner string, ref connections.AccountReference) []byte {
	return []byte("kingdom-central-personal/v1\x00" + owner + "\x00" + ref.ID + "\x001\x00github-copilot")
}

// SaveCopilot accepts only the result of Kingdom's private native OpenCode
// sign-in process. This method is not an HTTP credential-import endpoint.
func (s *Service) SaveCopilot(ctx context.Context, owner string, c accounts.OAuthCredential) (connections.AccountReference, error) {
	ref := connections.AccountReference{ID: uuid.NewString(), Revision: 1}
	return ref, s.SaveCopilotReference(ctx, owner, ref, c)
}

// SaveCopilotReference allows a sign-in coordinator to retain the authorization
// identity before the credential commit. Exact retries cannot create an orphan
// authorization or replace the credential of an existing identity.
func (s *Service) SaveCopilotReference(ctx context.Context, owner string, ref connections.AccountReference, c accounts.OAuthCredential) error {
	ctx, done, err := s.enter(ctx)
	if err != nil {
		return err
	}
	defer done()
	id, idErr := uuid.Parse(ref.ID)
	if owner == "" || len(owner) > 256 || idErr != nil || id == uuid.Nil || id.String() != ref.ID || ref.Revision != 1 {
		return ErrDenied
	}
	if _, err = copilotEndpoint(c); err != nil {
		return err
	}
	raw, err := json.Marshal(c)
	if err != nil {
		return ErrDenied
	}
	defer clear(raw)
	nonce := make([]byte, s.aead.NonceSize())
	if _, err = rand.Read(nonce); err != nil {
		return err
	}
	sealed := s.aead.Seal(nonce, nonce, raw, aad(owner, ref))
	_, err = s.db.ExecContext(ctx, `INSERT INTO central_personal_accounts(id,owner_id,provider,revision,state,credential) VALUES(?,?,'github-copilot',1,'ready',?) ON CONFLICT(id) DO NOTHING`, ref.ID, owner, sealed)
	if err != nil {
		return err
	}
	existing, err := s.read(ctx, owner, ref)
	if err != nil {
		return ErrDenied
	}
	previous, err := json.Marshal(existing)
	if err != nil {
		return ErrDenied
	}
	defer clear(previous)
	if subtle.ConstantTimeCompare(previous, raw) != 1 {
		return ErrDenied
	}
	return nil
}
func (s *Service) read(ctx context.Context, owner string, ref connections.AccountReference) (accounts.OAuthCredential, error) {
	if ref.Revision != 1 {
		return accounts.OAuthCredential{}, ErrDenied
	}
	var sealed []byte
	if s.db.QueryRowContext(ctx, `SELECT credential FROM central_personal_accounts WHERE id=? AND owner_id=? AND revision=? AND state='ready'`, ref.ID, owner, ref.Revision).Scan(&sealed) != nil || len(sealed) < s.aead.NonceSize() || len(sealed) > 32768 {
		return accounts.OAuthCredential{}, ErrDenied
	}
	n := s.aead.NonceSize()
	raw, err := s.aead.Open(nil, sealed[:n], sealed[n:], aad(owner, ref))
	if err != nil {
		return accounts.OAuthCredential{}, ErrDenied
	}
	defer clear(raw)
	var c accounts.OAuthCredential
	if json.Unmarshal(raw, &c) != nil {
		return c, ErrDenied
	}
	_, err = copilotEndpoint(c)
	return c, err
}
func (s *Service) fence(ctx context.Context, owner string, ref connections.AccountReference) error {
	r, err := s.db.ExecContext(ctx, `UPDATE central_personal_accounts SET state='signed_out',credential=NULL WHERE id=? AND owner_id=? AND revision=?`, ref.ID, owner, ref.Revision)
	if err != nil {
		return ErrDenied
	}
	n, _ := r.RowsAffected()
	if n != 1 {
		return ErrDenied
	}
	return nil
}
func (s *Service) enter(ctx context.Context) (context.Context, func(), error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, nil, ErrDenied
	}
	s.active.Add(1)
	child, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(s.lifetime, cancel)
	return child, func() { stop(); cancel(); s.active.Done() }, nil
}
func (s *Service) Close(ctx context.Context) error {
	s.mu.Lock()
	if !s.closed {
		s.closed = true
		s.cancel()
		go func() { s.active.Wait(); s.client.CloseIdleConnections(); close(s.done) }()
	}
	s.mu.Unlock()
	select {
	case <-s.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
