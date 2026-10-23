package personal

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"kingdom/internal/central/connections"
	"kingdom/internal/subscription/chatgpt"
)

type HandoffConfig struct {
	Accounts     *Service
	Connections  *connections.Store
	CheckSession func(context.Context, string, string) error
	HostID       string
	Registry     interface {
		RevokeConnection(context.Context, string, int64) error
	}
}
type Handoffs struct {
	config HandoffConfig
	mu     sync.Mutex
	locks  [32]sync.Mutex
}
type HandoffTarget struct {
	AttemptID string    `json:"attempt_id"`
	OwnerID   string    `json:"owner_id"`
	HostID    string    `json:"host_id"`
	ExpiresAt time.Time `json:"expires_at"`
}

func NewHandoffs(ctx context.Context, c HandoffConfig) (*Handoffs, error) {
	if c.Accounts == nil || c.Accounts.chatgpt == nil || c.Connections == nil || c.CheckSession == nil || c.Registry == nil || !strings.HasPrefix(c.HostID, "urn:uuid:") || !validUUID(strings.TrimPrefix(c.HostID, "urn:uuid:")) {
		return nil, ErrDenied
	}
	if err := initializePortalRequests(ctx, c.Accounts.db); err != nil {
		return nil, err
	}
	_, err := c.Accounts.db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS central_chatgpt_handoffs (
 id TEXT PRIMARY KEY, owner_id TEXT NOT NULL, session_id TEXT NOT NULL, label TEXT NOT NULL,
 request_key TEXT NOT NULL, code_hash BLOB NOT NULL UNIQUE, code BLOB NOT NULL,
 expires_ns INTEGER NOT NULL, state TEXT NOT NULL CHECK(state IN ('pending','completing','complete','canceled')),
 transfer_hash BLOB, connection_id TEXT NOT NULL DEFAULT '', UNIQUE(owner_id,request_key));
CREATE TRIGGER IF NOT EXISTS central_handoff_identity BEFORE UPDATE OF id,owner_id,session_id,label,request_key,code_hash,code,expires_ns ON central_chatgpt_handoffs BEGIN SELECT RAISE(ABORT,'handoff identity is immutable');END;
CREATE TRIGGER IF NOT EXISTS central_handoff_hash BEFORE UPDATE OF transfer_hash ON central_chatgpt_handoffs WHEN OLD.transfer_hash IS NOT NULL AND NEW.transfer_hash IS NOT OLD.transfer_hash BEGIN SELECT RAISE(ABORT,'handoff transfer is immutable');END;
CREATE TRIGGER IF NOT EXISTS central_handoff_connection BEFORE UPDATE OF connection_id ON central_chatgpt_handoffs WHEN OLD.connection_id<>'' AND NEW.connection_id<>OLD.connection_id BEGIN SELECT RAISE(ABORT,'handoff connection is immutable');END;
CREATE TRIGGER IF NOT EXISTS central_handoff_final BEFORE UPDATE OF state ON central_chatgpt_handoffs WHEN OLD.state IN ('complete','canceled') AND NEW.state<>OLD.state BEGIN SELECT RAISE(ABORT,'handoff state is final');END;
CREATE TRIGGER IF NOT EXISTS central_handoff_retained BEFORE DELETE ON central_chatgpt_handoffs BEGIN SELECT RAISE(ABORT,'handoff is retained');END;`)
	if err != nil {
		return nil, err
	}
	return &Handoffs{config: c}, nil
}

type handoffRecord struct {
	id, owner, session, label, state, connection string
	expires                                      int64
	code, transferHash                           []byte
}

func (h *Handoffs) read(ctx context.Context, owner, session, id string) (handoffRecord, error) {
	var r handoffRecord
	err := h.config.Accounts.db.QueryRowContext(ctx, `SELECT id,owner_id,session_id,label,state,connection_id,expires_ns,code,transfer_hash FROM central_chatgpt_handoffs WHERE id=? AND owner_id=? AND session_id=?`, id, owner, session).Scan(&r.id, &r.owner, &r.session, &r.label, &r.state, &r.connection, &r.expires, &r.code, &r.transferHash)
	if err != nil {
		return r, ErrDenied
	}
	return r, nil
}
func handoffAAD(owner, id string) []byte {
	return []byte("kingdom-chatgpt-handoff/v1\x00" + owner + "\x00" + id)
}
func (h *Handoffs) Begin(ctx context.Context, owner, session, label, key string) (SignIn, error) {
	ctx, done, err := h.config.Accounts.enter(ctx)
	if err != nil {
		return SignIn{}, err
	}
	defer done()
	h.mu.Lock()
	defer h.mu.Unlock()
	if !safe(label, 256) || !validUUID(key) || h.config.CheckSession(ctx, owner, session) != nil {
		return SignIn{}, ErrDenied
	}
	var existing string
	err = h.config.Accounts.db.QueryRowContext(ctx, `SELECT id FROM central_chatgpt_handoffs WHERE owner_id=? AND request_key=?`, owner, key).Scan(&existing)
	if err == nil {
		r, e := h.read(ctx, owner, session, existing)
		if e != nil || r.label != label {
			return SignIn{}, ErrDenied
		}
		return h.view(ctx, r, true)
	}
	if err != sql.ErrNoRows {
		return SignIn{}, err
	}
	var count int
	if h.config.Accounts.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM central_chatgpt_handoffs WHERE owner_id=? AND state IN ('pending','completing') AND expires_ns>?`, owner, time.Now().UnixNano()).Scan(&count) != nil || count >= 4 {
		return SignIn{}, ErrDenied
	}
	var secret [32]byte
	if _, err = rand.Read(secret[:]); err != nil {
		return SignIn{}, err
	}
	code := base64.RawURLEncoding.EncodeToString(secret[:])
	hash := sha256.Sum256([]byte(code))
	id := uuid.NewString()
	nonce := make([]byte, h.config.Accounts.aead.NonceSize())
	if _, err = rand.Read(nonce); err != nil {
		return SignIn{}, err
	}
	sealed := h.config.Accounts.aead.Seal(nonce, nonce, []byte(code), handoffAAD(owner, id))
	expiry := time.Now().Add(10 * time.Minute).UTC()
	_, err = h.config.Accounts.db.ExecContext(ctx, `INSERT INTO central_chatgpt_handoffs(id,owner_id,session_id,label,request_key,code_hash,code,expires_ns,state) VALUES(?,?,?,?,?,?,?,?,'pending')`, id, owner, session, label, key, hash[:], sealed, expiry.UnixNano())
	if err != nil {
		return SignIn{}, err
	}
	return SignIn{ID: id, Provider: "openai", Status: "pending", ExpiresAt: expiry, PairingCode: code}, nil
}
func validUUID(s string) bool {
	id, e := uuid.Parse(s)
	return e == nil && id != uuid.Nil && id.String() == s
}
func (h *Handoffs) view(ctx context.Context, r handoffRecord, includeCode bool) (SignIn, error) {
	v := SignIn{ID: r.id, Provider: "openai", Status: r.state, ExpiresAt: time.Unix(0, r.expires).UTC()}
	if (r.state == "pending" || r.state == "completing") && time.Now().UnixNano() >= r.expires {
		v.Status = "expired"
		return v, nil
	}
	if r.connection != "" {
		m, _, err := h.config.Connections.ResolveAccountForDisconnect(ctx, r.owner, r.connection, 1)
		if err != nil {
			return SignIn{}, ErrDenied
		}
		v.Connection = &m
	}
	if includeCode && r.state == "pending" {
		n := h.config.Accounts.aead.NonceSize()
		if len(r.code) < n {
			return SignIn{}, ErrDenied
		}
		plain, err := h.config.Accounts.aead.Open(nil, r.code[:n], r.code[n:], handoffAAD(r.owner, r.id))
		if err != nil {
			return SignIn{}, ErrDenied
		}
		defer clear(plain)
		v.PairingCode = string(plain)
	}
	return v, nil
}
func (h *Handoffs) Get(ctx context.Context, owner, session, id string) (SignIn, error) {
	ctx, done, err := h.config.Accounts.enter(ctx)
	if err != nil {
		return SignIn{}, err
	}
	defer done()
	if h.config.CheckSession(ctx, owner, session) != nil {
		return SignIn{}, ErrDenied
	}
	r, err := h.read(ctx, owner, session, id)
	if err != nil {
		return SignIn{}, err
	}
	return h.view(ctx, r, false)
}
func (h *Handoffs) Cancel(ctx context.Context, owner, session, id string) error {
	ctx, done, err := h.config.Accounts.enter(ctx)
	if err != nil {
		return err
	}
	defer done()
	unlock := h.lockAttempt(id)
	defer unlock()
	if h.config.CheckSession(ctx, owner, session) != nil {
		return ErrDenied
	}
	r, err := h.read(ctx, owner, session, id)
	if err != nil {
		return err
	}
	if r.state == "complete" || r.state == "completing" {
		return ErrDenied
	}
	_, err = h.config.Accounts.db.ExecContext(ctx, `UPDATE central_chatgpt_handoffs SET state='canceled' WHERE id=? AND state<>'complete'`, id)
	return err
}
func (h *Handoffs) byCode(ctx context.Context, code string) (handoffRecord, error) {
	var r handoffRecord
	if len(code) != 43 {
		return r, ErrDenied
	}
	hash := sha256.Sum256([]byte(code))
	var id, owner, session string
	if h.config.Accounts.db.QueryRowContext(ctx, `SELECT id,owner_id,session_id FROM central_chatgpt_handoffs WHERE code_hash=?`, hash[:]).Scan(&id, &owner, &session) != nil {
		return r, ErrDenied
	}
	if h.config.CheckSession(ctx, owner, session) != nil {
		return r, ErrDenied
	}
	r, err := h.read(ctx, owner, session, id)
	if err != nil || r.state == "canceled" || time.Now().UnixNano() >= r.expires {
		return r, ErrDenied
	}
	return r, nil
}
func (h *Handoffs) Prepare(ctx context.Context, code string) (HandoffTarget, error) {
	ctx, done, err := h.config.Accounts.enter(ctx)
	if err != nil {
		return HandoffTarget{}, err
	}
	defer done()
	r, err := h.byCode(ctx, code)
	if err != nil {
		return HandoffTarget{}, err
	}
	return HandoffTarget{AttemptID: r.id, OwnerID: r.owner, HostID: h.config.HostID, ExpiresAt: time.Unix(0, r.expires).UTC()}, nil
}
func (h *Handoffs) Complete(ctx context.Context, code string, payload json.RawMessage) (SignIn, error) {
	ctx, done, err := h.config.Accounts.enter(ctx)
	if err != nil {
		return SignIn{}, err
	}
	defer done()
	r, err := h.byCode(ctx, code)
	if err != nil {
		return SignIn{}, err
	}
	unlock := h.lockAttempt(r.id)
	defer unlock()
	r, err = h.byCode(ctx, code)
	if err != nil {
		return SignIn{}, err
	}
	transfer, err := chatgpt.DecodeTransfer(payload)
	if err != nil {
		return SignIn{}, ErrDenied
	}
	hash := sha256.Sum256(payload)
	if len(r.transferHash) != 0 && !h.matchesFingerprint(r, hash[:]) {
		return SignIn{}, ErrDenied
	}
	if r.state == "complete" {
		if !h.usableCompleted(ctx, r) {
			return SignIn{}, ErrDenied
		}
		return h.view(ctx, r, false)
	}
	if r.state == "pending" {
		fingerprint, e := h.sealFingerprint(r, hash[:])
		if e != nil {
			return SignIn{}, e
		}
		result, e := h.config.Accounts.db.ExecContext(ctx, `UPDATE central_chatgpt_handoffs SET state='completing',transfer_hash=? WHERE id=? AND state='pending' AND transfer_hash IS NULL`, fingerprint, r.id)
		if e != nil {
			return SignIn{}, e
		}
		changed, e := result.RowsAffected()
		if e != nil {
			return SignIn{}, e
		}
		if changed != 1 {
			r, e = h.read(ctx, r.owner, r.session, r.id)
			if e != nil || !h.matchesFingerprint(r, hash[:]) {
				return SignIn{}, ErrDenied
			}
		}
	}
	reg, err := h.config.Accounts.chatgpt.ImportTransfer(ctx, r.owner, transfer)
	if err != nil || reg.State != "ready" || reg.Revision < 1 {
		return SignIn{}, ErrDenied
	}
	if h.config.CheckSession(ctx, r.owner, r.session) != nil {
		_ = h.fenceImported(r.owner, "", reg.ID)
		return SignIn{}, ErrDenied
	}
	current, e := h.config.Accounts.chatgpt.Registration(ctx, r.owner, reg.ID)
	if e != nil || current != reg || current.State != "ready" {
		return SignIn{}, ErrDenied
	}
	m, err := h.config.Connections.CreateAccountRequest(ctx, r.owner, connections.Definition{Label: r.label, Provider: "openai", AuthKind: "openai_oauth"}, connections.AccountReference{ID: reg.ID, Revision: reg.Revision}, r.id)
	if err != nil {
		return SignIn{}, err
	}
	if h.config.CheckSession(ctx, r.owner, r.session) != nil {
		_ = h.fenceImported(r.owner, m.ID, reg.ID)
		return SignIn{}, ErrDenied
	}
	if _, err = h.config.Accounts.db.ExecContext(ctx, `UPDATE central_chatgpt_handoffs SET state='complete',connection_id=? WHERE id=?`, m.ID, r.id); err != nil {
		return SignIn{}, err
	}
	r.state = "complete"
	r.connection = m.ID
	if h.config.CheckSession(ctx, r.owner, r.session) != nil {
		_ = h.fenceImported(r.owner, m.ID, reg.ID)
		return SignIn{}, ErrDenied
	}
	if !h.usableCompleted(ctx, r) {
		return SignIn{}, ErrDenied
	}
	return h.view(ctx, r, false)
}

func (h *Handoffs) lockAttempt(id string) func() {
	sum := sha256.Sum256([]byte(id))
	m := &h.locks[int(sum[0])%len(h.locks)]
	m.Lock()
	return m.Unlock
}

func (h *Handoffs) sealFingerprint(r handoffRecord, digest []byte) ([]byte, error) {
	nonce := make([]byte, h.config.Accounts.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	return h.config.Accounts.aead.Seal(nonce, nonce, digest, append(handoffAAD(r.owner, r.id), []byte("\x00transfer")...)), nil
}
func (h *Handoffs) matchesFingerprint(r handoffRecord, digest []byte) bool {
	n := h.config.Accounts.aead.NonceSize()
	if len(r.transferHash) < n {
		return false
	}
	plain, err := h.config.Accounts.aead.Open(nil, r.transferHash[:n], r.transferHash[n:], append(handoffAAD(r.owner, r.id), []byte("\x00transfer")...))
	if err != nil {
		return false
	}
	defer clear(plain)
	return subtle.ConstantTimeCompare(plain, digest) == 1
}
func (h *Handoffs) usableCompleted(ctx context.Context, r handoffRecord) bool {
	m, ref, err := h.config.Connections.ResolveAccount(ctx, r.owner, r.connection, 1)
	if err != nil || m.Definition.AuthKind != "openai_oauth" || m.Definition.Provider != "openai" {
		return false
	}
	reg, err := h.config.Accounts.chatgpt.Registration(ctx, r.owner, ref.ID)
	return err == nil && reg.State == "ready" && reg.Revision == ref.Revision
}
func (h *Handoffs) fenceImported(owner, connection, registration string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var failures []error
	if connection != "" {
		failures = append(failures, h.config.Connections.Disable(ctx, owner, connection))
		failures = append(failures, h.config.Registry.RevokeConnection(ctx, connection, 0))
	}
	_, err := h.config.Accounts.chatgpt.Disconnect(ctx, owner, registration)
	failures = append(failures, err)
	return errors.Join(failures...)
}
