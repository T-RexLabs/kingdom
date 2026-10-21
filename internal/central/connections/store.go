// Package connections stores hosted model credentials independently of the host
// account store. Only trusted worker code may use Resolve; HTTP APIs must expose
// metadata only. A revision identifies immutable credentials and configuration.
package connections

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"
	"kingdom/internal/central/catalog"
)

var (
	ErrDenied      = errors.New("connection access denied")
	ErrConflict    = errors.New("connection revision conflict")
	ErrInvalid     = errors.New("invalid connection")
	ErrUnavailable = errors.New("connection credential unavailable")
)

type Scope string

const (
	Personal Scope = "personal"
	Team     Scope = "team"
)

// Definition contains display and provider settings, never authentication data.
// Endpoint is a routing choice, not authorization to contact a network address.
// The worker must enforce the operator's endpoint policy before making requests.
type Definition struct {
	Label    string `json:"label"`
	Provider string `json:"provider"`
	Endpoint string `json:"endpoint,omitempty"`
	AuthKind string `json:"auth_kind"`
}

type Metadata struct {
	ID         string     `json:"id"`
	OwnerID    string     `json:"owner_id"`
	Scope      Scope      `json:"scope"`
	Revision   int64      `json:"revision"`
	Definition Definition `json:"definition"`
	Disabled   bool       `json:"disabled"`
}

// Secret has no JSON representation: encoding an enclosing value cannot leak
// credentials. Secret bytes may be used only by the owned provider invocation.
type Secret struct{ value []byte }

func (s Secret) Bytes() []byte  { return append([]byte(nil), s.value...) }
func (Secret) String() string   { return "[credential]" }
func (Secret) GoString() string { return "[credential]" }
func (Secret) MarshalJSON() ([]byte, error) {
	return nil, errors.New("credentials cannot be serialized")
}

type Store struct {
	db   *sql.DB
	aead cipher.AEAD
}

// New requires an operator-managed 256-bit key. Losing the key makes stored
// credentials unreadable. Passing the wrong key never replaces stored data.
func New(ctx context.Context, db *sql.DB, key []byte) (*Store, error) {
	if db == nil || len(key) != 32 {
		return nil, ErrInvalid
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS central_connection_key (slot INTEGER PRIMARY KEY CHECK(slot=1), proof BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS central_connections (
 id TEXT PRIMARY KEY, owner_id TEXT NOT NULL REFERENCES central_users(id),
 scope TEXT NOT NULL CHECK(scope IN ('personal','team')),
 current_revision INTEGER NOT NULL CHECK(current_revision>0),
 disabled INTEGER NOT NULL DEFAULT 0 CHECK(disabled IN (0,1))
);
CREATE TABLE IF NOT EXISTS central_connection_revisions (
 connection_id TEXT NOT NULL REFERENCES central_connections(id),
 revision INTEGER NOT NULL CHECK(revision>0), definition TEXT NOT NULL,
 credential BLOB NOT NULL, created_at INTEGER NOT NULL,
 PRIMARY KEY(connection_id,revision)
);
CREATE TRIGGER IF NOT EXISTS central_connection_revision_immutable BEFORE UPDATE ON central_connection_revisions BEGIN SELECT RAISE(ABORT,'connection revisions are immutable'); END;
CREATE TRIGGER IF NOT EXISTS central_connection_revision_retained BEFORE DELETE ON central_connection_revisions BEGIN SELECT RAISE(ABORT,'connection revisions must be retained'); END;
CREATE TRIGGER IF NOT EXISTS central_connection_owner_immutable BEFORE UPDATE OF owner_id,scope ON central_connections BEGIN SELECT RAISE(ABORT,'connection owner is immutable'); END;
`)
	if err != nil {
		return nil, fmt.Errorf("initialize connection storage: %w", err)
	}
	// Check the key before accepting writes, including when opening an existing
	// catalog with a missing or incorrectly mounted operator key.
	if _, err := tx.ExecContext(ctx, `UPDATE central_connection_key SET proof=proof WHERE 0`); err != nil {
		return nil, err
	}
	var proof []byte
	err = tx.QueryRowContext(ctx, `SELECT proof FROM central_connection_key WHERE slot=1`).Scan(&proof)
	const keyDomain = "kingdom-central-connection-key/v1"
	if errors.Is(err, sql.ErrNoRows) {
		var count int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM central_connection_revisions`).Scan(&count); err != nil {
			return nil, err
		}
		if count != 0 {
			return nil, ErrUnavailable
		}
		nonce := make([]byte, aead.NonceSize())
		if _, err := rand.Read(nonce); err != nil {
			return nil, err
		}
		proof = aead.Seal(nonce, nonce, []byte(keyDomain), []byte(keyDomain))
		if _, err := tx.ExecContext(ctx, `INSERT INTO central_connection_key(slot,proof) VALUES(1,?)`, proof); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	} else {
		if len(proof) < aead.NonceSize() {
			return nil, ErrUnavailable
		}
		plain, err := aead.Open(nil, proof[:aead.NonceSize()], proof[aead.NonceSize():], []byte(keyDomain))
		if err != nil || string(plain) != keyDomain {
			return nil, ErrUnavailable
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &Store{db: db, aead: aead}, nil
}

func validDefinition(d Definition) bool {
	for _, v := range []string{d.Label, d.Provider, d.AuthKind} {
		if v == "" || strings.TrimSpace(v) != v || len(v) > 256 || strings.ContainsAny(v, "\x00\r\n") {
			return false
		}
	}
	if d.AuthKind != "api_key" && d.AuthKind != "openai_oauth" && d.AuthKind != "copilot_oauth" {
		return false
	}
	for _, ch := range d.Provider {
		if !(ch >= 'a' && ch <= 'z' || ch >= '0' && ch <= '9' || ch == '-' || ch == '_' || ch == '.') {
			return false
		}
	}
	// Provider sign-in tokens are restricted to that provider's built-in route.
	// A custom endpoint must use its own explicit API-key connection.
	if d.AuthKind == "openai_oauth" && (d.Provider != "openai" || d.Endpoint != "") {
		return false
	}
	if d.AuthKind == "copilot_oauth" && (d.Provider != "github-copilot" || d.Endpoint != "") {
		return false
	}
	if len(d.Endpoint) > 2048 {
		return false
	}
	if d.Endpoint != "" {
		u, err := url.Parse(d.Endpoint)
		if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Opaque != "" {
			return false
		}
	}
	return true
}

// beginWrite obtains the SQLite writer reservation before checking authority so
// a concurrent disable or ownership change cannot race the mutation.
func (s *Store) beginWrite(ctx context.Context) (*sql.Tx, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE central_connections SET disabled=disabled WHERE 0`); err != nil {
		tx.Rollback()
		return nil, err
	}
	return tx, nil
}

func user(ctx context.Context, tx *sql.Tx, id string) (catalog.User, error) {
	u, err := catalog.LookupUser(ctx, tx, id)
	if err != nil {
		return catalog.User{}, ErrDenied
	}
	return u, nil
}

func allowed(u catalog.User, m Metadata, write bool) bool {
	if m.Scope == Personal {
		return m.OwnerID == u.ID
	}
	return m.Scope == Team && (!write || u.TeamOwner)
}

func (s *Store) seal(m Metadata, secret []byte) ([]byte, error) {
	if len(secret) == 0 || len(secret) > 1<<20 {
		return nil, ErrInvalid
	}
	additional, err := binding(m)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, s.aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}
	return s.aead.Seal(nonce, nonce, secret, additional), nil
}

func binding(m Metadata) ([]byte, error) {
	// Disabled is mutable policy, not part of an immutable credential revision.
	m.Disabled = false
	return json.Marshal(struct {
		Version  int      `json:"version"`
		Metadata Metadata `json:"connection"`
	}{1, m})
}

func (s *Store) Create(ctx context.Context, actor string, scope Scope, d Definition, credential []byte) (Metadata, error) {
	if (scope != Personal && scope != Team) || !validDefinition(d) {
		return Metadata{}, ErrInvalid
	}
	tx, err := s.beginWrite(ctx)
	if err != nil {
		return Metadata{}, err
	}
	defer tx.Rollback()
	u, err := user(ctx, tx, actor)
	if err != nil {
		return Metadata{}, err
	}
	if scope == Team && !u.TeamOwner {
		return Metadata{}, ErrDenied
	}
	m := Metadata{ID: uuid.NewString(), OwnerID: u.ID, Scope: scope, Revision: 1, Definition: d}
	encrypted, err := s.seal(m, credential)
	if err != nil {
		return Metadata{}, err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO central_connections(id,owner_id,scope,current_revision) VALUES(?,?,?,1)`, m.ID, m.OwnerID, m.Scope); err != nil {
		return Metadata{}, err
	}
	if err := insertRevision(ctx, tx, m, encrypted); err != nil {
		return Metadata{}, err
	}
	return m, tx.Commit()
}

func insertRevision(ctx context.Context, tx *sql.Tx, m Metadata, encrypted []byte) error {
	definition, err := json.Marshal(m.Definition)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO central_connection_revisions(connection_id,revision,definition,credential,created_at) VALUES(?,?,?,?,?)`, m.ID, m.Revision, string(definition), encrypted, time.Now().UTC().UnixMilli())
	return err
}

func read(ctx context.Context, tx *sql.Tx, id string, revision int64) (Metadata, []byte, error) {
	var m Metadata
	var definition string
	var encrypted []byte
	err := tx.QueryRowContext(ctx, `SELECT c.id,c.owner_id,c.scope,r.revision,r.definition,c.disabled,r.credential FROM central_connections c JOIN central_connection_revisions r ON r.connection_id=c.id AND r.revision=CASE WHEN ?=0 THEN c.current_revision ELSE ? END WHERE c.id=?`, revision, revision, id).Scan(&m.ID, &m.OwnerID, &m.Scope, &m.Revision, &definition, &m.Disabled, &encrypted)
	if errors.Is(err, sql.ErrNoRows) {
		return m, nil, ErrDenied
	}
	if err != nil {
		return m, nil, err
	}
	if err := json.Unmarshal([]byte(definition), &m.Definition); err != nil {
		return m, nil, ErrUnavailable
	}
	return m, encrypted, nil
}

// Revise creates an immutable successor and checks the caller's observed current
// revision. An in-flight invocation can continue to refer to its pinned revision.
func (s *Store) Revise(ctx context.Context, actor, id string, expected int64, d Definition, credential []byte) (Metadata, error) {
	if !validDefinition(d) || expected <= 0 || expected == 1<<63-1 {
		return Metadata{}, ErrInvalid
	}
	tx, err := s.beginWrite(ctx)
	if err != nil {
		return Metadata{}, err
	}
	defer tx.Rollback()
	u, err := user(ctx, tx, actor)
	if err != nil {
		return Metadata{}, err
	}
	m, _, err := read(ctx, tx, id, 0)
	if err != nil {
		return Metadata{}, err
	}
	if !allowed(u, m, true) {
		return Metadata{}, ErrDenied
	}
	if m.Revision != expected || m.Disabled {
		return Metadata{}, ErrConflict
	}
	m.Revision++
	m.Definition = d
	encrypted, err := s.seal(m, credential)
	if err != nil {
		return Metadata{}, err
	}
	if err := insertRevision(ctx, tx, m, encrypted); err != nil {
		return Metadata{}, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE central_connections SET current_revision=? WHERE id=?`, m.Revision, m.ID); err != nil {
		return Metadata{}, err
	}
	return m, tx.Commit()
}

// Disable prevents future credential resolution for every revision. Revocation
// of an already-running provider is the invocation owner's separate obligation.
func (s *Store) Disable(ctx context.Context, actor, id string) error {
	tx, err := s.beginWrite(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	u, err := user(ctx, tx, actor)
	if err != nil {
		return err
	}
	m, _, err := read(ctx, tx, id, 0)
	if err != nil {
		return err
	}
	if !allowed(u, m, true) {
		return ErrDenied
	}
	if _, err := tx.ExecContext(ctx, `UPDATE central_connections SET disabled=1 WHERE id=?`, id); err != nil {
		return err
	}
	return tx.Commit()
}

// Resolve requires an exact positive revision and the real invocation actor.
// Team ownership never authorizes access to another user's personal credentials.
func (s *Store) Resolve(ctx context.Context, actor, id string, revision int64) (Metadata, Secret, error) {
	if revision <= 0 {
		return Metadata{}, Secret{}, ErrInvalid
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return Metadata{}, Secret{}, err
	}
	defer tx.Rollback()
	u, err := user(ctx, tx, actor)
	if err != nil {
		return Metadata{}, Secret{}, err
	}
	m, encrypted, err := read(ctx, tx, id, revision)
	if err != nil {
		return Metadata{}, Secret{}, err
	}
	if !allowed(u, m, false) || m.Disabled {
		return Metadata{}, Secret{}, ErrDenied
	}
	additional, err := binding(m)
	if err != nil {
		return Metadata{}, Secret{}, err
	}
	if len(encrypted) < s.aead.NonceSize() {
		return Metadata{}, Secret{}, ErrUnavailable
	}
	secret, err := s.aead.Open(nil, encrypted[:s.aead.NonceSize()], encrypted[s.aead.NonceSize():], additional)
	if err != nil {
		return Metadata{}, Secret{}, ErrUnavailable
	}
	if err := tx.Commit(); err != nil {
		return Metadata{}, Secret{}, err
	}
	return m, Secret{value: secret}, nil
}

// List returns only visible current metadata, including disabled connections.
func (s *Store) List(ctx context.Context, actor string) ([]Metadata, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	u, err := user(ctx, tx, actor)
	if err != nil {
		return nil, err
	}
	rows, err := tx.QueryContext(ctx, `SELECT c.id,c.owner_id,c.scope,c.current_revision,r.definition,c.disabled FROM central_connections c JOIN central_connection_revisions r ON r.connection_id=c.id AND r.revision=c.current_revision WHERE c.owner_id=? OR c.scope='team' ORDER BY c.id`, u.ID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []Metadata{}
	for rows.Next() {
		var m Metadata
		var definition string
		if err := rows.Scan(&m.ID, &m.OwnerID, &m.Scope, &m.Revision, &definition, &m.Disabled); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(definition), &m.Definition); err != nil {
			return nil, ErrUnavailable
		}
		result = append(result, m)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, tx.Commit()
}
