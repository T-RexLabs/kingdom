// Package chatgpt owns Kingdom's locally registered ChatGPT plan credentials.
// It never reads another application's tokens or creates a team credential.
package chatgpt

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"hash/fnv"
	"strings"
	"sync"
)

var (
	ErrInvalid        = errors.New("invalid ChatGPT registration configuration")
	ErrDenied         = errors.New("ChatGPT registration access denied")
	ErrReauthenticate = errors.New("sign in to ChatGPT again")
	ErrUnavailable    = errors.New("ChatGPT authentication is unavailable")
)

const issuer = "https://auth.openai.com"
const resource = "https://api.openai.com/v1"
const scopes = "openid profile email offline_access resource.invoke chatgpt.tokens.use.direct"

type Registration struct {
	ID       string `json:"id"`
	ClientID string `json:"client_id"`
	Subject  string `json:"subject"`
	Email    string `json:"email,omitempty"`
	Revision int64  `json:"revision"`
	State    string `json:"state"`
}
type Token struct{ value string }

func (t Token) Bearer() string             { return t.value }
func (Token) String() string               { return "[ChatGPT credential]" }
func (Token) GoString() string             { return "[ChatGPT credential]" }
func (Token) MarshalJSON() ([]byte, error) { return nil, ErrDenied }

type credentials struct {
	Access   string `json:"access"`
	Refresh  string `json:"refresh"`
	ID       string `json:"id"`
	Scopes   string `json:"scopes"`
	Expires  int64  `json:"expires"`
	Earliest int64  `json:"earliest"`
}
type record struct {
	Registration
	owner     string
	version   int64
	encrypted []byte
}
type Vault struct {
	db            *sql.DB
	aead          cipher.AEAD
	host          string
	locks         [64]sync.Mutex
	operationLock func(context.Context, string) (func(), error)
}

func textID(s string) bool {
	return s != "" && len(s) <= 256 && strings.TrimSpace(s) == s && !strings.ContainsAny(s, "\x00\r\n")
}
func clientID(s string) bool {
	return strings.HasPrefix(s, "oaiapp_") && textID(s) && !strings.ContainsAny(s, " /?#&")
}

// NewVault uses caller-protected SQLite storage and a separately protected
// 256-bit key. Owner identity is supplied by the trusted local account boundary.
func NewVault(ctx context.Context, db *sql.DB, key []byte) (*Vault, error) {
	if db == nil || len(key) != 32 {
		return nil, ErrInvalid
	}
	block, e := aes.NewCipher(key)
	if e != nil {
		return nil, e
	}
	a, e := cipher.NewGCM(block)
	if e != nil {
		return nil, e
	}
	v := &Vault{db: db, aead: a}
	tx, e := db.BeginTx(ctx, nil)
	if e != nil {
		return nil, e
	}
	defer tx.Rollback()
	_, e = tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS chatgpt_vault (
 slot INTEGER PRIMARY KEY CHECK(slot=1), version INTEGER NOT NULL CHECK(version=1), host_id TEXT NOT NULL, key_proof BLOB NOT NULL);
 CREATE TABLE IF NOT EXISTS chatgpt_registrations (
 id TEXT PRIMARY KEY, owner_id TEXT NOT NULL, client_id TEXT NOT NULL DEFAULT '', subject TEXT NOT NULL DEFAULT '', email TEXT NOT NULL DEFAULT '',
 revision INTEGER NOT NULL DEFAULT 0, token_version INTEGER NOT NULL DEFAULT 0,
 state TEXT NOT NULL CHECK(state IN ('pending','exchanging','ready','refreshing','reauth','revoking','revoking_uncertain','signed_out','signed_out_uncertain')), credential BLOB,
 UNIQUE(owner_id,client_id,id));
 CREATE UNIQUE INDEX IF NOT EXISTS chatgpt_client_unique ON chatgpt_registrations(client_id) WHERE client_id<>'';
 CREATE TRIGGER IF NOT EXISTS chatgpt_owner_immutable BEFORE UPDATE OF id,owner_id ON chatgpt_registrations BEGIN SELECT RAISE(ABORT,'registration owner is immutable'); END;
 CREATE TRIGGER IF NOT EXISTS chatgpt_client_immutable BEFORE UPDATE OF client_id ON chatgpt_registrations WHEN OLD.client_id<>'' AND NEW.client_id<>OLD.client_id BEGIN SELECT RAISE(ABORT,'registration client is immutable'); END;
 CREATE TRIGGER IF NOT EXISTS chatgpt_subject_immutable BEFORE UPDATE OF subject ON chatgpt_registrations WHEN OLD.subject<>'' AND NEW.subject<>OLD.subject BEGIN SELECT RAISE(ABORT,'registration subject is immutable'); END;
 CREATE TRIGGER IF NOT EXISTS chatgpt_registration_retained BEFORE DELETE ON chatgpt_registrations BEGIN SELECT RAISE(ABORT,'registration mapping is retained'); END;`)
	if e != nil {
		return nil, e
	}
	// Acquire a writer lock before inspecting or creating the one host identity.
	if _, e = tx.ExecContext(ctx, `UPDATE chatgpt_vault SET version=version WHERE slot=1`); e != nil {
		return nil, e
	}
	var proof []byte
	var version int
	e = tx.QueryRowContext(ctx, `SELECT version,host_id,key_proof FROM chatgpt_vault WHERE slot=1`).Scan(&version, &v.host, &proof)
	if errors.Is(e, sql.ErrNoRows) {
		v.host = "urn:uuid:" + uuid.NewString()
		proof, e = v.seal([]byte("kingdom-chatgpt-key/v1"), v.host)
		if e != nil {
			return nil, e
		}
		_, e = tx.ExecContext(ctx, `INSERT INTO chatgpt_vault VALUES(1,1,?,?)`, v.host, proof)
	} else if e == nil {
		plain, err := v.open(proof, v.host)
		if err != nil || string(plain) != "kingdom-chatgpt-key/v1" || version != 1 || !strings.HasPrefix(v.host, "urn:uuid:") {
			return nil, ErrDenied
		}
	}
	if e != nil {
		return nil, e
	}
	if e = tx.Commit(); e != nil {
		return nil, e
	}
	return v, nil
}
func (v *Vault) HostID() string { return v.host }
func (v *Vault) lock(ctx context.Context, id string) (func(), error) {
	h := fnv.New64a()
	_, _ = h.Write([]byte(id))
	m := &v.locks[h.Sum64()%uint64(len(v.locks))]
	m.Lock()
	if v.operationLock != nil {
		unlock, e := v.operationLock(ctx, id)
		if e != nil {
			m.Unlock()
			return nil, e
		}
		return func() { unlock(); m.Unlock() }, nil
	}
	return m.Unlock, nil
}
func (v *Vault) seal(b []byte, aad string) ([]byte, error) {
	n := make([]byte, v.aead.NonceSize())
	if _, e := rand.Read(n); e != nil {
		return nil, e
	}
	return v.aead.Seal(n, n, b, []byte(aad)), nil
}
func (v *Vault) open(b []byte, aad string) ([]byte, error) {
	n := v.aead.NonceSize()
	if len(b) < n || len(b) > 128<<10 {
		return nil, ErrDenied
	}
	p, e := v.aead.Open(nil, b[:n], b[n:], []byte(aad))
	if e != nil {
		return nil, ErrDenied
	}
	return p, nil
}
func (v *Vault) aad(r record) string {
	return fmt.Sprintf("kingdom-chatgpt-tokens/v1\x00%s\x00%s\x00%s\x00%s\x00%s\x00%d\x00%d", v.host, r.owner, r.ID, r.ClientID, r.Subject, r.Revision, r.version)
}
func (v *Vault) read(ctx context.Context, owner, id string) (record, error) {
	var r record
	if !textID(owner) || !textID(id) {
		return r, ErrDenied
	}
	e := v.db.QueryRowContext(ctx, `SELECT id,owner_id,client_id,subject,email,revision,token_version,state,credential FROM chatgpt_registrations WHERE id=? AND owner_id=?`, id, owner).Scan(&r.ID, &r.owner, &r.ClientID, &r.Subject, &r.Email, &r.Revision, &r.version, &r.State, &r.encrypted)
	if e != nil {
		return r, ErrDenied
	}
	return r, nil
}
func (v *Vault) Get(ctx context.Context, owner, id string) (Registration, error) {
	r, e := v.read(ctx, owner, id)
	return r.Registration, e
}
func (v *Vault) newRegistration(ctx context.Context, owner string) (record, error) {
	if !textID(owner) {
		return record{}, ErrDenied
	}
	r := record{Registration: Registration{ID: uuid.NewString(), State: "pending"}, owner: owner}
	_, e := v.db.ExecContext(ctx, `INSERT INTO chatgpt_registrations(id,owner_id,state) VALUES(?,?,'pending')`, r.ID, owner)
	return r, e
}
func (v *Vault) retainClient(ctx context.Context, r record, client string) error {
	if !clientID(client) || r.ClientID != "" && r.ClientID != client {
		return ErrDenied
	}
	result, e := v.db.ExecContext(ctx, `UPDATE chatgpt_registrations SET client_id=? WHERE id=? AND owner_id=? AND revision=? AND (client_id='' OR client_id=?)`, client, r.ID, r.owner, r.Revision, client)
	if e != nil {
		return ErrDenied
	}
	n, _ := result.RowsAffected()
	if n != 1 {
		return ErrDenied
	}
	return nil
}
func (v *Vault) decrypt(r record) (credentials, error) {
	var c credentials
	b, e := v.open(r.encrypted, v.aad(r))
	if e != nil {
		return c, e
	}
	if json.Unmarshal(b, &c) != nil {
		return c, ErrDenied
	}
	return c, nil
}
func (v *Vault) save(ctx context.Context, old record, next record, c credentials) error {
	b, e := json.Marshal(c)
	if e != nil {
		return ErrDenied
	}
	encrypted, e := v.seal(b, v.aad(next))
	if e != nil {
		return e
	}
	res, e := v.db.ExecContext(ctx, `UPDATE chatgpt_registrations SET subject=?,email=?,revision=?,token_version=?,state='ready',credential=? WHERE id=? AND owner_id=? AND client_id=? AND revision=? AND token_version=? AND state=?`, next.Subject, next.Email, next.Revision, next.version, encrypted, old.ID, old.owner, next.ClientID, old.Revision, old.version, old.State)
	if e != nil {
		return ErrDenied
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return ErrDenied
	}
	return nil
}
func (v *Vault) transition(ctx context.Context, r record, state string, clear bool) error {
	query := `UPDATE chatgpt_registrations SET state=? WHERE id=? AND owner_id=? AND revision=? AND token_version=? AND state=?`
	if clear {
		query = `UPDATE chatgpt_registrations SET state=?,credential=NULL WHERE id=? AND owner_id=? AND revision=? AND token_version=? AND state=?`
	}
	result, e := v.db.ExecContext(ctx, query, state, r.ID, r.owner, r.Revision, r.version, r.State)
	if e != nil {
		return e
	}
	n, _ := result.RowsAffected()
	if n != 1 {
		return ErrDenied
	}
	return nil
}
func hasScope(s, want string) bool {
	for _, x := range strings.Fields(s) {
		if x == want {
			return true
		}
	}
	return false
}
func validToken(s string) bool {
	if len(s) == 0 || len(s) > 16384 {
		return false
	}
	for _, c := range s {
		if c <= 32 || c >= 127 {
			return false
		}
	}
	return true
}
