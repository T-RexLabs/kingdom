// Package localowner owns explicitly supplied OS-local model credentials. It
// never reads provider account directories or grants central membership.
package localowner

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"kingdom/internal/central/modelproxy"
	"kingdom/internal/execution/access"
	"net/url"
	"strings"
)

type Model struct {
	Name    string   `json:"name,omitempty"`
	ID      string   `json:"id"`
	Efforts []string `json:"efforts"`
}
type Definition struct {
	Name     string  `json:"name,omitempty"`
	Provider string  `json:"provider"`
	Endpoint string  `json:"endpoint"`
	Models   []Model `json:"models"`
}
type Store struct {
	db             *sql.DB
	aead           cipher.AEAD
	owner, project string
}

func validID(s string) bool {
	id, e := uuid.Parse(s)
	return e == nil && id != uuid.Nil && id.String() == s
}
func New(ctx context.Context, db *sql.DB, key []byte, owner, project string) (*Store, error) {
	if db == nil || len(key) != 32 || !validID(owner) || !validID(project) {
		return nil, modelproxy.ErrInvalid
	}
	block, e := aes.NewCipher(key)
	if e != nil {
		return nil, e
	}
	aead, e := cipher.NewGCM(block)
	if e != nil {
		return nil, e
	}
	tx, e := db.BeginTx(ctx, nil)
	if e != nil {
		return nil, e
	}
	defer tx.Rollback()
	_, e = tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS local_model_owner(slot INTEGER PRIMARY KEY CHECK(slot=1),owner TEXT NOT NULL);
 CREATE TABLE IF NOT EXISTS local_model_revisions(connection_id TEXT NOT NULL,revision INTEGER NOT NULL,definition TEXT NOT NULL,disabled INTEGER NOT NULL,PRIMARY KEY(connection_id,revision));
 CREATE TABLE IF NOT EXISTS local_model_secrets(connection_id TEXT NOT NULL,revision INTEGER NOT NULL,nonce BLOB NOT NULL,ciphertext BLOB NOT NULL,PRIMARY KEY(connection_id,revision),FOREIGN KEY(connection_id,revision) REFERENCES local_model_revisions);
 CREATE TRIGGER IF NOT EXISTS local_model_owner_immutable BEFORE UPDATE ON local_model_owner BEGIN SELECT RAISE(ABORT,'local owner is immutable'); END;
 CREATE TRIGGER IF NOT EXISTS local_model_owner_retained BEFORE DELETE ON local_model_owner BEGIN SELECT RAISE(ABORT,'local owner is retained'); END;
 CREATE TRIGGER IF NOT EXISTS local_model_revision_immutable BEFORE UPDATE ON local_model_revisions BEGIN SELECT RAISE(ABORT,'local revision is immutable'); END;
 CREATE TRIGGER IF NOT EXISTS local_model_revision_retained BEFORE DELETE ON local_model_revisions BEGIN SELECT RAISE(ABORT,'local revision is retained'); END;
 CREATE TRIGGER IF NOT EXISTS local_model_secret_immutable BEFORE UPDATE ON local_model_secrets BEGIN SELECT RAISE(ABORT,'local secret is immutable'); END;
 CREATE TRIGGER IF NOT EXISTS local_model_secret_retained BEFORE DELETE ON local_model_secrets BEGIN SELECT RAISE(ABORT,'local secret is retained'); END;
 INSERT OR IGNORE INTO local_model_owner VALUES(1,?);`, owner)
	if e != nil {
		return nil, e
	}
	var saved string
	if e = tx.QueryRowContext(ctx, `SELECT owner FROM local_model_owner WHERE slot=1`).Scan(&saved); e != nil || saved != owner {
		return nil, modelproxy.ErrDenied
	}
	if e = tx.Commit(); e != nil {
		return nil, e
	}
	return &Store{db, aead, owner, project}, nil
}

// Validate checks model metadata without reading or changing credential storage.
func (d Definition) Validate() error { return validate(d) }

func validate(d Definition) error {
	u, e := url.Parse(d.Endpoint)
	if e != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.Opaque != "" || u.RawPath != "" || !strings.HasSuffix(u.Path, "/v1") {
		return modelproxy.ErrInvalid
	}
	if len(d.Models) == 0 || len(d.Models) > 512 {
		return modelproxy.ErrInvalid
	}
	seen := map[string]bool{}
	for _, m := range d.Models {
		sample := access.Snapshot{Version: 2, Domain: access.DomainLocalOwner, ActorID: "00000000-0000-4000-8000-000000000001", ProjectID: "00000000-0000-4000-8000-000000000002", Selection: access.Selection{Model: d.Provider + "/" + m.ID, Connection: access.Connection{ID: "00000000-0000-4000-8000-000000000003", Revision: 1, OwnerID: "00000000-0000-4000-8000-000000000001", Scope: "local_owner", Provider: d.Provider}}}
		if seen[m.ID] || sample.Validate() != nil || len(m.Efforts) > 32 {
			return modelproxy.ErrInvalid
		}
		seen[m.ID] = true
		efforts := map[string]bool{}
		for _, v := range m.Efforts {
			sample.Selection.ThinkingEffort = v
			if v == "" || efforts[v] || sample.Validate() != nil {
				return modelproxy.ErrInvalid
			}
			efforts[v] = true
		}
	}
	return nil
}
func (s *Store) aad(id string, revision int64) []byte {
	return []byte(fmt.Sprintf("kingdom.local-model.v1\x00%s\x00%s\x00%d", s.owner, id, revision))
}

// Put creates a new immutable revision only when metadata, enabled state, or
// secret changes. The caller supplies credentials explicitly; no discovery runs.
func (s *Store) Put(ctx context.Context, id string, d Definition, key []byte, disabled bool) (int64, error) {
	if !validID(id) || validate(d) != nil {
		return 0, modelproxy.ErrInvalid
	}
	if _, e := modelproxy.NewBearerCredential(string(key)); e != nil {
		return 0, e
	}
	raw, e := json.Marshal(d)
	if e != nil {
		return 0, e
	}
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return 0, e
	}
	defer tx.Rollback()
	var revision int64
	var previous string
	var off bool
	var nonce, ciphertext []byte
	e = tx.QueryRowContext(ctx, `SELECT r.revision,r.definition,r.disabled,s.nonce,s.ciphertext FROM local_model_revisions r JOIN local_model_secrets s USING(connection_id,revision) WHERE r.connection_id=? ORDER BY r.revision DESC LIMIT 1`, id).Scan(&revision, &previous, &off, &nonce, &ciphertext)
	if e != nil && e != sql.ErrNoRows {
		return 0, e
	}
	if e == nil {
		old, e := s.aead.Open(nil, nonce, ciphertext, s.aad(id, revision))
		if e != nil {
			return 0, modelproxy.ErrDenied
		}
		same := bytes.Equal(old, key)
		clear(old)
		if previous == string(raw) && off == disabled && same {
			return revision, nil
		}
	}
	revision++
	nonce = make([]byte, s.aead.NonceSize())
	if _, e = rand.Read(nonce); e != nil {
		return 0, e
	}
	ciphertext = s.aead.Seal(nil, nonce, key, s.aad(id, revision))
	if _, e = tx.ExecContext(ctx, `INSERT INTO local_model_revisions VALUES(?,?,?,?)`, id, revision, string(raw), disabled); e != nil {
		return 0, e
	}
	if _, e = tx.ExecContext(ctx, `INSERT INTO local_model_secrets VALUES(?,?,?,?)`, id, revision, nonce, ciphertext); e != nil {
		return 0, e
	}
	if e = tx.Commit(); e != nil {
		return 0, e
	}
	return revision, nil
}
func (s *Store) metadata(ctx context.Context, id string) (Definition, int64, error) {
	var d Definition
	var raw string
	var rev int64
	var disabled bool
	e := s.db.QueryRowContext(ctx, `SELECT definition,revision,disabled FROM local_model_revisions WHERE connection_id=? ORDER BY revision DESC LIMIT 1`, id).Scan(&raw, &rev, &disabled)
	if e != nil || disabled || len(raw) > 256<<10 || json.Unmarshal([]byte(raw), &d) != nil || validate(d) != nil {
		return d, 0, modelproxy.ErrDenied
	}
	return d, rev, nil
}
func supports(d Definition, model, effort string) bool {
	for _, m := range d.Models {
		if model == d.Provider+"/"+m.ID {
			if effort == "" {
				return true
			}
			for _, v := range m.Efforts {
				if v == effort {
					return true
				}
			}
		}
	}
	return false
}

// Selection returns only metadata for a configured connection and exact model.
func (s *Store) Selection(ctx context.Context, id, model, effort string) (access.Selection, error) {
	d, rev, e := s.metadata(ctx, id)
	if e != nil || !supports(d, model, effort) {
		return access.Selection{}, modelproxy.ErrDenied
	}
	return access.Selection{Model: model, ThinkingEffort: effort, Connection: access.Connection{ID: id, Revision: rev, OwnerID: s.owner, Scope: "local_owner", Provider: d.Provider}}, nil
}
func (s *Store) route(ctx context.Context, a access.Snapshot) (modelproxy.Route, error) {
	if a.Validate() != nil || a.AuthorityDomain() != access.DomainLocalOwner || a.ActorID != s.owner || a.ProjectID != s.project {
		return modelproxy.Route{}, modelproxy.ErrDenied
	}
	d, rev, e := s.metadata(ctx, a.Selection.Connection.ID)
	if e != nil || rev != a.Selection.Connection.Revision || d.Provider != a.Selection.Connection.Provider || !supports(d, a.Selection.Model, a.Selection.ThinkingEffort) {
		return modelproxy.Route{}, modelproxy.ErrDenied
	}
	return modelproxy.Route{Protocol: "openai", Endpoint: d.Endpoint, Model: strings.TrimPrefix(a.Selection.Model, d.Provider+"/")}, nil
}
func (s *Store) RequireLocalRevision(ctx context.Context, a access.Snapshot) error {
	_, e := s.route(ctx, a)
	return e
}
func (s *Store) Pin(ctx context.Context, b modelproxy.Binding) (modelproxy.Route, error) {
	return s.route(ctx, b.Access)
}
func (s *Store) Resolve(ctx context.Context, b modelproxy.Binding, expected modelproxy.Route) (modelproxy.Credential, error) {
	route, e := s.route(ctx, b.Access)
	if e != nil || route != expected {
		return modelproxy.Credential{}, modelproxy.ErrDenied
	}
	c := b.Access.Selection.Connection
	var nonce, ciphertext []byte
	if e = s.db.QueryRowContext(ctx, `SELECT nonce,ciphertext FROM local_model_secrets WHERE connection_id=? AND revision=?`, c.ID, c.Revision).Scan(&nonce, &ciphertext); e != nil {
		return modelproxy.Credential{}, modelproxy.ErrDenied
	}
	raw, e := s.aead.Open(nil, nonce, ciphertext, s.aad(c.ID, c.Revision))
	if e != nil {
		return modelproxy.Credential{}, modelproxy.ErrDenied
	}
	defer clear(raw)
	if _, e = s.route(ctx, b.Access); e != nil {
		return modelproxy.Credential{}, modelproxy.ErrDenied
	}
	return modelproxy.NewBearerCredential(string(raw))
}
