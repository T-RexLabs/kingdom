package chatgpt

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/coreos/go-oidc/v3/oidc"
	"github.com/google/uuid"
)

// Transfer is a selected credential session for an explicitly trusted channel.
// Ordinary serialization and formatting cannot reveal its credentials.
type Transfer struct{ payload transferPayload }

func (Transfer) String() string               { return "[ChatGPT session transfer]" }
func (Transfer) GoString() string             { return "[ChatGPT session transfer]" }
func (Transfer) MarshalJSON() ([]byte, error) { return nil, ErrDenied }
func (*Transfer) UnmarshalJSON([]byte) error  { return ErrDenied }

type transferPayload struct {
	Version    int    `json:"version"`
	ID         string `json:"transfer_id"`
	TargetHost string `json:"target_host_id"`
	ClientID   string `json:"client_id"`
	Subject    string `json:"subject"`
	Email      string `json:"email,omitempty"`
	Access     string `json:"access_token"`
	Refresh    string `json:"refresh_token"`
	IDToken    string `json:"id_token"`
	Scopes     string `json:"scope"`
	Expires    int64  `json:"expires_at"`
	Earliest   int64  `json:"earliest_refresh_at"`
}

func canonicalUUID(raw string) bool {
	id, err := uuid.Parse(raw)
	return err == nil && id != uuid.Nil && id.String() == raw
}
func transferHost(raw string) bool {
	return strings.HasPrefix(raw, "urn:uuid:") && canonicalUUID(strings.TrimPrefix(raw, "urn:uuid:"))
}
func (p transferPayload) valid() bool {
	return p.Version == 1 && canonicalUUID(p.ID) && transferHost(p.TargetHost) && clientID(p.ClientID) && textID(p.Subject) && len(p.Email) <= 320 && !strings.ContainsAny(p.Email, "\x00\r\n") && validToken(p.Access) && validToken(p.Refresh) && validToken(p.IDToken) && len(p.Scopes) <= 4096 && !strings.ContainsAny(p.Scopes, "\x00\r\n") && hasScope(p.Scopes, "chatgpt.tokens.use.direct") && hasScope(p.Scopes, "resource.invoke") && hasScope(p.Scopes, "offline_access") && p.Expires > 0 && p.Earliest >= 0 && p.Earliest <= p.Expires
}

// EncodeTransfer is the only supported credential export representation. The
// caller must send these bytes through the selected protected pairing channel.
func EncodeTransfer(t Transfer) ([]byte, error) {
	if !t.payload.valid() {
		return nil, ErrDenied
	}
	return json.Marshal(t.payload)
}

// DecodeTransfer accepts one bounded object with no duplicate or unknown fields.
// Decode alone grants no authority; ImportTransfer verifies the target and ID.
func DecodeTransfer(raw []byte) (Transfer, error) {
	if len(raw) == 0 || len(raw) > 64<<10 {
		return Transfer{}, ErrInvalid
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	start, err := d.Token()
	if err != nil || start != json.Delim('{') {
		return Transfer{}, ErrInvalid
	}
	seen := map[string]bool{}
	for d.More() {
		token, err := d.Token()
		name, ok := token.(string)
		if err != nil || !ok || seen[name] {
			return Transfer{}, ErrInvalid
		}
		seen[name] = true
		var field json.RawMessage
		if d.Decode(&field) != nil {
			return Transfer{}, ErrInvalid
		}
	}
	if end, err := d.Token(); err != nil || end != json.Delim('}') {
		return Transfer{}, ErrInvalid
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return Transfer{}, ErrInvalid
	}
	d = json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	var p transferPayload
	if d.Decode(&p) != nil || !p.valid() {
		return Transfer{}, ErrInvalid
	}
	return Transfer{p}, nil
}

func (s *Service) verifyTransfer(ctx context.Context, p transferPayload) error {
	now := s.now().Unix()
	if !p.valid() || p.TargetHost != s.vault.HostID() || p.Expires <= now+30 || p.Expires > now+3600 {
		return ErrDenied
	}
	return s.verifyTransferIdentity(ctx, p)
}
func (s *Service) verifyTransferIdentity(ctx context.Context, p transferPayload) error {
	if err := s.discover(ctx); err != nil {
		return err
	}
	id, err := s.provider.Verifier(&oidc.Config{ClientID: p.ClientID, Now: s.now}).Verify(oidc.ClientContext(ctx, s.client), p.IDToken)
	if err != nil || id.Subject != p.Subject {
		return ErrDenied
	}
	var claims struct {
		Email string `json:"email"`
	}
	if id.Claims(&claims) != nil || claims.Email != p.Email {
		return ErrDenied
	}
	return nil
}
func exportAAD(v *Vault, owner, id string, revision int64, target string) string {
	return fmt.Sprintf("kingdom-chatgpt-export/v1\x00%s\x00%s\x00%s\x00%d\x00%s", v.host, owner, id, revision, target)
}
func (v *Vault) importFingerprint(owner string, encoded []byte) []byte {
	mac := hmac.New(sha256.New, v.transferKey)
	_, _ = mac.Write([]byte(v.host + "\x00" + owner + "\x00"))
	_, _ = mac.Write(encoded)
	return mac.Sum(nil)
}
func (v *Vault) exported(ctx context.Context, r record) (bool, error) {
	var count int
	err := v.db.QueryRowContext(ctx, `SELECT count(*) FROM chatgpt_exports WHERE registration_id=? AND revision=?`, r.ID, r.Revision).Scan(&count)
	return count != 0, err
}

// ExportForTransfer permanently fences the selected local authorization before
// returning credentials. Retries recover the exact encrypted payload for that
// destination. A new local sign-in creates a distinct authorization revision.
func (s *Service) ExportForTransfer(ctx context.Context, owner, id, targetHost string) (Transfer, error) {
	ctx, done, err := s.enter(ctx)
	if err != nil {
		return Transfer{}, err
	}
	defer done()
	if !transferHost(targetHost) || targetHost == s.vault.HostID() {
		return Transfer{}, ErrInvalid
	}
	// Renew while this host still owns refresh. An existing export is recovered
	// below even though Access correctly refuses its already fenced session.
	_, _, accessErr := s.Access(ctx, owner, id)
	unlock, err := s.vault.lock(ctx, id)
	if err != nil {
		return Transfer{}, err
	}
	defer unlock()
	r, err := s.vault.read(ctx, owner, id)
	if err != nil {
		return Transfer{}, err
	}
	var target, state string
	var encrypted []byte
	err = s.vault.db.QueryRowContext(ctx, `SELECT target_host,state,payload FROM chatgpt_exports WHERE registration_id=? AND revision=?`, id, r.Revision).Scan(&target, &state, &encrypted)
	if err == nil {
		if target != targetHost || state != "pending" {
			return Transfer{}, ErrDenied
		}
		raw, err := s.vault.open(encrypted, exportAAD(s.vault, owner, id, r.Revision, targetHost))
		if err != nil {
			return Transfer{}, ErrDenied
		}
		defer clear(raw)
		return DecodeTransfer(raw)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return Transfer{}, ErrUnavailable
	}
	if accessErr != nil || r.State != "ready" {
		return Transfer{}, ErrReauthenticate
	}
	c, err := s.vault.decrypt(r)
	if err != nil {
		return Transfer{}, err
	}
	p := transferPayload{Version: 1, ID: uuid.NewString(), TargetHost: targetHost, ClientID: r.ClientID, Subject: r.Subject, Email: r.Email, Access: c.Access, Refresh: c.Refresh, IDToken: c.ID, Scopes: c.Scopes, Expires: c.Expires, Earliest: c.Earliest}
	if !p.valid() || p.Expires <= s.now().Unix()+30 || p.Expires > s.now().Unix()+3600 {
		return Transfer{}, ErrReauthenticate
	}
	if err = s.verifyTransferIdentity(ctx, p); err != nil {
		return Transfer{}, err
	}
	raw, err := EncodeTransfer(Transfer{p})
	if err != nil {
		return Transfer{}, err
	}
	defer clear(raw)
	encrypted, err = s.vault.seal(raw, exportAAD(s.vault, owner, id, r.Revision, targetHost))
	if err != nil {
		return Transfer{}, err
	}
	tx, err := s.vault.db.BeginTx(ctx, nil)
	if err != nil {
		return Transfer{}, err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `UPDATE chatgpt_registrations SET state='reauth' WHERE id=? AND owner_id=? AND revision=? AND token_version=? AND state='ready'`, id, owner, r.Revision, r.version)
	if err != nil {
		return Transfer{}, ErrDenied
	}
	if n, err := result.RowsAffected(); err != nil || n != 1 {
		return Transfer{}, ErrDenied
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO chatgpt_exports(registration_id,revision,transfer_id,target_host,state,payload) VALUES(?,?,?,?,'pending',?)`, id, r.Revision, p.ID, targetHost, encrypted); err != nil {
		return Transfer{}, ErrDenied
	}
	if err = tx.Commit(); err != nil {
		return Transfer{}, err
	}
	return Transfer{p}, nil
}

type importReader interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func (s *Service) replayImport(ctx context.Context, reader importReader, owner string, p transferPayload, encoded []byte) (Registration, bool, error) {
	var savedOwner string
	var fingerprint []byte
	var r Registration
	err := reader.QueryRowContext(ctx, `SELECT i.owner_id,i.fingerprint,r.id,r.client_id,r.subject,r.email,r.revision,r.state FROM chatgpt_imports i JOIN chatgpt_registrations r ON r.id=i.registration_id AND r.owner_id=i.owner_id WHERE i.transfer_id=?`, p.ID).Scan(&savedOwner, &fingerprint, &r.ID, &r.ClientID, &r.Subject, &r.Email, &r.Revision, &r.State)
	if errors.Is(err, sql.ErrNoRows) {
		return Registration{}, false, nil
	}
	if err != nil || savedOwner != owner {
		return Registration{}, true, ErrDenied
	}
	if !hmac.Equal(fingerprint, s.vault.importFingerprint(owner, encoded)) {
		return Registration{}, true, ErrDenied
	}
	if r.State != "ready" {
		return Registration{}, true, ErrReauthenticate
	}
	return r, true, nil
}

// ImportTransfer accepts only an explicitly owner-authorized secure transfer.
// Access tokens stay opaque. Verify the retained ID token, required granted
// scopes and expiry, bind a new registration to the existing target host, and
// retain the exact transfer identity so lost replies cannot create duplicates.
func (s *Service) ImportTransfer(ctx context.Context, owner string, t Transfer) (Registration, error) {
	ctx, done, err := s.enter(ctx)
	if err != nil {
		return Registration{}, err
	}
	defer done()
	p := t.payload
	if !textID(owner) || !p.valid() || p.TargetHost != s.vault.HostID() {
		return Registration{}, ErrDenied
	}
	encoded, err := EncodeTransfer(t)
	if err != nil {
		return Registration{}, err
	}
	defer clear(encoded)
	unlock, err := s.vault.lock(ctx, p.ID)
	if err != nil {
		return Registration{}, err
	}
	defer unlock()
	if replay, found, err := s.replayImport(ctx, s.vault.db, owner, p, encoded); err != nil || found {
		return replay, err
	}
	if err = s.verifyTransfer(ctx, p); err != nil {
		return Registration{}, err
	}
	r := record{Registration: Registration{ID: uuid.NewString(), ClientID: p.ClientID, Subject: p.Subject, Email: p.Email, Revision: 1, State: "ready"}, owner: owner, version: 1}
	c := credentials{Access: p.Access, Refresh: p.Refresh, ID: p.IDToken, Scopes: p.Scopes, Expires: p.Expires, Earliest: p.Earliest}
	credential, _ := json.Marshal(c)
	defer clear(credential)
	sealed, err := s.vault.seal(credential, s.vault.aad(r))
	if err != nil {
		return Registration{}, err
	}
	tx, err := s.vault.db.BeginTx(ctx, nil)
	if err != nil {
		return Registration{}, err
	}
	defer tx.Rollback()
	// Acquire the writer before replay inspection so separate service instances
	// cannot create two registrations for a single transferred session.
	if _, err = tx.ExecContext(ctx, `UPDATE chatgpt_registrations SET revision=revision WHERE 0`); err != nil {
		return Registration{}, ErrUnavailable
	}
	if replay, found, err := s.replayImport(ctx, tx, owner, p, encoded); err != nil || found {
		return replay, err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO chatgpt_registrations(id,owner_id,client_id,subject,email,revision,token_version,state,credential) VALUES(?,?,?,?,?,1,1,'ready',?)`, r.ID, owner, r.ClientID, r.Subject, r.Email, sealed); err != nil {
		return Registration{}, ErrDenied
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO chatgpt_imports(transfer_id,owner_id,registration_id,fingerprint) VALUES(?,?,?,?)`, p.ID, owner, r.ID, s.vault.importFingerprint(owner, encoded)); err != nil {
		return Registration{}, ErrDenied
	}
	if err = tx.Commit(); err != nil {
		return Registration{}, err
	}
	return r.Registration, nil
}

// AcknowledgeTransfer removes the source's token copies after a confirmed import.
// It never revokes the session now owned by the target. The permanent fence and
// original account/client mapping remain, and a newer local sign-in is preserved.
func (s *Service) AcknowledgeTransfer(ctx context.Context, owner, id string, transfer Transfer) error {
	ctx, done, err := s.enter(ctx)
	if err != nil {
		return err
	}
	defer done()
	unlock, err := s.vault.lock(ctx, id)
	if err != nil {
		return err
	}
	defer unlock()
	_, err = s.vault.read(ctx, owner, id)
	if err != nil {
		return err
	}
	tx, err := s.vault.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var target, state string
	var revision int64
	p := transfer.payload
	if !p.valid() {
		return ErrInvalid
	}
	if err = tx.QueryRowContext(ctx, `SELECT target_host,state,revision FROM chatgpt_exports WHERE registration_id=? AND transfer_id=?`, id, p.ID).Scan(&target, &state, &revision); err != nil || target != p.TargetHost {
		return ErrDenied
	}
	if state == "accepted" {
		return nil
	}
	if _, err = tx.ExecContext(ctx, `UPDATE chatgpt_exports SET state='accepted',payload=NULL WHERE registration_id=? AND transfer_id=? AND state='pending'`, id, p.ID); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE chatgpt_registrations SET credential=NULL,state=CASE WHEN state='reauth' THEN 'signed_out' ELSE state END WHERE id=? AND owner_id=? AND revision=? AND state<>'ready'`, id, owner, revision); err != nil {
		return err
	}
	return tx.Commit()
}
