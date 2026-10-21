package capability

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/url"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
	"kingdom/internal/core/providerexecution"
	"kingdom/internal/execution/providerrequest"
	_ "modernc.org/sqlite"
)

type capability struct {
	id       string
	pins     Pins
	request  *providerexecution.RequestIdentity
	expires  time.Time
	state    string
	requests map[uint64]context.CancelFunc
	done     chan struct{}
}
type Registry struct {
	mu              sync.Mutex
	db              *sql.DB
	lock            *os.File
	cfg             Config
	caps            map[string]*capability
	databasePath    string
	sequence        uint64
	activeRequests  int
	closing, closed bool
}

func New(ctx context.Context, cfg Config) (*Registry, error) {
	if cfg.DatabasePath == "" {
		return nil, ErrInvalid
	}
	if cfg.Lifetime == 0 {
		cfg.Lifetime = time.Hour
	}
	if cfg.Lifetime <= 0 || cfg.Lifetime > 24*time.Hour {
		return nil, ErrInvalid
	}
	file, path, err := lockDatabase(cfg.DatabasePath)
	if err != nil {
		return nil, err
	}
	before, err := os.Lstat(path)
	if err != nil {
		file.Close()
		return nil, ErrInvalid
	}
	u := url.URL{Scheme: "file", Path: path}
	q := u.Query()
	q.Add("_pragma", "foreign_keys(1)")
	q.Add("_pragma", "busy_timeout(5000)")
	u.RawQuery = q.Encode()
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		file.Close()
		return nil, ErrInvalid
	}
	db.SetMaxOpenConns(1)
	fail := func() (*Registry, error) { db.Close(); file.Close(); return nil, ErrInvalid }
	var version int
	if db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version) != nil || version < 0 || version > 2 {
		return fail()
	}
	if version == 0 {
		var count int
		if db.QueryRowContext(ctx, `SELECT count(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'`).Scan(&count) != nil || count != 0 {
			return fail()
		}
	}
	_, err = db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS worker_capabilities (
 id TEXT PRIMARY KEY,token_hash BLOB NOT NULL UNIQUE CHECK(length(token_hash)=32),binding BLOB NOT NULL, purpose TEXT NOT NULL, invocation_id TEXT NOT NULL, actor_id TEXT NOT NULL, project_id TEXT NOT NULL,
 request BLOB,expires INTEGER NOT NULL,state TEXT NOT NULL CHECK(state IN ('prepared','active','fenced','revoking','closed')),closed_at INTEGER,
 CHECK((state='closed')=(closed_at IS NOT NULL)));
 CREATE TABLE IF NOT EXISTS worker_capability_owner_fences (purpose TEXT NOT NULL,invocation_id TEXT NOT NULL,binding BLOB NOT NULL,PRIMARY KEY(purpose,invocation_id));
 CREATE TRIGGER IF NOT EXISTS worker_capability_fence_immutable BEFORE UPDATE ON worker_capability_owner_fences BEGIN SELECT RAISE(ABORT,'capability owner fence is immutable');END;
 CREATE TRIGGER IF NOT EXISTS worker_capability_fence_retained BEFORE DELETE ON worker_capability_owner_fences BEGIN SELECT RAISE(ABORT,'capability owner fence must be retained');END;
 CREATE INDEX IF NOT EXISTS worker_capability_owner ON worker_capabilities(purpose,invocation_id);
 CREATE INDEX IF NOT EXISTS worker_capability_access ON worker_capabilities(actor_id,project_id,state);
 CREATE TRIGGER IF NOT EXISTS worker_capability_pins BEFORE UPDATE OF id,token_hash,binding,expires,purpose,invocation_id,actor_id,project_id ON worker_capabilities BEGIN SELECT RAISE(ABORT,'model capability is immutable');END;
 CREATE TRIGGER IF NOT EXISTS worker_capability_request BEFORE UPDATE OF request ON worker_capabilities WHEN OLD.request IS NOT NULL BEGIN SELECT RAISE(ABORT,'model request identity is immutable');END;
 CREATE TRIGGER IF NOT EXISTS worker_capability_state BEFORE UPDATE OF state ON worker_capabilities WHEN
 (OLD.state='closed' AND NEW.state<>'closed') OR (OLD.state='revoking' AND NEW.state NOT IN ('revoking','closed')) OR (OLD.state='active' AND NEW.state='prepared') OR (OLD.state='fenced' AND NEW.state IN ('prepared','active')) BEGIN SELECT RAISE(ABORT,'model capability cannot reopen');END;
 CREATE TRIGGER IF NOT EXISTS worker_capability_retained BEFORE DELETE ON worker_capabilities BEGIN SELECT RAISE(ABORT,'model capability must be retained');END; PRAGMA user_version=2;`)
	if err != nil {
		return fail()
	}
	if err = initializeClosureSchema(ctx, db); err != nil {
		return fail()
	}
	actual, e2 := os.Lstat(path)
	if e2 != nil || !os.SameFile(before, actual) {
		return fail()
	}
	// Exclusive ownership proves the previous broker released its canonical sidecar lock.
	// Its capabilities are never accepted by this process, even after a crash.
	if _, err = db.ExecContext(ctx, `UPDATE worker_capabilities SET state='closed',closed_at=? WHERE state<>'closed'`, time.Now().UnixNano()); err != nil {
		return fail()
	}
	return &Registry{db: db, lock: file, cfg: cfg, databasePath: path, caps: map[string]*capability{}}, nil
}
func validBinding(b Binding) bool {
	p := b.Preparation
	r := b.Runtime
	return (p.Owner.Kind == providerrequest.OwnerRun || p.Owner.Kind == providerrequest.OwnerPlanning) && b.Request == nil && p.ResourceID != "" && p.Owner.InvocationID != "" && p.Lease.Owner != "" && p.Lease.Version > 0 && r.Allocation > 0 && r.SandboxID != "" && r.Generation != "" && r.MainInstanceID != "" && r.GatewayID != "" && r.WorkspaceID != "" && r.WorkerID != "" && b.Access.Validate() == nil
}
func (b *Registry) Issue(ctx context.Context, pins Pins) (Lease, error) {
	rawPins, err := encodePins(pins)
	if err != nil {
		return Lease{}, err
	}
	raw := make([]byte, 32)
	if _, err = rand.Read(raw); err != nil {
		return Lease{}, ErrDenied
	}
	token := base64.RawURLEncoding.EncodeToString(raw)
	hash := sha256.Sum256([]byte(token))
	id := uuid.NewString()
	expiry := time.Now().Add(b.cfg.Lifetime)
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closing {
		return Lease{}, ErrDenied
	}
	var fenced int
	if err = b.db.QueryRowContext(ctx, `SELECT count(*) FROM worker_capability_owner_fences WHERE purpose=? AND invocation_id=?`, pins.Purpose, pins.Binding.Preparation.Owner.InvocationID).Scan(&fenced); err != nil || fenced != 0 {
		return Lease{}, ErrDenied
	}
	rows, err := b.db.QueryContext(ctx, `SELECT binding FROM worker_capabilities WHERE state<>'closed' AND purpose=? AND invocation_id=?`, pins.Purpose, pins.Binding.Preparation.Owner.InvocationID)
	if err != nil {
		return Lease{}, ErrDenied
	}
	conflict := false
	for rows.Next() {
		var data []byte
		if rows.Scan(&data) != nil {
			conflict = true
			break
		}
		old, e := decodePins(data)
		if e != nil || (old.Purpose == pins.Purpose && old.Binding.Preparation.Owner.InvocationID == pins.Binding.Preparation.Owner.InvocationID) {
			conflict = true
			break
		}
	}
	scanErr := rows.Err()
	rows.Close()
	if conflict || scanErr != nil {
		return Lease{}, ErrDenied
	}
	if _, err = b.db.ExecContext(ctx, `INSERT INTO worker_capabilities(id,token_hash,binding,expires,state,purpose,invocation_id,actor_id,project_id) VALUES(?,?,?,?,'prepared',?,?,?,?)`, id, hash[:], rawPins, expiry.UnixNano(), pins.Purpose, pins.Binding.Preparation.Owner.InvocationID, pins.Binding.Access.ActorID, pins.Binding.Access.ProjectID); err != nil {
		return Lease{}, ErrDenied
	}
	saved, _ := decodePins(rawPins)
	b.caps[id] = &capability{id: id, pins: saved, expires: expiry, state: "prepared", requests: map[uint64]context.CancelFunc{}}
	return Lease{ID: id, ExpiresAt: expiry, token: token}, nil
}
func (b *Registry) Lookup(ctx context.Context, id string) (Pins, *providerexecution.RequestIdentity, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return Pins{}, nil, ErrDenied
	}
	var raw, request []byte
	if err := b.db.QueryRowContext(ctx, `SELECT binding,request FROM worker_capabilities WHERE id=?`, id).Scan(&raw, &request); err != nil {
		return Pins{}, nil, ErrDenied
	}
	pins, err := decodePins(raw)
	if err != nil {
		return Pins{}, nil, err
	}
	if len(request) == 0 {
		return pins, nil, nil
	}
	var identity providerexecution.RequestIdentity
	if decodeStrict(request, &identity) != nil || identity.Validate() != nil {
		return Pins{}, nil, ErrDenied
	}
	return pins, &identity, nil
}
func (b *Registry) Activate(ctx context.Context, id string, identity providerexecution.RequestIdentity) error {
	if identity.Validate() != nil {
		return ErrDenied
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	c := b.caps[id]
	if b.closing || c == nil || !time.Now().Before(c.expires) || identity.InstanceID != c.pins.Binding.Runtime.MainInstanceID {
		return ErrDenied
	}
	if c.state == "active" && c.request != nil && *c.request == identity {
		return nil
	}
	if c.state != "prepared" {
		return ErrDenied
	}
	raw, _ := json.Marshal(identity)
	if _, err := b.db.ExecContext(ctx, `UPDATE worker_capabilities SET state='active',request=? WHERE id=? AND state='prepared'`, raw, id); err != nil {
		return ErrDenied
	}
	c.request = &identity
	c.state = "active"
	return nil
}
func (b *Registry) FenceAdmissions(ctx context.Context, id string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	c := b.caps[id]
	if b.closed || c == nil {
		return ErrDenied
	}
	if c.state == "fenced" || c.state == "revoking" || c.state == "closed" {
		return nil
	}
	if _, err := b.db.ExecContext(ctx, `UPDATE worker_capabilities SET state='fenced' WHERE id=?`, id); err != nil {
		return ErrOpen
	}
	c.state = "fenced"
	return nil
}
func (b *Registry) Begin(ctx context.Context, token string, purpose Purpose, mode Mode) (*Admission, error) {
	if (purpose == Model && mode == ModeReceipt) || len(token) != 43 || (purpose != Model && purpose != Plugin) || (mode != ModeNew && mode != ModeReceipt) {
		return nil, ErrDenied
	}
	decoded, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil || len(decoded) != 32 || base64.RawURLEncoding.EncodeToString(decoded) != token {
		return nil, ErrDenied
	}
	hash := sha256.Sum256([]byte(token))
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closing {
		return nil, ErrDenied
	}
	var id, state string
	if err = b.db.QueryRowContext(ctx, `SELECT id,state FROM worker_capabilities WHERE token_hash=?`, hash[:]).Scan(&id, &state); err != nil {
		return nil, ErrDenied
	}
	c := b.caps[id]
	if c == nil || c.request == nil || c.pins.Purpose != purpose || !time.Now().Before(c.expires) || state != c.state || (state != "active" && !(state == "fenced" && mode == ModeReceipt)) {
		return nil, ErrDenied
	}
	limit, perCapability := 128, 32
	if mode == ModeReceipt {
		limit, perCapability = 256, 64
	}
	if b.activeRequests >= limit || len(c.requests) >= perCapability {
		return nil, ErrDenied
	}
	requestCtx, cancel := context.WithDeadline(ctx, c.expires)
	b.sequence++
	sequence := b.sequence
	c.requests[sequence] = cancel
	b.activeRequests++
	raw, _ := encodePins(c.pins)
	pins, _ := decodePins(raw)
	admission := &Admission{Context: requestCtx, Pins: pins, Request: *c.request}
	admission.finish = func() {
		cancel()
		b.mu.Lock()
		defer b.mu.Unlock()
		delete(c.requests, sequence)
		b.activeRequests--
		if len(c.requests) == 0 && c.done != nil {
			select {
			case <-c.done:
			default:
				close(c.done)
			}
		}
	}
	return admission, nil
}
func (b *Registry) Revoke(ctx context.Context, id string) error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return ErrDenied
	}
	c := b.caps[id]
	if c == nil {
		var state string
		err := b.db.QueryRowContext(ctx, `SELECT state FROM worker_capabilities WHERE id=?`, id).Scan(&state)
		b.mu.Unlock()
		if err == nil && state == "closed" {
			return nil
		}
		return ErrDenied
	}
	if c.state == "closed" {
		b.mu.Unlock()
		return nil
	}
	if _, err := b.db.ExecContext(ctx, `UPDATE worker_capabilities SET state='revoking' WHERE id=? AND state<>'closed'`, id); err != nil {
		b.mu.Unlock()
		return ErrOpen
	}
	c.state = "revoking"
	for _, cancel := range c.requests {
		cancel()
	}
	if c.done == nil {
		c.done = make(chan struct{})
		if len(c.requests) == 0 {
			close(c.done)
		}
	}
	done := c.done
	b.mu.Unlock()
	select {
	case <-done:
	case <-ctx.Done():
		return ErrOpen
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if c.state == "closed" {
		return nil
	}
	if _, err := b.db.ExecContext(ctx, `UPDATE worker_capabilities SET state='closed',closed_at=? WHERE id=?`, time.Now().UnixNano(), id); err != nil {
		return ErrOpen
	}
	c.state = "closed"
	return nil
}
func (b *Registry) RequireClosed(ctx context.Context, pins Pins) error {
	expected, err := encodePins(pins)
	if err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return ErrDenied
	}
	rows, err := b.db.QueryContext(ctx, `SELECT binding,state FROM worker_capabilities WHERE purpose=? AND invocation_id=?`, pins.Purpose, pins.Binding.Preparation.Owner.InvocationID)
	if err != nil {
		return ErrOpen
	}
	defer rows.Close()
	for rows.Next() {
		var raw []byte
		var state string
		if rows.Scan(&raw, &state) != nil {
			return ErrOpen
		}
		saved, e := decodePins(raw)
		if e != nil {
			return ErrOpen
		}
		if saved.Purpose == pins.Purpose && saved.Binding.Preparation.Owner.InvocationID == pins.Binding.Preparation.Owner.InvocationID {
			if !bytes.Equal(expected, raw) || state != "closed" {
				return ErrOpen
			}
		}
	}
	if rows.Err() != nil {
		return ErrOpen
	}
	return nil
}
func (b *Registry) Close(ctx context.Context) error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closing = true
	ids := make([]string, 0, len(b.caps))
	for id := range b.caps {
		ids = append(ids, id)
	}
	b.mu.Unlock()
	for _, id := range ids {
		if err := b.Revoke(ctx, id); err != nil {
			return err
		}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil
	}
	if err := b.db.Close(); err != nil {
		return err
	}
	if err := b.lock.Close(); err != nil {
		return err
	}
	b.closed = true
	return nil
}

func decodeStrict(raw []byte, dst any) error {
	if len(raw) == 0 || len(raw) > 32768 {
		return ErrDenied
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(dst) != nil {
		return ErrDenied
	}
	if d.Decode(new(any)) != io.EOF {
		return ErrDenied
	}
	return nil
}
func encodePins(p Pins) ([]byte, error) {
	if !validBinding(p.Binding) || (p.Purpose != Model && p.Purpose != Plugin) || p.Purpose == Model && (p.Route.Protocol != "openai" || p.Route.Endpoint == "" || p.Route.Model == "") || p.Purpose == Plugin && p.Route != (Route{}) {
		return nil, ErrDenied
	}
	raw, e := json.Marshal(p)
	if e != nil || len(raw) > 32768 {
		return nil, ErrDenied
	}
	return raw, nil
}
func decodePins(raw []byte) (Pins, error) {
	var p Pins
	if decodeStrict(raw, &p) != nil {
		return p, ErrDenied
	}
	canonical, e := encodePins(p)
	if e != nil || !bytes.Equal(raw, canonical) {
		return Pins{}, ErrDenied
	}
	return p, nil
}

// RequireBindingClosed checks all capabilities for the exact invocation and
// purpose without needing to resolve credentials again after access revocation.
func (b *Registry) RequireBindingClosed(ctx context.Context, purpose Purpose, binding Binding) error {
	binding.Request = nil
	if !validBinding(binding) || (purpose != Model && purpose != Plugin) {
		return ErrDenied
	}
	expected, _ := json.Marshal(binding)
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return ErrDenied
	}
	rows, err := b.db.QueryContext(ctx, `SELECT binding,state FROM worker_capabilities WHERE purpose=? AND invocation_id=?`, purpose, binding.Preparation.Owner.InvocationID)
	if err != nil {
		return ErrOpen
	}
	defer rows.Close()
	for rows.Next() {
		var raw []byte
		var state string
		if rows.Scan(&raw, &state) != nil {
			return ErrOpen
		}
		pins, e := decodePins(raw)
		if e != nil {
			return ErrOpen
		}
		if pins.Purpose == purpose && pins.Binding.Preparation.Owner.InvocationID == binding.Preparation.Owner.InvocationID {
			actual, _ := json.Marshal(pins.Binding)
			if !bytes.Equal(actual, expected) || state != "closed" {
				return ErrOpen
			}
		}
	}
	if rows.Err() != nil {
		return ErrOpen
	}
	return nil
}

// RevokeAccess fences new work for an actor's project. Model requests are
// cancelled and joined; plugin receipts remain admitted until worker shutdown.
// Call after the catalog revocation commits. Canonical authorization is still
// required for every new request; this notification is not a substitute.
// Existing invocation owners are durably fenced. A first Issue racing this
// separate database still requires canonical owner cancellation by orchestration.
func (b *Registry) RevokeAccess(ctx context.Context, actor, project string) error {
	if actor == "" || project == "" {
		return ErrDenied
	}
	return b.revokeAccess(ctx, actor, project)
}

// RevokeUser is the explicit user-disable form, spanning that actor's projects.
func (b *Registry) RevokeUser(ctx context.Context, actor string) error {
	if actor == "" {
		return ErrDenied
	}
	return b.revokeAccess(ctx, actor, "")
}
func (b *Registry) revokeAccess(ctx context.Context, actor, project string) error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return ErrDenied
	}
	rows, err := b.db.QueryContext(ctx, `SELECT id,binding FROM worker_capabilities WHERE actor_id=? AND (?='' OR project_id=?)`, actor, project, project)
	if err != nil {
		b.mu.Unlock()
		return ErrOpen
	}
	var ids []string
	var ownerPins []Pins
	var models []string
	for rows.Next() {
		var id string
		var raw []byte
		if rows.Scan(&id, &raw) != nil {
			rows.Close()
			b.mu.Unlock()
			return ErrOpen
		}
		pins, e := decodePins(raw)
		if e != nil || pins.Binding.Access.ActorID != actor || project != "" && pins.Binding.Access.ProjectID != project {
			rows.Close()
			b.mu.Unlock()
			return ErrOpen
		}
		ids = append(ids, id)
		ownerPins = append(ownerPins, pins)
		if pins.Purpose == Model {
			models = append(models, id)
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		b.mu.Unlock()
		return ErrOpen
	}
	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		b.mu.Unlock()
		return ErrOpen
	}
	for _, pins := range ownerPins {
		raw, _ := json.Marshal(pins.Binding)
		var retained []byte
		err = tx.QueryRowContext(ctx, `SELECT binding FROM worker_capability_owner_fences WHERE purpose=? AND invocation_id=?`, pins.Purpose, pins.Binding.Preparation.Owner.InvocationID).Scan(&retained)
		if err == sql.ErrNoRows {
			_, err = tx.ExecContext(ctx, `INSERT INTO worker_capability_owner_fences(purpose,invocation_id,binding) VALUES(?,?,?)`, pins.Purpose, pins.Binding.Preparation.Owner.InvocationID, raw)
		} else if err == nil && !bytes.Equal(retained, raw) {
			err = ErrDenied
		}
		if err != nil {
			tx.Rollback()
			b.mu.Unlock()
			return ErrOpen
		}
	}
	for _, id := range ids {
		if _, err = tx.ExecContext(ctx, `UPDATE worker_capabilities SET state=CASE WHEN purpose='model' THEN 'revoking' ELSE 'fenced' END WHERE id=? AND state IN ('prepared','active','fenced')`, id); err != nil {
			tx.Rollback()
			b.mu.Unlock()
			return ErrOpen
		}
	}
	if tx.Commit() != nil {
		b.mu.Unlock()
		return ErrOpen
	}
	for _, id := range ids {
		c := b.caps[id]
		if c == nil {
			continue
		}
		if c.state == "closed" || c.state == "revoking" {
			continue
		}
		if c.pins.Purpose == Model {
			c.state = "revoking"
			for _, cancel := range c.requests {
				cancel()
			}
		} else {
			c.state = "fenced"
		}
	}
	b.mu.Unlock()
	var failure error
	for _, id := range models {
		if err = b.Revoke(ctx, id); err != nil {
			failure = err
		}
	}
	return failure
}

// RevokeBinding closes all capabilities, including lost issuance replies, and
// durably prevents another issuance for the same invocation and purpose.
func (b *Registry) RevokeBinding(ctx context.Context, purpose Purpose, binding Binding) error {
	return b.fenceBinding(ctx, purpose, binding, true)
}

// FenceBinding also prevents lost or late issuance while allowing plugin receipts.
func (b *Registry) FenceBinding(ctx context.Context, purpose Purpose, binding Binding) error {
	return b.fenceBinding(ctx, purpose, binding, false)
}
func (b *Registry) fenceBinding(ctx context.Context, purpose Purpose, binding Binding, revoke bool) error {
	binding.Request = nil
	if !validBinding(binding) || (purpose != Model && purpose != Plugin) {
		return ErrDenied
	}
	raw, _ := json.Marshal(binding)
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return ErrDenied
	}
	rows, err := b.db.QueryContext(ctx, `SELECT id,binding FROM worker_capabilities WHERE purpose=? AND invocation_id=?`, purpose, binding.Preparation.Owner.InvocationID)
	if err != nil {
		b.mu.Unlock()
		return ErrOpen
	}
	var ids []string
	for rows.Next() {
		var id string
		var saved []byte
		if rows.Scan(&id, &saved) != nil {
			rows.Close()
			b.mu.Unlock()
			return ErrOpen
		}
		pins, e := decodePins(saved)
		actual, _ := json.Marshal(pins.Binding)
		if e != nil || !bytes.Equal(raw, actual) {
			rows.Close()
			b.mu.Unlock()
			return ErrDenied
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		b.mu.Unlock()
		return ErrOpen
	}
	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		b.mu.Unlock()
		return ErrOpen
	}
	var retained []byte
	err = tx.QueryRowContext(ctx, `SELECT binding FROM worker_capability_owner_fences WHERE purpose=? AND invocation_id=?`, purpose, binding.Preparation.Owner.InvocationID).Scan(&retained)
	if err == sql.ErrNoRows {
		_, err = tx.ExecContext(ctx, `INSERT INTO worker_capability_owner_fences(purpose,invocation_id,binding) VALUES(?,?,?)`, purpose, binding.Preparation.Owner.InvocationID, raw)
	} else if err == nil && !bytes.Equal(retained, raw) {
		err = ErrDenied
	}
	if err != nil {
		tx.Rollback()
		b.mu.Unlock()
		return ErrOpen
	}
	state := "fenced"
	if revoke {
		state = "revoking"
	}
	if _, err = tx.ExecContext(ctx, `UPDATE worker_capabilities SET state=? WHERE purpose=? AND invocation_id=? AND state NOT IN ('closed','revoking')`, state, purpose, binding.Preparation.Owner.InvocationID); err != nil {
		tx.Rollback()
		b.mu.Unlock()
		return ErrOpen
	}
	if tx.Commit() != nil {
		b.mu.Unlock()
		return ErrOpen
	}
	for _, id := range ids {
		if c := b.caps[id]; c != nil && c.state != "closed" && c.state != "revoking" {
			c.state = state
			if revoke {
				for _, cancel := range c.requests {
					cancel()
				}
			}
		}
	}
	b.mu.Unlock()
	if !revoke {
		return nil
	}

	var failure error
	for _, id := range ids {
		if err = b.Revoke(ctx, id); err != nil {
			failure = err
		}
	}
	return failure
}
