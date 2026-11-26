package capability

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"

	"github.com/google/uuid"
	"kingdom/internal/core/providerexecution"
)

// ClosureProof can only be created by the exclusive registry owner after a
// permanent issuance fence and joined request closure. It contains no tokens.
// The private certificate prevents callers from constructing closure evidence.
type ClosureProof struct {
	certificate []byte
	digest      string
}

type ClosedCapability struct {
	ID      string
	Request *providerexecution.RequestIdentity
}
type ClosureCertificate struct {
	Version       int
	RegistryID    string
	CertificateID string
	Purpose       Purpose
	Binding       Binding
	Capabilities  []ClosedCapability
}

// Certificate returns a detached copy. Changing it cannot change the proof.
func (p ClosureProof) Certificate() (ClosureCertificate, error) {
	var c ClosureCertificate
	if len(p.certificate) == 0 || decodeStrict(p.certificate, &c) != nil {
		return c, ErrDenied
	}
	return c, nil
}
func (p ClosureProof) Digest() string { return p.digest }

func initializeClosureSchema(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS worker_capability_registry_identity(singleton INTEGER PRIMARY KEY CHECK(singleton=1),id TEXT NOT NULL UNIQUE);
 CREATE TRIGGER IF NOT EXISTS worker_registry_identity_immutable BEFORE UPDATE ON worker_capability_registry_identity BEGIN SELECT RAISE(ABORT,'registry identity is immutable');END;
 CREATE TRIGGER IF NOT EXISTS worker_registry_identity_retained BEFORE DELETE ON worker_capability_registry_identity BEGIN SELECT RAISE(ABORT,'registry identity must be retained');END;
 CREATE TABLE IF NOT EXISTS worker_capability_closures(purpose TEXT NOT NULL,invocation_id TEXT NOT NULL,certificate BLOB NOT NULL,digest TEXT NOT NULL,PRIMARY KEY(purpose,invocation_id));
 CREATE TRIGGER IF NOT EXISTS worker_closure_immutable BEFORE UPDATE ON worker_capability_closures BEGIN SELECT RAISE(ABORT,'capability closure is immutable');END;
 CREATE TRIGGER IF NOT EXISTS worker_closure_retained BEFORE DELETE ON worker_capability_closures BEGIN SELECT RAISE(ABORT,'capability closure must be retained');END;`)
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, `INSERT OR IGNORE INTO worker_capability_registry_identity(singleton,id) VALUES(1,?)`, uuid.NewString())
	return err
}

// SealBindingClosure does not reauthorize revoked access. It checks immutable
// historical pins, including every activation, and refuses an unfenced empty set.
// A permanent owner fence makes the certificate valid across later processes.
func (b *Registry) SealBindingClosure(ctx context.Context, purpose Purpose, binding Binding) (ClosureProof, error) {
	requested := binding.Request
	binding.Request = nil
	if !validBinding(binding) || (purpose != Model && purpose != Plugin) {
		return ClosureProof{}, ErrDenied
	}
	raw, _ := json.Marshal(binding)
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed || b.closing {
		return ClosureProof{}, ErrOpen
	}
	var fence []byte
	if b.db.QueryRowContext(ctx, `SELECT binding FROM worker_capability_owner_fences WHERE purpose=? AND invocation_id=?`, purpose, binding.Preparation.Owner.InvocationID).Scan(&fence) != nil || !bytes.Equal(raw, fence) {
		return ClosureProof{}, ErrOpen
	}
	rows, err := b.db.QueryContext(ctx, `SELECT id,binding,request,state FROM worker_capabilities WHERE purpose=? AND invocation_id=? ORDER BY id`, purpose, binding.Preparation.Owner.InvocationID)
	if err != nil {
		return ClosureProof{}, ErrOpen
	}
	caps := []ClosedCapability{}
	for rows.Next() {
		var id, state string
		var pinsRaw, requestRaw []byte
		if rows.Scan(&id, &pinsRaw, &requestRaw, &state) != nil {
			rows.Close()
			return ClosureProof{}, ErrOpen
		}
		pins, e := decodePins(pinsRaw)
		actual, _ := json.Marshal(pins.Binding)
		if e != nil || pins.Purpose != purpose || !bytes.Equal(actual, raw) || state != "closed" {
			rows.Close()
			return ClosureProof{}, ErrOpen
		}
		if live := b.caps[id]; live != nil && (live.state != "closed" || len(live.requests) != 0) {
			rows.Close()
			return ClosureProof{}, ErrOpen
		}
		item := ClosedCapability{ID: id}
		if len(requestRaw) > 0 {
			var identity providerexecution.RequestIdentity
			if decodeStrict(requestRaw, &identity) != nil || identity.Validate() != nil || identity.InstanceID != binding.Runtime.MainInstanceID || (requested != nil && *requested != identity) {
				rows.Close()
				return ClosureProof{}, ErrDenied
			}
			item.Request = &identity
		}
		caps = append(caps, item)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return ClosureProof{}, ErrOpen
	}
	var registryID string
	if b.db.QueryRowContext(ctx, `SELECT id FROM worker_capability_registry_identity WHERE singleton=1`).Scan(&registryID) != nil {
		return ClosureProof{}, ErrOpen
	}
	cert := ClosureCertificate{Version: 1, RegistryID: registryID, CertificateID: uuid.NewString(), Purpose: purpose, Binding: binding, Capabilities: caps}
	var saved []byte
	var digest string
	err = b.db.QueryRowContext(ctx, `SELECT certificate,digest FROM worker_capability_closures WHERE purpose=? AND invocation_id=?`, purpose, binding.Preparation.Owner.InvocationID).Scan(&saved, &digest)
	if err == nil {
		var old ClosureCertificate
		if decodeStrict(saved, &old) != nil {
			return ClosureProof{}, ErrOpen
		}
		cert.CertificateID = old.CertificateID
		expected, _ := json.Marshal(cert)
		sum := sha256.Sum256(saved)
		if !bytes.Equal(expected, saved) || hex.EncodeToString(sum[:]) != digest {
			return ClosureProof{}, ErrOpen
		}
		return ClosureProof{certificate: saved, digest: digest}, nil
	}
	if err != sql.ErrNoRows {
		return ClosureProof{}, ErrOpen
	}
	saved, _ = json.Marshal(cert)
	sum := sha256.Sum256(saved)
	digest = hex.EncodeToString(sum[:])
	if _, err = b.db.ExecContext(ctx, `INSERT INTO worker_capability_closures(purpose,invocation_id,certificate,digest)VALUES(?,?,?,?)`, purpose, binding.Preparation.Owner.InvocationID, saved, digest); err != nil {
		return ClosureProof{}, ErrOpen
	}
	return ClosureProof{certificate: saved, digest: digest}, nil
}
