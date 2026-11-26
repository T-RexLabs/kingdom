package remote

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
)

// SourceArchive identifies a private immutable captured workspace. ID is a UUID,
// never a path. The source store verifies size and digest before each read.
type SourceArchive struct {
	ID     string
	SHA256 string
	Bytes  int64
}

// WorkerManifest fixes source and operator pins before any remote side effect.
// Admission authority must be checked by the caller in the same transaction.
type WorkerManifest struct {
	Owner      Owner
	ResourceID string
	CreationID string
	ImportID   string
	ExportID   string
	Source     SourceArchive
	PinsJSON   []byte
	Allocation int64
}

func validWorkerManifest(m WorkerManifest) bool {
	return validOwner(m.Owner) && text(m.ResourceID) && validUUID(m.CreationID) && validUUID(m.ImportID) && validUUID(m.ExportID) && m.CreationID != m.ImportID && m.CreationID != m.ExportID && m.ImportID != m.ExportID && validUUID(m.Source.ID) && validDigest(m.Source.SHA256) && m.Source.Bytes > 0 && len(m.PinsJSON) > 0 && len(m.PinsJSON) <= 65536 && json.Valid(m.PinsJSON) && m.Allocation >= 0
}
func RecordWorkerManifestTx(ctx context.Context, tx *sql.Tx, m WorkerManifest) error {
	if !validWorkerManifest(m) || m.Allocation != 0 {
		return ErrConflict
	}
	source, e := json.Marshal(m.Source)
	if e != nil {
		return e
	}
	_, e = tx.ExecContext(ctx, `INSERT OR IGNORE INTO remote_worker_manifests(invocation_id,owner_kind,binding_id,lease_owner,lease_version,resource_id,creation_id,import_id,export_id,source_json,pins_json) VALUES(?,?,?,?,?,?,?,?,?,?,?)`, m.Owner.InvocationID, m.Owner.Kind, m.Owner.BindingID, m.Owner.LeaseOwner, m.Owner.LeaseVersion, m.ResourceID, m.CreationID, m.ImportID, m.ExportID, source, m.PinsJSON)
	if e != nil {
		return e
	}
	got, e := ReadWorkerManifest(ctx, tx, m.Owner.Kind, m.Owner.InvocationID)
	if e != nil {
		return e
	}
	if got.Owner != m.Owner || got.ResourceID != m.ResourceID || got.CreationID != m.CreationID || got.ImportID != m.ImportID || got.ExportID != m.ExportID || got.Source != m.Source || !bytes.Equal(got.PinsJSON, m.PinsJSON) {
		return ErrConflict
	}
	return nil
}
func ReadWorkerManifest(ctx context.Context, q Reader, kind, invocationID string) (WorkerManifest, error) {
	var m WorkerManifest
	var source []byte
	var allocation sql.NullInt64
	e := q.QueryRowContext(ctx, `SELECT owner_kind,invocation_id,binding_id,lease_owner,lease_version,resource_id,creation_id,import_id,export_id,source_json,pins_json,allocation FROM remote_worker_manifests WHERE owner_kind=? AND invocation_id=?`, kind, invocationID).Scan(&m.Owner.Kind, &m.Owner.InvocationID, &m.Owner.BindingID, &m.Owner.LeaseOwner, &m.Owner.LeaseVersion, &m.ResourceID, &m.CreationID, &m.ImportID, &m.ExportID, &source, &m.PinsJSON, &allocation)
	if e != nil {
		return m, e
	}
	m.Allocation = allocation.Int64
	if json.Unmarshal(source, &m.Source) != nil || !validWorkerManifest(m) {
		return WorkerManifest{}, ErrConflict
	}
	if m.Allocation != 0 {
		r, e := ReadRuntime(ctx, q, m.Allocation)
		if e != nil {
			return WorkerManifest{}, e
		}
		if r.Owner != m.Owner || r.Reference.CreationID != m.CreationID {
			return WorkerManifest{}, ErrConflict
		}
	}
	return m, nil
}
func ReadWorkerManifestByAllocation(ctx context.Context, q Reader, allocation int64) (WorkerManifest, error) {
	var kind, id string
	if e := q.QueryRowContext(ctx, `SELECT owner_kind,invocation_id FROM remote_worker_manifests WHERE allocation=?`, allocation).Scan(&kind, &id); e != nil {
		return WorkerManifest{}, e
	}
	return ReadWorkerManifest(ctx, q, kind, id)
}
func BindWorkerManifestAllocationTx(ctx context.Context, tx *sql.Tx, owner Owner, ref Reference) error {
	r, e := requireOwner(ctx, tx, owner, ref, false)
	if e != nil {
		return e
	}
	m, e := ReadWorkerManifest(ctx, tx, owner.Kind, owner.InvocationID)
	if e != nil {
		return e
	}
	if m.Owner != owner || m.CreationID != r.Reference.CreationID || (m.Allocation != 0 && m.Allocation != ref.Allocation) {
		return ErrConflict
	}
	_, e = tx.ExecContext(ctx, `UPDATE remote_worker_manifests SET allocation=? WHERE invocation_id=? AND allocation IS NULL`, ref.Allocation, owner.InvocationID)
	return e
}
