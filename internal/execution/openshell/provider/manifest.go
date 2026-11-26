// Package provider assembles invocation-owned OpenShell preparation and cleanup.
package provider

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
	"kingdom/internal/execution/openshell/worker"
	"kingdom/internal/execution/providerrequest"
	"kingdom/internal/execution/remote"
	"kingdom/internal/sqlitex"
)

type SourceStore interface {
	Capture(context.Context, providerrequest.Preparation) (remote.SourceArchive, error)
	Open(context.Context, remote.SourceArchive) (worker.Archive, error)
}

type manifestStore struct {
	db     *sql.DB
	source SourceStore
	pins   []byte
	now    func() time.Time
}

func (s manifestStore) load(ctx context.Context, id string) (remote.WorkerManifest, error) {
	var kind string
	if e := s.db.QueryRowContext(ctx, `SELECT owner_kind FROM remote_worker_manifests WHERE invocation_id=?`, id).Scan(&kind); e != nil {
		return remote.WorkerManifest{}, e
	}
	m, e := remote.ReadWorkerManifest(ctx, s.db, kind, id)
	if e != nil {
		return m, e
	}
	if !bytes.Equal(m.PinsJSON, s.pins) {
		return remote.WorkerManifest{}, remote.ErrConflict
	}
	return m, nil
}
func (s manifestStore) ensure(ctx context.Context, p providerrequest.Preparation) (remote.WorkerManifest, error) {
	m, e := s.load(ctx, p.Owner.InvocationID)
	if e == nil {
		return m, matchPreparation(m, p)
	}
	if !errors.Is(e, sql.ErrNoRows) {
		return m, e
	}
	// Slow filesystem work stays outside SQL. A crash leaves only an unreferenced
	// private source file, never a partially committed remote reservation.
	captured, e := s.source.Capture(ctx, p)
	if e != nil {
		return m, e
	}
	m = remote.WorkerManifest{Owner: remote.Owner{Kind: string(p.Owner.Kind), InvocationID: p.Owner.InvocationID, BindingID: uuid.NewString(), LeaseOwner: p.Lease.Owner, LeaseVersion: p.Lease.Version}, ResourceID: p.ResourceID, CreationID: uuid.NewString(), ImportID: uuid.NewString(), ExportID: uuid.NewString(), Source: captured, PinsJSON: append([]byte(nil), s.pins...)}
	_, e = sqlitex.Transaction(ctx, s.db, func(_ context.Context, tx *sql.Tx) error {
		// Another preparer may have captured concurrently. The first immutable
		// manifest wins; the later private source remains an unreferenced orphan.
		existing, err := remote.ReadWorkerManifest(ctx, tx, string(p.Owner.Kind), p.Owner.InvocationID)
		if err == nil {
			if err = matchPreparation(existing, p); err != nil {
				return err
			}
			if !bytes.Equal(existing.PinsJSON, s.pins) {
				return remote.ErrConflict
			}
			m = existing
			return nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if err = providerrequest.RequireRemotePreparationTx(ctx, tx, p.Owner, p.Lease, m.Owner.BindingID, s.now()); err != nil {
			return err
		}
		return remote.RecordWorkerManifestTx(ctx, tx, m)
	}, sqlitex.Options{})
	return m, e
}
func matchPreparation(m remote.WorkerManifest, p providerrequest.Preparation) error {
	if m.Owner.Kind != string(p.Owner.Kind) || m.Owner.InvocationID != p.Owner.InvocationID || m.Owner.LeaseOwner != p.Lease.Owner || m.Owner.LeaseVersion != p.Lease.Version || m.ResourceID != p.ResourceID {
		return remote.ErrConflict
	}
	return nil
}
func preparation(m remote.WorkerManifest) providerrequest.Preparation {
	return providerrequest.Preparation{ResourceID: m.ResourceID, Owner: providerrequest.Owner{Kind: providerrequest.OwnerKind(m.Owner.Kind), InvocationID: m.Owner.InvocationID}, Lease: providerrequest.Lease{Owner: m.Owner.LeaseOwner, Version: m.Owner.LeaseVersion}}
}

type pinnedSource struct {
	store    SourceStore
	manifest remote.WorkerManifest
}

func (s pinnedSource) Open(ctx context.Context, p providerrequest.Preparation) (worker.Archive, error) {
	if e := matchPreparation(s.manifest, p); e != nil {
		return nil, e
	}
	return s.store.Open(ctx, s.manifest.Source)
}
func pinsJSON(values ...any) ([]byte, error) { return json.Marshal(values) }
