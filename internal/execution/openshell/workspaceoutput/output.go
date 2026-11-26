// Package workspaceoutput applies verified remote results to canonical workspaces.
package workspaceoutput

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"time"

	"kingdom/internal/execution/openshell/worker"
	"kingdom/internal/execution/providerrequest"
	"kingdom/internal/execution/remote"
	"kingdom/internal/runtimefs"
	"kingdom/internal/workspacearchive"
)

type Sources interface {
	Open(context.Context, remote.SourceArchive) (worker.Archive, error)
}
type Config struct {
	DB                   *sql.DB
	Worker               *worker.Worker
	Store                *runtimefs.Store
	Sources              Sources
	SpoolRoot, StageRoot string
	Limits               workspacearchive.Limits
	Now                  func() time.Time
}
type Output struct{ c Config }

func New(c Config) (*Output, error) {
	if c.DB == nil || c.Worker == nil || c.Store == nil || c.Sources == nil || c.SpoolRoot == "" || c.StageRoot == "" {
		return nil, remote.ErrConflict
	}
	if c.Now == nil {
		c.Now = time.Now
	}
	return &Output{c}, nil
}

func sameContents(a, b workspacearchive.Manifest) bool {
	a.ArchiveSHA256 = ""
	b.ArchiveSHA256 = ""
	return reflect.DeepEqual(a, b)
}

func (o *Output) Apply(ctx context.Context, p providerrequest.Preparation, r remote.Reservation, id string) error {
	m, e := remote.ReadWorkerManifestByAllocation(ctx, o.c.DB, r.Reference.Allocation)
	if e != nil {
		return e
	}
	if m.Owner != r.Owner || m.ExportID != id || m.ResourceID != p.ResourceID || m.Owner.InvocationID != p.Owner.InvocationID || m.Owner.Kind != string(p.Owner.Kind) || m.Owner.LeaseOwner != p.Lease.Owner || m.Owner.LeaseVersion != p.Lease.Version {
		return remote.ErrConflict
	}
	d, e := o.c.Worker.OpenDownload(ctx, r, id, o.c.SpoolRoot, o.c.StageRoot)
	if e != nil {
		return e
	}
	defer d.Staged.Close()
	runID := ""
	snapshot := "managed-output-" + id
	readOnly := p.Owner.Kind == "planning"
	if p.Owner.Kind != "planning" {
		w, err := providerrequest.ReadPreparationWorkspace(ctx, o.c.DB, p)
		if err != nil {
			return err
		}
		runID = w.RunID
		readOnly = w.Purpose == "source_only"
	}
	validate := func(q providerrequest.Reader) error {
		if p.Owner.Kind == "planning" {
			return nil
		}
		if readOnly {
			return providerrequest.ValidateReadOnlyOutputTx(ctx, q, p, o.c.Now())
		}
		return providerrequest.ValidateOutputTx(ctx, q, p, o.c.Now())
	}
	if e = validate(o.c.DB); e != nil {
		return e
	}
	if readOnly {
		if e = o.verifyUnchanged(ctx, m, d.Staged.Manifest); e != nil {
			return e
		}
		root, err := providerrequest.OpenPreparationWorkspace(ctx, o.c.DB, p)
		if err != nil {
			return err
		}
		defer root.Close()
	}
	tx, e := o.c.DB.BeginTx(ctx, nil)
	if e != nil {
		return e
	}
	defer tx.Rollback()
	if p.Owner.Kind != "planning" {
		if e = validate(tx); e != nil {
			return e
		}
	}
	_, e = tx.ExecContext(ctx, `INSERT OR IGNORE INTO remote_workspace_output_intents(operation_id,allocation,purpose,archive_sha256,manifest_sha256,snapshot_id,run_id,created_at) VALUES(?,?,?,?,?,?,?,?)`, id, r.Reference.Allocation, p.Owner.Kind, d.ArchiveSHA256, d.ManifestSHA256, snapshot, runID, o.c.Now().UTC().Format(time.RFC3339Nano))
	if e != nil {
		return e
	}
	var count int
	e = tx.QueryRowContext(ctx, `SELECT count(*) FROM remote_workspace_output_intents WHERE operation_id=? AND allocation=? AND purpose=? AND archive_sha256=? AND manifest_sha256=? AND snapshot_id=? AND run_id=?`, id, r.Reference.Allocation, p.Owner.Kind, d.ArchiveSHA256, d.ManifestSHA256, snapshot, runID).Scan(&count)
	if e != nil {
		return e
	}
	if count != 1 {
		return remote.ErrConflict
	}
	if e = tx.Commit(); e != nil {
		return e
	}
	if !readOnly {
		var savedRun, kind string
		var parent sql.NullString
		e = o.c.DB.QueryRowContext(ctx, `SELECT run_id,snapshot_kind,parent_snapshot_id FROM filesystem_snapshots WHERE snapshot_id=?`, snapshot).Scan(&savedRun, &kind, &parent)
		if errors.Is(e, sql.ErrNoRows) {
			_, e = o.c.Store.CaptureSnapshot(ctx, runtimefs.CaptureSnapshotRequest{SnapshotID: snapshot, RunID: runID, SnapshotKind: "attempt_result", RootPath: d.Staged.Path, CreatedAt: o.c.Now().UTC().Format(time.RFC3339Nano)})
		} else if e == nil && (savedRun != runID || kind != "attempt_result" || parent.Valid) {
			return remote.ErrConflict
		}
		if e != nil {
			return e
		}
		if e = o.verifySnapshot(ctx, snapshot, d.Staged.Manifest); e != nil {
			return e
		}
		root, e := providerrequest.OpenOutputWorkspace(ctx, o.c.DB, p)
		if e != nil {
			return e
		}
		defer root.Close()
		if e = providerrequest.ValidateOutputTx(ctx, o.c.DB, p, o.c.Now()); e != nil {
			return e
		}
		if e = o.c.Store.RestoreSnapshotRoot(ctx, snapshot, root); e != nil {
			return e
		}
		// Reopen checks that the retained pathname still denotes the same inode.
		checked, e := providerrequest.OpenOutputWorkspace(ctx, o.c.DB, p)
		if e != nil {
			return e
		}
		checked.Close()
	}
	if readOnly {
		checked, err := providerrequest.OpenPreparationWorkspace(ctx, o.c.DB, p)
		if err != nil {
			return err
		}
		checked.Close()
	}
	tx, e = o.c.DB.BeginTx(ctx, nil)
	if e != nil {
		return e
	}
	defer tx.Rollback()
	var applied any
	if p.Owner.Kind != "planning" {
		if e = validate(tx); e != nil {
			return e
		}
		if !readOnly {
			applied = snapshot
		}
	}
	_, e = tx.ExecContext(ctx, `INSERT OR IGNORE INTO remote_workspace_output_applications(operation_id,snapshot_id,applied_at) VALUES(?,?,?)`, id, applied, o.c.Now().UTC().Format(time.RFC3339Nano))
	if e != nil {
		return e
	}
	if e = remote.RequireWorkspaceOutputApplied(ctx, tx, r.Owner, r.Reference); e != nil {
		return e
	}
	return tx.Commit()
}
func (o *Output) verifySnapshot(ctx context.Context, id string, m workspacearchive.Manifest) error {
	files := map[string]workspacearchive.Entry{}
	for _, e := range m.Entries {
		if e.Kind == "file" {
			files[e.Path] = e
		}
	}
	e := o.c.Store.VisitSnapshotManifest(ctx, id, func(s runtimefs.SnapshotEntry) error {
		a, ok := files[s.Path]
		if !ok || s.BlobID != a.SHA256 || s.SizeBytes != a.Size || (s.ModeBits&0111 != 0) != a.Executable {
			return remote.ErrConflict
		}
		delete(files, s.Path)
		return nil
	})
	if e != nil {
		return e
	}
	if len(files) != 0 {
		return remote.ErrConflict
	}
	return nil
}

func (o *Output) verifyUnchanged(ctx context.Context, m remote.WorkerManifest, exported workspacearchive.Manifest) error {
	a, e := o.c.Sources.Open(ctx, m.Source)
	if e != nil {
		return e
	}
	defer a.Close()
	s, e := workspacearchive.ImportZIP(ctx, o.c.StageRoot, a, a.Size(), o.c.Limits)
	if e != nil {
		return e
	}
	defer s.Close()
	var importedSHA, importedManifest string
	var importedBytes int64
	if e = o.c.DB.QueryRowContext(ctx, `SELECT archive_sha256,archive_bytes,manifest_sha256 FROM remote_workspace_imports WHERE operation_id=?`, m.ImportID).Scan(&importedSHA, &importedBytes, &importedManifest); e != nil {
		return e
	}
	if importedSHA != m.Source.SHA256 || importedBytes != m.Source.Bytes || importedManifest != s.Digest {
		return remote.ErrConflict
	}
	if s.Manifest.ArchiveSHA256 != m.Source.SHA256 || a.Size() != m.Source.Bytes || !sameContents(s.Manifest, exported) {
		return remote.ErrConflict
	}

	return nil
}
