package worker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"kingdom/internal/execution/openshell/managed"
	pb "kingdom/internal/execution/openshell/managedpb"
	"kingdom/internal/execution/providerrequest"
	"kingdom/internal/execution/remote"
	"kingdom/internal/securefs"
	"kingdom/internal/workspacearchive"
	transfer "kingdom/internal/workspacetransfer"
)

// Download retains a verified durable archive and a disposable validated tree.
// Closing Staged never removes the canonical spool file.
type Download struct {
	OperationID    string
	SpoolName      string
	ArchiveSHA256  string
	ArchiveBytes   int64
	ManifestSHA256 string
	Staged         *workspacearchive.Staged
}

// Export retrieves one fixed project or receipt archive after all writers and
// capabilities close. SpoolRoot must be operator-owned, private, and separate
// from project inputs. An uncertain download is never retried with a new helper.
func (w *Worker) Export(ctx context.Context, p providerrequest.Preparation, r remote.Reservation, owner managed.Owner, id string, purpose remote.ExecutionPurpose, t Transfers, services ServerServices, spoolRoot string) (*Download, error) {
	if !ownerMatches(owner, r.Reference) || t.Forwarder == nil || services.Models == nil || services.Plugins == nil || t.Timeout < time.Second || t.Timeout > time.Hour || t.StageRoot == "" || spoolRoot == "" {
		return nil, remote.ErrConflict
	}
	parsed, e := uuid.Parse(id)
	if e != nil || parsed == uuid.Nil || parsed.String() != id {
		return nil, remote.ErrConflict
	}
	mode := "export"
	if purpose == remote.NativeReceipts {
		mode = "receipts"
	} else if purpose != remote.WorkspaceExport {
		return nil, remote.ErrConflict
	}
	ctx, cancel := context.WithTimeout(ctx, t.Timeout+15*time.Second)
	defer cancel()
	binding, e := w.binding(ctx, p, r)
	if e != nil {
		return nil, e
	}
	requireClosed := func() error {
		if e := services.Models.RequireClosed(ctx, binding); e != nil {
			return e
		}
		return services.Plugins.RequireClosed(ctx, binding)
	}
	if e = requireClosed(); e != nil {
		return nil, e
	}
	info, e := os.Lstat(spoolRoot)
	if e != nil || !info.IsDir() || info.Mode().Perm() != 0700 {
		return nil, remote.ErrConflict
	}
	root, e := securefs.OpenRoot(spoolRoot)
	if e != nil {
		return nil, e
	}
	defer root.Close()
	name := id + ".zip"
	var request *pb.StartExecutionRequest
	e = w.transaction(ctx, func(tx *sql.Tx) error {
		var err error
		if err = w.exactRuntime(ctx, tx, r); err != nil {
			return err
		}
		if _, err = remote.ReadWriterClosures(ctx, tx, r.Owner, r.Reference); err != nil {
			return err
		}
		request, err = w.requests.ReadManaged(ctx, tx, r.Owner, r.Reference, id)
		if err == nil {
			return nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		nonce, err := randomHex()
		if err != nil {
			return err
		}
		job := transfer.Job{Version: 1, Mode: mode, Nonce: nonce, HelperSHA256: t.HelperSHA256, Limits: t.Limits, TimeoutSeconds: int64(t.Timeout / time.Second)}
		spec, err := transfer.Command(job)
		if err != nil {
			return err
		}
		request = &pb.StartExecutionRequest{Execution: &pb.ExecutionSelector{Workspace: w.config.Workspace, Sandbox: nativeName(r.Reference), RequestId: id, Runtime: proto.Clone(owner.Runtime).(*pb.RuntimeBinding)}, Command: spec}
		if _, err = remote.PrepareManagedExecutionTx(ctx, tx, r.Owner, r.Reference, purpose, owner, request); err != nil {
			return err
		}
		return w.requests.SaveManagedTx(ctx, tx, r.Owner, r.Reference, request)
	})
	if e != nil {
		return nil, e
	}
	job, e := transfer.DecodeJob(bytes.NewReader(request.Command.Stdin))
	if e != nil {
		return nil, e
	}
	fixed, e := transfer.Command(job)
	if e != nil || !proto.Equal(fixed, request.Command) || job.Mode != mode || job.HelperSHA256 != t.HelperSHA256 || job.Limits != t.Limits {
		return nil, remote.ErrConflict
	}
	client, e := managed.New(ctx, w.wire, owner, w.config.Workspace, nativeName(r.Reference))
	if e != nil {
		return nil, e
	}
	if e = requireClosed(); e != nil {
		return nil, e
	}
	var send bool
	e = w.transaction(ctx, func(tx *sql.Tx) error {
		var err error
		send, err = w.requests.ClaimManagedDispatchTx(ctx, tx, r.Owner, r.Reference, id)
		return err
	})
	if e != nil {
		return nil, e
	}
	var status *pb.ExecutionStatus
	if send {
		status, e = client.Start(ctx, id, request.Command)
	} else {
		status, e = client.ObserveStart(ctx, request)
	}
	if e != nil {
		return nil, ErrUnresolved
	}
	if e = w.retainIdentity(ctx, r, status); e != nil {
		return nil, e
	}
	existing, e := root.OpenRegular(name)
	if e == nil {
		existing.Close()
	} else if !errors.Is(e, os.ErrNotExist) {
		return nil, e
	} else if status.State != pb.ExecutionState_EXECUTION_STATE_CLOSED {
		s := &Server{worker: w, reservation: r, owner: owner, client: client, services: services, identity: status.Identity}
		s.services.Forwarder = t.Forwarder
		transport, err := s.openTunnel(ctx, transfer.Port, "Bearer "+job.Nonce)
		if err != nil {
			return nil, err
		}
		transferErr := func() error {
			for {
				ready, _ := http.NewRequestWithContext(ctx, http.MethodGet, "https://managed.invalid/v1/ready", nil)
				response, err := transport.RoundTrip(ready)
				if err == nil {
					code := response.StatusCode
					response.Body.Close()
					if code == http.StatusConflict {
						return ErrUnresolved
					}
					if code == http.StatusNoContent {
						break
					}
				}
				current, err := client.Get(ctx, status.Identity)
				if err != nil || current.State == pb.ExecutionState_EXECUTION_STATE_CLOSED {
					return ErrUnresolved
				}
				select {
				case <-ctx.Done():
					return ErrUnresolved
				case <-time.After(100 * time.Millisecond):
				}
			}
			if err := requireClosed(); err != nil {
				return err
			}
			req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "https://managed.invalid/v1/archive", nil)
			response, err := transport.RoundTrip(req)
			if err != nil {
				return ErrUnresolved
			}
			defer response.Body.Close()
			if response.StatusCode != http.StatusOK || response.ContentLength > job.Limits.ArchiveBytes {
				return ErrUnresolved
			}
			return root.ReplaceRegularFrom(ctx, name, func(out io.Writer) error {
				n, err := securefs.Copy(ctx, out, io.LimitReader(response.Body, job.Limits.ArchiveBytes+1))
				if err != nil {
					return err
				}
				if n < 1 || n > job.Limits.ArchiveBytes {
					return remote.ErrConflict
				}
				return nil
			}, 0600, 0700, time.Time{})
		}()
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		closeErr := transport.Close(closeCtx)
		closeCancel()
		if transferErr != nil || closeErr != nil {
			return nil, ErrUnresolved
		}
	}
	for status.State != pb.ExecutionState_EXECUTION_STATE_CLOSED {
		select {
		case <-ctx.Done():
			return nil, ErrUnresolved
		case <-time.After(100 * time.Millisecond):
		}
		status, e = client.Get(ctx, status.Identity)
		if e != nil {
			return nil, ErrUnresolved
		}
	}
	output, e := w.retainTerminal(ctx, client, r, status)
	if e != nil {
		return nil, e
	}
	if !managed.SuccessfulProcess(status.Terminal) {
		return nil, ErrUnresolved
	}
	file, e := root.OpenRegular(name)
	if e != nil {
		return nil, ErrUnresolved
	}
	defer file.Close()
	stat, e := file.Stat()
	if e != nil || stat.Size() < 1 || stat.Size() > job.Limits.ArchiveBytes {
		return nil, remote.ErrConflict
	}
	staged, e := transfer.ValidateExport(ctx, t.StageRoot, file, stat.Size(), bytes.NewReader(output), job)
	if e != nil {
		return nil, e
	}
	result := &Download{OperationID: id, SpoolName: name, ArchiveSHA256: staged.Manifest.ArchiveSHA256, ArchiveBytes: stat.Size(), ManifestSHA256: staged.Digest, Staged: staged}
	// Rehash the pinned descriptor after validation; a different spool cannot be
	// adopted merely because its filename matches an operation UUID.
	h := sha256.New()
	if _, e = io.Copy(h, io.NewSectionReader(file, 0, stat.Size())); e != nil || hex.EncodeToString(h.Sum(nil)) != result.ArchiveSHA256 {
		staged.Close()
		return nil, remote.ErrConflict
	}
	if e = file.Sync(); e != nil {
		staged.Close()
		return nil, e
	}
	if e = root.SyncDir("."); e != nil {
		staged.Close()
		return nil, e
	}
	e = w.transaction(ctx, func(tx *sql.Tx) error {
		if err := w.exactRuntime(ctx, tx, r); err != nil {
			return err
		}
		receipt, found, err := remote.ReadManagedReceipt(ctx, tx, r.Owner, r.Reference, id)
		if err != nil {
			return err
		}
		if !found || !managed.SuccessfulProcess(receipt) {
			return remote.ErrConflict
		}
		_, err = tx.ExecContext(ctx, `INSERT OR IGNORE INTO remote_workspace_downloads(operation_id,purpose,spool_name,archive_sha256,archive_bytes,manifest_sha256,receipt_sha256)VALUES(?,?,?,?,?,?,?)`, id, string(purpose), name, result.ArchiveSHA256, result.ArchiveBytes, result.ManifestSHA256, receipt.ReceiptSha256)
		if err != nil {
			return err
		}
		var savedPurpose, savedName, archive, manifest string
		var size int64
		var digest []byte
		err = tx.QueryRowContext(ctx, `SELECT purpose,spool_name,archive_sha256,archive_bytes,manifest_sha256,receipt_sha256 FROM remote_workspace_downloads WHERE operation_id=?`, id).Scan(&savedPurpose, &savedName, &archive, &size, &manifest, &digest)
		if err != nil {
			return err
		}
		if savedPurpose != string(purpose) || savedName != name || archive != result.ArchiveSHA256 || size != result.ArchiveBytes || manifest != result.ManifestSHA256 || !bytes.Equal(digest, receipt.ReceiptSha256) {
			return remote.ErrConflict
		}
		return nil
	})
	if e != nil {
		staged.Close()
		return nil, e
	}
	return result, nil
}

// OpenDownload reads sealed evidence without contacting a live sandbox. It is
// available after finalization and deletion, and grants no new remote action.
func (w *Worker) OpenDownload(ctx context.Context, r remote.Reservation, id string, spoolRoot, stageRoot string) (*Download, error) {
	if e := w.exactRuntime(ctx, w.db, r); e != nil {
		return nil, e
	}
	request, e := w.requests.ReadManaged(ctx, w.db, r.Owner, r.Reference, id)
	if e != nil {
		return nil, e
	}
	job, e := transfer.DecodeJob(bytes.NewReader(request.Command.Stdin))
	if e != nil || (job.Mode != "export" && job.Mode != "receipts") {
		return nil, remote.ErrConflict
	}
	result := &Download{OperationID: id}
	var purpose string
	var receiptSHA []byte
	e = w.db.QueryRowContext(ctx, `SELECT purpose,spool_name,archive_sha256,archive_bytes,manifest_sha256,receipt_sha256 FROM remote_workspace_downloads WHERE operation_id=?`, id).Scan(&purpose, &result.SpoolName, &result.ArchiveSHA256, &result.ArchiveBytes, &result.ManifestSHA256, &receiptSHA)
	if e != nil {
		return nil, e
	}
	expectedPurpose := remote.WorkspaceExport
	if job.Mode == "receipts" {
		expectedPurpose = remote.NativeReceipts
	}
	if purpose != string(expectedPurpose) || result.SpoolName != id+".zip" {
		return nil, remote.ErrConflict
	}
	receipt, found, e := remote.ReadManagedReceipt(ctx, w.db, r.Owner, r.Reference, id)
	if e != nil {
		return nil, e
	}
	if !found || !managed.SuccessfulProcess(receipt) || !bytes.Equal(receipt.ReceiptSha256, receiptSHA) {
		return nil, remote.ErrConflict
	}
	root, e := securefs.OpenRoot(spoolRoot)
	if e != nil {
		return nil, e
	}
	defer root.Close()
	file, e := root.OpenRegular(result.SpoolName)
	if e != nil {
		return nil, e
	}
	defer file.Close()
	stat, e := file.Stat()
	if e != nil || stat.Size() != result.ArchiveBytes {
		return nil, remote.ErrConflict
	}
	staged, e := workspacearchive.ImportZIP(ctx, stageRoot, file, stat.Size(), job.Limits)
	if e != nil {
		return nil, e
	}
	if staged.Manifest.ArchiveSHA256 != result.ArchiveSHA256 || staged.Digest != result.ManifestSHA256 {
		staged.Close()
		return nil, remote.ErrConflict
	}
	result.Staged = staged
	return result, nil
}
