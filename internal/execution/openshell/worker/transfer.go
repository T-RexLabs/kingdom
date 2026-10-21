package worker

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"time"

	"google.golang.org/protobuf/proto"
	"kingdom/internal/execution/openshell/managed"
	pb "kingdom/internal/execution/openshell/managedpb"
	"kingdom/internal/execution/openshell/tunnel"
	"kingdom/internal/execution/providerrequest"
	"kingdom/internal/execution/remote"
	"kingdom/internal/workspacearchive"
	transfer "kingdom/internal/workspacetransfer"
)

// WorkspaceSource resolves the immutable input retained for this preparation.
// It must return the same bytes after restart; it never accepts browser paths.
type WorkspaceSource interface {
	Open(context.Context, providerrequest.Preparation) (Archive, error)
}
type Archive interface {
	io.ReaderAt
	io.Closer
	Size() int64
}
type Transfers struct {
	Source       WorkspaceSource
	Forwarder    tunnel.Forwarder
	StageRoot    string
	HelperSHA256 string
	Limits       workspacearchive.Limits
	Timeout      time.Duration
}

// Import prepares the workspace before provider credentials or server launch.
// The caller retains commandID with the invocation. All command dispatch and
// terminal evidence use the canonical encrypted ledger. HTTP delivery alone
// never establishes successful import.
func (w *Worker) Import(ctx context.Context, p providerrequest.Preparation, r remote.Reservation, expected managed.Owner, commandID string, t Transfers) error {
	if expected.Validate() != nil || r.Reference.Target != w.config.Target || r.Owner.Kind != string(p.Owner.Kind) || r.Owner.InvocationID != p.Owner.InvocationID || r.Owner.LeaseOwner != p.Lease.Owner || r.Owner.LeaseVersion != p.Lease.Version {
		return remote.ErrConflict
	}
	if t.Source == nil || t.Forwarder == nil || t.StageRoot == "" || t.Timeout < time.Second || t.Timeout > time.Hour {
		return remote.ErrConflict
	}
	ctx, cancel := context.WithTimeout(ctx, t.Timeout+15*time.Second)
	defer cancel()
	archive, e := t.Source.Open(ctx, p)
	if e != nil {
		return e
	}
	if archive == nil {
		return remote.ErrConflict
	}
	defer archive.Close()
	staged, e := workspacearchive.ImportZIP(ctx, t.StageRoot, archive, archive.Size(), t.Limits)
	if e != nil {
		return e
	}
	defer staged.Close()
	var request *pb.StartExecutionRequest
	e = w.transaction(ctx, func(tx *sql.Tx) error {
		var e error
		request, e = w.requests.ReadManaged(ctx, tx, r.Owner, r.Reference, commandID)
		if e == nil {
			return nil
		}
		if !errors.Is(e, sql.ErrNoRows) {
			return e
		}
		if e = w.check(ctx, tx, p, r.Owner.BindingID); e != nil {
			return e
		}
		nonce := make([]byte, 32)
		if _, e = rand.Read(nonce); e != nil {
			return e
		}
		job := transfer.Job{Version: 1, Mode: "import", Nonce: hex.EncodeToString(nonce), HelperSHA256: t.HelperSHA256, Limits: t.Limits, TimeoutSeconds: int64(t.Timeout / time.Second), ArchiveSHA256: staged.Manifest.ArchiveSHA256, ArchiveBytes: archive.Size()}
		spec, e := transfer.Command(job)
		if e != nil {
			return e
		}
		request = &pb.StartExecutionRequest{Execution: &pb.ExecutionSelector{Workspace: w.config.Workspace, Sandbox: nativeName(r.Reference), RequestId: commandID, Runtime: proto.Clone(expected.Runtime).(*pb.RuntimeBinding)}, Command: spec}
		if _, e = remote.PrepareManagedExecutionTx(ctx, tx, r.Owner, r.Reference, remote.WorkspaceImport, expected, request); e != nil {
			return e
		}
		return w.requests.SaveManagedTx(ctx, tx, r.Owner, r.Reference, request)
	})
	if e != nil {
		return e
	}
	job, e := transfer.DecodeJob(bytes.NewReader(request.Command.Stdin))
	if e != nil {
		return e
	}
	if job.Mode != "import" || job.HelperSHA256 != t.HelperSHA256 || job.Limits != t.Limits || job.ArchiveBytes != archive.Size() || job.ArchiveSHA256 != staged.Manifest.ArchiveSHA256 {
		return remote.ErrConflict
	}
	fixed, e := transfer.Command(job)
	if e != nil || !proto.Equal(fixed, request.Command) {
		return remote.ErrConflict
	}
	c, e := managed.New(ctx, w.wire, expected, w.config.Workspace, nativeName(r.Reference))
	if e != nil {
		return e
	}
	var send bool
	e = w.transaction(ctx, func(tx *sql.Tx) error {
		op, e := remote.ReadOperation(ctx, tx, commandID)
		if e != nil {
			return e
		}
		if op.Delivery != remote.Prepared {
			return nil
		}
		if e = w.check(ctx, tx, p, r.Owner.BindingID); e != nil {
			return e
		}
		send, e = w.requests.ClaimManagedDispatchTx(ctx, tx, r.Owner, r.Reference, commandID)
		return e
	})
	if e != nil {
		return e
	}
	var status *pb.ExecutionStatus
	if send {
		status, e = c.Start(ctx, commandID, request.Command)
	} else {
		status, e = c.ObserveStart(ctx, request)
	}
	if e != nil {
		return ErrUnresolved
	}
	if e = w.retainIdentity(ctx, r, status); e != nil {
		return e
	}
	if status.State != pb.ExecutionState_EXECUTION_STATE_CLOSED {
		transport, e := tunnel.New(ctx, tunnel.Config{Forwarder: t.Forwarder, Target: tunnel.Target{Workspace: w.config.Workspace, Allocation: uint64(r.Reference.Allocation), Owner: expected, Port: transfer.Port}, MaxConnections: 2, OpenTimeout: 5 * time.Second, Verify: func(check context.Context, _ tunnel.Target) error {
			current, e := managed.Discover(check, w.wire, w.config.Workspace, nativeName(r.Reference), w.config.Target.GatewayID, w.config.ProfileSHA256)
			if e != nil {
				return e
			}
			if !sameOwner(current, expected) {
				return remote.ErrConflict
			}
			return nil
		}}, "Bearer "+job.Nonce)
		if e != nil {
			return e
		}
		client := &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return remote.ErrConflict }}
		// A ready helper has not claimed its one upload. A claimed helper is only
		// observed; an uncertain HTTP upload is never repeated by this call.
		for {
			ready, _ := http.NewRequestWithContext(ctx, "GET", "https://managed.invalid/v1/ready", nil)
			response, readErr := client.Do(ready)
			if readErr == nil {
				code := response.StatusCode
				response.Body.Close()
				if code == http.StatusConflict {
					break
				}
				if code == http.StatusNoContent {
					// Recheck authority immediately before first upload. This is a new effect,
					// even if the helper admission was recovered from a previous worker turn.
					e = w.transaction(ctx, func(tx *sql.Tx) error { return w.check(ctx, tx, p, r.Owner.BindingID) })
					if e == nil {
						upload, _ := http.NewRequestWithContext(ctx, "PUT", "https://managed.invalid/v1/archive", io.NewSectionReader(archive, 0, archive.Size()))
						upload.ContentLength = archive.Size()
						reply, uploadErr := client.Do(upload)
						e = uploadErr
						if reply != nil {
							reply.Body.Close()
							if reply.StatusCode != http.StatusNoContent {
								e = ErrUnresolved
							}
						}
					}
					break
				}
				if code != http.StatusServiceUnavailable {
					e = ErrUnresolved
					break
				}
			}
			status, readErr = c.Get(ctx, status.Identity)
			if readErr != nil {
				e = ErrUnresolved
				break
			}
			if status.State == pb.ExecutionState_EXECUTION_STATE_CLOSED {
				break
			}
			select {
			case <-ctx.Done():
				e = ctx.Err()
			case <-time.After(100 * time.Millisecond):
			}
			if e != nil {
				break
			}
		}
		closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		closeErr := transport.Close(closeCtx)
		cancel()
		if e != nil || closeErr != nil {
			return ErrUnresolved
		}
	}
	for status.State != pb.ExecutionState_EXECUTION_STATE_CLOSED {
		select {
		case <-ctx.Done():
			return ErrUnresolved
		case <-time.After(100 * time.Millisecond):
		}
		status, e = c.Get(ctx, status.Identity)
		if e != nil {
			return ErrUnresolved
		}
	}
	output, e := w.retainTerminal(ctx, c, r, status)
	if e != nil {
		return e
	}
	if !managed.SuccessfulProcess(status.Terminal) {
		return ErrUnresolved
	}
	if e = transfer.ValidateImport(bytes.NewReader(output), job, staged); e != nil {
		return e
	}
	return w.transaction(ctx, func(tx *sql.Tx) error {
		return remote.VerifyWorkspaceImportTx(ctx, tx, r.Owner, r.Reference, commandID, w.requests, staged)
	})
}
func nativeName(r remote.Reference) string {
	return (tunnel.Target{Allocation: uint64(r.Allocation)}).Name()
}
func sameOwner(a, b managed.Owner) bool {
	return a.GatewayID == b.GatewayID && a.WorkspaceID == b.WorkspaceID && a.SubjectID == b.SubjectID && proto.Equal(a.Runtime, b.Runtime)
}
func (w *Worker) retainIdentity(ctx context.Context, r remote.Reservation, status *pb.ExecutionStatus) error {
	return w.transaction(ctx, func(tx *sql.Tx) error {
		if e := remote.RecordManagedIdentityTx(ctx, tx, r.Owner, r.Reference, status.Identity); e != nil {
			return e
		}
		op, e := remote.ReadOperation(ctx, tx, status.Identity.RequestId)
		if e != nil {
			return e
		}
		digest, e := managed.Digest(status.Identity)
		if e != nil {
			return e
		}
		return remote.RecordAdmissionTx(ctx, tx, r.Owner, r.Reference, status.Identity.RequestId, op.PayloadDigest, hex.EncodeToString(digest))
	})
}
func (w *Worker) retainTerminal(ctx context.Context, c *managed.Client, r remote.Reservation, status *pb.ExecutionStatus) ([]byte, error) {
	if status.Terminal == nil {
		return nil, ErrUnresolved
	}
	var output []byte
	var sequence uint64
	for sequence < status.Terminal.Output.LastSequence {
		next, e := c.Read(ctx, status.Identity, sequence, 64, func(chunk *pb.OutputChunk) error {
			if chunk.Stream == pb.OutputStream_OUTPUT_STREAM_STDOUT {
				if len(output)+len(chunk.Data) > transfer.MaxCompletionBytes {
					return remote.ErrConflict
				}
				output = append(output, chunk.Data...)
			}
			return w.transaction(ctx, func(tx *sql.Tx) error {
				return remote.AppendManagedOutputTx(ctx, tx, r.Owner, r.Reference, status.Identity.RequestId, chunk)
			})
		})
		if e != nil {
			return nil, e
		}
		if next == sequence {
			return nil, ErrUnresolved
		}
		sequence = next
	}
	e := w.transaction(ctx, func(tx *sql.Tx) error {
		return remote.RecordManagedReceiptTx(ctx, tx, r.Owner, r.Reference, status.Identity.RequestId, status.Terminal)
	})
	return output, e
}
