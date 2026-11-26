package worker

import (
	"archive/zip"
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	sdk "github.com/NVIDIA/OpenShell/sdk/go/openshell/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"kingdom/internal/execution/openshell/managed"
	pb "kingdom/internal/execution/openshell/managedpb"
	"kingdom/internal/execution/remote"
	"kingdom/internal/workspacearchive"
	transfer "kingdom/internal/workspacetransfer"
)

func TestExportRecoversSyncedOrphanOnlyWithMatchingTerminalManifest(t *testing.T) {
	for _, mode := range []string{"exact", "foreign", "download"} {
		t.Run(mode, func(t *testing.T) {
			corrupt := mode == "foreign"
			ctx := context.Background()
			w, p, r, owner := serverFixture(t)
			var archive bytes.Buffer
			z := zip.NewWriter(&archive)
			f, e := z.Create("project.txt")
			if e != nil {
				t.Fatal(e)
			}
			f.Write([]byte("retained result"))
			z.Close()
			stage, e := workspacearchive.ImportZIP(ctx, t.TempDir(), bytes.NewReader(archive.Bytes()), int64(archive.Len()), transfer.DefaultLimits())
			if e != nil {
				t.Fatal(e)
			}
			defer stage.Close()
			id := uuid.NewString()
			job := transfer.Job{Version: 1, Mode: "export", Nonce: strings.Repeat("a", 64), HelperSHA256: strings.Repeat("b", 64), Limits: transfer.DefaultLimits(), TimeoutSeconds: 60}
			spec, e := transfer.Command(job)
			if e != nil {
				t.Fatal(e)
			}
			request := &pb.StartExecutionRequest{Execution: &pb.ExecutionSelector{Workspace: w.config.Workspace, Sandbox: nativeName(r.Reference), RequestId: id, Runtime: owner.Runtime}, Command: spec}
			e = w.transaction(ctx, func(tx *sql.Tx) error {
				if e := remote.CloseAdmissionTx(ctx, tx, r.Owner, r.Reference); e != nil {
					return e
				}
				if _, e := remote.PrepareManagedExecutionTx(ctx, tx, r.Owner, r.Reference, remote.WorkspaceExport, owner, request); e != nil {
					return e
				}
				if e := w.requests.SaveManagedTx(ctx, tx, r.Owner, r.Reference, request); e != nil {
					return e
				}
				_, e := w.requests.ClaimManagedDispatchTx(ctx, tx, r.Owner, r.Reference, id)
				return e
			})
			if e != nil {
				t.Fatal(e)
			}
			original, _ := managed.Digest(request)
			effective, _ := managed.Digest(spec)
			identity := &pb.CommandIdentity{SchemaVersion: 1, GatewayId: owner.GatewayID, WorkspaceId: owner.WorkspaceID, SubjectId: owner.SubjectID, Method: managed.StartMethod, RequestId: id, BoundaryRequestId: id, Runtime: owner.Runtime, OriginalRequestSha256: original, EffectiveSpecSha256: effective}
			data, _ := json.Marshal(transfer.Completion{Version: 1, Mode: "export", ArchiveSHA256: stage.Manifest.ArchiveSHA256, ArchiveBytes: int64(archive.Len()), ManifestSHA256: stage.Digest, ExpandedBytes: stage.Manifest.ExpandedBytes, Entries: len(stage.Manifest.Entries)})
			chunk := &pb.OutputChunk{Sequence: 1, Stream: pb.OutputStream_OUTPUT_STREAM_STDOUT, Data: data}
			raw, _ := managed.Canonical(chunk)
			var prefix [4]byte
			binary.BigEndian.PutUint32(prefix[:], uint32(len(raw)))
			h := sha256.New()
			h.Write(prefix[:])
			h.Write(raw)
			receipt := &pb.TerminalReceipt{SchemaVersion: 1, Identity: identity, ExitStatus: &pb.TerminalReceipt_ExitCode{}, GroupClosed: true, ProcessClean: true, InputComplete: true, Output: &pb.OutputManifest{LastSequence: 1, TotalBytes: uint64(len(data)), Sha256: h.Sum(nil), Complete: true}}
			receipt.ReceiptSha256, _ = managed.ReceiptDigest(receipt)
			wire := &transferFixture{managedFixture: managedFixture{workspace: "workspace-id"}, chunk: chunk, status: &pb.ExecutionStatus{Identity: identity, State: pb.ExecutionState_EXECUTION_STATE_CLOSED, DurableOutputSequence: 1, Terminal: receipt}}
			live := &exportTransportFixture{transferFixture: wire, archive: archive.Bytes()}
			w.wire = live
			spool := t.TempDir()
			os.Chmod(spool, 0700)
			rawArchive := archive.Bytes()
			if corrupt {
				rawArchive = []byte("foreign bytes")
			}
			if mode != "download" {
				if e = os.WriteFile(spool+"/"+id+".zip", rawArchive, 0600); e != nil {
					t.Fatal(e)
				}
			} else {
				live.running = true
			}
			transfers := Transfers{Forwarder: neverForward{}, StageRoot: t.TempDir(), HelperSHA256: job.HelperSHA256, Limits: job.Limits, Timeout: time.Minute}
			if mode == "download" {
				transfers.Forwarder = live
			}
			services := ServerServices{Models: &failingModel{}, Plugins: &pluginFixture{}}
			result, e := w.Export(ctx, p, r, owner, id, remote.WorkspaceExport, transfers, services, spool)
			if corrupt {
				if e == nil {
					result.Staged.Close()
					t.Fatal("foreign orphan adopted")
				}
				var n int
				w.db.QueryRow(`SELECT count(*) FROM remote_workspace_downloads`).Scan(&n)
				if n != 0 {
					t.Fatal("unverified download retained")
				}
				return
			}
			if e != nil {
				t.Fatal(e)
			}
			defer result.Staged.Close()
			if result.ArchiveSHA256 != stage.Manifest.ArchiveSHA256 || result.ManifestSHA256 != stage.Digest || wire.starts != 0 {
				t.Fatal("recovery changed artifact or repeated launch")
			}
			replay, e := w.Export(ctx, p, r, owner, id, remote.WorkspaceExport, transfers, services, spool)
			if e != nil {
				t.Fatal(e)
			}
			replay.Staged.Close()

			// Retained artifact reads need no native or managed transport.
			w.wire = nil
			offline, e := w.OpenDownload(ctx, r, id, spool, t.TempDir())
			if e != nil {
				t.Fatal(e)
			}
			offline.Staged.Close()
			if _, e = w.db.Exec(`UPDATE remote_workspace_downloads SET archive_bytes=1`); e == nil {
				t.Fatal("retained download mutable")
			}
		})
	}
}

// The stream exercises the native tunnel path without a host listener.
type exportTransportFixture struct {
	*transferFixture
	mu      sync.Mutex
	running bool
	archive []byte
}

func (f *exportTransportFixture) GetExecution(context.Context, *pb.GetExecutionRequest, ...grpc.CallOption) (*pb.ExecutionStatus, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	s := proto.Clone(f.status).(*pb.ExecutionStatus)
	if f.running {
		s.State = pb.ExecutionState_EXECUTION_STATE_RUNNING
		s.Terminal = nil
		s.DurableOutputSequence = 0
	}
	return s, nil
}
func (f *exportTransportFixture) Forward(ctx context.Context, _ string, _ string, _ uint32, _ ...sdk.ForwardOption) (io.ReadWriteCloser, error) {
	client, server := net.Pipe()
	go func() {
		defer server.Close()
		req, e := http.ReadRequest(bufio.NewReader(server))
		if e != nil {
			return
		}
		defer req.Body.Close()
		body := []byte{}
		code := http.StatusNoContent
		if req.URL.Path == "/v1/archive" {
			body = f.archive
			code = http.StatusOK
		}
		response := &http.Response{StatusCode: code, ProtoMajor: 1, ProtoMinor: 1, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(body)), ContentLength: int64(len(body))}
		response.Write(server)
		if code == http.StatusOK {
			f.mu.Lock()
			f.running = false
			f.mu.Unlock()
		}
	}()
	return client, nil
}
