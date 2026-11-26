package worker

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	sdk "github.com/NVIDIA/OpenShell/sdk/go/openshell/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"io"
	"kingdom/internal/execution/openshell/managed"
	pb "kingdom/internal/execution/openshell/managedpb"
	"kingdom/internal/execution/providerrequest"
	"kingdom/internal/execution/remote"
	"kingdom/internal/workspacearchive"
	transfer "kingdom/internal/workspacetransfer"
	"strings"
	"testing"
	"time"
)

type archiveFixture struct{ *bytes.Reader }

func (a archiveFixture) Close() error { return nil }

type sourceFixture struct{ data []byte }

func (s sourceFixture) Open(context.Context, providerrequest.Preparation) (Archive, error) {
	return archiveFixture{bytes.NewReader(s.data)}, nil
}

type neverForward struct{}

func (neverForward) Forward(context.Context, string, string, uint32, ...sdk.ForwardOption) (io.ReadWriteCloser, error) {
	return nil, errors.New("closed process cannot open tunnel")
}

type outputFixture struct {
	grpc.ClientStream
	chunk *pb.OutputChunk
}

func (s *outputFixture) Recv() (*pb.OutputChunk, error) {
	if s.chunk == nil {
		return nil, io.EOF
	}
	c := s.chunk
	s.chunk = nil
	return c, nil
}

type transferFixture struct {
	managedFixture
	expected *workspacearchive.Staged
	chunk    *pb.OutputChunk
	status   *pb.ExecutionStatus
	starts   int
	lost     bool
	wrong    bool
}

func (f *transferFixture) StartExecution(_ context.Context, r *pb.StartExecutionRequest, _ ...grpc.CallOption) (*pb.ExecutionStatus, error) {
	f.starts++
	job, e := transfer.DecodeJob(bytes.NewReader(r.Command.Stdin))
	if e != nil {
		return nil, e
	}
	original, _ := managed.Digest(r)
	effective, _ := managed.Digest(r.Command)
	identity := &pb.CommandIdentity{SchemaVersion: 1, GatewayId: "gateway", WorkspaceId: "workspace-id", SubjectId: "subject", Method: managed.StartMethod, RequestId: r.Execution.RequestId, BoundaryRequestId: r.Execution.RequestId, Runtime: r.Execution.Runtime, OriginalRequestSha256: original, EffectiveSpecSha256: effective}
	digest := f.expected.Digest
	if f.wrong {
		digest = strings.Repeat("a", 64)
	}
	data, _ := json.Marshal(transfer.Completion{Version: 1, Mode: "import", ArchiveSHA256: job.ArchiveSHA256, ArchiveBytes: job.ArchiveBytes, ManifestSHA256: digest, Entries: len(f.expected.Manifest.Entries), ExpandedBytes: f.expected.Manifest.ExpandedBytes})
	f.chunk = &pb.OutputChunk{Sequence: 1, Stream: pb.OutputStream_OUTPUT_STREAM_STDOUT, Data: data}
	raw, _ := managed.Canonical(f.chunk)
	var prefix [4]byte
	binary.BigEndian.PutUint32(prefix[:], uint32(len(raw)))
	h := sha256.New()
	h.Write(prefix[:])
	h.Write(raw)
	receipt := &pb.TerminalReceipt{SchemaVersion: 1, Identity: identity, ExitStatus: &pb.TerminalReceipt_ExitCode{}, GroupClosed: true, ProcessClean: true, InputComplete: true, Output: &pb.OutputManifest{LastSequence: 1, TotalBytes: uint64(len(data)), Sha256: h.Sum(nil), Complete: true}}
	receipt.ReceiptSha256, _ = managed.ReceiptDigest(receipt)
	f.status = &pb.ExecutionStatus{Identity: identity, State: pb.ExecutionState_EXECUTION_STATE_CLOSED, DurableOutputSequence: 1, Terminal: receipt}
	if f.lost {
		return nil, errors.New("reply lost")
	}
	return f.status, nil
}
func (f *transferFixture) GetExecution(context.Context, *pb.GetExecutionRequest, ...grpc.CallOption) (*pb.ExecutionStatus, error) {
	return f.status, nil
}
func (f *transferFixture) ReadOutput(context.Context, *pb.ReadOutputRequest, ...grpc.CallOption) (grpc.ServerStreamingClient[pb.OutputChunk], error) {
	return &outputFixture{chunk: f.chunk}, nil
}
func TestImportRecoveryRetainsExactCommandAndVerifiedWorkspace(t *testing.T) {
	for _, wrong := range []bool{false, true} {
		t.Run(map[bool]string{false: "matching", true: "wrong manifest"}[wrong], func(t *testing.T) {
			w, p, _ := fixture(t)
			ctx := context.Background()
			r, e := w.Reserve(ctx, p, "binding", uuid.NewString())
			if e != nil {
				t.Fatal(e)
			}
			r, owner, e := w.Provision(ctx, p, r)
			if e != nil {
				t.Fatal(e)
			}
			var b bytes.Buffer
			z := zip.NewWriter(&b)
			file, _ := z.Create("file")
			file.Write([]byte("content"))
			if e = z.Close(); e != nil {
				t.Fatal(e)
			}
			expected, e := workspacearchive.ImportZIP(ctx, t.TempDir(), bytes.NewReader(b.Bytes()), int64(b.Len()), transfer.DefaultLimits())
			if e != nil {
				t.Fatal(e)
			}
			defer expected.Close()
			wire := &transferFixture{managedFixture: managedFixture{workspace: "workspace-id"}, expected: expected, lost: true, wrong: wrong}
			w.wire = wire
			transferConfig := Transfers{Source: sourceFixture{b.Bytes()}, Forwarder: neverForward{}, StageRoot: t.TempDir(), HelperSHA256: strings.Repeat("b", 64), Limits: transfer.DefaultLimits(), Timeout: time.Minute}
			id := uuid.NewString()
			if e = w.Import(ctx, p, r, owner, id, transferConfig); !errors.Is(e, ErrUnresolved) {
				t.Fatal(e)
			}
			// Recovery accepts exact evidence without sending again, even after lease expiry.
			w.config.Now = func() time.Time { return time.Now().Add(2 * time.Hour) }
			e = w.Import(ctx, p, r, owner, id, transferConfig)
			if wrong && e == nil || !wrong && e != nil {
				t.Fatal("unexpected import verification", e)
			}
			if wire.starts != 1 {
				t.Fatal("uncertain helper was restarted")
			}
			check := w.transaction(ctx, func(tx *sql.Tx) error { return remote.RequireWorkspaceImportTx(ctx, tx, r.Owner, r.Reference) })
			if wrong && check == nil || !wrong && check != nil {
				t.Fatal("workspace admission disagrees", check)
			}
		})
	}
}
