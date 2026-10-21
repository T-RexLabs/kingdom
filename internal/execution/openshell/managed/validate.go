// Package managed verifies the opt-in Kingdom managed OpenShell protocol.
// Receipt validation establishes protocol facts, not workflow settlement.
package managed

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"google.golang.org/protobuf/encoding/protowire"
	"hash"
	"sort"
	"strings"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	pb "kingdom/internal/execution/openshell/managedpb"
)

const Capability = "kingdom.openshell.managed-execution.v1"
const StartMethod = "/kingdom.openshell.managed.v1.ManagedExecutions/StartExecution"
const MaxOutputBytes = 16 * 1024 * 1024
const MaxOutputChunks = 4096
const MaxChunkBytes = 64 * 1024
const MaxControlBytes = 1024 * 1024
const MaxCommandBytes = MaxControlBytes - 16*1024

var ErrProtocol = errors.New("invalid managed execution evidence")

// Canonical encodes only schema-known fields. The Rust peer uses ordered maps
// and discards unknown fields. Preserve message presence and oneof presence.
func Canonical(value proto.Message) ([]byte, error) {
	if value == nil || !value.ProtoReflect().IsValid() {
		return nil, ErrProtocol
	}
	copy := proto.Clone(value)
	clearUnknown(copy.ProtoReflect())
	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(copy)
	if err != nil {
		return nil, err
	}
	return canonicalWire(encoded, copy.ProtoReflect().Descriptor())
}

// Go emits oneof fields after ordinary fields, while prost emits numeric
// field order. Deterministic marshal alone is therefore insufficient here.
func canonicalWire(encoded []byte, descriptor protoreflect.MessageDescriptor) ([]byte, error) {
	type field struct {
		number protowire.Number
		wire   []byte
	}
	fields := []field{}
	for len(encoded) > 0 {
		number, kind, tagSize := protowire.ConsumeTag(encoded)
		if tagSize < 0 {
			return nil, ErrProtocol
		}
		valueSize := protowire.ConsumeFieldValue(number, kind, encoded[tagSize:])
		if valueSize < 0 {
			return nil, ErrProtocol
		}
		wire := append([]byte(nil), encoded[:tagSize+valueSize]...)
		definition := descriptor.Fields().ByNumber(protoreflect.FieldNumber(number))
		if definition == nil {
			return nil, ErrProtocol
		}
		if kind == protowire.BytesType && definition.Kind() == protoreflect.MessageKind {
			payload, n := protowire.ConsumeBytes(encoded[tagSize:])
			if n < 0 {
				return nil, ErrProtocol
			}
			nested, err := canonicalWire(payload, definition.Message())
			if err != nil {
				return nil, err
			}
			wire = protowire.AppendTag(nil, number, kind)
			wire = protowire.AppendBytes(wire, nested)
		}
		fields = append(fields, field{number, wire})
		encoded = encoded[tagSize+valueSize:]
	}
	sort.SliceStable(fields, func(i, j int) bool { return fields[i].number < fields[j].number })
	var result []byte
	for _, f := range fields {
		result = append(result, f.wire...)
	}
	return result, nil
}

func clearUnknown(m protoreflect.Message) {
	m.SetUnknown(nil)
	m.Range(func(f protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		if f.IsMap() {
			if f.MapValue().Kind() == protoreflect.MessageKind {
				v.Map().Range(func(_ protoreflect.MapKey, v protoreflect.Value) bool { clearUnknown(v.Message()); return true })
			}
		} else if f.IsList() {
			if f.Kind() == protoreflect.MessageKind {
				for i := 0; i < v.List().Len(); i++ {
					clearUnknown(v.List().Get(i).Message())
				}
			}
		} else if f.Kind() == protoreflect.MessageKind {
			clearUnknown(v.Message())
		}
		return true
	})
}
func Digest(value proto.Message) ([]byte, error) {
	b, e := Canonical(value)
	if e != nil {
		return nil, e
	}
	d := sha256.Sum256(b)
	return d[:], nil
}
func validID(s string) bool {
	u, e := uuid.Parse(s)
	return e == nil && u != uuid.Nil && u.String() == s
}
func identifier(s string) bool { return len(s) > 0 && len(s) <= 256 && !strings.ContainsRune(s, 0) }
func ValidateRuntime(r *pb.RuntimeBinding) error {
	if r == nil || !identifier(r.SandboxId) || !identifier(r.BoundaryGeneration) || !identifier(r.MainProcessInstanceId) || len(r.DeploymentProfileSha256) != 32 {
		return ErrProtocol
	}
	return nil
}
func ValidateSelector(s *pb.ExecutionSelector) error {
	if s == nil || !identifier(s.Workspace) || !identifier(s.Sandbox) || !validID(s.RequestId) {
		return ErrProtocol
	}
	return ValidateRuntime(s.Runtime)
}
func ValidateStart(r *pb.StartExecutionRequest) error {
	if r == nil || ValidateSelector(r.Execution) != nil || r.Command == nil || proto.Size(r) > MaxCommandBytes {
		return ErrProtocol
	}
	c := r.Command
	if len(c.Argv) == 0 || len(c.Argv) > 4096 || c.Argv[0] == "" || !strings.HasPrefix(c.Workdir, "/") || len(c.Workdir) > 4096 || strings.ContainsRune(c.Workdir, 0) || c.OutputLimitBytes == 0 || c.OutputLimitBytes > MaxOutputBytes || len(c.Environment) > 256 {
		return ErrProtocol
	}
	for _, a := range c.Argv {
		if strings.ContainsRune(a, 0) {
			return ErrProtocol
		}
	}
	total := 0
	for k, v := range c.Environment {
		total += len(k) + len(v)
		if len(k) == 0 || strings.HasPrefix(k, "OPENSHELL_") || strings.ContainsRune(v, 0) {
			return ErrProtocol
		}
		for i, b := range []byte(k) {
			if !(b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' || b == '_' || i > 0 && b >= '0' && b <= '9') {
				return ErrProtocol
			}
		}
	}
	if total > 64*1024 {
		return ErrProtocol
	}
	if t := c.ExecutionTimeout; t != nil && (t.Seconds < 0 || t.Nanos < 0 || t.Nanos >= 1e9 || t.Seconds == 0 && t.Nanos == 0) {
		return ErrProtocol
	}
	return nil
}

// Owner is supplied by trusted gateway identity discovery and an authenticated,
// pinned deployment. It must not come from workload or browser request fields.
type Owner struct {
	GatewayID, WorkspaceID, SubjectID string
	Runtime                           *pb.RuntimeBinding
}

func (o Owner) Validate() error {
	if !identifier(o.GatewayID) || !identifier(o.WorkspaceID) || !identifier(o.SubjectID) {
		return ErrProtocol
	}
	return ValidateRuntime(o.Runtime)
}
func (o Owner) ValidateIdentity(i *pb.CommandIdentity) error {
	if o.Validate() != nil || i == nil || i.SchemaVersion != 1 || i.Method != StartMethod || i.GatewayId != o.GatewayID || i.WorkspaceId != o.WorkspaceID || i.SubjectId != o.SubjectID || !proto.Equal(i.Runtime, o.Runtime) || !validID(i.RequestId) || !validID(i.BoundaryRequestId) || len(i.OriginalRequestSha256) != 32 || len(i.EffectiveSpecSha256) != 32 {
		return ErrProtocol
	}
	return nil
}
func (o Owner) ValidateStartIdentity(r *pb.StartExecutionRequest, i *pb.CommandIdentity) error {
	if ValidateStart(r) != nil || o.ValidateIdentity(i) != nil || i.RequestId != r.Execution.RequestId || !proto.Equal(i.Runtime, r.Execution.Runtime) {
		return ErrProtocol
	}
	original, e := Digest(r)
	if e != nil {
		return e
	}
	effective, e := Digest(r.Command)
	if e != nil {
		return e
	}
	if !bytes.Equal(original, i.OriginalRequestSha256) || !bytes.Equal(effective, i.EffectiveSpecSha256) {
		return ErrProtocol
	}
	return nil
}
func ValidateStatus(s *pb.ExecutionStatus, i *pb.CommandIdentity) error {
	if s == nil || i == nil || !proto.Equal(s.Identity, i) || s.DurableOutputSequence > MaxOutputChunks {
		return ErrProtocol
	}
	switch s.State {
	case pb.ExecutionState_EXECUTION_STATE_CLOSED:
		if ValidateReceipt(s.Terminal, i) != nil || s.Terminal.Output.LastSequence != s.DurableOutputSequence {
			return ErrProtocol
		}
	case pb.ExecutionState_EXECUTION_STATE_ADMITTED, pb.ExecutionState_EXECUTION_STATE_RUNNING, pb.ExecutionState_EXECUTION_STATE_UNRESOLVED:
		if s.Terminal != nil {
			return ErrProtocol
		}
	default:
		return ErrProtocol
	}
	return nil
}
func ReceiptDigest(r *pb.TerminalReceipt) ([]byte, error) {
	if r == nil {
		return nil, ErrProtocol
	}
	c := proto.Clone(r).(*pb.TerminalReceipt)
	c.ReceiptSha256 = nil
	return Digest(c)
}
func ValidateReceipt(r *pb.TerminalReceipt, i *pb.CommandIdentity) error {
	if r == nil || i == nil || r.SchemaVersion != 1 || !proto.Equal(r.Identity, i) || !r.GroupClosed || r.ProcessClean != (!r.TerminationRequested && !r.ForcedCleanup) {
		return ErrProtocol
	}
	switch e := r.ExitStatus.(type) {
	case *pb.TerminalReceipt_ExitCode:
		if e.ExitCode < 0 || e.ExitCode > 255 {
			return ErrProtocol
		}
	case *pb.TerminalReceipt_Signal:
		if e.Signal < 1 || e.Signal > 64 {
			return ErrProtocol
		}
	default:
		return ErrProtocol
	}
	d, e := ReceiptDigest(r)
	if e != nil || !bytes.Equal(d, r.ReceiptSha256) {
		return ErrProtocol
	}
	return ValidateOutputManifest(r.Output)
}

func ValidateOutputManifest(o *pb.OutputManifest) error {
	empty := sha256.Sum256(nil)
	if o == nil || len(o.Sha256) != 32 || o.TotalBytes > MaxOutputBytes || o.TotalBytes == 0 && !bytes.Equal(o.Sha256, empty[:]) || (o.TotalBytes == 0) != (o.LastSequence == 0) || o.LastSequence > o.TotalBytes || o.LastSequence > MaxOutputChunks || o.Complete && (o.Gap || o.Truncated) {
		return ErrProtocol
	}
	return nil
}
func ValidateChunk(c *pb.OutputChunk, expected uint64) error {
	if c == nil || expected == 0 || expected > MaxOutputChunks || c.Sequence != expected || len(c.Data) == 0 || len(c.Data) > MaxChunkBytes || (c.Stream != pb.OutputStream_OUTPUT_STREAM_STDOUT && c.Stream != pb.OutputStream_OUTPUT_STREAM_STDERR) {
		return ErrProtocol
	}
	return nil
}

// OutputVerifier checks retained chunks in sequence against the terminal
// manifest. Reconstruct it from the caller's durable journal after restart.
type OutputVerifier struct {
	h               hash.Hash
	sequence, total uint64
	failed          bool
}

func NewOutputVerifier() *OutputVerifier { return &OutputVerifier{h: sha256.New()} }
func (v *OutputVerifier) Add(c *pb.OutputChunk) error {
	if v.h == nil || v.failed || ValidateChunk(c, v.sequence+1) != nil || v.total+uint64(len(c.Data)) > MaxOutputBytes {
		v.failed = true
		return ErrProtocol
	}
	b, e := Canonical(c)
	if e != nil {
		v.failed = true
		return e
	}
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], uint32(len(b)))
	v.h.Write(length[:])
	v.h.Write(b)
	v.sequence++
	v.total += uint64(len(c.Data))
	return nil
}
func (v *OutputVerifier) Verify(o *pb.OutputManifest) error {
	if v.h == nil || v.failed || o == nil || o.LastSequence != v.sequence || o.TotalBytes != v.total || !bytes.Equal(o.Sha256, v.h.Sum(nil)) {
		return ErrProtocol
	}
	return nil
}

// SuccessfulProcess is only a prerequisite. The provider request, tool
// admissions, workspace export and canonical settlement must also be verified.
func SuccessfulProcess(r *pb.TerminalReceipt) bool {
	return r != nil && ValidateReceipt(r, r.Identity) == nil && r.GetExitCode() == 0 && isExitCode(r) && r.GroupClosed && r.ProcessClean && r.InputComplete && r.Output != nil && r.Output.Complete && !r.Output.Gap && !r.Output.Truncated
}
func isExitCode(r *pb.TerminalReceipt) bool {
	_, ok := r.ExitStatus.(*pb.TerminalReceipt_ExitCode)
	return ok
}
