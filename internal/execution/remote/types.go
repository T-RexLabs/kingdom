// Package remote records remote execution identities and dispatch uncertainty.
// It does not authorize provider work or certify process closure. Callers must
// check canonical owner authority in the same transaction before admission.
// Any transaction method error requires rollback of the whole transaction.
// A successful method call has no durable effect until its transaction commits.
package remote

import (
	"encoding/hex"
	"errors"
	"strings"

	"github.com/google/uuid"
)

var ErrConflict = errors.New("remote execution identity conflicts with its durable record")
var ErrAdmissionClosed = errors.New("remote execution admission is closed")

// Owner is the immutable request and lease that reserved a runtime. A different
// lease cannot adopt it. Recovery can observe the original owner's operations.
type Owner struct {
	Kind         string
	InvocationID string
	BindingID    string
	LeaseOwner   string
	LeaseVersion int64
}

type Target struct{ WorkerID, GatewayID, WorkspaceID string }

// Reference identifies remote resources without borrowing local PID identities.
// Allocation is globally monotonic within this ledger; its database must never
// be reset while any gateway can still contain resources allocated from it.
type Reference struct {
	Target
	Allocation     int64
	CreationID     string
	SandboxID      string
	Generation     string
	MainInstanceID string
}

type Reservation struct {
	Owner           Owner
	Reference       Reference
	SpecDigest      string
	AdmissionClosed bool
}

type CommandReference struct {
	Runtime       Reference
	OwnerBinding  string
	ClaimID       string
	CommandID     string
	PayloadDigest string
	Sequence      int64
}

type OperationKind string

const (
	Create  OperationKind = "create"
	Execute OperationKind = "execute"
	Stop    OperationKind = "stop"
)

type Delivery string

const (
	Prepared  Delivery = "prepared"
	Uncertain Delivery = "delivery_uncertain"
	Admitted  Delivery = "admitted"
)

// Operation records transport admission only. Even Admitted carries no proof
// of process termination, output validity, disposal, or workflow completion.
type Operation struct {
	ID              string
	Allocation      int64
	Kind            OperationKind
	PayloadDigest   string
	Delivery        Delivery
	AdmissionDigest string
}

func text(s string) bool { return s != "" && strings.TrimSpace(s) == s && !strings.ContainsRune(s, 0) }
func validOwner(o Owner) bool {
	return (o.Kind == "run" || o.Kind == "planning") && text(o.InvocationID) && text(o.BindingID) && text(o.LeaseOwner) && o.LeaseVersion > 0
}
func validTarget(t Target) bool { return text(t.WorkerID) && text(t.GatewayID) && text(t.WorkspaceID) }
func validUUID(s string) bool {
	id, err := uuid.Parse(s)
	return err == nil && id != uuid.Nil && id.String() == s
}
func validDigest(s string) bool {
	raw, err := hex.DecodeString(s)
	return err == nil && len(raw) == 32 && hex.EncodeToString(raw) == s
}
func complete(r Reference) bool {
	return validTarget(r.Target) && r.Allocation > 0 && validUUID(r.CreationID) && text(r.SandboxID) && text(r.Generation) && text(r.MainInstanceID)
}
