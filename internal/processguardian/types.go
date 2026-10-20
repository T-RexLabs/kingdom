// Package processguardian owns short-lived provider command trees.
package processguardian

import (
	"context"
	"fmt"
	"io"
	"time"
)

// LaunchError reports a failed gated launch after the guardian had already
// acknowledged ownership. OwnershipPersisted records successful acknowledgement
// by the caller's durable launch gate, requiring durable ingestion of Result.
// False means persistence was not confirmed, not that the database is empty. A
// persistence failure retains its cause and stopped-tree result; any uncertain
// durable ownership remains subject to the caller's reconciliation rules.
type LaunchError struct {
	Cause              error
	Result             Result
	OwnershipPersisted bool
}

func (e *LaunchError) Error() string { return fmt.Sprintf("guardian launch failed: %v", e.Cause) }
func (e *LaunchError) Unwrap() error { return e.Cause }

const (
	HiddenGuardianMode = "--kingdom-process-guardian"
	HiddenGateMode     = "--kingdom-process-gate"

	maxWireBytes = 4 << 20
)

// Command contains the guarded executable metadata supplied over private
// pipes. Prompt/input payload belongs on Handle.Stdin, never in Args or the
// bounded guardian wire specification.
type Command struct {
	Path string
	Args []string
	Dir  string
	Env  []string
}

// Ownership identifies the process group acknowledged before the caller lets
// the gated command exec. Parent-side code persists this before Start returns.
type Ownership struct {
	GuardID               string
	GuardianPID           int
	GuardianIdentityToken string
	ChildPID              int
	ChildIdentityToken    string
	TreeID                int
	TreeIdentityToken     string
	Platform              string
	QuiescenceCapability  string
}

// Result is available only once the command tree is demonstrably quiescent.
type Result struct {
	Ownership
	ExitCode       int
	ExitSignal     int
	Forced         bool
	Quiescent      bool
	EvidenceSHA256 string
	EvidenceJSON   string `json:"evidence_json"`
	FailureCode    string
	FailureText    string
}

// PersistOwnership is the parent-side durable launch gate. The guardian never
// receives a database handle; a failure keeps the provider command gated.
type PersistOwnership func(context.Context, Ownership) error

type Config struct {
	Executable  string
	RuntimeRoot string
	CancelGrace time.Duration
	CancelForce time.Duration
}

// Runner starts one guardian process per command.
type Runner struct {
	executable  string
	runtimeRoot string
	cancelGrace time.Duration
	cancelForce time.Duration
}

// Handle owns a single started guardian command.
type Handle interface {
	GuardID() string
	// Stdin writes bytes to the guarded child. Close it to deliver EOF; a write
	// error on an open stream cancels the guarded process tree.
	Stdin() io.WriteCloser
	Stdout() io.ReadCloser
	Stderr() io.ReadCloser
	Cancel(context.Context) error
	Wait(context.Context) (Result, error)
}
