package state

import "fmt"

// CommitOutcomeUnknownError marks a transaction commit whose durability cannot
// be inferred from the returned error. Callers must reconcile durable state or
// prevent mutation re-execution before returning an ordinary failure.
type CommitOutcomeUnknownError struct {
	Operation  string
	ResourceID string
	Cause      error
}

func (e *CommitOutcomeUnknownError) Error() string {
	if e == nil {
		return "transaction commit outcome is unknown"
	}
	if e.Operation == "" {
		return fmt.Sprintf("transaction commit outcome is unknown: %v", e.Cause)
	}
	return fmt.Sprintf("%s commit outcome is unknown: %v", e.Operation, e.Cause)
}

func (e *CommitOutcomeUnknownError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

// CommitOutcomeAbsentError marks a commit error whose resource was confirmed
// absent by durable readback. Callers may return an ordinary server failure;
// the operation is known not to have committed.
type CommitOutcomeAbsentError struct {
	Operation  string
	ResourceID string
	Cause      error
}

func (e *CommitOutcomeAbsentError) Error() string {
	if e == nil {
		return "transaction commit outcome is absent"
	}
	if e.Operation == "" {
		return fmt.Sprintf("transaction did not commit: %v", e.Cause)
	}
	return fmt.Sprintf("%s did not commit: %v", e.Operation, e.Cause)
}

func (e *CommitOutcomeAbsentError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

// PostCommitProjectionError marks a durable mutation whose response projection
// could not be read back. Callers must preserve idempotency and avoid treating
// it as an ordinary pre-commit failure.
type PostCommitProjectionError struct {
	Operation  string
	ResourceID string
	Cause      error
}

func (e *PostCommitProjectionError) Error() string {
	if e == nil {
		return "post-commit projection is unavailable"
	}
	if e.Operation == "" {
		return fmt.Sprintf("post-commit projection is unavailable: %v", e.Cause)
	}
	return fmt.Sprintf("%s post-commit projection is unavailable: %v", e.Operation, e.Cause)
}

func (e *PostCommitProjectionError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}
