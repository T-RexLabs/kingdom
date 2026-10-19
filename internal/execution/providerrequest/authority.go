package providerrequest

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"kingdom/internal/core/providerexecution"
	"kingdom/internal/sqlitex"
)

// Authority commits each provider-side fact in its own canonical transaction.
// Callers name a request by identity; the stored binding supplies its lease, and
// the canonical owner checks inside every helper reject a superseded binding.
type Authority struct {
	db  *sql.DB
	now func() time.Time
}

func NewAuthority(db *sql.DB, now func() time.Time) *Authority {
	if now == nil {
		now = time.Now
	}
	return &Authority{db: db, now: now}
}

func (a *Authority) transact(ctx context.Context, identity providerexecution.RequestIdentity, body func(Binding, time.Time, *sql.Tx) error) error {
	_, err := sqlitex.Transaction(ctx, a.db, func(ctx context.Context, tx *sql.Tx) error {
		b, err := ByIdentity(ctx, tx, identity)
		if err != nil {
			return err
		}
		return body(b, a.now(), tx)
	}, sqlitex.Options{})
	return err
}

func (a *Authority) IssueDispatch(ctx context.Context, identity providerexecution.RequestIdentity) error {
	return a.transact(ctx, identity, func(b Binding, now time.Time, tx *sql.Tx) error {
		return IssueDispatchTx(ctx, tx, b.ID, identity, b.Lease, now)
	})
}

func (a *Authority) CloseAdmission(ctx context.Context, identity providerexecution.RequestIdentity) error {
	return a.transact(ctx, identity, func(b Binding, now time.Time, tx *sql.Tx) error {
		return CloseAdmissionTx(ctx, tx, b.ID, identity, b.Lease, now)
	})
}

// AdmitNative returns only after the grant is durable, so Execute=true is never
// observable without the obligation it creates.
func (a *Authority) AdmitNative(ctx context.Context, call NativeCall) (Grant, error) {
	var grant Grant
	err := a.transact(ctx, call.Identity, func(b Binding, now time.Time, tx *sql.Tx) (err error) {
		call.BindingID = b.ID
		grant, err = AdmitNativeTx(ctx, tx, call, now)
		return err
	})
	return grant, err
}

func (a *Authority) FinishNative(ctx context.Context, identity providerexecution.RequestIdentity, receipt NativeReceipt) error {
	return a.transact(ctx, identity, func(b Binding, now time.Time, tx *sql.Tx) error {
		var owner string
		if err := tx.QueryRowContext(ctx, `SELECT binding_id FROM provider_native_grants WHERE grant_id=?`, receipt.GrantID).Scan(&owner); err != nil {
			return err
		}
		if owner != b.ID {
			return fmt.Errorf("native receipt belongs to a different provider request")
		}
		return FinishNativeTx(ctx, tx, receipt, now)
	})
}

func (a *Authority) ClaimCommand(ctx context.Context, call NativeCall) (CommandClaim, error) {
	var claim CommandClaim
	err := a.transact(ctx, call.Identity, func(b Binding, now time.Time, tx *sql.Tx) (err error) {
		call.BindingID = b.ID
		claim, err = ClaimCommandTx(ctx, tx, call, now)
		return err
	})
	return claim, err
}

func (a *Authority) CompleteCommand(ctx context.Context, identity providerexecution.RequestIdentity, claimID, digest, guardID string) error {
	return a.transact(ctx, identity, func(b Binding, now time.Time, tx *sql.Tx) error {
		var owner string
		if err := tx.QueryRowContext(ctx, `SELECT binding_id FROM provider_command_calls WHERE claim_id=?`, claimID).Scan(&owner); err != nil {
			return err
		}
		if owner != b.ID {
			return fmt.Errorf("command claim belongs to a different provider request")
		}
		return CompleteCommandTx(ctx, tx, claimID, digest, guardID, now)
	})
}

// CheckTerminal refuses a terminal that settlement would refuse. It records
// nothing: the terminal becomes the request's evidence when it settles.
func (a *Authority) CheckTerminal(ctx context.Context, identity providerexecution.RequestIdentity, terminal providerexecution.RequestTerminal) error {
	return a.transact(ctx, identity, func(b Binding, _ time.Time, tx *sql.Tx) error {
		return CheckTerminalTx(ctx, tx, b.ID, b.Lease, terminal)
	})
}

func (a *Authority) AbortCommand(ctx context.Context, identity providerexecution.RequestIdentity, claimID, digest string) error {
	return a.transact(ctx, identity, func(b Binding, now time.Time, tx *sql.Tx) error {
		var owner string
		if err := tx.QueryRowContext(ctx, `SELECT binding_id FROM provider_command_calls WHERE claim_id=?`, claimID).Scan(&owner); err != nil {
			return err
		}
		if owner != b.ID {
			return fmt.Errorf("command claim belongs to a different provider request")
		}
		return AbortCommandTx(ctx, tx, claimID, digest, now)
	})
}

// Abandon closes an unprovable request that never admitted a tool.
func (a *Authority) Abandon(ctx context.Context, identity providerexecution.RequestIdentity) error {
	return a.transact(ctx, identity, func(b Binding, now time.Time, tx *sql.Tx) error {
		return AbandonTx(ctx, tx, b.ID, b.Lease, now)
	})
}

// RegisterServerInstance records a started shared server. It is the guardian's
// durable launch gate, so the server never runs unrecorded.
func (a *Authority) RegisterServerInstance(ctx context.Context, instance ServerInstance) error {
	_, err := sqlitex.Transaction(ctx, a.db, func(ctx context.Context, tx *sql.Tx) error {
		return RegisterServerInstanceTx(ctx, tx, instance, a.now())
	}, sqlitex.Options{})
	return err
}

// CloseServerInstance records that a shared server has been proven stopped.
func (a *Authority) CloseServerInstance(ctx context.Context, id string, closure ServerClosure) error {
	_, err := sqlitex.Transaction(ctx, a.db, func(ctx context.Context, tx *sql.Tx) error {
		return CloseServerInstanceTx(ctx, tx, id, closure, a.now())
	}, sqlitex.Options{})
	return err
}
