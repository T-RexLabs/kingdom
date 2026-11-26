package providerrequest

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"kingdom/internal/core/providerexecution"
)

type NativeCall struct {
	BindingID string
	Identity  providerexecution.RequestIdentity
	MessageID string
	CallID    string
	Tool      string
	Input     json.RawMessage
}
type Grant struct {
	ID      string
	Digest  string
	Execute bool
}
type NativeReceipt struct {
	GrantID string
	Digest  string
	Outcome string
	Started *bool
}

func callDigest(b Binding, call NativeCall) (string, string, error) {
	if call.MessageID == "" || call.MessageID != strings.TrimSpace(call.MessageID) || call.CallID == "" || call.CallID != strings.TrimSpace(call.CallID) || call.Identity != b.Identity {
		return "", "", fmt.Errorf("complete exact tool identity is required")
	}
	input, err := providerexecution.CanonicalToolInput(call.Input)
	if err != nil {
		return "", "", err
	}
	payload := struct {
		BindingID                      string
		Identity                       providerexecution.RequestIdentity
		Owner                          Owner
		Lease                          Lease
		MessageID, CallID, Tool, Input string
	}{b.ID, b.Identity, b.Owner, b.Lease, call.MessageID, call.CallID, call.Tool, input}
	raw, err := json.Marshal(payload)
	if err != nil {
		return "", "", err
	}
	sum := sha256.Sum256(raw)
	return input, hex.EncodeToString(sum[:]), nil
}
func requireAdmission(ctx context.Context, tx *sql.Tx, b Binding, now time.Time) error {
	if now.IsZero() {
		return fmt.Errorf("tool admission time is required")
	}
	if b.AdmissionClosedAt != "" || b.DispatchIssuedAt == "" {
		return fmt.Errorf("provider tool admission is closed or undispatched")
	}
	return RequireOwnerTx(ctx, tx, b.ID, b.Lease, now)
}
func AdmitNativeTx(ctx context.Context, tx *sql.Tx, call NativeCall, now time.Time) (Grant, error) {
	var grant Grant
	switch call.Tool {
	case "write", "edit", "patch", "read", "glob", "grep":
	default:
		return grant, fmt.Errorf("unsupported native tool")
	}
	b, err := Get(ctx, tx, call.BindingID)
	if err != nil {
		return grant, err
	}
	input, digest, err := callDigest(b, call)
	if err != nil {
		return grant, err
	}
	err = tx.QueryRowContext(ctx, `SELECT grant_id,identity_digest FROM provider_native_grants WHERE binding_id=? AND message_id=? AND call_id=?`, b.ID, call.MessageID, call.CallID).Scan(&grant.ID, &grant.Digest)
	if err == nil {
		if grant.Digest != digest {
			return Grant{}, fmt.Errorf("conflicting native call replay")
		}
		return grant, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return grant, err
	}
	// A tool call cannot switch executors while keeping its public call identity.
	var other int
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM provider_command_calls WHERE binding_id=? AND message_id=? AND call_id=?`, b.ID, call.MessageID, call.CallID).Scan(&other); err != nil {
		return grant, err
	}
	if other != 0 {
		return grant, fmt.Errorf("tool call already belongs to a command")
	}
	if err := requireAdmission(ctx, tx, b, now); err != nil {
		return grant, err
	}
	grant = Grant{ID: "native_" + digest, Digest: digest, Execute: true}
	_, err = tx.ExecContext(ctx, `INSERT INTO provider_native_grants(grant_id,binding_id,message_id,call_id,tool,input_json,identity_digest,created_at) VALUES(?,?,?,?,?,?,?,?)`, grant.ID, b.ID, call.MessageID, call.CallID, call.Tool, input, digest, now.UTC().Format(time.RFC3339Nano))
	return grant, err
}

// FinishNativeTx records observed native completion, including after the lease
// expires or admission closes. It cannot grant new execution authority.
func FinishNativeTx(ctx context.Context, tx *sql.Tx, receipt NativeReceipt, now time.Time) error {
	var linked int
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM remote_native_tools WHERE grant_id=?`, receipt.GrantID).Scan(&linked); err != nil {
		return err
	}
	if linked != 0 {
		return fmt.Errorf("managed native helper requires retained process evidence")
	}
	return finishNativeTx(ctx, tx, receipt, now)
}

func finishNativeTx(ctx context.Context, tx *sql.Tx, receipt NativeReceipt, now time.Time) error {
	if receipt.GrantID == "" || receipt.Digest == "" || receipt.Started == nil || now.IsZero() {
		return fmt.Errorf("explicit native receipt identity and started state are required")
	}
	if (receipt.Outcome == "aborted" && *receipt.Started) || ((receipt.Outcome == "completed" || receipt.Outcome == "failed") && !*receipt.Started) || (receipt.Outcome != "aborted" && receipt.Outcome != "completed" && receipt.Outcome != "failed") {
		return fmt.Errorf("invalid native receipt outcome")
	}
	var digest string
	var outcome sql.NullString
	var started sql.NullBool
	if err := tx.QueryRowContext(ctx, `SELECT identity_digest,outcome,native_started FROM provider_native_grants WHERE grant_id=?`, receipt.GrantID).Scan(&digest, &outcome, &started); err != nil {
		return err
	}
	if digest != receipt.Digest {
		return fmt.Errorf("native receipt identity mismatch")
	}
	if outcome.Valid {
		if outcome.String != receipt.Outcome || !started.Valid || started.Bool != *receipt.Started {
			return fmt.Errorf("conflicting native receipt")
		}
		return nil
	}
	_, err := tx.ExecContext(ctx, `UPDATE provider_native_grants SET outcome=?,native_started=?,completed_at=? WHERE grant_id=? AND outcome IS NULL`, receipt.Outcome, *receipt.Started, now.UTC().Format(time.RFC3339Nano), receipt.GrantID)
	return err
}
