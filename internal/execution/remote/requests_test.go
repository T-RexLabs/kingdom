package remote

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	native "github.com/NVIDIA/OpenShell/sdk/go/proto/openshellv1"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"kingdom/internal/execution/openshell/managed"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestEncryptedManagedRequestRecovery(t *testing.T) {
	db := openLedger(t, ":memory:")
	r := bound(t, db)
	o, request, _ := managedRequest(t, r)
	prepareManaged(t, db, r, ProviderServer, o, request)
	s, e := NewRequestStore(bytes.Repeat([]byte{1}, 32))
	if e != nil {
		t.Fatal(e)
	}
	reject(t, db, nil, func(tx *sql.Tx) error {
		_, e := s.ClaimManagedDispatchTx(ctx, tx, r.Owner, r.Reference, request.Execution.RequestId)
		return e
	})
	transaction(t, db, func(tx *sql.Tx) error { return s.SaveManagedTx(ctx, tx, r.Owner, r.Reference, request) })
	transaction(t, db, func(tx *sql.Tx) error { return s.SaveManagedTx(ctx, tx, r.Owner, r.Reference, request) })
	var ciphertext []byte
	if e = db.QueryRow(`SELECT ciphertext FROM remote_recovery_requests`).Scan(&ciphertext); e != nil {
		t.Fatal(e)
	}
	if bytes.Contains(ciphertext, []byte("private-fixture-secret")) {
		t.Fatal("plaintext persisted")
	}
	got, e := s.ReadManaged(ctx, db, r.Owner, r.Reference, request.Execution.RequestId)
	if e != nil || !proto.Equal(got, request) {
		t.Fatal(e)
	}
	wrong, _ := NewRequestStore(bytes.Repeat([]byte{2}, 32))
	if _, e = wrong.ReadManaged(ctx, db, r.Owner, r.Reference, request.Execution.RequestId); !errors.Is(e, ErrConflict) {
		t.Fatal(e)
	}
	stale := r.Owner
	stale.LeaseVersion++
	if _, e = s.ReadManaged(ctx, db, stale, r.Reference, request.Execution.RequestId); !errors.Is(e, ErrConflict) {
		t.Fatal(e)
	}
	transaction(t, db, func(tx *sql.Tx) error {
		sent, e := s.ClaimManagedDispatchTx(ctx, tx, r.Owner, r.Reference, request.Execution.RequestId)
		if !sent {
			t.Fatal("not claimed")
		}
		return e
	})
	transaction(t, db, func(tx *sql.Tx) error {
		sent, e := s.ClaimManagedDispatchTx(ctx, tx, r.Owner, r.Reference, request.Execution.RequestId)
		if sent {
			t.Fatal("uncertain operation resent")
		}
		return e
	})
	transaction(t, db, func(tx *sql.Tx) error { return CloseAdmissionTx(ctx, tx, r.Owner, r.Reference) })
	if _, e = s.ReadManaged(ctx, db, r.Owner, r.Reference, request.Execution.RequestId); e != nil {
		t.Fatal("fenced recovery denied", e)
	}
	for _, stmt := range []string{`DELETE FROM remote_recovery_requests`, `UPDATE remote_recovery_requests SET ciphertext=x'01'`} {
		reject(t, db, nil, func(tx *sql.Tx) error { _, e := tx.Exec(stmt); return e })
	}
	ciphertext[len(ciphertext)-1] ^= 1
	if _, e = s.open(1, ciphertext, []byte("wrong")); !errors.Is(e, ErrConflict) {
		t.Fatal(e)
	}
	if _, e = s.open(2, ciphertext, nil); !errors.Is(e, ErrConflict) {
		t.Fatal(e)
	}
}
func TestCreateRecoverySurvivesRuntimeBinding(t *testing.T) {
	db := openLedger(t, ":memory:")
	base := reserve(t, db)
	// A fresh allocation pins the full native request before its first dispatch.
	request := &native.CreateSandboxRequest{RequestId: uuid.NewString(), Name: "new", Spec: &native.SandboxSpec{Command: []string{"/bin/sleep", "60"}}}
	d, _ := managed.Digest(request)
	var r Reservation
	transaction(t, db, func(tx *sql.Tx) error {
		owner := base.Owner
		owner.InvocationID = "other"
		owner.BindingID = "other"
		var e error
		r, e = ReserveRuntimeTx(ctx, tx, owner, base.Reference.Target, request.RequestId, hex.EncodeToString(d))
		return e
	})
	s, _ := NewRequestStore(bytes.Repeat([]byte{1}, 32))
	transaction(t, db, func(tx *sql.Tx) error { return s.SaveCreateTx(ctx, tx, r.Owner, r.Reference, request) })
	transaction(t, db, func(tx *sql.Tx) error { _, e := s.ClaimCreateDispatchTx(ctx, tx, r.Owner, r.Reference); return e })
	r.Reference.SandboxID = "sandbox"
	r.Reference.Generation = "generation"
	r.Reference.MainInstanceID = "main"
	transaction(t, db, func(tx *sql.Tx) error { return BindRuntimeTx(ctx, tx, r.Owner, r.Reference) })
	got, e := s.ReadCreate(ctx, db, r.Owner, r.Reference)
	if e != nil || !proto.Equal(got, request) {
		t.Fatal("creation request lost after bind", e)
	}
}

func TestWorkspaceImportMustFinishBeforeServerAndCannotRepeat(t *testing.T) {
	db := openLedger(t, ":memory:")
	r := bound(t, db)
	o, request, identity := managedRequest(t, r)
	prepareManaged(t, db, r, WorkspaceImport, o, request)
	_, server, _ := managedRequest(t, r)
	reject(t, db, nil, func(tx *sql.Tx) error {
		_, e := PrepareManagedExecutionTx(ctx, tx, r.Owner, r.Reference, ProviderServer, o, server)
		return e
	})
	_, second, _ := managedRequest(t, r)
	reject(t, db, ErrConflict, func(tx *sql.Tx) error {
		_, e := PrepareManagedExecutionTx(ctx, tx, r.Owner, r.Reference, WorkspaceImport, o, second)
		return e
	})
	dispatchManaged(t, db, r, identity.RequestId)
	transaction(t, db, func(tx *sql.Tx) error { return RecordManagedIdentityTx(ctx, tx, r.Owner, r.Reference, identity) })
	transaction(t, db, func(tx *sql.Tx) error {
		return RecordManagedReceiptTx(ctx, tx, r.Owner, r.Reference, identity.RequestId, managedReceipt(identity))
	})
	// Process exit alone does not prove the imported workspace matches its input.
	reject(t, db, nil, func(tx *sql.Tx) error {
		_, e := PrepareManagedExecutionTx(ctx, tx, r.Owner, r.Reference, ProviderServer, o, server)
		return e
	})
	// Re-running the install helper exercises the migration on retained evidence.
	transaction(t, db, func(tx *sql.Tx) error { return InstallSchemaTx(ctx, tx) })
	if _, found, e := ReadManagedReceipt(ctx, db, r.Owner, r.Reference, identity.RequestId); e != nil || !found {
		t.Fatal("migration lost receipt", e)
	}
	reject(t, db, nil, func(tx *sql.Tx) error { _, e := tx.Exec(`DELETE FROM managed_execution_receipts`); return e })
}

// The owner check must finish before protected payload storage is consulted.
type observedReader struct {
	Reader
	payloadReads int
}

func (q *observedReader) QueryRowContext(c context.Context, query string, args ...any) *sql.Row {
	if strings.Contains(query, "ciphertext") {
		q.payloadReads++
	}
	return q.Reader.QueryRowContext(c, query, args...)
}
func TestRequestOwnerCheckedBeforeCiphertextAccess(t *testing.T) {
	db := openLedger(t, ":memory:")
	r := bound(t, db)
	o, request, _ := managedRequest(t, r)
	prepareManaged(t, db, r, ProviderServer, o, request)
	s, _ := NewRequestStore(bytes.Repeat([]byte{3}, 32))
	transaction(t, db, func(tx *sql.Tx) error { return s.SaveManagedTx(ctx, tx, r.Owner, r.Reference, request) })
	q := &observedReader{Reader: db}
	foreign := r.Owner
	foreign.BindingID = "foreign"
	if _, e := s.ReadManaged(ctx, q, foreign, r.Reference, request.Execution.RequestId); !errors.Is(e, ErrConflict) || q.payloadReads != 0 {
		t.Fatal(e, q.payloadReads)
	}
}
func TestRequestCiphertextSubstitutionAndLimits(t *testing.T) {
	db := openLedger(t, ":memory:")
	r := bound(t, db)
	o, request, _ := managedRequest(t, r)
	prepareManaged(t, db, r, ProviderServer, o, request)
	s, _ := NewRequestStore(bytes.Repeat([]byte{4}, 32))
	transaction(t, db, func(tx *sql.Tx) error { return s.SaveManagedTx(ctx, tx, r.Owner, r.Reference, request) })
	var sealed []byte
	if e := db.QueryRow(`SELECT ciphertext FROM remote_recovery_requests`).Scan(&sealed); e != nil {
		t.Fatal(e)
	}
	op, e := ReadOperation(ctx, db, request.Execution.RequestId)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.open(1, sealed, requestAAD(r.Owner, r.Reference, op)); e != nil {
		t.Fatal(e)
	}
	op.ID = uuid.NewString()
	if _, e = s.open(1, sealed, requestAAD(r.Owner, r.Reference, op)); !errors.Is(e, ErrConflict) {
		t.Fatal("substitution accepted", e)
	}
	if _, e = s.open(8, sealed, nil); !errors.Is(e, ErrConflict) {
		t.Fatal("unknown version accepted", e)
	}
	if _, e = s.open(1, make([]byte, maxRecoveryRequest+29), nil); !errors.Is(e, ErrConflict) {
		t.Fatal("unbounded ciphertext accepted", e)
	}
}

func TestCanonicalCreateAllocationRecoveryAndRetention(t *testing.T) {
	file := filepath.Join(t.TempDir(), "requests.db")
	db := openLedger(t, file)
	base := reserve(t, db)
	s, _ := NewRequestStore(bytes.Repeat([]byte{9}, 32))
	template := &native.CreateSandboxRequest{Spec: &native.SandboxSpec{Command: []string{"/bin/sleep", "60"}}}
	owner := base.Owner
	owner.InvocationID = "next"
	owner.BindingID = "next"
	id := uuid.NewString()
	var r Reservation
	var request *native.CreateSandboxRequest
	transaction(t, db, func(tx *sql.Tx) error {
		var e error
		r, request, e = s.ReserveCreateTx(ctx, tx, owner, base.Reference.Target, id, template)
		return e
	})
	if request.Name != "k-"+strconv.FormatInt(r.Reference.Allocation, 36) || r.Reference.Allocation <= base.Reference.Allocation {
		t.Fatal("noncanonical allocation")
	}
	transaction(t, db, func(tx *sql.Tx) error {
		again, req, e := s.ReserveCreateTx(ctx, tx, owner, base.Reference.Target, id, template)
		if e == nil && (again != r || !proto.Equal(req, request)) {
			t.Fatal("replay changed")
		}
		return e
	})
	changed := proto.Clone(template).(*native.CreateSandboxRequest)
	changed.Spec.Command = []string{"/bin/false"}
	reject(t, db, ErrConflict, func(tx *sql.Tx) error {
		_, _, e := s.ReserveCreateTx(ctx, tx, owner, base.Reference.Target, id, changed)
		return e
	})
	reject(t, db, nil, func(tx *sql.Tx) error { _, e := tx.Exec(`DELETE FROM remote_allocations`); return e })
	if e := db.Close(); e != nil {
		t.Fatal(e)
	}
	db = openLedger(t, file)
	got, e := s.ReadCreate(ctx, db, owner, r.Reference)
	if e != nil || !proto.Equal(got, request) {
		t.Fatal("restart lost request", e)
	}
	transaction(t, db, func(tx *sql.Tx) error {
		owner.InvocationID = "third"
		owner.BindingID = "third"
		next, _, e := s.ReserveCreateTx(ctx, tx, owner, base.Reference.Target, uuid.NewString(), template)
		if e == nil && next.Reference.Allocation <= r.Reference.Allocation {
			t.Fatal("allocation reused")
		}
		return e
	})
}
func TestAllocatorMigrationPreservesHistoricalSequence(t *testing.T) {
	db := openLedger(t, ":memory:")
	r := reserve(t, db)
	// Simulate a historical allocator whose sequence exceeds retained row IDs.
	if _, e := db.Exec(`UPDATE sqlite_sequence SET seq=? WHERE name='remote_runtimes'`, r.Reference.Allocation+100); e != nil {
		t.Fatal(e)
	}
	transaction(t, db, func(tx *sql.Tx) error { return InstallSchemaTx(ctx, tx) })
	transaction(t, db, func(tx *sql.Tx) error {
		o := r.Owner
		o.InvocationID = "later"
		o.BindingID = "later"
		next, e := ReserveRuntimeTx(ctx, tx, o, r.Reference.Target, uuid.NewString(), digest)
		if e == nil && next.Reference.Allocation <= r.Reference.Allocation+100 {
			t.Fatal("historical allocation reused")
		}
		return e
	})
}
