package remote

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	native "github.com/NVIDIA/OpenShell/sdk/go/proto/openshellv1"
	"google.golang.org/protobuf/proto"
	"kingdom/internal/execution/openshell/managed"
	pb "kingdom/internal/execution/openshell/managedpb"
	"strconv"
)

const maxRecoveryRequest = 2 << 20

// RequestStore encrypts sensitive request material under an operator-managed
// key. The key is never stored here. This store cannot authorize dispatch.
type RequestStore struct{ aead cipher.AEAD }

func NewRequestStore(key []byte) (*RequestStore, error) {
	if len(key) != 32 {
		return nil, ErrConflict
	}
	b, e := aes.NewCipher(key)
	if e != nil {
		return nil, e
	}
	a, e := cipher.NewGCM(b)
	if e != nil {
		return nil, e
	}
	return &RequestStore{aead: a}, nil
}
func (*RequestStore) String() string   { return "encrypted remote request store" }
func (*RequestStore) GoString() string { return "encrypted remote request store" }
func requestAAD(owner Owner, ref Reference, op Operation) []byte {
	if op.Kind == Create {
		ref.SandboxID = ""
		ref.Generation = ""
		ref.MainInstanceID = ""
	}
	b, _ := json.Marshal(struct {
		Domain      string
		Version     int
		Owner       Owner
		Reference   Reference
		OperationID string
		Kind        OperationKind
		Digest      string
	}{"kingdom.remote.request", 1, owner, ref, op.ID, op.Kind, op.PayloadDigest})
	return b
}
func (s *RequestStore) operation(ctx context.Context, q Reader, o Owner, r Reference, id string, kind OperationKind) (Operation, error) {
	if s == nil || s.aead == nil {
		return Operation{}, ErrConflict
	}
	if _, e := requireOwner(ctx, q, o, r, kind != Create); e != nil {
		return Operation{}, e
	}
	op, e := ReadOperation(ctx, q, id)
	if e != nil {
		return op, e
	}
	if op.Allocation != r.Allocation || op.Kind != kind {
		return op, ErrConflict
	}
	return op, nil
}
func (s *RequestStore) put(ctx context.Context, tx *sql.Tx, o Owner, r Reference, id string, kind OperationKind, m proto.Message) error {
	op, e := s.operation(ctx, tx, o, r, id, kind)
	if e != nil {
		return e
	}
	if proto.Size(m) > maxRecoveryRequest {
		return ErrConflict
	}
	raw, e := managed.Canonical(m)
	if e != nil || len(raw) == 0 || len(raw) > maxRecoveryRequest {
		return ErrConflict
	}
	d, e := managed.Digest(m)
	if e != nil || hex.EncodeToString(d) != op.PayloadDigest {
		return ErrConflict
	}
	var version int
	var saved []byte
	e = tx.QueryRowContext(ctx, `SELECT schema_version,ciphertext FROM remote_recovery_requests WHERE operation_id=?`, id).Scan(&version, &saved)
	if e == nil {
		plain, e := s.open(version, saved, requestAAD(o, r, op))
		if e != nil || !bytes.Equal(plain, raw) {
			return ErrConflict
		}
		return nil
	}
	if !errors.Is(e, sql.ErrNoRows) {
		return e
	}
	if op.Delivery != Prepared {
		return ErrConflict
	}
	nonce := make([]byte, s.aead.NonceSize())
	if _, e = rand.Read(nonce); e != nil {
		return e
	}
	sealed := s.aead.Seal(nonce, nonce, raw, requestAAD(o, r, op))
	_, e = tx.ExecContext(ctx, `INSERT INTO remote_recovery_requests(operation_id,schema_version,ciphertext) VALUES(?,1,?)`, id, sealed)
	return e
}
func (s *RequestStore) open(version int, sealed, aad []byte) ([]byte, error) {
	if version != 1 || len(sealed) <= s.aead.NonceSize()+s.aead.Overhead() || len(sealed) > maxRecoveryRequest+s.aead.NonceSize()+s.aead.Overhead() {
		return nil, ErrConflict
	}
	plain, e := s.aead.Open(nil, sealed[:s.aead.NonceSize()], sealed[s.aead.NonceSize():], aad)
	if e != nil {
		return nil, ErrConflict
	}
	return plain, nil
}
func (s *RequestStore) get(ctx context.Context, q Reader, o Owner, r Reference, id string, kind OperationKind, m proto.Message) error {
	op, e := s.operation(ctx, q, o, r, id, kind)
	if e != nil {
		return e
	}
	var version int
	var sealed []byte
	if e = q.QueryRowContext(ctx, `SELECT schema_version,ciphertext FROM remote_recovery_requests WHERE operation_id=?`, id).Scan(&version, &sealed); e != nil {
		return e
	}
	raw, e := s.open(version, sealed, requestAAD(o, r, op))
	if e != nil {
		return e
	}
	if e = proto.Unmarshal(raw, m); e != nil {
		return ErrConflict
	}
	canonical, e := managed.Canonical(m)
	if e != nil || !bytes.Equal(canonical, raw) {
		return ErrConflict
	}
	d, e := managed.Digest(m)
	if e != nil || hex.EncodeToString(d) != op.PayloadDigest {
		return ErrConflict
	}
	return nil
}
func (s *RequestStore) SaveManagedTx(ctx context.Context, tx *sql.Tx, o Owner, r Reference, request *pb.StartExecutionRequest) error {
	if managed.ValidateStart(request) != nil {
		return ErrConflict
	}
	return s.put(ctx, tx, o, r, request.Execution.RequestId, Execute, request)
}
func (s *RequestStore) ReadManaged(ctx context.Context, q Reader, o Owner, r Reference, id string) (*pb.StartExecutionRequest, error) {
	m := new(pb.StartExecutionRequest)
	if e := s.get(ctx, q, o, r, id, Execute, m); e != nil {
		return nil, e
	}
	if managed.ValidateStart(m) != nil || m.Execution.RequestId != id {
		return nil, ErrConflict
	}
	return m, nil
}
func (s *RequestStore) SaveCreateTx(ctx context.Context, tx *sql.Tx, o Owner, r Reference, request *native.CreateSandboxRequest) error {
	if request == nil || request.RequestId != r.CreationID || request.Spec == nil {
		return ErrConflict
	}
	return s.put(ctx, tx, o, r, request.RequestId, Create, request)
}
func (s *RequestStore) ReadCreate(ctx context.Context, q Reader, o Owner, r Reference) (*native.CreateSandboxRequest, error) {
	m := new(native.CreateSandboxRequest)
	if e := s.get(ctx, q, o, r, r.CreationID, Create, m); e != nil {
		return nil, e
	}
	if m.RequestId != r.CreationID || m.Spec == nil {
		return nil, ErrConflict
	}
	return m, nil
}

// ClaimManagedDispatchTx requires recoverable payload before changing delivery.
// False means a prior dispatcher owns the uncertain operation; never resend it.
func (s *RequestStore) ClaimManagedDispatchTx(ctx context.Context, tx *sql.Tx, o Owner, r Reference, id string) (bool, error) {
	if _, e := s.ReadManaged(ctx, tx, o, r, id); e != nil {
		return false, e
	}
	return ClaimDispatchTx(ctx, tx, o, r, id)
}
func (s *RequestStore) ClaimCreateDispatchTx(ctx context.Context, tx *sql.Tx, o Owner, r Reference) (bool, error) {
	if _, e := s.ReadCreate(ctx, tx, o, r); e != nil {
		return false, e
	}
	return ClaimDispatchTx(ctx, tx, o, r, r.CreationID)
}

// ReserveCreateTx allocates the durable native name before hashing the request.
// The caller must check canonical preparation authority in this transaction and
// commit before any RPC. A rollback exposes no allocation to the remote peer.
// template carries no name or request ID; both are set by this owner.
func (s *RequestStore) ReserveCreateTx(ctx context.Context, tx *sql.Tx, owner Owner, target Target, creationID string, template *native.CreateSandboxRequest) (Reservation, *native.CreateSandboxRequest, error) {
	if s == nil || s.aead == nil || !validOwner(owner) || !validTarget(target) || !validUUID(creationID) || template == nil || template.Spec == nil || template.Name != "" || template.RequestId != "" || proto.Size(template) > maxRecoveryRequest {
		return Reservation{}, nil, ErrConflict
	}
	var allocation int64
	err := tx.QueryRowContext(ctx, `SELECT allocation FROM remote_runtimes WHERE creation_id=?`, creationID).Scan(&allocation)
	existing := err == nil
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return Reservation{}, nil, err
	}
	if !existing {
		allocation, err = allocateRuntimeTx(ctx, tx)
		if err != nil {
			return Reservation{}, nil, err
		}
	}
	request := proto.Clone(template).(*native.CreateSandboxRequest)
	request.Name = "k-" + strconv.FormatInt(allocation, 36)
	request.RequestId = creationID
	d, err := managed.Digest(request)
	if err != nil {
		return Reservation{}, nil, err
	}
	digest := hex.EncodeToString(d)
	var reservation Reservation
	if existing {
		reservation, err = ReserveRuntimeTx(ctx, tx, owner, target, creationID, digest)
	} else {
		reservation, err = insertRuntimeTx(ctx, tx, owner, target, creationID, digest, allocation)
	}
	if err != nil {
		return Reservation{}, nil, err
	}
	if err = s.SaveCreateTx(ctx, tx, owner, reservation.Reference, request); err != nil {
		return Reservation{}, nil, err
	}
	return reservation, request, nil
}
