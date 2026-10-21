// Package mutation records central control-plane retries in the transaction
// that owns the change. Callers must recheck current authority before Replay.
package mutation

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/google/uuid"
)

var ErrConflict = errors.New("idempotency key conflict")
var ErrInvalid = errors.New("invalid idempotency key")

func Initialize(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS central_mutation_schema (slot INTEGER PRIMARY KEY CHECK(slot=1),version INTEGER NOT NULL);
INSERT INTO central_mutation_schema VALUES(1,1) ON CONFLICT DO NOTHING;
CREATE TABLE IF NOT EXISTS central_mutations (actor TEXT NOT NULL,request_id TEXT NOT NULL,operation TEXT NOT NULL,resource TEXT NOT NULL,fingerprint BLOB NOT NULL CHECK(length(fingerprint)=32),response BLOB NOT NULL,PRIMARY KEY(actor,request_id));
CREATE TRIGGER IF NOT EXISTS central_mutation_immutable BEFORE UPDATE ON central_mutations BEGIN SELECT RAISE(ABORT,'mutation record is immutable'); END;
CREATE TRIGGER IF NOT EXISTS central_mutation_retained BEFORE DELETE ON central_mutations BEGIN SELECT RAISE(ABORT,'mutation record must be retained'); END;`)
	if err != nil {
		return err
	}
	var version int
	if err := tx.QueryRowContext(ctx, `SELECT version FROM central_mutation_schema WHERE slot=1`).Scan(&version); err != nil {
		return err
	}
	if version != 1 {
		return errors.New("unsupported central mutation schema")
	}
	return nil
}
func ValidKey(key string) bool {
	id, err := uuid.Parse(key)
	return err == nil && id.String() == key && id != uuid.Nil
}

// Fingerprint is for public metadata only. Secret-bearing requests must use a
// keyed digest supplied by the credential store.
func Fingerprint(payload any) ([32]byte, error) {
	b, e := json.Marshal(payload)
	return sha256.Sum256(b), e
}
func Replay[T any](ctx context.Context, tx *sql.Tx, actor, key, operation, resource string, fingerprint [32]byte) (T, bool, error) {
	var result T
	if key == "" {
		return result, false, nil
	} // trusted in-process calls have no HTTP retry key
	if !ValidKey(key) {
		return result, false, ErrInvalid
	}
	var op, res string
	var digest, response []byte
	err := tx.QueryRowContext(ctx, `SELECT operation,resource,fingerprint,response FROM central_mutations WHERE actor=? AND request_id=?`, actor, key).Scan(&op, &res, &digest, &response)
	if errors.Is(err, sql.ErrNoRows) {
		return result, false, nil
	}
	if err != nil {
		return result, false, err
	}
	if op != operation || res != resource || string(digest) != string(fingerprint[:]) {
		return result, false, ErrConflict
	}
	if err := json.Unmarshal(response, &result); err != nil {
		return result, false, err
	}
	return result, true, nil
}

// Record accepts only metadata selected by the owning store, never request bodies.
func Record(ctx context.Context, tx *sql.Tx, actor, key, operation, resource string, fingerprint [32]byte, response any) error {
	if key == "" {
		return nil
	}
	if !ValidKey(key) {
		return ErrInvalid
	}
	b, err := json.Marshal(response)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO central_mutations(actor,request_id,operation,resource,fingerprint,response) VALUES(?,?,?,?,?,?)`, actor, key, operation, resource, fingerprint[:], b)
	return err
}
