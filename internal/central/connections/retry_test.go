package connections

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"github.com/google/uuid"
)

func TestCredentialRetryFingerprintRequiresVaultKey(t *testing.T) {
	s, db, _, member := setup(t)
	ctx := context.Background()
	secret := []byte("low-entropy-fixture-key")
	d := definition()
	key := uuid.NewString()
	_, err := s.CreateRequest(ctx, member.ID, Personal, d, secret, key)
	if err != nil {
		t.Fatal(err)
	}
	var actual []byte
	if err := db.QueryRow(`SELECT fingerprint FROM central_mutations WHERE actor=? AND request_id=?`, member.ID, key).Scan(&actual); err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal(struct {
		Scope      Scope
		Definition Definition
		Credential []byte
	}{Personal, d, secret})
	public := sha256.Sum256(payload)
	if bytes.Equal(actual, public[:]) {
		t.Fatal("credential fingerprint is publicly guessable")
	}
	derive := hmac.New(sha256.New, bytes.Repeat([]byte{1}, 32))
	derive.Write([]byte("kingdom-central-mutation-fingerprint/v1"))
	mac := hmac.New(sha256.New, derive.Sum(nil))
	mac.Write(payload)
	if !hmac.Equal(actual, mac.Sum(nil)) {
		t.Fatal("fingerprint not derived from vault key and domain")
	}
	if _, err := db.Exec(`UPDATE central_users SET disabled=1 WHERE id=?`, member.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateRequest(ctx, member.ID, Personal, d, secret, key); !errors.Is(err, ErrDenied) {
		t.Fatalf("disabled replay accepted: %v", err)
	}
}
func TestConcurrentConnectionCreationUsesOneRevision(t *testing.T) {
	s, db, _, member := setup(t)
	ctx := context.Background()
	key := uuid.NewString()
	var wg sync.WaitGroup
	results := make(chan Metadata, 8)
	errs := make(chan error, 8)
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			m, e := s.CreateRequest(ctx, member.ID, Personal, definition(), []byte("fixture-key"), key)
			results <- m
			errs <- e
		}()
	}
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	id := ""
	for m := range results {
		if id == "" {
			id = m.ID
		}
		if m.ID != id {
			t.Fatal("duplicate connection")
		}
	}
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM central_connection_revisions`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("revision count=%d err=%v", count, err)
	}
}
