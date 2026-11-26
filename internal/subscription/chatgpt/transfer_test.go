package chatgpt

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	jose "github.com/go-jose/go-jose/v4"
	"github.com/google/uuid"
)

func TestSelectedSessionTransferFencesSourceAndPreservesTargetHost(t *testing.T) {
	source, target := newFixture(t), newFixture(t)
	ctx := context.Background()
	reg, err := source.callback(source.flow(""))
	if err != nil {
		t.Fatal(err)
	}
	host := target.v.HostID()
	transfer, err := source.s.ExportForTransfer(ctx, "owner", reg.ID, host)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = source.s.Access(ctx, "owner", reg.ID); !errors.Is(err, ErrReauthenticate) {
		t.Fatalf("source still owns rotating session: %v", err)
	}
	if _, _, err = source.s.Access(ctx, "foreign-owner", reg.ID); !errors.Is(err, ErrDenied) {
		t.Fatalf("foreign source access: %v", err)
	}
	retry, err := source.s.ExportForTransfer(ctx, "owner", reg.ID, host)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := EncodeTransfer(transfer)
	if err != nil {
		t.Fatal(err)
	}
	retried, err := EncodeTransfer(retry)
	if err != nil || !bytes.Equal(encoded, retried) {
		t.Fatal("export retry changed credentials or transfer identity")
	}
	if _, err = source.s.ExportForTransfer(ctx, "owner", reg.ID, "urn:uuid:"+uuid.NewString()); !errors.Is(err, ErrDenied) {
		t.Fatalf("fenced export could change destination: %v", err)
	}
	if _, err = json.Marshal(transfer); err == nil {
		t.Fatal("transfer encoded through ordinary JSON")
	}
	if strings.Contains(fmt.Sprintf("%v %#v", transfer, transfer), "refresh-token") {
		t.Fatal("transfer leaked to formatting")
	}
	decoded, err := DecodeTransfer(encoded)
	if err != nil {
		t.Fatal(err)
	}
	imported, err := target.s.ImportTransfer(ctx, "target-owner", decoded)
	if err != nil {
		t.Fatal(err)
	}
	if imported.ID == reg.ID || imported.ClientID != reg.ClientID || imported.Subject != reg.Subject || imported.Revision != 1 || imported.State != "ready" {
		t.Fatalf("wrong target registration: %+v", imported)
	}
	if target.v.HostID() != host || target.v.HostID() == source.v.HostID() {
		t.Fatal("source host replaced target host")
	}
	token, current, err := target.s.Access(ctx, "target-owner", imported.ID)
	if err != nil || token.Bearer() != "access-token" || current != imported {
		t.Fatalf("target cannot own imported session: %+v %v", current, err)
	}
	again, err := target.s.ImportTransfer(ctx, "target-owner", decoded)
	if err != nil || again != imported {
		t.Fatalf("target import retry changed reference: %+v %v", again, err)
	}
	changed := decoded
	changed.payload.Refresh = "different-refresh-token"
	if _, err = target.s.ImportTransfer(ctx, "target-owner", changed); !errors.Is(err, ErrDenied) {
		t.Fatalf("retry changed imported credentials: %v", err)
	}
	if _, err = target.s.ImportTransfer(ctx, "another-owner", decoded); !errors.Is(err, ErrDenied) {
		t.Fatalf("replay changed target owner: %v", err)
	}
	if _, err = source.s.Disconnect(ctx, "owner", reg.ID); !errors.Is(err, ErrDenied) {
		t.Fatalf("source may revoke the target-owned session: %v", err)
	}
	var encrypted []byte
	if err = target.v.db.QueryRow(`SELECT credential FROM chatgpt_registrations WHERE id=?`, imported.ID).Scan(&encrypted); err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(encrypted, []byte("refresh-token")) {
		t.Fatal("target stored plaintext credentials")
	}
	if err = source.s.AcknowledgeTransfer(ctx, "owner", reg.ID, transfer); err != nil {
		t.Fatal(err)
	}
	if err = source.s.AcknowledgeTransfer(ctx, "owner", reg.ID, transfer); err != nil {
		t.Fatalf("ack retry: %v", err)
	}
	saved, err := source.v.read(ctx, "owner", reg.ID)
	if err != nil || len(saved.encrypted) != 0 || saved.State != "signed_out" {
		t.Fatalf("source token copy retained: %+v %v", saved.Registration, err)
	}
	var retained int
	if err = source.v.db.QueryRow(`SELECT count(*) FROM chatgpt_exports WHERE payload IS NOT NULL`).Scan(&retained); err != nil || retained != 0 {
		t.Fatalf("source export token copy retained: %d %v", retained, err)
	}
	if _, _, err = target.s.Access(ctx, "target-owner", imported.ID); err != nil {
		t.Fatalf("source ack revoked target session: %v", err)
	}
	// A ready target can replay an old transfer after it has rotated its token.
	// The saved request fingerprint is independent of that current generation.
	target.s.now = func() time.Time { return time.Now().Add(3541 * time.Second) }
	if _, _, err = target.s.Access(ctx, "target-owner", imported.ID); err != nil {
		t.Fatalf("target refresh failed: %v", err)
	}
	target.s.now = func() time.Time { return time.Now().Add(4000 * time.Second) }
	if replay, err := target.s.ImportTransfer(ctx, "target-owner", decoded); err != nil || replay.ID != imported.ID {
		t.Fatalf("ready target lost exact replay after refresh: %+v %v", replay, err)
	}
	if confirmed, err := target.s.Disconnect(ctx, "target-owner", imported.ID); err != nil || !confirmed {
		t.Fatalf("target disconnect: %v %v", confirmed, err)
	}
	if _, err = target.s.ImportTransfer(ctx, "target-owner", decoded); !errors.Is(err, ErrReauthenticate) {
		t.Fatalf("signed-out import was replayed as connected: %v", err)
	}
}

func TestTransferRejectsTamperingAndNeverOverwritesARegistration(t *testing.T) {
	for _, field := range []string{"target_host_id", "subject", "client_id", "scope", "expires_at", "id_token"} {
		t.Run(field, func(t *testing.T) {
			source, target := newFixture(t), newFixture(t)
			ctx := context.Background()
			reg, err := source.callback(source.flow(""))
			if err != nil {
				t.Fatal(err)
			}
			transfer, err := source.s.ExportForTransfer(ctx, "owner", reg.ID, target.v.HostID())
			if err != nil {
				t.Fatal(err)
			}
			encoded, err := EncodeTransfer(transfer)
			if err != nil {
				t.Fatal(err)
			}
			var data map[string]any
			if err = json.Unmarshal(encoded, &data); err != nil {
				t.Fatal(err)
			}
			if field == "expires_at" {
				data[field] = time.Now().Add(-time.Hour).Unix()
			} else {
				data[field] = "invalid"
			}
			encoded, _ = json.Marshal(data)
			bad, err := DecodeTransfer(encoded)
			if err == nil {
				_, err = target.s.ImportTransfer(ctx, "target-owner", bad)
			}
			if err == nil {
				t.Fatal("changed transfer was accepted")
			}
			var count int
			if err = target.v.db.QueryRow(`SELECT count(*) FROM chatgpt_registrations`).Scan(&count); err != nil || count != 0 {
				t.Fatalf("rejected transfer changed target: %d %v", count, err)
			}
		})
	}
}

func TestTransferredClientReconnectGetsANewExactReference(t *testing.T) {
	source, target := newFixture(t), newFixture(t)
	ctx := context.Background()
	reg, err := source.callback(source.flow(""))
	if err != nil {
		t.Fatal(err)
	}
	first, err := source.s.ExportForTransfer(ctx, "owner", reg.ID, target.v.HostID())
	if err != nil {
		t.Fatal(err)
	}
	a, err := target.s.ImportTransfer(ctx, "target-owner", first)
	if err != nil {
		t.Fatal(err)
	}
	reg, err = source.callback(source.flow(reg.ID))
	if err != nil {
		t.Fatal(err)
	}
	second, err := source.s.ExportForTransfer(ctx, "owner", reg.ID, target.v.HostID())
	if err != nil {
		t.Fatal(err)
	}
	b, err := target.s.ImportTransfer(ctx, "target-owner", second)
	if err != nil || a.ID == b.ID || a.ClientID != b.ClientID {
		t.Fatalf("reconnect overwrote prior reference: %+v %+v %v", a, b, err)
	}
	if err = source.s.AcknowledgeTransfer(ctx, "owner", reg.ID, first); err != nil {
		t.Fatal(err)
	}
	if _, err = source.s.ExportForTransfer(ctx, "owner", reg.ID, target.v.HostID()); err != nil {
		t.Fatalf("late ack removed newer export: %v", err)
	}
	if _, err = target.s.ImportTransfer(ctx, "another-owner", second); !errors.Is(err, ErrDenied) {
		t.Fatalf("connection changed owner: %v", err)
	}
}

func TestTransferImportVerifiesIDTokenClaims(t *testing.T) {
	for _, field := range []string{"iss", "aud", "sub", "exp"} {
		t.Run(field, func(t *testing.T) {
			source, target := newFixture(t), newFixture(t)
			ctx := context.Background()
			reg, err := source.callback(source.flow(""))
			if err != nil {
				t.Fatal(err)
			}
			transfer, err := source.s.ExportForTransfer(ctx, "owner", reg.ID, target.v.HostID())
			if err != nil {
				t.Fatal(err)
			}
			claims := map[string]any{"iss": issuer, "aud": reg.ClientID, "sub": reg.Subject, "exp": time.Now().Add(time.Hour).Unix()}
			claims[field] = "different"
			if field == "exp" {
				claims[field] = time.Now().Add(-time.Minute).Unix()
			}
			key := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{2}, 32))
			signer, err := jose.NewSigner(jose.SigningKey{Algorithm: jose.EdDSA, Key: key}, (&jose.SignerOptions{}).WithHeader("kid", "test"))
			if err != nil {
				t.Fatal(err)
			}
			raw, _ := json.Marshal(claims)
			signed, err := signer.Sign(raw)
			if err != nil {
				t.Fatal(err)
			}
			transfer.payload.IDToken, err = signed.CompactSerialize()
			if err != nil {
				t.Fatal(err)
			}
			if _, err = target.s.ImportTransfer(ctx, "target-owner", transfer); !errors.Is(err, ErrDenied) {
				t.Fatalf("invalid ID %s accepted: %v", field, err)
			}
		})
	}
}

func TestTransferDecodeRejectsAmbiguousJSON(t *testing.T) {
	source, target := newFixture(t), newFixture(t)
	reg, err := source.callback(source.flow(""))
	if err != nil {
		t.Fatal(err)
	}
	transfer, err := source.s.ExportForTransfer(context.Background(), "owner", reg.ID, target.v.HostID())
	if err != nil {
		t.Fatal(err)
	}
	raw, err := EncodeTransfer(transfer)
	if err != nil {
		t.Fatal(err)
	}
	for _, ambiguous := range []string{string(raw) + " {}", `{"version":1,` + string(raw[1:]), `{"unknown":true,` + string(raw[1:])} {
		if _, err = DecodeTransfer([]byte(ambiguous)); err == nil {
			t.Fatal("ambiguous transfer JSON accepted")
		}
	}
}

func TestTransferRecoverySurvivesVaultReopenAndConcurrentImport(t *testing.T) {
	source, target := newFixture(t), newFixture(t)
	ctx := context.Background()
	reg, err := source.callback(source.flow(""))
	if err != nil {
		t.Fatal(err)
	}
	transfer, err := source.s.ExportForTransfer(ctx, "owner", reg.ID, target.v.HostID())
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := NewVault(ctx, source.v.db, bytes.Repeat([]byte{1}, 32))
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := NewService(ctx, reopened, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close(ctx)
	if _, _, err = recovered.Access(ctx, "owner", reg.ID); !errors.Is(err, ErrReauthenticate) {
		t.Fatalf("source fence lost on reopen: %v", err)
	}
	copy, err := recovered.ExportForTransfer(ctx, "owner", reg.ID, target.v.HostID())
	if err != nil {
		t.Fatal(err)
	}
	original, _ := EncodeTransfer(transfer)
	retried, _ := EncodeTransfer(copy)
	if !bytes.Equal(original, retried) {
		t.Fatal("reopened export changed the selected session")
	}
	secondVault, err := NewVault(ctx, target.v.db, bytes.Repeat([]byte{1}, 32))
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewService(ctx, secondVault, &http.Client{Transport: target.s.client.Transport})
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close(ctx)
	var wg sync.WaitGroup
	results := make(chan Registration, 2)
	for _, service := range []*Service{target.s, second} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			imported, err := service.ImportTransfer(ctx, "target-owner", transfer)
			if err != nil {
				t.Error(err)
				return
			}
			results <- imported
		}()
	}
	wg.Wait()
	close(results)
	var id string
	for imported := range results {
		if id != "" && id != imported.ID {
			t.Fatal("concurrent import created two references")
		}
		id = imported.ID
	}
	if id == "" {
		t.Fatal("no import succeeded")
	}
	var count int
	if err = target.v.db.QueryRow(`SELECT count(*) FROM chatgpt_registrations`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("duplicate target registration: %d %v", count, err)
	}
}
