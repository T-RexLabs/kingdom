package personal

import (
	"bytes"
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"

	"kingdom/internal/central/accounts"
	_ "modernc.org/sqlite"
)

func TestCopilotCredentialsRemainEncryptedAndOwnerBound(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "accounts.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s, err := New(ctx, Config{DB: db, Key: bytes.Repeat([]byte{7}, 32)})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close(ctx)
	ref, err := s.SaveCopilot(ctx, "owner", accounts.OAuthCredential{Type: "oauth", MethodID: "device", Access: "private-token", Refresh: "private-token"})
	if err != nil {
		t.Fatal(err)
	}
	var encrypted []byte
	if err = db.QueryRow(`SELECT credential FROM central_personal_accounts WHERE id=?`, ref.ID).Scan(&encrypted); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encrypted), "private-token") {
		t.Fatal("plaintext token stored")
	}
	if _, err = s.read(ctx, "other", ref); err == nil {
		t.Fatal("another owner resolved token")
	}
	changed := ref
	changed.Revision++
	if _, err = s.read(ctx, "owner", changed); err == nil {
		t.Fatal("another authorization revision resolved token")
	}
	if _, err = s.read(ctx, "owner", ref); err != nil {
		t.Fatal(err)
	}
	if err = s.fence(ctx, "owner", ref); err != nil {
		t.Fatal(err)
	}
	if _, err = s.read(ctx, "owner", ref); err == nil {
		t.Fatal("disconnected account resolved token")
	}
}

func TestCopilotCredentialRejectsForeignEndpointsAndLegacyExchange(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s, err := New(ctx, Config{DB: db, Key: bytes.Repeat([]byte{9}, 32)})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close(ctx)
	for _, c := range []accounts.OAuthCredential{
		{Type: "oauth", MethodID: "device", Access: "token", Refresh: "token", Metadata: map[string]string{"apiEndpoint": "https://attacker.example"}},
		{Type: "oauth", MethodID: "device", Access: "token", Refresh: "token", Metadata: map[string]string{"apiEndpoint": "https://api.githubcopilot.com/foreign"}},
		{Type: "oauth", MethodID: "device", Access: "token", Refresh: "token", Metadata: map[string]string{"enterpriseUrl": "company.example"}},
		{Type: "oauth", MethodID: "device", Access: "token", Refresh: "different"},
		{Type: "oauth", MethodID: "device", Access: "token", Refresh: "token", Expires: 1234},
	} {
		if _, err = s.SaveCopilot(ctx, "owner", c); err == nil {
			t.Fatal("unsupported Copilot credential accepted")
		}
	}
}
