package cli

import (
	"bytes"
	"context"
	"encoding/hex"
	"github.com/google/uuid"
	"kingdom/internal/execution/openshell/localstack"
	"kingdom/internal/subscription/chatgpt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestChatGPTStatusUsesProtectedLocalVaultWithoutNetwork(t *testing.T) {
	root := filepath.Join(t.TempDir(), "vault")
	owner := uuid.NewString()
	master := bytes.Repeat([]byte{1}, 32)
	keyFile := filepath.Join(t.TempDir(), "master.key")
	if e := os.WriteFile(keyFile, []byte(hex.EncodeToString(master)), 0600); e != nil {
		t.Fatal(e)
	}
	key := localstack.OwnerCredentialKey(master, owner)
	vault, e := chatgpt.OpenFileVault(context.Background(), root, key)
	if e != nil {
		t.Fatal(e)
	}
	vault.Close()
	var out bytes.Buffer
	e = runChatGPTCommand(context.Background(), &out, []string{"status", "--state-root", root, "--owner-id", owner, "--master-key-file", keyFile, "--registration-id", uuid.NewString()})
	if e == nil || strings.Contains(out.String(), hex.EncodeToString(master)) {
		t.Fatal("unknown registration or credential leak")
	}
	if e = os.Chmod(keyFile, 0644); e != nil {
		t.Fatal(e)
	}
	if _, e = readChatGPTKey(keyFile, owner); e == nil {
		t.Fatal("readable secret accepted")
	}
}
func TestChatGPTCommandRejectsImplicitOwnerAndStorage(t *testing.T) {
	for _, args := range [][]string{{}, {"sign-in"}, {"status", "--state-root", "relative", "--owner-id", uuid.NewString()}, {"disconnect", "--owner-id", "other"}} {
		if e := runChatGPTCommand(context.Background(), &bytes.Buffer{}, args); e == nil {
			t.Fatal("invalid auth configuration accepted")
		}
	}
}
