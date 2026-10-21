package opencode

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"kingdom/internal/workspacearchive"
)

func TestManagedReceiptRecoveryRejectsForeignArchiveBeforeMutation(t *testing.T) {
	for _, scenario := range []string{"valid", "foreign request", "foreign grant", "changed content", "duplicate", "executable"} {
		t.Run(scenario, func(t *testing.T) {
			f := callbackOwner()
			staged := &workspacearchive.Staged{Path: t.TempDir()}
			for i := 0; i < 2; i++ {
				grant := uuid.NewString()
				started := true
				receipt := nativeReceipt{Identity: f.identity, GrantID: grant, IdentityDigest: "digest", Outcome: "completed", NativeStarted: &started}
				if i == 1 && scenario == "foreign request" {
					receipt.Identity.RequestID = "foreign"
				}
				if i == 1 && scenario == "foreign grant" {
					receipt.GrantID = uuid.NewString()
				}
				raw, err := json.Marshal(receipt)
				if err != nil {
					t.Fatal(err)
				}
				hash := sha256.Sum256(raw)
				entry := workspacearchive.Entry{Path: grant + ".json", Kind: "file", Size: int64(len(raw)), SHA256: hex.EncodeToString(hash[:])}
				if i == 1 && scenario == "changed content" {
					raw[0] = '['
				}
				if i == 1 && scenario == "executable" {
					entry.Executable = true
				}
				if err = os.WriteFile(filepath.Join(staged.Path, entry.Path), raw, 0600); err != nil {
					t.Fatal(err)
				}
				if i == 1 && scenario == "duplicate" {
					entry = staged.Manifest.Entries[0]
				}
				staged.Manifest.Entries = append(staged.Manifest.Entries, entry)
				staged.Manifest.ExpandedBytes += entry.Size
			}
			err := RecoverManagedReceipts(context.Background(), staged, f.identity, f)
			if scenario == "valid" {
				if err != nil || f.finished != 2 {
					t.Fatalf("valid recovery: %v %d", err, f.finished)
				}
			} else if err == nil || f.finished != 0 {
				t.Fatalf("invalid archive applied receipts: %v %d", err, f.finished)
			}
		})
	}
}
