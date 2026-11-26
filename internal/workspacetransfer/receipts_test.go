//go:build linux || darwin

package workspacetransfer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestReceiptNameMatchesCanonicalNativeGrant(t *testing.T) {
	if !ReceiptName("native_" + strings.Repeat("a", 64) + ".json") {
		t.Fatal("canonical native grant filename rejected")
	}
	if ReceiptName(uuid.NewString() + ".json") {
		t.Fatal("noncanonical grant filename accepted")
	}
}

func TestReceiptTransferExportsOnlyFixedOutbox(t *testing.T) {
	root := t.TempDir()
	outbox := filepath.Join(root, ".kingdom-provider", "outbox")
	if err := os.MkdirAll(outbox, 0700); err != nil {
		t.Fatal(err)
	}
	name := receiptFixtureName()
	for path, content := range map[string]string{
		filepath.Join(outbox, name):                             `{"receipt":"retained"}`,
		filepath.Join(outbox, name+"."+uuid.NewString()+".tmp"): "incomplete",
		filepath.Join(root, ".kingdom-provider", "config.json"): "private capability",
	} {
		if err := os.WriteFile(path, []byte(content), 0600); err != nil {
			t.Fatal(err)
		}
	}
	job := testJob("receipts")
	job.Limits = ReceiptLimits()
	helper := startHelper(t, root, job)
	response, err := helper.request("GET", "/v1/archive", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if err = helper.finish(t); err != nil {
		t.Fatal(err)
	}
	staged, err := ValidateExport(context.Background(), t.TempDir(), bytes.NewReader(raw), int64(len(raw)), bytes.NewReader(helper.output.Bytes()), job)
	if err != nil {
		t.Fatal(err)
	}
	defer staged.Close()
	if len(staged.Manifest.Entries) != 1 || staged.Manifest.Entries[0].Path != name {
		t.Fatalf("unexpected receipt archive: %+v", staged.Manifest.Entries)
	}
	wrong := job
	wrong.Mode = "export"
	if s, e := ValidateExport(context.Background(), t.TempDir(), bytes.NewReader(raw), int64(len(raw)), bytes.NewReader(helper.output.Bytes()), wrong); e == nil {
		s.Close()
		t.Fatal("receipt archive relabeled as workspace")
	}
}

func TestReceiptSnapshotRejectsUnsafeEntriesAndLimits(t *testing.T) {
	for _, scenario := range []string{"parent symlink", "file symlink", "foreign file", "oversized", "too many"} {
		t.Run(scenario, func(t *testing.T) {
			root := t.TempDir()
			runtime := filepath.Join(root, ".kingdom-provider")
			outbox := filepath.Join(runtime, "outbox")
			if err := os.MkdirAll(outbox, 0700); err != nil {
				t.Fatal(err)
			}
			limits := ReceiptLimits()
			name := receiptFixtureName()
			var err error
			switch scenario {
			case "parent symlink":
				outside := t.TempDir()
				err = os.Rename(runtime, filepath.Join(outside, "runtime"))
				if err == nil {
					err = os.Symlink(filepath.Join(outside, "runtime"), runtime)
				}
			case "file symlink":
				err = os.Symlink("/etc/passwd", filepath.Join(outbox, name))
			case "foreign file":
				err = os.WriteFile(filepath.Join(outbox, "config.json"), []byte("private"), 0600)
			case "oversized":
				limits.FileBytes = 1
				err = os.WriteFile(filepath.Join(outbox, name), []byte("{}"), 0600)
			case "too many":
				limits.Entries = 1
				err = os.WriteFile(filepath.Join(outbox, name), []byte("{}"), 0600)
				if err == nil {
					err = os.WriteFile(filepath.Join(outbox, receiptFixtureName()), []byte("{}"), 0600)
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err = snapshotReceipts(context.Background(), root, t.TempDir(), limits); err == nil {
				t.Fatal("unsafe outbox accepted")
			}
		})
	}
}

func receiptFixtureName() string {
	hash := sha256.Sum256([]byte(uuid.NewString()))
	return "native_" + hex.EncodeToString(hash[:]) + ".json"
}
