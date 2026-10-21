package workspacetransfer

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/uuid"
	"kingdom/internal/providerlauncher"
	"kingdom/internal/securefs"
	"kingdom/internal/workspacearchive"
)

// ReceiptName admits only the plugin's fixed, canonical grant filenames.
func ReceiptName(name string) bool {
	if !strings.HasSuffix(name, ".json") {
		return false
	}
	raw := strings.TrimSuffix(name, ".json")
	id, err := uuid.Parse(raw)
	return err == nil && id != uuid.Nil && id.String() == raw
}
func receiptTemporary(name string) bool {
	parts := strings.Split(name, ".")
	if len(parts) != 4 || parts[1] != "json" || parts[3] != "tmp" || !ReceiptName(parts[0]+".json") {
		return false
	}
	id, err := uuid.Parse(parts[2])
	return err == nil && id != uuid.Nil && id.String() == parts[2]
}

// Snapshot only receipt files. Never export the surrounding runtime directory,
// which contains configuration and ephemeral service capabilities.
func snapshotReceipts(ctx context.Context, root, scratch string, limits workspacearchive.Limits) (string, error) {
	input, err := securefs.OpenRoot(root)
	if err != nil {
		return "", err
	}
	defer input.Close()
	relative := strings.TrimPrefix(providerlauncher.RuntimePath, WorkspaceParent+"/") + "/outbox"
	entries, err := input.ReadDirBounded(relative, limits.Entries)
	if err != nil {
		return "", err
	}
	destination := filepath.Join(scratch, "receipts")
	if err = os.Mkdir(destination, 0700); err != nil {
		return "", err
	}
	output, err := securefs.OpenRoot(destination)
	if err != nil {
		return "", err
	}
	defer output.Close()
	remaining := limits.ExpandedBytes
	for _, entry := range entries {
		if err = ctx.Err(); err != nil {
			return "", err
		}
		name := entry.Name
		if entry.Type != securefs.EntryTypeRegular {
			return "", errors.New("invalid receipt entry")
		}
		if receiptTemporary(name) {
			continue
		}
		if !ReceiptName(name) {
			return "", errors.New("invalid receipt filename")
		}
		raw, e := input.ReadRegular(relative+"/"+name, min(limits.FileBytes, remaining))
		if e != nil {
			return "", e
		}
		remaining -= int64(len(raw))
		if _, e = output.CreateRegularExact(name, raw, 0600, 0700); e != nil {
			return "", e
		}
	}
	return destination, nil
}
