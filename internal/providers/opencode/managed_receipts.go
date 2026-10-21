package opencode

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"

	"kingdom/internal/core/providerexecution"
	"kingdom/internal/securefs"
	"kingdom/internal/workspacearchive"
	"kingdom/internal/workspacetransfer"
)

// RecoverManagedReceipts consumes a private host staging directory produced by
// ValidateExport for the fixed receipts helper. The worker must first verify
// the exact helper identity, clean receipt, complete output and archive hash.
// No workload path becomes a host path; the only reader root is this staging.
func RecoverManagedReceipts(ctx context.Context, staged *workspacearchive.Staged, identity providerexecution.RequestIdentity, authority ManagedNativeAuthority) error {
	limits := workspacetransfer.ReceiptLimits()
	if staged == nil || identity.Validate() != nil || nilManagedDependency(authority) || len(staged.Manifest.Entries) > limits.Entries || staged.Manifest.ExpandedBytes < 0 || staged.Manifest.ExpandedBytes > limits.ExpandedBytes {
		return errors.New("invalid managed receipt archive")
	}
	root, err := securefs.OpenRoot(staged.Path)
	if err != nil {
		return err
	}
	defer root.Close()
	var receipts []nativeReceipt
	seen := make(map[string]struct{})
	var total int64
	for _, entry := range staged.Manifest.Entries {
		if err = ctx.Err(); err != nil {
			return err
		}
		if entry.Kind != "file" || entry.Executable || !workspacetransfer.ReceiptName(entry.Path) || entry.Size < 1 || entry.Size > limits.FileBytes {
			return errors.New("invalid managed receipt entry")
		}
		if _, exists := seen[entry.Path]; exists {
			return errors.New("duplicate managed receipt")
		}
		seen[entry.Path] = struct{}{}
		total += entry.Size
		if total > limits.ExpandedBytes {
			return errors.New("managed receipt archive limit exceeded")
		}
		raw, err := root.ReadRegular(entry.Path, entry.Size)
		if err != nil {
			return err
		}
		hash := sha256.Sum256(raw)
		if int64(len(raw)) != entry.Size || hex.EncodeToString(hash[:]) != entry.SHA256 {
			return errors.New("managed receipt content changed")
		}
		var receipt nativeReceipt
		if decodeStrict(raw, &receipt) != nil || receipt.Identity != identity || receipt.GrantID != strings.TrimSuffix(entry.Path, ".json") {
			return errors.New("managed receipt identity conflict")
		}
		receipts = append(receipts, receipt)
	}
	if total != staged.Manifest.ExpandedBytes {
		return errors.New("managed receipt manifest size conflict")
	}
	// Validate the entire archive before applying any receipt. Canonical
	// authority still verifies each retained grant and its immutable digest.
	for _, receipt := range receipts {
		if err = authority.FinishNative(ctx, identity, receipt.providerReceipt()); err != nil {
			return err
		}
	}
	return nil
}
