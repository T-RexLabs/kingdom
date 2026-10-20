package processguardian

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io/fs"

	"kingdom/internal/securefs"
)

func publishEvidence(spec wireSpec, result Result) (string, string, error) {
	if !spec.Durable {
		return "", "", nil
	}
	if spec.RuntimeRoot == "" {
		return "", "", fmt.Errorf("runtime root is required for durable command guardian")
	}
	raw, err := json.Marshal(wireStatus{Type: "result", Result: result})
	if err != nil {
		return "", "", err
	}
	root, err := securefs.OpenRoot(spec.RuntimeRoot)
	if err != nil {
		return "", "", err
	}
	defer root.Close()
	const directory = "provider-process-guards/quiescence"
	if err := root.EnsureDir(directory, fs.FileMode(0o700)); err != nil {
		return "", "", err
	}
	name := sha256.Sum256([]byte(spec.GuardID))
	path := fmt.Sprintf("%s/%x.json", directory, name[:])
	if _, err := root.CreateRegularExact(path, raw, 0o600, 0o700); err != nil {
		return "", "", err
	}
	if err := root.SyncDir(directory); err != nil {
		return "", "", err
	}
	sum := sha256.Sum256(raw)
	return fmt.Sprintf("%x", sum[:]), string(raw), nil
}

// ReadQuiescenceEvidence reads the exact capability-bound record selected by a
// durable guard ID. Startup uses this only after selecting a pending guard from
// SQLite; filenames never route authority.
func ReadQuiescenceEvidence(runtimeRoot, guardID string) (Result, string, string, error) {
	if runtimeRoot == "" || guardID == "" {
		return Result{}, "", "", fmt.Errorf("runtime root and guard id are required")
	}
	nameSum := sha256.Sum256([]byte(guardID))
	root, err := securefs.OpenRoot(runtimeRoot)
	if err != nil {
		return Result{}, "", "", err
	}
	defer root.Close()
	relativePath := fmt.Sprintf("provider-process-guards/quiescence/%x.json", nameSum[:])
	raw, _, err := root.ReadRegularIdentified(relativePath, maxWireBytes)
	if err != nil {
		return Result{}, "", "", err
	}
	if len(raw) == 0 || len(raw) > maxWireBytes {
		return Result{}, "", "", fmt.Errorf("invalid guardian quiescence record size")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var status wireStatus
	if err := decoder.Decode(&status); err != nil {
		return Result{}, "", "", err
	}
	if status.Type != "result" || status.Result.GuardID != guardID || !status.Result.Quiescent {
		return Result{}, "", "", fmt.Errorf("invalid guardian quiescence record")
	}
	sum := sha256.Sum256(raw)
	return status.Result, fmt.Sprintf("%x", sum[:]), string(raw), nil
}

// RemoveQuiescenceEvidence removes only the exact record whose bytes were
// accepted transactionally. A replacement by name is left for later recovery.
func RemoveQuiescenceEvidence(runtimeRoot, guardID, evidence string) error {
	if runtimeRoot == "" || guardID == "" || evidence == "" {
		return fmt.Errorf("runtime root, guard id, and evidence are required")
	}
	nameSum := sha256.Sum256([]byte(guardID))
	relativePath := fmt.Sprintf("provider-process-guards/quiescence/%x.json", nameSum[:])
	root, err := securefs.OpenRoot(runtimeRoot)
	if err != nil {
		return err
	}
	defer root.Close()
	raw, identity, err := root.ReadRegularIdentified(relativePath, maxWireBytes)
	if err != nil {
		return err
	}
	if !bytes.Equal(raw, []byte(evidence)) {
		return fmt.Errorf("guardian quiescence record changed before cleanup")
	}
	return root.RemoveRegularIfIdentity(relativePath, identity)
}
