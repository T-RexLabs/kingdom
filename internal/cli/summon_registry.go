package cli

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"kingdom/internal/processguardian"
)

// The registry answers "which summons are running on this machine, and where".
// It is deliberately global rather than per-project, so it lives beside the
// user's other caches instead of inside any one `.kingdom` tree.
//
// It is a cache, not a source of truth. Every entry is validated against a live
// PID on the way out, and the authoritative per-project answer is the PID file
// under `$KINGDOM_HOME/run/`. That is what licenses the recovery behaviour
// below: a registry Kingdom cannot read is rebuilt rather than treated as a
// fatal condition, because nothing is lost that the running processes do not
// already know.
const (
	summonRegistryLockFileName = "summon_registry.lock"
)

// summonRegistryPathFunc is the one seam through which the registry's location
// is resolved. Tests redirect it package-wide in TestMain so that no test can
// reach the operator's real cache directory by forgetting to.
var summonRegistryPathFunc = defaultSummonRegistryPath

type summonInstanceRecord struct {
	PID                  int    `json:"pid"`
	ProcessIdentityToken string `json:"process_identity_token"`
	StopControlToken     string `json:"stop_control_token"`
	ConfigPath           string `json:"config_path"`
	KingdomHome          string `json:"kingdom_home"`
	Listen               string `json:"listen"`
	BasePath             string `json:"base_path"`
	StartedAt            string `json:"started_at"`
}

func summonProcessIdentityMatches(pid int, expected string) (bool, error) {
	if pid <= 0 || strings.TrimSpace(expected) == "" {
		return false, nil
	}
	matches, err := processguardian.ProcessIdentityMatches(pid, expected)
	if err != nil {
		return false, fmt.Errorf("inspect summon process identity: %w", err)
	}
	return matches, nil
}

type summonInstanceRegistry struct {
	Instances []summonInstanceRecord `json:"instances"`
}

func defaultSummonRegistryPath() (string, error) {
	cacheDir, err := os.UserCacheDir()
	if err != nil {
		return "", fmt.Errorf("resolve user cache dir: %w", err)
	}
	return filepath.Join(cacheDir, "kingdom", defaultSummonRegistryFileName), nil
}

// listActiveSummonInstances reports the summons that are still running.
//
// Reading never writes. Dead entries are dropped from the answer and left on
// disk for the next mutation to clear, so `kd summon status` and CLI address
// discovery cannot contend with each other — or with a starting summon — over a
// file they only wanted to look at.
func listActiveSummonInstances() ([]summonInstanceRecord, error) {
	path, err := summonRegistryPathFunc()
	if err != nil {
		return nil, err
	}
	stored, err := readSummonRegistryFile(path)
	if err != nil {
		return nil, err
	}
	return liveSummonInstances(stored)
}

func findSummonInstanceForConfig(configPath, kingdomHome string) (*summonInstanceRecord, bool, error) {
	items, err := listActiveSummonInstances()
	if err != nil {
		return nil, false, err
	}
	trimmedConfig := strings.TrimSpace(configPath)
	trimmedHome := strings.TrimSpace(kingdomHome)
	for _, item := range items {
		if trimmedConfig != "" && strings.TrimSpace(item.ConfigPath) == trimmedConfig {
			copyItem := item
			return &copyItem, true, nil
		}
		if trimmedHome != "" && strings.TrimSpace(item.KingdomHome) == trimmedHome {
			copyItem := item
			return &copyItem, true, nil
		}
	}
	return nil, false, nil
}

func findActiveInstallModeSummon() (summonInstanceRecord, bool) {
	configPath := filepath.Join(defaultInstallDirName, defaultConfigFileName)
	resolvedConfigPath, err := filepath.Abs(configPath)
	if err != nil {
		return summonInstanceRecord{}, false
	}
	installCfg, err := readInstallConfig(resolvedConfigPath)
	if err != nil {
		return summonInstanceRecord{}, false
	}
	record, found, err := findSummonInstanceForConfig(resolvedConfigPath, installCfg.KingdomHome)
	if err != nil || !found || record == nil {
		return summonInstanceRecord{}, false
	}
	return *record, true
}

func registerSummonInstance(record summonInstanceRecord) error {
	if record.PID <= 0 {
		return fmt.Errorf("summon process id must be positive")
	}
	if strings.TrimSpace(record.ProcessIdentityToken) == "" {
		identity, err := processguardian.ProcessIdentityToken(record.PID)
		if err != nil {
			return fmt.Errorf("identify summon process: %w", err)
		}
		record.ProcessIdentityToken = identity
	}
	if strings.TrimSpace(record.StopControlToken) == "" {
		return fmt.Errorf("summon stop control token is required")
	}
	matches, err := summonProcessIdentityMatches(record.PID, record.ProcessIdentityToken)
	if err != nil {
		return fmt.Errorf("verify summon process identity: %w", err)
	}
	if !matches {
		return fmt.Errorf("summon process identity does not match pid %d", record.PID)
	}
	return updateSummonRegistry(func(live []summonInstanceRecord) []summonInstanceRecord {
		next := make([]summonInstanceRecord, 0, len(live)+1)
		replaced := false
		for _, existing := range live {
			if existing.PID == record.PID {
				next = append(next, record)
				replaced = true
				continue
			}
			next = append(next, existing)
		}
		if !replaced {
			next = append(next, record)
		}
		return next
	})
}

func unregisterSummonInstance(pid int, processIdentityToken, stopControlToken string) error {
	if pid <= 0 || strings.TrimSpace(processIdentityToken) == "" || strings.TrimSpace(stopControlToken) == "" {
		return nil
	}
	return updateSummonRegistry(func(live []summonInstanceRecord) []summonInstanceRecord {
		next := make([]summonInstanceRecord, 0, len(live))
		for _, item := range live {
			if item.PID == pid && item.ProcessIdentityToken == strings.TrimSpace(processIdentityToken) && item.StopControlToken == strings.TrimSpace(stopControlToken) {
				continue
			}
			next = append(next, item)
		}
		return next
	})
}

// updateSummonRegistry applies one change under an exclusive cross-process lock.
//
// The lock is what makes the change safe, not the atomic write below it. Every
// mutation is a read-modify-write, so two unsynchronized summons starting at
// once would each read the same prior state and the second would write the
// first out of existence. Pruning dead entries happens here, as part of a
// mutation that was going to write anyway.
func updateSummonRegistry(mutate func(live []summonInstanceRecord) []summonInstanceRecord) error {
	return withSummonRegistryLock(func(path string) error {
		stored, err := readSummonRegistryFile(path)
		if err != nil {
			return err
		}
		live, err := liveSummonInstances(stored)
		if err != nil {
			return err
		}
		return writeSummonRegistry(path, mutate(live))
	})
}

func withSummonRegistryLock(fn func(path string) error) error {
	path, err := summonRegistryPathFunc()
	if err != nil {
		return err
	}
	if strings.TrimSpace(path) == "" {
		return fmt.Errorf("summon registry path is required")
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create summon registry directory: %w", err)
	}
	// The lock file is a rendezvous point, never read or written. It is kept
	// beside the registry rather than being the registry itself so that the
	// atomic rename below cannot replace the file the lock is held on.
	lock, err := os.OpenFile(filepath.Join(dir, summonRegistryLockFileName), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return fmt.Errorf("open summon registry lock: %w", err)
	}
	defer lock.Close()
	if err := lockSummonFile(lock); err != nil {
		return fmt.Errorf("lock summon registry: %w", err)
	}
	defer unlockSummonFile(lock) //nolint:errcheck -- best-effort unlock before close
	return fn(path)
}

// readSummonRegistryFile returns the registry exactly as stored.
//
// Content that will not decode is reported as an empty registry rather than as
// an error. This file is derived state that any live summon can re-announce,
// and refusing to read it would take `kd summon`, `kd summon status`,
// `kd summon stop` and `kd clean` down together until someone deleted it by
// hand. The next mutation rewrites it. `readRunningPID` treats a damaged PID
// file the same way.
func readSummonRegistryFile(path string) ([]summonInstanceRecord, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read summon registry: %w", err)
	}
	registry := summonInstanceRegistry{}
	if err := json.Unmarshal(raw, &registry); err != nil {
		return nil, nil
	}
	return registry.Instances, nil
}

func liveSummonInstances(items []summonInstanceRecord) ([]summonInstanceRecord, error) {
	return liveSummonInstancesWithIdentity(items, summonProcessIdentityMatches)
}

func liveSummonInstancesWithIdentity(items []summonInstanceRecord, identityMatches func(int, string) (bool, error)) ([]summonInstanceRecord, error) {
	if identityMatches == nil {
		return nil, fmt.Errorf("summon process identity checker is required")
	}
	live := make([]summonInstanceRecord, 0, len(items))
	for _, item := range items {
		if strings.TrimSpace(item.StopControlToken) == "" {
			continue
		}
		matches, err := identityMatches(item.PID, item.ProcessIdentityToken)
		if err != nil {
			return nil, fmt.Errorf("inspect summon registry process pid=%d: %w", item.PID, err)
		}
		if !matches {
			continue
		}
		live = append(live, item)
	}
	return live, nil
}

// writeSummonRegistry replaces the registry atomically.
//
// Writing in place would truncate first: two processes doing that concurrently
// leave the shorter payload followed by the tail of the longer one, which is
// how a registry ends up holding two concatenated JSON documents. Renaming a
// completed temporary file over the destination means a reader sees either the
// old contents or the new ones, and never a blend — which is also why reads
// need no lock of their own.
func writeSummonRegistry(path string, instances []summonInstanceRecord) error {
	if len(instances) == 0 {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove empty summon registry: %w", err)
		}
		return nil
	}
	raw, err := json.MarshalIndent(summonInstanceRegistry{Instances: instances}, "", "  ")
	if err != nil {
		return fmt.Errorf("encode summon registry: %w", err)
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create summon registry directory: %w", err)
	}
	temp, err := os.CreateTemp(dir, ".summon_registry-*")
	if err != nil {
		return fmt.Errorf("create summon registry temp file: %w", err)
	}
	tempPath := temp.Name()
	if _, err := temp.Write(raw); err != nil {
		temp.Close()
		os.Remove(tempPath)
		return fmt.Errorf("write summon registry: %w", err)
	}
	if err := temp.Sync(); err != nil {
		temp.Close()
		os.Remove(tempPath)
		return fmt.Errorf("sync summon registry: %w", err)
	}
	if err := temp.Close(); err != nil {
		os.Remove(tempPath)
		return fmt.Errorf("close summon registry temp file: %w", err)
	}
	if err := os.Rename(tempPath, path); err != nil {
		os.Remove(tempPath)
		return fmt.Errorf("replace summon registry: %w", err)
	}
	return nil
}
