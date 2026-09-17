package github

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"kingdom/internal/securefs"
)

type KeyStore struct {
	root      string
	anchor    string
	rootChain []securefs.Directory
}

const maxPrivateKeyBytes = 64 * 1024

var safeConnectionID = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

func NewKeyStore(secretsRoot string) (*KeyStore, error) {
	root := filepath.Clean(strings.TrimSpace(secretsRoot))
	if root == "." || root == "" {
		return nil, fmt.Errorf("secrets root is required")
	}
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("resolve secrets root: %w", err)
	}
	parent := filepath.Dir(absRoot)
	store := &KeyStore{root: absRoot, anchor: parent, rootChain: []securefs.Directory{{Name: filepath.Base(absRoot), Mode: 0o700, ExactMode: true}}}
	if filepath.Base(absRoot) == "secrets" && filepath.Base(parent) == ".kingdom" {
		store.anchor = filepath.Dir(parent)
		store.rootChain = []securefs.Directory{{Name: ".kingdom", Mode: 0o755}, {Name: "secrets", Mode: 0o700, ExactMode: true}}
	}
	return store, nil
}
func CredentialHandle(connectionID string) string {
	return "github-app:" + strings.TrimSpace(connectionID)
}
func (s *KeyStore) path(connectionID string, generation int) string {
	return filepath.Join(s.root, "github-apps", strings.TrimSpace(connectionID), privateKeyName(generation))
}

func (s *KeyStore) Provision(connectionID string, generation int, raw []byte) (string, error) {
	var fingerprint string
	err := s.WithConnectionLock(connectionID, func() error {
		var installErr error
		fingerprint, installErr = s.install(connectionID, generation, raw, false)
		return installErr
	})
	return fingerprint, err
}

func (s *KeyStore) Rotate(connectionID string, generation int, raw []byte) (string, error) {
	var fingerprint string
	err := s.WithConnectionLock(connectionID, func() error {
		var installErr error
		fingerprint, installErr = s.install(connectionID, generation, raw, true)
		return installErr
	})
	return fingerprint, err
}

// InstallAudited makes a new generation durable before publishing it through
// the database callback. It never overwrites the previously selected key.
func (s *KeyStore) InstallAudited(connectionID string, generation int, raw []byte, rotate bool, audit func() error) (string, error) {
	var fingerprint string
	err := s.WithConnectionLock(connectionID, func() error {
		var err error
		fingerprint, err = s.install(connectionID, generation, raw, rotate)
		if err != nil {
			return err
		}
		if err = audit(); err == nil {
			return nil
		}
		// Commit errors can be outcome-ambiguous. Keep the staged generation;
		// startup reconciliation will retain it if the DB committed or remove it
		// if the prior generation is still selected.
		return err
	})
	return fingerprint, err
}

// WithConnectionLock provides a cross-process fence shared by CLI key changes,
// API removal, and startup reconciliation.
func (s *KeyStore) WithConnectionLock(connectionID string, fn func() error) error {
	if err := validateConnectionID(connectionID); err != nil {
		return err
	}
	dir, err := s.openConnection(connectionID, true)
	if err != nil {
		return err
	}
	defer dir.Close()
	lock, err := dir.OpenFile(".lifecycle.lock", os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	createdLock := err == nil
	if errors.Is(err, fs.ErrExist) {
		lock, err = securefs.OpenRegularFile(dir, ".lifecycle.lock", os.O_RDWR, 0)
	}
	if err != nil {
		return err
	}
	defer lock.Close()
	if createdLock {
		if err := securefs.ProtectCurrentUserOnlyPath(filepath.Join(s.root, "github-apps", connectionID, ".lifecycle.lock")); err != nil {
			_ = dir.Remove(".lifecycle.lock")
			return fmt.Errorf("protect github lifecycle lock: %w", err)
		}
	}
	info, err := lock.Stat()
	if err != nil || !info.Mode().IsRegular() || (securefs.EnforcesPOSIXMode() && info.Mode().Perm() != 0o600) {
		return fmt.Errorf("github lifecycle lock must be a 0600 regular file")
	}
	if err = securefs.ValidateCurrentUserOnly(lock); err != nil {
		return err
	}
	if err = lockFile(lock); err != nil {
		return err
	}
	defer unlockFile(lock) //nolint:errcheck -- best-effort unlock during close
	return fn()
}

func (s *KeyStore) install(connectionID string, generation int, raw []byte, rotate bool) (string, error) {
	if err := validateConnectionID(connectionID); err != nil {
		return "", err
	}
	if generation <= 0 || rotate != (generation > 1) {
		return "", fmt.Errorf("invalid github private key generation")
	}
	key, err := parseRSAKey(raw)
	if err != nil {
		return "", err
	}
	dir, err := s.openConnection(connectionID, true)
	if err != nil {
		return "", err
	}
	defer dir.Close()
	destination := privateKeyName(generation)
	_, statErr := dir.Lstat(destination)
	if statErr == nil {
		return "", fmt.Errorf("GitHub private key generation is already staged")
	}
	if statErr != nil && !errors.Is(statErr, fs.ErrNotExist) {
		return "", statErr
	}
	if rotate {
		if _, _, err := s.Load(connectionID, generation-1); err != nil {
			return "", err
		}
	}
	tmpName, err := randomTempName()
	if err != nil {
		return "", err
	}
	tmp, err := dir.OpenFile(tmpName, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return "", err
	}
	if err := securefs.ProtectCurrentUserOnlyPath(filepath.Join(s.root, "github-apps", connectionID, tmpName)); err != nil {
		_ = tmp.Close()
		_ = dir.Remove(tmpName)
		return "", fmt.Errorf("protect staged github private key: %w", err)
	}
	defer dir.Remove(tmpName) //nolint:errcheck -- cleanup after install or failure
	_, err = tmp.Write(raw)
	if err == nil {
		err = tmp.Sync()
	}
	closeErr := tmp.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return "", fmt.Errorf("stage github private key: %w", err)
	}
	err = dir.Link(tmpName, destination)
	if err != nil {
		return "", fmt.Errorf("install github private key: %w", err)
	}
	if err = dir.Remove(tmpName); err != nil {
		_ = dir.Remove(destination)
		return "", fmt.Errorf("install github private key: %w", err)
	}
	d, err := dir.Open(".")
	if err == nil {
		err = d.Sync()
		_ = d.Close()
	}
	if err != nil {
		return "", err
	}
	der := x509.MarshalPKCS1PublicKey(&key.PublicKey)
	sum := sha256.Sum256(der)
	return hex.EncodeToString(sum[:]), nil
}

func (s *KeyStore) Load(connectionID string, generation int) (*rsa.PrivateKey, string, error) {
	if err := validateConnectionID(connectionID); err != nil {
		return nil, "", err
	}
	if generation <= 0 {
		return nil, "", fmt.Errorf("invalid github private key generation")
	}
	dir, err := s.openConnection(connectionID, false)
	if err != nil {
		return nil, "", err
	}
	defer dir.Close()
	file, err := securefs.OpenRegularFile(dir, privateKeyName(generation), os.O_RDONLY, 0)
	if err != nil {
		return nil, "", err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, "", err
	}
	if err = validatePrivateKeyInfo(info); err != nil {
		return nil, "", err
	}
	if err = securefs.ValidateCurrentUserOnly(file); err != nil {
		return nil, "", err
	}
	if err = validateFileSingleLink(file, "github private key"); err != nil {
		return nil, "", err
	}
	raw, err := io.ReadAll(io.LimitReader(file, maxPrivateKeyBytes+1))
	if err != nil {
		return nil, "", err
	}
	key, err := parseRSAKey(raw)
	if err != nil {
		return nil, "", err
	}
	der := x509.MarshalPKCS1PublicKey(&key.PublicKey)
	sum := sha256.Sum256(der)
	return key, hex.EncodeToString(sum[:]), nil
}
func (s *KeyStore) RemoveAll(connectionID string) error {
	return s.WithConnectionLock(connectionID, func() error { return s.RemoveAllWhileLocked(connectionID) })
}

func (s *KeyStore) RemoveAllWhileLocked(connectionID string) error {
	if err := validateConnectionID(connectionID); err != nil {
		return err
	}
	dir, err := s.openConnection(connectionID, false)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return err
	}
	defer dir.Close()
	return removeKeyGenerations(dir, 0)
}

// ReconcileGenerations removes temporary, stale, and unowned key generations.
// selectedGeneration runs while the connection lifecycle lock is held so it
// can read the SQLite publication pointer fenced by the same lock as writers.
// It must not attempt to acquire a credential lifecycle lock.
func (s *KeyStore) ReconcileGenerations(selectedGeneration func(connectionID string) (int, error)) error {
	secrets, err := s.openSecrets()
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer secrets.Close()
	apps, err := securefs.OpenDir(secrets, securefs.Directory{Name: "github-apps", Mode: 0o700, ExactMode: true})
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer apps.Close()
	rootFile, err := apps.Open(".")
	if err != nil {
		return err
	}
	entries, err := rootFile.ReadDir(-1)
	_ = rootFile.Close()
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !entry.IsDir() || !safeConnectionID.MatchString(entry.Name()) {
			continue
		}
		if err := s.WithConnectionLock(entry.Name(), func() error {
			selected, resolveErr := selectedGeneration(entry.Name())
			if resolveErr != nil {
				return resolveErr
			}
			dir, openErr := s.openConnection(entry.Name(), false)
			if openErr != nil {
				return openErr
			}
			defer dir.Close()
			return removeKeyGenerations(dir, selected)
		}); err != nil {
			return err
		}
	}
	return nil
}

func (s *KeyStore) openSecrets() (*os.Root, error) {
	return securefs.OpenDirChain(s.anchor, s.rootChain...)
}

func (s *KeyStore) openOrCreateSecrets() (*os.Root, error) {
	chain := append([]securefs.Directory(nil), s.rootChain...)
	chain[len(chain)-1].Create = true
	return securefs.OpenDirChain(s.anchor, chain...)
}

func (s *KeyStore) openConnection(connectionID string, create bool) (*os.Root, error) {
	if err := validateConnectionID(connectionID); err != nil {
		return nil, err
	}
	var secrets *os.Root
	var err error
	if create {
		secrets, err = s.openOrCreateSecrets()
	} else {
		secrets, err = s.openSecrets()
	}
	if err != nil {
		return nil, err
	}
	apps, err := securefs.OpenDir(secrets, securefs.Directory{Name: "github-apps", Mode: 0o700, Create: create, ExactMode: true})
	_ = secrets.Close()
	if err != nil {
		return nil, err
	}
	connection, err := securefs.OpenDir(apps, securefs.Directory{Name: strings.TrimSpace(connectionID), Mode: 0o700, Create: create, ExactMode: true})
	_ = apps.Close()
	return connection, err
}

func randomTempName() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return ".private-key-" + hex.EncodeToString(raw[:]) + ".tmp", nil
}

func privateKeyName(generation int) string {
	return "private-key." + strconv.Itoa(generation) + ".pem"
}

func privateKeyGeneration(name string) (int, bool) {
	if !strings.HasPrefix(name, "private-key.") || !strings.HasSuffix(name, ".pem") {
		return 0, false
	}
	raw := strings.TrimSuffix(strings.TrimPrefix(name, "private-key."), ".pem")
	generation, err := strconv.Atoi(raw)
	return generation, err == nil && generation > 0 && strconv.Itoa(generation) == raw
}

func removeKeyGenerations(dir *os.Root, selected int) error {
	file, err := dir.Open(".")
	if err != nil {
		return err
	}
	entries, err := file.ReadDir(-1)
	_ = file.Close()
	if err != nil {
		return err
	}
	changed := false
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasPrefix(name, ".private-key-") || !strings.HasSuffix(name, ".tmp") {
			continue
		}
		file, openErr := securefs.OpenRegularFile(dir, name, os.O_RDONLY, 0)
		if openErr != nil {
			return openErr
		}
		validateErr := validateReconcileTemp(file, name)
		_ = file.Close()
		if validateErr != nil {
			return validateErr
		}
		if err := dir.Remove(name); err != nil {
			return err
		}
		changed = true
	}
	for _, entry := range entries {
		name := entry.Name()
		generation, generationFile := privateKeyGeneration(name)
		if !generationFile || generation == selected {
			continue
		}
		opened, openErr := securefs.OpenRegularFile(dir, name, os.O_RDONLY, 0)
		if openErr != nil {
			return openErr
		}
		validateErr := validatePrivateKeyInfoFromFile(opened)
		_ = opened.Close()
		if validateErr != nil {
			return validateErr
		}
		if err := dir.Remove(name); err != nil {
			return err
		}
		changed = true
	}
	if !changed {
		return nil
	}
	d, err := dir.Open(".")
	if err == nil {
		err = d.Sync()
		_ = d.Close()
	}
	return err
}

func validateReconcileTemp(file *os.File, name string) error {
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || (securefs.EnforcesPOSIXMode() && info.Mode().Perm() != 0o600) {
		return fmt.Errorf("%s must be a 0600 regular file", name)
	}
	if err := securefs.ValidateCurrentUserOnly(file); err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	return nil
}

func validatePrivateKeyInfoFromFile(file *os.File) error {
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if err := validatePrivateKeyInfo(info); err != nil {
		return err
	}
	if err := securefs.ValidateCurrentUserOnly(file); err != nil {
		return err
	}
	return validateFileSingleLink(file, "github private key")
}

func parseRSAKey(raw []byte) (*rsa.PrivateKey, error) {
	if len(raw) == 0 || len(raw) > maxPrivateKeyBytes {
		return nil, fmt.Errorf("private key size is invalid")
	}
	block, rest := pem.Decode(raw)
	if block == nil {
		return nil, fmt.Errorf("private key must be PEM encoded")
	}
	if len(block.Headers) != 0 {
		return nil, fmt.Errorf("private key PEM headers are not supported")
	}
	if len(strings.TrimSpace(string(rest))) != 0 {
		return nil, fmt.Errorf("private key PEM contains trailing data")
	}
	switch block.Type {
	case "RSA PRIVATE KEY":
		key, err := x509.ParsePKCS1PrivateKey(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("private key must be RSA PKCS#1")
		}
		return validateRSAKey(key)
	case "PRIVATE KEY":
		parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("private key must be RSA PKCS#8")
		}
		key, ok := parsed.(*rsa.PrivateKey)
		if !ok {
			return nil, fmt.Errorf("private key must be RSA")
		}
		return validateRSAKey(key)
	default:
		return nil, fmt.Errorf("private key PEM block type is not supported")
	}
}

func validateRSAKey(key *rsa.PrivateKey) (*rsa.PrivateKey, error) {
	if key == nil || key.N.BitLen() < 2048 {
		return nil, fmt.Errorf("RSA private key must be at least 2048 bits")
	}
	if err := key.Validate(); err != nil {
		return nil, fmt.Errorf("invalid RSA private key")
	}
	return key, nil
}

func validateConnectionID(connectionID string) error {
	if connectionID != strings.TrimSpace(connectionID) || !safeConnectionID.MatchString(connectionID) {
		return fmt.Errorf("invalid connection id")
	}
	return nil
}

func validatePrivateKeyInfo(info fs.FileInfo) error {
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("github private key must be a regular file")
	}
	if securefs.EnforcesPOSIXMode() && info.Mode().Perm() != 0o600 {
		return fmt.Errorf("github private key permissions must be 0600")
	}
	return nil
}
