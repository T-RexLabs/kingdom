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
	"runtime"
	"strings"
	"syscall"

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
func (s *KeyStore) path(connectionID string) string {
	return filepath.Join(s.root, "github-apps", strings.TrimSpace(connectionID), "private-key.pem")
}

func (s *KeyStore) Provision(connectionID string, raw []byte) (string, error) {
	return s.install(connectionID, raw, false)
}

func (s *KeyStore) Rotate(connectionID string, raw []byte) (string, error) {
	return s.install(connectionID, raw, true)
}

// InstallAudited serializes local key changes with connection removal and
// restores the prior filesystem state when durable lifecycle recording fails.
func (s *KeyStore) InstallAudited(connectionID string, raw []byte, rotate bool, audit func() error) (string, error) {
	var fingerprint string
	err := s.WithConnectionLock(connectionID, func() error {
		var previous []byte
		if rotate {
			var err error
			previous, err = s.readRaw(connectionID)
			if err != nil {
				return err
			}
		}
		var err error
		fingerprint, err = s.install(connectionID, raw, rotate)
		if err != nil {
			return err
		}
		if err = audit(); err == nil {
			return nil
		}
		var rollbackErr error
		if rotate {
			_, rollbackErr = s.install(connectionID, previous, true)
		} else {
			rollbackErr = s.Remove(connectionID)
		}
		if rollbackErr != nil {
			return errors.Join(err, fmt.Errorf("rollback github private key: %w", rollbackErr))
		}
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
	if errors.Is(err, fs.ErrExist) {
		lock, err = securefs.OpenRegularFile(dir, ".lifecycle.lock", os.O_RDWR, 0)
	}
	if err != nil {
		return err
	}
	defer lock.Close()
	info, err := lock.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
		return fmt.Errorf("github lifecycle lock must be a 0600 regular file")
	}
	if err = validateOwner(info); err != nil {
		return err
	}
	if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); err != nil {
		return err
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN) //nolint:errcheck -- best-effort unlock during close
	return fn()
}

func (s *KeyStore) install(connectionID string, raw []byte, rotate bool) (string, error) {
	if err := validateConnectionID(connectionID); err != nil {
		return "", err
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
	destination := "private-key.pem"
	_, statErr := dir.Lstat(destination)
	if rotate && errors.Is(statErr, fs.ErrNotExist) {
		return "", fmt.Errorf("GitHub private key is not provisioned")
	}
	if !rotate && statErr == nil {
		return "", fmt.Errorf("GitHub private key is already provisioned; use rotate")
	}
	if statErr != nil && !errors.Is(statErr, fs.ErrNotExist) {
		return "", statErr
	}
	if rotate && statErr == nil {
		if _, _, err := s.Load(connectionID); err != nil {
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
	if rotate {
		err = dir.Rename(tmpName, destination)
	} else {
		err = dir.Link(tmpName, destination)
	}
	if err != nil {
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

func (s *KeyStore) Load(connectionID string) (*rsa.PrivateKey, string, error) {
	if err := validateConnectionID(connectionID); err != nil {
		return nil, "", err
	}
	dir, err := s.openConnection(connectionID, false)
	if err != nil {
		return nil, "", err
	}
	defer dir.Close()
	file, err := securefs.OpenRegularFile(dir, "private-key.pem", os.O_RDONLY, 0)
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
func (s *KeyStore) Remove(connectionID string) error {
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
	info, err := dir.Lstat("private-key.pem")
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if err = validatePrivateKeyInfo(info); err != nil {
		return err
	}
	return dir.Remove("private-key.pem")
}
func (s *KeyStore) CleanupTemps() error {
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
		connection, err := securefs.OpenDir(apps, securefs.Directory{Name: entry.Name(), Mode: 0o700, ExactMode: true})
		if err != nil {
			return err
		}
		file, err := connection.Open(".")
		if err != nil {
			_ = connection.Close()
			return err
		}
		children, readErr := file.ReadDir(-1)
		_ = file.Close()
		if readErr != nil {
			_ = connection.Close()
			return readErr
		}
		for _, child := range children {
			if child.Type().IsRegular() && strings.HasPrefix(child.Name(), ".private-key-") && strings.HasSuffix(child.Name(), ".tmp") {
				if err := connection.Remove(child.Name()); err != nil {
					_ = connection.Close()
					return err
				}
			}
		}
		_ = connection.Close()
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

func (s *KeyStore) readRaw(connectionID string) ([]byte, error) {
	dir, err := s.openConnection(connectionID, false)
	if err != nil {
		return nil, err
	}
	defer dir.Close()
	file, err := securefs.OpenRegularFile(dir, "private-key.pem", os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if err = validatePrivateKeyInfo(info); err != nil {
		return nil, err
	}
	return io.ReadAll(io.LimitReader(file, maxPrivateKeyBytes+1))
}

func randomTempName() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return ".private-key-" + hex.EncodeToString(raw[:]) + ".tmp", nil
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

func validateOwner(info fs.FileInfo) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Errorf("cannot verify github key ownership")
	}
	if int(stat.Uid) != os.Geteuid() {
		return fmt.Errorf("github key path must be owned by current user")
	}
	return nil
}

func validatePrivateKeyInfo(info fs.FileInfo) error {
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("github private key must be a regular file")
	}
	if info.Mode().Perm() != 0600 {
		return fmt.Errorf("github private key permissions must be 0600")
	}
	if err := validateOwner(info); err != nil {
		return err
	}
	if runtime.GOOS != "windows" {
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok || stat.Nlink != 1 {
			return fmt.Errorf("github private key must have exactly one link")
		}
	}
	return nil
}
