package github

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	"kingdom/internal/securefs"
)

const maxUserTokenFileBytes = 8 * 1024

type UserTokens struct {
	AccessToken      string    `json:"access_token"`
	RefreshToken     string    `json:"refresh_token"`
	AccessExpiresAt  time.Time `json:"access_expires_at"`
	RefreshExpiresAt time.Time `json:"refresh_expires_at"`
}

func (t UserTokens) Validate(now time.Time) error {
	if err := t.validateStructure(); err != nil {
		return err
	}
	if !t.AccessExpiresAt.After(now) {
		return fmt.Errorf("github user token expiries are invalid")
	}
	return nil
}

func (t UserTokens) validateStructure() error {
	if !validUserToken(t.AccessToken) || !validUserToken(t.RefreshToken) {
		return fmt.Errorf("github user token response is invalid")
	}
	if t.AccessExpiresAt.IsZero() || !t.RefreshExpiresAt.After(t.AccessExpiresAt) {
		return fmt.Errorf("github user token expiries are invalid")
	}
	return nil
}

func validUserToken(value string) bool {
	if len(value) < 8 || len(value) > 1024 {
		return false
	}
	for i := range len(value) {
		if value[i] <= 0x20 || value[i] >= 0x7f {
			return false
		}
	}
	return true
}

type UserTokenStore struct {
	anchor    string
	rootChain []securefs.Directory
}

func NewUserTokenStore(secretsRoot string) (*UserTokenStore, error) {
	root := filepath.Clean(strings.TrimSpace(secretsRoot))
	if root == "." || root == "" {
		return nil, fmt.Errorf("secrets root is required")
	}
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("resolve secrets root: %w", err)
	}
	parent := filepath.Dir(absRoot)
	store := &UserTokenStore{anchor: parent, rootChain: []securefs.Directory{{Name: filepath.Base(absRoot), Mode: 0o700, ExactMode: true}}}
	if filepath.Base(absRoot) == "secrets" && filepath.Base(parent) == ".kingdom" {
		store.anchor = filepath.Dir(parent)
		store.rootChain = []securefs.Directory{{Name: ".kingdom", Mode: 0o755}, {Name: "secrets", Mode: 0o700, ExactMode: true}}
	}
	return store, nil
}

func UserCredentialHandle(connectionID string) string {
	return "github-user:" + strings.TrimSpace(connectionID)
}

func (s *UserTokenStore) Load(connectionID string) (UserTokens, error) {
	raw, err := s.loadRaw(connectionID)
	if err != nil {
		return UserTokens{}, err
	}
	return decodeUserTokens(raw)
}

func (s *UserTokenStore) loadRaw(connectionID string) ([]byte, error) {
	dir, err := s.openConnection(connectionID, false)
	if err != nil {
		return nil, err
	}
	defer dir.Close()
	file, err := securefs.OpenRegularFile(dir, "tokens.json", os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	if err := validateUserTokenFile(file); err != nil {
		return nil, err
	}
	raw, err := io.ReadAll(io.LimitReader(file, maxUserTokenFileBytes+1))
	if err != nil || len(raw) > maxUserTokenFileBytes {
		return nil, fmt.Errorf("read github user tokens")
	}
	if _, err := decodeUserTokens(raw); err != nil {
		return nil, err
	}
	return raw, nil
}

func decodeUserTokens(raw []byte) (UserTokens, error) {
	var tokens UserTokens
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&tokens); err != nil {
		return UserTokens{}, fmt.Errorf("decode github user tokens")
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return UserTokens{}, fmt.Errorf("decode github user tokens")
	}
	if err := tokens.validateStructure(); err != nil {
		return UserTokens{}, err
	}
	return tokens, nil
}

func (s *UserTokenStore) Store(connectionID string, tokens UserTokens) error {
	return s.WithConnectionLock(connectionID, func() error { return s.store(connectionID, tokens) })
}

func (s *UserTokenStore) StoreWhileLocked(connectionID string, tokens UserTokens) error {
	return s.store(connectionID, tokens)
}

// InstallAudited restores the previous token file if the durable connection
// transaction fails. The callback must not retain token material.
func (s *UserTokenStore) InstallAudited(connectionID string, tokens UserTokens, audit func() error) error {
	return s.WithConnectionLock(connectionID, func() error {
		previous, loadErr := s.loadRaw(connectionID)
		hadPrevious := loadErr == nil
		if loadErr != nil && !errors.Is(loadErr, fs.ErrNotExist) {
			return loadErr
		}
		if err := s.store(connectionID, tokens); err != nil {
			return err
		}
		if err := audit(); err != nil {
			var rollbackErr error
			if hadPrevious {
				rollbackErr = s.storeRaw(connectionID, previous)
			} else {
				rollbackErr = s.removeUnlocked(connectionID)
			}
			if rollbackErr != nil {
				return errors.Join(err, fmt.Errorf("rollback github user tokens: %w", rollbackErr))
			}
			return err
		}
		return nil
	})
}

func (s *UserTokenStore) WithConnectionLock(connectionID string, fn func() error) error {
	if err := validateConnectionID(connectionID); err != nil {
		return err
	}
	dir, err := s.openConnection(connectionID, true)
	if err != nil {
		return err
	}
	defer dir.Close()
	if info, statErr := dir.Lstat("tokens.json"); statErr == nil {
		file, openErr := securefs.OpenRegularFile(dir, "tokens.json", os.O_RDONLY, 0)
		if openErr != nil {
			return openErr
		}
		validateErr := validateProtectedRegularFile(file, "tokens.json")
		_ = file.Close()
		if validateErr != nil || !info.Mode().IsRegular() {
			return errors.Join(validateErr, fmt.Errorf("existing github user token path is unsafe"))
		}
	} else if !errors.Is(statErr, fs.ErrNotExist) {
		return statErr
	}
	lock, err := dir.OpenFile(".lifecycle.lock", os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if errors.Is(err, fs.ErrExist) {
		lock, err = securefs.OpenRegularFile(dir, ".lifecycle.lock", os.O_RDWR, 0)
	}
	if err != nil {
		return err
	}
	defer lock.Close()
	if err := validateProtectedRegularFile(lock, ".lifecycle.lock"); err != nil {
		return err
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); err != nil {
		return err
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN) //nolint:errcheck
	return fn()
}

func (s *UserTokenStore) Remove(connectionID string) error {
	return s.WithConnectionLock(connectionID, func() error { return s.removeUnlocked(connectionID) })
}

// RemoveWhileLocked is for lifecycle transactions already executing inside
// WithConnectionLock.
func (s *UserTokenStore) RemoveWhileLocked(connectionID string) error {
	return s.removeUnlocked(connectionID)
}

func (s *UserTokenStore) removeUnlocked(connectionID string) error {
	dir, err := s.openConnection(connectionID, false)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer dir.Close()
	file, err := securefs.OpenRegularFile(dir, "tokens.json", os.O_RDONLY, 0)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if err := validateProtectedRegularFile(file, "tokens.json"); err != nil {
		_ = file.Close()
		return err
	}
	_ = file.Close()
	if err := dir.Remove("tokens.json"); err != nil {
		return err
	}
	d, err := dir.Open(".")
	if err == nil {
		err = d.Sync()
		_ = d.Close()
	}
	return err
}

func (s *UserTokenStore) store(connectionID string, tokens UserTokens) error {
	if err := validateConnectionID(connectionID); err != nil {
		return err
	}
	if err := tokens.Validate(time.Now().Add(-time.Minute)); err != nil {
		return err
	}
	raw, err := json.Marshal(tokens)
	if err != nil || len(raw) > maxUserTokenFileBytes {
		return fmt.Errorf("encode github user tokens")
	}
	return s.storeRaw(connectionID, raw)
}

func (s *UserTokenStore) storeRaw(connectionID string, raw []byte) error {
	if len(raw) > maxUserTokenFileBytes {
		return fmt.Errorf("encode github user tokens")
	}
	if _, err := decodeUserTokens(raw); err != nil {
		return err
	}
	dir, err := s.openConnection(connectionID, true)
	if err != nil {
		return err
	}
	defer dir.Close()
	if info, statErr := dir.Lstat("tokens.json"); statErr == nil {
		file, openErr := securefs.OpenRegularFile(dir, "tokens.json", os.O_RDONLY, 0)
		if openErr != nil {
			return openErr
		}
		validateErr := validateProtectedRegularFile(file, "tokens.json")
		_ = file.Close()
		if validateErr != nil || !info.Mode().IsRegular() {
			return errors.Join(validateErr, fmt.Errorf("existing github user token path is unsafe"))
		}
	} else if !errors.Is(statErr, fs.ErrNotExist) {
		return statErr
	}
	tmpName, err := userTokenTempName()
	if err != nil {
		return err
	}
	tmp, err := dir.OpenFile(tmpName, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	defer dir.Remove(tmpName) //nolint:errcheck
	if _, err = tmp.Write(raw); err == nil {
		err = tmp.Sync()
	}
	if closeErr := tmp.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return fmt.Errorf("stage github user tokens: %w", err)
	}
	if err := dir.Rename(tmpName, "tokens.json"); err != nil {
		return fmt.Errorf("install github user tokens: %w", err)
	}
	d, err := dir.Open(".")
	if err == nil {
		err = d.Sync()
		_ = d.Close()
	}
	return err
}

func (s *UserTokenStore) CleanupTemps() error {
	secrets, err := securefs.OpenDirChain(s.anchor, s.rootChain...)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer secrets.Close()
	users, err := securefs.OpenDir(secrets, securefs.Directory{Name: "github-user-tokens", Mode: 0o700, ExactMode: true})
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer users.Close()
	root, err := users.Open(".")
	if err != nil {
		return err
	}
	entries, err := root.ReadDir(-1)
	_ = root.Close()
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !entry.IsDir() || !safeConnectionID.MatchString(entry.Name()) {
			continue
		}
		dir, err := securefs.OpenDir(users, securefs.Directory{Name: entry.Name(), Mode: 0o700, ExactMode: true})
		if err != nil {
			return err
		}
		file, err := dir.Open(".")
		if err != nil {
			_ = dir.Close()
			return err
		}
		children, err := file.ReadDir(-1)
		_ = file.Close()
		if err != nil {
			_ = dir.Close()
			return err
		}
		for _, child := range children {
			if child.Type().IsRegular() && strings.HasPrefix(child.Name(), ".tokens-") && strings.HasSuffix(child.Name(), ".tmp") {
				if err := dir.Remove(child.Name()); err != nil {
					_ = dir.Close()
					return err
				}
			}
		}
		_ = dir.Close()
	}
	return nil
}

func (s *UserTokenStore) openConnection(connectionID string, create bool) (*os.Root, error) {
	if err := validateConnectionID(connectionID); err != nil {
		return nil, err
	}
	chain := append([]securefs.Directory(nil), s.rootChain...)
	if create {
		chain[len(chain)-1].Create = true
	}
	secrets, err := securefs.OpenDirChain(s.anchor, chain...)
	if err != nil {
		return nil, err
	}
	users, err := securefs.OpenDir(secrets, securefs.Directory{Name: "github-user-tokens", Mode: 0o700, Create: create, ExactMode: true})
	_ = secrets.Close()
	if err != nil {
		return nil, err
	}
	dir, err := securefs.OpenDir(users, securefs.Directory{Name: connectionID, Mode: 0o700, Create: create, ExactMode: true})
	_ = users.Close()
	return dir, err
}

func validateUserTokenFile(file *os.File) error {
	return validateProtectedRegularFile(file, "tokens.json")
}

func validateProtectedRegularFile(file *os.File, name string) error {
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
		return fmt.Errorf("%s must be a 0600 regular file", name)
	}
	if err := securefs.CurrentUserOwns(info); err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	if runtime.GOOS != "windows" {
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok || stat.Nlink != 1 {
			return fmt.Errorf("%s must have exactly one link", name)
		}
	}
	return nil
}

func userTokenTempName() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return ".tokens-" + hex.EncodeToString(raw[:]) + ".tmp", nil
}
