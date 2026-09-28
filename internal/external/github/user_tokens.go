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
	"strconv"
	"strings"
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
	root      string
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
	store := &UserTokenStore{root: absRoot, anchor: parent, rootChain: []securefs.Directory{{Name: filepath.Base(absRoot), Mode: 0o700, ExactMode: true}}}
	if filepath.Base(absRoot) == "secrets" && filepath.Base(parent) == ".kingdom" {
		store.anchor = filepath.Dir(parent)
		store.rootChain = []securefs.Directory{{Name: ".kingdom", Mode: 0o755}, {Name: "secrets", Mode: 0o700, ExactMode: true}}
	}
	return store, nil
}

func UserCredentialHandle(connectionID string) string {
	return "github-user:" + strings.TrimSpace(connectionID)
}

func (s *UserTokenStore) Load(connectionID string, generation int) (UserTokens, error) {
	raw, err := s.loadRaw(connectionID, generation)
	if err != nil {
		return UserTokens{}, err
	}
	return decodeUserTokens(raw)
}

func (s *UserTokenStore) loadRaw(connectionID string, generation int) ([]byte, error) {
	if generation <= 0 {
		return nil, fmt.Errorf("invalid github user token generation")
	}
	dir, err := s.openConnection(connectionID, false)
	if err != nil {
		return nil, err
	}
	defer dir.Close()
	name := userTokenName(generation)
	file, err := securefs.OpenRegularFile(dir, name, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	if err := validateProtectedRegularFile(file, name); err != nil {
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

func (s *UserTokenStore) Store(connectionID string, generation int, tokens UserTokens) error {
	return s.WithConnectionLock(connectionID, func() error { return s.store(connectionID, generation, tokens, true) })
}

func (s *UserTokenStore) StoreWhileLocked(connectionID string, generation int, tokens UserTokens) error {
	return s.store(connectionID, generation, tokens, true)
}

// InstallAudited makes a new generation durable before publishing it through
// the database callback. It never overwrites a selected token generation.
func (s *UserTokenStore) InstallAudited(connectionID string, generation int, tokens UserTokens, audit func() error) error {
	return s.WithConnectionLock(connectionID, func() error {
		if err := s.store(connectionID, generation, tokens, false); err != nil {
			return err
		}
		if err := audit(); err != nil {
			// Commit errors can be outcome-ambiguous. Pointer-driven startup
			// reconciliation decides whether this durable stage is owned.
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
		if err := securefs.ProtectCurrentUserOnlyPath(filepath.Join(s.tokenRoot(connectionID), ".lifecycle.lock")); err != nil {
			_ = dir.Remove(".lifecycle.lock")
			return fmt.Errorf("protect github lifecycle lock: %w", err)
		}
	}
	if err := validateProtectedRegularFile(lock, ".lifecycle.lock"); err != nil {
		return err
	}
	if err := lockFile(lock); err != nil {
		return err
	}
	defer unlockFile(lock) //nolint:errcheck
	return fn()
}

func (s *UserTokenStore) RemoveAll(connectionID string) error {
	return s.WithConnectionLock(connectionID, func() error { return s.removeAllUnlocked(connectionID) })
}

// RemoveAllWhileLocked is for lifecycle transactions already executing inside
// WithConnectionLock.
func (s *UserTokenStore) RemoveAllWhileLocked(connectionID string) error {
	return s.removeAllUnlocked(connectionID)
}

func (s *UserTokenStore) removeAllUnlocked(connectionID string) error {
	dir, err := s.openConnection(connectionID, false)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer dir.Close()
	return removeTokenGenerations(dir, 0)
}

func (s *UserTokenStore) store(connectionID string, generation int, tokens UserTokens, replace bool) error {
	if err := validateConnectionID(connectionID); err != nil {
		return err
	}
	if generation <= 0 {
		return fmt.Errorf("invalid github user token generation")
	}
	if err := tokens.Validate(time.Now().Add(-time.Minute)); err != nil {
		return err
	}
	raw, err := json.Marshal(tokens)
	if err != nil || len(raw) > maxUserTokenFileBytes {
		return fmt.Errorf("encode github user tokens")
	}
	return s.storeRaw(connectionID, generation, raw, replace)
}

func (s *UserTokenStore) storeRaw(connectionID string, generation int, raw []byte, replace bool) error {
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
	destination := userTokenName(generation)
	if info, statErr := dir.Lstat(destination); statErr == nil {
		if !replace {
			return fmt.Errorf("github user token generation is already staged")
		}
		file, openErr := securefs.OpenRegularFile(dir, destination, os.O_RDONLY, 0)
		if openErr != nil {
			return openErr
		}
		validateErr := validateProtectedRegularFile(file, destination)
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
	if err := securefs.ProtectCurrentUserOnlyPath(filepath.Join(s.tokenRoot(connectionID), tmpName)); err != nil {
		_ = tmp.Close()
		_ = dir.Remove(tmpName)
		return fmt.Errorf("protect staged github user tokens: %w", err)
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
	if replace {
		err = dir.Rename(tmpName, destination)
	} else {
		err = dir.Link(tmpName, destination)
		if err == nil {
			err = dir.Remove(tmpName)
		}
	}
	if err != nil {
		return fmt.Errorf("install github user tokens: %w", err)
	}
	return securefs.SyncDirectory(dir)
}

// ReconcileGenerations removes temporary, stale, and unowned token generations.
// selectedGeneration runs while the connection lifecycle lock is held so it
// can read the SQLite publication pointer fenced by the same lock as writers.
// It must not attempt to acquire a credential lifecycle lock.
func (s *UserTokenStore) ReconcileGenerations(selectedGeneration func(connectionID string) (int, error)) error {
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
			return removeTokenGenerations(dir, selected)
		}); err != nil {
			return err
		}
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

func validateProtectedRegularFile(file *os.File, name string) error {
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || (securefs.EnforcesPOSIXMode() && info.Mode().Perm() != 0o600) {
		return fmt.Errorf("%s must be a 0600 regular file", name)
	}
	if err := securefs.ValidateCurrentUserOnly(file); err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	return validateFileSingleLink(file, name)
}

func (s *UserTokenStore) tokenRoot(connectionID string) string {
	return filepath.Join(s.root, "github-user-tokens", connectionID)
}

func userTokenTempName() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return ".tokens-" + hex.EncodeToString(raw[:]) + ".tmp", nil
}

func userTokenName(generation int) string {
	return "tokens." + strconv.Itoa(generation) + ".json"
}

func userTokenGeneration(name string) (int, bool) {
	if !strings.HasPrefix(name, "tokens.") || !strings.HasSuffix(name, ".json") {
		return 0, false
	}
	raw := strings.TrimSuffix(strings.TrimPrefix(name, "tokens."), ".json")
	generation, err := strconv.Atoi(raw)
	return generation, err == nil && generation > 0 && strconv.Itoa(generation) == raw
}

func removeTokenGenerations(dir *os.Root, selected int) error {
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
		if !strings.HasPrefix(name, ".tokens-") || !strings.HasSuffix(name, ".tmp") {
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
		generation, generationFile := userTokenGeneration(name)
		if !generationFile || generation == selected {
			continue
		}
		file, openErr := securefs.OpenRegularFile(dir, name, os.O_RDONLY, 0)
		if openErr != nil {
			return openErr
		}
		validateErr := validateProtectedRegularFile(file, name)
		_ = file.Close()
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
	return securefs.SyncDirectory(dir)
}
