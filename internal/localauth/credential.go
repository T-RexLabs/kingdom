// Package localauth manages Kingdom's local operator bearer credential.
package localauth

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"kingdom/internal/securefs"
)

const CredentialFileName = "operator-token"

func CredentialPath(kingdomHome string) string {
	return filepath.Join(kingdomHome, "secrets", CredentialFileName)
}

// EnsureCredential creates a credential exactly once and preserves an
// existing credential across repeated init operations.
func EnsureCredential(kingdomHome string) (string, error) {
	secrets, err := openSecretsRoot(kingdomHome, true)
	if err != nil {
		return "", fmt.Errorf("open protected local secrets directory: %w", err)
	}
	defer secrets.Close()
	path := CredentialPath(kingdomHome)
	if _, err := secrets.Lstat(CredentialFileName); err == nil {
		if _, err := readCredentialFromRoot(secrets); err != nil {
			return "", err
		}
		return path, nil
	} else if !os.IsNotExist(err) {
		return "", fmt.Errorf("inspect local operator credential: %w", err)
	}

	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("generate local operator credential: %w", err)
	}
	token := base64.RawURLEncoding.EncodeToString(raw) + "\n"
	file, err := secrets.OpenFile(CredentialFileName, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return "", fmt.Errorf("create local operator credential: %w", err)
	}
	if err := securefs.ProtectCurrentUserOnlyPath(path); err != nil {
		_ = file.Close()
		_ = secrets.Remove(CredentialFileName)
		return "", fmt.Errorf("protect local operator credential: %w", err)
	}
	writeErr := error(nil)
	if _, err := file.WriteString(token); err != nil {
		writeErr = err
	} else if err := file.Sync(); err != nil {
		writeErr = err
	}
	closeErr := file.Close()
	if writeErr != nil {
		_ = secrets.Remove(CredentialFileName)
		return "", fmt.Errorf("write local operator credential: %w", writeErr)
	}
	if closeErr != nil {
		return "", fmt.Errorf("close local operator credential: %w", closeErr)
	}
	if err := securefs.SyncDirectory(secrets); err != nil {
		return "", fmt.Errorf("sync local secrets directory: %w", err)
	}
	return path, nil
}

func ReadCredential(path string) ([]byte, error) {
	kingdomHome := filepath.Dir(filepath.Dir(filepath.Clean(path)))
	secrets, err := openSecretsRoot(kingdomHome, false)
	if err != nil {
		return nil, fmt.Errorf("open protected local secrets directory: %w", err)
	}
	defer secrets.Close()
	return readCredentialNamed(secrets, filepath.Base(path))
}

// ReadCredentialFile reads an explicitly supplied credential from a protected
// external directory without treating that directory as a Kingdom workspace.
func ReadCredentialFile(path string) ([]byte, error) {
	absPath, err := filepath.Abs(strings.TrimSpace(path))
	if err != nil {
		return nil, fmt.Errorf("resolve explicit local operator credential: %w", err)
	}
	directory := filepath.Dir(absPath)
	if filepath.Base(directory) == "secrets" && filepath.Base(filepath.Dir(directory)) == ".kingdom" {
		return ReadCredential(absPath)
	}
	parent := filepath.Dir(directory)
	if directory == parent {
		return nil, fmt.Errorf("explicit local operator credential must be in a protected directory")
	}
	root, err := securefs.OpenDirChain(parent, securefs.Directory{Name: filepath.Base(directory), Mode: 0o700, ExactMode: true})
	if err != nil {
		return nil, fmt.Errorf("open protected explicit credential directory: %w", err)
	}
	defer root.Close()
	return readCredentialNamed(root, filepath.Base(absPath))
}

func openSecretsRoot(kingdomHome string, create bool) (*os.Root, error) {
	home := filepath.Clean(kingdomHome)
	return securefs.OpenDirChain(filepath.Dir(home),
		securefs.Directory{Name: filepath.Base(home), Mode: 0o755, Create: create},
		securefs.Directory{Name: "secrets", Mode: 0o700, Create: create, ExactMode: true},
	)
}

func readCredentialFromRoot(secrets *os.Root) ([]byte, error) {
	return readCredentialNamed(secrets, CredentialFileName)
}

func readCredentialNamed(secrets *os.Root, name string) ([]byte, error) {
	file, err := securefs.OpenRegularFile(secrets, name, os.O_RDONLY, 0)
	if err != nil {
		return nil, fmt.Errorf("read local operator credential: %w", err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("inspect local operator credential: %w", err)
	}
	if securefs.EnforcesPOSIXMode() && info.Mode().Perm() != 0o600 {
		return nil, fmt.Errorf("local operator credential must have mode 0600")
	}
	if err := securefs.ValidateCurrentUserOnly(file); err != nil {
		return nil, err
	}
	raw, err := io.ReadAll(io.LimitReader(file, 1024))
	if err != nil {
		return nil, fmt.Errorf("read local operator credential: %w", err)
	}
	token := strings.TrimSpace(string(raw))
	decoded, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil || len(decoded) != 32 {
		return nil, fmt.Errorf("local operator credential has invalid format")
	}
	return []byte(token), nil
}
