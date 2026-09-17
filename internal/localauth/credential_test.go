//go:build !windows

package localauth

import (
	"os"
	"path/filepath"
	"testing"
)

func TestEnsureCredentialCreatesProtectedStableSecret(t *testing.T) {
	home := filepath.Join(t.TempDir(), ".kingdom")
	first, err := EnsureCredential(home)
	if err != nil {
		t.Fatal(err)
	}
	firstValue, err := ReadCredential(first)
	if err != nil {
		t.Fatal(err)
	}
	second, err := EnsureCredential(home)
	if err != nil {
		t.Fatal(err)
	}
	secondValue, err := ReadCredential(second)
	if err != nil {
		t.Fatal(err)
	}
	if string(firstValue) != string(secondValue) {
		t.Fatal("repeated initialization rotated the operator credential")
	}
	if info, _ := os.Stat(filepath.Dir(first)); info.Mode().Perm() != 0o700 {
		t.Fatalf("secrets mode=%o", info.Mode().Perm())
	}
	if info, _ := os.Stat(first); info.Mode().Perm() != 0o600 {
		t.Fatalf("credential mode=%o", info.Mode().Perm())
	}
}

func TestReadCredentialRejectsSymlinkAndLooseMode(t *testing.T) {
	home := filepath.Join(t.TempDir(), ".kingdom")
	path, err := EnsureCredential(home)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadCredential(path); err == nil {
		t.Fatal("expected loose credential mode rejection")
	}
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(filepath.Dir(path), "credential-link")
	if err := os.Symlink(path, link); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadCredential(link); err == nil {
		t.Fatal("expected credential symlink rejection")
	}
}

func TestEnsureCredentialRejectsSymlinkedKingdomHome(t *testing.T) {
	project := t.TempDir()
	outside := t.TempDir()
	sentinel := filepath.Join(outside, "sentinel")
	if err := os.WriteFile(sentinel, []byte("unchanged"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(project, ".kingdom")); err != nil {
		t.Fatal(err)
	}
	if _, err := EnsureCredential(filepath.Join(project, ".kingdom")); err == nil {
		t.Fatal("expected symlinked kingdom home rejection")
	}
	if _, err := os.Stat(filepath.Join(outside, "secrets", CredentialFileName)); !os.IsNotExist(err) {
		t.Fatalf("credential escaped project: %v", err)
	}
	raw, _ := os.ReadFile(sentinel)
	if string(raw) != "unchanged" {
		t.Fatal("outside sentinel changed")
	}
}

func TestEnsureCredentialRejectsSymlinkedSecretsDirectory(t *testing.T) {
	project := t.TempDir()
	home := filepath.Join(project, ".kingdom")
	outside := t.TempDir()
	if err := os.Mkdir(home, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(home, "secrets")); err != nil {
		t.Fatal(err)
	}
	before, _ := os.Stat(outside)
	if _, err := EnsureCredential(home); err == nil {
		t.Fatal("expected symlinked secrets rejection")
	}
	after, _ := os.Stat(outside)
	if before.Mode().Perm() != after.Mode().Perm() {
		t.Fatalf("outside mode changed from %o to %o", before.Mode().Perm(), after.Mode().Perm())
	}
	if _, err := os.Stat(filepath.Join(outside, CredentialFileName)); !os.IsNotExist(err) {
		t.Fatalf("credential escaped secrets directory: %v", err)
	}
}

func TestReadCredentialRejectsSymlinkedKingdomAncestor(t *testing.T) {
	project := t.TempDir()
	realHome := filepath.Join(t.TempDir(), ".kingdom")
	path, err := EnsureCredential(realHome)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(realHome, filepath.Join(project, ".kingdom")); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadCredential(filepath.Join(project, ".kingdom", "secrets", filepath.Base(path))); err == nil {
		t.Fatal("expected symlinked kingdom ancestor rejection")
	}
	if _, err := ReadCredentialFile(filepath.Join(project, ".kingdom", "secrets", filepath.Base(path))); err == nil {
		t.Fatal("expected explicit credential to reject symlinked kingdom ancestor")
	}
}

func TestReadCredentialFileReadsProtectedExternalCredential(t *testing.T) {
	t.Parallel()

	home := filepath.Join(t.TempDir(), ".kingdom")
	credentialPath, err := EnsureCredential(home)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(credentialPath)
	if err != nil {
		t.Fatal(err)
	}
	directory := filepath.Join(t.TempDir(), "credentials")
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	externalPath := filepath.Join(directory, "operator-token")
	if err := os.WriteFile(externalPath, raw, 0o600); err != nil {
		t.Fatal(err)
	}

	credential, err := ReadCredentialFile(externalPath)
	if err != nil {
		t.Fatalf("read protected external credential: %v", err)
	}
	if string(credential) != string(raw[:len(raw)-1]) {
		t.Fatalf("credential=%q want=%q", credential, raw[:len(raw)-1])
	}
	link := filepath.Join(t.TempDir(), "credentials-link")
	if err := os.Symlink(directory, link); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadCredentialFile(filepath.Join(link, "operator-token")); err == nil {
		t.Fatal("expected symlinked external credential directory rejection")
	}
}
