//go:build linux || darwin

package providerlauncher

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestProviderHasFixedOpenShellTrust(t *testing.T) {
	t.Setenv("NODE_EXTRA_CA_CERTS", "/workspace/hostile.pem")
	t.Setenv("SSL_CERT_FILE", "/workspace/hostile.pem")
	env := strings.Join(environment(job()), "\n")
	for _, expected := range []string{"NODE_EXTRA_CA_CERTS=/run/openshell-supervisor-ca/material/ca.crt", "SSL_CERT_FILE=/run/openshell-supervisor-ca/material/ca-bundle.crt"} {
		if !strings.Contains(env, expected) {
			t.Fatalf("missing fixed trust: %s", expected)
		}
	}
	if strings.Contains(env, "hostile.pem") {
		t.Fatal("inherited trust override")
	}
}

func TestTrustFileRejectsUnsafeEntries(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "ca.crt")
	if err := os.WriteFile(path, []byte("certificate"), 0444); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"regular", "writable", "symlink", "missing", "directory"} {
		t.Run(name, func(t *testing.T) {
			candidate := path
			switch name {
			case "writable":
				candidate = filepath.Join(root, name)
				_ = os.WriteFile(candidate, []byte("certificate"), 0644)
			case "symlink":
				candidate = filepath.Join(root, name)
				_ = os.Symlink(path, candidate)
			case "missing":
				candidate = filepath.Join(root, name)
			case "directory":
				candidate = filepath.Join(root, name)
				_ = os.Mkdir(candidate, 0555)
			}
			err := checkTrustFile(candidate, uint32(os.Getuid()))
			if (err == nil) != (name == "regular") {
				t.Fatalf("unexpected validation result: %v", err)
			}
		})
	}
}
