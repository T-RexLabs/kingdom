package daemon

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func certificate(t *testing.T, root string) ([]byte, []byte) {
	t.Helper()
	key, e := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if e != nil {
		t.Fatal(e)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "Kingdom Test"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}, BasicConstraintsValid: true, IsCA: true, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}}
	der, e := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if e != nil {
		t.Fatal(e)
	}
	encoded, e := x509.MarshalPKCS8PrivateKey(key)
	if e != nil {
		t.Fatal(e)
	}
	cert, secret := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: encoded})
	if e = os.WriteFile(filepath.Join(root, "server.crt"), cert, 0644); e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(filepath.Join(root, "server.key"), secret, 0600); e != nil {
		t.Fatal(e)
	}
	return cert, secret
}
func fixture(t *testing.T) (string, fileConfig) {
	t.Helper()
	root := filepath.Join(t.TempDir(), "config ? #")
	if e := os.Mkdir(root, 0700); e != nil {
		t.Fatal(e)
	}
	certificate(t, root)
	if e := os.WriteFile(filepath.Join(root, "master.key"), []byte(strings.Repeat("m", 32)), 0600); e != nil {
		t.Fatal(e)
	}
	if e := os.WriteFile(filepath.Join(root, "github.secret"), []byte("private-client-secret\n"), 0600); e != nil {
		t.Fatal(e)
	}
	f := fileConfig{Version: 1, PublicOrigin: "https://127.0.0.1:9090", DataRoot: "state", MasterKeyFile: "master.key", TLSCertificate: "server.crt", TLSKey: "server.key", InitialOwners: []ownerConfig{{Issuer: "https://github.com", Subject: "1"}}, GitHub: &githubConfig{ClientID: "client", ClientSecretFile: "github.secret"}, Gateway: gatewayConfig{Address: "127.0.0.1:59563", GatewayID: "gateway", CAFile: "server.crt", CertificateFile: "server.crt", KeyFile: "server.key", Image: "example.invalid/worker@sha256:" + strings.Repeat("a", 64), Profile: strings.Repeat("b", 64), Launcher: strings.Repeat("c", 64), OpenCode: strings.Repeat("d", 64), Helper: strings.Repeat("e", 64), FileTool: strings.Repeat("f", 64)}}
	path := filepath.Join(root, "service.json")
	writeConfig(t, path, f)
	return path, f
}
func writeConfig(t *testing.T, path string, f fileConfig) {
	t.Helper()
	raw, e := json.Marshal(f)
	if e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(path, raw, 0600); e != nil {
		t.Fatal(e)
	}
}
func TestConfigurationUsesRelativePrivateFilesAndExactPins(t *testing.T) {
	path, f := fixture(t)
	c, e := load(path)
	if e != nil {
		t.Fatal(e)
	}
	defer clear(c.key)
	if c.file.DataRoot != filepath.Join(filepath.Dir(path), "state") || c.file.Gateway.WorkerID != "kingdom-central" || c.github.ClientSecret != "private-client-secret" || string(c.key) != strings.Repeat("m", 32) {
		t.Fatal("incorrect configuration binding")
	}
	f.Gateway.Profile = strings.Repeat("B", 64)
	writeConfig(t, path, f)
	if _, e = load(path); !errors.Is(e, ErrConfiguration) {
		t.Fatal("noncanonical pin accepted")
	}
}
func TestConfigurationRejectsPublicOrSymlinkSecretsAndTrailingData(t *testing.T) {
	for _, change := range []string{"permissions", "symlink", "trailing", "oversized"} {
		t.Run(change, func(t *testing.T) {
			path, _ := fixture(t)
			root := filepath.Dir(path)
			switch change {
			case "permissions":
				if e := os.Chmod(filepath.Join(root, "master.key"), 0644); e != nil {
					t.Fatal(e)
				}
			case "symlink":
				if e := os.Remove(filepath.Join(root, "master.key")); e != nil {
					t.Fatal(e)
				}
				if e := os.Symlink("github.secret", filepath.Join(root, "master.key")); e != nil {
					t.Fatal(e)
				}
			case "trailing", "oversized":
				file, e := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
				if e != nil {
					t.Fatal(e)
				}
				suffix := " {}"
				if change == "oversized" {
					suffix = strings.Repeat(" ", 128<<10) + "{}"
				}
				_, _ = file.WriteString(suffix)
				_ = file.Close()
			}
			if _, e := load(path); !errors.Is(e, ErrConfiguration) {
				t.Fatalf("unsafe config accepted: %v", e)
			}
		})
	}
}

func TestConfigurationRejectsUnsupportedWorkerLimitsAndClearsKey(t *testing.T) {
	for _, change := range []string{"subsecond", "fractional", "lifetime", "context", "output", "output-exceeds-context"} {
		t.Run(change, func(t *testing.T) {
			path, f := fixture(t)
			switch change {
			case "subsecond":
				f.Gateway.Lifetime = "500ms"
			case "fractional":
				f.Gateway.Lifetime = "1.5s"
			case "lifetime":
				f.Gateway.Lifetime = "25h"
			case "context":
				f.Gateway.ContextLimit = 2000001
			case "output":
				f.Gateway.ContextLimit = 2000000
				f.Gateway.OutputLimit = 200001
			case "output-exceeds-context":
				f.Gateway.ContextLimit = 1000
				f.Gateway.OutputLimit = 1001
			}
			writeConfig(t, path, f)
			c, e := load(path)
			if !errors.Is(e, ErrConfiguration) || c.key != nil {
				t.Fatal("unsupported worker configuration accepted or key retained", e)
			}
		})
	}
}

type streamingApp struct {
	entered, closing chan struct{}
	closeErr         error
}

func (a *streamingApp) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	close(a.entered)
	<-a.closing
	w.WriteHeader(204)
}
func (a *streamingApp) Close(context.Context) error { close(a.closing); return a.closeErr }
func TestListenerCancelsApplicationWhileDrainingHTTPAndReportsCleanupFailure(t *testing.T) {
	root := t.TempDir()
	cert, key := certificate(t, root)
	pair, e := tls.X509KeyPair(cert, key)
	if e != nil {
		t.Fatal(e)
	}
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(cert)
	listener, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	failure := errors.New("provider cleanup unresolved")
	app := &streamingApp{entered: make(chan struct{}), closing: make(chan struct{}), closeErr: failure}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		result <- serve(ctx, listener, &tls.Config{Certificates: []tls.Certificate{pair}, MinVersion: tls.VersionTLS13}, app)
	}()
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS13}}, Timeout: time.Second}
	response := make(chan error, 1)
	go func() {
		r, e := client.Get("https://" + listener.Addr().String())
		if e == nil {
			r.Body.Close()
		}
		response <- e
	}()
	select {
	case <-app.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("listener did not admit request")
	}
	cancel()
	select {
	case e := <-result:
		if !errors.Is(e, failure) {
			t.Fatal("cleanup failure lost", e)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("shutdown waited for HTTP before cancelling consumers")
	}
	if e := <-response; e != nil {
		t.Fatal(e)
	}
}
