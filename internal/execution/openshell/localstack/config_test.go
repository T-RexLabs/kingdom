package localstack

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func validConfig(t *testing.T) Config {
	t.Helper()
	var c Config
	c.Version = 1
	c.StateRoot = t.TempDir()
	c.OwnerID = "00000000-0000-4000-8000-000000000001"
	c.GatewayID = "gateway"
	c.WorkerID = "worker"
	c.Gateway.Address = "localhost:50051"
	c.Gateway.CAFile = "/ca"
	c.Gateway.CertificateFile = "/cert"
	c.Gateway.KeyFile = "/key"
	hash := strings.Repeat("a", 64)
	c.ProfileSHA256 = hash
	c.HelperSHA256 = hash
	c.FileToolSHA256 = hash
	c.LauncherSHA256 = hash
	c.OpenCodeSHA256 = hash
	c.Image = "worker@sha256:" + hash
	c.ContextLimit = 100
	c.OutputLimit = 100
	c.LifetimeSeconds = 60
	c.MasterKeyFile = "/master"
	c.Model.APIKeyFile = "/secret"
	c.Model.AuthKind = "api_key"
	c.Callback.Listen = "127.0.0.1:5443"
	c.Callback.Origin = "https://localhost:5443"
	c.Callback.CertificateFile = "/cert"
	c.Callback.KeyFile = "/key"
	c.Callback.CAFile = "/ca"
	return c
}
func TestExplicitManagedConfigRejectsUnsafeFilesAndRoutes(t *testing.T) {
	c := validConfig(t)
	path := filepath.Join(t.TempDir(), "managed.json")
	write := func() {
		t.Helper()
		raw, e := json.Marshal(c)
		if e != nil {
			t.Fatal(e)
		}
		if e = os.WriteFile(path, raw, 0600); e != nil {
			t.Fatal(e)
		}
	}
	write()
	if _, e := Load(path); e != nil {
		t.Fatal(e)
	}
	for _, change := range []func(){func() { c.Callback.Origin = "http://localhost:5443" }, func() { c.Callback.Origin = "https://localhost:5443/path" }, func() { c.Callback.Origin = "https://localhost:6443" }, func() { c.MasterKeyFile = "relative" }, func() { c.Image = "worker:latest" }} {
		c = validConfig(t)
		change()
		write()
		if _, e := Load(path); e == nil {
			t.Fatal("unsafe configuration accepted")
		}
	}
	c = validConfig(t)
	write()
	if e := os.Chmod(path, 0644); e != nil {
		t.Fatal(e)
	}
	if _, e := Load(path); e == nil {
		t.Fatal("public configuration accepted")
	}
	os.Chmod(path, 0600)
	link := filepath.Join(t.TempDir(), "link")
	if e := os.Symlink(path, link); e != nil {
		t.Fatal(e)
	}
	if _, e := Load(link); e == nil {
		t.Fatal("symlink configuration accepted")
	}
	if e := os.WriteFile(path, []byte(`{"version":1,"unknown":true}`), 0600); e != nil {
		t.Fatal(e)
	}
	if _, e := Load(path); e == nil {
		t.Fatal("unknown fields accepted")
	}
}
