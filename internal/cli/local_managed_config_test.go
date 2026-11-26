package cli

import (
	"path/filepath"
	"testing"
)

func TestLocalOpenShellRequiresExplicitAbsoluteConfiguration(t *testing.T) {
	c := installConfig{}
	c.KingdomHome = t.TempDir()
	c.DBPath = filepath.Join(c.KingdomHome, "state.db")
	c.Server.Listen = "127.0.0.1:5444"
	c.Server.BasePath = "/api/v1"
	c.Provider.GovernanceMode = "autonomous"
	got, e := readInstallConfigFromStruct(c)
	if e != nil {
		t.Fatal(e)
	}
	if got.Execution.Backend != "local" || got.Execution.ManagedConfig != "" {
		t.Fatal("default acquired managed configuration")
	}
	c.Execution.Backend = "openshell"
	c.Execution.ManagedConfig = "relative.json"
	if _, e = readInstallConfigFromStruct(c); e == nil {
		t.Fatal("relative managed config accepted")
	}
	c.Execution.ManagedConfig = filepath.Join(t.TempDir(), "does-not-exist.json")
	if _, e = readInstallConfigFromStruct(c); e != nil {
		t.Fatal("configuration normalization read operator files", e)
	}
	c.Execution.Backend = "local"
	if _, e = readInstallConfigFromStruct(c); e == nil {
		t.Fatal("unused managed config silently accepted")
	}
}
