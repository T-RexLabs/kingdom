package deployment

import (
	"archive/tar"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
)

func fixtureBundle(t *testing.T) Bundle {
	t.Helper()
	h := strings.Repeat("a", 64)
	b := Bundle{Kind: Kind, Version: 1, WorkerID: "local-worker"}
	b.Gateway.ID, b.Gateway.Address, b.Gateway.Image = "gateway", "localhost:59563", "example/gateway@sha256:"+h
	b.Gateway.CAFile, b.Gateway.CertificateFile, b.Gateway.KeyFile = "/private/ca.crt", "/private/operator.crt", "/private/operator.key"
	b.Profile = Profile{1, h, "example/supervisor@sha256:" + h, "example/runtime@sha256:" + h, h}
	b.ProfileSHA256 = b.Profile.Digest()
	b.Worker = Worker{"example/worker@sha256:" + h, h, h, h, h}
	b.Callback.Listen, b.Callback.Origin = "0.0.0.0:9443", "https://localhost:9443"
	b.Callback.CertificateFile, b.Callback.KeyFile, b.Callback.CAFile = "/private/server.crt", "/private/server.key", "/private/ca.crt"
	b.Callback.AllowedIPs = []string{"127.0.0.1"}
	b.Limits = Limits{128000, 16000, 3600}
	return b
}

func TestBundleRejectsAlteredOrAmbiguousDeployment(t *testing.T) {
	b := fixtureBundle(t)
	raw, _ := json.Marshal(b)
	if _, err := Decode(raw); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*Bundle){
		func(v *Bundle) { v.Version = 2 },
		func(v *Bundle) { v.ProfileSHA256 = strings.Repeat("b", 64) },
		func(v *Bundle) { v.Worker.Image = "example/worker:latest" },
		func(v *Bundle) { v.Worker.LauncherSHA256 = "short" },
		func(v *Bundle) { v.Gateway.KeyFile = "relative.key" },
		func(v *Bundle) { v.Callback.Origin = "http://localhost:9443" },
		func(v *Bundle) { v.Callback.AllowedIPs = []string{"host.example"} },
	} {
		v := b
		mutate(&v)
		data, _ := json.Marshal(v)
		if _, err := Decode(data); err == nil {
			t.Fatalf("invalid deployment accepted: %s", data)
		}
	}
	for _, data := range [][]byte{append([]byte(`{"unknown":true,`), raw[1:]...), append([]byte(`{"version":1,`), raw[1:]...)} {
		if _, err := Decode(data); err == nil {
			t.Fatal("unknown or duplicate field accepted")
		}
	}
}

func TestFixedArtifactExtractionRejectsUnsafeOrExtraEntries(t *testing.T) {
	for _, scenario := range []string{"valid", "symlink", "traversal", "extra"} {
		t.Run(scenario, func(t *testing.T) {
			var archive bytes.Buffer
			w := tar.NewWriter(&archive)
			h := &tar.Header{Name: "opencode", Mode: 0555, Size: 3, Typeflag: tar.TypeReg}
			if scenario == "symlink" {
				h.Typeflag = tar.TypeSymlink
				h.Linkname = "/private/key"
				h.Size = 0
			}
			if scenario == "traversal" {
				h.Name = "../opencode"
			}
			if err := w.WriteHeader(h); err != nil {
				t.Fatal(err)
			}
			if h.Size > 0 {
				_, _ = w.Write([]byte("elf"))
			}
			if scenario == "extra" {
				_ = w.WriteHeader(&tar.Header{Name: "other", Mode: 0600, Typeflag: tar.TypeReg})
			}
			_ = w.Close()
			digest, err := artifactDigest(&archive, "opencode")
			if scenario == "valid" {
				want := sha256.Sum256([]byte("elf"))
				if err != nil || digest != hex.EncodeToString(want[:]) {
					t.Fatalf("digest=%s err=%v", digest, err)
				}
			} else if err == nil {
				t.Fatal("unsafe artifact archive accepted")
			}
		})
	}
}
