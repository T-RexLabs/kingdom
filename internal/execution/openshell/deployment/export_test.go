package deployment

import (
	"archive/tar"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestExportUsesOnlyExplicitContextAndStoppedFixedArtifacts(t *testing.T) {
	dir := t.TempDir()
	archive := filepath.Join(dir, "artifact.tar")
	file, e := os.Create(archive)
	if e != nil {
		t.Fatal(e)
	}
	writer := tar.NewWriter(file)
	_ = writer.WriteHeader(&tar.Header{Name: "artifact", Typeflag: tar.TypeReg, Mode: 0555, Size: 3})
	_, _ = writer.Write([]byte("elf"))
	_ = writer.Close()
	_ = file.Close()
	// The fixture emits one basename selected from the requested fixed path.
	// No real Docker daemon or image registry is contacted.
	helper := `#!/bin/sh
printf '%s\n' "$*" >> "$FIXTURE_DOCKER_LOG"
[ "$1" = --context ] && [ "$2" = explicit-fixture ] || exit 91
shift 2
case "$1" in
 image) for pin do :; done; printf '[{"Architecture":"arm64","Os":"linux","RepoDigests":["%s"]}]' "$pin" ;;
 create) [ "$FIXTURE_CREATE_MALFORMED" != 1 ] || { printf invalid; exit 0; }; printf '%064d\n' 1 ;;
 container) name="$3"; operation="${name#kingdom-openshell-export-}"; printf '[{"ID":"%064d","Config":{"Image":"%s","Labels":{"kingdom.openshell.export":"%s"}},"State":{"Running":false}}]' 1 "$FIXTURE_GATEWAY_IMAGE" "$operation" ;;
 cp) [ "$FIXTURE_FAIL_COPY" != 1 ] || exit 94; for destination do :; done; source="$2"; name="${source##*/}"; cat "$FIXTURE_TAR_DIR/$name.tar" ;;
 rm) [ "$2" = --volumes ] || exit 92; [ "$FIXTURE_FAIL_RM" != 1 ] || exit 95 ;;
 *) exit 93 ;;
esac
`
	for _, name := range []string{"openshell-gateway", "openshell-sandbox", "kingdom-provider-launcher", "opencode", "kingdom-workspace-helper", "kingdom-file-tool-helper"} {
		f, e := os.Create(filepath.Join(dir, name+".tar"))
		if e != nil {
			t.Fatal(e)
		}
		w := tar.NewWriter(f)
		_ = w.WriteHeader(&tar.Header{Name: name, Typeflag: tar.TypeReg, Mode: 0555, Size: 3})
		_, _ = w.Write([]byte("elf"))
		_ = w.Close()
		_ = f.Close()
	}
	if e = os.WriteFile(filepath.Join(dir, "docker"), []byte(helper), 0700); e != nil {
		t.Fatal(e)
	}
	log := filepath.Join(dir, "commands.log")
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("FIXTURE_DOCKER_LOG", log)
	t.Setenv("FIXTURE_TAR_DIR", dir)
	b := fixtureBundle(t)
	sum := sha256.Sum256([]byte("elf"))
	h := hex.EncodeToString(sum[:])
	b.Profile.GatewayBinarySHA256 = h
	b.Profile.SandboxRuntimeBinarySHA256 = h
	o := ExportOptions{DockerContext: "explicit-fixture", GatewayImage: b.Gateway.Image, WorkerImage: b.Worker.Image, Profile: b.Profile, Environment: Environment{Gateway: b.Gateway, WorkerID: b.WorkerID, Callback: b.Callback, Limits: b.Limits}}
	result, e := Export(context.Background(), o)
	if e != nil {
		t.Fatal(e)
	}
	if result.Worker.LauncherSHA256 != h || result.Worker.OpenCodeSHA256 != h {
		t.Fatal("worker artifact bytes were not hashed")
	}
	raw, _ := os.ReadFile(log)
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		if !strings.HasPrefix(line, "--context explicit-fixture ") {
			t.Fatal("implicit Docker context", line)
		}
		if strings.Contains(line, " create ") && !strings.Contains(line, "--pull=never") {
			t.Fatal("create can pull an image")
		}
		if strings.Contains(line, " start ") || strings.Contains(line, " run ") || strings.Contains(line, " pull ") {
			t.Fatal("unexpected runtime mutation", line)
		}
	}
	before := string(raw)
	o.DockerContext = ""
	if _, e = Export(context.Background(), o); e == nil {
		t.Fatal("implicit context accepted")
	}
	after, _ := os.ReadFile(log)
	if string(after) != before {
		t.Fatal("missing context invoked Docker")
	}
	o.DockerContext = "explicit-fixture"
	t.Setenv("FIXTURE_GATEWAY_IMAGE", o.GatewayImage)
	t.Setenv("FIXTURE_CREATE_MALFORMED", "1")
	_ = os.WriteFile(log, nil, 0600)
	if _, e = Export(context.Background(), o); e == nil {
		t.Fatal("malformed creation reply accepted")
	}
	faultLog, _ := os.ReadFile(log)
	if !strings.Contains(string(faultLog), " container inspect kingdom-openshell-export-") || !strings.Contains(string(faultLog), " rm --volumes ") {
		t.Fatal("malformed create reply lost stopped-container cleanup identity", string(faultLog))
	}
	t.Setenv("FIXTURE_CREATE_MALFORMED", "")
	t.Setenv("FIXTURE_FAIL_COPY", "1")
	t.Setenv("FIXTURE_FAIL_RM", "1")
	if _, e = Export(context.Background(), o); e == nil || !strings.Contains(e.Error(), "cleanup failed") || !strings.Contains(e.Error(), strings.Repeat("0", 63)+"1") {
		t.Fatal("extraction error hid exact cleanup debt", e)
	}
}
