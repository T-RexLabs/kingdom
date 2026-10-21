package filetoolhelper

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"kingdom/internal/securefs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func job(tool string, input any) Job {
	raw, _ := json.Marshal(input)
	return Job{Version: 1, HelperSHA256: strings.Repeat("a", 64), Tool: tool, Input: raw}
}
func TestConfinedPathsNeverFollowSymlinks(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	os.WriteFile(filepath.Join(outside, "secret"), []byte("private"), 0600)
	os.Symlink(outside, filepath.Join(root, "escape"))
	for _, j := range []Job{job("read", map[string]any{"path": "escape/secret"}), job("write", map[string]any{"path": "escape/secret", "content": "changed"}), job("edit", map[string]any{"path": "escape/secret", "oldString": "private", "newString": "changed"}), job("glob", map[string]any{"path": "escape", "pattern": "**/*"}), job("grep", map[string]any{"path": "escape", "pattern": "private"})} {
		result := execute(context.Background(), root, j)
		if result.OK {
			t.Fatalf("%s followed symlink", j.Tool)
		}
	}
	raw, _ := os.ReadFile(filepath.Join(outside, "secret"))
	if string(raw) != "private" {
		t.Fatal("outside data changed")
	}
}
func TestStrictJobRejectsDuplicateUnknownAndTrailingFields(t *testing.T) {
	valid, _ := json.Marshal(job("read", map[string]any{"path": "file"}))
	for _, raw := range []string{string(valid) + " {}", strings.Replace(string(valid), `"version":1`, `"version":1,"version":1`, 1), strings.Replace(string(valid), `"path":"file"`, `"path":"file","path":"other"`, 1), strings.Replace(string(valid), `"path":"file"`, `"path":"file","environment":{}`, 1)} {
		if _, e := DecodeJob(strings.NewReader(raw)); e == nil {
			t.Fatal("invalid structured input accepted")
		}
	}
}
func TestProtectedComponentsAndTraversalRejected(t *testing.T) {
	for _, p := range []string{"../secret", "a/../b", ".kingdom/state", "nested/.git/config", "/workspace/.kingdom-provider/config", "a/.kingdom-provider/token"} {
		if _, e := relative(p); e == nil {
			t.Fatalf("accepted %s", p)
		}
	}
}

func TestFixedExecutableIdentityNeedsNoProcAndRejectsSymlink(t *testing.T) {
	rootPath := t.TempDir()
	name := filepath.Join(rootPath, strings.TrimPrefix(HelperPath, "/"))
	os.MkdirAll(filepath.Dir(name), 0755)
	content := []byte("fixed image helper")
	os.WriteFile(name, content, 0755)
	root, e := securefs.OpenRoot(rootPath)
	if e != nil {
		t.Fatal(e)
	}
	defer root.Close()
	digest := sha256.Sum256(content)
	if e = verifyExecutable(root, hex.EncodeToString(digest[:])); e != nil {
		t.Fatal(e)
	}
	os.Remove(name)
	outside := filepath.Join(t.TempDir(), "other")
	os.WriteFile(outside, content, 0755)
	os.Symlink(outside, name)
	if e = verifyExecutable(root, hex.EncodeToString(digest[:])); e == nil {
		t.Fatal("helper identity followed symlink")
	}
}
