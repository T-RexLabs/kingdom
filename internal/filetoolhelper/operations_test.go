package filetoolhelper

import (
	"bytes"
	"context"
	"encoding/json"
	pb "kingdom/internal/execution/openshell/managedpb"
	"kingdom/internal/securefs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func perform(t *testing.T, root, tool string, input any) Result {
	t.Helper()
	r := execute(context.Background(), root, job(tool, input))
	raw, e := json.Marshal(r)
	if e != nil {
		t.Fatal(e)
	}
	if len(raw) > MaxResultBytes {
		t.Fatal("unbounded result")
	}
	if _, e = DecodeResult(bytes.NewReader(raw), tool); e != nil {
		t.Fatal(e)
	}
	return r
}
func TestSixOperationsAndExactOutput(t *testing.T) {
	root := t.TempDir()
	r := perform(t, root, "write", map[string]any{"path": "src/file.txt", "content": "first\nsecond\n"})
	if !r.OK || r.Result.Content != "Created file successfully: src/file.txt" {
		t.Fatalf("write %+v", r)
	}
	r = perform(t, root, "read", map[string]any{"path": "src/file.txt"})
	if !r.OK || r.Result.Content != "Read file src/file.txt, lines 1-2\n1: first\n2: second" {
		t.Fatalf("read %+v", r)
	}
	r = perform(t, root, "edit", map[string]any{"path": "src/file.txt", "oldString": "second", "newString": "changed"})
	if !r.OK || r.Result.Content != "Edited src/file.txt (1 replacement)" {
		t.Fatalf("edit %+v", r)
	}
	r = perform(t, root, "patch", map[string]any{"patchText": "*** Begin Patch\n*** Update File: src/file.txt\n@@\n first\n-changed\n+last\n*** Add File: added.txt\n+new\n*** End Patch"})
	if !r.OK {
		t.Fatalf("patch %+v", r.Error)
	}
	r = perform(t, root, "glob", map[string]any{"pattern": "**/*.txt"})
	if !r.OK || r.Result.Content != ProjectPath+"/added.txt\n"+ProjectPath+"/src/file.txt" {
		t.Fatalf("glob %+v", r)
	}
	r = perform(t, root, "grep", map[string]any{"pattern": "last"})
	if !r.OK || r.Result.Content != "Found 1 matches\n"+ProjectPath+"/src/file.txt:\n  Line 2: last\n" {
		t.Fatalf("grep %+v", r)
	}
}
func TestPatchPreflightsAllFilesBeforeMutation(t *testing.T) {
	root := t.TempDir()
	os.WriteFile(filepath.Join(root, "a"), []byte("old\n"), 0600)
	r := perform(t, root, "patch", map[string]any{"patchText": "*** Begin Patch\n*** Update File: a\n@@\n-old\n+new\n*** Update File: absent\n@@\n-old\n+new\n*** End Patch"})
	if r.OK || r.Error.Effect != "none" {
		t.Fatalf("unverified patch %+v", r)
	}
	raw, _ := os.ReadFile(filepath.Join(root, "a"))
	if string(raw) != "old\n" {
		t.Fatal("partial mutation during preflight")
	}
}
func TestEditPreconditionsAndBOMPreservation(t *testing.T) {
	root := t.TempDir()
	name := filepath.Join(root, "a")
	os.WriteFile(name, []byte("\ufeffold old\n"), 0600)
	r := perform(t, root, "edit", map[string]any{"path": "a", "oldString": "old", "newString": "new"})
	if r.OK || r.Error.Effect != "none" {
		t.Fatal("ambiguous edit applied")
	}
	r = perform(t, root, "edit", map[string]any{"path": "a", "oldString": "old", "newString": "new", "replaceAll": true})
	if !r.OK {
		t.Fatal(r.Error)
	}
	raw, _ := os.ReadFile(name)
	if string(raw) != "\ufeffnew new\n" {
		t.Fatal("BOM or exact replacements changed")
	}
	r = perform(t, root, "write", map[string]any{"path": "a", "content": "next"})
	if !r.OK {
		t.Fatal(r.Error)
	}
	raw, _ = os.ReadFile(name)
	if string(raw) != "\ufeffnext" {
		t.Fatal("write lost BOM")
	}
}
func TestReadPagingAndBoundedOutput(t *testing.T) {
	root := t.TempDir()
	os.WriteFile(filepath.Join(root, "a"), []byte("one\ntwo\nthree\n"), 0600)
	r := perform(t, root, "read", map[string]any{"path": "a", "offset": 2, "limit": 1})
	if !r.OK || r.Result.Content != "Read file a, lines 2-2\n2: two\n[Output truncated. Continue reading with offset: 3]" {
		t.Fatalf("paging %+v", r)
	}
	os.WriteFile(filepath.Join(root, "a"), []byte(strings.Repeat("x", 3000)), 0600)
	r = perform(t, root, "read", map[string]any{"path": "a"})
	if !r.OK || !strings.Contains(r.Result.Content, "line truncated to 2000 chars") {
		t.Fatal("line limit missing")
	}
	os.WriteFile(filepath.Join(root, "a"), []byte{0, 1, 2}, 0600)
	r = perform(t, root, "read", map[string]any{"path": "a"})
	if r.OK || r.Error.Code != "unsupported" {
		t.Fatal("binary read accepted")
	}
}
func TestCommandPinsScopeAndNoEnvironment(t *testing.T) {
	spec, e := Command(job("read", map[string]any{"path": "a"}))
	if e != nil {
		t.Fatal(e)
	}
	if spec.Scope != pb.CommandScope_COMMAND_SCOPE_PROJECT_TOOL || spec.Workdir != ProjectPath || len(spec.Environment) != 0 || strings.Join(spec.Argv, " ") != HelperPath+" run-v1" {
		t.Fatal("command profile changed")
	}
}
func TestResultRejectsContradictionAndForeignTool(t *testing.T) {
	for _, raw := range []string{`{"version":1,"tool":"read","ok":true,"error":{"code":"x","message":"x","effect":"none"}}`, `{"version":1,"tool":"write","ok":false,"error":{"code":"x","message":"x","effect":"none"}}`, `{"version":1,"tool":"read","ok":false,"error":{"code":"x","message":"x","effect":"success"}}`} {
		if _, e := DecodeResult(strings.NewReader(raw), "read"); e == nil {
			t.Fatal("contradictory result accepted")
		}
	}
}

func TestSearchIgnoreHiddenAndLiteralSemantics(t *testing.T) {
	root := t.TempDir()
	os.Mkdir(filepath.Join(root, "ignored"), 0755)
	os.WriteFile(filepath.Join(root, ".ignore"), []byte("ignored/\nskip.txt\n"), 0600)
	for name, content := range map[string]string{"ignored/file.txt": "needle\n", "skip.txt": "needle\n", "keep.txt": "needle\n", ".hidden": "needle\n"} {
		os.WriteFile(filepath.Join(root, name), []byte(content), 0600)
	}
	r := perform(t, root, "glob", map[string]any{"pattern": "**/*.txt"})
	if !r.OK {
		t.Fatal(r.Error)
	}
	// A positive glob overrides an ignored file but does not enter ignored directories.
	if r.Result.Content != ProjectPath+"/keep.txt\n"+ProjectPath+"/skip.txt" {
		t.Fatalf("glob %q", r.Result.Content)
	}
	r = perform(t, root, "grep", map[string]any{"pattern": "needle", "literal": true})
	if !r.OK {
		t.Fatal(r.Error)
	}
	if !strings.Contains(r.Result.Content, "/.hidden:") || strings.Contains(r.Result.Content, "skip.txt") || strings.Contains(r.Result.Content, "ignored/file") {
		t.Fatalf("grep ignores %q", r.Result.Content)
	}
}
func TestMutationKeepsExecutableModeAndChecksChangedContent(t *testing.T) {
	rootPath := t.TempDir()
	os.WriteFile(filepath.Join(rootPath, "run"), []byte("old\n"), 0755)
	r := perform(t, rootPath, "edit", map[string]any{"path": "run", "oldString": "old", "newString": "new"})
	if !r.OK {
		t.Fatal(r.Error)
	}
	info, _ := os.Stat(filepath.Join(rootPath, "run"))
	if info.Mode().Perm() != 0755 {
		t.Fatal("executable mode lost")
	}
	root, e := securefs.OpenRoot(rootPath)
	if e != nil {
		t.Fatal(e)
	}
	defer root.Close()
	_, commit, e := mutation(root, "edit", input{Path: "run", Old: "new", New: "changed"})
	if e != nil {
		t.Fatal(e)
	}
	os.WriteFile(filepath.Join(rootPath, "run"), []byte("external\n"), 0755)
	if e = commit(); e == nil {
		t.Fatal("stale edit overwrote replacement")
	}
	raw, _ := os.ReadFile(filepath.Join(rootPath, "run"))
	if string(raw) != "external\n" {
		t.Fatal("changed file overwritten")
	}
}
func TestPatchMoveAndDelete(t *testing.T) {
	root := t.TempDir()
	os.WriteFile(filepath.Join(root, "from"), []byte("before\n"), 0600)
	os.WriteFile(filepath.Join(root, "remove"), []byte("remove\n"), 0600)
	r := perform(t, root, "patch", map[string]any{"patchText": "*** Begin Patch\n*** Update File: from\n*** Move to: to\n@@\n-before\n+after\n*** Delete File: remove\n*** End Patch"})
	if !r.OK {
		t.Fatal(r.Error)
	}
	raw, _ := os.ReadFile(filepath.Join(root, "to"))
	if string(raw) != "after\n" {
		t.Fatal("move content mismatch")
	}
	for _, name := range []string{"from", "remove"} {
		if _, e := os.Stat(filepath.Join(root, name)); !os.IsNotExist(e) {
			t.Fatal("source retained")
		}
	}
}

func TestSearchRejectsDifferentRegexSemanticsAndMatchesUnicodeGlob(t *testing.T) {
	root := t.TempDir()
	os.WriteFile(filepath.Join(root, "é.txt"), []byte("é\n"), 0600)
	r := perform(t, root, "glob", map[string]any{"pattern": "é.*"})
	if !r.OK || !strings.Contains(r.Result.Content, "é.txt") {
		t.Fatal("Unicode glob changed")
	}
	r = perform(t, root, "grep", map[string]any{"pattern": `\w`})
	if r.OK || r.Error.Code != "unsupported" {
		t.Fatal("different Unicode regex semantics accepted")
	}
}
