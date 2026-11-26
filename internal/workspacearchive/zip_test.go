package workspacearchive

import (
	"archive/zip"
	"bytes"
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type fixture struct {
	name, data string
	mode       fs.FileMode
}

func archiveFor(t *testing.T, entries ...fixture) []byte {
	t.Helper()
	var buffer bytes.Buffer
	w := zip.NewWriter(&buffer)
	for _, entry := range entries {
		header := &zip.FileHeader{Name: entry.name, Method: zip.Store}
		if entry.mode != 0 {
			header.SetMode(entry.mode)
		}
		writer, err := w.CreateHeader(header)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := writer.Write([]byte(entry.data)); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buffer.Bytes()
}

func importLimits() Limits {
	return Limits{ArchiveBytes: 1 << 20, ExpandedBytes: 1024, FileBytes: 512, Entries: 32, Depth: 8, PathBytes: 128}
}

func TestImportRetainsInputAndDeterministicManifest(t *testing.T) {
	data := archiveFor(t, fixture{"src/main.go", "package main", 0644}, fixture{"empty/", "", fs.ModeDir | 0755}, fixture{"bin/run", "#!/bin/sh\n", 0755}, fixture{"src/", "", fs.ModeDir | 0755})
	parent := t.TempDir()
	first, err := ImportZIP(context.Background(), parent, bytes.NewReader(data), int64(len(data)), importLimits())
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := ImportZIP(context.Background(), parent, bytes.NewReader(data), int64(len(data)), importLimits())
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	if first.Path == second.Path || first.Digest != second.Digest || len(first.Manifest.Entries) != 5 {
		t.Fatalf("invalid manifest: %+v", first.Manifest)
	}
	content, err := os.ReadFile(filepath.Join(first.Path, "src/main.go"))
	if err != nil || string(content) != "package main" {
		t.Fatalf("content: %q %v", content, err)
	}
	info, err := os.Stat(filepath.Join(first.Path, "bin/run"))
	if err != nil || info.Mode().Perm() != 0755 {
		t.Fatalf("executable mode: %v %v", info, err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(first.Path); !os.IsNotExist(err) {
		t.Fatalf("staging retained: %v", err)
	}
}

func TestImportRejectsUnsafePathsWithoutResidue(t *testing.T) {
	cases := [][]fixture{
		{{"../escape", "x", 0644}}, {{"/escape", "x", 0644}}, {{"a/../../escape", "x", 0644}}, {{"a//b", "x", 0644}}, {{"a/./b", "x", 0644}}, {{"a\\b", "x", 0644}}, {{"C:evil", "x", 0644}},
		{{".git/hooks/post-checkout", "x", 0755}}, {{"a/.KINGDOM/state", "x", 0644}}, {{"NUL.txt", "x", 0644}}, {{"a. /b", "x", 0644}}, {{"a\x00b", "x", 0644}}, {{"e\u0301.txt", "x", 0644}},
		{{"link", "outside", fs.ModeSymlink | 0777}}, {{"fifo", "", fs.ModeNamedPipe | 0600}}, {{"setuid", "x", fs.ModeSetuid | 0755}},
		{{"a", "x", 0644}, {"a/b", "x", 0644}}, {{"a/b", "x", 0644}, {"a", "x", 0644}}, {{"a", "x", 0644}, {"a", "x", 0644}}, {{"A/b", "x", 0644}, {"a/c", "x", 0644}}, {{"a/", "", fs.ModeDir | 0755}, {"a/", "", fs.ModeDir | 0755}},
	}
	for _, fixtures := range cases {
		t.Run(fixtures[0].name, func(t *testing.T) {
			parent := t.TempDir()
			data := archiveFor(t, fixtures...)
			staged, err := ImportZIP(context.Background(), parent, bytes.NewReader(data), int64(len(data)), importLimits())
			if err == nil {
				staged.Close()
				t.Fatal("unsafe input accepted")
			}
			files, err := os.ReadDir(parent)
			if err != nil || len(files) != 0 {
				t.Fatalf("failed import left files: %v %v", files, err)
			}
		})
	}
}

func TestImportLimitsIncludeImplicitDirectories(t *testing.T) {
	for _, test := range []struct {
		name    string
		entries []fixture
		change  func(*Limits)
	}{
		{"archive", []fixture{{"a", "x", 0644}}, func(l *Limits) { l.ArchiveBytes = 1 }},
		{"file", []fixture{{"a", "abc", 0644}}, func(l *Limits) { l.FileBytes = 2 }},
		{"total", []fixture{{"a", "abc", 0644}, {"b", "abc", 0644}}, func(l *Limits) { l.ExpandedBytes = 5 }},
		{"implicit entries", []fixture{{"a/b/c", "x", 0644}}, func(l *Limits) { l.Entries = 2 }},
		{"depth", []fixture{{"a/b/c", "x", 0644}}, func(l *Limits) { l.Depth = 2 }},
		{"path", []fixture{{"abcdef", "x", 0644}}, func(l *Limits) { l.PathBytes = 5 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			limits := importLimits()
			test.change(&limits)
			data := archiveFor(t, test.entries...)
			staged, err := ImportZIP(context.Background(), t.TempDir(), bytes.NewReader(data), int64(len(data)), limits)
			if err == nil {
				staged.Close()
				t.Fatal("limit not enforced")
			}
		})
	}
}

func TestCorruptionAndTruncationFailAndCleanUp(t *testing.T) {
	data := archiveFor(t, fixture{"a", "unique payload", 0644})
	pos := bytes.Index(data, []byte("unique payload"))
	if pos < 0 {
		t.Fatal("fixture content missing")
	}
	data[pos] ^= 1
	for _, size := range []int64{int64(len(data)), int64(len(data)) + 1} {
		parent := t.TempDir()
		staged, err := ImportZIP(context.Background(), parent, bytes.NewReader(data), size, importLimits())
		if err == nil {
			staged.Close()
			t.Fatal("corrupt archive accepted")
		}
		files, _ := os.ReadDir(parent)
		if len(files) != 0 {
			t.Fatal("corrupt import retained")
		}
	}
}

func TestCancelledImportLeavesNoFiles(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	parent := t.TempDir()
	data := archiveFor(t, fixture{"a", strings.Repeat("a", 100), 0644})
	if _, err := ImportZIP(ctx, parent, bytes.NewReader(data), int64(len(data)), importLimits()); err != context.Canceled {
		t.Fatalf("got %v", err)
	}
	files, _ := os.ReadDir(parent)
	if len(files) != 0 {
		t.Fatal("cancelled import retained")
	}
}
