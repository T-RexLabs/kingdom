package workspacearchive

import (
	"bytes"
	"context"
	"fmt"
	"testing"
)

func TestImportRejectsDirectoryCountBeforeAllocatingEntries(t *testing.T) {
	entries := make([]fixture, 4096)
	for i := range entries {
		entries[i] = fixture{name: fmt.Sprintf("file-%05d", i), mode: 0644}
	}
	raw := archiveFor(t, entries...)
	parent := t.TempDir()
	limits := importLimits()
	limits.Entries = 1
	var importErr error
	allocations := testing.AllocsPerRun(1, func() {
		staged, err := ImportZIP(context.Background(), parent, bytes.NewReader(raw), int64(len(raw)), limits)
		importErr = err
		if staged != nil {
			staged.Close()
		}
	})
	if importErr == nil {
		t.Fatal("oversized directory accepted")
	}
	if allocations > 2048 {
		t.Fatalf("ZIP directory allocated before bounded validation: %.0f allocations", allocations)
	}
}

func TestZIPProfileRejectsUnsupportedDirectoryLayouts(t *testing.T) {
	raw := archiveFor(t, fixture{name: "file", data: "data", mode: 0644})
	end := len(raw) - 22
	for _, kind := range []string{"zip64", "split", "trailing", "self-extracting", "wrong-size", "wrong-count", "hidden-records"} {
		t.Run(kind, func(t *testing.T) {
			modified := append([]byte(nil), raw...)
			limits := importLimits()
			switch kind {
			case "zip64":
				modified[end+10] = 255
				modified[end+11] = 255
			case "split":
				modified[end+4] = 1
			case "trailing":
				modified = append(modified, 0)
			case "self-extracting":
				modified = append([]byte("MZ"), modified...)
			case "wrong-size":
				modified[end+12]++
			case "wrong-count":
				modified[end+10] = 0
				modified[end+8] = 0
			case "hidden-records":
				modified = archiveFor(t, fixture{name: "a", mode: 0644}, fixture{name: "b", mode: 0644})
				e := len(modified) - 22
				modified[e+8] = 1
				modified[e+10] = 1
				limits.Entries = 1
			}
			staged, err := ImportZIP(context.Background(), t.TempDir(), bytes.NewReader(modified), int64(len(modified)), limits)
			if staged != nil {
				staged.Close()
			}
			if err == nil {
				t.Fatal("unsupported layout accepted")
			}
		})
	}
	// Ordinary ZIP comments remain valid and do not affect content extraction.
	commented := append([]byte(nil), raw...)
	commented[end+20] = 4
	commented = append(commented, []byte("note")...)
	staged, err := ImportZIP(context.Background(), t.TempDir(), bytes.NewReader(commented), int64(len(commented)), importLimits())
	if err != nil {
		t.Fatal(err)
	}
	staged.Close()
}
