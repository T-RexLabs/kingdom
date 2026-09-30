package runtimefs

import (
	"context"
	"fmt"
	"testing"
)

// Keep both sizes: their ratio distinguishes repeated scans from linear work.
func BenchmarkWorkspaceMetadata(b *testing.B) {
	for _, count := range []int{1000, 10000} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			db := openRuntimeFSTestDB(b)
			applyRuntimeFSMigrations(b, db)
			store, err := NewStore(db, b.TempDir())
			if err != nil {
				b.Fatal(err)
			}
			seedRuntimeFSRun(b, db, "benchmark")
			root := b.TempDir()
			for i := range count {
				writeTestFile(b, root, fmt.Sprintf("file-%06d", i), "")
			}
			ctx := context.Background()
			if _, err := store.CaptureSnapshot(ctx, CaptureSnapshotRequest{SnapshotID: "base", RunID: "benchmark", SnapshotKind: "baseline", RootPath: root, CreatedAt: "2026-03-01T12:00:00Z"}); err != nil {
				b.Fatal(err)
			}
			b.Run("Count", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					if n, err := store.CountChangesAgainstSnapshot(ctx, root, "base"); err != nil || n != 0 {
						b.Fatalf("count=%d error=%v", n, err)
					}
				}
			})
			b.Run("Capture", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					if _, err := store.CaptureSnapshot(ctx, CaptureSnapshotRequest{SnapshotID: fmt.Sprintf("capture-%d", i), RunID: "benchmark", SnapshotKind: "baseline", RootPath: root, CreatedAt: "2026-03-01T12:00:00Z"}); err != nil {
						b.Fatal(err)
					}
				}
			})
			b.Run("Restore", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					if err := store.RestoreSnapshot(ctx, "base", root); err != nil {
						b.Fatal(err)
					}
				}
			})
			b.Run("Diff", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					visited := 0
					if err := store.VisitSnapshotDiff(ctx, "", "base", func(SnapshotDiffEntry) error { visited++; return nil }); err != nil || visited != count {
						b.Fatalf("diff count=%d error=%v", visited, err)
					}
				}
			})
		})
	}
}
