package orchestration

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"kingdom/internal/securefs"
)

// BenchmarkPrivateSourcePreparation exercises the same preparation used by
// response-only roles. Setup and fixture disposal are excluded from timing.
// Each file is 16 KiB, close to the observed multi-repository run's mean size.
func BenchmarkPrivateSourcePreparation(b *testing.B) {
	for _, count := range []int{1000, 5000} {
		b.Run(fmt.Sprintf("Files%d", count), func(b *testing.B) {
			seed := b.TempDir()
			payload := make([]byte, 16*1024)
			for i := range payload {
				payload[i] = byte(i*31 + 17)
			}
			for i := 0; i < count; i++ {
				name := filepath.Join(seed, fmt.Sprintf("repo%d/packages/pkg%d/src/components/file%05d.ts", i%6, (i/6)%25, i))
				if err := os.MkdirAll(filepath.Dir(name), 0o755); err != nil {
					b.Fatal(err)
				}
				if err := os.WriteFile(name, payload, 0o644); err != nil {
					b.Fatal(err)
				}
			}
			for _, scenario := range []struct {
				name    string
				copies  int
				cleanup int
			}{
				{name: "Single", copies: 1},
				{name: "FourParallel", copies: 4},
				{name: "FourWithDurableCleanup", copies: 4, cleanup: 1},
				{name: "SingleWithFourDurableCleanup", copies: 1, cleanup: 4},
			} {
				b.Run(scenario.name, func(b *testing.B) {
					ctx := context.Background()
					loop := &orchestrationLoop{}
					workspace := executionWorkspaceContext{SeedRoot: seed, WorkspaceKind: workspacePrivateSource}
					var copyElapsed, cleanupElapsed time.Duration
					b.ReportAllocs()
					b.SetBytes(int64(count * len(payload) * scenario.copies))
					for iteration := 0; iteration < b.N; iteration++ {
						b.StopTimer()
						parent := b.TempDir()
						var cleanupRoot *securefs.Root
						if scenario.cleanup > 0 {
							for i := 0; i < scenario.cleanup; i++ {
								obsolete := filepath.Join(parent, fmt.Sprintf("obsolete%d", i))
								if err := os.Mkdir(obsolete, 0o700); err != nil {
									b.Fatal(err)
								}
								if err := loop.prepareExecutionWorkspaceContext(ctx, obsolete, workspace); err != nil {
									b.Fatal(err)
								}
							}
							var err error
							cleanupRoot, err = securefs.OpenRoot(parent)
							if err != nil {
								b.Fatal(err)
							}
						}
						destinations := make([]string, scenario.copies)
						for i := range destinations {
							destinations[i] = filepath.Join(parent, fmt.Sprintf("copy%d", i))
							if err := os.Mkdir(destinations[i], 0o700); err != nil {
								b.Fatal(err)
							}
						}
						start := make(chan struct{})
						errors := make(chan error, scenario.copies+scenario.cleanup)
						var copies, cleanup sync.WaitGroup
						copies.Add(scenario.copies)
						for _, destination := range destinations {
							go func() {
								defer copies.Done()
								<-start
								errors <- loop.prepareExecutionWorkspaceContext(ctx, destination, workspace)
							}()
						}
						if cleanupRoot != nil {
							cleanup.Add(scenario.cleanup)
							for i := 0; i < scenario.cleanup; i++ {
								go func() {
									defer cleanup.Done()
									<-start
									errors <- cleanupRoot.RemoveTree(ctx, fmt.Sprintf("obsolete%d", i), 128)
								}()
							}
						}
						cleanupComplete := make(chan struct{})
						go func() {
							<-start
							began := time.Now()
							cleanup.Wait()
							if scenario.cleanup > 0 {
								cleanupElapsed += time.Since(began)
							}
							close(cleanupComplete)
						}()
						b.StartTimer()
						began := time.Now()
						close(start)
						copies.Wait()
						copyElapsed += time.Since(began)
						<-cleanupComplete
						b.StopTimer()
						close(errors)
						for err := range errors {
							if err != nil {
								b.Fatal(err)
							}
						}
						if cleanupRoot != nil {
							cleanupRoot.Close()
						}
						if err := os.RemoveAll(parent); err != nil {
							b.Fatal(err)
						}
					}
					b.ReportMetric(copyElapsed.Seconds()/float64(b.N), "copy-s/op")
					if scenario.cleanup > 0 {
						b.ReportMetric(cleanupElapsed.Seconds()/float64(b.N), "cleanup-s/op")
					}
				})
			}
		})
	}
}
