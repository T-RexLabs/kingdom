package securefs

import (
	"context"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"
)

func BenchmarkPrivateWorkspaceWrites(b *testing.B) {
	for _, private := range []bool{false, true} {
		name := "durable"
		if private {
			name = "materialized"
		}
		for _, count := range []int{1000, 5000} {
			b.Run(fmt.Sprintf("%s/%d", name, count), func(b *testing.B) {
				body := strings.Repeat("x", 1024)
				paths := make([]string, count)
				for index := range paths {
					paths[index] = fmt.Sprintf("dir-%03d/file-%05d", index/100, index)
				}
				b.SetBytes(int64(count * len(body)))
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					b.StopTimer()
					root, err := OpenRoot(b.TempDir())
					if err != nil {
						b.Fatal(err)
					}
					write := root.ReplaceRegularFrom
					if private {
						write = root.MaterializeRegularFrom
					}
					b.StartTimer()
					for _, path := range paths {
						if err := write(context.Background(), path, func(w io.Writer) error {
							_, err := io.WriteString(w, body)
							return err
						}, 0o644, 0o755, time.Unix(1700000000, 0)); err != nil {
							b.Fatal(err)
						}
					}
					b.StopTimer()
					if err := root.Close(); err != nil {
						b.Fatal(err)
					}
					b.StartTimer()
				}
			})
		}
	}
}
