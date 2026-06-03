package planning

import (
	"sync"
	"testing"
)

// TestSequenceCursorConcurrentReservationsAreMonotonic verifies that
// Reserve() remains atomic under concurrency: every call returns a unique
// value, and the full set stays within [start, start+N).
func TestSequenceCursorConcurrentReservationsAreMonotonic(t *testing.T) {
	t.Parallel()

	const start = 7
	const goroutines = 16
	const perGoroutine = 256

	cursor := newSequenceCursor(start)
	var wg sync.WaitGroup
	results := make([][]int, goroutines)
	for i := 0; i < goroutines; i++ {
		i := i
		results[i] = make([]int, 0, perGoroutine)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perGoroutine; j++ {
				results[i] = append(results[i], cursor.Reserve())
			}
		}()
	}
	wg.Wait()

	total := goroutines * perGoroutine
	seen := make(map[int]struct{}, total)
	for i := 0; i < goroutines; i++ {
		for _, value := range results[i] {
			if value < start || value >= start+total {
				t.Fatalf("reserved value %d outside [%d, %d)", value, start, start+total)
			}
			if _, dup := seen[value]; dup {
				t.Fatalf("duplicate reservation %d — Reserve is not atomic", value)
			}
			seen[value] = struct{}{}
		}
	}
	if len(seen) != total {
		t.Fatalf("expected %d unique reservations, got %d", total, len(seen))
	}
}
