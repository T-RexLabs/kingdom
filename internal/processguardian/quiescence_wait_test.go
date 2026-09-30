package processguardian

import (
	"errors"
	"testing"
	"time"
)

func TestWaitForProcessesQuiescentWaitsForDescendantsToExit(t *testing.T) {
	var calls int
	active := func() (uint32, error) {
		calls++
		if calls < 3 {
			return 2, nil
		}
		return 0, nil
	}

	count, err := waitForProcessesQuiescent(time.Second, active)
	if err != nil || count != 0 || calls != 3 {
		t.Fatalf("quiescence count=%d calls=%d err=%v, want empty after three observations", count, calls, err)
	}
}

func TestWaitForProcessesQuiescentPreservesActiveCountAtDeadline(t *testing.T) {
	var calls int
	count, err := waitForProcessesQuiescent(25*time.Millisecond, func() (uint32, error) {
		calls++
		return 1, nil
	})
	if err != nil || count != 1 || calls < 2 {
		t.Fatalf("timed quiescence count=%d calls=%d err=%v, want one remaining process after polling", count, calls, err)
	}
}

func TestWaitForProcessesQuiescentReturnsQueryError(t *testing.T) {
	want := errors.New("job query failed")
	count, err := waitForProcessesQuiescent(time.Second, func() (uint32, error) {
		return 1, want
	})
	if count != 1 || !errors.Is(err, want) {
		t.Fatalf("quiescence query count=%d err=%v, want query error", count, err)
	}
}
