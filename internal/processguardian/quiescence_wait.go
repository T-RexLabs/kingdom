package processguardian

import (
	"fmt"
	"time"
)

const processQuiescencePollInterval = 10 * time.Millisecond

// waitForProcessesQuiescent polls an OS process-group/job count until it is
// empty or the termination budget expires. A leader's exit is not proof that
// its descendants have completed termination.
func waitForProcessesQuiescent(timeout time.Duration, active func() (uint32, error)) (uint32, error) {
	if active == nil {
		return 0, fmt.Errorf("process activity query is required")
	}
	deadline := time.Now().Add(timeout)
	for {
		count, err := active()
		if err != nil || count == 0 {
			return count, err
		}
		remaining := time.Until(deadline)
		if timeout <= 0 || remaining <= 0 {
			return count, nil
		}
		delay := processQuiescencePollInterval
		if remaining < delay {
			delay = remaining
		}
		timer := time.NewTimer(delay)
		<-timer.C
	}
}
