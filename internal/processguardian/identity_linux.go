//go:build linux

package processguardian

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

func processIdentityToken(pid int) (string, error) {
	bootID, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return "", fmt.Errorf("read linux boot id: %w", err)
	}
	stat, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return "", fmt.Errorf("read process stat: %w", err)
	}
	closing := strings.LastIndex(string(stat), ")")
	if closing < 0 {
		return "", fmt.Errorf("invalid process stat")
	}
	fields := strings.Fields(string(stat[closing+1:]))
	if len(fields) <= 19 {
		return "", fmt.Errorf("incomplete process stat")
	}
	startTicks, err := strconv.ParseUint(fields[19], 10, 64)
	if err != nil || startTicks == 0 {
		return "", fmt.Errorf("invalid process start ticks")
	}
	return strings.TrimSpace(string(bootID)) + ":" + strconv.FormatUint(startTicks, 10), nil
}
