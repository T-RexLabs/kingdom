package processguardian

import (
	"fmt"
	"strings"
)

const processIdentityEnvironmentName = "KINGDOM_PROCESS_INSTANCE_TOKEN"

// ProcessIdentityToken returns the platform's best available token for the
// current process generation. Linux guarded commands include a random launch
// marker so same-tick PID reuse remains distinguishable.
func ProcessIdentityToken(pid int) (string, error) {
	if pid <= 0 {
		return "", fmt.Errorf("process id must be positive")
	}
	return processIdentityToken(pid)
}

// ProcessIdentityMatches reports whether pid still identifies the process
// generation represented by expected. A process that has exited is a clean
// mismatch; errors that prevent identity inspection remain errors.
func ProcessIdentityMatches(pid int, expected string) (bool, error) {
	expected = strings.TrimSpace(expected)
	if pid <= 0 || expected == "" {
		return false, nil
	}
	actual, err := processIdentityToken(pid)
	if err != nil {
		if processIdentityNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return actual == expected, nil
}

func processIdentityEnvironmentValue(environment []string) string {
	for _, entry := range environment {
		name, value, ok := strings.Cut(entry, "=")
		if ok && name == processIdentityEnvironmentName {
			return value
		}
	}
	return ""
}

func withProcessIdentityEnvironment(environment []string, token string) []string {
	result := make([]string, 0, len(environment)+1)
	for _, entry := range environment {
		name, _, _ := strings.Cut(entry, "=")
		if name != processIdentityEnvironmentName {
			result = append(result, entry)
		}
	}
	if token != "" {
		result = append(result, processIdentityEnvironmentName+"="+token)
	}
	return result
}
