package verdict

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

const (
	// ReworkReasonCodeExecutionReview and ReworkReasonCodeValidationGate are the
	// failure codes an owner node writes when it asks for changes instead of
	// failing. They are the only signal that separates "this step asked for
	// another pass" from "this step broke", so every surface that draws that
	// distinction resolves it through IsReworkReasonCode rather than by
	// spelling the codes out again.
	ReworkReasonCodeExecutionReview = "execution_review_rework"
	ReworkReasonCodeValidationGate  = "validation_gate_rework"
)

// IsReworkReasonCode reports whether a failure code records requested rework.
func IsReworkReasonCode(reasonCode string) bool {
	switch strings.ToLower(strings.TrimSpace(reasonCode)) {
	case ReworkReasonCodeExecutionReview, ReworkReasonCodeValidationGate:
		return true
	default:
		return false
	}
}

var (
	ErrMultipleVerdicts    = errors.New("multiple verdict lines")
	ErrConflictingVerdicts = errors.New("conflicting verdict lines")
	controlVerdictPattern  = regexp.MustCompile(`(?i)^VERDICT:\s*(PASS|REWORK)\s*$`)
)

type VerdictContractError struct {
	ReasonCode string
	message    string
}

func (e *VerdictContractError) Error() string {
	if e == nil {
		return "verdict contract error"
	}
	return e.message
}

func FindNearTop(outputText string) (string, bool, error) {
	verdict := ""
	firstNonEmptySeen := false
	for _, rawLine := range strings.Split(outputText, "\n") {
		line := strings.TrimSpace(rawLine)
		if line == "" {
			continue
		}
		match := controlVerdictPattern.FindStringSubmatch(line)
		if !firstNonEmptySeen {
			firstNonEmptySeen = true
			if len(match) != 2 {
				return "", false, nil
			}
		}
		if len(match) != 2 {
			continue
		}
		candidate := strings.ToUpper(strings.TrimSpace(match[1]))
		if verdict != "" && verdict != candidate {
			return "", false, ErrConflictingVerdicts
		}
		if verdict != "" {
			return "", false, ErrMultipleVerdicts
		}
		verdict = candidate
	}
	if verdict == "" {
		return "", false, nil
	}
	return verdict, true, nil
}

func ExtractNearTop(outputText string) string {
	verdict, found, err := FindNearTop(outputText)
	if err != nil || !found {
		return ""
	}
	return verdict
}

func ParseRequiredNearTop(outputText, ownerLabel, invalidReasonCode string) (string, error) {
	resolvedVerdict, found, err := FindNearTop(outputText)
	if err != nil {
		return "", &VerdictContractError{
			ReasonCode: strings.TrimSpace(invalidReasonCode),
			message:    fmt.Sprintf("%s response contains invalid verdict lines", strings.TrimSpace(ownerLabel)),
		}
	}
	if !found {
		return "", &VerdictContractError{
			ReasonCode: strings.TrimSpace(invalidReasonCode),
			message:    fmt.Sprintf("%s response missing required verdict line near top: VERDICT: PASS|REWORK", strings.TrimSpace(ownerLabel)),
		}
	}
	return resolvedVerdict, nil
}
