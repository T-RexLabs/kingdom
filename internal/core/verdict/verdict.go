package verdict

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
	"unicode"
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
	result, err := ScanReader(strings.NewReader(outputText))
	if err != nil {
		return "", false, err
	}
	if result.Invalid != nil {
		return "", false, result.Invalid
	}
	return result.Verdict, result.Found, nil
}

type StreamResult struct {
	Verdict         string
	Found, NonEmpty bool
	Invalid         error
}

// ScanReader validates all lines, including duplicate controls arbitrarily far
// into the response. Whitespace runs and non-control lines need constant space.
// It drains even after a semantic failure so a verifying reader can prove EOF.
func ScanReader(source io.Reader) (StreamResult, error) {
	r := bufio.NewReaderSize(source, 64*1024)
	var result StreamResult
	var line strings.Builder
	pending, ascii, overflow, firstBad := false, true, false, false
	finish := func() {
		if line.Len() == 0 && !overflow {
			pending = false
			ascii = true
			return
		}
		match := controlVerdictPattern.FindStringSubmatch(line.String())
		if overflow {
			match = nil
		}
		if !result.NonEmpty {
			result.NonEmpty = true
			firstBad = len(match) != 2
		}
		if !firstBad && result.Invalid == nil && len(match) == 2 {
			candidate := strings.ToUpper(match[1])
			if result.Found {
				if candidate != result.Verdict {
					result.Invalid = ErrConflictingVerdicts
				} else {
					result.Invalid = ErrMultipleVerdicts
				}
			} else {
				result.Found = true
				result.Verdict = candidate
			}
		}
		line.Reset()
		pending = false
		ascii = true
		overflow = false
	}
	for {
		ch, _, err := r.ReadRune()
		if err == io.EOF {
			finish()
			break
		}
		if err != nil {
			return result, err
		}
		if ch == '\n' {
			finish()
			continue
		}
		if overflow {
			continue
		}
		if unicode.IsSpace(ch) {
			if line.Len() > 0 {
				pending = true
				ascii = ascii && (ch == ' ' || ch == '\t' || ch == '\r' || ch == '\f')
			}
			continue
		}
		if pending {
			if ascii {
				line.WriteByte(' ')
			} else {
				line.WriteRune('\u00a0')
			}
			pending = false
			ascii = true
		}
		line.WriteRune(ch)
		if line.Len() > 64 {
			overflow = true
		}
	}
	return result, nil
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
