package verdict

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"kingdom/internal/core/workflow"
)

const MaxControlVerdictLines = 12

var (
	ErrMultipleVerdicts    = errors.New("multiple verdict lines")
	ErrConflictingVerdicts = errors.New("conflicting verdict lines")
	controlVerdictPattern  = regexp.MustCompile(`(?i)^VERDICT:\s*(PASS|REWORK)\s*$`)
)

type ReceiptDecision struct {
	Verdict           string
	FailureReasonCode string
	FailureReasonText string
}

type ReceiptContractError struct {
	ReasonCode string
	message    string
}

func (e *ReceiptContractError) Error() string {
	if e == nil {
		return "receipt contract error"
	}
	return e.message
}

func FindNearTop(outputText string) (string, bool, error) {
	verdict := ""
	nonEmptySeen := 0
	for _, rawLine := range strings.Split(outputText, "\n") {
		line := strings.TrimSpace(rawLine)
		if line == "" {
			continue
		}
		nonEmptySeen++
		if nonEmptySeen > MaxControlVerdictLines {
			break
		}
		match := controlVerdictPattern.FindStringSubmatch(line)
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
		return "", &ReceiptContractError{
			ReasonCode: strings.TrimSpace(invalidReasonCode),
			message:    fmt.Sprintf("%s response contains invalid verdict lines", strings.TrimSpace(ownerLabel)),
		}
	}
	if !found {
		return "", &ReceiptContractError{
			ReasonCode: strings.TrimSpace(invalidReasonCode),
			message:    fmt.Sprintf("%s response missing required verdict line near top: VERDICT: PASS|REWORK", strings.TrimSpace(ownerLabel)),
		}
	}
	return resolvedVerdict, nil
}

func ReceiptDecisionForNode(nodeID, nodeClass, structuredPayloadJSON, finalText string) (ReceiptDecision, bool, error) {
	switch workflow.ResolveNodeClass(nodeID, nodeClass) {
	case workflow.NodeClassExecutionReview:
		decision, err := receiptDecisionForOwner(structuredPayloadJSON, finalText, "execution review", "invalid_execution_review_verdict", "execution_review_rework", "review requested rework")
		return decision, true, err
	case workflow.NodeClassValidationGates:
		decision, err := receiptDecisionForOwner(structuredPayloadJSON, finalText, "validation", "invalid_validation_verdict", "validation_gate_rework", "validation requested rework")
		return decision, true, err
	default:
		return ReceiptDecision{}, false, nil
	}
}

func receiptDecisionForOwner(structuredPayloadJSON, finalText, ownerLabel, invalidReasonCode, reworkReasonCode, reworkReasonText string) (ReceiptDecision, error) {
	resolvedVerdict, err := settledReceiptVerdict(structuredPayloadJSON, finalText, ownerLabel, invalidReasonCode)
	if err != nil {
		return ReceiptDecision{}, err
	}
	decision := ReceiptDecision{Verdict: resolvedVerdict}
	if resolvedVerdict == "REWORK" {
		decision.FailureReasonCode = strings.TrimSpace(reworkReasonCode)
		decision.FailureReasonText = strings.TrimSpace(reworkReasonText)
	}
	return decision, nil
}

func settledReceiptVerdict(structuredPayloadJSON, finalText, ownerLabel, invalidReasonCode string) (string, error) {
	trimmedStructured := strings.TrimSpace(structuredPayloadJSON)
	if trimmedStructured != "" {
		var payload struct {
			Verdict string `json:"verdict"`
		}
		if err := json.Unmarshal([]byte(trimmedStructured), &payload); err != nil {
			return "", &ReceiptContractError{
				ReasonCode: strings.TrimSpace(invalidReasonCode),
				message:    fmt.Sprintf("decode %s completion receipt structured verdict: %v", strings.TrimSpace(ownerLabel), err),
			}
		}
		resolvedVerdict := strings.ToUpper(strings.TrimSpace(payload.Verdict))
		if resolvedVerdict == "PASS" || resolvedVerdict == "REWORK" {
			return resolvedVerdict, nil
		}
	}
	return ParseRequiredNearTop(finalText, ownerLabel, invalidReasonCode)
}
