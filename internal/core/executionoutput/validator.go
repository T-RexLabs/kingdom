package executionoutput

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"kingdom/internal/core/errorcatalog"
	"kingdom/internal/core/verdict"
	"kingdom/internal/core/workflow"
)

type Result struct {
	CanonicalText         string
	StructuredPayloadJSON string
	WorkflowFailureCode   string
	WorkflowFailureText   string
}

type ValidationError struct {
	ReasonCode string
	message    string
}

func (e *ValidationError) Error() string {
	if e == nil {
		return "invalid execution output"
	}
	return e.message
}

func Validate(nodeID, nodeClass, finalText, structuredPayloadJSON string) (Result, error) {
	result, err := ValidateReader(nodeID, nodeClass, strings.NewReader(finalText), structuredPayloadJSON)
	if err != nil {
		return Result{}, err
	}
	result.CanonicalText = strings.TrimSpace(finalText)
	return result, nil
}

func ValidateReader(nodeID, nodeClass string, reader io.Reader, structuredPayloadJSON string) (Result, error) {
	scan, err := verdict.ScanReader(reader)
	if err != nil {
		return Result{}, &ValidationError{ReasonCode: string(errorcatalog.CodeLLMInvalidResponse), message: fmt.Sprintf("read provider response: %v", err)}
	}
	if !scan.NonEmpty {
		return Result{}, &ValidationError{ReasonCode: "provider_empty_output", message: "provider returned an empty terminal response"}
	}

	structured, err := decodeStructuredPayload(structuredPayloadJSON)
	if err != nil {
		return Result{}, err
	}
	result := Result{}

	switch workflow.ResolveNodeClass(nodeID, nodeClass) {
	case workflow.NodeClassExecutionReview:
		if err := applyVerdict(&result, structured, scan, "execution review", string(errorcatalog.CodeLLMInvalidResponse), verdict.ReworkReasonCodeExecutionReview, "review requested rework"); err != nil {
			return Result{}, err
		}
	case workflow.NodeClassValidationGates:
		if err := applyVerdict(&result, structured, scan, "validation", string(errorcatalog.CodeLLMInvalidResponse), verdict.ReworkReasonCodeValidationGate, "validation requested rework"); err != nil {
			return Result{}, err
		}
	}

	raw, err := json.Marshal(structured)
	if err != nil {
		return Result{}, &ValidationError{ReasonCode: string(errorcatalog.CodeLLMInvalidResponse), message: fmt.Sprintf("canonicalize provider structured output: %v", err)}
	}
	result.StructuredPayloadJSON = string(raw)
	return result, nil
}

func decodeStructuredPayload(raw string) (map[string]any, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return map[string]any{}, nil
	}
	var structured map[string]any
	if err := json.Unmarshal([]byte(trimmed), &structured); err != nil {
		return nil, &ValidationError{ReasonCode: string(errorcatalog.CodeLLMInvalidResponse), message: fmt.Sprintf("decode provider structured output: %v", err)}
	}
	if structured == nil {
		return nil, &ValidationError{ReasonCode: string(errorcatalog.CodeLLMInvalidResponse), message: "provider structured output must be an object"}
	}
	return structured, nil
}

func applyVerdict(result *Result, structured map[string]any, scan verdict.StreamResult, ownerLabel, invalidReasonCode, reworkReasonCode, reworkReasonText string) error {
	if scan.Invalid != nil {
		return &ValidationError{ReasonCode: invalidReasonCode, message: fmt.Sprintf("%s response contains invalid verdict lines", ownerLabel)}
	}
	if !scan.Found {
		return &ValidationError{ReasonCode: invalidReasonCode, message: fmt.Sprintf("%s response missing required verdict line near top: VERDICT: PASS|REWORK", ownerLabel)}
	}
	resolved := scan.Verdict
	if claimed, exists := structured["verdict"]; exists {
		claim, ok := claimed.(string)
		if !ok || !strings.EqualFold(strings.TrimSpace(claim), resolved) {
			return &ValidationError{ReasonCode: invalidReasonCode, message: fmt.Sprintf("%s structured verdict conflicts with final text", ownerLabel)}
		}
	}
	structured["verdict"] = resolved
	if resolved == "REWORK" {
		result.WorkflowFailureCode = reworkReasonCode
		result.WorkflowFailureText = reworkReasonText
	}
	return nil
}
