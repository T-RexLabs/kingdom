package executionoutput

import (
	"encoding/json"
	"fmt"
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
	canonicalText := strings.TrimSpace(finalText)
	if canonicalText == "" {
		return Result{}, &ValidationError{ReasonCode: "provider_empty_output", message: "provider returned an empty terminal response"}
	}

	structured, err := decodeStructuredPayload(structuredPayloadJSON)
	if err != nil {
		return Result{}, err
	}
	result := Result{CanonicalText: canonicalText}

	switch workflow.ResolveNodeClass(nodeID, nodeClass) {
	case workflow.NodeClassExecutionReview:
		if err := applyVerdict(&result, structured, canonicalText, "execution review", string(errorcatalog.CodeLLMInvalidResponse), verdict.ReworkReasonCodeExecutionReview, "review requested rework"); err != nil {
			return Result{}, err
		}
	case workflow.NodeClassValidationGates:
		if err := applyVerdict(&result, structured, canonicalText, "validation", string(errorcatalog.CodeLLMInvalidResponse), verdict.ReworkReasonCodeValidationGate, "validation requested rework"); err != nil {
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

func applyVerdict(result *Result, structured map[string]any, finalText, ownerLabel, invalidReasonCode, reworkReasonCode, reworkReasonText string) error {
	resolved, err := verdict.ParseRequiredNearTop(finalText, ownerLabel, invalidReasonCode)
	if err != nil {
		return &ValidationError{ReasonCode: invalidReasonCode, message: err.Error()}
	}
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
