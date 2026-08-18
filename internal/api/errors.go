package api

import (
	"encoding/json"
	"net/http"
	"time"

	"kingdom/internal/core/errorcatalog"
)

type canonicalErrorEnvelope struct {
	Error canonicalErrorPayload `json:"error"`
}

type canonicalErrorPayload struct {
	Code          errorcatalog.Code     `json:"code"`
	Message       string                `json:"message"`
	Retryable     bool                  `json:"retryable"`
	Category      errorcatalog.Category `json:"category"`
	Details       map[string]any        `json:"details"`
	CorrelationID string                `json:"correlation_id"`
	OccurredAt    time.Time             `json:"occurred_at"`
	RetryAt       string                `json:"retry_at,omitempty"`
}

func writeCanonicalError(w http.ResponseWriter, statusCode int, code errorcatalog.Code, message string, correlationID string, details map[string]any) {
	writeCanonicalErrorWithRetryAt(w, statusCode, code, message, correlationID, details, nil)
}

func writeCanonicalErrorWithRetryAt(w http.ResponseWriter, statusCode int, code errorcatalog.Code, message string, correlationID string, details map[string]any, retryAt *time.Time) {
	envelope := buildCanonicalErrorEnvelope(code, message, correlationID, details)
	if retryAt != nil {
		value := retryAt.UTC()
		if !value.IsZero() && value.Year() >= 1 && value.Year() <= 9999 {
			envelope.Error.RetryAt = value.Format(time.RFC3339)
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(envelope)
}

func buildCanonicalErrorEnvelope(code errorcatalog.Code, message string, correlationID string, details map[string]any) canonicalErrorEnvelope {
	profile, ok := errorcatalog.Lookup(code)
	if !ok {
		clonedDetails := cloneErrorDetails(details)
		clonedDetails["requested_code"] = string(code)
		code = errorcatalog.CodeInternalError
		profile, _ = errorcatalog.Lookup(code)
		details = clonedDetails
	} else {
		details = cloneErrorDetails(details)
	}

	if correlationID == "" {
		correlationID = "api"
	}

	return canonicalErrorEnvelope{
		Error: canonicalErrorPayload{
			Code:          code,
			Message:       message,
			Retryable:     profile.Retryable,
			Category:      profile.Category,
			Details:       details,
			CorrelationID: correlationID,
			OccurredAt:    time.Now().UTC(),
		},
	}
}

func cloneErrorDetails(details map[string]any) map[string]any {
	if len(details) == 0 {
		return map[string]any{}
	}
	cloned := make(map[string]any, len(details))
	for key, value := range details {
		cloned[key] = value
	}
	return cloned
}
