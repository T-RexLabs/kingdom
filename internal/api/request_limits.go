package api

import (
	"net/http"
	"strings"

	"kingdom/internal/core/errorcatalog"
)

const (
	maxAPIRequestBodyBytes          int64 = 1 << 20
	// An issued external decree carries the retained issue/comments rendered as
	// attributed Markdown and JSON-encoded inside the request. This route budget
	// fits the maximum stored source representation; all other routes keep the
	// smaller general request-body bound.
	maxDecreeCreateRequestBodyBytes int64 = 16 << 20
)

func requestBodyLimitForRequest(r *http.Request) int64 {
	if r != nil && r.Method == http.MethodPost && strings.HasSuffix(strings.TrimRight(r.URL.Path, "/"), "/decrees") {
		return maxDecreeCreateRequestBodyBytes
	}
	return maxAPIRequestBodyBytes
}

func limitRequestBody(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		limit := requestBodyLimitForRequest(r)
		if r.ContentLength > limit {
			writeRequestBodyTooLargeForLimit(w, limit)
			return
		}

		r.Body = http.MaxBytesReader(w, r.Body, limit)
		next.ServeHTTP(w, r)
	})
}

func writeRequestBodyTooLarge(w http.ResponseWriter) {
	writeRequestBodyTooLargeForLimit(w, maxAPIRequestBodyBytes)
}

func writeRequestBodyTooLargeForLimit(w http.ResponseWriter, limit int64) {
	message := "request body exceeds the 1 MiB limit"
	if limit == maxDecreeCreateRequestBodyBytes {
		message = "decree request body exceeds the 16 MiB limit"
	}
	writeCanonicalError(
		w,
		http.StatusRequestEntityTooLarge,
		errorcatalog.CodePolicyViolation,
		message,
		"api.request_body_limit",
		map[string]any{"limit_bytes": limit},
	)
}
