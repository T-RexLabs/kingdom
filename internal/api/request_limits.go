package api

import (
	"net/http"

	"kingdom/internal/core/errorcatalog"
)

const maxAPIRequestBodyBytes int64 = 1 << 20

func limitRequestBody(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ContentLength > maxAPIRequestBodyBytes {
			writeRequestBodyTooLarge(w)
			return
		}

		r.Body = http.MaxBytesReader(w, r.Body, maxAPIRequestBodyBytes)
		next.ServeHTTP(w, r)
	})
}

func writeRequestBodyTooLarge(w http.ResponseWriter) {
	writeCanonicalError(
		w,
		http.StatusRequestEntityTooLarge,
		errorcatalog.CodePolicyViolation,
		"request body exceeds the 1 MiB limit",
		"api.request_body_limit",
		map[string]any{"limit_bytes": maxAPIRequestBodyBytes},
	)
}
