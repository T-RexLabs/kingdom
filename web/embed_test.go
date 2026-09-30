package webui

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHandlerInjectsConfiguredAPIBaseIntoIndexAndFallback(t *testing.T) {
	t.Parallel()

	handler := Handler("/internal/api")
	for _, path := range []string{"/", "/runs/run-1"} {
		request := httptest.NewRequest(http.MethodGet, path, nil)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)

		if response.Code != http.StatusOK {
			t.Fatalf("%s status=%d", path, response.Code)
		}
		if !strings.Contains(response.Body.String(), `<meta name="kingdom-api-base" content="/internal/api" />`) {
			t.Fatalf("%s did not receive configured API base metadata", path)
		}
	}
}
