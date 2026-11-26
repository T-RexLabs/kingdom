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

func TestCentralHandlerSelectsExactProjectMetadata(t *testing.T) {
	h := CentralHandler()
	for _, tc := range []struct {
		path, api, base, mode string
		status                int
	}{
		{"/", "/api/central/v1", "", "central", 200},
		{"/projects/9d38bdaa-34be-4d48-9b1c-a75ecc927847/runs", "/api/projects/9d38bdaa-34be-4d48-9b1c-a75ecc927847/api/v1", "/projects/9d38bdaa-34be-4d48-9b1c-a75ecc927847", "hosted-project", 200},
		{path: "/projects/foreign/runs", status: 404}, {path: "/projects/00000000-0000-0000-0000-000000000000/", status: 404},
	} {
		t.Run(tc.path, func(t *testing.T) {
			w := httptest.NewRecorder()
			h.ServeHTTP(w, httptest.NewRequest("GET", tc.path, nil))
			if w.Code != tc.status {
				t.Fatal(w.Code)
			}
			if tc.status == 200 {
				for _, metadata := range []string{`name="kingdom-api-base" content="` + tc.api + `"`, `name="kingdom-app-base" content="` + tc.base + `"`, `name="kingdom-runtime-mode" content="` + tc.mode + `"`} {
					if !strings.Contains(w.Body.String(), metadata) {
						t.Fatal("missing metadata", metadata)
					}
				}
			}
		})
	}
}
