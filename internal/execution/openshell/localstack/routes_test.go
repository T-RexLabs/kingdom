package localstack

import (
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestLocalCallbacksRequireTLSAndFixedRoutes(t *testing.T) {
	h := fixedRoutes(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) }))
	for _, tc := range []struct {
		method, path string
		tls          bool
		want         int
	}{
		{"POST", "/model/v1/responses", true, 204}, {"POST", "/plugin/v1/native/invoke", true, 204}, {"POST", "/plugin/v1/command/invoke", true, 204}, {"POST", "/plugin/v1/command/cancel", true, 204},
		{"POST", "/plugin/v1/native/finish", true, 404}, {"POST", "/model/v1/responses?target=x", true, 404}, {"POST", "/model/v1/responses", false, 404}, {"GET", "/model/v1/responses", true, 404},
	} {
		r := httptest.NewRequest(tc.method, "http://localhost"+tc.path, nil)
		if tc.tls {
			r.TLS = &tls.ConnectionState{}
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != tc.want {
			t.Fatalf("%s %s TLS=%v: %d", tc.method, tc.path, tc.tls, w.Code)
		}
	}
}
