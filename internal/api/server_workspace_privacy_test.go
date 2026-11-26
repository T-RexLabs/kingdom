package api

import (
	"encoding/json"
	gen "kingdom/internal/apigen"
	"net/http"
	"net/http/httptest"
	"testing"
)

type workspaceHostedAuth struct{}

func (workspaceHostedAuth) Authenticate(*http.Request) (Principal, error) {
	return Principal{}, ErrAuthenticationRequired
}
func TestWorkspaceCapabilitiesHideHostedFilesystemRoot(t *testing.T) {
	for _, hosted := range []bool{false, true} {
		t.Run(map[bool]string{false: "local", true: "hosted"}[hosted], func(t *testing.T) {
			root := t.TempDir()
			s := &Server{projectRoot: root, defaultPublishMode: "direct"}
			if hosted {
				s.authenticator = workspaceHostedAuth{}
			}
			w := httptest.NewRecorder()
			s.handleGetWorkspaceCapabilities(w, httptest.NewRequest("GET", "/", nil))
			var got gen.WorkspaceCapabilities
			if e := json.Unmarshal(w.Body.Bytes(), &got); e != nil {
				t.Fatal(e)
			}
			if hosted && got.ProjectRoot != "" {
				t.Fatal("hosted response exposed host root")
			}
			if !hosted && got.ProjectRoot != root {
				t.Fatal("local root changed")
			}
		})
	}
}
