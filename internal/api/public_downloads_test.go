package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
)

func TestArtifactListUsesPublicProjectPrefix(t *testing.T) {
	const prefix = "/api/projects/11111111-1111-4111-8111-111111111111/api/v1"
	h, db := newAPIServerForTestWithConfig(t, ServerConfig{PublicAPIBase: prefix})
	run := seedRunViaAPI(t, h)
	seedArtifact(t, db, run)
	response := performJSONRequest(t, h, requestSpec{method: http.MethodGet, path: "/api/v1/runs/" + run + "/artifacts"})
	if response.Code != 200 {
		t.Fatal(response.Code, response.Body.String())
	}
	var body struct {
		Items []struct {
			DownloadURL string `json:"download_url"`
		} `json:"items"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if len(body.Items) == 0 || !strings.HasPrefix(body.Items[0].DownloadURL, prefix+"/artifacts/") {
		t.Fatal(response.Body.String())
	}
	detail := performJSONRequest(t, h, requestSpec{method: http.MethodGet, path: "/api/v1/artifacts/artifact-1"})
	if detail.Code != 200 || !strings.Contains(detail.Body.String(), prefix+"/artifacts/artifact-1/content") {
		t.Fatal(detail.Code, detail.Body.String())
	}
	content := performJSONRequest(t, http.StripPrefix(strings.TrimSuffix(prefix, "/api/v1"), h), requestSpec{method: http.MethodGet, path: body.Items[0].DownloadURL})
	if content.Code != 200 || content.Body.String() != "seed artifact" {
		t.Fatal(content.Code, content.Body.String())
	}

}

func TestPublicDownloadConfigurationRejectsNoncanonicalAndExternalPaths(t *testing.T) {
	_, db := newAPIServerForTest(t)
	for _, prefix := range []string{"https://foreign.example/api", "//foreign.example/api", "/api/../other", "/api?other", "/api/", "api", " /api"} {
		if server, err := NewHandler(ServerConfig{DB: db, PublicAPIBase: prefix}); err == nil {
			server.Close(context.Background())
			t.Fatalf("accepted %q", prefix)
		}
	}
}

func TestPublicMailboxURLKeepsMountSeparate(t *testing.T) {
	s := Server{basePath: "/api/v1", publicAPIBase: "/api/projects/project/api/v1"}
	if got := s.mailboxContentURL("run", "message"); got != "/api/projects/project/api/v1/runs/run/mailbox/messages/message/content" {
		t.Fatal(got)
	}
}
