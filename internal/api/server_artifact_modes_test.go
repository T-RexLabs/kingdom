package api

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
)

func TestArtifactModesReachAPIAndZIP(t *testing.T) {
	for _, mode := range []int64{0, 0600, 0700, 0751} {
		t.Run(fmt.Sprintf("%04o", mode), func(t *testing.T) {
			handler, db := newAPIServerForTest(t)
			runID := seedRunViaAPI(t, handler)
			seedArtifact(t, db, runID)
			if _, err := db.Exec(`UPDATE artifacts SET mode_bits=? WHERE artifact_id='artifact-1'`, mode); err != nil {
				t.Fatal(err)
			}
			detail := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: "/api/v1/artifacts/artifact-1"})
			if detail.Code != http.StatusOK {
				t.Fatalf("detail status=%d body=%s", detail.Code, detail.Body.String())
			}
			var item map[string]any
			if err := json.Unmarshal(detail.Body.Bytes(), &item); err != nil {
				t.Fatal(err)
			}
			if item["mode_bits"] != float64(mode) {
				t.Fatalf("detail mode=%v want=%d", item["mode_bits"], mode)
			}
			list := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: "/api/v1/runs/" + runID + "/artifacts"})
			if list.Code != http.StatusOK {
				t.Fatalf("list status=%d body=%s", list.Code, list.Body.String())
			}
			var page struct {
				Items []map[string]any `json:"items"`
			}
			if err := json.Unmarshal(list.Body.Bytes(), &page); err != nil {
				t.Fatal(err)
			}
			if len(page.Items) != 1 || page.Items[0]["mode_bits"] != float64(mode) {
				t.Fatalf("list modes=%v", page.Items)
			}
			response := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: "/api/v1/runs/" + runID + "/artifacts/archive"})
			if response.Code != http.StatusOK {
				t.Fatalf("archive status=%d body=%s", response.Code, response.Body.String())
			}
			archive, err := zip.NewReader(bytes.NewReader(response.Body.Bytes()), int64(response.Body.Len()))
			if err != nil {
				t.Fatal(err)
			}
			if len(archive.File) != 1 || int64(archive.File[0].Mode().Perm()) != mode {
				t.Fatalf("archive permissions=%v want=%o", archive.File, mode)
			}
		})
	}
}
