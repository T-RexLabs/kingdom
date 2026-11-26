package api

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

type cancelArtifactWriter struct {
	*httptest.ResponseRecorder
	cancel context.CancelFunc
	writes int
}

func (w *cancelArtifactWriter) Write(p []byte) (int, error) {
	w.writes++
	n, err := w.ResponseRecorder.Write(p)
	if w.writes == 1 {
		w.cancel()
	}
	return n, err
}
func (w *cancelArtifactWriter) WriteString(s string) (int, error) { return w.Write([]byte(s)) }

func TestArtifactDownloadsStopCopyingAfterRequestCancellation(t *testing.T) {
	for _, kind := range []string{"archive", "content"} {
		t.Run(kind, func(t *testing.T) {
			handler, db := newAPIServerForTest(t)
			runID := seedRunViaAPI(t, handler)
			seedArtifact(t, db, runID)
			payload := make([]byte, 1<<20)
			if _, err := rand.Read(payload); err != nil {
				t.Fatal(err)
			}
			sum := sha256.Sum256(payload)
			if _, err := db.Exec(`UPDATE artifact_content_chunks SET content=? WHERE artifact_id='artifact-1' AND chunk_index=0`, payload); err != nil {
				t.Fatal(err)
			}
			for _, table := range []string{"artifacts", "artifact_contents"} {
				if _, err := db.Exec(`UPDATE `+table+` SET sha256=?,size_bytes=? WHERE artifact_id='artifact-1'`, fmt.Sprintf("%x", sum), len(payload)); err != nil {
					t.Fatal(err)
				}
			}
			path := "/api/v1/artifacts/artifact-1/content"
			if kind == "archive" {
				path = "/api/v1/runs/" + runID + "/artifacts/archive"
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			request := httptest.NewRequest(http.MethodGet, path, nil).WithContext(ctx)
			authenticateAPITestRequest(request)
			writer := &cancelArtifactWriter{ResponseRecorder: httptest.NewRecorder(), cancel: cancel}
			var recovered any
			func() { defer func() { recovered = recover() }(); handler.ServeHTTP(writer, request) }()
			if writer.Code != 200 {
				t.Fatalf("download did not start: %d %s", writer.Code, writer.Body.String())
			}
			if recovered != http.ErrAbortHandler || writer.writes != 1 {
				t.Fatalf("canceled %s copied %d chunks; abort=%v", kind, writer.writes, recovered)
			}
		})
	}
}
