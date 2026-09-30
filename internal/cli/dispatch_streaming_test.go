package cli

import (
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestMutationResponseBeyondFormerLimitIsSpooledAndRemoved(t *testing.T) {
	const size = (2 << 20) + 1
	transport := bearerTransport{token: []byte("test"), allowedOrigin: "https://example.test", base: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusCreated, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(strings.Repeat("x", size)))}, nil
	})}
	req, _ := http.NewRequest(http.MethodPost, "https://example.test/runs", strings.NewReader("{}"))
	req.Header.Set("Idempotency-Key", "stable-key")
	res, err := transport.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	spool, ok := res.Body.(*temporaryResponseBody)
	if !ok {
		t.Fatalf("response is not disk-backed: %T", res.Body)
	}
	name := spool.Name()
	n, err := io.Copy(io.Discard, res.Body)
	if err != nil || n != size {
		t.Fatalf("response size=%d err=%v", n, err)
	}
	if err := res.Body.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(name); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("response spool leaked: %v", err)
	}
}

func TestArtifactDownloadPublishesAtomicallyAndCleansPartialFiles(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "archive.zip")
	if err := os.WriteFile(target, []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		reader io.Reader
		size   int64
	}{
		{"read error", io.MultiReader(strings.NewReader("partial"), &failingResponseBody{}), -1},
		{"short content length", strings.NewReader("short"), 100},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := saveArtifactDownload(tc.reader, tc.size, target); err == nil {
				t.Fatal("partial download succeeded")
			}
			body, err := os.ReadFile(target)
			if err != nil || string(body) != "old" {
				t.Fatalf("destination changed: %q %v", body, err)
			}
			entries, err := os.ReadDir(dir)
			if err != nil || len(entries) != 1 {
				t.Fatalf("temporary files leaked: %v %v", entries, err)
			}
		})
	}
	const size = (2 << 20) + 1
	n, err := saveArtifactDownload(strings.NewReader(strings.Repeat("z", size)), size, target)
	if err != nil || n != size {
		t.Fatalf("download size=%d err=%v", n, err)
	}
	info, err := os.Stat(target)
	if err != nil || info.Size() != size {
		t.Fatalf("published stat: %v %v", info, err)
	}
}
