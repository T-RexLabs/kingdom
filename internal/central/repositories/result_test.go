package repositories

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"kingdom/internal/publishedresult"
)

type resultFixture struct{ data string }

func (f resultFixture) Resolve(context.Context, string, string, string) (publishedresult.ResultSnapshot, error) {
	return publishedresult.ResultSnapshot{}, nil
}
func (f resultFixture) Open(context.Context, string, string, string, string, string) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader(f.data)), nil
}
func TestPublishedDeltaPreservesUnrelatedFilesAndChecksBefore(t *testing.T) {
	for _, scenario := range []string{"valid", "corrupt-result", "upstream-change"} {
		t.Run(scenario, func(t *testing.T) {
			c := fixture(t)
			beforeBytes := []byte("before")
			beforeSum := sha256.Sum256(beforeBytes)
			resultSum := sha256.Sum256([]byte("result"))
			before := publishedresult.ResultFile{Path: "main.txt", Mode: 0644, Size: 6, SHA256: hex.EncodeToString(beforeSum[:])}
			after := publishedresult.ResultFile{Path: "main.txt", Mode: 0644, Size: 6, SHA256: hex.EncodeToString(resultSum[:])}
			snapshot := publishedresult.ResultSnapshot{RunID: "run", SnapshotID: "snapshot", SourceSnapshotID: "source", ManifestSHA256: strings.Repeat("a", 64), Files: []publishedresult.ResultFile{after}, Changes: []publishedresult.ResultChange{{Path: "main.txt", Before: &before, After: &after}}}
			baseData := beforeBytes
			if scenario == "upstream-change" {
				baseData = []byte("edited")
			}
			baseBlob := gitObject("blob", baseData)
			keepBlob := gitObject("blob", []byte("keep"))
			entries := []treeEntry{{Path: "main.txt", Mode: "100644", Type: "blob", SHA: baseBlob}, {Path: "unrelated.txt", Mode: "100644", Type: "blob", SHA: keepBlob}}
			tree := gitObject("tree", treeBytes(entries))
			baseCommit := strings.Repeat("b", 40)
			writes := 0
			c.http.Transport = roundTrip(func(r *http.Request) (*http.Response, error) {
				if r.Method == http.MethodPost {
					writes++
				}
				switch r.URL.Path {
				case "/repos/owner/repo/git/commits/" + baseCommit:
					return response(200, map[string]any{"sha": baseCommit, "tree": map[string]string{"sha": tree}}), nil
				case "/repos/owner/repo/git/trees/" + tree:
					return response(200, map[string]any{"sha": tree, "tree": entries}), nil
				case "/repos/owner/repo/git/blobs/" + baseBlob:
					return response(200, map[string]any{"sha": baseBlob, "size": len(baseData), "encoding": "base64", "content": base64.StdEncoding.EncodeToString(baseData)}), nil
				case "/repos/owner/repo/git/blobs":
					var body map[string]string
					json.NewDecoder(r.Body).Decode(&body)
					data, _ := base64.StdEncoding.DecodeString(body["content"])
					return response(201, map[string]string{"sha": gitObject("blob", data)}), nil
				case "/repos/owner/repo/git/trees":
					var body struct {
						Tree []treeEntry `json:"tree"`
					}
					json.NewDecoder(r.Body).Decode(&body)
					if len(body.Tree) != 2 {
						t.Fatal("unrelated file deleted")
					}
					var kept bool
					for _, entry := range body.Tree {
						if entry.Path == "unrelated.txt" && entry.SHA == keepBlob {
							kept = true
						}
					}
					if !kept {
						t.Fatal("unrelated blob changed")
					}
					return response(201, map[string]string{"sha": gitObject("tree", treeBytes(body.Tree))}), nil
				default:
					t.Fatal("unexpected path", r.URL.Path)
					return nil, nil
				}
			})
			a := access{token: "token"}
			a.repo.Name = "repo"
			a.repo.Owner.Login = "owner"
			source := resultFixture{"result"}
			if scenario == "corrupt-result" {
				source.data = "wrong!"
			}
			result, err := c.buildResultDelta(context.Background(), a, source, "actor", "project", snapshot, baseCommit, func() error { return nil })
			if scenario != "valid" {
				if err == nil || writes != 0 {
					t.Fatal("unverified or conflicting bytes published")
				}
			} else if err != nil || !gitSHA.MatchString(result) || writes != 2 {
				t.Fatalf("tree: %s %v writes%d", result, err, writes)
			}
		})
	}
}
func TestCommitIdentityIsDeterministicAndChecked(t *testing.T) {
	c := fixture(t)
	a := access{token: "token"}
	a.repo.Name = "repo"
	a.repo.Owner.Login = "owner"
	tree, parent := strings.Repeat("a", 40), strings.Repeat("b", 40)
	date := time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)
	message := "Publish result\n"
	raw := "tree " + tree + "\nparent " + parent + "\nauthor Kingdom <kingdom@users.noreply.github.com> 1792756800 +0000\ncommitter Kingdom <kingdom@users.noreply.github.com> 1792756800 +0000\n\n" + message
	expected := gitObject("commit", []byte(raw))
	c.http.Transport = roundTrip(func(r *http.Request) (*http.Response, error) {
		return response(201, map[string]string{"sha": expected}), nil
	})
	actual, err := c.createResultCommit(context.Background(), a, tree, parent, message, date)
	if err != nil || actual != expected {
		t.Fatalf("commit: %s %v epoch=%d", actual, err, date.Unix())
	}
	c.http.Transport = roundTrip(func(r *http.Request) (*http.Response, error) {
		return response(201, map[string]string{"sha": parent}), nil
	})
	if _, err = c.createResultCommit(context.Background(), a, tree, parent, message, date); err == nil {
		t.Fatal("foreign commit accepted")
	}
}
