package repositories

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestImportVerifiesObjectsAndPreservesExecutableMode(t *testing.T) {
	for _, scenario := range []string{"valid", "corrupt-blob", "corrupt-tree", "symlink", "truncated", "lfs"} {
		t.Run(scenario, func(t *testing.T) {
			c := fixture(t)
			data := []byte("hello\n")
			if scenario == "lfs" {
				data = []byte("version https://git-lfs.github.com/spec/v1\noid sha256:123\n")
			}
			blob := gitObject("blob", data)
			digest, _ := hex.DecodeString(blob)
			tree := gitObject("tree", append([]byte("100755 hello.sh\x00"), digest...))
			commit := strings.Repeat("a", 40)
			calls := 0
			c.http.Transport = roundTrip(func(r *http.Request) (*http.Response, error) {
				calls++
				switch r.URL.Path {
				case "/app/installations/2/access_tokens":
					return response(201, map[string]any{"token": "private", "expires_at": time.Now().Add(time.Hour), "permissions": map[string]string{"contents": "read"}, "repositories": []any{map[string]any{"id": 3}}}), nil
				case "/repositories/3":
					return response(200, map[string]any{"id": 3, "name": "repo", "owner": map[string]any{"login": "owner"}}), nil
				case "/repos/owner/repo/commits/main":
					return response(200, map[string]any{"sha": commit, "commit": map[string]any{"tree": map[string]any{"sha": tree}}}), nil
				case "/repos/owner/repo/git/trees/" + tree:
					mode := "100755"
					name := "hello.sh"
					if scenario == "symlink" {
						mode = "120000"
					}
					if scenario == "corrupt-tree" {
						name = "other.sh"
					}
					return response(200, map[string]any{"sha": tree, "truncated": scenario == "truncated", "tree": []any{map[string]any{"path": name, "mode": mode, "type": "blob", "sha": blob, "size": len(data)}}}), nil
				case "/repos/owner/repo/git/blobs/" + blob:
					payload := data
					if scenario == "corrupt-blob" {
						payload = []byte("wrong\n")
					}
					return response(200, map[string]any{"sha": blob, "size": len(data), "encoding": "base64", "content": base64.StdEncoding.EncodeToString(payload)}), nil
				default:
					t.Fatalf("unexpected endpoint %s", r.URL.Path)
					return nil, nil
				}
			})
			var out bytes.Buffer
			proof, err := c.ReadArchive(context.Background(), "project", "main", &out)
			if scenario != "valid" {
				if err == nil {
					t.Fatal("unsafe import accepted")
				}
				return
			}
			if err != nil || proof.RepositoryID != 3 || proof.CommitSHA != commit || proof.TreeSHA != tree || calls != 5 {
				t.Fatalf("import failed: %+v %v", proof, err)
			}
			archive, err := zip.NewReader(bytes.NewReader(out.Bytes()), int64(out.Len()))
			if err != nil || len(archive.File) != 1 {
				t.Fatal("invalid archive")
			}
			file := archive.File[0]
			if file.Name != "hello.sh" || file.Mode().Perm() != 0755 {
				t.Fatal("lost executable mode")
			}
			reader, _ := file.Open()
			actual, _ := io.ReadAll(reader)
			reader.Close()
			if !bytes.Equal(actual, data) {
				t.Fatal("changed bytes")
			}
		})
	}
}
func TestImportRejectsUntrustedRefBeforeNetwork(t *testing.T) {
	c := fixture(t)
	c.http.Transport = roundTrip(func(*http.Request) (*http.Response, error) { t.Fatal("network for invalid ref"); return nil, nil })
	for _, ref := range []string{"../main", "a?token=x", "a#fragment", "/main", "a//b", "main.lock"} {
		if _, err := c.ReadArchive(context.Background(), "project", ref, io.Discard); err != ErrInvalid {
			t.Fatalf("accepted %q", ref)
		}
	}
}
