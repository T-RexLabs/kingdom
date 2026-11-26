package repositories

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"path"
	"strconv"
	"strings"
	"time"

	"kingdom/internal/publishedresult"
)

type ResultSource interface {
	Resolve(context.Context, string, string, string) (publishedresult.ResultSnapshot, error)
	Open(context.Context, string, string, string, string, string) (io.ReadCloser, error)
}

func validResult(snapshot publishedresult.ResultSnapshot, run string) bool {
	if snapshot.RunID != run || snapshot.SnapshotID == "" || len(snapshot.Files) > publishedresult.MaxFiles || len(snapshot.ManifestSHA256) != 64 {
		return false
	}
	if _, err := hex.DecodeString(snapshot.ManifestSHA256); err != nil {
		return false
	}
	var total int64
	seen := map[string]bool{}
	for _, file := range snapshot.Files {
		if file.Path == "" || path.Clean(file.Path) != file.Path || strings.HasPrefix(file.Path, "/") || strings.ContainsAny(file.Path, "\\\x00\r\n") || len(file.Path) > 4096 || file.Size < 0 || file.Size > publishedresult.MaxFileBytes || (file.Mode != 0644 && file.Mode != 0755) || len(file.SHA256) != 64 {
			return false
		}
		if _, err := hex.DecodeString(file.SHA256); err != nil {
			return false
		}
		parts := strings.Split(file.Path, "/")
		if len(parts) > 64 {
			return false
		}
		for _, part := range parts {
			if part == ".." || part == "." || strings.EqualFold(part, ".git") || strings.EqualFold(part, ".kingdom") {
				return false
			}
		}
		folded := strings.ToLower(file.Path)
		if seen[folded] {
			return false
		}
		seen[folded] = true
		total += file.Size
		if total > publishedresult.MaxTotalBytes {
			return false
		}
	}
	for name := range seen {
		for parent := path.Dir(name); parent != "."; parent = path.Dir(parent) {
			if seen[parent] {
				return false
			}
		}
	}
	return true
}

func (c *Client) createResultCommit(ctx context.Context, a access, tree, parent, message string, date time.Time) (string, error) {
	if !gitSHA.MatchString(tree) || !gitSHA.MatchString(parent) || strings.ContainsRune(message, '\x00') {
		return "", ErrInvalid
	}
	date = date.UTC().Truncate(time.Second)
	identity := "Kingdom <kingdom@users.noreply.github.com> " + strconv.FormatInt(date.Unix(), 10) + " +0000"
	raw := fmt.Sprintf("tree %s\nparent %s\nauthor %s\ncommitter %s\n\n%s", tree, parent, identity, identity, message)
	expected := gitObject("commit", []byte(raw))
	person := map[string]string{"name": "Kingdom", "email": "kingdom@users.noreply.github.com", "date": date.Format(time.RFC3339)}
	var result struct {
		SHA string `json:"sha"`
	}
	if err := c.request(ctx, http.MethodPost, a.path("/git/commits"), a.token, map[string]any{"tree": tree, "parents": []string{parent}, "message": message, "author": person, "committer": person}, maxMetadataBytes, &result); err != nil {
		return "", err
	}
	if result.SHA != expected {
		return "", ErrUnavailable
	}
	return expected, nil
}
