package repositories

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"io"
	"net/http"
	"sort"
	"strings"

	"kingdom/internal/central/mutation"
	"kingdom/internal/publishedresult"
)

type deltaTree struct {
	entries  map[string]treeEntry
	children map[string]*deltaTree
}

func emptyDeltaTree() *deltaTree {
	return &deltaTree{entries: map[string]treeEntry{}, children: map[string]*deltaTree{}}
}
func treeBytes(entries []treeEntry) []byte {
	sort.Slice(entries, func(i, j int) bool {
		left, right := entries[i].Path, entries[j].Path
		if entries[i].Type == "tree" {
			left += "/"
		}
		if entries[j].Type == "tree" {
			right += "/"
		}
		return left < right
	})
	raw := []byte{}
	for _, entry := range entries {
		raw = append(raw, []byte(strings.TrimLeft(entry.Mode, "0")+" "+entry.Path+"\x00")...)
		sum, _ := hex.DecodeString(entry.SHA)
		raw = append(raw, sum...)
	}
	return raw
}
func (c *Client) readDeltaTree(ctx context.Context, a access, sha string) (*deltaTree, error) {
	if !gitSHA.MatchString(sha) {
		return nil, ErrInvalid
	}
	var result struct {
		SHA       string      `json:"sha"`
		Truncated bool        `json:"truncated"`
		Tree      []treeEntry `json:"tree"`
	}
	if err := c.request(ctx, http.MethodGet, a.path("/git/trees/"+sha), a.token, nil, maxMetadataBytes, &result); err != nil {
		return nil, err
	}
	if result.SHA != sha || result.Truncated || len(result.Tree) > maxImportFiles {
		return nil, ErrUnavailable
	}
	node := emptyDeltaTree()
	for _, entry := range result.Tree {
		if entry.Path == "" || entry.Path == "." || entry.Path == ".." || strings.ContainsAny(entry.Path, "/\\\x00") || !gitSHA.MatchString(entry.SHA) {
			return nil, ErrUnavailable
		}
		if _, ok := node.entries[entry.Path]; ok {
			return nil, ErrUnavailable
		}
		switch entry.Mode {
		case "040000":
			if entry.Type != "tree" {
				return nil, ErrUnavailable
			}
		case "100644", "100755", "120000":
			if entry.Type != "blob" {
				return nil, ErrUnavailable
			}
		case "160000":
			if entry.Type != "commit" {
				return nil, ErrUnavailable
			}
		default:
			return nil, ErrUnavailable
		}
		node.entries[entry.Path] = entry
	}
	if gitObject("tree", treeBytes(result.Tree)) != sha {
		return nil, ErrUnavailable
	}
	return node, nil
}
func (c *Client) readGitBlob(ctx context.Context, a access, entry treeEntry) ([]byte, error) {
	var blob struct {
		SHA      string `json:"sha"`
		Size     int64  `json:"size"`
		Encoding string `json:"encoding"`
		Content  string `json:"content"`
	}
	if err := c.request(ctx, http.MethodGet, a.path("/git/blobs/"+entry.SHA), a.token, nil, 2*maxBlobBytes, &blob); err != nil {
		return nil, err
	}
	if blob.SHA != entry.SHA || blob.Size < 0 || blob.Size > maxBlobBytes || blob.Encoding != "base64" {
		return nil, ErrUnavailable
	}
	data, err := base64.StdEncoding.DecodeString(blob.Content)
	if err != nil || int64(len(data)) != blob.Size || gitObject("blob", data) != entry.SHA {
		return nil, ErrUnavailable
	}
	return data, nil
}

// buildResultDelta preserves every unrelated entry from the pinned base tree.
// Before bytes and modes must match exactly; upstream changes are conflicts.
func (c *Client) buildResultDelta(ctx context.Context, a access, source ResultSource, actor, project string, snapshot publishedresult.ResultSnapshot, baseCommit string, authorize func() error) (string, error) {
	if source == nil || snapshot.SourceSnapshotID == "" || !validResult(snapshot, snapshot.RunID) || len(snapshot.Changes) == 0 || len(snapshot.Changes) > publishedresult.MaxFiles {
		return "", ErrInvalid
	}
	var commit struct {
		SHA  string `json:"sha"`
		Tree struct {
			SHA string `json:"sha"`
		} `json:"tree"`
	}
	if err := c.request(ctx, http.MethodGet, a.path("/git/commits/"+baseCommit), a.token, nil, maxMetadataBytes, &commit); err != nil {
		return "", err
	}
	if commit.SHA != baseCommit {
		return "", ErrUnavailable
	}
	root, err := c.readDeltaTree(ctx, a, commit.Tree.SHA)
	if err != nil {
		return "", err
	}
	treeReads := 1
	afterFiles := map[string]publishedresult.ResultFile{}
	for _, file := range snapshot.Files {
		afterFiles[file.Path] = file
	}
	var beforeBytes int64
	parents := map[string]*deltaTree{}
	seen := map[string]bool{}
	for _, change := range snapshot.Changes {
		if change.After != nil {
			actual, ok := afterFiles[change.Path]
			if !ok || actual != *change.After {
				return "", ErrInvalid
			}
		} else if _, exists := afterFiles[change.Path]; exists {
			return "", ErrInvalid
		}
		if change.Before == nil && change.After == nil {
			return "", ErrInvalid
		}
		file := change.Before
		if file == nil {
			file = change.After
		}
		if file.Path != change.Path {
			return "", ErrInvalid
		}
		if change.After != nil && change.After.Path != change.Path {
			return "", ErrInvalid
		}
		if seen[strings.ToLower(change.Path)] {
			return "", ErrInvalid
		}
		seen[strings.ToLower(change.Path)] = true
		if !validResult(publishedresult.ResultSnapshot{RunID: snapshot.RunID, SnapshotID: snapshot.SnapshotID, ManifestSHA256: snapshot.ManifestSHA256, Files: []publishedresult.ResultFile{*file}}, snapshot.RunID) {
			return "", ErrInvalid
		}
		parts := strings.Split(change.Path, "/")
		node := root
		for _, part := range parts[:len(parts)-1] {
			for existing := range node.entries {
				if existing != part && strings.EqualFold(existing, part) {
					return "", mutation.ErrConflict
				}
			}
			if child := node.children[part]; child != nil {
				node = child
				continue
			}
			entry, exists := node.entries[part]
			var child *deltaTree
			if exists {
				if entry.Type != "tree" {
					return "", mutation.ErrConflict
				}
				treeReads++
				if treeReads > maxTreeRequests {
					return "", ErrInvalid
				}
				child, err = c.readDeltaTree(ctx, a, entry.SHA)
				if err != nil {
					return "", err
				}
			} else {
				if change.Before != nil {
					return "", mutation.ErrConflict
				}
				child = emptyDeltaTree()
			}
			node.children[part] = child
			node = child
		}
		name := parts[len(parts)-1]
		for existing := range node.entries {
			if existing != name && strings.EqualFold(existing, name) {
				return "", mutation.ErrConflict
			}
		}
		parents[change.Path] = node
		entry, exists := node.entries[name]
		if change.Before == nil {
			if exists {
				return "", mutation.ErrConflict
			}
			continue
		}
		before := change.Before
		beforeBytes += before.Size
		if beforeBytes > publishedresult.MaxTotalBytes {
			return "", ErrInvalid
		}
		expectedMode := "100644"
		if before.Mode == 0755 {
			expectedMode = "100755"
		}
		if !exists || entry.Type != "blob" || entry.Mode != expectedMode {
			return "", mutation.ErrConflict
		}
		data, err := c.readGitBlob(ctx, a, entry)
		if err != nil {
			return "", err
		}
		sum := sha256.Sum256(data)
		if int64(len(data)) != before.Size || hex.EncodeToString(sum[:]) != before.SHA256 {
			return "", mutation.ErrConflict
		}
	}
	for _, change := range snapshot.Changes {
		if err := authorize(); err != nil {
			return "", err
		}
		node := parents[change.Path]
		parts := strings.Split(change.Path, "/")
		name := parts[len(parts)-1]
		if change.After == nil {
			delete(node.entries, name)
			continue
		}
		file := change.After
		reader, err := source.Open(ctx, actor, project, snapshot.RunID, snapshot.SnapshotID, file.Path)
		if err != nil {
			return "", ErrUnavailable
		}
		data, err := io.ReadAll(io.LimitReader(reader, file.Size+1))
		closeErr := reader.Close()
		sum := sha256.Sum256(data)
		if err != nil || closeErr != nil || int64(len(data)) != file.Size || hex.EncodeToString(sum[:]) != file.SHA256 {
			return "", ErrUnavailable
		}
		expected := gitObject("blob", data)
		if err = authorize(); err != nil {
			return "", err
		}
		var result struct {
			SHA string `json:"sha"`
		}
		if err = c.request(ctx, http.MethodPost, a.path("/git/blobs"), a.token, map[string]string{"encoding": "base64", "content": base64.StdEncoding.EncodeToString(data)}, maxMetadataBytes, &result); err != nil {
			return "", err
		}
		if result.SHA != expected {
			return "", ErrUnavailable
		}
		mode := "100644"
		if file.Mode == 0755 {
			mode = "100755"
		}
		node.entries[name] = treeEntry{Path: name, Mode: mode, Type: "blob", SHA: expected}
	}
	var upload func(*deltaTree) (string, error)
	upload = func(node *deltaTree) (string, error) {
		for name, child := range node.children {
			sha, err := upload(child)
			if err != nil {
				return "", err
			}
			if sha == "" {
				delete(node.entries, name)
			} else {
				node.entries[name] = treeEntry{Path: name, Mode: "040000", Type: "tree", SHA: sha}
			}
		}
		if len(node.entries) == 0 && node != root {
			return "", nil
		}
		entries := make([]treeEntry, 0, len(node.entries))
		for _, entry := range node.entries {
			entries = append(entries, entry)
		}
		expected := gitObject("tree", treeBytes(entries))
		request := make([]map[string]string, 0, len(entries))
		for _, entry := range entries {
			request = append(request, map[string]string{"path": entry.Path, "mode": entry.Mode, "type": entry.Type, "sha": entry.SHA})
		}
		if err := authorize(); err != nil {
			return "", err
		}
		var result struct {
			SHA string `json:"sha"`
		}
		if err := c.request(ctx, http.MethodPost, a.path("/git/trees"), a.token, map[string]any{"tree": request}, maxMetadataBytes, &result); err != nil {
			return "", err
		}
		if result.SHA != expected {
			return "", ErrUnavailable
		}
		return expected, nil
	}
	return upload(root)
}
