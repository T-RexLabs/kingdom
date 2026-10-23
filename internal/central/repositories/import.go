package repositories

import (
	"archive/zip"
	"context"
	"crypto/sha1" // Git object identity uses SHA-1; archives also retain SHA-256 downstream.
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"regexp"
	"sort"
	"strings"
	"time"
)

const maxImportFiles = 32768
const maxImportBytes = int64(1 << 30)
const maxBlobBytes = int64(64 << 20)
const maxTreeRequests = 1024

var gitSHA = regexp.MustCompile(`^[a-f0-9]{40}$`)
var gitRef = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9_./-]{0,199}$`)

type Provenance struct {
	RepositoryID int64  `json:"repository_id"`
	CommitSHA    string `json:"commit_sha"`
	TreeSHA      string `json:"tree_sha"`
}
type treeEntry struct {
	Path string `json:"path"`
	Mode string `json:"mode"`
	Type string `json:"type"`
	SHA  string `json:"sha"`
	Size int64  `json:"size"`
}
type importFile struct {
	path, sha string
	mode      uint32
	size      int64
}

func gitObject(kind string, data []byte) string {
	h := sha1.New()
	fmt.Fprintf(h, "%s %d\x00", kind, len(data))
	h.Write(data)
	return hex.EncodeToString(h.Sum(nil))
}
func validRef(ref string) bool {
	return gitRef.MatchString(ref) && !strings.Contains(ref, "..") && !strings.Contains(ref, "//") && !strings.HasSuffix(ref, "/") && !strings.HasSuffix(ref, ".") && !strings.HasSuffix(ref, ".lock")
}

// ReadArchive reads a fixed commit without invoking Git, accepting hooks, or
// following provider redirects. The caller must discard partial output on error.
// Authorization to the project is checked by the service before and after this
// bounded external read; this client only enforces the operator repository grant.
func (c *Client) ReadArchive(ctx context.Context, project, ref string, dst io.Writer) (Provenance, error) {
	if !validRef(ref) || dst == nil {
		return Provenance{}, ErrInvalid
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	a, err := c.authorize(ctx, project, false)
	if err != nil {
		return Provenance{}, err
	}
	var commit struct {
		SHA    string `json:"sha"`
		Commit struct {
			Tree struct {
				SHA string `json:"sha"`
			} `json:"tree"`
		} `json:"commit"`
	}
	if err = c.request(ctx, http.MethodGet, a.path("/commits/"+ref), a.token, nil, maxMetadataBytes, &commit); err != nil {
		return Provenance{}, err
	}
	if !gitSHA.MatchString(commit.SHA) || !gitSHA.MatchString(commit.Commit.Tree.SHA) {
		return Provenance{}, ErrUnavailable
	}
	if gitSHA.MatchString(ref) && ref != commit.SHA {
		return Provenance{}, ErrUnavailable
	}
	files := []importFile{}
	var total int64
	requests := 0
	entries := 0
	var walk func(string, string, int) error
	walk = func(tree, prefix string, depth int) error {
		requests++
		if requests > maxTreeRequests || depth > 64 {
			return ErrInvalid
		}
		var result struct {
			SHA       string      `json:"sha"`
			Truncated bool        `json:"truncated"`
			Tree      []treeEntry `json:"tree"`
		}
		if err := c.request(ctx, http.MethodGet, a.path("/git/trees/"+tree), a.token, nil, maxMetadataBytes, &result); err != nil {
			return err
		}
		if result.SHA != tree || result.Truncated {
			return ErrUnavailable
		}
		seen := map[string]bool{}
		for _, entry := range result.Tree {
			entries++
			if entries > maxImportFiles || entry.Path == "" || entry.Path == "." || entry.Path == ".." || strings.ContainsAny(entry.Path, "/\\\x00") || seen[strings.ToLower(entry.Path)] || !gitSHA.MatchString(entry.SHA) {
				return ErrInvalid
			}
			seen[strings.ToLower(entry.Path)] = true
			// Case-insensitive metadata rejection also protects checkout on macOS.
			if strings.EqualFold(entry.Path, ".git") || strings.EqualFold(entry.Path, ".kingdom") {
				return ErrInvalid
			}
			if (entry.Mode != "040000" || entry.Type != "tree") && (entry.Mode != "100644" && entry.Mode != "100755" || entry.Type != "blob") {
				return ErrInvalid
			}
		}
		// Verify the complete tree bytes, including modes and names, against its ID.
		sort.Slice(result.Tree, func(i, j int) bool {
			left, right := result.Tree[i].Path, result.Tree[j].Path
			if result.Tree[i].Type == "tree" {
				left += "/"
			}
			if result.Tree[j].Type == "tree" {
				right += "/"
			}
			return left < right
		})
		raw := []byte{}
		for _, entry := range result.Tree {
			mode := strings.TrimLeft(entry.Mode, "0")
			raw = append(raw, []byte(mode+" "+entry.Path+"\x00")...)
			digest, _ := hex.DecodeString(entry.SHA)
			raw = append(raw, digest...)
		}
		if gitObject("tree", raw) != tree {
			return ErrUnavailable
		}
		for _, entry := range result.Tree {
			name := path.Join(prefix, entry.Path)
			if len(name) > 4096 {
				return ErrInvalid
			}
			if entry.Type == "tree" {
				if err := walk(entry.SHA, name, depth+1); err != nil {
					return err
				}
				continue
			}
			if entry.Size < 0 || entry.Size > maxBlobBytes {
				return ErrInvalid
			}
			total += entry.Size
			if total > maxImportBytes {
				return ErrInvalid
			}
			mode := uint32(0644)
			if entry.Mode == "100755" {
				mode = 0755
			}
			files = append(files, importFile{name, entry.SHA, mode, entry.Size})
		}
		return nil
	}
	if err = walk(commit.Commit.Tree.SHA, "", 0); err != nil {
		return Provenance{}, err
	}
	archive := zip.NewWriter(dst)
	for _, file := range files {
		var blob struct {
			SHA      string `json:"sha"`
			Size     int64  `json:"size"`
			Encoding string `json:"encoding"`
			Content  string `json:"content"`
		}
		if err = c.request(ctx, http.MethodGet, a.path("/git/blobs/"+file.sha), a.token, nil, 2*maxBlobBytes, &blob); err != nil {
			return Provenance{}, err
		}
		if blob.SHA != file.sha || blob.Size != file.size || blob.Encoding != "base64" {
			return Provenance{}, ErrUnavailable
		}
		data, err := base64.StdEncoding.DecodeString(blob.Content)
		if err != nil || int64(len(data)) != file.size || gitObject("blob", data) != file.sha {
			return Provenance{}, ErrUnavailable
		}
		if strings.HasPrefix(string(data), "version https://git-lfs.github.com/spec/v1\n") {
			return Provenance{}, ErrInvalid
		}
		header := &zip.FileHeader{Name: file.path, Method: zip.Deflate}
		header.SetMode(os.FileMode(file.mode))
		out, err := archive.CreateHeader(header)
		if err != nil {
			return Provenance{}, ErrUnavailable
		}
		if _, err = out.Write(data); err != nil {
			return Provenance{}, ErrUnavailable
		}
	}
	if err = archive.Close(); err != nil {
		return Provenance{}, ErrUnavailable
	}
	return Provenance{a.grant.RepositoryID, commit.SHA, commit.Commit.Tree.SHA}, nil
}
