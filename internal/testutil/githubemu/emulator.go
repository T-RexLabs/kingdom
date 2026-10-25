// Package githubemu provides an isolated, stateful GitHub contract emulator for
// tests. It never opens a network connection or accepts production credentials.
package githubemu

import (
	"bytes"
	"crypto"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const APIVersion = "2026-03-10"

type File struct {
	Data []byte
	Mode string
}
type Config struct {
	AppID, InstallationID, RepositoryID int64
	PublicKey                           *rsa.PublicKey
	Owner, Name                         string
	Files                               map[string]File
	Now                                 func() time.Time
}
type Entry struct {
	Path string `json:"path"`
	Mode string `json:"mode"`
	Type string `json:"type"`
	SHA  string `json:"sha"`
	Size int    `json:"size,omitempty"`
}
type Pull struct {
	Number                    int64
	HeadRef, HeadSHA, BaseRef string
}
type pullRecord struct {
	Pull
	body, title string
}

type Scope struct {
	RepositoryID int64
	Permissions  map[string]string
}
type Snapshot struct {
	MainCommit                                            string
	Refs                                                  map[string]string
	FilesByRef                                            map[string]map[string]File
	Pulls                                                 []Pull
	Requests, Writes, RefCreates, PullCreates, MainWrites int
	TokenRequests                                         []Scope
	Violations                                            []string
}
type commit struct {
	SHA, Tree string
	Parents   []string
}
type token struct {
	permissions map[string]string
	expires     time.Time
}
type Emulator struct {
	mu                          sync.Mutex
	c                           Config
	blobs                       map[string][]byte
	trees                       map[string][]Entry
	commits                     map[string]commit
	refs                        map[string]string
	pulls                       []pullRecord
	tokens                      map[string]token
	snapshot                    Snapshot
	denied, losePull, hidePulls bool
	faultMethod, faultPath      string
}

func New(c Config) (*Emulator, error) {
	if c.AppID <= 0 || c.InstallationID <= 0 || c.RepositoryID <= 0 || c.PublicKey == nil || c.PublicKey.N == nil || c.PublicKey.N.BitLen() < 2048 || c.Owner == "" || c.Name == "" || len(c.Files) == 0 {
		return nil, errors.New("invalid emulator fixture")
	}
	if c.Now == nil {
		c.Now = time.Now
	}
	e := &Emulator{c: c, blobs: map[string][]byte{}, trees: map[string][]Entry{}, commits: map[string]commit{}, refs: map[string]string{}, tokens: map[string]token{}}
	type directory struct {
		files map[string]File
		dirs  map[string]*directory
	}
	var build func(*directory) string
	root := &directory{files: map[string]File{}, dirs: map[string]*directory{}}
	for name, file := range c.Files {
		if !fs.ValidPath(name) || name == "." {
			return nil, errors.New("invalid fixture path")
		}
		if file.Mode == "" {
			file.Mode = "100644"
		}
		if file.Mode != "100644" && file.Mode != "100755" {
			return nil, errors.New("invalid fixture mode")
		}
		node := root
		parts := strings.Split(name, "/")
		for _, part := range parts[:len(parts)-1] {
			if node.dirs[part] == nil {
				node.dirs[part] = &directory{files: map[string]File{}, dirs: map[string]*directory{}}
			}
			node = node.dirs[part]
		}
		node.files[parts[len(parts)-1]] = file
	}
	build = func(node *directory) string {
		entries := []Entry{}
		for name, file := range node.files {
			sha := object("blob", file.Data)
			e.blobs[sha] = bytes.Clone(file.Data)
			entries = append(entries, Entry{Path: name, Mode: file.Mode, Type: "blob", SHA: sha, Size: len(file.Data)})
		}
		for name, child := range node.dirs {
			entries = append(entries, Entry{Path: name, Mode: "040000", Type: "tree", SHA: build(child)})
		}
		return e.putTree(entries)
	}
	tree := build(root)
	raw := []byte("tree " + tree + "\nauthor Fixture <fixture@example.invalid> 1700000000 +0000\ncommitter Fixture <fixture@example.invalid> 1700000000 +0000\n\nInitial fixture\n")
	sha := object("commit", raw)
	e.commits[sha] = commit{SHA: sha, Tree: tree}
	e.refs["main"] = sha
	e.snapshot.MainCommit = sha
	return e, nil
}
func (e *Emulator) Transport() http.RoundTripper { return e }
func (e *Emulator) DenyWrites(value bool)        { e.mu.Lock(); defer e.mu.Unlock(); e.denied = value }
func (e *Emulator) LoseNextPullReply(value bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.losePull = value
}
func (e *Emulator) HidePulls(value bool) { e.mu.Lock(); defer e.mu.Unlock(); e.hidePulls = value }
func (e *Emulator) FailNext(method, path string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.faultMethod, e.faultPath = method, path
}
func object(kind string, data []byte) string {
	h := sha1.New()
	fmt.Fprintf(h, "%s %d\x00", kind, len(data))
	h.Write(data)
	return hex.EncodeToString(h.Sum(nil))
}
func treeBytes(entries []Entry) []byte {
	copy := append([]Entry(nil), entries...)
	sort.Slice(copy, func(i, j int) bool {
		a, b := copy[i].Path, copy[j].Path
		if copy[i].Type == "tree" {
			a += "/"
		}
		if copy[j].Type == "tree" {
			b += "/"
		}
		return a < b
	})
	var raw []byte
	for _, entry := range copy {
		sum, _ := hex.DecodeString(entry.SHA)
		raw = append(raw, []byte(strings.TrimLeft(entry.Mode, "0")+" "+entry.Path+"\x00")...)
		raw = append(raw, sum...)
	}
	return raw
}
func (e *Emulator) putTree(entries []Entry) string {
	sha := object("tree", treeBytes(entries))
	e.trees[sha] = append([]Entry(nil), entries...)
	return sha
}
func reply(status int, value any) *http.Response {
	data, _ := json.Marshal(value)
	return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(bytes.NewReader(data))}
}
func (e *Emulator) reject(reason string) *http.Response {
	e.snapshot.Violations = append(e.snapshot.Violations, reason)
	return reply(403, map[string]string{"message": "private-emulator-response"})
}
func decode(r *http.Request, out any) error {
	if r.Body == nil {
		return io.EOF
	}
	d := json.NewDecoder(io.LimitReader(r.Body, 8<<20))
	d.DisallowUnknownFields()
	if err := d.Decode(out); err != nil {
		return err
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return errors.New("extra body")
	}
	return nil
}
func (e *Emulator) jwt(raw string) bool {
	parts := strings.Split(raw, ".")
	if len(parts) != 3 {
		return false
	}
	header, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return false
	}
	var h struct{ Alg, Typ string }
	if json.Unmarshal(header, &h) != nil || h.Alg != "RS256" || h.Typ != "JWT" {
		return false
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return false
	}
	sum := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	if rsa.VerifyPKCS1v15(e.c.PublicKey, crypto.SHA256, sum[:], signature) != nil {
		return false
	}
	body, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return false
	}
	var claims struct {
		Issuer         string `json:"iss"`
		Issued, Expiry int64
	}
	var values map[string]json.RawMessage
	if json.Unmarshal(body, &values) != nil || json.Unmarshal(values["iss"], &claims.Issuer) != nil || json.Unmarshal(values["iat"], &claims.Issued) != nil || json.Unmarshal(values["exp"], &claims.Expiry) != nil {
		return false
	}
	now := e.c.Now().Unix()
	return claims.Issuer == strconv.FormatInt(e.c.AppID, 10) && claims.Issued <= now+30 && claims.Issued >= now-90 && claims.Expiry > now && claims.Expiry <= now+600
}
func (e *Emulator) RoundTrip(r *http.Request) (*http.Response, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.snapshot.Requests++
	if err := r.Context().Err(); err != nil {
		return nil, err
	}
	if r.URL.Scheme != "https" || r.URL.Host != "api.github.com" || r.URL.User != nil || r.Header.Get("X-GitHub-Api-Version") != APIVersion || r.Header.Get("Accept") != "application/vnd.github+json" {
		return e.reject("origin_or_version"), nil
	}
	if e.faultMethod == r.Method && e.faultPath == r.URL.Path {
		e.faultMethod, e.faultPath = "", ""
		return nil, errors.New("private-emulator-transport")
	}
	bearer := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	if r.URL.Path == fmt.Sprintf("/app/installations/%d/access_tokens", e.c.InstallationID) && r.Method == http.MethodPost {
		if !e.jwt(bearer) {
			return e.reject("app_jwt"), nil
		}
		var input struct {
			Repositories []int64           `json:"repository_ids"`
			Permissions  map[string]string `json:"permissions"`
		}
		if decode(r, &input) != nil || len(input.Repositories) != 1 || input.Repositories[0] != e.c.RepositoryID {
			return e.reject("repository_scope"), nil
		}
		write := input.Permissions["contents"] == "write"
		valid := len(input.Permissions) == 1 && input.Permissions["contents"] == "read" || len(input.Permissions) == 2 && write && input.Permissions["pull_requests"] == "write"
		if !valid {
			return e.reject("permission_scope"), nil
		}
		if write && e.denied {
			return reply(403, map[string]string{"message": "private-emulator-denied"}), nil
		}
		permissions := map[string]string{}
		for k, v := range input.Permissions {
			permissions[k] = v
		}
		tokenValue := fmt.Sprintf("fixture-installation-%d", len(e.tokens)+1)
		expires := e.c.Now().Add(time.Hour)
		e.tokens[tokenValue] = token{permissions: permissions, expires: expires}
		e.snapshot.TokenRequests = append(e.snapshot.TokenRequests, Scope{RepositoryID: e.c.RepositoryID, Permissions: permissions})
		return reply(201, map[string]any{"token": tokenValue, "expires_at": expires, "permissions": permissions, "repositories": []any{map[string]any{"id": e.c.RepositoryID}}}), nil
	}
	credential, ok := e.tokens[bearer]
	if !ok || !credential.expires.After(e.c.Now()) {
		return e.reject("installation_token"), nil
	}
	if r.URL.Path == fmt.Sprintf("/repositories/%d", e.c.RepositoryID) && r.Method == http.MethodGet {
		return reply(200, e.repository()), nil
	}
	prefix := "/repos/" + e.c.Owner + "/" + e.c.Name
	if !strings.HasPrefix(r.URL.Path, prefix+"/") {
		return e.reject("repository_route"), nil
	}
	path := strings.TrimPrefix(r.URL.Path, prefix)
	if r.Method != http.MethodGet {
		if credential.permissions["contents"] != "write" || path == "/pulls" && credential.permissions["pull_requests"] != "write" || e.denied {
			return e.reject("write_permission"), nil
		}
	}
	switch {
	case r.Method == http.MethodGet && strings.HasPrefix(path, "/commits/"):
		ref := strings.TrimPrefix(path, "/commits/")
		sha := e.refs[ref]
		if sha == "" {
			sha = ref
		}
		c, ok := e.commits[sha]
		if !ok {
			return reply(404, nil), nil
		}
		return reply(200, map[string]any{"sha": c.SHA, "commit": map[string]any{"tree": map[string]string{"sha": c.Tree}}}), nil
	case r.Method == http.MethodGet && strings.HasPrefix(path, "/git/commits/"):
		c, ok := e.commits[strings.TrimPrefix(path, "/git/commits/")]
		if !ok {
			return reply(404, nil), nil
		}
		return reply(200, map[string]any{"sha": c.SHA, "tree": map[string]string{"sha": c.Tree}}), nil
	case r.Method == http.MethodGet && strings.HasPrefix(path, "/git/trees/"):
		sha := strings.TrimPrefix(path, "/git/trees/")
		entries, ok := e.trees[sha]
		if !ok {
			return reply(404, nil), nil
		}
		return reply(200, map[string]any{"sha": sha, "tree": entries, "truncated": false}), nil
	case r.Method == http.MethodGet && strings.HasPrefix(path, "/git/blobs/"):
		sha := strings.TrimPrefix(path, "/git/blobs/")
		data, ok := e.blobs[sha]
		if !ok {
			return reply(404, nil), nil
		}
		return reply(200, map[string]any{"sha": sha, "size": len(data), "encoding": "base64", "content": base64.StdEncoding.EncodeToString(data)}), nil
	case r.Method == http.MethodPost && path == "/git/blobs":
		var input struct{ Encoding, Content string }
		if decode(r, &input) != nil || input.Encoding != "base64" {
			return e.reject("blob_contract"), nil
		}
		data, err := base64.StdEncoding.DecodeString(input.Content)
		if err != nil {
			return e.reject("blob_encoding"), nil
		}
		sha := object("blob", data)
		e.blobs[sha] = bytes.Clone(data)
		e.snapshot.Writes++
		return reply(201, map[string]string{"sha": sha}), nil
	case r.Method == http.MethodPost && path == "/git/trees":
		var input struct{ Tree []Entry }
		if decode(r, &input) != nil {
			return e.reject("tree_contract"), nil
		}
		seen := map[string]bool{}
		for _, entry := range input.Tree {
			if entry.Path == "" || strings.ContainsAny(entry.Path, "/\\\x00") || seen[entry.Path] {
				return e.reject("tree_path"), nil
			}
			seen[entry.Path] = true
			valid := entry.Type == "blob" && (entry.Mode == "100644" || entry.Mode == "100755") && e.blobs[entry.SHA] != nil || entry.Type == "tree" && entry.Mode == "040000" && e.trees[entry.SHA] != nil
			if !valid {
				return e.reject("tree_object"), nil
			}
		}
		sha := e.putTree(input.Tree)
		e.snapshot.Writes++
		return reply(201, map[string]string{"sha": sha}), nil
	case r.Method == http.MethodPost && path == "/git/commits":
		var input struct {
			Tree              string
			Parents           []string
			Message           string
			Author, Committer struct{ Name, Email, Date string }
		}
		if decode(r, &input) != nil || e.trees[input.Tree] == nil || len(input.Parents) != 1 || e.commits[input.Parents[0]].SHA == "" {
			return e.reject("commit_contract"), nil
		}
		a, err := person(input.Author.Name, input.Author.Email, input.Author.Date)
		if err != nil {
			return e.reject("commit_author"), nil
		}
		c, err := person(input.Committer.Name, input.Committer.Email, input.Committer.Date)
		if err != nil {
			return e.reject("commit_committer"), nil
		}
		raw := []byte("tree " + input.Tree + "\nparent " + input.Parents[0] + "\nauthor " + a + "\ncommitter " + c + "\n\n" + input.Message)
		sha := object("commit", raw)
		e.commits[sha] = commit{SHA: sha, Tree: input.Tree, Parents: append([]string(nil), input.Parents...)}
		e.snapshot.Writes++
		return reply(201, map[string]string{"sha": sha}), nil
	case r.Method == http.MethodGet && strings.HasPrefix(path, "/git/ref/heads/"):
		ref := strings.TrimPrefix(path, "/git/ref/heads/")
		sha, ok := e.refs[ref]
		if !ok {
			return reply(404, nil), nil
		}
		return reply(200, reference(ref, sha)), nil
	case r.Method == http.MethodPost && path == "/git/refs":
		var input struct{ Ref, SHA string }
		if decode(r, &input) != nil {
			return e.reject("ref_contract"), nil
		}
		ref := strings.TrimPrefix(input.Ref, "refs/heads/")
		if ref == "main" {
			e.snapshot.MainWrites++
			return e.reject("main_write"), nil
		}
		if !strings.HasPrefix(ref, "kingdom/") || e.commits[input.SHA].SHA == "" {
			return e.reject("ref_target"), nil
		}
		if _, ok := e.refs[ref]; ok {
			return reply(422, nil), nil
		}
		e.refs[ref] = input.SHA
		e.snapshot.Writes++
		e.snapshot.RefCreates++
		return reply(201, reference(ref, input.SHA)), nil
	case r.Method == http.MethodGet && path == "/pulls":
		if r.URL.Query().Get("state") != "all" || r.URL.Query().Get("per_page") != "100" || !strings.HasPrefix(r.URL.Query().Get("head"), e.c.Owner+":kingdom/") || r.URL.Query().Get("base") != "main" {
			return e.reject("pull_lookup"), nil
		}
		out := []any{}
		if !e.hidePulls {
			for _, p := range e.pulls {
				if e.c.Owner+":"+p.HeadRef == r.URL.Query().Get("head") && p.BaseRef == r.URL.Query().Get("base") {
					out = append(out, e.pullResponse(p))
				}
			}
		}
		return reply(200, out), nil
	case r.Method == http.MethodPost && path == "/pulls":
		var input struct{ Title, Body, Head, Base string }
		if decode(r, &input) != nil || input.Base != "main" || !strings.HasPrefix(input.Head, "kingdom/") || e.refs[input.Head] == "" {
			return e.reject("pull_contract"), nil
		}
		p := pullRecord{Pull: Pull{Number: int64(len(e.pulls) + 1), HeadRef: input.Head, HeadSHA: e.refs[input.Head], BaseRef: input.Base}, body: input.Body, title: input.Title}
		e.pulls = append(e.pulls, p)
		e.snapshot.Writes++
		e.snapshot.PullCreates++
		if e.losePull {
			e.losePull = false
			return nil, errors.New("private-emulator-lost-pull-reply")
		}
		return reply(201, e.pullResponse(p)), nil
	default:
		return e.reject("unsupported_endpoint"), nil
	}
}
func person(name, email, date string) (string, error) {
	at, err := time.Parse(time.RFC3339, date)
	if err != nil || strings.ContainsAny(name+email, "\r\n<>") || name == "" || email == "" {
		return "", errors.New("invalid person")
	}
	return fmt.Sprintf("%s <%s> %d +0000", name, email, at.Unix()), nil
}
func reference(ref, sha string) any {
	return map[string]any{"ref": "refs/heads/" + ref, "object": map[string]string{"sha": sha, "type": "commit"}}
}
func (e *Emulator) repository() any {
	return map[string]any{"id": e.c.RepositoryID, "name": e.c.Name, "owner": map[string]string{"login": e.c.Owner}, "default_branch": "main"}
}
func (e *Emulator) pullResponse(p pullRecord) any {
	return map[string]any{"number": p.Number, "title": p.title, "body": p.body, "head": map[string]any{"ref": p.HeadRef, "sha": p.HeadSHA, "repo": e.repository()}, "base": map[string]any{"ref": p.BaseRef, "repo": e.repository()}}
}
func (e *Emulator) Snapshot() Snapshot {
	e.mu.Lock()
	defer e.mu.Unlock()
	s := e.snapshot
	s.Refs = map[string]string{}
	s.FilesByRef = map[string]map[string]File{}
	s.Pulls = make([]Pull, 0, len(e.pulls))
	for _, pull := range e.pulls {
		s.Pulls = append(s.Pulls, pull.Pull)
	}
	s.Violations = append([]string(nil), s.Violations...)
	s.TokenRequests = append([]Scope(nil), s.TokenRequests...)
	for i, scope := range s.TokenRequests {
		s.TokenRequests[i].Permissions = map[string]string{}
		for k, v := range scope.Permissions {
			s.TokenRequests[i].Permissions[k] = v
		}
	}
	var walk func(string, string, map[string]File)
	walk = func(tree, prefix string, out map[string]File) {
		for _, entry := range e.trees[tree] {
			if entry.Type == "tree" {
				walk(entry.SHA, prefix+entry.Path+"/", out)
			} else {
				out[prefix+entry.Path] = File{Data: bytes.Clone(e.blobs[entry.SHA]), Mode: entry.Mode}
			}
		}
	}
	for ref, sha := range e.refs {
		s.Refs[ref] = sha
		files := map[string]File{}
		walk(e.commits[sha].Tree, "", files)
		s.FilesByRef[ref] = files
	}
	return s
}
