package githubemu

import (
	"bytes"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func testJWT(t *testing.T, key *rsa.PrivateKey, issuer string, now time.Time, expiry time.Duration) string {
	t.Helper()
	raw, _ := json.Marshal(map[string]any{"iss": issuer, "iat": now.Add(-time.Minute).Unix(), "exp": now.Add(expiry).Unix()})
	value := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"RS256","typ":"JWT"}`)) + "." + base64.RawURLEncoding.EncodeToString(raw)
	sum := sha256.Sum256([]byte(value))
	sig, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, sum[:])
	if err != nil {
		t.Fatal(err)
	}
	return value + "." + base64.RawURLEncoding.EncodeToString(sig)
}
func call(t *testing.T, e *Emulator, method, path, bearer string, body any) *http.Response {
	t.Helper()
	raw, _ := json.Marshal(body)
	r, _ := http.NewRequest(method, "https://api.github.com"+path, bytes.NewReader(raw))
	r.Header.Set("Authorization", "Bearer "+bearer)
	r.Header.Set("Accept", "application/vnd.github+json")
	r.Header.Set("X-GitHub-Api-Version", APIVersion)
	response, err := e.RoundTrip(r)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { response.Body.Close() })
	return response
}
func TestEmulatorVerifiesJWTScopesAndFixedOrigin(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	e, err := New(Config{AppID: 1, InstallationID: 2, RepositoryID: 3, PublicKey: &key.PublicKey, Owner: "owner", Name: "repo", Files: map[string]File{"keep.sh": {Data: []byte("keep\n"), Mode: "100755"}}, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	input := map[string]any{"repository_ids": []int64{3}, "permissions": map[string]string{"contents": "read"}}
	jwt := testJWT(t, key, "1", now, time.Minute)
	for _, bad := range []string{jwt + "x", testJWT(t, key, "1", now, -time.Minute), testJWT(t, key, "2", now, time.Minute)} {
		if got := call(t, e, "POST", "/app/installations/2/access_tokens", bad, input); got.StatusCode != 403 {
			t.Fatal("invalid JWT admitted")
		}
	}
	badScope := map[string]any{"repository_ids": []int64{4}, "permissions": map[string]string{"contents": "read"}}
	if call(t, e, "POST", "/app/installations/2/access_tokens", jwt, badScope).StatusCode != 403 {
		t.Fatal("foreign repository scope admitted")
	}
	broader := map[string]any{"repository_ids": []int64{3}, "permissions": map[string]string{"contents": "read", "issues": "write"}}
	if call(t, e, "POST", "/app/installations/2/access_tokens", jwt, broader).StatusCode != 403 {
		t.Fatal("broader permissions admitted")
	}
	response := call(t, e, "POST", "/app/installations/2/access_tokens", jwt, input)
	var token struct{ Token string }
	if response.StatusCode != 201 || json.NewDecoder(response.Body).Decode(&token) != nil {
		t.Fatal("read token rejected")
	}
	if call(t, e, "POST", "/repos/owner/repo/git/blobs", token.Token, map[string]string{"encoding": "base64", "content": "aGk="}).StatusCode != 403 {
		t.Fatal("read credential wrote blob")
	}
	if call(t, e, "GET", "/repositories/4", token.Token, nil).StatusCode != 403 {
		t.Fatal("foreign repository read admitted")
	}
	for _, tc := range []struct{ origin, version string }{{"http://api.github.com", APIVersion}, {"https://other.invalid", APIVersion}, {"https://api.github.com", "old"}} {
		r, _ := http.NewRequest("GET", tc.origin+"/repositories/3", nil)
		r.Header.Set("X-GitHub-Api-Version", tc.version)
		r.Header.Set("Accept", "application/vnd.github+json")
		r.Header.Set("Authorization", "Bearer "+token.Token)
		response, err := e.RoundTrip(r)
		if err != nil || response.StatusCode != 403 {
			t.Fatal("origin/version bypass")
		}
		response.Body.Close()
	}
	snapshot := e.Snapshot()
	if snapshot.Writes != 0 || snapshot.MainWrites != 0 || len(snapshot.TokenRequests) != 1 {
		t.Fatal("negative contract mutated repository")
	}
	raw, _ := json.Marshal(snapshot)
	for _, secret := range []string{jwt, token.Token, "private-emulator-response"} {
		if strings.Contains(string(raw), secret) {
			t.Fatal("private request data escaped snapshot")
		}
	}
}
func TestEmulatorUsesRealGitObjectsAndIndependentSnapshots(t *testing.T) {
	if actual := object("blob", nil); actual != "e69de29bb2d1d6434b8b29ae775ad8c2e48c5391" {
		t.Fatal("Git object identity differs")
	}
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("preserved\n")
	e, err := New(Config{AppID: 1, InstallationID: 2, RepositoryID: 3, PublicKey: &key.PublicKey, Owner: "owner", Name: "repo", Files: map[string]File{"nested/keep.sh": {Data: data, Mode: "100755"}}})
	if err != nil {
		t.Fatal(err)
	}
	data[0] = 'x'
	first := e.Snapshot()
	if string(first.FilesByRef["main"]["nested/keep.sh"].Data) != "preserved\n" {
		t.Fatal("fixture retained caller buffer")
	}
	first.FilesByRef["main"]["nested/keep.sh"].Data[0] = 'x'
	first.Refs["main"] = "changed"
	if actual := e.Snapshot(); actual.Refs["main"] != actual.MainCommit || string(actual.FilesByRef["main"]["nested/keep.sh"].Data) != "preserved\n" {
		t.Fatal("snapshot mutation rewrote emulator")
	}
	jwt := testJWT(t, key, "1", time.Now(), time.Minute)
	input := map[string]any{"repository_ids": []int64{3}, "permissions": map[string]string{"contents": "write", "pull_requests": "write"}}
	issued := call(t, e, "POST", "/app/installations/2/access_tokens", jwt, input)
	var token struct{ Token string }
	json.NewDecoder(issued.Body).Decode(&token)
	e.FailNext("GET", "/repositories/3")
	r, _ := http.NewRequest("GET", "https://api.github.com/repositories/3", nil)
	r.Header.Set("X-GitHub-Api-Version", APIVersion)
	r.Header.Set("Accept", "application/vnd.github+json")
	r.Header.Set("Authorization", "Bearer "+token.Token)
	if _, err := e.RoundTrip(r); err == nil {
		t.Fatal("connection fault absent")
	}
	if call(t, e, "GET", "/repositories/3", token.Token, nil).StatusCode != 200 {
		t.Fatal("one-shot connection fault persisted")
	}
	e.DenyWrites(true)
	denied := call(t, e, "POST", "/repos/owner/repo/git/blobs", token.Token, map[string]string{"encoding": "base64", "content": "aGk="})
	raw, _ := io.ReadAll(denied.Body)
	if denied.StatusCode != 403 || e.Snapshot().Writes != 0 {
		t.Fatal("permission denial wrote", fmt.Sprint(raw))
	}
}
