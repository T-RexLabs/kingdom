package repositories

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"io"
	"math/big"
	"net/http"
	"strings"
	"testing"
	"time"
)

type roundTrip func(*http.Request) (*http.Response, error)

func (f roundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func fixture(t *testing.T) *Client {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	c, err := NewClient(Config{AppID: 1, PrivateKey: key, Grants: []Grant{{ProjectID: "project", InstallationID: 2, RepositoryID: 3, Publish: true}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}
func response(status int, value any) *http.Response {
	data, _ := json.Marshal(value)
	return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(string(data))), Header: make(http.Header)}
}
func TestRepositoryCredentialsAreNarrowAndSeparate(t *testing.T) {
	for _, publish := range []bool{false, true} {
		t.Run(map[bool]string{false: "read", true: "publish"}[publish], func(t *testing.T) {
			c := fixture(t)
			calls := 0
			c.http.Transport = roundTrip(func(r *http.Request) (*http.Response, error) {
				calls++
				if r.URL.Scheme != "https" || r.URL.Host != "api.github.com" || r.Header.Get("X-GitHub-Api-Version") != apiVersion {
					t.Fatal("incorrect endpoint")
				}
				if calls == 1 {
					if r.URL.Path != "/app/installations/2/access_tokens" || !strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ey") {
						t.Fatal("missing app JWT")
					}
					var body struct {
						Repositories []int64           `json:"repository_ids"`
						Permissions  map[string]string `json:"permissions"`
					}
					json.NewDecoder(r.Body).Decode(&body)
					if len(body.Repositories) != 1 || body.Repositories[0] != 3 {
						t.Fatal("unscoped repository")
					}
					want := "read"
					if publish {
						want = "write"
					}
					if body.Permissions["contents"] != want || (!publish && body.Permissions["pull_requests"] != "") {
						t.Fatal("wrong scope")
					}
					return response(201, map[string]any{"token": "private-token", "expires_at": time.Now().Add(time.Hour), "permissions": body.Permissions, "repositories": []any{map[string]any{"id": 3}}}), nil
				}
				if calls != 2 || r.URL.Path != "/repositories/3" || r.Header.Get("Authorization") != "Bearer private-token" {
					t.Fatal("wrong repository lookup")
				}
				return response(200, map[string]any{"id": 3, "name": "repo", "owner": map[string]any{"login": "owner"}}), nil
			})
			a, err := c.authorize(context.Background(), "project", publish)
			if err != nil || a.repo.ID != 3 || calls != 2 {
				t.Fatalf("authorize: %v", err)
			}
			if _, err := c.authorize(context.Background(), "foreign", false); err != ErrDenied || calls != 2 {
				t.Fatal("foreign project acquired credentials")
			}
		})
	}
}
func TestRepositoryRejectsBroaderTokenAndRedirect(t *testing.T) {
	for _, scenario := range []string{"broader", "foreign", "expired", "redirect"} {
		t.Run(scenario, func(t *testing.T) {
			c := fixture(t)
			calls := 0
			c.http.Transport = roundTrip(func(r *http.Request) (*http.Response, error) {
				calls++
				if calls > 1 {
					t.Fatal("used rejected token")
				}
				if scenario == "redirect" {
					res := response(302, map[string]string{"secret": "private"})
					res.Header.Set("Location", "https://other.invalid/secret")
					return res, nil
				}
				p := map[string]string{"contents": "read"}
				id := int64(3)
				expiry := time.Now().Add(time.Hour)
				switch scenario {
				case "broader":
					p["issues"] = "write"
				case "foreign":
					id = 4
				case "expired":
					expiry = time.Now().Add(-time.Minute)
				}
				return response(201, map[string]any{"token": "private-token", "expires_at": expiry, "permissions": p, "repositories": []any{map[string]any{"id": id}}}), nil
			})
			_, err := c.authorize(context.Background(), "project", false)
			if err == nil || strings.Contains(err.Error(), "private") {
				t.Fatal("unsafe token accepted or leaked")
			}
		})
	}
}

func TestMalformedAppKeyFailsWithoutPanic(t *testing.T) {
	for _, key := range []*rsa.PrivateKey{nil, {}, {PublicKey: rsa.PublicKey{N: new(big.Int).Lsh(big.NewInt(1), 2048), E: 65537}}} {
		if _, err := NewClient(Config{AppID: 1, PrivateKey: key}); err != ErrInvalid {
			t.Fatal("malformed key accepted")
		}
	}
}
