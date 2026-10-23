package modelcatalog

import (
	"bytes"
	"context"
	"crypto/x509"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"kingdom/internal/central/catalog"
	"kingdom/internal/central/connections"
	"kingdom/internal/central/modelproxy"
	_ "modernc.org/sqlite"
)

type fixture struct {
	service       *Service
	catalog       *catalog.Store
	vault         *connections.Store
	owner, member catalog.User
	project       catalog.Project
	connection    connections.Metadata
	calls         atomic.Int32
	body          atomic.Value
	now           time.Time
	upstream      *httptest.Server
	entered       chan struct{}
	release       chan struct{}
}

func setup(t *testing.T) *fixture {
	t.Helper()
	f := &fixture{entered: make(chan struct{}, 1), release: make(chan struct{}), now: time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)}
	f.body.Store(`{"object":"list","data":[{"id":"model"},{"id":"unknown"}]}`)
	f.upstream = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.calls.Add(1)
		if r.Method != "GET" || r.URL.Path != "/v1/models" || r.Header.Get("Authorization") != "Bearer fixture-secret" {
			t.Error("wrong provider discovery request")
		}
		if f.body.Load().(string) == "__wait__" {
			f.entered <- struct{}{}
			select {
			case <-f.release:
			case <-r.Context().Done():
				return
			}
		}
		w.Header().Set("Content-Type", "application/json")
		body := f.body.Load().(string)
		if body == "__wait__" {
			body = `{"object":"list","data":[{"id":"model"}]}`
		}
		w.Write([]byte(body))
	}))
	t.Cleanup(f.upstream.Close)
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "central.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	id := catalog.Identity{Issuer: "https://issuer.example", Subject: "owner"}
	f.catalog, err = catalog.New(context.Background(), db, catalog.Config{InitialOwners: []catalog.Identity{id}})
	if err != nil {
		t.Fatal(err)
	}
	f.owner, err = f.catalog.BootstrapOwner(context.Background(), id, "Owner")
	if err != nil {
		t.Fatal(err)
	}
	f.member, err = f.catalog.CreateUser(context.Background(), f.owner.ID, catalog.Identity{Issuer: id.Issuer, Subject: "member"}, "Member")
	if err != nil {
		t.Fatal(err)
	}
	f.project, err = f.catalog.CreateProject(context.Background(), f.owner.ID, "Project")
	if err != nil {
		t.Fatal(err)
	}
	if err = f.catalog.SetMembership(context.Background(), f.owner.ID, f.project.ID, f.member.ID, catalog.Member); err != nil {
		t.Fatal(err)
	}
	f.vault, err = connections.New(context.Background(), db, bytes.Repeat([]byte{1}, 32))
	if err != nil {
		t.Fatal(err)
	}
	f.connection, err = f.vault.Create(context.Background(), f.member.ID, connections.Personal, connections.Definition{Label: "Custom", Provider: "custom", Endpoint: f.upstream.URL + "/v1", AuthKind: "api_key"}, []byte("fixture-secret"))
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(f.upstream.Certificate())
	f.service, err = New(Config{Connections: f.vault, Origins: []modelproxy.OriginPolicy{{Origin: f.upstream.URL, AllowedPrivateAddresses: []netip.Prefix{netip.MustParsePrefix("127.0.0.1/32")}}}, TLSRoots: roots, Profiles: []Profile{{Provider: "custom", Endpoint: f.upstream.URL + "/v1", Models: []Model{{ID: "model", Name: "Test Model", Efforts: []string{"medium", "high"}}}}}, TTL: time.Minute, Now: func() time.Time { return f.now }})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.service.Close(context.Background()) })
	return f
}
func (f *fixture) refresh() (Snapshot, error) {
	return f.service.Refresh(context.Background(), f.member.ID, f.project.ID, f.connection.ID, 1)
}
func TestExactRevisionCatalogIsAuthorizedAndPureMemory(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	if _, err := f.service.Refresh(ctx, f.owner.ID, f.project.ID, f.connection.ID, 1); err == nil || f.calls.Load() != 0 {
		t.Fatal("personal credential exposed to owner")
	}
	got, err := f.refresh()
	if err != nil || len(got.Models) != 1 {
		t.Fatal(got, err)
	}
	raw, _ := json.Marshal(got)
	if strings.Contains(string(raw), "fixture-secret") || strings.Contains(string(raw), f.upstream.URL) {
		t.Fatal("private discovery details leaked")
	}
	if err = f.service.Validate(ctx, f.connection, "custom/model", "medium"); err != nil {
		t.Fatal(err)
	}
	if err = f.service.Validate(ctx, f.connection, "custom/model", "low"); err == nil {
		t.Fatal("guessed effort accepted")
	}
	rotated, err := f.vault.Revise(ctx, f.member.ID, f.connection.ID, 1, f.connection.Definition, []byte("second-secret"))
	if err != nil {
		t.Fatal(err)
	}
	if err = f.service.Validate(ctx, rotated, "custom/model", "medium"); err == nil {
		t.Fatal("catalog drifted to new revision")
	}
	if err = f.service.Validate(ctx, f.connection, "custom/model", "medium"); err != nil {
		t.Fatal(err)
	}
	if f.calls.Load() != 1 {
		t.Fatal("validation performed discovery")
	}
	if err = f.catalog.RevokeMembership(ctx, f.owner.ID, f.project.ID, f.member.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = f.service.List(ctx, f.member.ID, f.project.ID, f.connection.ID, 1); err == nil {
		t.Fatal("revoked member listed catalog")
	}
	f.now = f.now.Add(2 * time.Minute)
	if err = f.service.Validate(ctx, f.connection, "custom/model", "medium"); err == nil {
		t.Fatal("expired cache authorized selection")
	}
}
func TestDiscoveryRejectsMalformedAndDuplicateModels(t *testing.T) {
	for _, body := range []string{`{"object":"list","data":[{"id":"model"},{"id":"model"}]}`, `{"object":"list","data":[],"data":[{"id":"model"}]}`, `{"object":"list","data":[{"id":"model","id":"unknown"}]}`, `{"object":"list","data":[{"id":"bad\nname"}]}`, strings.Repeat("x", 2*1024*1024+1)} {
		f := setup(t)
		if _, err := f.refresh(); err != nil {
			t.Fatal(err)
		}
		f.body.Store(body)
		if _, err := f.refresh(); err == nil {
			t.Fatal("malformed response accepted")
		}
		if err := f.service.Validate(context.Background(), f.connection, "custom/model", "medium"); err == nil {
			t.Fatal("failed refresh retained selection authority")
		}
	}
}
func TestCatalogStartupDoesNotUseHostOrCredentials(t *testing.T) {
	f := setup(t)
	v, err := f.service.ForProject(f.project.ID).Snapshot(context.Background(), true)
	if err != nil || v.Status != "unavailable" || f.calls.Load() != 0 {
		t.Fatal(v, err)
	}
}
func TestPinnedRegistryContainsExactLunaEfforts(t *testing.T) {
	var p Profile
	if json.Unmarshal(builtin, &p) != nil {
		t.Fatal("profile invalid")
	}
	for _, m := range p.Models {
		if m.ID == "gpt-6-luna" {
			if strings.Join(m.Efforts, ",") != "none,low,medium,high,xhigh,max" {
				t.Fatal(m)
			}
			return
		}
	}
	t.Fatal("pinned Luna model missing")
}

func TestDiscoveryRechecksRevocationAndJoinsOnClose(t *testing.T) {
	for _, mode := range []string{"revoke", "close"} {
		t.Run(mode, func(t *testing.T) {
			f := setup(t)
			f.body.Store("__wait__")
			done := make(chan error, 1)
			go func() { _, err := f.refresh(); done <- err }()
			select {
			case <-f.entered:
			case <-time.After(3 * time.Second):
				t.Fatal("discovery did not enter")
			}
			if mode == "revoke" {
				if err := f.catalog.RevokeMembership(context.Background(), f.owner.ID, f.project.ID, f.member.ID); err != nil {
					t.Fatal(err)
				}
				close(f.release)
			} else {
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				if err := f.service.Close(ctx); err != nil {
					t.Fatal(err)
				}
			}
			select {
			case err := <-done:
				if err == nil {
					t.Fatal("revoked discovery published")
				}
			case <-time.After(3 * time.Second):
				t.Fatal("discovery was not joined")
			}
			if err := f.service.Validate(context.Background(), f.connection, "custom/model", "medium"); err == nil {
				t.Fatal("closed/revoked discovery cached")
			}
		})
	}
}

func TestDiscoveryTransportRejectsRedirectsAndUnapprovedPrivateAddresses(t *testing.T) {
	f := setup(t)
	roots := x509.NewCertPool()
	roots.AddCert(f.upstream.Certificate())
	client, err := modelproxy.DiscoveryClient([]modelproxy.OriginPolicy{{Origin: f.upstream.URL}}, roots)
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	if _, err = client.Get(f.upstream.URL + "/v1/models"); err == nil {
		t.Fatal("implicit private address allowed")
	}
	if f.calls.Load() != 0 {
		t.Fatal("private endpoint reached")
	}
	var targetCalls atomic.Int32
	target := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { targetCalls.Add(1); w.WriteHeader(200) }))
	defer target.Close()
	redirect := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL+"/models", http.StatusFound)
	}))
	defer redirect.Close()
	roots.AddCert(redirect.Certificate())
	roots.AddCert(target.Certificate())
	policy := []modelproxy.OriginPolicy{}
	for _, origin := range []string{redirect.URL, target.URL} {
		policy = append(policy, modelproxy.OriginPolicy{Origin: origin, AllowedPrivateAddresses: []netip.Prefix{netip.MustParsePrefix("127.0.0.1/32")}})
	}
	client, err = modelproxy.DiscoveryClient(policy, roots)
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	response, err := client.Get(redirect.URL + "/models")
	if response != nil {
		response.Body.Close()
	}
	if err == nil || targetCalls.Load() != 0 {
		t.Fatal("redirect followed")
	}
}
