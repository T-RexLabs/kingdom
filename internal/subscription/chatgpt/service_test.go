package chatgpt

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	jose "github.com/go-jose/go-jose/v4"
	_ "modernc.org/sqlite"
)

type transportFunc func(*http.Request) (*http.Response, error)

func (f transportFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type fixture struct {
	t                                   *testing.T
	s                                   *Service
	v                                   *Vault
	nonce, challenge, redirect, subject string
	mutate                              func(map[string]any)
	fail                                bool
	lost                                bool
	calls                               atomic.Int32
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	db, e := sql.Open("sqlite", filepath.Join(t.TempDir(), "auth.db"))
	if e != nil {
		t.Fatal(e)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })
	v, e := NewVault(context.Background(), db, bytes.Repeat([]byte{1}, 32))
	if e != nil {
		t.Fatal(e)
	}
	f := &fixture{t: t, v: v, subject: "account-one"}
	key := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{2}, 32))
	signer, e := jose.NewSigner(jose.SigningKey{Algorithm: jose.EdDSA, Key: key}, (&jose.SignerOptions{}).WithHeader("kid", "test"))
	if e != nil {
		t.Fatal(e)
	}
	tr := transportFunc(func(r *http.Request) (*http.Response, error) {
		var body any
		status := 200
		switch r.URL.Path {
		case "/.well-known/openid-configuration":
			body = map[string]any{"issuer": issuer, "authorization_endpoint": authorizeURL, "token_endpoint": tokenURL, "jwks_uri": issuer + "/keys", "revocation_endpoint": issuer + "/revoke", "id_token_signing_alg_values_supported": []string{"EdDSA"}}
		case "/keys":
			body = jose.JSONWebKeySet{Keys: []jose.JSONWebKey{{Key: key.Public(), KeyID: "test", Algorithm: "EdDSA", Use: "sig"}}}
		case "/api/accounts/oauth/token":
			f.calls.Add(1)
			if f.lost {
				return nil, io.ErrUnexpectedEOF
			}
			r.ParseForm()
			if r.Form.Get("client_id") != "oaiapp_kingdom" || r.Form.Get("resource") != resource {
				t.Error("wrong token request binding")
			}
			if r.Form.Get("grant_type") == "authorization_code" {
				var count int
				e := db.QueryRow(`SELECT count(*) FROM chatgpt_registrations WHERE client_id='oaiapp_kingdom'`).Scan(&count)
				if e != nil || count != 1 {
					t.Error("issued client not durably retained before exchange")
				}
				h := sha256.Sum256([]byte(r.Form.Get("code_verifier")))
				if base64.RawURLEncoding.EncodeToString(h[:]) != f.challenge || r.Form.Get("redirect_uri") != f.redirect {
					t.Error("PKCE or callback mismatch")
				}
			} else if r.Form.Get("scope") != "" {
				t.Error("refresh scope must be absent")
			}
			if f.fail {
				status = 400
				body = map[string]string{"error": "invalid_grant"}
				break
			}
			claims := map[string]any{"iss": issuer, "aud": "oaiapp_kingdom", "sub": f.subject, "nonce": f.nonce, "exp": time.Now().Add(time.Hour).Unix()}
			if f.mutate != nil {
				f.mutate(claims)
			}
			b, _ := json.Marshal(claims)
			signed, _ := signer.Sign(b)
			id, _ := signed.CompactSerialize()
			body = map[string]any{"access_token": "access-token", "refresh_token": "refresh-token", "id_token": id, "expires_in": 3600, "token_type": "bEaReR", "scope": scopes}
		case "/revoke":
			if f.lost {
				return nil, io.ErrUnexpectedEOF
			}
			body = map[string]any{}
		default:
			t.Errorf("unexpected endpoint %s", r.URL.Path)
		}
		b, _ := json.Marshal(body)
		return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(b)), Request: r}, nil
	})
	f.s, e = NewService(context.Background(), v, &http.Client{Transport: tr})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if e := f.s.Close(ctx); e != nil {
			t.Error(e)
		}
	})
	return f
}
func (f *fixture) flow(id string) *Flow {
	f.t.Helper()
	fl, e := f.s.Begin(context.Background(), "owner", id)
	if e != nil {
		f.t.Fatal(e)
	}
	f.t.Cleanup(fl.Close)
	u, _ := url.Parse(fl.AuthorizationURL())
	q := u.Query()
	f.nonce = q.Get("nonce")
	f.challenge = q.Get("code_challenge")
	f.redirect = q.Get("redirect_uri")
	if q.Get("ext_agent_host_id") != f.v.HostID() {
		f.t.Fatal("host binding missing")
	}
	return fl
}
func (f *fixture) callback(fl *Flow) (Registration, error) {
	u, _ := url.Parse(fl.redirect)
	u.RawQuery = url.Values{"state": {fl.state}, "code": {"one-use-code"}, "client_id": {"oaiapp_kingdom"}}.Encode()
	r := httptest.NewRequest("GET", u.String(), nil)
	w := httptest.NewRecorder()
	fl.ServeHTTP(w, r)
	return fl.Wait(context.Background())
}
func TestRegistrationRetainedBeforeFailedExchange(t *testing.T) {
	f := newFixture(t)
	fl := f.flow("")
	f.fail = true
	if _, e := f.callback(fl); e == nil {
		t.Fatal("failed exchange accepted")
	}
	r, e := f.v.Get(context.Background(), "owner", fl.id)
	if e != nil || r.ClientID != "oaiapp_kingdom" || r.State != "exchanging" {
		t.Fatalf("lost registration: %+v %v", r, e)
	}
	next := f.flow(fl.id)
	u, _ := url.Parse(next.AuthorizationURL())
	if u.Query().Get("client_id") != r.ClientID || u.Query().Has("agent_name_hint") {
		t.Fatal("reauth uses a borrowed or new client")
	}
}
func TestCallbackBindingAndAccountContinuity(t *testing.T) {
	f := newFixture(t)
	fl := f.flow("")
	r := httptest.NewRequest("GET", fl.redirect+"?state="+fl.state+"&code=x", nil)
	r.Host = "attacker.example"
	w := httptest.NewRecorder()
	fl.ServeHTTP(w, r)
	if w.Code != 400 || f.calls.Load() != 0 {
		t.Fatal("wrong Host accepted")
	}
	reg, e := f.callback(fl)
	if e != nil {
		t.Fatal(e)
	}
	f.subject = "other-account"
	if _, e = f.callback(f.flow(reg.ID)); e == nil {
		t.Fatal("account replacement accepted")
	}
	if _, e = f.v.Get(context.Background(), "other-owner", reg.ID); !errors.Is(e, ErrDenied) {
		t.Fatal("cross-owner access")
	}
}
func TestIDTokenClaimsRejected(t *testing.T) {
	for _, claim := range []string{"nonce", "iss", "aud", "sub", "exp"} {
		t.Run(claim, func(t *testing.T) {
			f := newFixture(t)
			f.mutate = func(m map[string]any) {
				m[claim] = "wrong"
				if claim == "sub" {
					m[claim] = ""
				}
				if claim == "exp" {
					m[claim] = time.Now().Add(-time.Hour).Unix()
				}
			}
			if _, e := f.callback(f.flow("")); e == nil {
				t.Fatal("invalid ID token accepted")
			}
		})
	}
}
func TestRefreshIsSerializedAndUncertainRefreshIsNeverReplayed(t *testing.T) {
	f := newFixture(t)
	reg, e := f.callback(f.flow(""))
	if e != nil {
		t.Fatal(e)
	}
	f.s.now = func() time.Time { return time.Now().Add(3541 * time.Second) }
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, e := f.s.Access(context.Background(), "owner", reg.ID)
			if e != nil {
				t.Error(e)
			}
		}()
	}
	wg.Wait()
	if f.calls.Load() != 2 {
		t.Fatalf("refresh count %d", f.calls.Load())
	}
	f.s.now = func() time.Time { return time.Now().Add(7100 * time.Second) }
	f.lost = true
	_, _, e = f.s.Access(context.Background(), "owner", reg.ID)
	if !errors.Is(e, ErrReauthenticate) {
		t.Fatal(e)
	}
	calls := f.calls.Load()
	_, _, e = f.s.Access(context.Background(), "owner", reg.ID)
	if !errors.Is(e, ErrReauthenticate) || f.calls.Load() != calls {
		t.Fatal("uncertain refresh replayed")
	}
}
func TestVaultCiphertextIsolationAndWrongKey(t *testing.T) {
	f := newFixture(t)
	reg, e := f.callback(f.flow(""))
	if e != nil {
		t.Fatal(e)
	}
	if _, e = NewVault(context.Background(), f.v.db, bytes.Repeat([]byte{3}, 32)); !errors.Is(e, ErrDenied) {
		t.Fatal("wrong key accepted")
	}
	r, e := f.v.read(context.Background(), "owner", reg.ID)
	if e != nil {
		t.Fatal(e)
	}
	r.ID = "substitute"
	if _, e = f.v.decrypt(r); !errors.Is(e, ErrDenied) {
		t.Fatal("substituted ciphertext accepted")
	}
	if bytes.Contains(r.encrypted, []byte("refresh-token")) {
		t.Fatal("plaintext token persisted")
	}
}
func TestDisconnectFencesUseBeforeRemoteConfirmation(t *testing.T) {
	f := newFixture(t)
	reg, e := f.callback(f.flow(""))
	if e != nil {
		t.Fatal(e)
	}
	f.lost = true
	if yes, e := f.s.Disconnect(context.Background(), "owner", reg.ID); yes || e == nil {
		t.Fatal("uncertain revocation reported confirmed")
	}
	if _, _, e = f.s.Access(context.Background(), "owner", reg.ID); !errors.Is(e, ErrReauthenticate) {
		t.Fatal("revoking credential usable")
	}
	f.lost = false
	if yes, e := f.s.Disconnect(context.Background(), "owner", reg.ID); !yes || e != nil {
		t.Fatal(e)
	}
}

func TestDisconnectFencesUseBeforeOfflineDiscovery(t *testing.T) {
	f := newFixture(t)
	reg, e := f.callback(f.flow(""))
	if e != nil {
		t.Fatal(e)
	}
	calls := 0
	offline, e := NewService(context.Background(), f.v, &http.Client{Transport: transportFunc(func(r *http.Request) (*http.Response, error) {
		calls++
		current, err := f.v.Get(r.Context(), "owner", reg.ID)
		if err != nil || current.State != "revoking" {
			t.Error("remote discovery started before durable local disconnect")
		}
		return nil, io.ErrUnexpectedEOF
	})})
	if e != nil {
		t.Fatal("offline service cannot disconnect local credentials:", e)
	}
	defer offline.Close(context.Background())
	if confirmed, e := offline.Disconnect(context.Background(), "owner", reg.ID); confirmed || e == nil || calls != 1 {
		t.Fatal("offline revocation was not reported as uncertain", e)
	}
	if _, _, e := f.s.Access(context.Background(), "owner", reg.ID); !errors.Is(e, ErrReauthenticate) {
		t.Fatal("another runtime can still use the disconnected credential")
	}
	if confirmed, e := f.s.Disconnect(context.Background(), "owner", reg.ID); !confirmed || e != nil {
		t.Fatal("remote revocation cannot resume after discovery recovers", e)
	}
}

func TestDisconnectAfterLostRefreshCannotConfirmRemoteRevocation(t *testing.T) {
	f := newFixture(t)
	reg, e := f.callback(f.flow(""))
	if e != nil {
		t.Fatal(e)
	}
	f.s.now = func() time.Time { return time.Now().Add(3541 * time.Second) }
	f.lost = true
	if _, _, e = f.s.Access(context.Background(), "owner", reg.ID); !errors.Is(e, ErrReauthenticate) {
		t.Fatal(e)
	}
	f.lost = false
	if confirmed, e := f.s.Disconnect(context.Background(), "owner", reg.ID); confirmed || !errors.Is(e, ErrReauthenticate) {
		t.Fatal("old-token revocation cannot prove lost replacement revoked")
	}
	if confirmed, e := f.s.Disconnect(context.Background(), "owner", reg.ID); confirmed || !errors.Is(e, ErrReauthenticate) {
		t.Fatal("uncertainty lost on repeat")
	}
	r, e := f.v.read(context.Background(), "owner", reg.ID)
	if e != nil || len(r.encrypted) != 0 || r.State != "signed_out_uncertain" {
		t.Fatal("local credential not cleared with durable uncertainty")
	}
}
func TestHostIdentityPersists(t *testing.T) {
	f := newFixture(t)
	v, e := NewVault(context.Background(), f.v.db, bytes.Repeat([]byte{1}, 32))
	if e != nil || v.HostID() != f.v.HostID() {
		t.Fatal("host identity changed")
	}
}
func TestDisconnectInvalidatesPendingReauthentication(t *testing.T) {
	f := newFixture(t)
	reg, e := f.callback(f.flow(""))
	if e != nil {
		t.Fatal(e)
	}
	pending := f.flow(reg.ID)
	if ok, e := f.s.Disconnect(context.Background(), "owner", reg.ID); !ok || e != nil {
		t.Fatal(e)
	}
	if _, e = f.callback(pending); e == nil {
		t.Fatal("sign-in started before disconnect restored credentials")
	}
}

func TestLostCodeExchangeRetainsDisconnectUncertainty(t *testing.T) {
	f := newFixture(t)
	fl := f.flow("")
	f.lost = true
	if _, e := f.callback(fl); e == nil {
		t.Fatal("lost exchange accepted")
	}
	f.lost = false
	if confirmed, e := f.s.Disconnect(context.Background(), "owner", fl.id); confirmed || !errors.Is(e, ErrReauthenticate) {
		t.Fatal("lost exchange remote grant assumed absent")
	}
}

func TestCallbackIssuerIdentification(t *testing.T) {
	f := newFixture(t)
	fl := f.flow("")
	for _, value := range []string{"https://wrong.example", issuer} {
		r := httptest.NewRequest("GET", fl.redirect+"?state="+fl.state+"&code=x&client_id=oaiapp_kingdom&iss="+url.QueryEscape(value), nil)
		w := httptest.NewRecorder()
		fl.ServeHTTP(w, r)
		if value != issuer {
			if w.Code != 400 || f.calls.Load() != 0 {
				t.Fatal("wrong callback issuer accepted")
			}
		} else if w.Code != 200 {
			t.Fatal("exact callback issuer rejected")
		}
	}
}
func TestCloseCancelsAndJoinsPendingFlow(t *testing.T) {
	f := newFixture(t)
	fl := f.flow("")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if e := f.s.Close(ctx); e != nil {
		t.Fatal(e)
	}
	if _, e := fl.Wait(ctx); e == nil {
		t.Fatal("closed flow succeeded")
	}
	if _, e := f.s.Begin(ctx, "owner", ""); !errors.Is(e, ErrUnavailable) {
		t.Fatal("closed service admitted flow")
	}
}
func TestFileVaultSerializesIndependentOwnersAndRetainsHost(t *testing.T) {
	ctx := context.Background()
	root := filepath.Join(t.TempDir(), "vault")
	a, e := OpenFileVault(ctx, root, bytes.Repeat([]byte{1}, 32))
	if e != nil {
		t.Fatal(e)
	}
	defer a.Close()
	b, e := OpenFileVault(ctx, root, bytes.Repeat([]byte{1}, 32))
	if e != nil {
		t.Fatal(e)
	}
	defer b.Close()
	if a.HostID() != b.HostID() {
		t.Fatal("host changed")
	}
	r, e := a.newRegistration(ctx, "owner")
	if e != nil {
		t.Fatal(e)
	}
	unlock, e := a.lock(ctx, r.ID)
	if e != nil {
		t.Fatal(e)
	}
	defer unlock()
	bounded, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	if release, e := b.lock(bounded, r.ID); e == nil {
		release()
		t.Fatal("independent vault bypassed refresh serialization")
	}
}
func TestModelsUseOnlyPublicAccountCatalogAndNoRedirect(t *testing.T) {
	f := newFixture(t)
	reg, e := f.callback(f.flow(""))
	if e != nil {
		t.Fatal(e)
	}
	calls := 0
	tr := transportFunc(func(r *http.Request) (*http.Response, error) {
		calls++
		if r.URL.String() != resource+"/models" || r.Header.Get("Authorization") != "Bearer access-token" {
			t.Fatal("catalog not bound to account and public endpoint")
		}
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(bytes.NewBufferString(`{"models":[{"slug":"visible","display_name":"Visible","visibility":"list"},{"slug":"hidden","display_name":"Hidden","visibility":"hidden"}]}`))}, nil
	})
	models, _, e := f.s.Models(context.Background(), "owner", reg.ID, tr)
	if e != nil || len(models) != 1 || models[0].Slug != "visible" || calls != 1 {
		t.Fatal("account catalog failed", e)
	}
	tr = transportFunc(func(r *http.Request) (*http.Response, error) {
		calls++
		return &http.Response{StatusCode: 302, Header: http.Header{"Location": []string{"https://attacker.example"}}, Body: io.NopCloser(bytes.NewReader(nil))}, nil
	})
	if _, _, e = f.s.Models(context.Background(), "owner", reg.ID, tr); e == nil || calls != 2 {
		t.Fatal("catalog redirect followed")
	}
}
