package api

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type authenticatorFunc func(*http.Request) (Principal, error)

func (f authenticatorFunc) Authenticate(r *http.Request) (Principal, error) { return f(r) }

func TestHostedAuthenticationPreservesActorAndStripsSpoofedHeaders(t *testing.T) {
	for _, actor := range []string{"alice", "bob"} {
		s := &Server{authenticator: authenticatorFunc(func(r *http.Request) (Principal, error) {
			if r.Header.Get("X-Actor-Id") != "" || r.Header.Get("X-Role-Id") != "" {
				t.Fatal("spoofed attribution reached authenticator")
			}
			return Principal{ActorID: actor, RoleID: "member", Method: "cookie", ExpiresAt: time.Now().Add(time.Hour)}, nil
		})}
		h := s.authenticationMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if authenticatedActorID(r.Context()) != actor {
				t.Fatal("actor collapsed to local identity")
			}
			if _, ok := r.Context().Deadline(); !ok {
				t.Fatal("session expiry missing from request context")
			}
			w.WriteHeader(204)
		}))
		r := httptest.NewRequest("GET", "/roles", nil)
		r.Header.Set("X-Actor-Id", "admin")
		r.Header.Set("X-Role-Id", "owner")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != 204 {
			t.Fatalf("status=%d", w.Code)
		}
	}
}

func TestHostedAuthenticationFailsClosedWithoutLocalBypass(t *testing.T) {
	for _, path := range []string{"/roles", "/connections/github/manifest-flows/oauth-callback"} {
		for _, failure := range []error{ErrAuthenticationRequired, errors.New("private identity diagnostic"), ErrAccessDenied} {
			s := &Server{authenticator: authenticatorFunc(func(*http.Request) (Principal, error) { return Principal{}, failure })}
			h := s.authenticationMiddleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Fatal("unauthorized handler reached") }))
			w := httptest.NewRecorder()
			r := httptest.NewRequest("GET", path, nil)
			r.Header.Set("Authorization", "Bearer local-secret")
			h.ServeHTTP(w, r)
			want := 401
			if errors.Is(failure, ErrAccessDenied) {
				want = 403
			}
			if w.Code != want {
				t.Fatalf("status=%d want=%d", w.Code, want)
			}
			if strings.Contains(w.Body.String(), "private identity") {
				t.Fatal("authentication error leaked")
			}
		}
	}
	s := &Server{authenticator: authenticatorFunc(func(*http.Request) (Principal, error) {
		t.Fatal("local session bootstrap exposed")
		return Principal{}, nil
	})}
	w := httptest.NewRecorder()
	s.authenticationMiddleware(http.NotFoundHandler()).ServeHTTP(w, httptest.NewRequest("POST", "/auth/session", nil))
	if w.Code != 404 {
		t.Fatalf("bootstrap status=%d", w.Code)
	}
}

func TestHostedAuthenticationRequiresLiveCompletePrincipal(t *testing.T) {
	valid := Principal{ActorID: "alice", RoleID: "member", Method: "cookie", ExpiresAt: time.Now().Add(time.Hour)}
	for _, alter := range []func(*Principal){func(p *Principal) { p.ActorID = "" }, func(p *Principal) { p.RoleID = "" }, func(p *Principal) { p.Method = "" }, func(p *Principal) { p.ExpiresAt = time.Time{} }, func(p *Principal) { p.ExpiresAt = time.Now().Add(-time.Second) }} {
		p := valid
		alter(&p)
		s := &Server{authenticator: authenticatorFunc(func(*http.Request) (Principal, error) { return p, nil })}
		w := httptest.NewRecorder()
		s.authenticationMiddleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Fatal("invalid principal accepted") })).ServeHTTP(w, httptest.NewRequest("GET", "/roles", nil))
		if w.Code != 401 {
			t.Fatalf("status=%d", w.Code)
		}
	}
}

func TestHostedAuthenticationStreamEndsOnSessionExpiryAndRevocation(t *testing.T) {
	for _, revoked := range []bool{false, true} {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		expires := time.Now().Add(30 * time.Millisecond)
		if revoked {
			expires = time.Now().Add(time.Hour)
		}
		s := &Server{authenticator: authenticatorFunc(func(*http.Request) (Principal, error) {
			return Principal{ActorID: "alice", RoleID: "member", Method: "cookie", ExpiresAt: expires}, nil
		})}
		h := s.authenticationMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if revoked {
				cancel()
			}
			select {
			case <-r.Context().Done():
			case <-time.After(time.Second):
				t.Fatal("stream retained invalid authorization")
			}
		}))
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/events", nil).WithContext(ctx))
	}
}

func TestHostedActorsHaveSeparateDurableIdempotencyRecords(t *testing.T) {
	store := newIdempotencyStoreForAPITest(t)
	now := time.Now()
	key := testIdempotencyKey(t, "hosted-same-key", now)
	actor := "alice"
	s := &Server{authenticator: authenticatorFunc(func(*http.Request) (Principal, error) {
		return Principal{ActorID: actor, RoleID: "member", Method: "cookie", SessionID: actor + "-session", ExpiresAt: now.Add(time.Hour)}, nil
	})}
	calls := 0
	h := s.authenticationMiddleware(NewIdempotencyMiddleware(store, func() time.Time { return now })(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.WriteHeader(201)
		_, _ = w.Write([]byte(authenticatedActorID(r.Context())))
	})))
	for _, user := range []string{"alice", "bob", "alice", "bob"} {
		actor = user
		r := httptest.NewRequest("POST", "/runs", nil)
		r.Header.Set(idempotencyKeyHeader, key)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != 201 || w.Body.String() != user {
			t.Fatalf("user=%s status=%d body=%s", user, w.Code, w.Body.String())
		}
	}
	if calls != 2 {
		t.Fatalf("calls=%d want=2", calls)
	}
	for _, user := range []string{"alice", "bob"} {
		_, found, err := store.Lookup(context.Background(), "POST /runs", user, key)
		if err != nil || !found {
			t.Fatalf("durable actor record %s found=%v error=%v", user, found, err)
		}
	}
}
