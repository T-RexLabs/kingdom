package auth

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestPersonalSignInRequiresExactLiveInitiatingSession(t *testing.T) {
	f := setup(t, time.Hour)
	ctx := context.Background()
	session, cookie := f.session(t)
	if err := f.manager.CheckSession(ctx, f.member.ID, session.SessionID); err != nil {
		t.Fatal(err)
	}
	if err := f.manager.CheckSession(ctx, f.owner.ID, session.SessionID); err == nil {
		t.Fatal("foreign owner accepted")
	}
	if err := f.manager.CheckSession(ctx, f.member.ID, "invalid"); err == nil {
		t.Fatal("invalid session accepted")
	}
	called := false
	h := f.manager.SessionHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got, err := f.manager.AuthenticatedSession(r)
		if err != nil || got.SessionID != session.SessionID || got.User.ID != f.member.ID {
			t.Error("session identity changed", err)
		}
		called = true
	}))
	h.ServeHTTP(httptest.NewRecorder(), request("GET", "/", cookie, ""))
	if !called {
		t.Fatal("session not admitted")
	}
	if _, err := f.db.Exec(`UPDATE central_sessions SET revoked=1`); err != nil {
		t.Fatal(err)
	}
	if err := f.manager.CheckSession(ctx, f.member.ID, session.SessionID); err == nil {
		t.Fatal("revoked session retained sign-in authority")
	}
}
