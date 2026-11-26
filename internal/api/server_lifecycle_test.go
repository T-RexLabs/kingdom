package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestCompletedStartupCancellationDoesNotCloseServingAPI(t *testing.T) {
	startup, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, err := NewHandler(ServerConfig{
		DB: openAPIServerTestDB(t), ProjectRoot: t.TempDir(),
		ProviderModelCatalog: staticProviderModelCatalog{}, PlannerAgent: &testPlannerAgent{},
		StartupContext: startup,
		Authenticator: authenticatorFunc(func(r *http.Request) (Principal, error) {
			if r.Context().Err() != nil {
				return Principal{}, ErrAuthenticationRequired
			}
			return Principal{ActorID: "member", RoleID: "member", Method: "test", ExpiresAt: time.Now().Add(time.Hour)}, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	cancel()
	if err := s.lifecycleCtx.Err(); err != nil {
		t.Fatal("completed startup cancelled the serving lifetime", err)
	}
	w := httptest.NewRecorder()
	s.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/roles", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("live authenticated API status=%d body=%s", w.Code, w.Body.String())
	}
}

func TestExplicitServingLifetimeCancelsInflightRequests(t *testing.T) {
	lifetime, cancel := context.WithCancel(context.Background())
	defer cancel()
	h, _ := newAPIServerForTestWithConfig(t, ServerConfig{LifetimeContext: lifetime})
	s := h.(*Server)
	entered, returned := make(chan struct{}), make(chan struct{})
	s.handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-r.Context().Done()
		w.WriteHeader(http.StatusNoContent)
	})
	go func() {
		defer close(returned)
		s.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/events", nil))
	}()
	<-entered
	cancel()
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("serving lifetime cancellation did not close the request")
	}
}
