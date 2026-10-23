package projects

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"kingdom/internal/api"
)

type authenticator struct{}

func (authenticator) Authenticate(r *http.Request) (api.Principal, error) {
	return api.Principal{ActorID: r.Header.Get("Test-Actor")}, nil
}

type runtimeFixture struct {
	auth      api.Authenticator
	closes    atomic.Int32
	failClose atomic.Bool
}

func (r *runtimeFixture) ServeHTTP(w http.ResponseWriter, q *http.Request) {
	p, e := r.auth.Authenticate(q)
	if e != nil {
		w.WriteHeader(401)
		return
	}
	_, _ = w.Write([]byte(p.ActorID))
}
func (r *runtimeFixture) Close(context.Context) error {
	r.closes.Add(1)
	if r.failClose.Load() {
		return errors.New("unresolved provider")
	}
	return nil
}
func manager(t *testing.T, build Builder) *Manager {
	t.Helper()
	m, e := New(Config{Build: build})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = m.Close(ctx)
	})
	return m
}
func TestConcurrentRequestsShareRuntimeButKeepActorIdentity(t *testing.T) {
	var calls atomic.Int32
	runtime := &runtimeFixture{}
	m := manager(t, func(ctx context.Context, id string, a api.Authenticator) (Runtime, error) {
		calls.Add(1)
		runtime.auth = a
		return runtime, nil
	})
	id := uuid.NewString()
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			h, e := m.Factory(context.Background(), id, authenticator{})
			if e != nil {
				t.Error(e)
				return
			}
			if h != runtime {
				t.Error("project received a different runtime")
			}
		}()
	}
	wg.Wait()
	if calls.Load() != 1 {
		t.Fatalf("build count=%d", calls.Load())
	}
	for _, actor := range []string{"owner", "member"} {
		q := httptest.NewRequest("GET", "/", nil)
		q.Header.Set("Test-Actor", actor)
		w := httptest.NewRecorder()
		runtime.ServeHTTP(w, q)
		if w.Body.String() != actor {
			t.Fatal("cached the first requesting actor")
		}
	}
	other, e := m.Factory(context.Background(), uuid.NewString(), authenticator{})
	if e != nil || other == nil || calls.Load() != 2 {
		t.Fatal("project ownership was not independent")
	}
}
func TestClientCancellationDoesNotCancelOwnedStartup(t *testing.T) {
	entered, release := make(chan context.Context, 1), make(chan struct{})
	runtime := &runtimeFixture{}
	m := manager(t, func(ctx context.Context, _ string, a api.Authenticator) (Runtime, error) {
		entered <- ctx
		<-release
		runtime.auth = a
		return runtime, nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	id := uuid.NewString()
	result := make(chan error, 1)
	go func() { _, e := m.Factory(ctx, id, authenticator{}); result <- e }()
	owned := <-entered
	cancel()
	if e := <-result; !errors.Is(e, context.Canceled) {
		t.Fatalf("client cancel=%v", e)
	}
	if owned.Err() != nil {
		t.Fatal("HTTP request cancellation reached runtime startup")
	}
	close(release)
	if _, e := m.Factory(context.Background(), id, authenticator{}); e != nil {
		t.Fatal(e)
	}
}
func TestCloseJoinsLateStartupAndRetainsFailedCleanup(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	runtime := &runtimeFixture{}
	m := manager(t, func(ctx context.Context, _ string, a api.Authenticator) (Runtime, error) {
		close(entered)
		<-release
		runtime.auth = a
		return runtime, nil
	})
	id := uuid.NewString()
	result := make(chan error, 1)
	go func() { _, e := m.Factory(context.Background(), id, authenticator{}); result <- e }()
	<-entered
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if e := m.Close(ctx); !errors.Is(e, context.DeadlineExceeded) {
		t.Fatalf("unjoined build close=%v", e)
	}
	runtime.failClose.Store(true)
	close(release)
	if e := <-result; !errors.Is(e, ErrUnavailable) {
		t.Fatalf("late startup was published: %v", e)
	}
	if e := m.Close(context.Background()); e == nil {
		t.Fatal("unresolved provider released ownership")
	}
	runtime.failClose.Store(false)
	if e := m.Close(context.Background()); e != nil {
		t.Fatal(e)
	}
	count := runtime.closes.Load()
	if e := m.Close(context.Background()); e != nil || runtime.closes.Load() != count {
		t.Fatal("completed runtime closed twice")
	}
	if _, e := m.Factory(context.Background(), id, authenticator{}); !errors.Is(e, ErrUnavailable) {
		t.Fatal("closed manager admitted project")
	}
}
func TestFailedUnownedStartupCanRetryAndInvalidInputCannotBuild(t *testing.T) {
	var calls atomic.Int32
	m := manager(t, func(_ context.Context, _ string, a api.Authenticator) (Runtime, error) {
		if calls.Add(1) == 1 {
			return nil, errors.New("database busy")
		}
		return &runtimeFixture{auth: a}, nil
	})
	for _, id := range []string{"", uuid.Nil.String(), "../project"} {
		if _, e := m.Factory(context.Background(), id, authenticator{}); e == nil {
			t.Fatal("invalid ID admitted")
		}
	}
	var nilAuth *authenticator
	if _, e := m.Factory(context.Background(), uuid.NewString(), nilAuth); e == nil {
		t.Fatal("typed nil auth admitted")
	}
	if calls.Load() != 0 {
		t.Fatal("invalid input started runtime")
	}
	id := uuid.NewString()
	if _, e := m.Factory(context.Background(), id, authenticator{}); e == nil {
		t.Fatal("failed startup accepted")
	}
	if _, e := m.Factory(context.Background(), id, authenticator{}); e != nil {
		t.Fatal(e)
	}
}

func TestCapacityIncludesStartupAndNilRuntimeCannotPublish(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	m, e := New(Config{MaxProjects: 1, Build: func(context.Context, string, api.Authenticator) (Runtime, error) {
		close(entered)
		<-release
		var runtime *runtimeFixture
		return runtime, nil
	}})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = m.Close(context.Background()) })
	result := make(chan error, 1)
	go func() { _, e := m.Factory(context.Background(), uuid.NewString(), authenticator{}); result <- e }()
	<-entered
	if _, e := m.Factory(context.Background(), uuid.NewString(), authenticator{}); !errors.Is(e, ErrUnavailable) {
		t.Fatal("startup did not retain capacity")
	}
	close(release)
	if e := <-result; !errors.Is(e, ErrUnavailable) {
		t.Fatal("typed nil runtime published")
	}
}
