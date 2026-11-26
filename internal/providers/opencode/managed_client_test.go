package opencode

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestManagedClientUsesOnlyOwnedTransport(t *testing.T) {
	calls := 0
	c, err := newManagedClient(clientTestTransport(func(r *http.Request) (*http.Response, error) {
		calls++
		if r.URL.Host != "managed.invalid" || r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" {
			t.Fatal("unscoped request", r.URL)
		}
		return &http.Response{StatusCode: 302, Header: http.Header{"Location": []string{"https://elsewhere.invalid"}}, Body: io.NopCloser(strings.NewReader("")), Request: r}, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	for _, hc := range []*http.Client{c.http, c.long} {
		req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, managedOrigin+"/test", nil)
		req.Header.Set("Authorization", "Bearer caller")
		req.Header.Set("Cookie", "caller=value")
		response, err := hc.Do(req)
		if err != nil || response.StatusCode != 302 {
			t.Fatal(response, err)
		}
		response.Body.Close()
		bad, _ := http.NewRequest(http.MethodGet, "https://elsewhere.invalid/test", nil)
		if _, err := hc.Do(bad); err == nil {
			t.Fatal("accepted foreign target")
		}
	}
	if calls != 2 {
		t.Fatal("redirect or foreign request escaped", calls)
	}
}
func TestManagedClientRequiresTransport(t *testing.T) {
	if _, err := newManagedClient(nil); err == nil {
		t.Fatal("nil transport accepted")
	}
	var callback clientTestTransport
	if _, err := newManagedClient(callback); err == nil {
		t.Fatal("nil function transport accepted")
	}
	var transport *http.Transport
	if _, err := newManagedClient(transport); err == nil {
		t.Fatal("typed nil accepted")
	}
}

func TestManagedClientStreamingHonorsCancellation(t *testing.T) {
	entered := make(chan struct{})
	c, err := newManagedClient(clientTestTransport(func(r *http.Request) (*http.Response, error) {
		close(entered)
		<-r.Context().Done()
		return nil, r.Context().Err()
	}))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	finished := make(chan error, 1)
	go func() {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, managedOrigin+"/events", nil)
		_, err := c.long.Do(req)
		finished <- err
	}()
	<-entered
	cancel()
	if err := <-finished; !errors.Is(err, context.Canceled) {
		t.Fatal("stream lost cancellation", err)
	}
}
