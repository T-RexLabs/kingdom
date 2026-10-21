package opencode

import (
	"errors"
	"net/http"
	"reflect"
	"time"
)

const managedOrigin = "https://managed.invalid"

// newManagedClient uses only the worker's authenticated, runtime-bound tunnel.
// It cannot fall back to host networking or read a host OpenCode credential.
func newManagedClient(transport http.RoundTripper) (*Client, error) {
	if nilManagedDependency(transport) {
		return nil, errors.New("managed OpenCode transport is required")
	}
	fixed := managedTransport{next: transport}
	noRedirect := func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &Client{baseURL: managedOrigin,
		http: &http.Client{Transport: fixed, Timeout: 30 * time.Second, CheckRedirect: noRedirect},
		long: &http.Client{Transport: fixed, CheckRedirect: noRedirect}}, nil
}

type managedTransport struct{ next http.RoundTripper }

func (t managedTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.URL == nil || req.URL.Scheme != "https" || req.URL.Host != "managed.invalid" || req.URL.User != nil || req.URL.Opaque != "" || req.URL.Fragment != "" {
		return nil, errors.New("managed OpenCode transport target is invalid")
	}
	// Authentication is installed by the worker, never inherited from a caller.
	owned := req.Clone(req.Context())
	owned.Header.Del("Authorization")
	owned.Header.Del("Proxy-Authorization")
	owned.Header.Del("Cookie")
	owned.Host = "managed.invalid"
	return t.next.RoundTrip(owned)
}
func (t managedTransport) CloseIdleConnections() {
	if closer, ok := t.next.(interface{ CloseIdleConnections() }); ok {
		closer.CloseIdleConnections()
	}
}

func nilManagedDependency(transport any) bool {
	if transport == nil {
		return true
	}
	value := reflect.ValueOf(transport)
	switch value.Kind() {
	case reflect.Pointer, reflect.Func, reflect.Map, reflect.Slice, reflect.Chan, reflect.Interface:
		return value.IsNil()
	default:
		return false
	}
}
