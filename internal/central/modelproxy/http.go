package modelproxy

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"kingdom/internal/central/capability"
	"mime"
	"net/http"
	"net/url"
	"strings"
	"time"
	"unicode/utf8"
)

func (b *Broker) Handler() http.Handler { return b }
func (b *Broker) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	deny := func(code int) { http.Error(w, "Model request unavailable.", code) }
	if r.TLS == nil {
		deny(http.StatusBadRequest)
		return
	}
	if r.Method != http.MethodPost || (r.URL.Path != "/v1/responses" && r.URL.Path != "/v1/chat/completions") || r.URL.RawQuery != "" || r.URL.RawPath != "" {
		deny(http.StatusNotFound)
		return
	}
	auth := r.Header.Values("Authorization")
	if len(auth) != 1 || !strings.HasPrefix(auth[0], "Bearer ") || len(auth[0]) != 50 {
		deny(http.StatusUnauthorized)
		return
	}
	b.mu.Lock()
	closing := b.closing
	b.mu.Unlock()
	if closing {
		deny(http.StatusServiceUnavailable)
		return
	}
	admission, e := b.cfg.Registry.Begin(r.Context(), strings.TrimPrefix(auth[0], "Bearer "), capability.Model, capability.ModeNew)
	if e != nil {
		deny(http.StatusUnauthorized)
		return
	}
	defer admission.Done()
	ctx, cancel := context.WithTimeout(admission.Context, b.cfg.RequestTimeout)
	defer cancel()
	controller := http.NewResponseController(w)
	deadline, _ := ctx.Deadline()
	_ = controller.SetReadDeadline(deadline)
	_ = controller.SetWriteDeadline(deadline)
	// Revocation must also interrupt blocked client reads/writes. The handler
	// still owns admission until all work returns; unsupported writers keep debt.
	finished := make(chan struct{})
	watchDone := make(chan struct{})
	defer func() { close(finished); <-watchDone }()
	go func() {
		defer close(watchDone)
		select {
		case <-ctx.Done():
			_ = controller.SetReadDeadline(time.Now())
			_ = controller.SetWriteDeadline(time.Now())
		case <-finished:
		}
	}()
	binding := admission.Pins.Binding
	binding.Request = &admission.Request
	if b.cfg.Authorizer.Check(ctx, binding, StageActive) != nil {
		deny(http.StatusForbidden)
		return
	}
	// Notifications give prompt cancellation. Polling also covers direct catalog
	// changes and canonical lease loss, within one second plus query latency.
	authorityDone := make(chan struct{})
	authorityContext, stopAuthority := context.WithCancel(ctx)
	defer func() { stopAuthority(); <-authorityDone }()
	go func() {
		defer close(authorityDone)
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-authorityContext.Done():
				return
			case <-ticker.C:
				if b.cfg.Authorizer.Check(authorityContext, binding, StageActive) != nil {
					if authorityContext.Err() != nil {
						return
					}
					cancel()
					return
				}
			}
		}
	}()
	contentType, _, e := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if e != nil || contentType != "application/json" || r.Header.Get("Content-Encoding") != "" {
		deny(http.StatusBadRequest)
		return
	}
	body, e := io.ReadAll(io.LimitReader(r.Body, b.cfg.MaxRequestBytes+1))
	if e != nil || !utf8.Valid(body) || int64(len(body)) > b.cfg.MaxRequestBytes || ctx.Err() != nil {
		deny(http.StatusBadRequest)
		return
	}
	object, e := uniqueObject(body)
	if e != nil || object["model"] != admission.Pins.Route.Model {
		deny(http.StatusBadRequest)
		return
	}
	for _, field := range []string{"base_url", "api_key", "authorization", "headers"} {
		if _, ok := object[field]; ok {
			deny(http.StatusBadRequest)
			return
		}
	}
	route := admission.Pins.Route
	if b.validateRoute(route) != nil {
		deny(http.StatusForbidden)
		return
	}
	credential, e := b.cfg.Resolver.Resolve(ctx, binding, route)
	if e != nil || credential.token == "" || ctx.Err() != nil {
		deny(http.StatusForbidden)
		return
	}
	// Recheck current authority after credential lookup, immediately before I/O.
	if b.cfg.Authorizer.Check(ctx, binding, StageActive) != nil || ctx.Err() != nil {
		deny(http.StatusForbidden)
		return
	}
	endpoint, _ := url.Parse(route.Endpoint)
	endpoint.Path = strings.TrimSuffix(endpoint.Path, "/") + strings.TrimPrefix(r.URL.Path, "/v1")
	upstream, e := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), bytes.NewReader(body))
	if e != nil {
		deny(http.StatusBadGateway)
		return
	}
	upstream.Header.Set("Content-Type", "application/json")
	upstream.Header.Set("Accept", "application/json, text/event-stream")
	upstream.Header.Set("Authorization", "Bearer "+credential.token)
	response, e := b.transport.RoundTrip(upstream)
	if e != nil {
		deny(http.StatusBadGateway)
		return
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		deny(http.StatusBadGateway)
		return
	}
	responseType, _, e := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if e != nil || (responseType != "application/json" && responseType != "text/event-stream") || response.Header.Get("Content-Encoding") != "" {
		deny(http.StatusBadGateway)
		return
	}
	w.Header().Set("Content-Type", responseType)
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(response.StatusCode)
	buffer := make([]byte, 32<<10)
	remaining := b.cfg.MaxResponseBytes
	for remaining > 0 {
		size := int64(len(buffer))
		if size > remaining {
			size = remaining
		}
		n, readErr := response.Body.Read(buffer[:size])
		if n > 0 {
			remaining -= int64(n)
			if _, e = w.Write(buffer[:n]); e != nil {
				return
			}
			if responseType == "text/event-stream" {
				if e = controller.Flush(); e != nil {
					return
				}
			}
		}
		if readErr == io.EOF && ctx.Err() == nil {
			return
		}
		if readErr != nil || ctx.Err() != nil {
			panic(http.ErrAbortHandler)
		}
	}
	// One extra byte distinguishes a valid exact-limit response from truncation.
	var overflow [1]byte
	n, readErr := io.ReadFull(response.Body, overflow[:])
	if n != 0 || readErr != io.EOF || ctx.Err() != nil {
		panic(http.ErrAbortHandler)
	}
}
func uniqueObject(raw []byte) (map[string]any, error) {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	value, e := uniqueValue(d, 0)
	if e != nil {
		return nil, e
	}
	if _, e = d.Token(); e != io.EOF {
		return nil, ErrDenied
	}
	object, ok := value.(map[string]any)
	if !ok {
		return nil, ErrDenied
	}
	return object, nil
}
func uniqueValue(d *json.Decoder, depth int) (any, error) {
	if depth > 64 {
		return nil, ErrDenied
	}
	token, e := d.Token()
	if e != nil {
		return nil, ErrDenied
	}
	delimiter, ok := token.(json.Delim)
	if !ok {
		return token, nil
	}
	switch delimiter {
	case '{':
		result := map[string]any{}
		for d.More() {
			key, e := d.Token()
			if e != nil {
				return nil, ErrDenied
			}
			name, ok := key.(string)
			if !ok {
				return nil, ErrDenied
			}
			if _, exists := result[name]; exists {
				return nil, ErrDenied
			}
			value, e := uniqueValue(d, depth+1)
			if e != nil {
				return nil, e
			}
			result[name] = value
		}
		end, e := d.Token()
		if e != nil || end != json.Delim('}') {
			return nil, ErrDenied
		}
		return result, nil
	case '[':
		result := []any{}
		for d.More() {
			value, e := uniqueValue(d, depth+1)
			if e != nil {
				return nil, e
			}
			result = append(result, value)
		}
		end, e := d.Token()
		if e != nil || end != json.Delim(']') {
			return nil, ErrDenied
		}
		return result, nil
	default:
		return nil, ErrDenied
	}
}
