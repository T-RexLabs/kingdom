package opencode

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"kingdom/internal/core/providerexecution"
	"kingdom/internal/execution/providerrequest"
)

type callbackFixture struct {
	identity                                         providerexecution.RequestIdentity
	fenced                                           bool
	released, admitted, finished, invoked, cancelled int
	result                                           ManagedCommandResult
	fail                                             error
}

func (f *callbackFixture) Begin(ctx context.Context, token string, receipt bool) (ManagedCallbackScope, error) {
	if token != "owned" || f.fenced && !receipt {
		return ManagedCallbackScope{}, errors.New("private token diagnostics")
	}
	return ManagedCallbackScope{Context: ctx, Identity: f.identity, Release: func() { f.released++ }}, nil
}
func (f *callbackFixture) AdmitNative(context.Context, providerrequest.NativeCall) (providerrequest.Grant, error) {
	f.admitted++
	return providerrequest.Grant{ID: "grant", Digest: "digest", Execute: true}, f.fail
}
func (f *callbackFixture) FinishNative(context.Context, providerexecution.RequestIdentity, providerrequest.NativeReceipt) error {
	f.finished++
	return f.fail
}
func (f *callbackFixture) Invoke(context.Context, ManagedCommandInput) (ManagedCommandResult, error) {
	f.invoked++
	return f.result, f.fail
}
func (f *callbackFixture) Cancel(context.Context, providerexecution.RequestIdentity) error {
	f.cancelled++
	return f.fail
}
func callbackRequest(t *testing.T, f *callbackFixture, route, token string, body any) *httptest.ResponseRecorder {
	t.Helper()
	h, e := NewManagedCallbackHandler(f, f, f)
	if e != nil {
		t.Fatal(e)
	}
	raw, e := json.Marshal(body)
	if e != nil {
		t.Fatal(e)
	}
	r := httptest.NewRequest(http.MethodPost, route, bytes.NewReader(raw))
	r.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}
func callbackOwner() *callbackFixture {
	return &callbackFixture{identity: providerexecution.RequestIdentity{InstanceID: "instance", Generation: "generation", Location: "/workspace/project", SessionID: "session", ProfileID: "profile", RequestID: "request"}, result: ManagedCommandResult{ExecutionID: "remote-operation", Closed: true, Output: "output", Exit: 0}}
}
func TestManagedCallbacksRejectForeignIdentityBeforeEffects(t *testing.T) {
	for _, route := range []string{"/v1/native/admit", "/v1/native/finish", "/v1/command/invoke", "/v1/command/cancel"} {
		t.Run(route, func(t *testing.T) {
			f := callbackOwner()
			other := f.identity
			other.InstanceID = "foreign"
			w := callbackRequest(t, f, route, "owned", map[string]any{"identity": other})
			if w.Code != http.StatusForbidden || f.admitted+f.finished+f.invoked+f.cancelled != 0 || f.released != 1 {
				t.Fatalf("foreign effect admitted: %d %+v", w.Code, f)
			}
		})
	}
}
func TestManagedCallbacksFenceAdmissionsButAcceptLateReceipts(t *testing.T) {
	f := callbackOwner()
	f.fenced = true
	for _, route := range []string{"/v1/native/admit", "/v1/command/invoke"} {
		if w := callbackRequest(t, f, route, "owned", map[string]any{"identity": f.identity}); w.Code != http.StatusForbidden {
			t.Fatal(w.Code)
		}
	}
	if w := callbackRequest(t, f, "/v1/native/finish", "owned", nativeReceipt{Identity: f.identity, GrantID: "grant", IdentityDigest: "digest", Outcome: "succeeded"}); w.Code != http.StatusOK {
		t.Fatal(w.Code)
	}
	if w := callbackRequest(t, f, "/v1/command/cancel", "owned", commandCancellation{Identity: f.identity}); w.Code != http.StatusOK {
		t.Fatal(w.Code)
	}
	if f.admitted+f.invoked != 0 || f.finished != 1 || f.cancelled != 1 || f.released != 2 {
		t.Fatalf("fence changed receipts: %+v", f)
	}
}
func TestManagedCallbacksRedactErrorsAndRequireRemoteClosure(t *testing.T) {
	f := callbackOwner()
	f.fail = errors.New("secret upstream URL and credential")
	w := callbackRequest(t, f, "/v1/command/invoke", "owned", commandInput{Identity: f.identity})
	if w.Code != http.StatusConflict || strings.Contains(w.Body.String(), "secret") {
		t.Fatalf("private failure leaked: %s", w.Body.String())
	}
	f.fail = nil
	f.result.Closed = false
	if w = callbackRequest(t, f, "/v1/command/invoke", "owned", commandInput{Identity: f.identity}); w.Code != http.StatusConflict {
		t.Fatal("unknown closure accepted")
	}
	f.result.Closed = true
	w = callbackRequest(t, f, "/v1/command/invoke", "owned", commandInput{Identity: f.identity})
	if w.Code != http.StatusOK || strings.Contains(w.Body.String(), "guardID") || !strings.Contains(w.Body.String(), "executionID") {
		t.Fatalf("remote result claimed local guardian: %s", w.Body.String())
	}
}
func TestManagedCallbacksRejectBadCapabilityAndUnknownFields(t *testing.T) {
	f := callbackOwner()
	w := callbackRequest(t, f, "/v1/native/admit", "foreign", nativeAdmission{Identity: f.identity})
	if w.Code != http.StatusForbidden || f.admitted != 0 {
		t.Fatal("foreign capability accepted")
	}
	w = callbackRequest(t, f, "/v1/native/admit", "owned", map[string]any{"identity": f.identity, "actor": "forged"})
	if w.Code != http.StatusForbidden || f.admitted != 0 || f.released != 1 {
		t.Fatal("unknown authority field accepted")
	}
}

type callbackAuthorization func(context.Context, string, bool) (ManagedCallbackScope, error)

func (f callbackAuthorization) Begin(c context.Context, t string, r bool) (ManagedCallbackScope, error) {
	return f(c, t, r)
}

type stalledCallbackBody struct {
	closed chan struct{}
	once   sync.Once
}

func (b *stalledCallbackBody) Read([]byte) (int, error) { <-b.closed; return 0, io.ErrClosedPipe }
func (b *stalledCallbackBody) Close() error             { b.once.Do(func() { close(b.closed) }); return nil }
func TestManagedCallbackRevocationInterruptsBodyUpload(t *testing.T) {
	f := callbackOwner()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	registered := make(chan struct{})
	released := make(chan struct{})
	auth := callbackAuthorization(func(context.Context, string, bool) (ManagedCallbackScope, error) {
		close(registered)
		return ManagedCallbackScope{Context: ctx, Identity: f.identity, Release: func() { close(released) }}, nil
	})
	h, e := NewManagedCallbackHandler(auth, f, f)
	if e != nil {
		t.Fatal(e)
	}
	body := &stalledCallbackBody{closed: make(chan struct{})}
	r := httptest.NewRequest(http.MethodPost, "/v1/native/admit", body)
	r.Header.Set("Authorization", "Bearer owned")
	done := make(chan struct{})
	go func() { defer close(done); h.ServeHTTP(httptest.NewRecorder(), r) }()
	<-registered
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("revocation left body reader blocked")
	}
	select {
	case <-released:
	default:
		t.Fatal("handler ownership not released")
	}
	if f.admitted != 0 {
		t.Fatal("cancelled body admitted work")
	}
}

type stalledCallbackWriter struct {
	header           http.Header
	writing, expired chan struct{}
	once             sync.Once
}

func (w *stalledCallbackWriter) Header() http.Header { return w.header }
func (w *stalledCallbackWriter) WriteHeader(int)     {}
func (w *stalledCallbackWriter) Write([]byte) (int, error) {
	close(w.writing)
	<-w.expired
	return 0, io.ErrClosedPipe
}
func (w *stalledCallbackWriter) SetWriteDeadline(time.Time) error {
	w.once.Do(func() { close(w.expired) })
	return nil
}
func TestManagedCallbackRevocationInterruptsResponseWrite(t *testing.T) {
	f := callbackOwner()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	released := make(chan struct{})
	auth := callbackAuthorization(func(context.Context, string, bool) (ManagedCallbackScope, error) {
		return ManagedCallbackScope{Context: ctx, Identity: f.identity, Release: func() { close(released) }}, nil
	})
	h, err := NewManagedCallbackHandler(auth, f, f)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(commandInput{Identity: f.identity})
	r := httptest.NewRequest(http.MethodPost, "/v1/command/invoke", bytes.NewReader(raw))
	r.Header.Set("Authorization", "Bearer owned")
	w := &stalledCallbackWriter{header: make(http.Header), writing: make(chan struct{}), expired: make(chan struct{})}
	done := make(chan struct{})
	go func() { defer close(done); h.ServeHTTP(w, r) }()
	defer func() { _ = w.SetWriteDeadline(time.Now()); <-done }()
	<-w.writing
	cancel()
	select {
	case <-released:
	case <-time.After(time.Second):
		t.Fatal("revocation left response writer and handler ownership blocked")
	}
}
