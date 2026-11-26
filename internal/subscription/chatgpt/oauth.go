package chatgpt

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"
)

const callbackPath = "/auth/callback"

type Flow struct {
	id, authorization, redirect, state, nonce, verifier string
	initial                                             record
	service                                             *Service
	server                                              *http.Server
	cancel                                              context.CancelFunc
	ctx                                                 context.Context
	done                                                chan struct{}
	once                                                sync.Once
	result                                              Registration
	err                                                 error
}

func randomValue() string {
	var b [32]byte
	if _, e := rand.Read(b[:]); e != nil {
		panic("secure random unavailable")
	}
	return base64.RawURLEncoding.EncodeToString(b[:])
}
func (f *Flow) String() string               { return "ChatGPT sign-in attempt" }
func (f *Flow) GoString() string             { return f.String() }
func (f *Flow) MarshalJSON() ([]byte, error) { return nil, ErrDenied }
func (f *Flow) RegistrationID() string       { return f.id }

// AuthorizationURL is intended only for the user's browser. Do not log it.
func (f *Flow) AuthorizationURL() string { return f.authorization }
func (f *Flow) finish(r Registration, e error) {
	f.once.Do(func() { f.result = r; f.err = e; close(f.done); f.cancel() })
}
func (f *Flow) Close() { f.finish(Registration{}, context.Canceled) }
func (f *Flow) Wait(ctx context.Context) (Registration, error) {
	select {
	case <-ctx.Done():
		f.Close()
		return Registration{}, ctx.Err()
	case <-f.done:
		return f.result, f.err
	}
}

// Begin starts the exact loopback listener before generating the browser URL.
// Empty registrationID requests Kingdom's own dynamic registration; subsequent
// attempts reuse only this owner's retained issued client and validated account.
func (s *Service) Begin(ctx context.Context, owner, registrationID string) (*Flow, error) {
	ctx, operationDone, enterErr := s.enter(ctx)
	if enterErr != nil {
		return nil, enterErr
	}
	handedOff := false
	defer func() {
		if !handedOff {
			operationDone()
		}
	}()
	if e := s.discover(ctx); e != nil {
		return nil, e
	}
	select {
	case s.slots <- struct{}{}:
	default:
		return nil, ErrUnavailable
	}
	release := true
	defer func() {
		if release {
			<-s.slots
		}
	}()
	var r record
	var e error
	if registrationID == "" {
		r, e = s.vault.newRegistration(ctx, owner)
	} else {
		r, e = s.vault.read(ctx, owner, registrationID)
	}
	if e != nil {
		return nil, e
	}
	if r.State == "revoking" || r.State == "revoking_uncertain" {
		return nil, ErrDenied
	}
	l, e := net.Listen("tcp4", "127.0.0.1:0")
	if e != nil {
		return nil, ErrUnavailable
	}
	lifetime, cancel := context.WithTimeout(ctx, 5*time.Minute)
	f := &Flow{id: r.ID, redirect: "http://" + l.Addr().String() + callbackPath, state: randomValue(), nonce: randomValue(), verifier: randomValue(), initial: r, service: s, ctx: lifetime, cancel: cancel, done: make(chan struct{})}
	hash := sha256.Sum256([]byte(f.verifier))
	client := r.ClientID
	if client == "" {
		client = "dynamic_agent_client"
	}
	q := url.Values{"client_id": {client}, "ext_agent_host_id": {s.vault.host}, "response_type": {"code"}, "redirect_uri": {f.redirect}, "scope": {scopes}, "resource": {resource}, "state": {f.state}, "nonce": {f.nonce}, "code_challenge_method": {"S256"}, "code_challenge": {base64.RawURLEncoding.EncodeToString(hash[:])}}
	if r.ClientID == "" {
		q.Set("agent_name_hint", "Kingdom")
	}
	f.authorization = authorizeURL + "?" + q.Encode()
	f.server = &http.Server{Handler: f, ReadHeaderTimeout: 3 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: time.Second, MaxHeaderBytes: 8192}
	// Four accepted connections cap slow local clients independently of handlers.
	limited := &limitedListener{Listener: l, slots: make(chan struct{}, 4)}
	go func() {
		defer func() { <-s.slots }()
		go func() {
			<-lifetime.Done()
			f.finish(Registration{}, ErrUnavailable)
			_ = f.server.Shutdown(context.Background())
			operationDone()
		}()
		_ = f.server.Serve(limited)
		f.finish(Registration{}, ErrUnavailable)
	}()
	release = false
	handedOff = true
	return f, nil
}
func (f *Flow) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	target, _ := url.Parse(f.redirect)
	if r.Method != "GET" || r.Host != target.Host || r.URL.Path != callbackPath || r.URL.RawPath != "" || len(r.URL.RawQuery) > 8192 {
		http.Error(w, "Invalid sign-in callback.", 400)
		return
	}
	q, e := url.ParseQuery(r.URL.RawQuery)
	if e != nil {
		http.Error(w, "Invalid sign-in callback.", 400)
		return
	}
	for k, values := range q {
		if len(values) != 1 || (k != "iss" && k != "state" && k != "code" && k != "client_id" && k != "scope" && k != "error" && k != "error_description") {
			http.Error(w, "Invalid sign-in callback.", 400)
			return
		}
	}
	if q.Has("iss") && q.Get("iss") != issuer {
		http.Error(w, "Invalid sign-in callback.", 400)
		return
	}
	if subtle.ConstantTimeCompare([]byte(q.Get("state")), []byte(f.state)) != 1 {
		http.Error(w, "Invalid sign-in callback.", 400)
		return
	}
	unlock, lockErr := f.service.vault.lock(f.ctx, f.id)
	if lockErr != nil {
		http.Error(w, "Registration is unavailable.", 503)
		f.finish(Registration{}, lockErr)
		return
	}
	defer unlock()
	select {
	case <-f.done:
		http.Error(w, "Sign-in attempt already ended.", 400)
		return
	default:
	}
	if q.Get("error") != "" {
		f.finish(Registration{}, ErrDenied)
		http.Error(w, "ChatGPT sign-in was not approved.", 400)
		return
	}
	code := q.Get("code")
	if !validToken(code) {
		f.finish(Registration{}, ErrDenied)
		http.Error(w, "Invalid sign-in callback.", 400)
		return
	}
	current, e := f.service.vault.read(f.ctx, f.initial.owner, f.id)
	if e != nil || current.Revision != f.initial.Revision || current.version != f.initial.version || current.State != f.initial.State || (current.State == "revoking" || current.State == "revoking_uncertain") {
		f.finish(Registration{}, ErrDenied)
		http.Error(w, "Registration changed. Start sign-in again.", 400)
		return
	}
	client := q.Get("client_id")
	if client == "" {
		client = current.ClientID
	}
	if !clientID(client) || current.ClientID != "" && current.ClientID != client {
		f.finish(Registration{}, ErrDenied)
		http.Error(w, "Invalid ChatGPT registration.", 400)
		return
	}
	// This commit precedes exchange, including a later invalid_grant response.
	if e = f.service.vault.retainClient(f.ctx, current, client); e != nil {
		f.finish(Registration{}, e)
		http.Error(w, "Registration could not be saved.", 500)
		return
	}
	current.ClientID = client
	// An exchange can create remote credentials even if its response is lost.
	if e = f.service.vault.transition(f.ctx, current, "exchanging", false); e != nil {
		http.Error(w, "Registration could not be saved.", 500)
		f.finish(Registration{}, e)
		return
	}
	current.State = "exchanging"
	tokens, identity, e := f.service.tokens(f.ctx, url.Values{"grant_type": {"authorization_code"}, "client_id": {client}, "code": {code}, "code_verifier": {f.verifier}, "redirect_uri": {f.redirect}, "resource": {resource}}, current, f.nonce, false)
	if e == nil {
		next := current
		next.Registration = identity
		next.Revision++
		next.version++
		next.State = "ready"
		e = f.service.vault.save(f.ctx, current, next, tokens)
		if e == nil {
			fmt.Fprintln(w, "ChatGPT connected. You can close this window.")
			if flush, ok := w.(http.Flusher); ok {
				flush.Flush()
			}
			f.finish(next.Registration, nil)
			return
		}
	}
	http.Error(w, "ChatGPT sign-in could not be verified. Start sign-in again.", 400)
	if flush, ok := w.(http.Flusher); ok {
		flush.Flush()
	}
	f.finish(Registration{}, e)
}

type limitedListener struct {
	net.Listener
	slots chan struct{}
}

func (l *limitedListener) Accept() (net.Conn, error) {
	for {
		c, e := l.Listener.Accept()
		if e != nil {
			return nil, e
		}
		select {
		case l.slots <- struct{}{}:
			return &limitedConn{Conn: c, release: func() { <-l.slots }}, nil
		default:
			_ = c.Close()
		}
	}
}

type limitedConn struct {
	net.Conn
	once    sync.Once
	release func()
}

func (c *limitedConn) Close() error { e := c.Conn.Close(); c.once.Do(c.release); return e }
