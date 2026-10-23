package server

import (
	"context"
	"encoding/json"
	"io"
	"mime"
	"net/http"
	"strings"

	"kingdom/internal/central/personal"
)

type PersonalAccounts interface {
	Providers() []string
	Begin(context.Context, string, string, string, string, string) (personal.SignIn, error)
	Get(context.Context, string, string, string) (personal.SignIn, error)
	Cancel(context.Context, string, string, string) error
	Disconnect(context.Context, string, string, string) (bool, error)
	PrepareHandoff(context.Context, string) (personal.HandoffTarget, error)
	CompleteHandoff(context.Context, string, json.RawMessage) (personal.SignIn, error)
}

func (s *Server) personalAccountMethods(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.actor(w, r); !ok {
		return
	}
	providers := []string{}
	if s.cfg.PersonalAccounts != nil {
		providers = s.cfg.PersonalAccounts.Providers()
	}
	write(w, 200, struct {
		Providers []string `json:"providers"`
	}{providers})
}
func (s *Server) beginPersonalAccount(w http.ResponseWriter, r *http.Request) {
	session, err := s.cfg.Sessions.AuthenticatedSession(r)
	if err != nil {
		fail(w, err)
		return
	}
	if s.cfg.PersonalAccounts == nil {
		fail(w, personal.ErrDenied)
		return
	}
	k, ok := key(w, r)
	if !ok {
		return
	}
	var req struct {
		Provider string `json:"provider"`
		Label    string `json:"label"`
	}
	if !decode(w, r, &req) {
		return
	}
	v, err := s.cfg.PersonalAccounts.Begin(r.Context(), session.User.ID, session.SessionID, req.Provider, req.Label, k)
	if err != nil {
		fail(w, err)
		return
	}
	write(w, 201, v)
}
func (s *Server) personalAccountAttempt(w http.ResponseWriter, r *http.Request) {
	session, err := s.cfg.Sessions.AuthenticatedSession(r)
	if err != nil {
		fail(w, err)
		return
	}
	if s.cfg.PersonalAccounts == nil {
		fail(w, personal.ErrDenied)
		return
	}
	id := r.PathValue("attempt")
	if !validID(id) {
		http.NotFound(w, r)
		return
	}
	if r.Method == http.MethodDelete {
		if _, ok := key(w, r); !ok {
			return
		}
		if err = s.cfg.PersonalAccounts.Cancel(r.Context(), session.User.ID, session.SessionID, id); err != nil {
			fail(w, err)
			return
		}
		w.WriteHeader(204)
		return
	}
	v, err := s.cfg.PersonalAccounts.Get(r.Context(), session.User.ID, session.SessionID, id)
	if err != nil {
		fail(w, err)
		return
	}
	write(w, 200, v)
}
func (s *Server) accountHandoff(w http.ResponseWriter, r *http.Request) {
	if r.TLS == nil || s.cfg.PersonalAccounts == nil {
		fail(w, personal.ErrDenied)
		return
	}
	values := r.Header.Values("Authorization")
	if len(values) != 1 || !strings.HasPrefix(values[0], "Bearer ") || len(values[0]) != 50 {
		fail(w, personal.ErrDenied)
		return
	}
	kind, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || kind != "application/json" || r.Header.Get("Content-Encoding") != "" {
		w.WriteHeader(400)
		return
	}
	var req struct {
		Action   string          `json:"action"`
		Transfer json.RawMessage `json:"transfer,omitempty"`
	}
	d := json.NewDecoder(http.MaxBytesReader(w, r.Body, 256<<10))
	start, err := d.Token()
	if err != nil || start != json.Delim('{') {
		w.WriteHeader(400)
		return
	}
	seen := map[string]bool{}
	for d.More() {
		token, e := d.Token()
		name, ok := token.(string)
		if e != nil || !ok || seen[name] {
			w.WriteHeader(400)
			return
		}
		seen[name] = true
		switch name {
		case "action":
			e = d.Decode(&req.Action)
		case "transfer":
			e = d.Decode(&req.Transfer)
		default:
			w.WriteHeader(400)
			return
		}
		if e != nil {
			w.WriteHeader(400)
			return
		}
	}
	end, err := d.Token()
	if err != nil || end != json.Delim('}') || d.Decode(new(any)) != io.EOF {
		w.WriteHeader(400)
		return
	}
	code := strings.TrimPrefix(values[0], "Bearer ")
	switch req.Action {
	case "prepare":
		if len(req.Transfer) != 0 {
			w.WriteHeader(400)
			return
		}
		v, err := s.cfg.PersonalAccounts.PrepareHandoff(r.Context(), code)
		if err != nil {
			fail(w, err)
			return
		}
		write(w, 200, v)
	case "complete":
		if len(req.Transfer) == 0 {
			w.WriteHeader(400)
			return
		}
		v, err := s.cfg.PersonalAccounts.CompleteHandoff(r.Context(), code, req.Transfer)
		if err != nil {
			fail(w, err)
			return
		}
		write(w, 200, v)
	default:
		w.WriteHeader(400)
	}
}
