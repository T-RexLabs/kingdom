// Package access retains execution identity and credential revision references.
// It never stores secrets and never grants lasting central authorization.
package access

import (
	"errors"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/google/uuid"
)

var ErrInvalid = errors.New("invalid execution access snapshot")

type Domain string

const (
	DomainHosted     Domain = "hosted"
	DomainLocalOwner Domain = "local_owner"
)

type Connection struct {
	ID       string `json:"id"`
	Revision int64  `json:"revision"`
	OwnerID  string `json:"owner_id"`
	Scope    string `json:"scope"`
	Provider string `json:"provider"`
}

type Selection struct {
	Model          string     `json:"model"`
	ThinkingEffort string     `json:"thinking_effort,omitempty"`
	Connection     Connection `json:"connection"`
}

type Snapshot struct {
	Version   int       `json:"version"`
	Domain    Domain    `json:"domain,omitempty"`
	ActorID   string    `json:"actor_id"`
	ProjectID string    `json:"project_id"`
	Selection Selection `json:"selection"`
}

type RunSnapshot struct {
	Version   int                  `json:"version"`
	Domain    Domain               `json:"domain,omitempty"`
	ActorID   string               `json:"actor_id"`
	ProjectID string               `json:"project_id"`
	Global    Selection            `json:"global"`
	Roles     map[string]Selection `json:"roles,omitempty"`
}

func validID(s string) bool {
	parsed, err := uuid.Parse(s)
	return err == nil && parsed != uuid.Nil && parsed.String() == s
}
func validText(s string, max int) bool {
	return s != "" && strings.TrimSpace(s) == s && len(s) <= max && utf8.ValidString(s) && strings.IndexFunc(s, unicode.IsControl) < 0
}

// AuthorityDomain preserves the meaning of retained v1 hosted receipts. New
// local ownership is always explicit and cannot inherit central membership.
func (s Snapshot) AuthorityDomain() Domain {
	if s.Version == 1 && s.Domain == "" {
		return DomainHosted
	}
	return s.Domain
}

func (s Snapshot) Validate() error {
	c := s.Selection.Connection
	if !((s.Version == 1 && s.Domain == "") || (s.Version == 2 && (s.Domain == DomainHosted || s.Domain == DomainLocalOwner))) || !validID(s.ActorID) || !validID(s.ProjectID) || !validID(c.ID) || !validID(c.OwnerID) || c.Revision < 1 {
		return ErrInvalid
	}
	if s.AuthorityDomain() == DomainLocalOwner {
		if c.Scope != "local_owner" || c.OwnerID != s.ActorID {
			return ErrInvalid
		}
	} else if (c.Scope != "personal" && c.Scope != "team") || (c.Scope == "personal" && c.OwnerID != s.ActorID) {
		return ErrInvalid
	}
	if !validText(c.Provider, 256) || !validText(s.Selection.Model, 1024) || !strings.HasPrefix(s.Selection.Model, c.Provider+"/") || len(s.Selection.Model) == len(c.Provider)+1 || (s.Selection.ThinkingEffort != "" && !validText(s.Selection.ThinkingEffort, 128)) {
		return ErrInvalid
	}
	for _, ch := range c.Provider {
		if !(ch >= 'a' && ch <= 'z' || ch >= '0' && ch <= '9' || ch == '-' || ch == '_' || ch == '.') {
			return ErrInvalid
		}
	}
	return nil
}

func (s RunSnapshot) Validate() error {
	if len(s.Roles) > 64 {
		return ErrInvalid
	}
	if err := s.snapshot(s.Global).Validate(); err != nil {
		return err
	}
	for role, selection := range s.Roles {
		if !validText(role, 256) {
			return ErrInvalid
		}
		if err := s.snapshot(selection).Validate(); err != nil {
			return err
		}
	}
	return nil
}
func (s RunSnapshot) snapshot(selection Selection) Snapshot {
	return Snapshot{Version: s.Version, Domain: s.Domain, ActorID: s.ActorID, ProjectID: s.ProjectID, Selection: selection}
}

// ForRole resolves the entire selection. A role override never borrows another
// model's credential or thinking effort from the global selection.
func (s RunSnapshot) ForRole(role string) (Snapshot, error) {
	if err := s.Validate(); err != nil {
		return Snapshot{}, err
	}
	selection, found := s.Roles[role]
	if !found {
		selection = s.Global
	}
	return s.snapshot(selection), nil
}
