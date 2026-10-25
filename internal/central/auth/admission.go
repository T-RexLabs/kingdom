package auth

import (
	"strconv"
	"strings"

	"kingdom/internal/central/catalog"
)

// AdmissionProvider describes only the account ID needed for preauthorization.
// The configured issuer and all OAuth credentials remain server-owned.
type AdmissionProvider struct {
	ID             string `json:"id"`
	Label          string `json:"label"`
	AccountIDLabel string `json:"account_id_label"`
	Help           string `json:"help"`
}

func (l *Login) AdmissionProviders() []AdmissionProvider {
	out := []AdmissionProvider{}
	for _, id := range []string{"github", "oidc"} {
		if _, ok := l.providers[id]; !ok {
			continue
		}
		if id == "github" {
			out = append(out, AdmissionProvider{ID: id, Label: "GitHub", AccountIDLabel: "GitHub User ID", Help: "Use the permanent numeric GitHub user ID. A username or email address cannot grant access."})
		} else {
			out = append(out, AdmissionProvider{ID: id, Label: "Organization Sign-In", AccountIDLabel: "Organization Account ID", Help: "Use the exact stable account ID supplied by your identity administrator. Preserve its case. An email address does not link accounts."})
		}
	}
	return out
}

// AdmissionIdentity resolves an administrator's provider choice through the
// same immutable provider configuration used to verify sign-in assertions.
func (l *Login) AdmissionIdentity(provider, account string) (catalog.Identity, error) {
	p, ok := l.providers[provider]
	if !ok || account == "" || len(account) > 512 || strings.TrimSpace(account) != account || strings.ContainsAny(account, "\x00\r\n") {
		return catalog.Identity{}, catalog.ErrInvalid
	}
	if provider == "github" {
		id, err := strconv.ParseInt(account, 10, 64)
		if err != nil || id <= 0 || strconv.FormatInt(id, 10) != account {
			return catalog.Identity{}, catalog.ErrInvalid
		}
	}
	if provider != "github" && provider != "oidc" {
		return catalog.Identity{}, catalog.ErrInvalid
	}
	return catalog.Identity{Issuer: p.issuer, Subject: account}, nil
}

// AdmissionReference returns only the configured provider alias. A historical
// identity for a removed provider is not made available through another issuer.
func (l *Login) AdmissionReference(identity catalog.Identity) (string, string, bool) {
	for _, id := range []string{"github", "oidc"} {
		if p, ok := l.providers[id]; ok && p.issuer == identity.Issuer {
			return id, identity.Subject, true
		}
	}
	return "", "", false
}
