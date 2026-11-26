package auth

import (
	"strings"
	"testing"
)

func TestAdmissionIdentityUsesOnlyConfiguredProviderAndStableAccountID(t *testing.T) {
	login := &Login{providers: map[string]*loginProvider{"github": {name: "github", issuer: "https://github.com"}, "oidc": {name: "oidc", issuer: "https://organization.example"}}}
	for _, id := range []string{"", "0", "-1", "+1", "01", "1 ", " 1", "alice", "alice@example.com", "9223372036854775808"} {
		if _, err := login.AdmissionIdentity("github", id); err == nil {
			t.Fatalf("noncanonical GitHub ID %q accepted", id)
		}
	}
	id, err := login.AdmissionIdentity("github", "123456")
	if err != nil || id.Issuer != "https://github.com" || id.Subject != "123456" {
		t.Fatal("GitHub admission identity differs", err)
	}
	id, err = login.AdmissionIdentity("oidc", "Case-Sensitive-Subject")
	if err != nil || id.Issuer != "https://organization.example" || id.Subject != "Case-Sensitive-Subject" {
		t.Fatal("OIDC subject changed", err)
	}
	for _, subject := range []string{" Subject", "Subject ", "a\nb", strings.Repeat("x", 513)} {
		if _, err := login.AdmissionIdentity("oidc", subject); err == nil {
			t.Fatal("OIDC subject normalized or invalid subject accepted")
		}
	}
	if _, err = login.AdmissionIdentity("unconfigured", "123"); err == nil {
		t.Fatal("unconfigured provider admitted")
	}
	providers := login.AdmissionProviders()
	if len(providers) != 2 || providers[0].ID != "github" || providers[1].ID != "oidc" {
		t.Fatal("configured provider metadata differs")
	}
	delete(login.providers, "oidc")
	if _, err = login.AdmissionIdentity("oidc", "Subject"); err == nil {
		t.Fatal("removed provider admitted")
	}
}
