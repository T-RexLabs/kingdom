package github

import "testing"

func TestOfficialSharedAppIdentityIsValid(t *testing.T) {
	if !appSlugPattern.MatchString(OfficialSharedAppSlug) {
		t.Fatalf("official shared App slug is invalid: %q", OfficialSharedAppSlug)
	}
	if !clientIDPattern.MatchString(OfficialSharedAppClientID) {
		t.Fatalf("official shared App client ID is invalid: %q", OfficialSharedAppClientID)
	}
}
