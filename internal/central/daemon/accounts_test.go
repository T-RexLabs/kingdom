package daemon

import (
	"net/netip"
	"testing"

	"kingdom/internal/central/modelproxy"
)

func TestAccountOriginsPreserveOperatorPolicyWithoutDuplicateDefaultPorts(t *testing.T) {
	ip := netip.MustParsePrefix("127.0.0.1/32")
	configured := []modelproxy.OriginPolicy{{Origin: "https://api.openai.com:443", AllowedPrivateAddresses: []netip.Prefix{ip}}, {Origin: "https://api.individual.githubcopilot.com/"}}
	got := accountOrigins(configured)
	if len(got) != 5 || got[0].Origin != configured[0].Origin || len(got[0].AllowedPrivateAddresses) != 1 || got[0].AllowedPrivateAddresses[0] != ip {
		t.Fatal(got)
	}
}
