package worker

import (
	policy "github.com/NVIDIA/OpenShell/sdk/go/proto/sandboxv1"
	"kingdom/internal/providerlauncher"
	"testing"
)

func TestFixedEgressRejectsBroadOrUntrustedEndpoints(t *testing.T) {
	for _, bad := range []Egress{
		{ModelURL: "http://model.example/v1", CallbackURL: "https://callback.example"},
		{ModelURL: "https://*.example/v1", CallbackURL: "https://callback.example"},
		{ModelURL: "https://model.example/v1?token=secret", CallbackURL: "https://callback.example"},
		{ModelURL: "https://model.example/v1", CallbackURL: "https://callback.example", AllowedIPs: []string{"10.0.0.0/8"}},
		{ModelURL: "https://model.example/v1", CallbackURL: "https://callback.example", AllowedIPs: []string{"127.0.0.1"}},
		{ModelURL: "https://model.example/v1", CallbackURL: "https://callback.example/../escape"},
	} {
		if _, e := bad.policy(); e == nil {
			t.Fatal("unsafe egress accepted")
		}
	}
	p, e := (Egress{ModelURL: "https://model.example:8443/api/v1", CallbackURL: "https://callback.example/callback", AllowedIPs: []string{"10.2.3.4"}}).policy()
	if e != nil {
		t.Fatal(e)
	}
	for _, rule := range p {
		if len(rule.Binaries) != 1 || rule.Binaries[0].Path != providerlauncher.OpenCodePath {
			t.Fatal("egress binary not pinned")
		}
		for _, endpoint := range rule.Endpoints {
			if endpoint.Enforcement != policy.NetworkEnforcementMode_NETWORK_ENFORCEMENT_MODE_ENFORCE || endpoint.Tls != policy.NetworkTlsMode_NETWORK_TLS_MODE_UNSPECIFIED {
				t.Fatal("egress not enforced")
			}
			for _, r := range endpoint.Rules {
				if r.Allow.Method != "POST" {
					t.Fatal("arbitrary method allowed")
				}
			}
		}
	}
	if len(p["callback"].Endpoints[0].Rules) != 3 {
		t.Fatal("unsupported callback route retained")
	}
	if p["model"].Endpoints[0].Rules[0].Allow.Path != "/api/v1/responses" || p["callback"].Endpoints[0].Rules[0].Allow.Path != "/callback/v1/native/invoke" {
		t.Fatal("fixed paths changed")
	}
}
