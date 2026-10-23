package worker

import (
	"errors"
	policy "github.com/NVIDIA/OpenShell/sdk/go/proto/sandboxv1"
	"kingdom/internal/providerlauncher"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
)

// Egress pins the two operator-owned HTTPS services. Private addresses require
// explicit exact IPs; wildcard hosts, CIDRs, redirects and arbitrary paths are
// not part of this profile. OpenShell independently rejects loopback egress.
type Egress struct {
	ModelURL, CallbackURL string
	AllowedIPs            []string
}

func (e Egress) policy() (map[string]*policy.NetworkPolicyRule, error) {
	if e.ModelURL == "" && e.CallbackURL == "" && len(e.AllowedIPs) == 0 {
		return nil, nil
	}
	for _, value := range e.AllowedIPs {
		ip, err := netip.ParseAddr(value)
		if err != nil || ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsUnspecified() || ip.String() != value {
			return nil, errors.New("invalid worker egress address pin")
		}
	}
	result := map[string]*policy.NetworkPolicyRule{}
	for _, service := range []struct {
		name, base string
		paths      []string
	}{{"model", e.ModelURL, []string{"/responses", "/chat/completions"}}, {"callback", e.CallbackURL, []string{"/v1/native/invoke", "/v1/command/invoke", "/v1/command/cancel"}}} {
		u, err := url.Parse(service.base)
		if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.RawPath != "" || u.Opaque != "" || strings.ContainsAny(u.Hostname(), "*?[] \\%") || strings.HasSuffix(u.Path, "/") || strings.ContainsAny(u.Path, "*?\\") || strings.Contains(u.Path, "..") || service.name == "model" && !strings.HasSuffix(u.Path, "/v1") {
			return nil, errors.New("invalid worker service endpoint pin")
		}
		port := uint64(443)
		if u.Port() != "" {
			port, err = strconv.ParseUint(u.Port(), 10, 16)
			if err != nil || port == 0 {
				return nil, errors.New("invalid worker service port")
			}
		}
		rules := make([]*policy.L7Rule, 0, len(service.paths))
		for _, path := range service.paths {
			rules = append(rules, &policy.L7Rule{Allow: &policy.L7Allow{Method: "POST", Path: u.Path + path}})
		}
		// Current OpenShell performs TLS termination by default. The legacy
		// explicit TERMINATE enum is rejected by the native policy validator.
		endpoint := &policy.NetworkEndpoint{Host: u.Hostname(), Port: uint32(port), Protocol: "rest", Enforcement: policy.NetworkEnforcementMode_NETWORK_ENFORCEMENT_MODE_ENFORCE, Rules: rules, AllowedIps: append([]string(nil), e.AllowedIPs...)}
		result[service.name] = &policy.NetworkPolicyRule{Name: "kingdom-" + service.name, Endpoints: []*policy.NetworkEndpoint{endpoint}, Binaries: []*policy.NetworkBinary{{Path: providerlauncher.OpenCodePath}}}
	}
	return result, nil
}
