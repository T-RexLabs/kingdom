package modelproxy

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"path"
	"strings"
	"time"
)

func origin(u *url.URL) string { return "https://" + strings.ToLower(u.Hostname()) + ":" + port(u) }
func port(u *url.URL) string {
	if u.Port() != "" {
		return u.Port()
	}
	return "443"
}
func parseEndpoint(raw string) (*url.URL, error) {
	u, e := url.Parse(raw)
	if e != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.RawPath != "" || u.Opaque != "" || strings.Contains(u.Host, "%") || u.Path != "" && path.Clean(u.Path) != u.Path {
		return nil, ErrInvalid
	}
	return u, nil
}
func (b *Broker) validateRoute(r Route) error {
	if r.Protocol != "openai" || r.Model == "" || len(r.Model) > 1024 {
		return ErrDenied
	}
	u, e := parseEndpoint(r.Endpoint)
	if e != nil {
		return ErrDenied
	}
	for _, p := range b.cfg.Origins {
		target, e := parseEndpoint(p.Origin)
		if e == nil && origin(target) == origin(u) {
			return nil
		}
	}
	return ErrDenied
}
func newTransport(cfg Config) (*http.Transport, error) {
	policies := map[string]OriginPolicy{}
	for _, p := range cfg.Origins {
		u, e := parseEndpoint(p.Origin)
		if e != nil || u.Path != "" && u.Path != "/" {
			return nil, ErrInvalid
		}
		if _, exists := policies[origin(u)]; exists {
			return nil, ErrInvalid
		}
		for _, prefix := range p.AllowedPrivateAddresses {
			if !prefix.IsValid() {
				return nil, ErrInvalid
			}
		}
		copyPolicy := p
		copyPolicy.AllowedPrivateAddresses = append([]netip.Prefix(nil), p.AllowedPrivateAddresses...)
		policies[origin(u)] = copyPolicy
	}
	if len(policies) == 0 {
		return nil, ErrInvalid
	}
	dialer := net.Dialer{Timeout: 15 * time.Second, KeepAlive: 30 * time.Second}
	var roots = cfg.TLSRoots
	if roots != nil {
		roots = roots.Clone()
	}
	transport := &http.Transport{Proxy: nil, ForceAttemptHTTP2: true, TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}, TLSHandshakeTimeout: 15 * time.Second, ResponseHeaderTimeout: 30 * time.Second, MaxResponseHeaderBytes: 16384, DisableCompression: true, MaxConnsPerHost: 32, MaxIdleConns: 32, IdleConnTimeout: 30 * time.Second}
	transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		host, p, e := net.SplitHostPort(address)
		if e != nil {
			return nil, ErrDenied
		}
		policy, ok := policies["https://"+strings.ToLower(host)+":"+p]
		if !ok {
			return nil, ErrDenied
		}
		addresses, e := net.DefaultResolver.LookupNetIP(ctx, "ip", host)
		if e != nil || len(addresses) == 0 {
			return nil, ErrDenied
		}
		for _, ip := range addresses {
			ip = ip.Unmap()
			allowed := publicAddress(ip)
			if !allowed {
				for _, prefix := range policy.AllowedPrivateAddresses {
					if prefix.Contains(ip) {
						allowed = true
						break
					}
				}
			}
			if !allowed {
				return nil, ErrDenied
			}
		}
		for _, ip := range addresses {
			connection, e := dialer.DialContext(ctx, network, net.JoinHostPort(ip.String(), p))
			if e == nil {
				return connection, nil
			}
			if ctx.Err() != nil {
				break
			}
		}
		return nil, ErrDenied
	}
	return transport, nil
}
func publicAddress(ip netip.Addr) bool {
	if !ip.IsValid() || !ip.IsGlobalUnicast() || ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast() {
		return false
	}
	for _, raw := range []string{"100.64.0.0/10", "192.0.0.0/24", "192.0.2.0/24", "198.18.0.0/15", "198.51.100.0/24", "203.0.113.0/24", "240.0.0.0/4", "2001:db8::/32", "2001::/32", "2002::/16", "64:ff9b::/96", "64:ff9b:1::/48", "fec0::/10"} {
		if netip.MustParsePrefix(raw).Contains(ip) {
			return false
		}
	}
	return true
}
