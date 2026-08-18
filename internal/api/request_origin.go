package api

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
)

const browserBootstrapHeader = "X-Kingdom-Browser-Bootstrap"

type requestOrigin struct {
	Scheme     string
	Hostname   string
	Port       string
	Serialized string
}

func requireLocalBrowserBootstrap(r *http.Request) (requestOrigin, error) {
	origin, err := directLocalRequestOrigin(r)
	if err != nil {
		return requestOrigin{}, err
	}
	if r.Header.Get(browserBootstrapHeader) != "1" {
		return requestOrigin{}, fmt.Errorf("browser bootstrap header is required")
	}
	if site := strings.TrimSpace(r.Header.Get("Sec-Fetch-Site")); site != "" && site != "same-origin" {
		return requestOrigin{}, fmt.Errorf("browser bootstrap must be same-origin")
	}
	provided, err := parseOriginHeader(r.Header.Values("Origin"))
	if err != nil || provided != origin {
		return requestOrigin{}, fmt.Errorf("browser bootstrap origin is invalid")
	}
	return origin, nil
}

func directLocalRequestOrigin(r *http.Request) (requestOrigin, error) {
	if r == nil {
		return requestOrigin{}, fmt.Errorf("request is required")
	}
	if hasForwardingHeaders(r.Header) {
		return requestOrigin{}, fmt.Errorf("forwarded requests are not accepted")
	}
	loopback, err := directPeerIsLoopback(r)
	if err != nil || !loopback {
		return requestOrigin{}, fmt.Errorf("direct peer is not loopback")
	}
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	origin, local, err := canonicalOrigin(scheme, r.Host)
	if err != nil || !local {
		return requestOrigin{}, fmt.Errorf("request host is not loopback")
	}
	return origin, nil
}

func directPeerIsLoopback(r *http.Request) (bool, error) {
	if r == nil {
		return false, fmt.Errorf("request is required")
	}
	host, _, err := net.SplitHostPort(strings.TrimSpace(r.RemoteAddr))
	if err != nil {
		return false, fmt.Errorf("direct peer address is invalid")
	}
	peer, err := netip.ParseAddr(host)
	if err != nil {
		return false, fmt.Errorf("direct peer address is invalid")
	}
	return peer.Unmap().IsLoopback(), nil
}

func canonicalOrigin(scheme, authority string) (requestOrigin, bool, error) {
	if scheme != "http" && scheme != "https" {
		return requestOrigin{}, false, fmt.Errorf("unsupported origin scheme")
	}
	if authority == "" || authority != strings.TrimSpace(authority) {
		return requestOrigin{}, false, fmt.Errorf("origin authority is invalid")
	}
	parsed, err := url.Parse(scheme + "://" + authority)
	if err != nil || parsed.User != nil || parsed.Host == "" || parsed.Path != "" || parsed.RawQuery != "" || parsed.Fragment != "" {
		return requestOrigin{}, false, fmt.Errorf("origin authority is invalid")
	}
	hostname := parsed.Hostname()
	if hostname == "" || strings.HasSuffix(hostname, ".") {
		return requestOrigin{}, false, fmt.Errorf("origin hostname is invalid")
	}

	local := false
	normalizedHost := ""
	if strings.EqualFold(hostname, "localhost") {
		normalizedHost = "localhost"
		local = true
	} else {
		addr, parseErr := netip.ParseAddr(hostname)
		if parseErr != nil || addr.Zone() != "" {
			return requestOrigin{}, false, fmt.Errorf("origin hostname must be an IP literal or localhost")
		}
		addr = addr.Unmap()
		normalizedHost = addr.String()
		local = addr.IsLoopback()
	}

	port := parsed.Port()
	if port == "" {
		if scheme == "https" {
			port = "443"
		} else {
			port = "80"
		}
	} else {
		value, parseErr := strconv.Atoi(port)
		if parseErr != nil || value < 1 || value > 65535 {
			return requestOrigin{}, false, fmt.Errorf("origin port is invalid")
		}
		port = strconv.Itoa(value)
	}

	serializedHost := normalizedHost
	if strings.Contains(normalizedHost, ":") {
		serializedHost = "[" + normalizedHost + "]"
	}
	serialized := scheme + "://" + serializedHost
	if (scheme == "http" && port != "80") || (scheme == "https" && port != "443") {
		serialized += ":" + port
	}
	return requestOrigin{Scheme: scheme, Hostname: normalizedHost, Port: port, Serialized: serialized}, local, nil
}

func parseOriginHeader(values []string) (requestOrigin, error) {
	if len(values) != 1 || values[0] == "" || strings.Contains(values[0], ",") {
		return requestOrigin{}, fmt.Errorf("exactly one origin is required")
	}
	parsed, err := url.Parse(values[0])
	if err != nil || parsed.Opaque != "" || parsed.User != nil || parsed.Host == "" || parsed.Path != "" || parsed.RawQuery != "" || parsed.Fragment != "" {
		return requestOrigin{}, fmt.Errorf("origin is invalid")
	}
	origin, _, err := canonicalOrigin(strings.ToLower(parsed.Scheme), parsed.Host)
	return origin, err
}

func hasForwardingHeaders(header http.Header) bool {
	for name := range header {
		lower := strings.ToLower(name)
		if lower == "forwarded" || lower == "via" || strings.HasPrefix(lower, "x-forwarded-") || lower == "x-real-ip" {
			return true
		}
	}
	return false
}

func browserSessionCookieName(origin requestOrigin, basePath string) string {
	sum := sha256.Sum256([]byte(origin.Serialized + "\x00" + NormalizeBasePath(basePath)))
	return "kingdom_session_" + hex.EncodeToString(sum[:12])
}
