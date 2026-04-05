package proxy

import (
	"strings"
)

// SubdomainFromHost returns the tunnel subdomain if host matches "{sub}."+baseDomain.
// baseDomain is e.g. "horizon.live" or "localhost" (no port).
func SubdomainFromHost(host, baseDomain string) (sub string, ok bool) {
	host = strings.TrimSpace(host)
	if h, _, found := strings.Cut(host, ":"); found {
		host = h
	}
	suffix := "." + baseDomain
	if !strings.HasSuffix(host, suffix) {
		return "", false
	}
	prefix := strings.TrimSuffix(host, suffix)
	if prefix == "" || strings.Contains(prefix, ".") {
		return "", false
	}
	return prefix, true
}
