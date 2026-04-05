// Package config loads Horizon server settings from environment variables.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds runtime options for the tunnel server.
type Config struct {
	Addr              string
	BaseDomain        string        // e.g. horizon.live or localhost (first label = subdomain)
	PublicScheme      string        // https or http
	PublicPort        string        // e.g. :8080 — appended to public URL when non-empty
	DatabaseURL       string
	ProxyTimeout      time.Duration // wait for client response
	MaxBodyBytes      int64
	ReadHeaderTimeout time.Duration
}

// Load reads configuration from the environment with sensible defaults for local dev.
func Load() Config {
	c := Config{
		Addr:              get("HORIZON_SERVER_ADDR", ":8080"),
		BaseDomain:        get("HORIZON_BASE_DOMAIN", "horizon.live"),
		PublicScheme:      get("HORIZON_PUBLIC_SCHEME", "https"),
		PublicPort:        get("HORIZON_PUBLIC_PORT", ""),
		DatabaseURL:       os.Getenv("DATABASE_URL"),
		ProxyTimeout:      60 * time.Second,
		MaxBodyBytes:      2 << 20, // 2 MiB
		ReadHeaderTimeout: 10 * time.Second,
	}
	if v := os.Getenv("HORIZON_PROXY_TIMEOUT_SEC"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			c.ProxyTimeout = time.Duration(n) * time.Second
		}
	}
	return c
}

// PublicURL returns the user-facing URL for a tunnel subdomain.
func (c Config) PublicURL(sub string) string {
	host := sub + "." + c.BaseDomain
	if c.PublicPort != "" {
		// PublicPort should include leading colon, e.g. ":8080"
		host = host + c.PublicPort
	}
	return fmt.Sprintf("%s://%s", c.PublicScheme, host)
}

// PublicHost is host[:port] without scheme (for display / Host header hints).
func (c Config) PublicHost(sub string) string {
	host := sub + "." + c.BaseDomain
	if c.PublicPort != "" {
		host = host + c.PublicPort
	}
	return host
}

func get(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}
