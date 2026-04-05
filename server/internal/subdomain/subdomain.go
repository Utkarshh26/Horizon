// Package subdomain generates unique short host labels for tunnel URLs.
package subdomain

import (
	"crypto/rand"
	"io"
)

const (
	// Length is the number of characters in each assigned subdomain (e.g. abc123).
	Length = 6
	// charset excludes ambiguous characters; URL-safe unreserved subset.
	charset = "abcdefghijklmnopqrstuvwxyz0123456789"
)

// Generate returns a cryptographically random subdomain string of Length characters.
func Generate() (string, error) {
	b := make([]byte, Length)
	max := byte(255 - (256 % len(charset)))
	for i := range b {
		for {
			if _, err := io.ReadFull(rand.Reader, b[i:i+1]); err != nil {
				return "", err
			}
			v := b[i]
			if v <= max {
				b[i] = charset[int(v)%len(charset)]
				break
			}
		}
	}
	return string(b), nil
}
