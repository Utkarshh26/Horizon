// Package protocol defines WebSocket JSON messages between tunnel server and CLI client.
package protocol

const (
	TypeTunnelReady = "tunnel_ready"
	TypeRequest     = "request"
	TypeResponse    = "response"
)

// RequestMessage is sent server → client for each incoming HTTP request on the public URL.
type RequestMessage struct {
	Type    string            `json:"type"`
	ID      string            `json:"id"`
	Method  string            `json:"method"`
	Path    string            `json:"path"`
	Headers map[string]string `json:"headers"`
	Body    string            `json:"body"` // base64.StdEncoding of raw body (empty if none)
}

// ResponseMessage is sent client → server after localhost round-trip.
type ResponseMessage struct {
	Type    string            `json:"type"`
	ID      string            `json:"id"`
	Status  int               `json:"status"`
	Headers map[string]string `json:"headers"`
	Body    string            `json:"body"` // base64
}
