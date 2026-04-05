// Package registry holds an in-memory, thread-safe map from subdomain to tunnel clients.
package registry

import (
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

// PendingResult is delivered to the HTTP proxy goroutine when the CLI responds (or on error).
type PendingResult struct {
	Status     int
	Headers    http.Header
	HeaderMap  map[string]string // original map from JSON (for logging)
	Body       []byte
	Err        error
}

// ClientConnection represents one CLI tunnel agent connected over WebSocket.
type ClientConnection struct {
	ID          uuid.UUID
	Subdomain   string
	LocalPort   int
	ConnectedAt time.Time
	Conn        *websocket.Conn
	// writeMu serializes writes to Conn; gorilla/websocket requires one writer at a time.
	writeMu sync.Mutex
	// pending matches request id → waiter channel for the public HTTP handler.
	pendingMu sync.Mutex
	pending   map[string]chan *PendingResult
	// RequestCount increments after each successful proxied request (for dashboard).
	RequestCount atomic.Int64
}

// WriteJSON sends a JSON message to the tunnel client (thread-safe).
func (c *ClientConnection) WriteJSON(v interface{}) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.Conn.WriteJSON(v)
}

// InstallWebSocketKeepalive sets a read idle timeout and wires ping/pong so it is refreshed on
// every control frame from the peer. The client should send periodic WebSocket pings (e.g. every
// < readIdleTimeout); this handler echoes pongs with the same write lock as WriteJSON.
func (c *ClientConnection) InstallWebSocketKeepalive(readIdleTimeout time.Duration) {
	if readIdleTimeout <= 0 {
		readIdleTimeout = 60 * time.Second
	}
	refresh := func() error {
		return c.Conn.SetReadDeadline(time.Now().Add(readIdleTimeout))
	}
	_ = refresh()

	// Fires when we sent a Ping and the tunnel client replied with Pong.
	c.Conn.SetPongHandler(func(string) error {
		return refresh()
	})

	// Fires when the client sends a Ping (CLI keep-alive); extend deadline and echo Pong.
	c.Conn.SetPingHandler(func(appData string) error {
		if err := refresh(); err != nil {
			return err
		}
		c.writeMu.Lock()
		defer c.writeMu.Unlock()
		return c.Conn.WriteControl(websocket.PongMessage, []byte(appData), time.Now().Add(10*time.Second))
	})
}

// RegisterPending registers a waiter for a proxied request id. Buffer 1 so CompletePending never blocks.
func (c *ClientConnection) RegisterPending(id string) <-chan *PendingResult {
	ch := make(chan *PendingResult, 1)
	c.pendingMu.Lock()
	if c.pending == nil {
		c.pending = make(map[string]chan *PendingResult)
	}
	c.pending[id] = ch
	c.pendingMu.Unlock()
	return ch
}

// UnregisterPending removes a waiter (e.g. timeout) so late responses are ignored.
func (c *ClientConnection) UnregisterPending(id string) {
	c.pendingMu.Lock()
	delete(c.pending, id)
	c.pendingMu.Unlock()
}

// CompletePending delivers a result to the waiting HTTP handler, if still registered.
func (c *ClientConnection) CompletePending(id string, res *PendingResult) {
	c.pendingMu.Lock()
	ch, ok := c.pending[id]
	if ok {
		delete(c.pending, id)
	}
	c.pendingMu.Unlock()
	if !ok {
		return
	}
	select {
	case ch <- res:
	default:
	}
}

// FailPending notifies all waiters (tunnel disconnect).
func (c *ClientConnection) FailPending(reason error) {
	c.pendingMu.Lock()
	defer c.pendingMu.Unlock()
	for id, ch := range c.pending {
		select {
		case ch <- &PendingResult{Status: http.StatusBadGateway, Err: reason}:
		default:
		}
		delete(c.pending, id)
	}
}

// Registry maps subdomain -> active client. Lookups are used by the HTTP proxy
// and WebSocket lifecycle; all methods are safe for concurrent use.
type Registry struct {
	mu     sync.RWMutex
	bySub  map[string]*ClientConnection
	byConn map[*websocket.Conn]*ClientConnection
}

// New builds an empty registry.
func New() *Registry {
	return &Registry{
		bySub:  make(map[string]*ClientConnection),
		byConn: make(map[*websocket.Conn]*ClientConnection),
	}
}

func (c *ClientConnection) initPending() {
	c.pendingMu.Lock()
	if c.pending == nil {
		c.pending = make(map[string]chan *PendingResult)
	}
	c.pendingMu.Unlock()
}

// Register adds a client under subdomain. Returns false if subdomain is already taken.
func (r *Registry) Register(sub string, conn *websocket.Conn, localPort int) (*ClientConnection, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.bySub[sub]; exists {
		return nil, false
	}
	id := uuid.New()
	cl := &ClientConnection{
		ID:          id,
		Subdomain:   sub,
		LocalPort:   localPort,
		ConnectedAt: time.Now().UTC(),
		Conn:        conn,
	}
	cl.initPending()
	r.bySub[sub] = cl
	r.byConn[conn] = cl
	return cl, true
}

// Unregister removes a client by WebSocket connection (e.g. on disconnect).
func (r *Registry) Unregister(conn *websocket.Conn) *ClientConnection {
	r.mu.Lock()
	defer r.mu.Unlock()
	c, ok := r.byConn[conn]
	if !ok {
		return nil
	}
	delete(r.byConn, conn)
	delete(r.bySub, c.Subdomain)
	return c
}

// Get returns the client for a subdomain, or nil if none.
func (r *Registry) Get(sub string) *ClientConnection {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.bySub[sub]
}

// Count returns the number of active tunnels (for metrics / health).
func (r *Registry) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.bySub)
}

// Snapshot returns a shallow copy of active tunnels for the dashboard API.
func (r *Registry) Snapshot() []TunnelSnapshot {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]TunnelSnapshot, 0, len(r.bySub))
	for _, c := range r.bySub {
		out = append(out, TunnelSnapshot{
			ID:           c.ID.String(),
			Subdomain:    c.Subdomain,
			LocalPort:    c.LocalPort,
			ConnectedAt:  c.ConnectedAt,
			RequestCount: c.RequestCount.Load(),
		})
	}
	return out
}

// TunnelSnapshot is JSON-friendly metadata for one live tunnel.
type TunnelSnapshot struct {
	ID           string    `json:"id"`
	Subdomain    string    `json:"subdomain"`
	LocalPort    int       `json:"local_port"`
	ConnectedAt  time.Time `json:"connected_at"`
	RequestCount int64     `json:"request_count"`
}
