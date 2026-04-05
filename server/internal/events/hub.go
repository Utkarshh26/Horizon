// Package events broadcasts real-time JSON events to dashboard WebSocket clients.
package events

import (
	"encoding/json"
	"log"
	"sync"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

const (
	TypeTunnelUp   = "tunnel_up"
	TypeTunnelDown = "tunnel_down"
	TypeRequest    = "request"
)

// Event is a small envelope pushed to dashboards.
type Event struct {
	Type    string      `json:"type"`
	Payload interface{} `json:"payload"`
}

// Hub tracks dashboard websocket subscribers and fans out events.
type Hub struct {
	mu     sync.RWMutex
	conns  map[*websocket.Conn]struct{}
	writeM map[*websocket.Conn]*sync.Mutex
}

// NewHub builds an empty hub.
func NewHub() *Hub {
	return &Hub{
		conns:  make(map[*websocket.Conn]struct{}),
		writeM: make(map[*websocket.Conn]*sync.Mutex),
	}
}

// Register adds a dashboard client connection.
func (h *Hub) Register(c *websocket.Conn) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.conns[c] = struct{}{}
	h.writeM[c] = &sync.Mutex{}
}

// Unregister removes a client.
func (h *Hub) Unregister(c *websocket.Conn) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.conns, c)
	delete(h.writeM, c)
}

// Publish sends an event to all connected dashboards (best-effort).
func (h *Hub) Publish(ev Event) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	b, err := json.Marshal(ev)
	if err != nil {
		return
	}
	for c := range h.conns {
		mu := h.writeM[c]
		if mu == nil {
			continue
		}
		mu.Lock()
		err := c.WriteMessage(websocket.TextMessage, b)
		mu.Unlock()
		if err != nil {
			log.Printf("events: write failed: %v", err)
			_ = c.Close()
		}
	}
}

// NewRequestID generates a UUID string for correlation (optional helper).
func NewRequestID() string {
	return uuid.NewString()
}
