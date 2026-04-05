// Package tunnel handles WebSocket tunnel agent connections: upgrade, assign subdomain, register.
package tunnel

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/websocket"

	"github.com/horizon/server/internal/config"
	"github.com/horizon/server/internal/db"
	"github.com/horizon/server/internal/events"
	"github.com/horizon/server/internal/protocol"
	"github.com/horizon/server/internal/registry"
	"github.com/horizon/server/internal/subdomain"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

// TunnelReadyMessage is sent once after the tunnel is registered.
type TunnelReadyMessage struct {
	Type       string `json:"type"`
	Subdomain  string `json:"subdomain"`
	PublicURL  string `json:"public_url"`
	TunnelID   string `json:"tunnel_id"`
	PublicHost string `json:"public_host"`
}

// HandleConnect upgrades HTTP to WebSocket, assigns a unique subdomain, registers the client,
// persists the tunnel row, notifies dashboards, sends tunnel_ready, then processes incoming frames.
func HandleConnect(
	reg *registry.Registry,
	store *db.Store,
	hub *events.Hub,
	cfg config.Config,
	w http.ResponseWriter,
	r *http.Request,
) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("tunnel: websocket upgrade failed: %v", err)
		return
	}

	localPort, _ := strconv.Atoi(r.URL.Query().Get("port"))
	if localPort <= 0 {
		localPort = 80
	}

	var client *registry.ClientConnection
	var sub string
	for attempts := 0; attempts < 32; attempts++ {
		s, err := subdomain.Generate()
		if err != nil {
			log.Printf("tunnel: subdomain generate failed: %v", err)
			conn.Close()
			return
		}
		var ok bool
		client, ok = reg.Register(s, conn, localPort)
		if ok {
			sub = s
			break
		}
	}
	if client == nil {
		log.Printf("tunnel: could not assign unique subdomain after retries")
		_ = conn.WriteControl(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseTryAgainLater, "no subdomain"),
			time.Now().Add(time.Second))
		conn.Close()
		return
	}

	if store != nil {
		if err := store.InsertTunnel(r.Context(), client.ID, sub, localPort); err != nil {
			log.Printf("tunnel: db insert tunnel: %v", err)
			reg.Unregister(conn)
			conn.Close()
			return
		}
	}

	msg := TunnelReadyMessage{
		Type:       protocol.TypeTunnelReady,
		Subdomain:  sub,
		PublicURL:  cfg.PublicURL(sub),
		TunnelID:   client.ID.String(),
		PublicHost: cfg.PublicHost(sub),
	}
	if err := client.WriteJSON(msg); err != nil {
		log.Printf("tunnel: send tunnel_ready failed: %v", err)
		reg.Unregister(conn)
		if store != nil {
			_ = store.CloseTunnel(context.Background(), client.ID)
		}
		conn.Close()
		return
	}

	if hub != nil {
		hub.Publish(events.Event{
			Type: events.TypeTunnelUp,
			Payload: map[string]interface{}{
				"id":           client.ID.String(),
				"subdomain":    sub,
				"local_port":   localPort,
				"connected_at": client.ConnectedAt.Format(time.RFC3339Nano),
				"public_url":   cfg.PublicURL(sub),
			},
		})
	}

	log.Printf("tunnel: registered subdomain=%s id=%s port=%d remote=%s", sub, client.ID, localPort, r.RemoteAddr)

	defer func() {
		reg.Unregister(conn)
		client.FailPending(errors.New("tunnel closed"))
		if store != nil {
			_ = store.CloseTunnel(context.Background(), client.ID)
		}
		if hub != nil {
			hub.Publish(events.Event{
				Type: events.TypeTunnelDown,
				Payload: map[string]interface{}{
					"id":        client.ID.String(),
					"subdomain": sub,
				},
			})
		}
		conn.Close()
		log.Printf("tunnel: closed subdomain=%s", sub)
	}()

	conn.SetReadLimit(8 << 20) // 8 MiB cap for JSON payloads
	// Read deadline + ping/pong: CLI sends periodic Ping frames; PingHandler refreshes deadline and
	// echoes Pong (serialized with WriteJSON). PongHandler covers the case we add server→client pings later.
	const readIdleTimeout = 60 * time.Second
	client.InstallWebSocketKeepalive(readIdleTimeout)

	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("tunnel: read error subdomain=%s: %v", sub, err)
			}
			return
		}
		var env struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(data, &env); err != nil {
			continue
		}
		if env.Type != protocol.TypeResponse {
			continue
		}
		var rm protocol.ResponseMessage
		if err := json.Unmarshal(data, &rm); err != nil {
			log.Printf("tunnel: bad response json subdomain=%s: %v", sub, err)
			continue
		}
		body, err := base64.StdEncoding.DecodeString(rm.Body)
		if err != nil {
			log.Printf("tunnel: response body b64 subdomain=%s: %v", sub, err)
			body = nil
		}
		client.CompletePending(rm.ID, &registry.PendingResult{
			Status:    rm.Status,
			HeaderMap: rm.Headers,
			Body:      body,
			Err:       nil,
		})
	}
}
