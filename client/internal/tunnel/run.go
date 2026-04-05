// Package tunnel implements the Horizon CLI WebSocket client and localhost forwarding.
package tunnel

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	typeTunnelReady = "tunnel_ready"
	typeRequest     = "request"
	typeResponse    = "response"

	// Client pings the server more often than the server's read idle timeout so the tunnel stays up.
	pingInterval     = 27 * time.Second
	pingWriteTimeout = 10 * time.Second
	readIdleTimeout  = 90 * time.Second
)

type tunnelReady struct {
	Type      string `json:"type"`
	Subdomain string `json:"subdomain"`
	PublicURL string `json:"public_url"`
}

type requestMsg struct {
	Type    string            `json:"type"`
	ID      string            `json:"id"`
	Method  string            `json:"method"`
	Path    string            `json:"path"`
	Headers map[string]string `json:"headers"`
	Body    string            `json:"body"`
}

type responseMsg struct {
	Type    string            `json:"type"`
	ID      string            `json:"id"`
	Status  int               `json:"status"`
	Headers map[string]string `json:"headers"`
	Body    string            `json:"body"`
}

// Run dials the server, registers this tunnel, and processes request/response until the connection ends.
func Run(ctx context.Context, wsBase string, localPort int) error {
	u, err := url.Parse(wsBase)
	if err != nil {
		return err
	}
	q := u.Query()
	q.Set("port", strconv.Itoa(localPort))
	u.RawQuery = q.Encode()

	dialer := websocket.Dialer{HandshakeTimeout: 15 * time.Second}
	conn, _, err := dialer.DialContext(ctx, u.String(), nil)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	var writeMu sync.Mutex
	installClientKeepalive(conn, &writeMu)

	if err := readTunnelReady(conn); err != nil {
		return err
	}

	pingStop := make(chan struct{})
	defer close(pingStop)
	go runPingLoop(ctx, conn, &writeMu, pingStop)

	httpClient := &http.Client{Timeout: 60 * time.Second}

	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			return fmt.Errorf("read: %w", err)
		}
		var env struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(data, &env); err != nil {
			continue
		}
		if env.Type != typeRequest {
			continue
		}
		var req requestMsg
		if err := json.Unmarshal(data, &req); err != nil {
			continue
		}
		resp := forwardLocal(httpClient, localPort, req)
		writeMu.Lock()
		err = conn.WriteJSON(resp)
		writeMu.Unlock()
		if err != nil {
			return fmt.Errorf("write response: %w", err)
		}
	}
}

// installClientKeepalive refreshes read deadlines on pong and echoes pong for server pings (mutex-safe).
func installClientKeepalive(conn *websocket.Conn, writeMu *sync.Mutex) {
	refresh := func() error {
		return conn.SetReadDeadline(time.Now().Add(readIdleTimeout))
	}
	_ = refresh()

	conn.SetPongHandler(func(string) error {
		return refresh()
	})

	conn.SetPingHandler(func(appData string) error {
		if err := refresh(); err != nil {
			return err
		}
		writeMu.Lock()
		defer writeMu.Unlock()
		return conn.WriteControl(websocket.PongMessage, []byte(appData), time.Now().Add(pingWriteTimeout))
	})
}

// runPingLoop sends WebSocket ping frames periodically; failure closes the connection so outer reconnect runs.
func runPingLoop(ctx context.Context, conn *websocket.Conn, writeMu *sync.Mutex, stop <-chan struct{}) {
	send := func() bool {
		writeMu.Lock()
		err := conn.WriteControl(websocket.PingMessage, []byte("horizon"), time.Now().Add(pingWriteTimeout))
		writeMu.Unlock()
		if err != nil {
			_ = conn.Close()
			return false
		}
		return true
	}
	if !send() {
		return
	}

	t := time.NewTicker(pingInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-stop:
			return
		case <-t.C:
			if !send() {
				return
			}
		}
	}
}

func readTunnelReady(conn *websocket.Conn) error {
	_, data, err := conn.ReadMessage()
	if err != nil {
		return fmt.Errorf("read tunnel_ready: %w", err)
	}
	var tr tunnelReady
	if err := json.Unmarshal(data, &tr); err != nil {
		return fmt.Errorf("tunnel_ready json: %w", err)
	}
	if tr.Type != typeTunnelReady {
		return fmt.Errorf("expected tunnel_ready, got %q", tr.Type)
	}
	fmt.Printf("Horizon tunnel ready\n  Public URL: %s\n", tr.PublicURL)
	return nil
}

func forwardLocal(client *http.Client, port int, req requestMsg) responseMsg {
	target := fmt.Sprintf("http://127.0.0.1:%d", port)
	u, err := url.Parse(target + req.Path)
	if err != nil {
		return errorResponse(req.ID, http.StatusBadGateway, err.Error())
	}

	var body io.Reader
	if req.Body != "" {
		raw, err := base64.StdEncoding.DecodeString(req.Body)
		if err != nil {
			return errorResponse(req.ID, http.StatusBadRequest, "invalid body encoding")
		}
		body = bytes.NewReader(raw)
	}

	hreq, err := http.NewRequest(req.Method, u.String(), body)
	if err != nil {
		return errorResponse(req.ID, http.StatusBadGateway, err.Error())
	}
	for k, v := range req.Headers {
		if strings.EqualFold(k, "Host") {
			continue
		}
		hreq.Header.Set(k, v)
	}

	res, err := client.Do(hreq)
	if err != nil {
		return errorResponse(req.ID, http.StatusBadGateway, err.Error())
	}
	defer res.Body.Close()
	b, err := io.ReadAll(io.LimitReader(res.Body, 8<<20))
	if err != nil {
		return errorResponse(req.ID, http.StatusBadGateway, err.Error())
	}

	flat := make(map[string]string)
	for k, vv := range res.Header {
		flat[k] = strings.Join(vv, ", ")
	}

	return responseMsg{
		Type:    typeResponse,
		ID:      req.ID,
		Status:  res.StatusCode,
		Headers: flat,
		Body:    base64.StdEncoding.EncodeToString(b),
	}
}

func errorResponse(id string, code int, msg string) responseMsg {
	h := map[string]string{"Content-Type": "text/plain; charset=utf-8"}
	return responseMsg{
		Type:    typeResponse,
		ID:      id,
		Status:  code,
		Headers: h,
		Body:    base64.StdEncoding.EncodeToString([]byte(msg)),
	}
}
