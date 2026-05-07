package proxy

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/horizon/server/internal/config"
	"github.com/horizon/server/internal/db"
	"github.com/horizon/server/internal/events"
	"github.com/horizon/server/internal/memlog"
	"github.com/horizon/server/internal/protocol"
	"github.com/horizon/server/internal/registry"
)

// Handler forwards public HTTP to the tunnel client over WebSocket and waits on a Go channel for the response.
type Handler struct {
	Reg    *registry.Registry
	Cfg    config.Config
	Store  *db.Store
	Mem    *memlog.Log
	Events *events.Hub
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	sub, ok := SubdomainFromHost(r.Host, h.Cfg.BaseDomain)
	if !ok {
		http.NotFound(w, r)
		return
	}

	client := h.Reg.Get(sub)
	if client == nil {
		http.Error(w, "tunnel not connected", http.StatusBadGateway)
		return
	}

	reqID := uuid.NewString()
	ctx, cancel := context.WithTimeout(r.Context(), h.Cfg.ProxyTimeout)
	defer cancel()

	body, err := io.ReadAll(io.LimitReader(r.Body, h.Cfg.MaxBodyBytes))
	if err != nil {
		http.Error(w, "read body", http.StatusBadRequest)
		return
	}
	_ = r.Body.Close()

	hdr := flattenHeaders(r.Header)
	msg := protocol.RequestMessage{
		Type:    protocol.TypeRequest,
		ID:      reqID,
		Method:  r.Method,
		Path:    requestPath(r),
		Headers: hdr,
		Body:    base64.StdEncoding.EncodeToString(body),
	}

	ch := client.RegisterPending(reqID)
	defer client.UnregisterPending(reqID)

	start := time.Now()
	if err := client.WriteJSON(msg); err != nil {
		log.Printf("proxy: send to tunnel %s: %v", sub, err)
		http.Error(w, "tunnel write failed", http.StatusBadGateway)
		return
	}

	var res *registry.PendingResult
	select {
	case <-ctx.Done():
		http.Error(w, "tunnel timeout", http.StatusGatewayTimeout)
		return
	case res = <-ch:
	}

	latency := time.Since(start)
	if res == nil || res.Err != nil {
		msg := "tunnel disconnected"
		if res != nil && res.Err != nil {
			msg = res.Err.Error()
		}
		http.Error(w, msg, http.StatusBadGateway)
		return
	}

	client.RequestCount.Add(1)

	reqLogHeaders := headersForLog(r, hdr)
	go h.logAndBroadcast(sub, client, reqID, msg.Method, msg.Path, res, latency, reqLogHeaders, string(body))

	copyHeaders(w.Header(), res.HeaderMap)
	w.WriteHeader(res.Status)
	if len(res.Body) > 0 {
		_, _ = w.Write(res.Body)
	}
}

func requestPath(r *http.Request) string {
	if r.URL.RawQuery != "" {
		return r.URL.Path + "?" + r.URL.RawQuery
	}
	return r.URL.Path
}

func flattenHeaders(h http.Header) map[string]string {
	out := make(map[string]string, len(h))
	for k, vv := range h {
		if strings.EqualFold(k, "Host") {
			continue
		}
		out[k] = strings.Join(vv, ", ")
	}
	return out
}

func headersForLog(r *http.Request, flattened map[string]string) map[string]string {
	cp := make(map[string]string, len(flattened)+1)
	for k, v := range flattened {
		cp[k] = v
	}
	if r.Host != "" {
		cp["Host"] = r.Host
	}
	return cp
}

func copyHeaders(dst http.Header, src map[string]string) {
	if src == nil {
		return
	}
	for k, v := range src {
		dst.Add(k, v)
	}
}

func (h *Handler) logAndBroadcast(
	sub string,
	client *registry.ClientConnection,
	reqID string,
	method string,
	path string,
	res *registry.PendingResult,
	latency time.Duration,
	reqHeaders map[string]string,
	reqBodyPlain string,
) {
	now := time.Now().UTC()
	latencyMs := int(latency.Milliseconds())

	// Always populate the in-memory dashboard log if enabled (works without Postgres).
	if h.Mem != nil {
		sc, lm := res.Status, latencyMs
		reqHeadersJSON, _ := json.Marshal(reqHeaders)
		respHeadersJSON, _ := json.Marshal(res.HeaderMap)
		reqBody := truncate(reqBodyPlain, 65536)
		respBody := truncate(string(res.Body), 65536)
		h.Mem.Add(db.RequestLogRow{
			ID:              uuid.MustParse(reqID),
			TunnelID:        client.ID,
			Method:          method,
			Path:            path,
			StatusCode:      &sc,
			LatencyMs:       &lm,
			RequestAt:       now,
			RequestHeaders:  reqHeadersJSON,
			RequestBody:     stringPtrOrNil(reqBody),
			ResponseHeaders: respHeadersJSON,
			ResponseBody:    stringPtrOrNil(respBody),
		})
	}

	if h.Store != nil {
		reqHeadersJSON, _ := json.Marshal(reqHeaders)
		respHeadersJSON, _ := json.Marshal(res.HeaderMap)
		reqBody := truncate(reqBodyPlain, 65536)
		respBody := truncate(string(res.Body), 65536)
		rec := db.RequestRecord{
			ID:              uuid.MustParse(reqID),
			TunnelID:        client.ID,
			Method:          method,
			Path:            path,
			StatusCode:      res.Status,
			LatencyMs:       latencyMs,
			RequestHeaders:  reqHeadersJSON,
			RequestBody:     reqBody,
			ResponseHeaders: respHeadersJSON,
			ResponseBody:    respBody,
		}
		if err := h.Store.InsertRequest(context.Background(), rec); err != nil {
			log.Printf("db: insert request: %v", err)
		}
	}

	if h.Events != nil {
		h.publishRequestEvent(sub, client, reqID, method, path, res.Status, latencyMs, now)
	}

}

func (h *Handler) publishRequestEvent(sub string, client *registry.ClientConnection, reqID, method, path string, status, latencyMs int, at time.Time) {
	h.Events.Publish(events.Event{
		Type: events.TypeRequest,
		Payload: map[string]interface{}{
			"id":          reqID,
			"tunnel_id":   client.ID.String(),
			"subdomain":   sub,
			"method":      method,
			"path":        path,
			"status_code": status,
			"latency_ms":  latencyMs,
			"request_at":  at.UTC().Format(time.RFC3339Nano),
		},
	})
}

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + fmt.Sprintf("…(truncated,%d bytes)", len(s))
}

func stringPtrOrNil(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
