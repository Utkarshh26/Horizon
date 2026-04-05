package api

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/gorilla/websocket"
	"github.com/jackc/pgx/v5"

	"github.com/horizon/server/internal/config"
	"github.com/horizon/server/internal/db"
	"github.com/horizon/server/internal/events"
	"github.com/horizon/server/internal/registry"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool { return true },
}

// Router mounts REST + dashboard WebSocket on /api/*.
func Router(reg *registry.Registry, store *db.Store, hub *events.Hub, cfg config.Config) http.Handler {
	r := mux.NewRouter()
	r.Use(corsMiddleware)

	r.HandleFunc("/tunnels/active", func(w http.ResponseWriter, req *http.Request) {
		writeJSON(w, reg.Snapshot())
	}).Methods(http.MethodGet, http.MethodOptions)

	r.HandleFunc("/tunnels/recent", func(w http.ResponseWriter, req *http.Request) {
		if store == nil {
			writeJSON(w, []db.TunnelRow{})
			return
		}
		ctx, cancel := context.WithTimeout(req.Context(), 10*time.Second)
		defer cancel()
		rows, err := store.ListRecentTunnels(ctx, 50)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, rows)
	}).Methods(http.MethodGet, http.MethodOptions)

	r.HandleFunc("/requests", func(w http.ResponseWriter, req *http.Request) {
		if store == nil {
			writeJSON(w, []db.RequestLogRow{})
			return
		}
		ctx, cancel := context.WithTimeout(req.Context(), 10*time.Second)
		defer cancel()
		rows, err := store.ListRecentRequests(ctx, 200)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, rows)
	}).Methods(http.MethodGet, http.MethodOptions)

	r.HandleFunc("/requests/{id}", func(w http.ResponseWriter, req *http.Request) {
		if store == nil {
			http.Error(w, "database not configured", http.StatusServiceUnavailable)
			return
		}
		id, err := uuid.Parse(mux.Vars(req)["id"])
		if err != nil {
			http.Error(w, "bad id", http.StatusBadRequest)
			return
		}
		ctx, cancel := context.WithTimeout(req.Context(), 10*time.Second)
		defer cancel()
		row, err := store.GetRequest(ctx, id)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, row)
	}).Methods(http.MethodGet, http.MethodOptions)

	r.HandleFunc("/stats/volume", func(w http.ResponseWriter, req *http.Request) {
		ctx, cancel := context.WithTimeout(req.Context(), 10*time.Second)
		defer cancel()
		var points []db.RequestVolumePoint
		var err error
		if store != nil {
			points, err = store.RequestVolumeLastMinutes(ctx, 30)
		} else {
			points = db.EmptyVolumePoints(30)
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, points)
	}).Methods(http.MethodGet, http.MethodOptions)

	r.HandleFunc("/config", func(w http.ResponseWriter, req *http.Request) {
		writeJSON(w, map[string]string{
			"base_domain":   cfg.BaseDomain,
			"public_scheme": cfg.PublicScheme,
			"public_port":   cfg.PublicPort,
		})
	}).Methods(http.MethodGet, http.MethodOptions)

	r.HandleFunc("/ws", func(w http.ResponseWriter, req *http.Request) {
		if hub == nil {
			http.Error(w, "events disabled", http.StatusServiceUnavailable)
			return
		}
		c, err := upgrader.Upgrade(w, req, nil)
		if err != nil {
			log.Printf("api: ws upgrade: %v", err)
			return
		}
		hub.Register(c)
		defer hub.Unregister(c)
		// Keep connection open; client does not need to send messages.
		for {
			if _, _, err := c.ReadMessage(); err != nil {
				return
			}
		}
	}).Methods(http.MethodGet)

	return http.StripPrefix("/api", r)
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(v)
}
