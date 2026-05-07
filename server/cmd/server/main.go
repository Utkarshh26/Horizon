// Command server is the Horizon cloud tunnel: WebSocket registry, HTTP proxy, API, and optional Postgres logging.
package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gorilla/mux"

	"github.com/horizon/server/internal/api"
	"github.com/horizon/server/internal/config"
	"github.com/horizon/server/internal/db"
	"github.com/horizon/server/internal/events"
	"github.com/horizon/server/internal/memlog"
	"github.com/horizon/server/internal/proxy"
	"github.com/horizon/server/internal/registry"
	"github.com/horizon/server/internal/tunnel"
)

func main() {
	cfg := config.Load()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	store, err := db.Open(ctx, cfg.DatabaseURL)
	cancel()
	if err != nil {
		log.Fatalf("database: %v", err)
	}
	defer store.Close()

	reg := registry.New()
	hub := events.NewHub()
	mem := memlog.New()

	ph := &proxy.Handler{Reg: reg, Cfg: cfg, Store: store, Mem: mem, Events: hub}

	r := mux.NewRouter()
	r.HandleFunc("/health", healthHandler(reg, store)).Methods(http.MethodGet)
	r.PathPrefix("/api").Handler(api.Router(reg, store, mem, hub, cfg))
	r.HandleFunc("/tunnel/connect", func(w http.ResponseWriter, req *http.Request) {
		tunnel.HandleConnect(reg, store, hub, cfg, w, req)
	}).Methods(http.MethodGet)
	r.PathPrefix("/").Handler(ph)

	srv := &http.Server{
		Addr:              cfg.Addr,
		Handler:           r,
		ReadHeaderTimeout: cfg.ReadHeaderTimeout,
	}

	go func() {
		log.Printf("horizon server on %s (base_domain=%s)", cfg.Addr, cfg.BaseDomain)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("server shutdown: %v", err)
	}
}

func healthHandler(reg *registry.Registry, store *db.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		dbOK := false
		if store != nil {
			ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
			dbOK = store.Ping(ctx)
			cancel()
		}
		_, _ = fmt.Fprintf(w, `{"status":"ok","active_tunnels":%d,"database":%t}`,
			reg.Count(), dbOK)
	}
}
