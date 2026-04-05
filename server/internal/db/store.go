// Package db persists tunnels and request logs to PostgreSQL.
package db

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Store wraps a connection pool and runs migrations on open.
type Store struct {
	pool *pgxpool.Pool
}

// Open connects to Postgres and applies schema. Returns nil store if databaseURL is empty.
func Open(ctx context.Context, databaseURL string) (*Store, error) {
	if databaseURL == "" {
		return nil, nil
	}
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	s := &Store{pool: pool}
	if err := s.migrate(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) migrate(ctx context.Context) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS tunnels (
			id UUID PRIMARY KEY,
			subdomain VARCHAR(64) NOT NULL,
			local_port INT NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			closed_at TIMESTAMPTZ
		)`,
		`CREATE INDEX IF NOT EXISTS idx_tunnels_created_at ON tunnels(created_at DESC)`,
		`CREATE TABLE IF NOT EXISTS requests (
			id UUID PRIMARY KEY,
			tunnel_id UUID NOT NULL REFERENCES tunnels(id) ON DELETE CASCADE,
			method VARCHAR(16) NOT NULL,
			path TEXT NOT NULL,
			status_code INT,
			latency_ms INT,
			request_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			request_headers JSONB,
			request_body TEXT,
			response_headers JSONB,
			response_body TEXT
		)`,
		`CREATE INDEX IF NOT EXISTS idx_requests_request_at ON requests(request_at DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_requests_tunnel_id ON requests(tunnel_id)`,
	}
	for _, q := range stmts {
		if _, err := s.pool.Exec(ctx, q); err != nil {
			return err
		}
	}
	return nil
}

// Close releases the pool.
func (s *Store) Close() {
	if s != nil && s.pool != nil {
		s.pool.Close()
	}
}

// Ping reports whether the database accepts a connection.
func (s *Store) Ping(ctx context.Context) bool {
	if s == nil || s.pool == nil {
		return false
	}
	return s.pool.Ping(ctx) == nil
}

// InsertTunnel records a new tunnel when the CLI connects.
func (s *Store) InsertTunnel(ctx context.Context, id uuid.UUID, subdomain string, localPort int) error {
	if s == nil || s.pool == nil {
		return nil
	}
	_, err := s.pool.Exec(ctx,
		`INSERT INTO tunnels (id, subdomain, local_port) VALUES ($1, $2, $3)`,
		id, subdomain, localPort)
	return err
}

// CloseTunnel sets closed_at for a tunnel row.
func (s *Store) CloseTunnel(ctx context.Context, id uuid.UUID) error {
	if s == nil || s.pool == nil {
		return nil
	}
	_, err := s.pool.Exec(ctx,
		`UPDATE tunnels SET closed_at = now() WHERE id = $1 AND closed_at IS NULL`, id)
	return err
}

// RequestRecord is one logged HTTP exchange through a tunnel.
type RequestRecord struct {
	ID              uuid.UUID
	TunnelID        uuid.UUID
	Method          string
	Path            string
	StatusCode      int
	LatencyMs       int
	RequestHeaders  json.RawMessage
	RequestBody     string
	ResponseHeaders json.RawMessage
	ResponseBody    string
}

// InsertRequest appends a row to requests.
func (s *Store) InsertRequest(ctx context.Context, r RequestRecord) error {
	if s == nil || s.pool == nil {
		return nil
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO requests (
			id, tunnel_id, method, path, status_code, latency_ms,
			request_headers, request_body, response_headers, response_body
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
		r.ID, r.TunnelID, r.Method, r.Path, r.StatusCode, r.LatencyMs,
		r.RequestHeaders, r.RequestBody, r.ResponseHeaders, r.ResponseBody)
	return err
}

// TunnelRow is a tunnel row for dashboard listing.
type TunnelRow struct {
	ID         uuid.UUID  `json:"id"`
	Subdomain  string     `json:"subdomain"`
	LocalPort  int        `json:"local_port"`
	CreatedAt  time.Time  `json:"created_at"`
	ClosedAt   *time.Time `json:"closed_at,omitempty"`
}

// ListRecentTunnels returns the latest tunnels (including closed).
func (s *Store) ListRecentTunnels(ctx context.Context, limit int) ([]TunnelRow, error) {
	if s == nil || s.pool == nil {
		return []TunnelRow{}, nil
	}
	if limit <= 0 {
		limit = 50
	}
	rows, err := s.pool.Query(ctx, `
		SELECT id, subdomain, local_port, created_at, closed_at
		FROM tunnels
		ORDER BY created_at DESC
		LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []TunnelRow
	for rows.Next() {
		var t TunnelRow
		if err := rows.Scan(&t.ID, &t.Subdomain, &t.LocalPort, &t.CreatedAt, &t.ClosedAt); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// RequestLogRow is a request log entry with optional bodies for the inspector.
type RequestLogRow struct {
	ID              uuid.UUID       `json:"id"`
	TunnelID        uuid.UUID       `json:"tunnel_id"`
	Method          string          `json:"method"`
	Path            string          `json:"path"`
	StatusCode      *int            `json:"status_code,omitempty"`
	LatencyMs       *int            `json:"latency_ms,omitempty"`
	RequestAt       time.Time       `json:"request_at"`
	RequestHeaders  json.RawMessage `json:"request_headers,omitempty"`
	RequestBody     *string         `json:"request_body,omitempty"`
	ResponseHeaders json.RawMessage `json:"response_headers,omitempty"`
	ResponseBody    *string         `json:"response_body,omitempty"`
}

// ListRecentRequests returns latest request logs.
func (s *Store) ListRecentRequests(ctx context.Context, limit int) ([]RequestLogRow, error) {
	if s == nil || s.pool == nil {
		return []RequestLogRow{}, nil
	}
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.pool.Query(ctx, `
		SELECT id, tunnel_id, method, path, status_code, latency_ms, request_at,
		       request_headers, request_body, response_headers, response_body
		FROM requests
		ORDER BY request_at DESC
		LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []RequestLogRow
	for rows.Next() {
		var r RequestLogRow
		if err := rows.Scan(
			&r.ID, &r.TunnelID, &r.Method, &r.Path, &r.StatusCode, &r.LatencyMs, &r.RequestAt,
			&r.RequestHeaders, &r.RequestBody, &r.ResponseHeaders, &r.ResponseBody,
		); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// RequestVolumePoint is one minute bucket for charts.
type RequestVolumePoint struct {
	Minute time.Time `json:"minute"`
	Count  int64     `json:"count"`
}

// MarshalJSON encodes minute in RFC3339 for dashboard clients.
func (p RequestVolumePoint) MarshalJSON() ([]byte, error) {
	type alias struct {
		Minute string `json:"minute"`
		Count  int64  `json:"count"`
	}
	return json.Marshal(alias{
		Minute: p.Minute.UTC().Format(time.RFC3339),
		Count:  p.Count,
	})
}

// RequestVolumeLastMinutes returns request counts per minute for the last N minutes.
func (s *Store) RequestVolumeLastMinutes(ctx context.Context, minutes int) ([]RequestVolumePoint, error) {
	if s == nil || s.pool == nil {
		return emptyVolume(minutes), nil
	}
	if minutes <= 0 {
		minutes = 30
	}
	since := time.Now().UTC().Add(-time.Duration(minutes) * time.Minute)
	rows, err := s.pool.Query(ctx, `
		SELECT date_trunc('minute', request_at) AS m, COUNT(*)::bigint
		FROM requests
		WHERE request_at >= $1
		GROUP BY 1
		ORDER BY 1 ASC`, since)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []RequestVolumePoint
	for rows.Next() {
		var p RequestVolumePoint
		if err := rows.Scan(&p.Minute, &p.Count); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return fillVolumeGaps(out, minutes), nil
}

// EmptyVolumePoints returns zero-filled minute buckets (e.g. when Postgres is disabled).
func EmptyVolumePoints(minutes int) []RequestVolumePoint {
	return emptyVolume(minutes)
}

func emptyVolume(minutes int) []RequestVolumePoint {
	if minutes <= 0 {
		minutes = 30
	}
	now := time.Now().UTC().Truncate(time.Minute)
	out := make([]RequestVolumePoint, 0, minutes)
	for i := minutes - 1; i >= 0; i-- {
		out = append(out, RequestVolumePoint{Minute: now.Add(-time.Duration(i) * time.Minute), Count: 0})
	}
	return out
}

func fillVolumeGaps(points []RequestVolumePoint, minutes int) []RequestVolumePoint {
	now := time.Now().UTC().Truncate(time.Minute)
	start := now.Add(-time.Duration(minutes-1) * time.Minute)
	byMin := make(map[int64]int64)
	for _, p := range points {
		t := p.Minute.UTC().Truncate(time.Minute)
		byMin[t.Unix()] = p.Count
	}
	out := make([]RequestVolumePoint, 0, minutes)
	for t := start; !t.After(now); t = t.Add(time.Minute) {
		out = append(out, RequestVolumePoint{Minute: t, Count: byMin[t.Unix()]})
	}
	return out
}

// GetRequest returns one log row for the inspector.
func (s *Store) GetRequest(ctx context.Context, id uuid.UUID) (RequestLogRow, error) {
	var r RequestLogRow
	if s == nil || s.pool == nil {
		return r, errors.New("no database")
	}
	err := s.pool.QueryRow(ctx, `
		SELECT id, tunnel_id, method, path, status_code, latency_ms, request_at,
		       request_headers, request_body, response_headers, response_body
		FROM requests WHERE id = $1`, id,
	).Scan(
		&r.ID, &r.TunnelID, &r.Method, &r.Path, &r.StatusCode, &r.LatencyMs, &r.RequestAt,
		&r.RequestHeaders, &r.RequestBody, &r.ResponseHeaders, &r.ResponseBody,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return r, err
	}
	return r, err
}
