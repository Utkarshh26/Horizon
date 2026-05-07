// Package memlog holds recent request logs and per-minute volume for the dashboard without Postgres.
package memlog

import (
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/horizon/server/internal/db"
)

const defaultMaxEntries = 200

// Log is a thread-safe ring of recent requests plus rolling minute buckets.
type Log struct {
	mu    sync.RWMutex
	byID  map[uuid.UUID]db.RequestLogRow
	order []uuid.UUID // newest first

	volMu   sync.Mutex
	volumes map[int64]int64 // UTC minute unix -> count

	maxEntries int
}

// New creates an empty log.
func New() *Log {
	return &Log{
		byID:       make(map[uuid.UUID]db.RequestLogRow),
		volumes:    make(map[int64]int64),
		maxEntries: defaultMaxEntries,
	}
}

// Add records one proxied request (newest-first, evicts oldest when full).
func (l *Log) Add(row db.RequestLogRow) {
	l.mu.Lock()
	if len(l.order) >= l.maxEntries {
		last := l.order[len(l.order)-1]
		l.order = l.order[:len(l.order)-1]
		delete(l.byID, last)
	}
	l.byID[row.ID] = row
	l.order = append([]uuid.UUID{row.ID}, l.order...)
	l.mu.Unlock()

	l.volMu.Lock()
	k := row.RequestAt.UTC().Truncate(time.Minute).Unix()
	l.volumes[k]++
	l.pruneVolumeLocked(31 * time.Minute)
	l.volMu.Unlock()
}

func (l *Log) pruneVolumeLocked(olderThan time.Duration) {
	cutoff := time.Now().UTC().Add(-olderThan).Truncate(time.Minute).Unix()
	for key := range l.volumes {
		if key < cutoff {
			delete(l.volumes, key)
		}
	}
}

// List returns up to limit rows, newest first.
func (l *Log) List(limit int) []db.RequestLogRow {
	if limit <= 0 {
		limit = 200
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	n := len(l.order)
	if n > limit {
		n = limit
	}
	out := make([]db.RequestLogRow, 0, n)
	for i := 0; i < n; i++ {
		id := l.order[i]
		if r, ok := l.byID[id]; ok {
			out = append(out, r)
		}
	}
	return out
}

// Get returns one row by id.
func (l *Log) Get(id uuid.UUID) (db.RequestLogRow, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	r, ok := l.byID[id]
	return r, ok
}

// VolumeLastMinutes returns one point per minute for the last `minutes` minutes (UTC).
func (l *Log) VolumeLastMinutes(minutes int) []db.RequestVolumePoint {
	if minutes <= 0 {
		minutes = 30
	}
	l.volMu.Lock()
	l.pruneVolumeLocked(time.Duration(minutes+1) * time.Minute)
	now := time.Now().UTC().Truncate(time.Minute)
	start := now.Add(-time.Duration(minutes-1) * time.Minute)
	out := make([]db.RequestVolumePoint, 0, minutes)
	for t := start; !t.After(now); t = t.Add(time.Minute) {
		out = append(out, db.RequestVolumePoint{
			Minute: t,
			Count:  l.volumes[t.Unix()],
		})
	}
	l.volMu.Unlock()
	return out
}

// VolumeLast30Min is a convenience wrapper for the dashboard chart.
func (l *Log) VolumeLast30Min() []db.RequestVolumePoint {
	return l.VolumeLastMinutes(30)
}

