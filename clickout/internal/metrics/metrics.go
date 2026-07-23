package metrics

import (
	"context"
	"sync"
	"time"
)

type Query struct {
	Name          string        `json:"name"`
	ReadRows      uint64        `json:"read_rows"`
	ReadBytes     uint64        `json:"read_bytes"`
	ServerElapsed time.Duration `json:"server_elapsed_ns"`
	WallElapsed   time.Duration `json:"wall_elapsed_ns"`
}

type Collector struct {
	mu      sync.Mutex
	queries []Query
}

type collectorKey struct{}

func WithCollector(ctx context.Context, collector *Collector) context.Context {
	return context.WithValue(ctx, collectorKey{}, collector)
}

func Add(ctx context.Context, query Query) {
	collector, ok := ctx.Value(collectorKey{}).(*Collector)
	if !ok || collector == nil {
		return
	}
	collector.mu.Lock()
	defer collector.mu.Unlock()
	collector.queries = append(collector.queries, query)
}

func (collector *Collector) Snapshot() []Query {
	collector.mu.Lock()
	defer collector.mu.Unlock()
	result := make([]Query, len(collector.queries))
	copy(result, collector.queries)
	return result
}
