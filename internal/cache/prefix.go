// Package cache holds the prefix-hash -> worker index that drives KV-cache
// affinity. Redis is used when configured; an in-memory map keeps the POC
// runnable with zero infrastructure and acts as a fallback on Redis errors.
package cache

import (
	"context"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

type entry struct {
	worker  string
	expires time.Time
}

// PrefixIndex maps a prompt prefix hash to the worker most likely holding its
// KV cache. Entries are advisory: a dead worker is simply treated as a miss.
type PrefixIndex struct {
	redis *redis.Client
	ttl   time.Duration

	mu  sync.Mutex
	mem map[string]entry
}

func NewPrefixIndex(client *redis.Client, ttl time.Duration) *PrefixIndex {
	if ttl <= 0 {
		ttl = 10 * time.Minute
	}
	return &PrefixIndex{redis: client, ttl: ttl, mem: map[string]entry{}}
}

func (p *PrefixIndex) Get(ctx context.Context, key string) (string, bool) {
	if p.redis != nil {
		if v, err := p.redis.Get(ctx, "kv:"+key).Result(); err == nil && v != "" {
			return v, true
		}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	e, ok := p.mem[key]
	if !ok || time.Now().After(e.expires) {
		delete(p.mem, key)
		return "", false
	}
	return e.worker, true
}

func (p *PrefixIndex) Set(ctx context.Context, key, worker string) {
	p.mu.Lock()
	p.mem[key] = entry{worker: worker, expires: time.Now().Add(p.ttl)}
	p.mu.Unlock()
	if p.redis != nil {
		_ = p.redis.Set(ctx, "kv:"+key, worker, p.ttl).Err()
	}
}

func (p *PrefixIndex) Size() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.mem)
}
