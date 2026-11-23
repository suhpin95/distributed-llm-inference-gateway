// Package ratelimit implements per-tenant token buckets. Redis is shared
// across gateway replicas; a local map is used when Redis is absent.
package ratelimit

import (
	"context"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

const bucketScript = `
local rate = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local data = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(data[1])
local ts = tonumber(data[2])
if tokens == nil then tokens = burst end
if ts == nil then ts = now end
tokens = math.min(burst, tokens + (now - ts) * rate)
local allowed = 0
if tokens >= 1 then
  tokens = tokens - 1
  allowed = 1
end
redis.call('HSET', KEYS[1], 'tokens', tokens, 'ts', now)
redis.call('PEXPIRE', KEYS[1], 300000)
return allowed
`

type bucket struct {
	tokens float64
	last   time.Time
}

// Limiter enforces one shared rate/burst for all tenants by default; callers
// key it by tenant ID.
type Limiter struct {
	redis  *redis.Client
	rate   float64
	burst  float64
	script *redis.Script

	mu      sync.Mutex
	buckets map[string]*bucket
}

func New(client *redis.Client, rate, burst float64) *Limiter {
	if rate <= 0 {
		rate = 10
	}
	if burst <= 0 {
		burst = rate * 2
	}
	return &Limiter{
		redis:   client,
		rate:    rate,
		burst:   burst,
		script:  redis.NewScript(bucketScript),
		buckets: map[string]*bucket{},
	}
}

// Allow consumes one token for the given key (tenant ID).
func (l *Limiter) Allow(ctx context.Context, key string) bool {
	if l.redis != nil {
		now := float64(time.Now().UnixMilli()) / 1000
		v, err := l.script.Run(ctx, l.redis, []string{"rl:" + key}, l.rate, l.burst, now).Int()
		if err == nil {
			return v == 1
		}
		// Redis failure: degrade to local enforcement rather than fail open.
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	b, ok := l.buckets[key]
	if !ok {
		b = &bucket{tokens: l.burst, last: now}
		l.buckets[key] = b
	}
	b.tokens = min(l.burst, b.tokens+now.Sub(b.last).Seconds()*l.rate)
	b.last = now
	if b.tokens >= 1 {
		b.tokens--
		return true
	}
	return false
}
