// Package router chooses a backend for each request using prefix affinity,
// consistent hashing, least-loaded fallback, and per-worker circuit breakers.
package router

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/example/distributed-llm-gateway/gen"
	"github.com/example/distributed-llm-gateway/internal/breaker"
	"github.com/example/distributed-llm-gateway/internal/cache"
	"github.com/example/distributed-llm-gateway/internal/config"
	"github.com/example/distributed-llm-gateway/internal/observability"
	"github.com/example/distributed-llm-gateway/internal/ring"
	"github.com/example/distributed-llm-gateway/internal/worker"
)

type Selection struct {
	Client    *worker.Client
	Reason    string
	Model     string
	PrefixHit bool
}

type Router struct {
	cfg     config.GatewayConfig
	index   *cache.PrefixIndex
	metrics *observability.Metrics
	ring    *ring.Ring

	mu      sync.RWMutex
	clients map[string]*worker.Client
	rr      atomic.Uint64
}

func New(cfg config.GatewayConfig, index *cache.PrefixIndex, metrics *observability.Metrics) *Router {
	return &Router{
		cfg: cfg, index: index, metrics: metrics,
		ring: ring.New(128), clients: map[string]*worker.Client{},
	}
}

// Start dials configured workers and runs the health/ring maintenance loop.
func (r *Router) Start(ctx context.Context) {
	for i, spec := range r.cfg.WorkerAddrs {
		id := fmt.Sprintf("worker-%d", i+1)
		model, addr := parseWorkerAddr(spec)
		b := breaker.New(r.cfg.BreakerFailureThreshold, r.cfg.BreakerWindow, r.cfg.BreakerCooldown)
		c, err := worker.Dial(ctx, id, addr, model, b)
		if err != nil {
			log.Printf("router: dial %s failed: %v", addr, err)
			continue
		}
		r.mu.Lock()
		r.clients[id] = c
		r.mu.Unlock()
	}

	ticker := time.NewTicker(r.cfg.HealthPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			r.healthPass(ctx)
		case <-ctx.Done():
			return
		}
	}
}

// parseWorkerAddr accepts "host:port" or "tier=host:port" specs.
func parseWorkerAddr(spec string) (model, addr string) {
	if k, v, ok := strings.Cut(spec, "="); ok {
		return k, v
	}
	return "workhorse", spec
}

func (r *Router) healthPass(ctx context.Context) {
	r.mu.RLock()
	clients := make([]*worker.Client, 0, len(r.clients))
	for _, c := range r.clients {
		clients = append(clients, c)
	}
	r.mu.RUnlock()

	healthyIDs := make([]string, 0, len(clients))
	for _, c := range clients {
		if _, err := c.RefreshStats(ctx); err != nil {
			c.Breaker.Failure()
			r.metrics.ShedTotal.WithLabelValues("worker-unreachable").Inc()
		} else {
			c.Breaker.HealthOK()
			if c.Healthy() {
				healthyIDs = append(healthyIDs, c.ID)
			}
		}
		state := map[string]float64{"closed": 0, "half-open": 0.5, "open": 1}[c.Breaker.State()]
		r.metrics.CircuitState.WithLabelValues(c.ID).Set(state)
		if stats, _ := c.Snapshot(); stats != nil && stats.WorkerId != "" {
			r.metrics.WorkerInFlight.WithLabelValues(c.ID).Set(float64(stats.InFlight))
		}
	}
	sort.Strings(healthyIDs)
	r.ring.Set(healthyIDs)
}

func (r *Router) HealthyWorkers() []*worker.Client {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*worker.Client, 0, len(r.clients))
	for _, c := range r.clients {
		if c.Healthy() {
			out = append(out, c)
		}
	}
	return out
}

// Workers returns all configured clients, including unhealthy ones, for admin views.
func (r *Router) Workers() []*worker.Client {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*worker.Client, 0, len(r.clients))
	for _, c := range r.clients {
		out = append(out, c)
	}
	return out
}

// Select picks a backend. Prefix affinity first, then consistent hashing, then
// least-loaded; overloaded preferred workers degrade to any healthy backend.
func (r *Router) Select(ctx context.Context, req *gen.GenerateRequest, exclude ...string) (*Selection, error) {
	all := r.Workers()
	model := req.Model
	eligible := filter(all, model, exclude)
	tierFallback := false
	if len(eligible) == 0 && model != "workhorse" {
		eligible = filter(all, "workhorse", exclude)
		tierFallback = len(eligible) > 0
	}
	if len(eligible) == 0 {
		return nil, fmt.Errorf("no healthy worker for model %q", req.Model)
	}

	if req.RoutingPolicy == "round-robin" {
		c := eligible[int(r.rr.Add(1)-1)%len(eligible)]
		r.metrics.RouteDecisions.WithLabelValues("round-robin").Inc()
		return &Selection{Client: c, Reason: "round-robin", Model: c.Model}, nil
	}

	key := ""
	if n := len(req.PrefixHashes); n > 0 {
		key = req.PrefixHashes[n-1]
	}

	// Longest-prefix affinity: check from the most specific block backwards.
	for i := len(req.PrefixHashes) - 1; i >= 0; i-- {
		owner, ok := r.index.Get(ctx, req.PrefixHashes[i])
		if !ok {
			continue
		}
		if c := find(eligible, owner); c != nil {
			reason := "cache"
			if i < len(req.PrefixHashes)-1 {
				reason = "cache-partial"
			}
			r.recordAffinity(req.Model, true, reason)
			r.remember(ctx, req.PrefixHashes, c.ID)
			return &Selection{Client: c, Reason: reason, Model: c.Model, PrefixHit: true}, nil
		}
	}

	if key != "" {
		if owner := r.ring.Lookup(key); owner != "" {
			if c := find(eligible, owner); c != nil && load(c) < 0.95 {
				r.recordAffinity(req.Model, false, "ring")
				r.remember(ctx, req.PrefixHashes, c.ID)
				return &Selection{Client: c, Reason: "ring", Model: c.Model}, nil
			}
		}
	}

	least := eligible[0]
	for _, c := range eligible[1:] {
		if load(c) < load(least) {
			least = c
		}
	}
	reason := "least-loaded"
	if tierFallback {
		reason = "tier-fallback"
	}
	r.recordAffinity(req.Model, false, reason)
	r.remember(ctx, req.PrefixHashes, least.ID)
	return &Selection{Client: least, Reason: reason, Model: least.Model}, nil
}

func (r *Router) recordAffinity(model string, hit bool, reason string) {
	if hit {
		r.metrics.CacheHits.WithLabelValues(model).Inc()
	} else {
		r.metrics.CacheMisses.WithLabelValues(model).Inc()
	}
	r.metrics.RouteDecisions.WithLabelValues(reason).Inc()
}

func (r *Router) remember(ctx context.Context, hashes []string, id string) {
	for _, h := range hashes {
		r.index.Set(ctx, h, id)
	}
}

func filter(clients []*worker.Client, model string, exclude []string) []*worker.Client {
	skip := map[string]bool{}
	for _, id := range exclude {
		skip[id] = true
	}
	out := []*worker.Client{}
	for _, c := range clients {
		if c.Model == model && c.Healthy() && !skip[c.ID] {
			out = append(out, c)
		}
	}
	return out
}

func find(clients []*worker.Client, id string) *worker.Client {
	for _, c := range clients {
		if c.ID == id {
			return c
		}
	}
	return nil
}

func load(c *worker.Client) float64 {
	s, _ := c.Snapshot()
	if s == nil || s.Capacity <= 0 {
		return 1
	}
	return float64(s.InFlight) / float64(s.Capacity)
}
