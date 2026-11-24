package gateway

import (
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/example/distributed-llm-gateway/gen"
	"github.com/example/distributed-llm-gateway/internal/adaptive"
	"github.com/example/distributed-llm-gateway/internal/cache"
	"github.com/example/distributed-llm-gateway/internal/config"
	"github.com/example/distributed-llm-gateway/internal/observability"
	"github.com/example/distributed-llm-gateway/internal/prefix"
	"github.com/example/distributed-llm-gateway/internal/ratelimit"
	"github.com/example/distributed-llm-gateway/internal/ring"
	"github.com/example/distributed-llm-gateway/internal/router"
	"github.com/example/distributed-llm-gateway/internal/worker"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/grpc"
)

func TestStreamingCacheAffinityAndFailover(t *testing.T) {
	lis1, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	lis2, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	server1 := startWorker(lis1, "w1", 0)
	server2 := startWorker(lis2, "w2", 0)
	defer server1.Stop()

	cfg := config.GatewayConfig{
		WorkerAddrs:             []string{lis1.Addr().String(), lis2.Addr().String()},
		HealthPeriod:            50 * time.Millisecond,
		BlockSize:               32,
		MaxRetries:              3,
		RetryBase:               5 * time.Millisecond,
		QueueWait:               100 * time.Millisecond,
		MaxInFlight:             16,
		QueueSize:               4,
		PrefixTTL:               time.Minute,
		BreakerFailureThreshold: 2,
		BreakerCooldown:         200 * time.Millisecond,
		BreakerWindow:           time.Minute,
		Tenants:                 map[string]string{"dev-key": "default"},
	}
	metrics := observability.New()
	index := cache.NewPrefixIndex(nil, cfg.PrefixTTL)
	rt := router.New(cfg, index, metrics)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go rt.Start(ctx)
	waitForHealthy(t, rt, 2)

	svc := NewService(cfg, rt, ratelimit.New(nil, 1000, 2000),
		adaptive.New(8, 16, 4, 100*time.Millisecond), metrics, nil)

	prompt := "You are a helpful gateway. " + strings.Repeat("Repeat this shared prefix for cache affinity. ", 5)
	_, blocks := prefix.Hashes(prompt, cfg.BlockSize)
	request := func(id string) (string, int) {
		t.Helper()
		var tokens []string
		res, err := svc.Execute(ctx, Request{
			RequestID: id, TenantID: "default", Model: "workhorse",
			Prompt: prompt, PrefixHashes: blocks, MaxTokens: 6,
		}, func(token string) error {
			tokens = append(tokens, token)
			return nil
		})
		if err != nil {
			t.Fatalf("execute %s: %v", id, err)
		}
		if res.Status != "ok" || res.OutputTokens != 6 || len(tokens) != 6 {
			t.Fatalf("unexpected result: %+v tokens=%d", res, len(tokens))
		}
		return res.Route, len(tokens)
	}

	firstRoute, _ := request("req-1")
	secondRoute, _ := request("req-2")
	if !strings.HasPrefix(secondRoute, "cache") {
		t.Fatalf("expected cache affinity on repeated prefix, got %q (first=%q)", secondRoute, firstRoute)
	}

	// Kill one backend mid-test: health checks open its breaker and the ring
	// rebalances, so subsequent requests still succeed on the survivor.
	server2.Stop()
	waitForHealthy(t, rt, 1)
	thirdRoute, _ := request("req-3")
	if thirdRoute == "" {
		t.Fatal("expected a route after failover")
	}
}

func TestRetryBeforeFirstToken(t *testing.T) {
	lis1, _ := net.Listen("tcp", "127.0.0.1:0")
	lis2, _ := net.Listen("tcp", "127.0.0.1:0")
	s1 := startWorker(lis1, "w1", 1) // fails its first Generate
	s2 := startWorker(lis2, "w2", 0)
	defer s1.Stop()
	defer s2.Stop()

	cfg := config.GatewayConfig{
		WorkerAddrs:  []string{lis1.Addr().String(), lis2.Addr().String()},
		HealthPeriod: 50 * time.Millisecond, BlockSize: 32,
		MaxRetries: 3, RetryBase: time.Millisecond, QueueWait: time.Second,
		MaxInFlight: 8, QueueSize: 2, PrefixTTL: time.Minute,
		BreakerFailureThreshold: 5, BreakerCooldown: time.Second, BreakerWindow: time.Minute,
	}
	metrics := observability.New()
	rt := router.New(cfg, cache.NewPrefixIndex(nil, cfg.PrefixTTL), metrics)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go rt.Start(ctx)
	waitForHealthy(t, rt, 2)

	// Pick a prompt whose consistent-hash owner is the failing worker so the
	// first attempt is guaranteed to fail before emitting any token.
	hring := ring.New(128)
	hring.Set([]string{"worker-1", "worker-2"})
	prompt, hashes := "", []string(nil)
	for i := 0; i < 500; i++ {
		candidate := fmt.Sprintf("prompt number %d for retry test", i)
		_, hs := prefix.Hashes(candidate, 32)
		if hring.Lookup(hs[len(hs)-1]) == "worker-1" {
			prompt, hashes = candidate, hs
			break
		}
	}
	if prompt == "" {
		t.Fatal("could not find a prompt owned by the failing worker")
	}

	svc := NewService(cfg, rt, ratelimit.New(nil, 100, 200),
		adaptive.New(4, 8, 2, time.Second), metrics, nil)
	res, err := svc.Execute(ctx, Request{
		RequestID: "retry-1", TenantID: "default", Model: "workhorse",
		Prompt: prompt, PrefixHashes: hashes, MaxTokens: 4,
	}, nil)
	if err != nil {
		t.Fatalf("retry should eventually succeed: %v", err)
	}
	if res.WorkerID != "worker-2" || res.Status != "ok" {
		t.Fatalf("expected success on worker-2 after retry, got %+v", res)
	}
}

func TestRoundRobinOverrideSpreadsAcrossWorkers(t *testing.T) {
	lis1, _ := net.Listen("tcp", "127.0.0.1:0")
	lis2, _ := net.Listen("tcp", "127.0.0.1:0")
	s1 := startWorker(lis1, "w1", 0)
	s2 := startWorker(lis2, "w2", 0)
	defer s1.Stop()
	defer s2.Stop()

	cfg := testGatewayConfig(lis1, lis2)
	metrics := observability.New()
	rt := router.New(cfg, cache.NewPrefixIndex(nil, cfg.PrefixTTL), metrics)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go rt.Start(ctx)
	waitForHealthy(t, rt, 2)

	svc := NewService(cfg, rt, ratelimit.New(nil, 1000, 2000),
		adaptive.New(16, 32, 8, time.Second), metrics, nil)

	seen := map[string]bool{}
	for i := 0; i < 12; i++ {
		res, err := svc.Execute(ctx, Request{
			RequestID: fmt.Sprintf("rr-%d", i), TenantID: "default", Model: "workhorse",
			Prompt: fmt.Sprintf("round robin prompt %d", i), MaxTokens: 2,
			RoutingPolicy: "round-robin",
		}, nil)
		if err != nil {
			t.Fatalf("execute rr-%d: %v", i, err)
		}
		if res.Status != "ok" || res.Route != "round-robin" {
			t.Fatalf("unexpected result for rr-%d: %+v", i, res)
		}
		seen[res.WorkerID] = true
	}
	if len(seen) != 2 {
		t.Fatalf("round-robin should hit both workers, saw %v", seen)
	}
}

func TestTierFallbackToWorkhorse(t *testing.T) {
	lis, _ := net.Listen("tcp", "127.0.0.1:0")
	s := startWorker(lis, "w1", 0)
	defer s.Stop()

	cfg := testGatewayConfig(lis)
	metrics := observability.New()
	rt := router.New(cfg, cache.NewPrefixIndex(nil, cfg.PrefixTTL), metrics)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go rt.Start(ctx)
	waitForHealthy(t, rt, 1)

	svc := NewService(cfg, rt, ratelimit.New(nil, 1000, 2000),
		adaptive.New(8, 16, 4, time.Second), metrics, nil)

	res, err := svc.Execute(ctx, Request{
		RequestID: "tier-1", TenantID: "default", Model: "secondary",
		Prompt: "a request for an unavailable tier", MaxTokens: 2,
	}, nil)
	if err != nil {
		t.Fatalf("tier fallback should succeed: %v", err)
	}
	if res.Status != "ok" || res.Route != "tier-fallback" || res.ServedModel != "workhorse" {
		t.Fatalf("unexpected tier fallback result: %+v", res)
	}
}

// TestFailoverThirtyNineRequestsOneRetry sends 39 requests against three
// workers where worker-1 rejects its first Generate. The first request is
// engineered onto worker-1 so the injected failure is exercised, then retried
// before any token is emitted. Every request must succeed and the gateway must
// report exactly one retry.
func TestFailoverThirtyNineRequestsOneRetry(t *testing.T) {
	lis1, _ := net.Listen("tcp", "127.0.0.1:0")
	lis2, _ := net.Listen("tcp", "127.0.0.1:0")
	lis3, _ := net.Listen("tcp", "127.0.0.1:0")
	s1 := startWorker(lis1, "w1", 1) // fails its first Generate
	s2 := startWorker(lis2, "w2", 0)
	s3 := startWorker(lis3, "w3", 0)
	defer s1.Stop()
	defer s2.Stop()
	defer s3.Stop()

	cfg := testGatewayConfig(lis1, lis2, lis3)
	metrics := observability.New()
	rt := router.New(cfg, cache.NewPrefixIndex(nil, cfg.PrefixTTL), metrics)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go rt.Start(ctx)
	waitForHealthy(t, rt, 3)

	svc := NewService(cfg, rt, ratelimit.New(nil, 5000, 5000),
		adaptive.New(32, 64, 16, time.Second), metrics, nil)

	// Pick a prompt whose consistent-hash owner is worker-1 so the injected
	// first-request failure is guaranteed to be hit before any token streams.
	hring := ring.New(128)
	hring.Set([]string{"worker-1", "worker-2", "worker-3"})
	prompt, hashes := "", []string(nil)
	for i := 0; i < 2000; i++ {
		candidate := fmt.Sprintf("failover prompt %d", i)
		_, hs := prefix.Hashes(candidate, cfg.BlockSize)
		if hring.Lookup(hs[len(hs)-1]) == "worker-1" {
			prompt, hashes = candidate, hs
			break
		}
	}
	if prompt == "" {
		t.Fatal("could not find a prompt owned by the failing worker")
	}

	first, err := svc.Execute(ctx, Request{
		RequestID: "failover-first", TenantID: "default", Model: "workhorse",
		Prompt: prompt, PrefixHashes: hashes, MaxTokens: 2,
	}, nil)
	if err != nil {
		t.Fatalf("first request should fail over: %v", err)
	}
	if first.Status != "ok" || first.WorkerID == "worker-1" {
		t.Fatalf("first request should retry onto another worker: %+v", first)
	}

	for i := 1; i < 39; i++ {
		res, err := svc.Execute(ctx, Request{
			RequestID: fmt.Sprintf("failover-%d", i), TenantID: "default", Model: "workhorse",
			Prompt: fmt.Sprintf("failover follow-up %d", i), MaxTokens: 2,
			RoutingPolicy: "round-robin",
		}, nil)
		if err != nil {
			t.Fatalf("request %d failed: %v", i, err)
		}
		if res.Status != "ok" {
			t.Fatalf("request %d status = %q, want ok", i, res.Status)
		}
	}

	if got := testutil.ToFloat64(metrics.RetriesTotal); got != 1 {
		t.Fatalf("expected exactly one retry, got %v", got)
	}
}

func testGatewayConfig(lis ...net.Listener) config.GatewayConfig {
	addrs := make([]string, len(lis))
	for i, l := range lis {
		addrs[i] = l.Addr().String()
	}
	return config.GatewayConfig{
		WorkerAddrs:             addrs,
		HealthPeriod:            20 * time.Millisecond,
		BlockSize:               32,
		MaxRetries:              3,
		RetryBase:               time.Millisecond,
		QueueWait:               time.Second,
		MaxInFlight:             32,
		QueueSize:               16,
		PrefixTTL:               time.Minute,
		BreakerFailureThreshold: 5,
		BreakerCooldown:         time.Second,
		BreakerWindow:           time.Minute,
		Tenants:                 map[string]string{"dev-key": "default"},
	}
}

func startWorker(lis net.Listener, id string, failFirst int) *grpc.Server {
	cfg := config.WorkerConfig{
		ID: id, GRPCAddr: lis.Addr().String(), Model: "workhorse", Mode: "mock",
		Capacity: 8, TokensPerSec: 2000, TokenJitterMS: 0, MaxTokens: 8,
		FailFirst: failFirst,
	}
	s := grpc.NewServer()
	gen.RegisterWorkerServiceServer(s, worker.NewServer(cfg))
	go func() { _ = s.Serve(lis) }()
	return s
}

func waitForHealthy(t *testing.T, rt *router.Router, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if len(rt.HealthyWorkers()) == want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("expected %d healthy workers, got %d", want, len(rt.HealthyWorkers()))
}
