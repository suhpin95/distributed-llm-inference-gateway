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
