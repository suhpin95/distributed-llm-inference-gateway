package worker

import (
	"context"
	"testing"

	"github.com/example/distributed-llm-gateway/gen"
	"github.com/example/distributed-llm-gateway/internal/config"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestConsumePrefixesCountsHits(t *testing.T) {
	s := NewServer(config.WorkerConfig{
		ID: "worker-1", Model: "workhorse", Mode: "mock",
		Capacity: 2, TokensPerSec: 1000, MaxTokens: 4,
	})

	hashes := []string{"a", "b", "c"}
	if got := s.consumePrefixes(hashes); got != 0 {
		t.Fatalf("first sight of prefixes should be a miss, got %d cached tokens", got)
	}
	if got := s.consumePrefixes(hashes); got != 3*32 {
		t.Fatalf("second sight should hit all 3 blocks, got %d cached tokens", got)
	}

	stats, err := s.Stats(context.Background(), &gen.StatsRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if stats.CacheHits != 3 || stats.CacheMisses != 3 {
		t.Fatalf("stats hits/misses = %d/%d, want 3/3", stats.CacheHits, stats.CacheMisses)
	}
	if stats.CachedPrefixes != 3 {
		t.Fatalf("cached prefixes = %d, want 3", stats.CachedPrefixes)
	}
}

func TestGenerateRejectsAtCapacity(t *testing.T) {
	s := NewServer(config.WorkerConfig{
		ID: "worker-1", Model: "workhorse", Mode: "mock",
		Capacity: 1, TokensPerSec: 1000, MaxTokens: 4,
	})
	s.inFlight = s.cfg.Capacity

	err := s.Generate(&gen.GenerateRequest{}, nil)
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("expected ResourceExhausted at capacity, got %v", err)
	}
}
