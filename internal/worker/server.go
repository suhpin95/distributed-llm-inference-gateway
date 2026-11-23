package worker

import (
	"context"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/example/distributed-llm-gateway/gen"
	"github.com/example/distributed-llm-gateway/internal/config"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var vocabulary = strings.Fields(
	"the gateway routes tokens across distributed workers with prefix affinity " +
		"and bounded queues streaming responses while shedding overload " +
		"cache hits reduce time to first token under bursty tenant traffic patterns",
)

// Server implements WorkerService in deterministic mock mode or by proxying a
// local llama.cpp server in llama mode.
type Server struct {
	gen.UnimplementedWorkerServiceServer

	cfg config.WorkerConfig

	mu          sync.Mutex
	inFlight    int
	cache       map[string]struct{}
	cacheHits   int64
	cacheMisses int64

	tokensTotal atomic.Int64
	requests    atomic.Int64
	started     time.Time
}

func NewServer(cfg config.WorkerConfig) *Server {
	return &Server{
		cfg:     cfg,
		cache:   map[string]struct{}{},
		started: time.Now(),
	}
}

func (s *Server) Generate(req *gen.GenerateRequest, stream gen.WorkerService_GenerateServer) error {
	s.mu.Lock()
	if s.inFlight >= s.cfg.Capacity {
		s.mu.Unlock()
		return status.Error(codes.ResourceExhausted, "worker at capacity")
	}
	s.inFlight++
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.inFlight--
		s.mu.Unlock()
	}()

	ctx := stream.Context()
	maxTokens := req.MaxTokens
	if maxTokens <= 0 {
		maxTokens = s.cfg.MaxTokens
	}
	if maxTokens > 512 {
		maxTokens = 512
	}

	cached := s.consumePrefixes(req.PrefixHashes)
	workerID := s.cfg.ID

	attempt := s.requests.Add(1)
	if s.cfg.FailFirst > 0 && int(attempt) <= s.cfg.FailFirst {
		return status.Error(codes.Unavailable, "injected first-request failure")
	}
	if s.cfg.FailureRate > 0 && rand.Float64() < s.cfg.FailureRate {
		return status.Error(codes.Unavailable, "injected backend failure")
	}

	if strings.EqualFold(s.cfg.Mode, "llama") {
		cachedFromBackend, err := streamLlama(ctx, s.cfg, req, maxTokens, func(token string) error {
			return stream.Send(&gen.GenerateResponse{
				Kind: gen.GenerateResponse_TOKEN, Token: token,
				WorkerId: workerID, CachedTokens: cached,
			})
		})
		if err != nil {
			return status.Errorf(codes.Internal, "llama backend: %v", err)
		}
		if cachedFromBackend > cached {
			cached = cachedFromBackend
		}
	} else {
		if err := s.streamMock(ctx, req, maxTokens, cached, workerID, stream); err != nil {
			return err
		}
	}

	s.tokensTotal.Add(int64(maxTokens))
	return stream.Send(&gen.GenerateResponse{
		Kind: gen.GenerateResponse_DONE, WorkerId: workerID,
		TokensGenerated: maxTokens, CachedTokens: cached,
	})
}

func (s *Server) streamMock(ctx context.Context, req *gen.GenerateRequest, n int32, cached int64, id string, stream gen.WorkerService_GenerateServer) error {
	seed := int64(0)
	for _, c := range req.RequestId {
		seed = seed*31 + int64(c)
	}
	r := rand.New(rand.NewSource(seed))
	delay := time.Duration(float64(time.Second) / max(1, s.cfg.TokensPerSec))
	for i := 0; i < int(n); i++ {
		jitter := 0
		if s.cfg.TokenJitterMS > 0 {
			jitter = r.Intn(s.cfg.TokenJitterMS) - s.cfg.TokenJitterMS/2
		}
		timer := time.NewTimer(max(0, delay+time.Duration(jitter)*time.Millisecond))
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		word := vocabulary[r.Intn(len(vocabulary))]
		if i > 0 {
			word = " " + word
		}
		if err := stream.Send(&gen.GenerateResponse{
			Kind: gen.GenerateResponse_TOKEN, Token: word,
			WorkerId: id, CachedTokens: cached,
		}); err != nil {
			return err
		}
	}
	return nil
}

// consumePrefixes records this worker as the owner of every prefix and counts
// how many were already resident, approximating the backend's KV cache.
func (s *Server) consumePrefixes(hashes []string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	var hits int64
	for _, h := range hashes {
		if _, ok := s.cache[h]; ok {
			hits++
		} else {
			s.cacheMisses++
		}
		s.cache[h] = struct{}{}
	}
	s.cacheHits += hits
	return hits * 32 // approximate tokens per prefix block
}

func (s *Server) Stats(_ context.Context, _ *gen.StatsRequest) (*gen.WorkerStats, error) {
	s.mu.Lock()
	inFlight := s.inFlight
	cached := len(s.cache)
	hits := s.cacheHits
	misses := s.cacheMisses
	s.mu.Unlock()

	elapsed := time.Since(s.started).Seconds()
	tps := 0.0
	if elapsed > 0 {
		tps = float64(s.tokensTotal.Load()) / elapsed
	}
	return &gen.WorkerStats{
		WorkerId: s.cfg.ID, Model: s.cfg.Model,
		InFlight: int32(inFlight), Capacity: int32(s.cfg.Capacity),
		CachedPrefixes: int64(cached), Healthy: true,
		TokensPerSecond: tps, CacheHits: hits, CacheMisses: misses,
	}, nil
}
