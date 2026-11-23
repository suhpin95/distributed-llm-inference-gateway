// Package gateway implements the request pipeline shared by the HTTP/SSE and
// native gRPC frontends: rate limit -> admission -> route -> stream -> retry.
package gateway

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"time"

	"github.com/example/distributed-llm-gateway/gen"
	"github.com/example/distributed-llm-gateway/internal/adaptive"
	"github.com/example/distributed-llm-gateway/internal/config"
	"github.com/example/distributed-llm-gateway/internal/logkafka"
	"github.com/example/distributed-llm-gateway/internal/observability"
	"github.com/example/distributed-llm-gateway/internal/ratelimit"
	"github.com/example/distributed-llm-gateway/internal/router"
)

var (
	ErrRateLimited = errors.New("tenant rate limit exceeded")
	ErrShed        = errors.New("gateway load shedding")
	ErrNoWorker    = errors.New("no healthy backend available")
)

type Request struct {
	RequestID     string
	TenantID      string
	Model         string
	Prompt        string
	PrefixHashes  []string
	MaxTokens     int32
	Temperature   float64
	RoutingPolicy string

	// PreflightRelease is set by HTTP/SSE callers that reserved admission
	// before committing response headers.
	PreflightRelease func()
}

type Result struct {
	RequestID    string
	Tenant       string
	Model        string
	ServedModel  string
	WorkerID     string
	Route        string
	Status       string
	OutputTokens int
	CachedTokens int64
	TTFT         time.Duration
	Total        time.Duration
}

type Service struct {
	cfg       config.GatewayConfig
	router    *router.Router
	limiter   *ratelimit.Limiter
	admission *adaptive.Limiter
	metrics   *observability.Metrics
	kafka     *logkafka.Logger
}

func NewService(cfg config.GatewayConfig, r *router.Router, limiter *ratelimit.Limiter,
	admission *adaptive.Limiter, metrics *observability.Metrics, kafka *logkafka.Logger) *Service {
	return &Service{cfg: cfg, router: r, limiter: limiter, admission: admission, metrics: metrics, kafka: kafka}
}

// Preflight reserves a rate-limit token and an admission slot before an HTTP
// handler commits streaming headers, so rejections can carry a real 429/503.
func (s *Service) Preflight(ctx context.Context, tenant, model string) (func(), error) {
	if !s.limiter.Allow(ctx, tenant) {
		s.metrics.RateLimitRej.WithLabelValues(tenant).Inc()
		s.metrics.ShedTotal.WithLabelValues("rate-limit").Inc()
		s.record(&Result{Tenant: tenant, Model: model, Status: "rate_limited"})
		return nil, ErrRateLimited
	}
	release, ok := s.admission.Acquire(ctx)
	if !ok {
		s.metrics.ShedTotal.WithLabelValues("admission").Inc()
		s.record(&Result{Tenant: tenant, Model: model, Status: "shed"})
		return nil, ErrShed
	}
	return release, nil
}

// Execute streams tokens through emit and returns accounting metadata. Retries
// only happen before the first token, so clients never see duplicated output.
func (s *Service) Execute(ctx context.Context, req Request, emit func(string) error) (*Result, error) {
	started := time.Now()
	result := &Result{RequestID: req.RequestID, Tenant: req.TenantID, Model: req.Model}

	var release func()
	if req.PreflightRelease != nil {
		release = req.PreflightRelease
	} else {
		if !s.limiter.Allow(ctx, req.TenantID) {
			s.metrics.RateLimitRej.WithLabelValues(req.TenantID).Inc()
			s.metrics.ShedTotal.WithLabelValues("rate-limit").Inc()
			result.Status = "rate_limited"
			s.record(result)
			return result, ErrRateLimited
		}
		var ok bool
		release, ok = s.admission.Acquire(ctx)
		if !ok {
			s.metrics.ShedTotal.WithLabelValues("admission").Inc()
			result.Status = "shed"
			s.record(result)
			return result, ErrShed
		}
	}
	succeeded := false
	defer func() {
		s.admission.Report(succeeded)
		release()
	}()

	greq := &gen.GenerateRequest{
		RequestId: req.RequestID, TenantId: req.TenantID, Model: req.Model,
		Prompt: req.Prompt, PrefixHashes: req.PrefixHashes,
		MaxTokens: req.MaxTokens, Temperature: float32(req.Temperature),
		RoutingPolicy: req.RoutingPolicy,
	}

	excluded := []string{}
	attempts := max(1, s.cfg.MaxRetries)
	for attempt := 1; attempt <= attempts; attempt++ {
		sel, err := s.router.Select(ctx, greq, excluded...)
		if err != nil {
			result.Status = "no_worker"
			s.record(result)
			return result, fmt.Errorf("%w: %v", ErrNoWorker, err)
		}
		if !sel.Client.Breaker.Allow() {
			excluded = append(excluded, sel.Client.ID)
			continue
		}

		streamCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
		stream, err := sel.Client.Generate(streamCtx, greq)
		if err != nil {
			cancel()
			excluded = s.failAttempt(sel, result, excluded)
			if attempt == attempts {
				break
			}
			s.backoff(ctx, attempt)
			continue
		}

		first, err := stream.Recv()
		if err != nil || first == nil || first.Kind == gen.GenerateResponse_ERROR {
			cancel()
			excluded = s.failAttempt(sel, result, excluded)
			if attempt == attempts {
				break
			}
			s.backoff(ctx, attempt)
			continue
		}

		result.WorkerID = sel.Client.ID
		result.ServedModel = sel.Model
		result.Route = sel.Reason
		result.Status = "ok"
		result.CachedTokens = first.CachedTokens
		result.TTFT = time.Since(started)
		s.metrics.TTFT.WithLabelValues(sel.Model, sel.Reason).Observe(result.TTFT.Seconds())

		if emit != nil {
			if err := emit(first.Token); err != nil {
				cancel()
				result.Status = "client_disconnected"
				return result, err
			}
		}
		result.OutputTokens++

		for {
			resp, err := stream.Recv()
			if errors.Is(err, io.EOF) || (err == nil && resp.Kind == gen.GenerateResponse_DONE) {
				if resp != nil && resp.CachedTokens > result.CachedTokens {
					result.CachedTokens = resp.CachedTokens
				}
				break
			}
			if err != nil {
				result.Status = "upstream_error"
				log.Printf("gateway: upstream stream error from %s: %v", sel.Client.ID, err)
				break
			}
			if resp.Kind == gen.GenerateResponse_TOKEN {
				if emit != nil {
					if e := emit(resp.Token); e != nil {
						cancel()
						result.Status = "client_disconnected"
						return result, e
					}
				}
				result.OutputTokens++
				if resp.CachedTokens > result.CachedTokens {
					result.CachedTokens = resp.CachedTokens
				}
			}
		}
		cancel()

		upstreamOK := result.Status == "ok"
		if upstreamOK {
			sel.Client.Breaker.Success()
			succeeded = true
		} else {
			sel.Client.Breaker.Failure()
		}
		result.Total = time.Since(started)
		s.record(result)
		return result, nil
	}

	result.Total = time.Since(started)
	if result.Status == "retry" {
		result.Status = "exhausted"
	}
	s.record(result)
	return result, ErrNoWorker
}

func (s *Service) failAttempt(sel *router.Selection, result *Result, excluded []string) []string {
	sel.Client.Breaker.Failure()
	result.Status = "retry"
	s.metrics.RetriesTotal.Inc()
	return append(excluded, sel.Client.ID)
}

func (s *Service) backoff(ctx context.Context, attempt int) {
	base := time.Duration(attempt) * s.cfg.RetryBase
	jitter := time.Duration(rand.Int63n(int64(max(1, base))))
	delay := base/2 + jitter
	select {
	case <-time.After(delay):
	case <-ctx.Done():
	}
}

func (s *Service) record(r *Result) {
	s.metrics.RequestsTotal.WithLabelValues(r.Tenant, r.Model, r.Status).Inc()
	s.metrics.RequestLatency.WithLabelValues(r.Tenant, r.Model).Observe(r.Total.Seconds())
	if r.OutputTokens > 0 {
		s.metrics.OutputTokens.WithLabelValues(r.Tenant, r.Model).Add(float64(r.OutputTokens))
		s.metrics.AddTokens(r.OutputTokens)
	}
	s.kafka.Write(context.Background(), logkafka.Record{
		Timestamp: time.Now(), RequestID: r.RequestID, Tenant: r.Tenant,
		Model: r.Model, ServedModel: r.ServedModel, Worker: r.WorkerID,
		Route: r.Route, Status: r.Status, OutputTokens: r.OutputTokens,
		CachedTokens: r.CachedTokens, TTFTMs: float64(r.TTFT.Milliseconds()),
		TotalMs: float64(r.Total.Milliseconds()),
	})
}
