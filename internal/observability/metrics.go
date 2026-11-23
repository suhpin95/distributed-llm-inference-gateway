// Package observability owns Prometheus metrics, including the tokens/sec
// gauge used by KEDA/HPA instead of CPU.
package observability

import (
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"net/http"
)

type Metrics struct {
	registry *prometheus.Registry

	RequestsTotal  *prometheus.CounterVec
	RequestLatency *prometheus.HistogramVec
	TTFT           *prometheus.HistogramVec
	OutputTokens   *prometheus.CounterVec
	TokensPerSec   prometheus.Gauge

	CacheHits      *prometheus.CounterVec
	CacheMisses    *prometheus.CounterVec
	RouteDecisions *prometheus.CounterVec
	ShedTotal      *prometheus.CounterVec
	RetriesTotal   prometheus.Counter
	RateLimitRej   *prometheus.CounterVec
	CircuitState   *prometheus.GaugeVec
	WorkerInFlight *prometheus.GaugeVec

	AdmissionLimit    prometheus.Gauge
	AdmissionInFlight prometheus.Gauge
	AdmissionQueued   prometheus.Gauge

	tokens atomic.Int64
	tps    atomic.Int64
}

func New() *Metrics {
	m := &Metrics{registry: prometheus.NewRegistry()}

	m.RequestsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "gateway_requests_total", Help: "Requests by tenant, model and status.",
	}, []string{"tenant", "model", "status"})
	m.RequestLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "gateway_request_duration_seconds", Help: "End-to-end request latency.",
		Buckets: []float64{0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30},
	}, []string{"tenant", "model"})
	m.TTFT = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "gateway_ttft_seconds", Help: "Time to first streamed token.",
		Buckets: []float64{0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5},
	}, []string{"model", "route"})
	m.OutputTokens = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "gateway_output_tokens_total", Help: "Generated output tokens.",
	}, []string{"tenant", "model"})
	m.TokensPerSec = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "gateway_output_tokens_per_second", Help: "Rolling output throughput; drives autoscaling.",
	})
	m.CacheHits = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "gateway_cache_hits_total", Help: "Prefix cache affinity hits.",
	}, []string{"model"})
	m.CacheMisses = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "gateway_cache_misses_total", Help: "Prefix cache affinity misses.",
	}, []string{"model"})
	m.RouteDecisions = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "gateway_route_decisions_total", Help: "Routing decision reasons.",
	}, []string{"reason"})
	m.ShedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "gateway_shed_total", Help: "Rejected requests by reason.",
	}, []string{"reason"})
	m.RetriesTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "gateway_retries_total", Help: "Retry attempts after a pre-token failure.",
	})
	m.RateLimitRej = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "gateway_rate_limit_rejects_total", Help: "Per-tenant rate limit rejections.",
	}, []string{"tenant"})
	m.CircuitState = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "gateway_circuit_state", Help: "Circuit breaker state (0 closed, 1 open, 0.5 half-open).",
	}, []string{"worker"})
	m.WorkerInFlight = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "gateway_worker_in_flight", Help: "In-flight requests observed per worker.",
	}, []string{"worker"})

	m.AdmissionLimit = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "gateway_admission_limit", Help: "Current AIMD concurrency limit.",
	})
	m.AdmissionInFlight = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "gateway_admission_in_flight", Help: "Requests currently admitted.",
	})
	m.AdmissionQueued = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "gateway_admission_queued", Help: "Requests waiting in the bounded queue.",
	})

	m.registry.MustRegister(
		m.RequestsTotal, m.RequestLatency, m.TTFT, m.OutputTokens, m.TokensPerSec,
		m.CacheHits, m.CacheMisses, m.RouteDecisions, m.ShedTotal, m.RetriesTotal,
		m.RateLimitRej, m.CircuitState, m.WorkerInFlight,
		m.AdmissionLimit, m.AdmissionInFlight, m.AdmissionQueued,
	)
	return m
}

func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

func (m *Metrics) AddTokens(n int)      { m.tokens.Add(int64(n)) }
func (m *Metrics) TokensPerSecond() int { return int(m.tps.Load()) }

// StartTPS samples output tokens once per second for the autoscaling signal.
func (m *Metrics) StartTPS(stop <-chan struct{}) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			m.tps.Store(m.tokens.Swap(0))
		case <-stop:
			return
		}
	}
}
