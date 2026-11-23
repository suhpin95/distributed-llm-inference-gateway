// Package config loads POC settings from the environment. Every setting has a
// local-first default so the gateway runs with zero external services.
package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// GatewayConfig configures the gateway process.
type GatewayConfig struct {
	HTTPAddr     string
	GRPCAddr     string
	MetricsAddr  string
	WorkerAddrs  []string
	RedisAddr    string
	KafkaBrokers []string
	KafkaTopic   string
	Tenants      map[string]string // API key -> tenant id

	PrefixTTL    time.Duration
	BlockSize    int
	TenantRate   float64
	TenantBurst  float64
	MaxRetries   int
	RetryBase    time.Duration
	QueueWait    time.Duration
	MaxInFlight  int
	QueueSize    int
	HealthPeriod time.Duration

	BreakerFailureThreshold int
	BreakerWindow           time.Duration
	BreakerCooldown         time.Duration
}

// WorkerConfig configures one worker agent process.
type WorkerConfig struct {
	ID             string
	GRPCAddr       string
	Model          string
	Mode           string // "mock" or "llama"
	Capacity       int
	TokensPerSec   float64
	TokenJitterMS  int
	FailureRate    float64
	FailFirst      int
	LlamaURL       string
	LlamaModelName string
	MaxTokens      int32
}

// LoadGateway reads gateway configuration from the environment.
func LoadGateway() GatewayConfig {
	return GatewayConfig{
		HTTPAddr:     env("GATEWAY_HTTP_ADDR", ":8080"),
		GRPCAddr:     env("GATEWAY_GRPC_ADDR", ":9090"),
		MetricsAddr:  env("GATEWAY_METRICS_ADDR", ":9091"),
		WorkerAddrs:  csv("WORKER_ADDRS", "localhost:9101,localhost:9102,localhost:9103"),
		RedisAddr:    os.Getenv("REDIS_ADDR"),
		KafkaBrokers: csv("KAFKA_BROKERS", ""),
		KafkaTopic:   env("KAFKA_TOPIC", "inference-requests"),
		Tenants:      tenants(env("GATEWAY_TENANTS", "dev-key:default")),

		PrefixTTL:    duration("PREFIX_TTL", 10*time.Minute),
		BlockSize:    integer("PREFIX_BLOCK_SIZE", 64),
		TenantRate:   float("TENANT_RATE", 25),
		TenantBurst:  float("TENANT_BURST", 50),
		MaxRetries:   integer("MAX_RETRIES", 3),
		RetryBase:    duration("RETRY_BASE", 50*time.Millisecond),
		QueueWait:    duration("QUEUE_WAIT", 500*time.Millisecond),
		MaxInFlight:  integer("MAX_IN_FLIGHT", 64),
		QueueSize:    integer("QUEUE_SIZE", 32),
		HealthPeriod: duration("HEALTH_PERIOD", 2*time.Second),

		BreakerFailureThreshold: integer("BREAKER_FAILURE_THRESHOLD", 5),
		BreakerWindow:           duration("BREAKER_WINDOW", 30*time.Second),
		BreakerCooldown:         duration("BREAKER_COOLDOWN", 5*time.Second),
	}
}

// LoadWorker reads worker configuration from the environment.
func LoadWorker() WorkerConfig {
	return WorkerConfig{
		ID:             env("WORKER_ID", "worker-1"),
		GRPCAddr:       env("WORKER_GRPC_ADDR", ":9101"),
		Model:          env("WORKER_MODEL", "workhorse"),
		Mode:           env("WORKER_MODE", "mock"),
		Capacity:       integer("WORKER_CAPACITY", 8),
		TokensPerSec:   float("WORKER_TOKENS_PER_SEC", 45),
		TokenJitterMS:  integer("WORKER_TOKEN_JITTER_MS", 8),
		FailureRate:    float("WORKER_FAILURE_RATE", 0),
		FailFirst:      integer("WORKER_FAIL_FIRST", 0),
		LlamaURL:       env("LLAMA_URL", "http://127.0.0.1:8081"),
		LlamaModelName: env("LLAMA_MODEL", "qwen3-8b-q4_k_m"),
		MaxTokens:      int32(integer("WORKER_MAX_TOKENS", 96)),
	}
}

func env(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

func csv(key, fallback string) []string {
	raw := env(key, fallback)
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func tenants(raw string) map[string]string {
	out := map[string]string{}
	for _, pair := range strings.Split(raw, ",") {
		kv := strings.SplitN(strings.TrimSpace(pair), ":", 2)
		if len(kv) == 2 {
			out[kv[0]] = kv[1]
		}
	}
	return out
}

func integer(key string, fallback int) int {
	v, err := strconv.Atoi(os.Getenv(key))
	if err != nil {
		return fallback
	}
	return v
}

func float(key string, fallback float64) float64 {
	v, err := strconv.ParseFloat(os.Getenv(key), 64)
	if err != nil {
		return fallback
	}
	return v
}

func duration(key string, fallback time.Duration) time.Duration {
	v, err := time.ParseDuration(os.Getenv(key))
	if err != nil {
		return fallback
	}
	return v
}
