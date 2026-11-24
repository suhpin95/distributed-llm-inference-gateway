package config

import (
	"reflect"
	"testing"
	"time"
)

func clearGatewayEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{
		"GATEWAY_HTTP_ADDR", "GATEWAY_GRPC_ADDR", "GATEWAY_METRICS_ADDR",
		"WORKER_ADDRS", "REDIS_ADDR", "KAFKA_BROKERS", "KAFKA_TOPIC",
		"GATEWAY_TENANTS", "PREFIX_TTL", "PREFIX_BLOCK_SIZE", "TENANT_RATE",
		"TENANT_BURST", "MAX_RETRIES", "RETRY_BASE", "QUEUE_WAIT",
		"MAX_IN_FLIGHT", "QUEUE_SIZE", "HEALTH_PERIOD",
		"BREAKER_FAILURE_THRESHOLD", "BREAKER_WINDOW", "BREAKER_COOLDOWN",
	} {
		t.Setenv(key, "")
	}
}

func TestLoadGatewayDefaults(t *testing.T) {
	clearGatewayEnv(t)
	cfg := LoadGateway()

	if cfg.HTTPAddr != ":8080" || cfg.GRPCAddr != ":9090" || cfg.MetricsAddr != ":9091" {
		t.Fatalf("address defaults = %+v", cfg)
	}
	wantAddrs := []string{"localhost:9101", "localhost:9102", "localhost:9103"}
	if !reflect.DeepEqual(cfg.WorkerAddrs, wantAddrs) {
		t.Fatalf("worker addrs = %v, want %v", cfg.WorkerAddrs, wantAddrs)
	}
	if cfg.TenantRate != 25 || cfg.TenantBurst != 50 {
		t.Fatalf("token bucket = %v/%v", cfg.TenantRate, cfg.TenantBurst)
	}
	if cfg.MaxInFlight != 64 || cfg.QueueSize != 32 || cfg.QueueWait != 500*time.Millisecond {
		t.Fatalf("admission = %d/%d/%v", cfg.MaxInFlight, cfg.QueueSize, cfg.QueueWait)
	}
	if cfg.MaxRetries != 3 || cfg.RetryBase != 50*time.Millisecond {
		t.Fatalf("retry = %d/%v", cfg.MaxRetries, cfg.RetryBase)
	}
	if cfg.HealthPeriod != 2*time.Second {
		t.Fatalf("health period = %v", cfg.HealthPeriod)
	}
	if cfg.BreakerFailureThreshold != 5 || cfg.BreakerWindow != 30*time.Second || cfg.BreakerCooldown != 5*time.Second {
		t.Fatalf("breaker = %d/%v/%v", cfg.BreakerFailureThreshold, cfg.BreakerWindow, cfg.BreakerCooldown)
	}
	if cfg.PrefixTTL != 10*time.Minute || cfg.BlockSize != 64 {
		t.Fatalf("prefix = %v/%d", cfg.PrefixTTL, cfg.BlockSize)
	}
	if cfg.Tenants["dev-key"] != "default" {
		t.Fatalf("tenants = %v", cfg.Tenants)
	}
	if cfg.KafkaTopic != "inference-requests" {
		t.Fatalf("kafka topic = %q", cfg.KafkaTopic)
	}
}

func TestLoadGatewayOverrides(t *testing.T) {
	clearGatewayEnv(t)
	t.Setenv("GATEWAY_HTTP_ADDR", ":18080")
	t.Setenv("WORKER_ADDRS", " w1:9101 , secondary=w2:9102 ")
	t.Setenv("TENANT_RATE", "99.5")
	t.Setenv("TENANT_BURST", "150")
	t.Setenv("MAX_IN_FLIGHT", "128")
	t.Setenv("QUEUE_SIZE", "64")
	t.Setenv("QUEUE_WAIT", "750ms")
	t.Setenv("MAX_RETRIES", "5")
	t.Setenv("RETRY_BASE", "25ms")
	t.Setenv("HEALTH_PERIOD", "750ms")
	t.Setenv("BREAKER_FAILURE_THRESHOLD", "9")
	t.Setenv("BREAKER_WINDOW", "45s")
	t.Setenv("BREAKER_COOLDOWN", "7s")
	t.Setenv("GATEWAY_TENANTS", "k1:t1, k2:t2")

	cfg := LoadGateway()

	if cfg.HTTPAddr != ":18080" {
		t.Fatalf("http addr = %q", cfg.HTTPAddr)
	}
	wantAddrs := []string{"w1:9101", "secondary=w2:9102"}
	if !reflect.DeepEqual(cfg.WorkerAddrs, wantAddrs) {
		t.Fatalf("worker addrs = %v, want %v", cfg.WorkerAddrs, wantAddrs)
	}
	if cfg.TenantRate != 99.5 || cfg.TenantBurst != 150 {
		t.Fatalf("token bucket = %v/%v", cfg.TenantRate, cfg.TenantBurst)
	}
	if cfg.MaxInFlight != 128 || cfg.QueueSize != 64 || cfg.QueueWait != 750*time.Millisecond {
		t.Fatalf("admission = %d/%d/%v", cfg.MaxInFlight, cfg.QueueSize, cfg.QueueWait)
	}
	if cfg.MaxRetries != 5 || cfg.RetryBase != 25*time.Millisecond {
		t.Fatalf("retry = %d/%v", cfg.MaxRetries, cfg.RetryBase)
	}
	if cfg.HealthPeriod != 750*time.Millisecond {
		t.Fatalf("health period = %v", cfg.HealthPeriod)
	}
	if cfg.BreakerFailureThreshold != 9 || cfg.BreakerWindow != 45*time.Second || cfg.BreakerCooldown != 7*time.Second {
		t.Fatalf("breaker = %d/%v/%v", cfg.BreakerFailureThreshold, cfg.BreakerWindow, cfg.BreakerCooldown)
	}
	if cfg.Tenants["k1"] != "t1" || cfg.Tenants["k2"] != "t2" {
		t.Fatalf("tenants = %v", cfg.Tenants)
	}
}

func TestLoadGatewayInvalidValuesFallBack(t *testing.T) {
	clearGatewayEnv(t)
	t.Setenv("TENANT_RATE", "not-a-number")
	t.Setenv("MAX_IN_FLIGHT", "also-bad")
	t.Setenv("HEALTH_PERIOD", "soon")
	t.Setenv("PREFIX_TTL", "later")

	cfg := LoadGateway()

	if cfg.TenantRate != 25 || cfg.MaxInFlight != 64 {
		t.Fatalf("invalid numeric values should fall back, got rate=%v in-flight=%d", cfg.TenantRate, cfg.MaxInFlight)
	}
	if cfg.HealthPeriod != 2*time.Second || cfg.PrefixTTL != 10*time.Minute {
		t.Fatalf("invalid durations should fall back, got health=%v ttl=%v", cfg.HealthPeriod, cfg.PrefixTTL)
	}
}

func TestLoadWorkerDefaultsAndOverrides(t *testing.T) {
	for _, key := range []string{
		"WORKER_ID", "WORKER_GRPC_ADDR", "WORKER_MODEL", "WORKER_MODE",
		"WORKER_CAPACITY", "WORKER_TOKENS_PER_SEC", "WORKER_TOKEN_JITTER_MS",
		"WORKER_FAILURE_RATE", "WORKER_FAIL_FIRST", "LLAMA_URL", "LLAMA_MODEL",
		"WORKER_MAX_TOKENS",
	} {
		t.Setenv(key, "")
	}

	cfg := LoadWorker()
	if cfg.ID != "worker-1" || cfg.GRPCAddr != ":9101" || cfg.Model != "workhorse" || cfg.Mode != "mock" {
		t.Fatalf("worker defaults = %+v", cfg)
	}
	if cfg.Capacity != 8 || cfg.TokensPerSec != 45 || cfg.TokenJitterMS != 8 || cfg.MaxTokens != 96 {
		t.Fatalf("worker numeric defaults = %+v", cfg)
	}
	if cfg.LlamaURL != "http://127.0.0.1:8081" || cfg.LlamaModelName != "qwen3-8b-q4_k_m" {
		t.Fatalf("llama defaults = %+v", cfg)
	}

	t.Setenv("WORKER_ID", "worker-9")
	t.Setenv("WORKER_GRPC_ADDR", ":9200")
	t.Setenv("WORKER_MODEL", "secondary")
	t.Setenv("WORKER_MODE", "llama")
	t.Setenv("WORKER_CAPACITY", "12")
	t.Setenv("WORKER_TOKENS_PER_SEC", "123.5")
	t.Setenv("WORKER_TOKEN_JITTER_MS", "3")
	t.Setenv("WORKER_FAILURE_RATE", "0.25")
	t.Setenv("WORKER_FAIL_FIRST", "2")
	t.Setenv("LLAMA_URL", "http://127.0.0.1:9999")
	t.Setenv("LLAMA_MODEL", "qwen3-0.6b")
	t.Setenv("WORKER_MAX_TOKENS", "48")

	cfg = LoadWorker()
	if cfg.ID != "worker-9" || cfg.GRPCAddr != ":9200" || cfg.Model != "secondary" || cfg.Mode != "llama" {
		t.Fatalf("worker overrides = %+v", cfg)
	}
	if cfg.Capacity != 12 || cfg.TokensPerSec != 123.5 || cfg.TokenJitterMS != 3 {
		t.Fatalf("worker numeric overrides = %+v", cfg)
	}
	if cfg.FailureRate != 0.25 || cfg.FailFirst != 2 || cfg.MaxTokens != 48 {
		t.Fatalf("worker failure overrides = %+v", cfg)
	}
	if cfg.LlamaURL != "http://127.0.0.1:9999" || cfg.LlamaModelName != "qwen3-0.6b" {
		t.Fatalf("llama overrides = %+v", cfg)
	}
}
