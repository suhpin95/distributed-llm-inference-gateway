# Distributed LLM Inference Gateway — POC

A Go POC gateway in front of multiple model servers. It demonstrates token streaming,
KV-cache-aware routing, consistent hashing, load shedding, per-tenant rate limiting,
retries with backoff, circuit breakers, Kafka request logs, and autoscaling on
**tokens/sec** instead of CPU.

```
client (curl / k6 / gRPC)
   │  OpenAI-compatible HTTP+SSE  ─or─  native gRPC stream
   ▼
┌──────────────────────────── gateway ────────────────────────────┐
│ auth → Redis token bucket → AIMD admission → router → stream    │
│ router: prefix index → consistent-hash ring → least-loaded       │
│         → per-worker circuit breaker → retry (pre-token only)    │
│ metrics (Prometheus) + request log (Kafka)                       │
└──────────────────────────────────────────────────────────────────┘
   ▼
WorkerService (gRPC): deterministic mock workers | llama.cpp worker agent (Metal)
   ▼
Redis (rate limits + prefix index) · Kafka (logs) · K8s KEDA/HPA on tokens/sec
```

All backends implement one gRPC contract (`proto/inference.proto`), so the gateway
cannot tell a real llama.cpp worker from a deterministic mock.

## Requirements

- Go 1.27+ (`brew install go`), `make`
- Optional: Docker Desktop (Redis/Kafka/Prometheus), `brew install k6`,
  `brew install llama.cpp` for real local inference

## Quickstart (no GPU, no Docker, no model download)

```bash
make test          # unit + integration tests
make run-mocks     # 3 mock workers + gateway on :8080, gRPC :9090, metrics :9091
```

In another terminal:

> **Note:** Ensure the gateway is running (e.g., with `make run-mocks`) before running the following commands.

```bash
# Token streaming (SSE)
curl -N -X POST http://127.0.0.1:8080/v1/chat/completions \
  -H 'Content-Type: application/json' -H 'Authorization: Bearer dev-key' \
  -d '{"model":"workhorse","messages":[{"role":"user","content":"Hello gateway"}],"stream":true}'

# Non-streaming, with routing metadata in the response
curl -s -X POST http://127.0.0.1:8080/v1/chat/completions \
  -H 'Content-Type: application/json' -H 'Authorization: Bearer dev-key' \
  -d '{"model":"workhorse","messages":[{"role":"user","content":"Explain prefix caching"}]}'

# Admin view of workers, breakers and cache hits
curl -s http://127.0.0.1:8080/admin/workers | python3 -m json.tool

# Prometheus metrics
curl -s http://127.0.0.1:9091/metrics | grep '^gateway_'

# Health check
curl http://127.0.0.1:8080/healthz

# Native gRPC streaming client
./bin/grpc-client -addr localhost:9090 -prompt 'Stream over gRPC'
```

To format the Go source or build the binaries without starting the services:

```bash
make fmt
make build
```

## Real local model (M2 Pro / 16 GB)

vLLM requires CUDA, so local inference uses **llama.cpp (Metal)** with a quantized
model. An 8B Q4_K_M model (~5 GB) fits comfortably in 16 GB unified memory.

```bash
brew install llama.cpp
# Download Qwen3-8B-Q4_K_M.gguf (base model) from Qwen/Qwen3-8B-GGUF on Hugging Face, then:
export LLAMA_MODEL_PATH=/path/to/Qwen3-8B-Q4_K_M.gguf
make run-real
```

`scripts/run-real.sh` starts llama.cpp on `:8081`, one llama worker agent on `:9101`,
and two mock replicas on `:9102/:9103`. The worker sends `cache_prompt: true` to
llama.cpp so repeated prefixes actually reuse KV blocks; the gateway's own prefix
index additionally prefers the worker that already holds a prefix.
If `LLAMA_MODEL_PATH` is unset, `make run-real` starts the mock path instead.

For a lightweight real-inference smoke test, use `Qwen3-0.6B-Q4_K_M.gguf` from
`Qwen/Qwen3-0.6B-GGUF` (~0.4 GB) instead of the 8B model. No frontier-class model is
configured: a 16 GB M2 Pro cannot host one, and none is simulated. The `tier=host:port`
syntax in `WORKER_ADDRS` is generic, so a hosted endpoint can be added later without
gateway changes.

## Infrastructure (optional)

```bash
make infra        # Redis :6379, Kafka :9092, Prometheus :9090
REDIS_ADDR=localhost:6379 KAFKA_BROKERS=localhost:9092 make run-mocks
```

Kafka receives one JSON record per request on topic `inference-requests`
(`request_id`, tenant, model, worker, route, cached_tokens, TTFT, status, ...).
With no brokers configured, logging is a no-op.

Stop the optional infrastructure with:

```bash
make infra-down
```

## Load testing

With the mock gateway running, start it with higher admission limits:

```bash
TENANT_RATE=50000 \
TENANT_BURST=100000 \
MAX_IN_FLIGHT=200 \
QUEUE_SIZE=200 \
make run-mocks
```

In another terminal, run the k6 load test (install k6 first, for example with
`brew install k6`):

```bash
make k6
```

## The four proofs

| Proof | How to reproduce |
|---|---|
| **p99 under load** | Start the gateway with `TENANT_RATE=50000 TENANT_BURST=100000 MAX_IN_FLIGHT=200 QUEUE_SIZE=200 make run-mocks`, then `make k6` (ramp 0→100 VUs, 80% shared prefixes). Read `http_req_duration p(99)` in the k6 summary or Grafana. |
| **Backend dies** | `bash scripts/failover-demo.sh` — kills worker-2 mid-traffic, then prints breaker states, retries, route decisions. |
| **Cache hit improvement** | `bash scripts/proof.sh` — runs identical k6 workloads with `X-Routing-Policy: round-robin` and `smart`, printing cache hit counters and p99. |
| **Load shedding** | Raise k6 target or lower `MAX_IN_FLIGHT`; watch `gateway_shed_total` climb while p99 stays bounded and clients get `429 + Retry-After`. |

Expected behavior: smart routing concentrates shared prefixes on few workers,
raising `gateway_cache_hits_total` and lowering TTFT/p99 versus round-robin;
killing a backend opens its circuit (metric `gateway_circuit_state=1`), traffic
moves to survivors, and pre-first-token failures are retried on another worker.

`dashboards/grafana.json` can be imported into Grafana with a Prometheus datasource.

## Measured results (mock workers, not real inference)

All figures below were measured on an **Apple M2 Pro (16 GB unified memory)** with
`make run-mocks` (three deterministic Go mock workers) and `make k6` (ramp 0→100 VUs,
80% shared prefixes). The mocks emit synthetic tokens and simulate KV-cache hits;
**no model was loaded and no GPU was used**. These numbers demonstrate the gateway's
routing and admission behaviour only — they are **not** real-model inference
benchmarks.

| Metric | Result | Source |
|---|---|---|
| Throughput | **71.3 req/s** | k6 `http_reqs` at peak load |
| Cache hit rate | **79%** | `gateway_cache_hits_total / (hits + misses)` |
| Smart routing vs round-robin | **+7.5%** | `scripts/proof.sh`, same workload, both policies |
| p99 latency | **975 ms** | k6 `http_req_duration p(99)` under the same load |

Reproduce them with the commands in [The four proofs](#the-four-proofs). A
real-inference TTFT comparison with Qwen3-0.6B-GGUF on llama.cpp/Metal is a separate
follow-up; no real-model latency numbers are claimed here.

## Configuration

All settings are environment variables. Key knobs:

| Variable | Default | Purpose |
|---|---|---|
| `WORKER_ADDRS` | `localhost:9101,...` | Workers; optional `tier=host:port` names a tier (none configured by default). |
| `GATEWAY_TENANTS` | `dev-key:default` | API key → tenant map. |
| `TENANT_RATE` / `TENANT_BURST` | `25` / `50` | Per-tenant token bucket. |
| `MAX_IN_FLIGHT` / `QUEUE_SIZE` / `QUEUE_WAIT` | `64` / `32` / `500ms` | Admission control and shedding. |
| `MAX_RETRIES` / `RETRY_BASE` | `3` / `50ms` | Pre-token retry policy with jitter. |
| `BREAKER_FAILURE_THRESHOLD` / `BREAKER_COOLDOWN` | `5` / `5s` | Circuit breaker. |
| `PREFIX_TTL` / `PREFIX_BLOCK_SIZE` | `10m` / `64` | Prefix index lifetime and block size. |
| `REDIS_ADDR` / `KAFKA_BROKERS` | empty | Enable shared state / request logs. |
| `WORKER_MODE`, `WORKER_FAILURE_RATE`, `WORKER_TOKENS_PER_SEC` | `mock`, `0`, `45` | Worker behavior in tests. |

## Kubernetes / autoscaling

```bash
docker build -f Dockerfile.gateway -t inference-gateway:latest .
docker build -f Dockerfile.worker  -t inference-worker:latest .
# with kind:
kind load docker-image inference-gateway:latest inference-worker:latest
kubectl apply -f deploy/k8s/gateway.yaml
kubectl apply -f deploy/k8s/workers.yaml
kubectl apply -f deploy/k8s/autoscale.yaml   # requires KEDA + Prometheus Operator
```

`deploy/k8s/autoscale.yaml` scales the worker StatefulSet on
`sum(gateway_output_tokens_per_second)` (threshold 40 tok/s, 2–8 replicas)
instead of CPU. For plain HPA, expose the same metric through the Prometheus
Adapter and point an `HorizontalPodAutoscaler` at it.

## Design notes / limitations

- **Retries stop at the first token**: once output has been emitted, an upstream
  failure is surfaced as `upstream_error` rather than replaying duplicate tokens.
- **Prefix index is advisory**: stale entries pointing at dead workers are
  ignored; the consistent-hash ring and health checks keep routing correct.
- **Mocks are deterministic** so k6 results are reproducible. The real llama.cpp
  worker is for end-to-end fidelity; use several GPU hosts/vLLM for a production
  version of the multi-replica story.
- **No frontier tier is configured**: the local M2 Pro cannot host a frontier-class
  model and no hosted endpoint is wired in. Tier routing is generic
  (`tier=host:port`) and falls back to `workhorse` when a tier has no healthy workers.
- The POC uses in-process metrics on `:9091` and a separate HTTP mux on `:8080`;
  no service mesh or TLS is included.
