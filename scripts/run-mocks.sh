#!/usr/bin/env bash
# Starts three deterministic mock workers plus the gateway. No GPU, no Docker,
# no model download required. Press Ctrl-C to stop everything.
set -euo pipefail
cd "$(dirname "$0")/.."

pids=()
cleanup() {
  for pid in "${pids[@]:-}"; do
    kill "$pid" 2>/dev/null || true
  done
  wait 2>/dev/null || true
}
trap cleanup EXIT INT TERM

start_worker() {
  local id="$1" port="$2" rate="$3"
  WORKER_ID="$id" \
  WORKER_GRPC_ADDR=":$port" \
  WORKER_MODEL="workhorse" \
  WORKER_MODE="mock" \
  WORKER_CAPACITY="${WORKER_CAPACITY:-8}" \
  WORKER_TOKENS_PER_SEC="$rate" \
  ./bin/worker &
  pids+=("$!")
}

start_worker worker-1 9101 45
start_worker worker-2 9102 35
start_worker worker-3 9103 25

sleep 1

WORKER_ADDRS="localhost:9101,localhost:9102,localhost:9103" \
GATEWAY_HTTP_ADDR=":8080" \
GATEWAY_GRPC_ADDR=":9090" \
GATEWAY_METRICS_ADDR=":9091" \
./bin/gateway &
pids+=("$!")

echo "Gateway ready: http://127.0.0.1:8080  metrics: http://127.0.0.1:9091/metrics"
echo "Try: curl -N -X POST http://127.0.0.1:8080/v1/chat/completions \\"
echo '  -H "Content-Type: application/json" -H "Authorization: Bearer dev-key" \'
echo '  -d '"'"'{"model":"workhorse","messages":[{"role":"user","content":"Hello gateway"}],"stream":true}'"'"''
wait
