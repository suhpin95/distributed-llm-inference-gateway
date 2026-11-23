#!/usr/bin/env bash
# Kills one backend while traffic is flowing and shows breaker/failover state.
# Stop run-mocks.sh first so ports 9101-9103 are free.
set -euo pipefail
cd "$(dirname "$0")/.."
make build >/dev/null

pids=()
cleanup() {
  for pid in "${pids[@]:-}"; do kill "$pid" 2>/dev/null || true; done
  wait 2>/dev/null || true
}
trap cleanup EXIT INT TERM

start_worker() {
  WORKER_ID="$1" WORKER_GRPC_ADDR=":$2" WORKER_MODEL=workhorse \
  WORKER_MODE=mock WORKER_CAPACITY=8 WORKER_TOKENS_PER_SEC="$3" \
  ./bin/worker >/tmp/gw-"$1".log 2>&1 &
  pids+=("$!")
}
start_worker worker-1 9101 45
start_worker worker-2 9102 35
start_worker worker-3 9103 25
sleep 1
WORKER_ADDRS=localhost:9101,localhost:9102,localhost:9103 \
HEALTH_PERIOD=1s BREAKER_FAILURE_THRESHOLD=2 BREAKER_COOLDOWN=2s \
./bin/gateway >/tmp/gw-gateway.log 2>&1 &
pids+=("$!")
sleep 2

# Steady traffic with varied prompts so all three workers see requests.
(
  for i in $(seq 1 400); do
    curl -s -o /dev/null -X POST http://127.0.0.1:8080/v1/chat/completions \
      -H 'Content-Type: application/json' -H 'Authorization: Bearer dev-key' \
      -d "{\"model\":\"workhorse\",\"max_tokens\":8,\"messages\":[{\"role\":\"user\",\"content\":\"request $i for routing\"}]}" || true
  done
) &
traffic_pid=$!

echo "Killing worker-2 in 4 seconds..."
sleep 4
kill "${pids[1]}"
echo "worker-2 killed at $(date +%T)"
sleep 6
kill "$traffic_pid" 2>/dev/null || true

echo
echo "=== Workers after failure ==="
curl -s http://127.0.0.1:8080/admin/workers | python3 -m json.tool
echo
echo "=== Relevant metrics ==="
curl -s http://127.0.0.1:9091/metrics | grep -E 'gateway_(circuit_state|retries_total|shed_total|requests_total|route_decisions_total)' | grep -v '^#' | head -40
