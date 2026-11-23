#!/usr/bin/env bash
# A/B proof: identical k6 workload with round-robin vs KV-cache-aware routing.
# Requires a running gateway and `brew install k6`. Start the gateway with
# high limits for load testing, e.g.:
#   TENANT_RATE=50000 TENANT_BURST=100000 MAX_IN_FLIGHT=200 QUEUE_SIZE=200 make run-mocks
set -euo pipefail
cd "$(dirname "$0")/.."
BASE_URL="${BASE_URL:-http://127.0.0.1:8080}"

for policy in round-robin smart; do
  echo "===== policy=$policy ====="
  BASE_URL="$BASE_URL" ROUTING_POLICY="$policy" k6 run tests/k6/load.js \
    --summary-trend-stats='avg,p(95),p(99)'
  echo
  echo "--- gateway counters after $policy ---"
  curl -s http://127.0.0.1:9091/metrics | grep -E 'gateway_(cache_hits_total|cache_misses_total|route_decisions_total|ttft_seconds|request_duration_seconds|shed_total)' \
    | grep -v '^#' | grep -E '_count|_sum|_total' | head -30
  echo
done

echo "Compare p(99) http_req_duration and cache_hits_total between the two runs."
