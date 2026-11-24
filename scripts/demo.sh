#!/usr/bin/env bash
# 60-second end-to-end demo:
#   1. stream tokens over SSE,
#   2. repeat the same prefix and show KV-cache reuse,
#   3. kill the worker holding that prefix and show failover + retry.
#
# Self-contained: starts two mock workers and a gateway, then cleans up.
# No Docker, GPU, or model download required.
set -euo pipefail
cd "$(dirname "$0")/.."

HTTP="http://127.0.0.1:8080"
METRICS="http://127.0.0.1:9091/metrics"
PROMPT="Explain why KV-cache-aware routing lowers time to first token for repeated prefixes."
TMP="$(mktemp -d)"
HEADERS="$TMP/headers"

line() { printf '\n\033[1m%s\033[0m\n' "$*"; }

need() {
  command -v "$1" >/dev/null 2>&1 || { echo "missing dependency: $1" >&2; exit 1; }
}
need curl
need python3

if command -v lsof >/dev/null 2>&1; then
  for port in 8080 9090 9091 9101 9102; do
    if lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
      echo "port $port is already in use; stop the other gateway/workers first" >&2
      exit 1
    fi
  done
fi

make build >/dev/null

pids=()
cleanup() {
  for pid in "${pids[@]:-}"; do kill "$pid" 2>/dev/null || true; done
  wait 2>/dev/null || true
  rm -rf "$TMP"
}
trap cleanup EXIT INT TERM

start_worker() {
  local id="$1" port="$2" rate="$3"
  WORKER_ID="$id" WORKER_GRPC_ADDR=":$port" WORKER_MODEL=workhorse \
    WORKER_MODE=mock WORKER_CAPACITY=8 WORKER_TOKENS_PER_SEC="$rate" \
    ./bin/worker >"$TMP/$id.log" 2>&1 &
  pids+=("$!")
}

echo "Starting 2 mock workers + gateway..."
start_worker worker-1 9101 45
start_worker worker-2 9102 30

WORKER_ADDRS=localhost:9101,localhost:9102 \
GATEWAY_HTTP_ADDR=:8080 GATEWAY_GRPC_ADDR=:9090 GATEWAY_METRICS_ADDR=:9091 \
GATEWAY_TENANTS=dev-key:default \
TENANT_RATE=5000 TENANT_BURST=5000 MAX_IN_FLIGHT=32 QUEUE_SIZE=32 \
HEALTH_PERIOD=5s BREAKER_FAILURE_THRESHOLD=2 BREAKER_COOLDOWN=2s \
./bin/gateway >"$TMP/gateway.log" 2>&1 &
pids+=("$!")

for i in $(seq 1 50); do
  if curl -fsS "$HTTP/healthz" >/dev/null 2>&1; then break; fi
  if [ "$i" -eq 50 ]; then
    echo "gateway did not become healthy; see $TMP/gateway.log" >&2
    exit 1
  fi
  sleep 0.1
done

line "1/3  Streaming tokens over SSE"
curl -sS -N -X POST "$HTTP/v1/chat/completions" \
  -H 'Content-Type: application/json' -H 'Authorization: Bearer dev-key' \
  -d "{\"model\":\"workhorse\",\"max_tokens\":12,\"stream\":true,\"messages\":[{\"role\":\"user\",\"content\":\"$PROMPT\"}]}" \
  | sed -n 's/^data: //p' \
  | python3 -c '
import json, sys
for raw in sys.stdin:
    raw = raw.strip()
    if raw == "[DONE]":
        print("\n[done]")
        break
    try:
        obj = json.loads(raw)
    except ValueError:
        continue
    token = obj.get("choices", [{}])[0].get("delta", {}).get("content", "")
    if token:
        print(token, end="", flush=True)
'

line "2/3  Repeat the same prefix (expect cached_tokens > 0)"
resp=$(curl -sS -D "$HEADERS" -X POST "$HTTP/v1/chat/completions" \
  -H 'Content-Type: application/json' -H 'Authorization: Bearer dev-key' \
  -d "{\"model\":\"workhorse\",\"max_tokens\":8,\"messages\":[{\"role\":\"user\",\"content\":\"$PROMPT\"}]}")
worker=$(printf '%s' "$resp" | python3 -c 'import json,sys; print(json.load(sys.stdin)["routing"]["worker"])')
reason=$(printf '%s' "$resp" | python3 -c 'import json,sys; print(json.load(sys.stdin)["routing"]["reason"])')
cached=$(printf '%s' "$resp" | python3 -c 'import json,sys; print(json.load(sys.stdin)["usage"]["cached_tokens"])')
printf 'routing: worker=%s reason=%s cached_tokens=%s\n' "$worker" "$reason" "$cached"
grep -i '^x-' "$HEADERS" | tr -d '\r' | sed 's/^/  /'

line "3/3  Kill $worker, then retry the same prompt"
case "$worker" in
  worker-1) victim=${pids[0]} ;;
  worker-2) victim=${pids[1]} ;;
  *) echo "unexpected worker: $worker" >&2; exit 1 ;;
esac
kill "$victim" 2>/dev/null || true
for _ in $(seq 1 50); do
  kill -0 "$victim" 2>/dev/null || break
  sleep 0.1
done

before=$(curl -s "$METRICS" | awk '$1 == "gateway_retries_total" {print $2}')
resp2=$(curl -sS -D "$TMP/headers2" -X POST "$HTTP/v1/chat/completions" \
  -H 'Content-Type: application/json' -H 'Authorization: Bearer dev-key' \
  -d "{\"model\":\"workhorse\",\"max_tokens\":8,\"messages\":[{\"role\":\"user\",\"content\":\"$PROMPT\"}]}")
after=$(curl -s "$METRICS" | awk '$1 == "gateway_retries_total" {print $2}')
before=${before:-0}
after=${after:-0}
retries=$((after - before))

if printf '%s' "$resp2" | grep -q '"error"'; then
  echo "request failed after worker kill: $resp2" >&2
  exit 1
fi
worker2=$(printf '%s' "$resp2" | python3 -c 'import json,sys; print(json.load(sys.stdin)["routing"]["worker"])')
reason2=$(printf '%s' "$resp2" | python3 -c 'import json,sys; print(json.load(sys.stdin)["routing"]["reason"])')
printf 'served_by=%s route=%s retries_delta=%s\n' "$worker2" "$reason2" "$retries"
grep -i '^x-' "$TMP/headers2" | tr -d '\r' | sed 's/^/  /'

line "After the failure (waiting for the health pass)"
sleep 6
curl -s "$HTTP/admin/workers" | python3 -c '
import json, sys
for w in json.load(sys.stdin)["workers"]:
    print("  %s: healthy=%s breaker=%s cache_hits=%s" % (
        w["id"], w["healthy"], w["breaker"], w["cache_hits"]))
'
curl -s "$METRICS" | grep -E '^gateway_(retries_total|circuit_state|cache_hits_total|cache_misses_total)' | sed 's/^/  /'

line "Demo complete"
echo "  streaming:     OK"
echo "  prefix reuse:  cached_tokens=$cached"
echo "  failover:      served_by=$worker2 route=$reason2 retries=$retries"

if [ "${cached:-0}" -le 0 ]; then
  echo "FAIL: expected the second request to reuse cached prefix blocks" >&2
  exit 1
fi
if [ "$retries" -lt 1 ]; then
  echo "FAIL: expected at least one retry after killing the cached worker" >&2
  exit 1
fi
