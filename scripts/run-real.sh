#!/usr/bin/env bash
# Real local inference path: one llama.cpp worker (Metal) + two mock replicas,
# all behind the same gateway. Set LLAMA_MODEL_PATH to a GGUF file first.
#
#   brew install llama.cpp
#   export LLAMA_MODEL_PATH=/path/to/Qwen3-8B-Q4_K_M.gguf
#   make run-real
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

MODE="mock"
CAPACITY="8"
if [[ -n "${LLAMA_MODEL_PATH:-}" ]]; then
  MODE="llama"
  CAPACITY="2"
  command -v llama-server >/dev/null 2>&1 || { echo "llama-server not found: brew install llama.cpp" >&2; exit 1; }
  llama-server \
    --model "$LLAMA_MODEL_PATH" \
    --host 127.0.0.1 --port 8081 \
    --ctx-size 4096 --n-gpu-layers 99 --parallel 2 \
    >/tmp/inference-gateway-llama.log 2>&1 &
  pids+=("$!")
  echo "Starting llama.cpp on :8081 (log: /tmp/inference-gateway-llama.log)"
  sleep 5
else
  echo "LLAMA_MODEL_PATH is unset: running the deterministic mock path only." >&2
  echo "Set it to a Qwen3 8B Q4_K_M GGUF to enable real local inference." >&2
fi

WORKER_ID=worker-1 WORKER_GRPC_ADDR=:9101 WORKER_MODEL=workhorse \
WORKER_MODE="$MODE" WORKER_CAPACITY="$CAPACITY" WORKER_MAX_TOKENS=96 \
LLAMA_URL=http://127.0.0.1:8081 ./bin/worker &
pids+=("$!")

for spec in "worker-2 9102 35" "worker-3 9103 25"; do
  read -r id port rate <<<"$spec"
  WORKER_ID="$id" WORKER_GRPC_ADDR=":$port" WORKER_MODEL=workhorse \
  WORKER_MODE=mock WORKER_CAPACITY=8 WORKER_TOKENS_PER_SEC="$rate" ./bin/worker &
  pids+=("$!")
done

sleep 1
WORKER_ADDRS="localhost:9101,localhost:9102,localhost:9103" ./bin/gateway &
pids+=("$!")
echo "Gateway ready on :8080 (metrics :9091). Llama log: /tmp/inference-gateway-llama.log"
wait
