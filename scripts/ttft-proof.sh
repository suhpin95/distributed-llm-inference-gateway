#!/usr/bin/env bash
# Real-inference TTFT proof: one llama.cpp worker (Metal) behind the gateway,
# comparing a cold prompt (full prefill) with an identical warm prompt (KV reuse).
#
#   brew install llama.cpp
#   curl -L -o models/Qwen3-0.6B-Q8_0.gguf \
#     https://huggingface.co/Qwen/Qwen3-0.6B-GGUF/resolve/main/Qwen3-0.6B-Q8_0.gguf
#   bash scripts/ttft-proof.sh
#
# Override the model with LLAMA_MODEL_PATH and the trial count with TTFT_TRIALS.
set -euo pipefail
cd "$(dirname "$0")/.."

MODEL="${LLAMA_MODEL_PATH:-models/Qwen3-0.6B-Q8_0.gguf}"
TRIALS="${TTFT_TRIALS:-5}"
if [[ ! -f "$MODEL" ]]; then
  echo "Model not found: $MODEL" >&2
  echo "Download it from Qwen/Qwen3-0.6B-GGUF (see the header of this script)." >&2
  exit 1
fi
command -v llama-server >/dev/null 2>&1 || {
  echo "llama-server not found: brew install llama.cpp" >&2
  exit 1
}

for port in 8080 8081 9090 9091 9101; do
  if lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
    echo "Port $port is already in use; stop the running stack first." >&2
    exit 1
  fi
done

make build >/dev/null

pids=()
cleanup() {
  for pid in "${pids[@]:-}"; do
    kill "$pid" 2>/dev/null || true
  done
  wait 2>/dev/null || true
}
trap cleanup EXIT INT TERM

echo "Loading $MODEL on :8081 (Metal) ..."
llama-server \
  --model "$MODEL" \
  --host 127.0.0.1 --port 8081 \
  --ctx-size 4096 --n-gpu-layers 99 --parallel 2 \
  >/tmp/inference-gateway-llama.log 2>&1 &
pids+=("$!")

for _ in $(seq 1 240); do
  if curl -sf http://127.0.0.1:8081/health >/dev/null 2>&1; then
    break
  fi
  sleep 0.5
done
if ! curl -sf http://127.0.0.1:8081/health >/dev/null 2>&1; then
  echo "llama.cpp did not become healthy; see /tmp/inference-gateway-llama.log" >&2
  exit 1
fi

WORKER_ID=worker-llama WORKER_GRPC_ADDR=:9101 WORKER_MODEL=workhorse \
WORKER_MODE=llama WORKER_CAPACITY=2 WORKER_MAX_TOKENS=64 \
LLAMA_URL=http://127.0.0.1:8081 \
./bin/worker >/tmp/inference-gateway-worker.log 2>&1 &
pids+=("$!")

sleep 1
WORKER_ADDRS=localhost:9101 \
GATEWAY_HTTP_ADDR=:8080 GATEWAY_GRPC_ADDR=:9090 GATEWAY_METRICS_ADDR=:9091 \
TENANT_RATE=10000 TENANT_BURST=20000 MAX_IN_FLIGHT=16 QUEUE_SIZE=16 \
./bin/gateway >/tmp/inference-gateway-gateway.log 2>&1 &
pids+=("$!")

for _ in $(seq 1 120); do
  if curl -sf http://127.0.0.1:8080/healthz >/dev/null 2>&1; then
    break
  fi
  sleep 0.5
done
if ! curl -sf http://127.0.0.1:8080/healthz >/dev/null 2>&1; then
  echo "gateway did not become healthy; see /tmp/inference-gateway-gateway.log" >&2
  exit 1
fi

# Let the health loop discover the worker before measuring.
sleep 3

python3 scripts/ttft_measure.py --url http://127.0.0.1:8080 --trials "$TRIALS"
