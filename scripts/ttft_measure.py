#!/usr/bin/env python3
"""Measure cold vs warm time-to-first-token through the gateway.

Each trial sends a long, never-before-seen prompt (cold) and then repeats it
(warm). The cold request pays full prompt prefill; the warm request should reuse
llama.cpp's KV cache via cache_prompt and the gateway's prefix affinity.

Only the standard library is used so the proof runs anywhere Python 3 does.
"""

import argparse
import http.client
import json
import statistics
import sys
import time
import urllib.parse

SHARED_BODY = (
    "The gateway keeps a consistent hash ring of workers and routes each prompt "
    "to the worker that most likely already holds its KV cache. Prefixes are "
    "normalized, split into cumulative blocks, and looked up from the most "
    "specific block backwards. A cache hit skips prompt prefill on the backend, "
    "which lowers time to first token for bursty traffic with shared context. "
) * 6


def post(host, port, path, payload, api_key, timeout=180):
    conn = http.client.HTTPConnection(host, port, timeout=timeout)
    body = json.dumps(payload)
    headers = {
        "Content-Type": "application/json",
        "Authorization": "Bearer " + api_key,
    }
    conn.request("POST", path, body=body, headers=headers)
    return conn, conn.getresponse()


def stream_ttft(host, port, prompt, max_tokens, api_key):
    """Return seconds until the first content token, reading the SSE stream."""
    payload = {
        "model": "workhorse",
        "prompt": prompt,
        "max_tokens": max_tokens,
        "stream": True,
    }
    started = time.perf_counter()
    conn, resp = post(host, port, "/v1/chat/completions", payload, api_key)
    try:
        if resp.status != 200:
            raise RuntimeError("HTTP %d: %s" % (resp.status, resp.read()[:300]))
        ttft = None
        while True:
            line = resp.readline()
            if not line:
                break
            if line.startswith(b"data:"):
                data = line[5:].strip()
                if data == b"[DONE]":
                    break
                if ttft is None and data:
                    try:
                        event = json.loads(data)
                    except json.JSONDecodeError:
                        event = None
                    if event:
                        choices = event.get("choices") or []
                        delta = (choices[0].get("delta") or {}) if choices else {}
                        if delta.get("content"):
                            ttft = time.perf_counter() - started
            if line.strip() == b"data: [DONE]":
                break
        if ttft is None:
            raise RuntimeError("stream ended without a content token")
        return ttft
    finally:
        conn.close()


def completion(host, port, prompt, max_tokens, api_key):
    payload = {
        "model": "workhorse",
        "prompt": prompt,
        "max_tokens": max_tokens,
    }
    conn, resp = post(host, port, "/v1/chat/completions", payload, api_key)
    try:
        if resp.status != 200:
            raise RuntimeError("HTTP %d: %s" % (resp.status, resp.read()[:300]))
        return json.loads(resp.read())
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://127.0.0.1:8080")
    parser.add_argument("--trials", type=int, default=5)
    parser.add_argument("--max-tokens", type=int, default=8)
    parser.add_argument("--api-key", default="dev-key")
    args = parser.parse_args()

    parsed = urllib.parse.urlparse(args.url)
    host = parsed.hostname or "127.0.0.1"
    port = parsed.port or 80

    def prompt_for(trial):
        # A unique nonce at the very start guarantees the cold request cannot
        # reuse any prefix from an earlier trial.
        nonce = "trial-%d-%d. " % (trial, time.time_ns())
        return nonce + SHARED_BODY + "\nReply with one short sentence."

    # Warm up Metal kernels and the HTTP path; discard the measurement.
    warmup = prompt_for(-1)
    stream_ttft(host, port, warmup, args.max_tokens, args.api_key)
    stream_ttft(host, port, warmup, args.max_tokens, args.api_key)

    cold, warm = [], []
    print("model: workhorse (Qwen3-0.6B real inference)")
    print()
    print("| Trial | Cold TTFT | Warm TTFT | Reduction |")
    print("|---:|---:|---:|---:|")
    for trial in range(1, args.trials + 1):
        prompt = prompt_for(trial)
        cold_s = stream_ttft(host, port, prompt, args.max_tokens, args.api_key)
        warm_s = stream_ttft(host, port, prompt, args.max_tokens, args.api_key)
        cold.append(cold_s)
        warm.append(warm_s)
        reduction = (1 - warm_s / cold_s) * 100 if cold_s else 0
        print("| %d | %.1f ms | %.1f ms | %.0f%% |" % (
            trial, cold_s * 1000, warm_s * 1000, reduction))

    def pct(values, q):
        ordered = sorted(values)
        idx = min(len(ordered) - 1, max(0, int(round(q * (len(ordered) - 1)))))
        return ordered[idx]

    cold_med = statistics.median(cold)
    warm_med = statistics.median(warm)
    print()
    print("summary (n=%d):" % args.trials)
    print("  cold TTFT median %.1f ms  p95 %.1f ms" % (cold_med * 1000, pct(cold, 0.95) * 1000))
    print("  warm TTFT median %.1f ms  p95 %.1f ms" % (warm_med * 1000, pct(warm, 0.95) * 1000))
    print("  median reduction %.0f%% (%.2fx faster)" % (
        (1 - warm_med / cold_med) * 100, cold_med / warm_med))

    # One non-streaming warm request exposes llama.cpp's cached-token count and
    # the gateway's routing reason in the JSON body.
    probe_prompt = prompt_for(999)
    completion(host, port, probe_prompt, args.max_tokens, args.api_key)  # populate
    warm_probe = completion(host, port, probe_prompt, args.max_tokens, args.api_key)
    usage = warm_probe.get("usage") or {}
    routing = warm_probe.get("routing") or {}
    print()
    print("warm non-streaming probe: cached_tokens=%s route=%s worker=%s" % (
        usage.get("cached_tokens"), routing.get("reason"), routing.get("worker")))

    if cold_med <= 0:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
