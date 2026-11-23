package gateway

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strings"

	"github.com/example/distributed-llm-gateway/internal/config"
	"github.com/example/distributed-llm-gateway/internal/observability"
	"github.com/example/distributed-llm-gateway/internal/prefix"
	"github.com/example/distributed-llm-gateway/internal/router"
)

type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type chatRequest struct {
	Model       string        `json:"model"`
	Messages    []chatMessage `json:"messages"`
	Prompt      string        `json:"prompt"`
	MaxTokens   int32         `json:"max_tokens"`
	Temperature float64       `json:"temperature"`
	Stream      bool          `json:"stream"`
}

type HTTPServer struct {
	cfg     config.GatewayConfig
	svc     *Service
	router  *router.Router
	metrics *observability.Metrics
}

func NewHTTPServer(cfg config.GatewayConfig, svc *Service, r *router.Router, m *observability.Metrics) *HTTPServer {
	return &HTTPServer{cfg: cfg, svc: svc, router: r, metrics: m}
}

func (h *HTTPServer) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/chat/completions", h.chat)
	mux.HandleFunc("/v1/completions", h.chat)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok\n")) })
	mux.HandleFunc("/admin/workers", h.admin)
	mux.Handle("/metrics", h.metrics.Handler())
	return mux
}

func (h *HTTPServer) chat(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	key := apiKey(r)
	tenant, ok := h.cfg.Tenants[key]
	if !ok {
		writeError(w, http.StatusUnauthorized, "invalid_api_key", "unknown API key")
		return
	}

	var body chatRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 2<<20))
	if err := dec.Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	prompt := body.Prompt
	if prompt == "" {
		parts := make([]string, 0, len(body.Messages))
		for _, m := range body.Messages {
			parts = append(parts, m.Content)
		}
		prompt = strings.Join(parts, "\n")
	}
	if strings.TrimSpace(prompt) == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "prompt or messages required")
		return
	}
	model := body.Model
	if model == "" {
		model = "workhorse"
	}
	maxTokens := body.MaxTokens
	if maxTokens <= 0 {
		maxTokens = 96
	}
	full, blocks := prefix.Hashes(prompt, h.cfg.BlockSize)
	req := Request{
		RequestID: requestID(r), TenantID: tenant, Model: model,
		Prompt: prompt, PrefixHashes: blocks, MaxTokens: maxTokens,
		Temperature: body.Temperature, RoutingPolicy: r.Header.Get("X-Routing-Policy"),
	}

	if body.Stream {
		release, err := h.svc.Preflight(r.Context(), tenant, model)
		if err != nil {
			writeExecutionError(w, err)
			return
		}
		req.PreflightRelease = release
		h.stream(w, r, req, full)
		return
	}

	var buf strings.Builder
	result, err := h.svc.Execute(r.Context(), req, func(token string) error {
		buf.WriteString(token)
		return nil
	})
	if err != nil {
		writeExecutionError(w, err)
		return
	}
	setHeaders(w, result)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"id": "chatcmpl-" + result.RequestID, "object": "chat.completion",
		"model": model, "served_model": result.ServedModel,
		"choices": []any{map[string]any{
			"index": 0, "message": map[string]any{"role": "assistant", "content": buf.String()},
			"finish_reason": "stop",
		}},
		"usage": map[string]any{
			"prompt_tokens":     prefix.EstimateTokens(prompt),
			"completion_tokens": result.OutputTokens,
			"cached_tokens":     result.CachedTokens,
		},
		"routing": map[string]any{"worker": result.WorkerID, "reason": result.Route},
	})
}

func (h *HTTPServer) stream(w http.ResponseWriter, r *http.Request, req Request, full string) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "stream_unavailable", "flushing not supported")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	w.Header().Set("Connection", "keep-alive")
	writeSSEHeader(w, full)

	emit := func(token string) error {
		if err := r.Context().Err(); err != nil {
			return err
		}
		payload, _ := json.Marshal(map[string]any{
			"id": "chatcmpl-" + req.RequestID, "object": "chat.completion.chunk",
			"model": req.Model,
			"choices": []any{map[string]any{
				"index": 0, "delta": map[string]any{"content": token}, "finish_reason": nil,
			}},
		})
		if _, err := w.Write([]byte("data: " + string(payload) + "\n\n")); err != nil {
			return err
		}
		flusher.Flush()
		return nil
	}

	result, err := h.svc.Execute(r.Context(), req, emit)
	if err != nil {
		if errors.Is(err, ErrRateLimited) || errors.Is(err, ErrShed) {
			// Headers are already committed for SSE; signal the error in-band.
			payload, _ := json.Marshal(map[string]any{"error": map[string]any{"message": err.Error()}})
			_, _ = w.Write([]byte("event: error\ndata: " + string(payload) + "\n\n"))
			_, _ = w.Write([]byte("data: [DONE]\n\n"))
			flusher.Flush()
		}
		log.Printf("gateway: stream request %s ended with %v", req.RequestID, err)
		return
	}
	setHeaders(w, result)
	_, _ = w.Write([]byte("data: [DONE]\n\n"))
	flusher.Flush()
}

func writeSSEHeader(w http.ResponseWriter, full string) {
	payload, _ := json.Marshal(map[string]any{
		"id": "chatcmpl-" + full[:min(8, len(full))], "object": "chat.completion.chunk",
		"choices": []any{map[string]any{"index": 0, "delta": map[string]any{"role": "assistant"}, "finish_reason": nil}},
	})
	_, _ = w.Write([]byte("data: " + string(payload) + "\n\n"))
}

func setHeaders(w http.ResponseWriter, r *Result) {
	w.Header().Set("X-Worker-ID", r.WorkerID)
	w.Header().Set("X-Route-Reason", r.Route)
	w.Header().Set("X-Served-Model", r.ServedModel)
	w.Header().Set("X-Cached-Tokens", itoa(r.CachedTokens))
}

func (h *HTTPServer) admin(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	out := []map[string]any{}
	for _, c := range h.router.Workers() {
		stats, _ := c.Snapshot()
		if stats == nil {
			continue
		}
		out = append(out, map[string]any{
			"id": c.ID, "address": c.Addr, "model": c.Model,
			"healthy": c.Healthy(), "breaker": c.Breaker.State(),
			"in_flight": stats.InFlight, "capacity": stats.Capacity,
			"cached_prefixes": stats.CachedPrefixes,
			"cache_hits":      stats.CacheHits, "cache_misses": stats.CacheMisses,
			"tokens_per_second": stats.TokensPerSecond,
		})
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"workers": out})
}

func apiKey(r *http.Request) string {
	if v := strings.TrimSpace(r.Header.Get("Authorization")); strings.HasPrefix(v, "Bearer ") {
		return strings.TrimSpace(strings.TrimPrefix(v, "Bearer "))
	}
	if v := r.Header.Get("X-Tenant-Key"); v != "" {
		return v
	}
	return "dev-key"
}

func requestID(r *http.Request) string {
	if v := r.Header.Get("X-Request-ID"); v != "" {
		return v
	}
	buf := make([]byte, 8)
	_, _ = rand.Read(buf)
	return hex.EncodeToString(buf)
}

func writeExecutionError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrRateLimited):
		w.Header().Set("Retry-After", "1")
		writeError(w, http.StatusTooManyRequests, "rate_limit_exceeded", err.Error())
	case errors.Is(err, ErrShed):
		w.Header().Set("Retry-After", "1")
		w.Header().Set("X-Shed-Reason", "admission")
		writeError(w, http.StatusTooManyRequests, "overloaded", err.Error())
	case errors.Is(err, ErrNoWorker):
		writeError(w, http.StatusServiceUnavailable, "no_backend", err.Error())
	default:
		writeError(w, http.StatusInternalServerError, "internal_error", err.Error())
	}
}

func writeError(w http.ResponseWriter, code int, kind, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]any{"type": kind, "message": msg},
	})
}

func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	buf := [24]byte{}
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
