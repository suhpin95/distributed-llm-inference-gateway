package worker

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/example/distributed-llm-gateway/gen"
	"github.com/example/distributed-llm-gateway/internal/config"
)

type llamaEvent struct {
	Content      string `json:"content"`
	Stop         bool   `json:"stop"`
	CachedTokens int64  `json:"cached_tokens"`
	Timings      struct {
		CachedTokens int64 `json:"cached_tokens"`
		PromptN      int64 `json:"prompt_n"`
	} `json:"timings"`
}

// streamLlama proxies llama.cpp's streaming /completion endpoint, enabling
// cache_prompt so repeated prefixes reuse KV blocks inside the backend.
func streamLlama(ctx context.Context, cfg config.WorkerConfig, req *gen.GenerateRequest, maxTokens int32, emit func(string) error) (int64, error) {
	body, err := json.Marshal(map[string]any{
		"prompt":       req.Prompt,
		"n_predict":    maxTokens,
		"temperature":  req.Temperature,
		"stream":       true,
		"cache_prompt": true,
		"model":        cfg.LlamaModelName,
	})
	if err != nil {
		return 0, err
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost,
		strings.TrimRight(cfg.LlamaURL, "/")+"/completion", bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	httpReq.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 5 * time.Minute}
	resp, err := client.Do(httpReq)
	if err != nil {
		return 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return 0, fmt.Errorf("llama.cpp returned %s: %s", resp.Status, raw)
	}

	var cached int64
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		payload := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if payload == "" || payload == "[DONE]" {
			break
		}
		var ev llamaEvent
		if err := json.Unmarshal([]byte(payload), &ev); err != nil {
			continue
		}
		if ev.Timings.CachedTokens > cached {
			cached = ev.Timings.CachedTokens
		}
		if ev.CachedTokens > cached {
			cached = ev.CachedTokens
		}
		if ev.Content != "" {
			if err := emit(ev.Content); err != nil {
				return cached, err
			}
		}
		if ev.Stop {
			break
		}
	}
	return cached, scanner.Err()
}
