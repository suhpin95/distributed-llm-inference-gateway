// Package worker contains both ends of the worker contract: the gateway's gRPC
// client and the worker agent server (mock and llama.cpp modes).
package worker

import (
	"context"
	"sync"
	"time"

	"github.com/example/distributed-llm-gateway/gen"
	"github.com/example/distributed-llm-gateway/internal/breaker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Client is the gateway-side handle for one backend worker.
type Client struct {
	ID    string
	Addr  string
	Model string

	conn *grpc.ClientConn
	rpc  gen.WorkerServiceClient

	Breaker *breaker.Breaker

	mu       sync.RWMutex
	stats    *gen.WorkerStats
	lastSeen time.Time
}

func Dial(ctx context.Context, id, addr, model string, b *breaker.Breaker) (*Client, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	c := &Client{
		ID: id, Addr: addr, Model: model, conn: conn,
		rpc: gen.NewWorkerServiceClient(conn), Breaker: b,
		stats: &gen.WorkerStats{WorkerId: id, Model: model},
	}
	_, _ = c.RefreshStats(ctx)
	return c, nil
}

func (c *Client) Generate(ctx context.Context, req *gen.GenerateRequest) (gen.WorkerService_GenerateClient, error) {
	return c.rpc.Generate(ctx, req)
}

func (c *Client) RefreshStats(ctx context.Context) (*gen.WorkerStats, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	s, err := c.rpc.Stats(ctx, &gen.StatsRequest{})
	if err == nil && s != nil {
		c.mu.Lock()
		c.stats = s
		c.lastSeen = time.Now()
		c.mu.Unlock()
	}
	return s, err
}

func (c *Client) Snapshot() (*gen.WorkerStats, time.Time) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.stats, c.lastSeen
}

func (c *Client) Healthy() bool {
	_, seen := c.Snapshot()
	return time.Since(seen) < 8*time.Second && c.Breaker.State() != "open"
}

func (c *Client) Close() { _ = c.conn.Close() }
