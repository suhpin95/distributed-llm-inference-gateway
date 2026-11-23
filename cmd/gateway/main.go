package main

import (
	"context"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/example/distributed-llm-gateway/gen"
	"github.com/example/distributed-llm-gateway/internal/adaptive"
	"github.com/example/distributed-llm-gateway/internal/cache"
	"github.com/example/distributed-llm-gateway/internal/config"
	"github.com/example/distributed-llm-gateway/internal/gateway"
	"github.com/example/distributed-llm-gateway/internal/logkafka"
	"github.com/example/distributed-llm-gateway/internal/observability"
	"github.com/example/distributed-llm-gateway/internal/ratelimit"
	"github.com/example/distributed-llm-gateway/internal/router"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
)

func main() {
	cfg := config.LoadGateway()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var redisClient *redis.Client
	if cfg.RedisAddr != "" {
		client := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr, DialTimeout: time.Second})
		pingCtx, pingCancel := context.WithTimeout(ctx, 2*time.Second)
		if err := client.Ping(pingCtx).Err(); err != nil {
			log.Printf("redis unavailable (%v); using in-memory rate limits and prefix index", err)
			_ = client.Close()
		} else {
			redisClient = client
			log.Printf("redis connected at %s", cfg.RedisAddr)
		}
		pingCancel()
	}

	metrics := observability.New()
	index := cache.NewPrefixIndex(redisClient, cfg.PrefixTTL)
	limiter := ratelimit.New(redisClient, cfg.TenantRate, cfg.TenantBurst)
	admission := adaptive.New(max(1, cfg.MaxInFlight/2), cfg.MaxInFlight, cfg.QueueSize, cfg.QueueWait)
	kafkaLogger := logkafka.New(cfg.KafkaBrokers, cfg.KafkaTopic)
	defer kafkaLogger.Close()

	r := router.New(cfg, index, metrics)
	go r.Start(ctx)
	go syncAdmissionMetrics(ctx, admission, metrics)
	go metrics.StartTPS(ctx.Done())

	svc := gateway.NewService(cfg, r, limiter, admission, metrics, kafkaLogger)
	httpServer := &http.Server{
		Addr: cfg.HTTPAddr, Handler: gateway.NewHTTPServer(cfg, svc, r, metrics).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	metricsServer := &http.Server{Addr: cfg.MetricsAddr, Handler: metrics.Handler(), ReadHeaderTimeout: 5 * time.Second}

	grpcLis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		log.Fatalf("gateway gRPC listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	gen.RegisterGatewayServiceServer(grpcServer, gateway.NewGRPCServer(svc, cfg.BlockSize))

	go func() {
		log.Printf("gateway HTTP on %s (metrics %s), gRPC on %s", cfg.HTTPAddr, cfg.MetricsAddr, cfg.GRPCAddr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("http server: %v", err)
		}
	}()
	go func() {
		if err := metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("metrics server: %v", err)
		}
	}()
	go func() {
		if err := grpcServer.Serve(grpcLis); err != nil {
			log.Printf("grpc server: %v", err)
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Println("gateway shutting down")
	cancel()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer shutdownCancel()
	_ = httpServer.Shutdown(shutdownCtx)
	_ = metricsServer.Shutdown(shutdownCtx)
	grpcServer.GracefulStop()
}

func syncAdmissionMetrics(ctx context.Context, l *adaptive.Limiter, m *observability.Metrics) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			limit, inFlight, queued := l.Stats()
			m.AdmissionLimit.Set(float64(limit))
			m.AdmissionInFlight.Set(float64(inFlight))
			m.AdmissionQueued.Set(float64(queued))
		case <-ctx.Done():
			return
		}
	}
}
