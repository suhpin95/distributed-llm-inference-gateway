package main

import (
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/example/distributed-llm-gateway/gen"
	"github.com/example/distributed-llm-gateway/internal/config"
	"github.com/example/distributed-llm-gateway/internal/worker"
	"google.golang.org/grpc"
)

func main() {
	cfg := config.LoadWorker()
	lis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		log.Fatalf("worker listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	gen.RegisterWorkerServiceServer(grpcServer, worker.NewServer(cfg))
	log.Printf("worker %s starting on %s model=%s mode=%s capacity=%d",
		cfg.ID, cfg.GRPCAddr, cfg.Model, cfg.Mode, cfg.Capacity)

	errCh := make(chan error, 1)
	go func() { errCh <- grpcServer.Serve(lis) }()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	select {
	case err := <-errCh:
		log.Fatalf("worker serve: %v", err)
	case <-sig:
		log.Printf("worker %s shutting down", cfg.ID)
		grpcServer.GracefulStop()
	}
}
