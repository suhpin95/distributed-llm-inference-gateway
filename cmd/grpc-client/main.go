package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/example/distributed-llm-gateway/gen"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	addr := flag.String("addr", "localhost:9090", "gateway gRPC address")
	model := flag.String("model", "workhorse", "model tier")
	prompt := flag.String("prompt", "Hello from the native gRPC client", "prompt text")
	tokens := flag.Int("tokens", 12, "max tokens")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	stream, err := gen.NewGatewayServiceClient(conn).Generate(ctx, &gen.GenerateRequest{
		RequestId: "grpc-demo", TenantId: "default", Model: *model,
		Prompt: *prompt, MaxTokens: int32(*tokens),
	})
	if err != nil {
		log.Fatal(err)
	}
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			return
		}
		if err != nil {
			log.Fatal(err)
		}
		switch resp.Kind {
		case gen.GenerateResponse_TOKEN:
			fmt.Print(resp.Token)
		case gen.GenerateResponse_DONE:
			fmt.Printf("\n[worker=%s tokens=%d cached=%d route=%s]\n",
				resp.WorkerId, resp.TokensGenerated, resp.CachedTokens, resp.Message)
			return
		case gen.GenerateResponse_ERROR:
			log.Fatal(resp.Message)
		}
	}
}
