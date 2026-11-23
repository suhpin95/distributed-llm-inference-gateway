package gateway

import (
	"errors"

	"github.com/example/distributed-llm-gateway/gen"
	"github.com/example/distributed-llm-gateway/internal/prefix"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type GRPCServer struct {
	gen.UnimplementedGatewayServiceServer
	svc       *Service
	blockSize int
}

func NewGRPCServer(svc *Service, blockSize int) *GRPCServer {
	return &GRPCServer{svc: svc, blockSize: blockSize}
}

func (g *GRPCServer) Generate(req *gen.GenerateRequest, stream gen.GatewayService_GenerateServer) error {
	hashes := req.PrefixHashes
	if len(hashes) == 0 {
		_, hashes = prefix.Hashes(req.Prompt, g.blockSize)
	}
	if req.RequestId == "" {
		req.RequestId = "grpc-" + req.TenantId
	}
	model := req.Model
	if model == "" {
		model = "workhorse"
	}

	var result *Result
	result, err := g.svc.Execute(stream.Context(), Request{
		RequestID: req.RequestId, TenantID: tenantOf(req.TenantId), Model: model,
		Prompt: req.Prompt, PrefixHashes: hashes, MaxTokens: req.MaxTokens,
		Temperature: float64(req.Temperature), RoutingPolicy: req.RoutingPolicy,
	}, func(token string) error {
		return stream.Send(&gen.GenerateResponse{
			Kind: gen.GenerateResponse_TOKEN, Token: token,
		})
	})
	if err != nil {
		switch {
		case errors.Is(err, ErrRateLimited), errors.Is(err, ErrShed):
			return status.Error(codes.ResourceExhausted, err.Error())
		default:
			return status.Error(codes.Unavailable, err.Error())
		}
	}
	return stream.Send(&gen.GenerateResponse{
		Kind: gen.GenerateResponse_DONE, WorkerId: result.WorkerID,
		TokensGenerated: int32(result.OutputTokens), CachedTokens: result.CachedTokens,
		Message: result.Route,
	})
}

func tenantOf(id string) string {
	if id == "" {
		return "default"
	}
	return id
}
