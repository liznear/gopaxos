package core

import (
	"context"
	"fmt"

	"github.com/liznear/gopaxos/core/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Transport interface {
	Prepare(ctx context.Context, req *proto.PrepareRequest) (*proto.PrepareResponse, error)
	Accept(ctx context.Context, req *proto.AcceptRequest) (*proto.AcceptResponse, error)
	Commit(ctx context.Context, req *proto.CommitRequest) (*proto.CommitResponse, error)
}

type GRPCTransport struct {
	proto.PaxosClient
}

func (t *GRPCTransport) Prepare(ctx context.Context, req *proto.PrepareRequest) (*proto.PrepareResponse, error) {
	return t.PaxosClient.Prepare(ctx, req)
}

func (t *GRPCTransport) Accept(ctx context.Context, req *proto.AcceptRequest) (*proto.AcceptResponse, error) {
	return t.PaxosClient.Accept(ctx, req)
}

func (t *GRPCTransport) Commit(ctx context.Context, req *proto.CommitRequest) (*proto.CommitResponse, error) {
	return t.PaxosClient.Commit(ctx, req)
}

type GRPCServer struct {
	proto.UnimplementedPaxosServer
	Handler
}

func (s *GRPCServer) Prepare(ctx context.Context, req *proto.PrepareRequest) (*proto.PrepareResponse, error) {
	return s.Handler.HandlePrepare(ctx, req), nil
}

func (s *GRPCServer) Accept(ctx context.Context, req *proto.AcceptRequest) (*proto.AcceptResponse, error) {
	return s.Handler.HandleAccept(ctx, req), nil
}

func (s *GRPCServer) Commit(ctx context.Context, req *proto.CommitRequest) (*proto.CommitResponse, error) {
	return s.Handler.HandleCommit(ctx, req), nil
}

var _ proto.PaxosServer = (*GRPCServer)(nil)

func NewTransport(cfg *TransportConfig) (Transport, error) {
	switch cfg.Type {
	case "grpc":
		c, err := grpc.NewClient(cfg.DSN, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, fmt.Errorf("paxos: fail to create grpc client: %w", err)
		}
		return &GRPCTransport{
			PaxosClient: proto.NewPaxosClient(c),
		}, nil
	default:
		return nil, fmt.Errorf("paxos: unknown transport type %q for dsn %q", cfg.Type, cfg.DSN)
	}
}
