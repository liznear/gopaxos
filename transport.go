package gopaxos

import (
	"context"
	"fmt"

	"github.com/liznear/gopaxos/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type grpcTransport struct {
	proto.PaxosClient
}

func (t *grpcTransport) prepare(ctx context.Context, req *proto.PrepareRequest) (*proto.PrepareResponse, error) {
	return t.Prepare(ctx, req)
}

func (t *grpcTransport) accept(ctx context.Context, req *proto.AcceptRequest) (*proto.AcceptResponse, error) {
	return t.Accept(ctx, req)
}

func (t *grpcTransport) commit(ctx context.Context, req *proto.CommitRequest) (*proto.CommitResponse, error) {
	return t.Commit(ctx, req)
}

var _ transport = (*grpcTransport)(nil)

type handler interface {
	handlePrepare(ctx context.Context, req *proto.PrepareRequest) *proto.PrepareResponse
	handleAccept(ctx context.Context, req *proto.AcceptRequest) *proto.AcceptResponse
	handleCommit(ctx context.Context, req *proto.CommitRequest) *proto.CommitResponse
}

type grpcServer struct {
	proto.UnimplementedPaxosServer
	handler
}

func (s *grpcServer) Prepare(ctx context.Context, req *proto.PrepareRequest) (*proto.PrepareResponse, error) {
	return s.handler.handlePrepare(ctx, req), nil
}

func (s *grpcServer) Accept(ctx context.Context, req *proto.AcceptRequest) (*proto.AcceptResponse, error) {
	return s.handler.handleAccept(ctx, req), nil
}

func (s *grpcServer) Commit(ctx context.Context, req *proto.CommitRequest) (*proto.CommitResponse, error) {
	return s.handler.handleCommit(ctx, req), nil
}

var _ proto.PaxosServer = (*grpcServer)(nil)

func newGrpcTransport(addr string) (*grpcTransport, error) {
	c, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("transport: fail to create grpc client: %w", err)
	}
	return &grpcTransport{PaxosClient: proto.NewPaxosClient(c)}, nil
}
