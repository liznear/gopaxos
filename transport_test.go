package gopaxos

import (
	"context"

	"github.com/liznear/gopaxos/proto"
)

type fakePrepareFn func(ctx context.Context, req *proto.PrepareRequest) (*proto.PrepareResponse, error)

// Implement the transport interface for fakePrepareFn
func (f fakePrepareFn) prepare(ctx context.Context, req *proto.PrepareRequest) (*proto.PrepareResponse, error) {
	return f(ctx, req)
}

func (f fakePrepareFn) accept(ctx context.Context, req *proto.AcceptRequest) (*proto.AcceptResponse, error) {
	return nil, nil
}

func (f fakePrepareFn) commit(ctx context.Context, req *proto.CommitRequest) (*proto.CommitResponse, error) {
	return nil, nil
}

var _ transport = fakePrepareFn(nil)
