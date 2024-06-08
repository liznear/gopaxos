package gopaxos

import (
	"context"
	"errors"

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

func alwaysOKPrepareFn(insts []*proto.Instance) fakePrepareFn {
	return func(ctx context.Context, req *proto.PrepareRequest) (*proto.PrepareResponse, error) {
		return &proto.PrepareResponse{ReplyType: proto.ReplyType_REPLY_TYPE_OK, Instances: insts}, nil
	}
}
func alwaysRejectPrepareFn(abn int64) fakePrepareFn {
	return func(ctx context.Context, req *proto.PrepareRequest) (*proto.PrepareResponse, error) {
		return &proto.PrepareResponse{ReplyType: proto.ReplyType_REPLY_TYPE_REJECT, Ballot: abn}, nil
	}
}

var alwaysFailPrepareFn = fakePrepareFn(func(ctx context.Context, req *proto.PrepareRequest) (*proto.PrepareResponse, error) {
	return nil, errors.New("unreachable")
})
