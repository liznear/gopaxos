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

func (f fakePrepareFn) accept(context.Context, *proto.AcceptRequest) (*proto.AcceptResponse, error) {
	return nil, nil
}

func (f fakePrepareFn) commit(context.Context, *proto.CommitRequest) (*proto.CommitResponse, error) {
	return nil, nil
}

var _ transport = fakePrepareFn(nil)

func alwaysOKPrepareFn(insts []*proto.Instance) fakePrepareFn {
	return func(context.Context, *proto.PrepareRequest) (*proto.PrepareResponse, error) {
		return &proto.PrepareResponse{ReplyType: proto.ReplyType_REPLY_TYPE_OK, Instances: insts}, nil
	}
}
func alwaysRejectPrepareFn(abn int64) fakePrepareFn {
	return func(context.Context, *proto.PrepareRequest) (*proto.PrepareResponse, error) {
		return &proto.PrepareResponse{ReplyType: proto.ReplyType_REPLY_TYPE_REJECT, Ballot: abn}, nil
	}
}

var alwaysFailPrepareFn = fakePrepareFn(func(context.Context, *proto.PrepareRequest) (*proto.PrepareResponse, error) {
	return nil, errors.New("unreachable")
})

type fakeAcceptFn func(ctx context.Context, req *proto.AcceptRequest) (*proto.AcceptResponse, error)

// Implement the transport interface for fakePrepareFn
func (f fakeAcceptFn) prepare(context.Context, *proto.PrepareRequest) (*proto.PrepareResponse, error) {
	return nil, nil
}

func (f fakeAcceptFn) accept(ctx context.Context, req *proto.AcceptRequest) (*proto.AcceptResponse, error) {
	return f(ctx, req)
}

func (f fakeAcceptFn) commit(context.Context, *proto.CommitRequest) (*proto.CommitResponse, error) {
	return nil, nil
}

var _ transport = fakeAcceptFn(nil)

func alwaysOKAcceptFn() fakeAcceptFn {
	return func(context.Context, *proto.AcceptRequest) (*proto.AcceptResponse, error) {
		return &proto.AcceptResponse{ReplyType: proto.ReplyType_REPLY_TYPE_OK}, nil
	}
}
func alwaysRejectAcceptFn(abn int64) fakeAcceptFn {
	return func(context.Context, *proto.AcceptRequest) (*proto.AcceptResponse, error) {
		return &proto.AcceptResponse{ReplyType: proto.ReplyType_REPLY_TYPE_REJECT, Ballot: abn}, nil
	}
}

var alwaysFailAcceptFn = fakeAcceptFn(func(context.Context, *proto.AcceptRequest) (*proto.AcceptResponse, error) {
	return nil, errors.New("unreachable")
})
