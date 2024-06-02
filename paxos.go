package gopaxos

import (
	"context"

	"github.com/liznear/gopaxos/proto"
)

type NodeID int
type instanceID int64

type transport interface {
	prepare(ctx context.Context, req *proto.PrepareRequest) (*proto.PrepareResponse, error)
	accept(ctx context.Context, req *proto.AcceptRequest) (*proto.AcceptResponse, error)
	commit(ctx context.Context, req *proto.CommitRequest) (*proto.CommitResponse, error)
}

type node struct {
	id    NodeID
	trans transport
}

type paxos struct {
}
