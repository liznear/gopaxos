package gopaxos

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/liznear/gopaxos/proto"
)

type NodeID int

type transport interface {
	prepare(ctx context.Context, req *proto.PrepareRequest) (*proto.PrepareResponse, error)
	accept(ctx context.Context, req *proto.AcceptRequest) (*proto.AcceptResponse, error)
	commit(ctx context.Context, req *proto.CommitRequest) (*proto.CommitResponse, error)
}

type node struct {
	id    NodeID
	trans transport
}

type paxosConfig struct {
	commitInterval time.Duration
	maxPeersNumber int64
}

type paxosState struct {
	activeBallot       atomic.Int64
	commitReceived     atomic.Bool
	log                log
	lastExecuted       atomic.Int64
	globalLastExecuted atomic.Int64
}

type paxos struct {
	paxosConfig

	// Cluster Metadata
	id    NodeID
	peers map[NodeID]node

	// Running State
	paxosState

	enterFollower chan struct{}
	enterLeader   chan struct{}
	acceptCh      chan []*proto.Instance
	onCommit      func(context.Context, []byte) error
}
