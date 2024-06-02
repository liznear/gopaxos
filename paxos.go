package gopaxos

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/liznear/gopaxos/proto"
	"github.com/sirupsen/logrus"
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
	id             NodeID
	commitInterval time.Duration
	maxPeersNumber int64
}

type paxosState struct {
	activeBallot       atomic.Int64
	commitReceived     atomic.Bool
	log                *log
	lastExecuted       atomic.Int64
	globalLastExecuted atomic.Int64
}

func newPaxosState() *paxosState {
	ps := &paxosState{
		log: newLog(0),
	}
	ps.lastExecuted.Store(-1)
	ps.globalLastExecuted.Store(-1)
	return ps
}

type paxos struct {
	logger *logrus.Entry

	// Cluster Metadata
	paxosConfig
	peers map[NodeID]node

	// Running State
	*paxosState

	enterFollower chan struct{}
	enterLeader   chan struct{}
	acceptCh      chan []*proto.Instance
	onCommit      Executor
}

func newPaxos(cfg *stateMachineConfig) (*paxos, error) {
	logger := logrus.WithField("id", cfg.id)
	peers := make(map[NodeID]node, len(cfg.nodes)-1)
	for id, addr := range cfg.nodes {
		if id == cfg.id {
			continue
		}
		trans, err := newGrpcTransport(addr)
		if err != nil {
			return nil, fmt.Errorf("fail to create transport for node %d = %q: %w", id, addr, err)
		}
		peers[id] = node{id, trans}
	}

	const bufferSize = 100
	return &paxos{
		logger:        logger,
		paxosConfig:   cfg.paxosConfig,
		peers:         peers,
		paxosState:    newPaxosState(),
		enterFollower: make(chan struct{}, 1),
		enterLeader:   make(chan struct{}, 1),
		acceptCh:      make(chan []*proto.Instance, bufferSize),
		onCommit:      cfg.executor,
	}, nil
}
