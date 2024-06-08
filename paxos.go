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
	rpcTimeout     time.Duration
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
	logger := logrus.New()
	if cfg.debug {
		logger.SetLevel(logrus.DebugLevel)
	}
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
		logger:        logger.WithField("id", cfg.id),
		paxosConfig:   cfg.paxosConfig,
		peers:         peers,
		paxosState:    newPaxosState(),
		enterFollower: make(chan struct{}, 1),
		enterLeader:   make(chan struct{}, 1),
		acceptCh:      make(chan []*proto.Instance, bufferSize),
		onCommit:      cfg.executor,
	}, nil
}

func (p *paxos) broadcast(ctx context.Context, f func(context.Context, transport) (any, error)) chan any {
	resps := make(chan any, len(p.peers))
	for _, peer := range p.peers {
		peer := peer
		go func() {
			rctx, cancel := context.WithTimeout(ctx, p.rpcTimeout)
			defer cancel()
			resp, err := f(rctx, peer.trans)
			if err != nil {
				p.logger.WithField("peer", peer.id).WithError(err).Errorf("fail to send request")
				resps <- nil
			}
			resps <- resp
		}()
	}
	return resps
}

func (p *paxos) currentBallot() (int64, bool) {
	abn := p.activeBallot.Load()
	return abn, leaderID(abn, p.maxPeersNumber) == p.id
}

func leaderID(abn, maxPeersNumber int64) NodeID {
	if abn == 0 {
		return 0
	}
	id := abn % maxPeersNumber
	if id != 0 {
		return NodeID(id)
	}
	return NodeID(maxPeersNumber)
}
