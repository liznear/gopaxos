package gopaxos

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/liznear/gopaxos/proto"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
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
	maxNodesNumber int64
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
	acceptCh      chan acceptPayload
	onCommit      Executor
}

var _ handler = (*paxos)(nil)

func newPaxos(cfg *stateMachineConfig) (*paxos, error) {
	logger := logrus.New()
	formatter := &logrus.TextFormatter{
		FullTimestamp:   true,
		TimestampFormat: time.RFC3339,
	}
	logger.SetFormatter(formatter)
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
		acceptCh:      make(chan acceptPayload, bufferSize),
		onCommit:      cfg.executor,
	}, nil
}

func (p *paxos) start(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return p.prepareLoop(ctx)
	})
	g.Go(func() error {
		return p.acceptLoop(ctx)
	})
	g.Go(func() error {
		return p.commitLoop(ctx)
	})
	return g.Wait()
}

func (p *paxos) propose(ctx context.Context, value []byte) error {
	abn, isLeader := p.currentBallot()
	if !isLeader {
		return fmt.Errorf("paxos: not the leader")
	}
	inst := &proto.Instance{
		Value: value,
		State: proto.State_STATE_IN_PROGRESS,
	}
	p.log.appendAsLeader(abn, inst)
	select {
	case <-ctx.Done():
		return fmt.Errorf("paxos: stop propose: %w", ctx.Err())
	case p.acceptCh <- acceptPayload{abn, [2]instanceID{instanceID(inst.Id), instanceID(inst.Id + 1)}}:
		// do nothing
	}
	return nil
}

func (p *paxos) broadcast(ctx context.Context, f func(context.Context, transport) (any, error)) chan any {
	// TODO: handle channel close
	resps := make(chan any, len(p.peers))
	for _, peer := range p.peers {
		peer := peer
		go func() {
			rctx, cancel := context.WithTimeout(ctx, p.rpcTimeout)
			defer cancel()
			resp, err := f(rctx, peer.trans)
			if err != nil {
				p.logger.WithField("peer", peer.id).WithError(err).Warn("fail to send request")
				resps <- nil
				return
			}
			resps <- resp
		}()
	}
	return resps
}

func (p *paxos) updateBallot(newBallot int64) (old int64, updated bool) {
	i := 0
	for {
		old, wasLeader := p.currentBallot()
		if old > newBallot {
			return old, false
		}
		if old == newBallot {
			return old, false
		}
		if !p.activeBallot.CompareAndSwap(old, newBallot) {
			// Retry
			i++
			if i > 5 {
				p.logger.WithField("old", old).WithField("new", newBallot).WithField("retry", i).Debug("retry update ballot")
			}
		}

		// Updated to new ballot
		if wasLeader {
			p.enterFollower <- struct{}{}
		}
		if leaderID(newBallot, p.maxNodesNumber) == p.id {
			p.enterLeader <- struct{}{}
		}
		return old, true
	}
}

func (p *paxos) currentBallot() (int64, bool) {
	abn := p.activeBallot.Load()
	return abn, leaderID(abn, p.maxNodesNumber) == p.id
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
