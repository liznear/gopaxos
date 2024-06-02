package core

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/liznear/gopaxos/core/proto"
	"golang.org/x/sync/errgroup"
)

type NodeID int
type instanceID int64

type peer struct {
	id        NodeID
	transport Transport
}

type log struct {
	base  instanceID
	insts []*proto.Instance
}

func newLog(insts []*proto.Instance) log {
	if len(insts) == 0 {
		return log{}
	}
	return log{
		base:  instanceID(insts[0].Id),
		insts: insts,
	}
}

func (l *log) indexOf(id instanceID) (int, bool) {
	offset := int(id - l.base)
	if offset < 0 || offset >= len(l.insts) {
		return 0, false
	}
	inst := l.insts[offset]
	return offset, inst != nil && inst.State != proto.State_STATE_MISSING
}

type paxosState struct {
	id             NodeID
	commitInterval time.Duration
	maxPeersNum    int64
	peers          map[NodeID]peer

	commitReceived atomic.Bool
	activeBallot   atomic.Int64

	log                log
	lastExecuted       atomic.Int64
	globalLastExecuted atomic.Int64

	enterFollower chan struct{}
	enterLeader   chan struct{}
	acceptCh      chan []*proto.Instance

	onCommit func(context.Context, []byte) error
}

func newPaxosState(cfg *StateMachineConfig) (*paxosState, error) {
	// MaxPeerNumbers = x means that the node IDs are in the range [1, x].
	// We want to make abn = 0 the initial value which means no one is the leader.
	if cfg.ID <= 0 || cfg.ID > NodeID(cfg.MaxPeerNumber) {
		return nil, fmt.Errorf("invalid node id %d", cfg.ID)
	}

	peers := make(map[NodeID]peer)
	for id, tc := range cfg.Nodes {
		if id == cfg.ID {
			continue
		}
		t, err := NewTransport(tc)
		if err != nil {
			return nil, fmt.Errorf("fail to create transport: %w", err)
		}
		peers[id] = peer{
			id:        id,
			transport: t,
		}
	}

	// TODO: benchmark this
	const bufferSize = 100
	p := &paxosState{
		id:                 cfg.ID,
		commitInterval:     cfg.CommitInterval,
		maxPeersNum:        int64(cfg.MaxPeerNumber),
		peers:              peers,
		commitReceived:     atomic.Bool{},
		activeBallot:       atomic.Int64{},
		log:                log{},
		lastExecuted:       atomic.Int64{},
		globalLastExecuted: atomic.Int64{},
		enterFollower:      make(chan struct{}, 1),
		enterLeader:        make(chan struct{}, 1),
		acceptCh:           make(chan []*proto.Instance, bufferSize),
		onCommit:           cfg.Executor.Execute,
	}
	p.lastExecuted.Store(-1)
	p.globalLastExecuted.Store(-1)
	return p, nil
}

func (s *paxosState) start(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		return s.prepareLoop(ctx)
	})
	g.Go(func() error {
		return s.acceptLoop(ctx)
	})
	g.Go(func() error {
		return s.commitLoop(ctx)
	})
	return g.Wait()
}

func (s *paxosState) leaderID() NodeID {
	return NodeID(s.activeBallot.Load() % s.maxPeersNum)
}

func leaderID(abn, maxPeersNum int64) NodeID {
	return NodeID(abn % (maxPeersNum + 1))
}

func (s *paxosState) currentActiveBallot() (abn int64, isLeader bool) {
	abn = s.activeBallot.Load()
	return abn, leaderID(abn, s.maxPeersNum) == s.id
}

func (s *paxosState) updateBallot(ballot int64) (abn int64, updated bool) {
	for {
		abn, _ := s.currentActiveBallot()
		if abn >= ballot {
			return abn, false
		}
		if s.activeBallot.CompareAndSwap(abn, ballot) {
			wasLeader := leaderID(abn, s.maxPeersNum) == s.id
			if wasLeader {
				s.enterFollower <- struct{}{}
			}
			return ballot, true
		}
	}
}

func (s *paxosState) propose(ctx context.Context, value []byte) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case s.acceptCh <- []*proto.Instance{{Value: value}}:
		return nil
	}
}

type Handler interface {
	HandlePrepare(ctx context.Context, req *proto.PrepareRequest) *proto.PrepareResponse
	HandleAccept(ctx context.Context, req *proto.AcceptRequest) *proto.AcceptResponse
	HandleCommit(ctx context.Context, req *proto.CommitRequest) *proto.CommitResponse
}

var _ Handler = (*paxosState)(nil)
