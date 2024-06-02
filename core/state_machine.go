package core

import (
	"context"
	"fmt"
	"log/slog"
	"net"

	"github.com/liznear/gopaxos/core/proto"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

type Executor interface {
	Execute(ctx context.Context, value []byte) error
}

type ExecutorFn func(ctx context.Context, value []byte) error

func (f ExecutorFn) Execute(ctx context.Context, value []byte) error {
	return f(ctx, value)
}

// StateMachine represents the replicated state machine. It implements the MultiPaxos algorithm.
// Users should register their own Handler, which would be called by the state machine when
// a message should be executed.
type StateMachine struct {
	cfg StateMachineConfig

	paxos    *paxosState
	executor Executor
	err      error
	done     chan struct{}
}

func NewStateMachine(opts ...StateMachineOption) (*StateMachine, error) {
	cfg := defaultConfig()
	for _, opt := range opts {
		if err := opt(cfg); err != nil {
			return nil, err
		}
	}
	if cfg.Executor == nil {
		return nil, fmt.Errorf("paxos: executor is required")
	}
	if cfg.Debug {
		slog.SetLogLoggerLevel(slog.LevelDebug)
	}

	state, err := newPaxosState(cfg)
	if err != nil {
		return nil, fmt.Errorf("paxos: fail to create state: %w", err)
	}
	return &StateMachine{
		cfg:      *cfg,
		paxos:    state,
		executor: cfg.Executor,
		done:     make(chan struct{}, 1),
	}, nil
}

func (sm *StateMachine) Start(ctx context.Context) error {
	lis, err := net.Listen("tcp", sm.cfg.Nodes[sm.cfg.ID].DSN)
	if err != nil {
		return fmt.Errorf("paxos: fail to start server: %w", err)
	}
	go func() {
		defer func() {
			sm.done <- struct{}{}
		}()

		g, ctx := errgroup.WithContext(ctx)
		g.Go(func() error {
			return sm.paxos.start(ctx)
		})
		g.Go(func() error {
			s := grpc.NewServer()
			server := &GRPCServer{Handler: sm.paxos}
			proto.RegisterPaxosServer(s, server)
			return s.Serve(lis)
		})
		sm.err = g.Wait()
	}()
	return nil
}

func (sm *StateMachine) Wait(ctx context.Context) error {
	select {
	case <-sm.done:
		return sm.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (sm *StateMachine) Propose(ctx context.Context, value []byte) error {
	return sm.paxos.propose(ctx, value)
}
