package gopaxos

import (
	"context"
	"fmt"
	"net"

	"github.com/liznear/gopaxos/proto"
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

type StateMachine struct {
	*stateMachineConfig

	paxos *paxos
	done  chan struct{}
	err   error
}

func NewStateMachine(opts ...StateMachineOption) (*StateMachine, error) {
	config := defaultConfig()
	for _, opt := range opts {
		if err := opt(config); err != nil {
			return nil, err
		}
	}
	paxos, err := newPaxos(config)
	if err != nil {
		return nil, fmt.Errorf("paxos: fail to create state machine: %w", err)
	}
	return &StateMachine{
		stateMachineConfig: config,
		paxos:              paxos,
		done:               make(chan struct{}, 1),
	}, nil
}

func (sm *StateMachine) Start(ctx context.Context) error {
	lis, err := net.Listen("tcp", sm.nodes[sm.id])
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
			server := &grpcServer{handler: sm.paxos}
			proto.RegisterPaxosServer(s, server)
			return s.Serve(lis)
		})
		sm.err = g.Wait()
	}()
	return nil
}

func (sm *StateMachine) Wait() error {
	<-sm.done
	return sm.err
}

func (sm *StateMachine) Propose(ctx context.Context, value []byte) error {
	return sm.paxos.propose(ctx, value)
}
