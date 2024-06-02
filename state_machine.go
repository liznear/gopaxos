package gopaxos

import (
	"context"
	"fmt"
)

type Executor interface {
	Execute(ctx context.Context, value []byte) error
}

type ExecutorFn func(ctx context.Context, value []byte) error

func (f ExecutorFn) Execute(ctx context.Context, value []byte) error {
	return f(ctx, value)
}

type StateMachine struct {
	paxos *paxos
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
	return &StateMachine{paxos: paxos}, nil
}
