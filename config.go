package gopaxos

import "time"

type stateMachineConfig struct {
	paxosConfig

	executor Executor
	debug    bool
	nodes    map[NodeID]string
}

func defaultConfig() *stateMachineConfig {
	const (
		defaultCommitInterval = 5 * time.Second
		defaultMaxNodesNumber = 9
		defaultRpcTimeout     = 2 * time.Second
	)
	return &stateMachineConfig{
		paxosConfig: paxosConfig{
			commitInterval: defaultCommitInterval,
			maxNodesNumber: defaultMaxNodesNumber,
			rpcTimeout:     defaultRpcTimeout,
		},
		nodes: make(map[NodeID]string),
	}
}

type StateMachineOption func(*stateMachineConfig) error

func WithExecutor(e Executor) StateMachineOption {
	return func(config *stateMachineConfig) error {
		config.executor = e
		return nil
	}
}

func WithID(id NodeID) StateMachineOption {
	return func(config *stateMachineConfig) error {
		config.id = id
		return nil
	}
}

func WithDebugMode() StateMachineOption {
	return func(config *stateMachineConfig) error {
		config.debug = true
		return nil
	}
}

func WithNode(id NodeID, addr string) StateMachineOption {
	return func(config *stateMachineConfig) error {
		config.nodes[id] = addr
		return nil
	}
}

func WithCommitInterval(d time.Duration) StateMachineOption {
	return func(config *stateMachineConfig) error {
		config.commitInterval = d
		return nil
	}
}

func WithMaxNodes(n int) StateMachineOption {
	return func(config *stateMachineConfig) error {
		config.maxNodesNumber = int64(n)
		return nil
	}
}
