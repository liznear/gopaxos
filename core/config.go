package core

import "time"

type StateMachineConfig struct {
	ID       NodeID
	Debug    bool
	Executor Executor
	Nodes    map[NodeID]*TransportConfig

	CommitInterval time.Duration
	MaxPeerNumber  int
}

func defaultConfig() *StateMachineConfig {
	return &StateMachineConfig{
		Nodes:          make(map[NodeID]*TransportConfig),
		CommitInterval: 5 * time.Second,
		MaxPeerNumber:  9,
	}
}

type StateMachineOption func(*StateMachineConfig) error

func WithExecutor(e Executor) StateMachineOption {
	return func(config *StateMachineConfig) error {
		config.Executor = e
		return nil
	}
}

func WithID(id NodeID) StateMachineOption {
	return func(config *StateMachineConfig) error {
		config.ID = id
		return nil
	}
}

func WithDebugMode() StateMachineOption {
	return func(config *StateMachineConfig) error {
		config.Debug = true
		return nil
	}
}

func WithPeer(id NodeID, typ, dsn string) StateMachineOption {
	return func(config *StateMachineConfig) error {
		config.Nodes[id] = &TransportConfig{
			Type: typ,
			DSN:  dsn,
		}
		return nil
	}
}

func WithCommitInterval(d time.Duration) StateMachineOption {
	return func(config *StateMachineConfig) error {
		config.CommitInterval = d
		return nil
	}
}

func WithMaxPeers(n int) StateMachineOption {
	return func(config *StateMachineConfig) error {
		config.MaxPeerNumber = n
		return nil
	}
}

type TransportConfig struct {
	Type string
	DSN  string
}
