package gopaxos

import (
	"context"

	"github.com/liznear/gopaxos/proto"
	"github.com/sirupsen/logrus"
)

func newTestPaxos(id NodeID, trans map[NodeID]transport, e Executor) *paxos {
	peers := make(map[NodeID]node, len(trans))
	for id, t := range trans {
		peers[id] = node{id: id, trans: t}
	}
	return &paxos{
		logger: logrus.New().WithField("id", id),
		paxosConfig: paxosConfig{
			id:             id,
			commitInterval: 10,
			maxPeersNumber: 3,
			rpcTimeout:     1,
		},
		peers:         peers,
		paxosState:    newPaxosState(),
		enterFollower: make(chan struct{}, 1),
		enterLeader:   make(chan struct{}, 1),
		acceptCh:      make(chan []*proto.Instance, 10),
		onCommit:      e,
	}
}

var noOpExecutor = ExecutorFn(func(context.Context, []byte) error { return nil })
