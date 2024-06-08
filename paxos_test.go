package gopaxos

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
)

func Test_LeaderID(t *testing.T) {
	t.Parallel()
	testcases := []struct {
		abn           int64
		maxPeerNumber int64
		expected      NodeID
	}{
		{
			abn:           0,
			maxPeerNumber: 3,
			expected:      0,
		},
		{
			abn:           1,
			maxPeerNumber: 3,
			expected:      1,
		},
		{
			abn:           3,
			maxPeerNumber: 3,
			expected:      3,
		},
		{
			abn:           13,
			maxPeerNumber: 3,
			expected:      1,
		},
	}
	for _, tc := range testcases {
		tc := tc
		t.Run(fmt.Sprintf("abn=%d,maxPeerNumber=%d", tc.abn, tc.maxPeerNumber), func(t *testing.T) {
			t.Parallel()
			got := leaderID(tc.abn, tc.maxPeerNumber)
			if got != tc.expected {
				t.Errorf("Got %d, want %d", got, tc.expected)
			}
		})
	}
}

func newTestPaxos(id NodeID, trans map[NodeID]transport, e Executor) *paxos {
	peers := make(map[NodeID]node, len(trans))
	for id, t := range trans {
		peers[id] = node{id: id, trans: t}
	}
	return &paxos{
		logger: logrus.New().WithField("id", id),
		paxosConfig: paxosConfig{
			id:             id,
			commitInterval: 100 * time.Millisecond,
			maxPeersNumber: 3,
			rpcTimeout:     time.Second,
		},
		peers:         peers,
		paxosState:    newPaxosState(),
		enterFollower: make(chan struct{}, 1),
		enterLeader:   make(chan struct{}, 1),
		acceptCh:      make(chan acceptPayload, 10),
		onCommit:      e,
	}
}

var noOpExecutor = ExecutorFn(func(context.Context, []byte) error { return nil })
