package gopaxos

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/liznear/gopaxos/proto"
)

func Test_BroadcastAccept(t *testing.T) {
	t.Parallel()
	testcase := []struct {
		name      string
		id        NodeID
		initABN   int64
		peers     map[NodeID]transport
		expectRes bool
		expectABN int64
		expectLE  int64
	}{
		{
			name:    "two rejection node",
			id:      1,
			initABN: 1,
			peers: map[NodeID]transport{
				2: alwaysRejectAcceptFn(10),
				3: alwaysRejectAcceptFn(10),
			},
			expectRes: false,
			expectABN: 10,
			expectLE:  -1,
		},
		{
			name:    "one unreachable node",
			id:      1,
			initABN: 1,
			peers: map[NodeID]transport{
				2: alwaysOKAcceptFn(),
				3: alwaysFailAcceptFn,
			},
			expectRes: true,
			expectABN: 1,
			expectLE:  1,
		},
		{
			// It would win the election but still realized it shouldn't be the leader later.
			name:    "one slow rejection",
			id:      1,
			initABN: 1,
			peers: map[NodeID]transport{
				2: alwaysOKAcceptFn(),
				3: fakeAcceptFn(func(ctx context.Context, req *proto.AcceptRequest) (*proto.AcceptResponse, error) {
					// Assumes this `sleep` makes sure this response arrives later than the node 2.
					time.Sleep(100 * time.Millisecond)
					return &proto.AcceptResponse{ReplyType: proto.ReplyType_REPLY_TYPE_REJECT, Ballot: 10}, nil
				}),
			},
			expectRes: true,
			expectABN: 1,
			expectLE:  1,
		},
	}
	for _, tc := range testcase {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			p := newTestPaxos(tc.id, tc.peers, noOpExecutor)
			p.activeBallot.Store(tc.initABN)
			p.log = newLogWithInstances(
				newInstance(0, tc.initABN, proto.State_STATE_IN_PROGRESS, []byte("hello")),
				newInstance(1, tc.initABN, proto.State_STATE_IN_PROGRESS, []byte("hello")),
			)
			accepted, err := p.broadcastAcceptedInstances(context.Background(), acceptPayload{tc.initABN, [2]instanceID{0, 2}})
			if err != nil {
				t.Fatalf("fail to broadcast: %v", err)
			}
			if accepted != tc.expectRes {
				t.Errorf("Got result %v, want %v", accepted, tc.expectRes)
			}
			if p.activeBallot.Load() != tc.expectABN {
				t.Errorf("Got abn %d, want %d", p.activeBallot.Load(), tc.expectABN)
			}
			if p.lastExecuted.Load() != tc.expectLE {
				t.Errorf("Got le %d, want %d", p.lastExecuted.Load(), tc.expectLE)
			}
		})
	}
}

func Test_HandleAccept(t *testing.T) {
	t.Parallel()
	testcases := []struct {
		name       string
		id         NodeID
		initABN    int64
		req        *proto.AcceptRequest
		expectResp *proto.AcceptResponse
		expectABN  int64
		expectLog  *log
	}{
		{
			name:    "reject",
			id:      1,
			initABN: 2,
			req: &proto.AcceptRequest{
				Ballot: 1,
				Instances: []*proto.Instance{
					newInstance(0, 1, proto.State_STATE_IN_PROGRESS, []byte("hello")),
				},
			},
			expectResp: &proto.AcceptResponse{
				ReplyType: proto.ReplyType_REPLY_TYPE_REJECT,
				Ballot:    2,
			},
			expectABN: 2,
			expectLog: newLog(0),
		},
		{
			name:    "ok",
			id:      1,
			initABN: 2,
			req: &proto.AcceptRequest{
				Ballot: 3,
				Instances: []*proto.Instance{
					newInstance(0, 1, proto.State_STATE_IN_PROGRESS, []byte("hello")),
				},
			},
			expectResp: &proto.AcceptResponse{
				ReplyType: proto.ReplyType_REPLY_TYPE_OK,
			},
			expectABN: 3,
			expectLog: newLogWithInstances(
				newInstance(0, 1, proto.State_STATE_IN_PROGRESS, []byte("hello")),
			),
		},
	}
	for _, tc := range testcases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			p := newTestPaxos(tc.id, nil, noOpExecutor)
			p.activeBallot.Store(tc.initABN)
			resp, err := p.handleAccept(context.Background(), tc.req)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(resp, tc.expectResp) {
				t.Errorf("Got reply %s, want %s", resp, tc.expectResp)
			}
			if p.activeBallot.Load() != tc.expectABN {
				t.Errorf("Got abn %d, want %d", p.activeBallot.Load(), tc.expectABN)
			}
			if p.log.String() != tc.expectLog.String() {
				t.Errorf("Got log %v, want %v", p.log, tc.expectLog)
			}
		})
	}
}
