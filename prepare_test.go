package gopaxos

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/liznear/gopaxos/proto"
	"golang.org/x/sync/errgroup"
)

func Test_MergeLogs(t *testing.T) {
	t.Parallel()
	testcases := []struct {
		name     string
		logs     []*log
		pbn      int64
		expected *log
	}{
		{
			name:     "empty",
			logs:     []*log{},
			pbn:      1,
			expected: &log{},
		},
		{
			name: "one log",
			logs: []*log{
				{
					base: 10,
					insts: []*proto.Instance{
						newInstance(10, 1, proto.State_STATE_IN_PROGRESS, []byte("hello")),
					},
				},
			},
			pbn: 2,
			expected: &log{
				base: 10,
				insts: []*proto.Instance{
					newInstance(10, 2, proto.State_STATE_IN_PROGRESS, []byte("hello")),
				},
			},
		},
		{
			name: "keep committed but change to in-progress",
			logs: []*log{
				{
					base: 10,
					insts: []*proto.Instance{
						newInstance(10, 1, proto.State_STATE_IN_PROGRESS, []byte("hello")),
					},
				},
				{
					base: 10,
					insts: []*proto.Instance{
						newInstance(10, 1, proto.State_STATE_COMMITTED, []byte("world")),
					},
				},
			},
			pbn: 2,
			expected: &log{
				base: 10,
				insts: []*proto.Instance{
					newInstance(10, 2, proto.State_STATE_IN_PROGRESS, []byte("world")),
				},
			},
		},
		{
			name: "keep higher ballot",
			logs: []*log{
				{
					base: 10,
					insts: []*proto.Instance{
						newInstance(10, 1, proto.State_STATE_IN_PROGRESS, []byte("hello")),
					},
				},
				{
					base: 10,
					insts: []*proto.Instance{
						newInstance(10, 2, proto.State_STATE_IN_PROGRESS, []byte("world")),
					},
				},
			},
			pbn: 3,
			expected: &log{
				base: 10,
				insts: []*proto.Instance{
					newInstance(10, 3, proto.State_STATE_IN_PROGRESS, []byte("world")),
				},
			},
		},
		{
			name: "different bases and gaps",
			logs: []*log{
				{
					base: 10,
					insts: []*proto.Instance{
						newInstance(10, 1, proto.State_STATE_IN_PROGRESS, []byte("hello")),
					},
				},
				{
					base: 13,
					insts: []*proto.Instance{
						newInstance(13, 2, proto.State_STATE_IN_PROGRESS, []byte("world")),
					},
				},
			},
			pbn: 3,
			expected: &log{
				base: 10,
				insts: []*proto.Instance{
					newInstance(10, 3, proto.State_STATE_IN_PROGRESS, []byte("hello")),
					nil, nil, // 11 & 12
					newInstance(13, 3, proto.State_STATE_IN_PROGRESS, []byte("world")),
				},
			},
		},
	}

	for _, tc := range testcases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result := mergeLogs(tc.pbn, tc.logs)
			if result.String() != tc.expected.String() {
				t.Errorf("Got %s, want %s", result, tc.expected)
			}
		})
	}
}

func Test_NextPrepareBallot(t *testing.T) {
	t.Parallel()
	testcase := []struct {
		name     string
		id       NodeID
		abn      int64
		maxPeers int64
		expected int64
	}{
		{
			name:     "init",
			id:       1,
			abn:      0,
			maxPeers: 3,
			expected: 1,
		},
		{
			name:     "ongoing",
			id:       1,
			abn:      9, // prev leader is 3
			maxPeers: 3,
			expected: 10,
		},
		{
			name:     "non-first node",
			id:       2,
			abn:      9, // prev leader is 3
			maxPeers: 3,
			expected: 11,
		},
		{
			name:     "was leader",
			id:       3,
			abn:      9, // prev leader is 3
			maxPeers: 3,
			expected: 12,
		},
	}
	for _, tc := range testcase {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := nextPrepareBallot(tc.id, tc.abn, tc.maxPeers)
			if got != tc.expected {
				t.Errorf("Got %d, want %d", got, tc.expected)
			}
		})
	}
}

func Test_Election(t *testing.T) {
	t.Parallel()
	testcase := []struct {
		name      string
		id        NodeID
		peers     map[NodeID]transport
		expectRes bool
		expectABN int64
	}{
		{
			name: "two rejection node",
			id:   1,
			peers: map[NodeID]transport{
				2: alwaysRejectPrepareFn(10),
				3: alwaysRejectPrepareFn(10),
			},
			expectRes: false,
			expectABN: 10,
		},
		{
			name: "one unreachable node",
			id:   1,
			peers: map[NodeID]transport{
				2: alwaysOKPrepareFn(nil),
				3: alwaysFailPrepareFn,
			},
			expectRes: true,
			expectABN: 1,
		},
		{
			// It would win the election but still realized it shouldn't be the leader later.
			name: "one slow rejection",
			id:   1,
			peers: map[NodeID]transport{
				2: alwaysOKPrepareFn(nil),
				3: fakePrepareFn(func(ctx context.Context, req *proto.PrepareRequest) (*proto.PrepareResponse, error) {
					// Assumes this `sleep` makes sure this response arrives later than the node 2.
					time.Sleep(100 * time.Millisecond)
					return &proto.PrepareResponse{ReplyType: proto.ReplyType_REPLY_TYPE_REJECT, Ballot: 10}, nil
				}),
			},
			expectRes: true,
			expectABN: 1,
		},
	}
	for _, tc := range testcase {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			p := newTestPaxos(tc.id, tc.peers, noOpExecutor)
			succeed, err := p.election(context.Background())
			if err != nil {
				t.Fatalf("fail to elect: %v", err)
			}
			if succeed != tc.expectRes {
				t.Errorf("Got result %v, want %v", succeed, tc.expectRes)
			}
			if p.activeBallot.Load() != tc.expectABN {
				t.Errorf("Got abn %d, want %d", p.activeBallot.Load(), tc.expectABN)
			}
		})
	}
}

func Test_PrepareLoop_NoElectionWithCommit(t *testing.T) {
	p := newTestPaxos(1, map[NodeID]transport{
		2: alwaysOKPrepareFn(nil),
		3: alwaysOKPrepareFn(nil),
	}, noOpExecutor)

	// leader was 2, since there is commit message, leader won't change
	originalABN := int64(2)
	p.activeBallot.Store(originalABN)

	ctx, cancel := context.WithCancel(context.Background())
	g := errgroup.Group{}
	g.Go(func() error { return p.prepareLoop(ctx) })
	p.commitReceived.Store(true)
	time.Sleep(4 * p.commitInterval)
	cancel()
	if err := g.Wait(); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if p.activeBallot.Load() != originalABN {
		t.Errorf("Got abn %d, want 2", p.activeBallot.Load())
	}
}

func Test_PrepareLoop_ElectionWithoutCommit(t *testing.T) {
	p := newTestPaxos(1, map[NodeID]transport{
		2: alwaysOKPrepareFn(nil),
		3: alwaysOKPrepareFn(nil),
	}, noOpExecutor)

	// leader was 2, since there is no commit message, we should start election and become the new leader.
	originalABN := int64(2)
	p.activeBallot.Store(originalABN)
	wantABN := nextPrepareBallot(p.id, originalABN, p.maxNodesNumber)

	ctx, cancel := context.WithCancel(context.Background())
	g := errgroup.Group{}
	g.Go(func() error { return p.prepareLoop(ctx) })
	time.Sleep(4 * p.commitInterval)
	cancel()
	if err := g.Wait(); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if p.activeBallot.Load() != wantABN {
		t.Errorf("Got abn %d, want %d", p.activeBallot.Load(), wantABN)
	}
}

func Test_HandlePrepare(t *testing.T) {
	t.Parallel()
	testcases := []struct {
		name       string
		id         NodeID
		initABN    int64
		initLogs   []*proto.Instance
		req        *proto.PrepareRequest
		expectResp *proto.PrepareResponse
		expectABN  int64
	}{
		{
			name:    "reject",
			id:      1,
			initABN: 2,
			req: &proto.PrepareRequest{
				Ballot: 1,
			},
			expectResp: &proto.PrepareResponse{
				ReplyType: proto.ReplyType_REPLY_TYPE_REJECT,
				Ballot:    2,
			},
			expectABN: 2,
		},
		{
			name:    "ok",
			id:      1,
			initABN: 2,
			req: &proto.PrepareRequest{
				Ballot: 3,
			},
			expectResp: &proto.PrepareResponse{
				ReplyType: proto.ReplyType_REPLY_TYPE_OK,
				Instances: nil,
			},
			expectABN: 3,
		},
		{
			name:    "ok with instances",
			id:      1,
			initABN: 2,
			initLogs: []*proto.Instance{
				newInstance(0, 2, proto.State_STATE_IN_PROGRESS, []byte("hello")),
			},
			req: &proto.PrepareRequest{
				Ballot: 3,
			},
			expectResp: &proto.PrepareResponse{
				ReplyType: proto.ReplyType_REPLY_TYPE_OK,
				Instances: []*proto.Instance{
					newInstance(0, 2, proto.State_STATE_IN_PROGRESS, []byte("hello")),
				},
			},
			expectABN: 3,
		},
	}
	for _, tc := range testcases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			p := newTestPaxos(tc.id, nil, noOpExecutor)
			p.activeBallot.Store(tc.initABN)
			p.log = newLogWithInstances(tc.initLogs...)
			resp, err := p.handlePrepare(context.Background(), tc.req)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(resp, tc.expectResp) {
				t.Errorf("Got reply %s, want %s", resp, tc.expectResp)
			}
			if p.activeBallot.Load() != tc.expectABN {
				t.Errorf("Got abn %d, want %d", p.activeBallot.Load(), tc.expectABN)
			}
		})
	}
}
