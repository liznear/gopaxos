package gopaxos

import (
	"testing"

	"github.com/liznear/gopaxos/proto"
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
			name: "keep committed",
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
					newInstance(10, 2, proto.State_STATE_COMMITTED, []byte("world")),
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
