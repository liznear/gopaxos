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
