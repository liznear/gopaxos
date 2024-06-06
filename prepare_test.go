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
						{
							Id:     10,
							State:  proto.State_STATE_IN_PROGRESS,
							Ballot: 1,
							Value:  []byte("hello"),
						},
					},
				},
			},
			pbn: 2,
			expected: &log{
				base: 10,
				insts: []*proto.Instance{
					{
						Id:     10,
						State:  proto.State_STATE_IN_PROGRESS,
						Ballot: 2,
						Value:  []byte("hello"),
					},
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
