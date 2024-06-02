package gopaxos

import (
	"testing"

	"github.com/liznear/gopaxos/proto"
)

func Test_ExtendLog(t *testing.T) {
	t.Parallel()
	testcases := []struct {
		name             string
		base             instanceID
		targetLength     int
		expectedCapacity int
	}{
		{
			name:             "initial capacity",
			base:             0,
			targetLength:     1,
			expectedCapacity: 4,
		},
		{
			name:             "double capacity",
			base:             0,
			targetLength:     5,
			expectedCapacity: 8,
		},
		{
			name:             "non-zero base",
			base:             11,
			targetLength:     10,
			expectedCapacity: 16,
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			log := newLog(tc.base)
			log.extend(tc.targetLength)
			if cap(log.insts) != tc.expectedCapacity {
				t.Errorf("Got capacity %d, want %d", cap(log.insts), tc.expectedCapacity)
			}
			if len(log.insts) != tc.expectedCapacity {
				t.Errorf("Got length %d, expect length to be same as capacity %d", len(log.insts), tc.expectedCapacity)
			}
		})
	}
}

func Test_AppendToLogAsLeader(t *testing.T) {
	t.Parallel()
	testcases := []struct {
		name     string
		log      *log
		abn      int64
		insts    []*proto.Instance
		expected *log
	}{
		{
			name: "append one to empty",
			log: &log{
				base: 0,
			},
			abn: 1,
			insts: []*proto.Instance{
				{
					Id:     100,
					Ballot: 200,
					State:  proto.State_STATE_COMMITTED,
					Value:  []byte("hello"),
				},
			},
			expected: &log{
				base: 0,
				insts: []*proto.Instance{
					{
						Id:     0,
						Ballot: 1,
						State:  proto.State_STATE_IN_PROGRESS,
						Value:  []byte("hello"),
					},
				},
			},
		},
		{
			name: "append one to non-empty",
			log: &log{
				base: 0,
				insts: []*proto.Instance{
					{
						Id:     0,
						Ballot: 1,
						State:  proto.State_STATE_IN_PROGRESS,
						Value:  []byte("hello"),
					},
				},
			},
			abn: 2,
			insts: []*proto.Instance{
				{
					Id:     100,
					Ballot: 200,
					State:  proto.State_STATE_COMMITTED,
					Value:  []byte("world"),
				},
			},
			expected: &log{
				base: 0,
				insts: []*proto.Instance{
					{
						Id:     0,
						Ballot: 1,
						State:  proto.State_STATE_IN_PROGRESS,
						Value:  []byte("hello"),
					},
					{
						Id:     1,
						Ballot: 2,
						State:  proto.State_STATE_IN_PROGRESS,
						Value:  []byte("world"),
					},
				},
			},
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.log.appendAsLeader(tc.abn, tc.insts...)
			if tc.log.String() != tc.expected.String() {
				t.Errorf("Got %s, want %s", tc.log, tc.expected)
			}
		})
	}
}

func Test_AppendToLogAsFollower(t *testing.T) {
	t.Parallel()
	testcases := []struct {
		name     string
		log      *log
		insts    []*proto.Instance
		expected *log
	}{
		{
			name: "append one to empty",
			log: &log{
				base: 0,
			},
			insts: []*proto.Instance{
				{
					Id:     0,
					Ballot: 1,
					State:  proto.State_STATE_COMMITTED,
					Value:  []byte("hello"),
				},
			},
			expected: &log{
				base: 0,
				insts: []*proto.Instance{
					{
						Id:     0,
						Ballot: 1,
						State:  proto.State_STATE_COMMITTED,
						Value:  []byte("hello"),
					},
				},
			},
		},
		{
			name: "append one to non-empty",
			log: &log{
				base: 0,
				insts: []*proto.Instance{
					{
						Id:     0,
						Ballot: 1,
						State:  proto.State_STATE_IN_PROGRESS,
						Value:  []byte("hello"),
					},
				},
			},
			insts: []*proto.Instance{
				{
					Id:     1,
					Ballot: 1,
					State:  proto.State_STATE_COMMITTED,
					Value:  []byte("world"),
				},
			},
			expected: &log{
				base: 0,
				insts: []*proto.Instance{
					{
						Id:     0,
						Ballot: 1,
						State:  proto.State_STATE_IN_PROGRESS,
						Value:  []byte("hello"),
					},
					{
						Id:     1,
						Ballot: 1,
						State:  proto.State_STATE_COMMITTED,
						Value:  []byte("world"),
					},
				},
			},
		},
		{
			name: "overwrite",
			log: &log{
				base: 0,
				insts: []*proto.Instance{
					{
						Id:     0,
						Ballot: 1,
						State:  proto.State_STATE_IN_PROGRESS,
						Value:  []byte("hello"),
					},
				},
			},
			insts: []*proto.Instance{
				{
					Id:     0,
					Ballot: 1,
					State:  proto.State_STATE_COMMITTED,
					Value:  []byte("world"),
				},
			},
			expected: &log{
				base: 0,
				insts: []*proto.Instance{
					{
						Id:     0,
						Ballot: 1,
						State:  proto.State_STATE_COMMITTED,
						Value:  []byte("world"),
					},
				},
			},
		},
		{
			name: "with gap",
			log: &log{
				base: 0,
				insts: []*proto.Instance{
					{
						Id:     0,
						Ballot: 1,
						State:  proto.State_STATE_IN_PROGRESS,
						Value:  []byte("hello"),
					},
				},
			},
			insts: []*proto.Instance{
				{
					Id:     3,
					Ballot: 1,
					State:  proto.State_STATE_COMMITTED,
					Value:  []byte("world"),
				},
			},
			expected: &log{
				base: 0,
				insts: []*proto.Instance{
					{
						Id:     0,
						Ballot: 1,
						State:  proto.State_STATE_IN_PROGRESS,
						Value:  []byte("hello"),
					},
					nil, nil,
					{
						Id:     3,
						Ballot: 1,
						State:  proto.State_STATE_COMMITTED,
						Value:  []byte("world"),
					},
				},
			},
		},
		{
			name: "ignore instances before base",
			log: &log{
				base: 5,
				insts: []*proto.Instance{
					{
						Id:     5,
						Ballot: 1,
						State:  proto.State_STATE_IN_PROGRESS,
						Value:  []byte("hello"),
					},
				},
			},
			insts: []*proto.Instance{
				{
					Id:     4,
					Ballot: 1,
					State:  proto.State_STATE_COMMITTED,
					Value:  []byte("world"),
				},
			},
			expected: &log{
				base: 5,
				insts: []*proto.Instance{
					{
						Id:     5,
						Ballot: 1,
						State:  proto.State_STATE_IN_PROGRESS,
						Value:  []byte("hello"),
					},
				},
			},
		},
		{
			name: "append multiple instances",
			log: &log{
				base: 5,
				insts: []*proto.Instance{
					{
						Id:     5,
						Ballot: 1,
						State:  proto.State_STATE_IN_PROGRESS,
						Value:  []byte("hello"),
					},
				},
			},
			insts: []*proto.Instance{
				{
					Id:     4,
					Ballot: 1,
					State:  proto.State_STATE_COMMITTED,
					Value:  []byte("world"),
				},
				{
					Id:     5,
					Ballot: 1,
					State:  proto.State_STATE_COMMITTED,
					Value:  []byte("say"),
				},
				{
					Id:     6,
					Ballot: 1,
					State:  proto.State_STATE_COMMITTED,
					Value:  []byte("goodbye"),
				},
			},
			expected: &log{
				base: 5,
				insts: []*proto.Instance{
					{
						Id:     5,
						Ballot: 1,
						State:  proto.State_STATE_COMMITTED,
						Value:  []byte("say"),
					},
					{
						Id:     6,
						Ballot: 1,
						State:  proto.State_STATE_COMMITTED,
						Value:  []byte("goodbye"),
					},
				},
			},
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.log.appendAsFollower(tc.insts...)
			if tc.log.String() != tc.expected.String() {
				t.Errorf("Got %s, want %s", tc.log, tc.expected)
			}
		})
	}
}
