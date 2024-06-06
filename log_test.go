package gopaxos

import (
	"testing"

	"github.com/liznear/gopaxos/proto"
)

func Test_NewLog(t *testing.T) {
	t.Parallel()
	t.Run("empty insts", func(t *testing.T) {
		t.Parallel()
		log := newLogWithInstances()
		if log.base != 0 {
			t.Errorf("Got base %d, want 0", log.base)
		}
		if log.insts != nil {
			t.Errorf("Got non-nil insts %s, want nil", log)
		}
	})
	t.Run("non-empty insts", func(t *testing.T) {
		t.Parallel()
		log := newLogWithInstances(&proto.Instance{Id: 10})
		if log.base != 10 {
			t.Errorf("Got base %d, want 10", log.base)
		}
		if len(log.insts) != 1 {
			t.Errorf("Got length %d, want 1", len(log.insts))
		}
		if cap(log.insts) != 1 {
			t.Errorf("Got capacity %d, want 1", cap(log.insts))
		}
	})
	t.Run("insts with larger capacity", func(t *testing.T) {
		t.Parallel()
		insts := make([]*proto.Instance, 0, 10)
		insts = append(insts, &proto.Instance{Id: 10})
		log := newLogWithInstances(insts...)
		if log.base != 10 {
			t.Errorf("Got base %d, want 10", log.base)
		}
		if len(log.insts) != 1 {
			t.Errorf("Got length %d, want 1", len(log.insts))
		}
		if cap(log.insts) != 1 {
			t.Errorf("Got capacity %d, want 1", cap(log.insts))
		}
	})
}

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
		tc := tc
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
				newInstance(100, 200, proto.State_STATE_COMMITTED, []byte("hello")),
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
		tc := tc
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
					newInstance(0, 1, proto.State_STATE_IN_PROGRESS, []byte("hello")),
				},
			},
			insts: []*proto.Instance{
				newInstance(3, 1, proto.State_STATE_COMMITTED, []byte("world")),
			},
			expected: &log{
				base: 0,
				insts: []*proto.Instance{
					newInstance(0, 1, proto.State_STATE_IN_PROGRESS, []byte("hello")),
					nil, nil,
					newInstance(3, 1, proto.State_STATE_COMMITTED, []byte("world")),
				},
			},
		},
		{
			name: "ignore instances before base",
			log: &log{
				base: 5,
				insts: []*proto.Instance{
					newInstance(5, 1, proto.State_STATE_IN_PROGRESS, []byte("hello")),
				},
			},
			insts: []*proto.Instance{
				newInstance(4, 1, proto.State_STATE_COMMITTED, []byte("world")),
			},
			expected: &log{
				base: 5,
				insts: []*proto.Instance{
					newInstance(5, 1, proto.State_STATE_IN_PROGRESS, []byte("hello")),
				},
			},
		},
		{
			name: "append multiple instances",
			log: &log{
				base: 5,
				insts: []*proto.Instance{
					newInstance(5, 1, proto.State_STATE_IN_PROGRESS, []byte("hello")),
				},
			},
			insts: []*proto.Instance{
				newInstance(4, 1, proto.State_STATE_COMMITTED, []byte("world")),
				newInstance(5, 1, proto.State_STATE_COMMITTED, []byte("say")),
				newInstance(6, 1, proto.State_STATE_COMMITTED, []byte("goodbye")),
			},
			expected: &log{
				base: 5,
				insts: []*proto.Instance{
					newInstance(5, 1, proto.State_STATE_COMMITTED, []byte("say")),
					newInstance(6, 1, proto.State_STATE_COMMITTED, []byte("goodbye")),
				},
			},
		},
	}
	for _, tc := range testcases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.log.appendAsFollower(tc.insts...)
			if tc.log.String() != tc.expected.String() {
				t.Errorf("Got %s, want %s", tc.log, tc.expected)
			}
		})
	}
}

func newInstance(id instanceID, ballot int64, state proto.State, value []byte) *proto.Instance {
	return &proto.Instance{
		Id:     int64(id),
		Ballot: ballot,
		State:  state,
		Value:  value,
	}
}
