package gopaxos

import (
	"fmt"
	"strings"

	"github.com/liznear/gopaxos/proto"
)

type instanceID int64

type log struct {
	base  instanceID
	insts []*proto.Instance
}

func newLog(base instanceID) *log {
	return &log{
		base:  base,
		insts: nil,
	}
}

// appendAsLeader appends instances to the log as a leader.
//
// The insts are just a list of proposals wrapped as Instance. Only the Value field is used.
func (l *log) appendAsLeader(abn int64, insts ...*proto.Instance) {
	if len(insts) == 0 {
		return
	}
	offset := len(l.insts)
	l.extend(len(l.insts) + len(insts))
	for i, inst := range insts {
		index := offset + i
		inst.Id = int64(l.base) + int64(index)
		inst.Ballot = abn
		inst.State = proto.State_STATE_IN_PROGRESS
		l.insts[index] = inst
	}
}

// appendAsFollower appends instances to the log as a follower.
//
// Although it's named "append", it is possible that existing instances are overwritten.
func (l *log) appendAsFollower(insts ...*proto.Instance) {
	if len(insts) == 0 {
		return
	}
	length := int(insts[len(insts)-1].Id - int64(l.base) + 1)
	l.extend(length)
	for _, inst := range insts {
		index := int(inst.Id - int64(l.base))
		if index < 0 {
			continue
		}
		l.insts[index] = inst
	}
}

// extend extends the capacity of the log to hold more instances.
func (l *log) extend(length int) {
	capacity := cap(l.insts)
	if capacity >= length {
		return
	}
	const initCapacity = 4
	if capacity == 0 {
		capacity = initCapacity
	}
	for capacity < length {
		capacity *= 2
	}
	newInsts := make([]*proto.Instance, capacity)
	copy(newInsts, l.insts)
	l.insts = newInsts
}

func (l *log) String() string {
	lines := []string{fmt.Sprintf("Base: %d\n", l.base)}
	var buf []string
	for i, inst := range l.insts {
		if inst == nil || inst.State == proto.State_STATE_MISSING {
			buf = append(buf, fmt.Sprintf("\t[%d]\t%d: STATE_MISSING\n", i, int64(i)+int64(l.base)))
		} else {
			lines = append(lines, buf...)
			buf = buf[:0]
			lines = append(lines, fmt.Sprintf("\t[%d]\t%d: %s, bn=%d, value=%q\n", i, inst.Id, inst.State.String(), inst.Ballot, string(inst.Value)))
		}
	}
	return strings.Join(lines, "\n")
}
