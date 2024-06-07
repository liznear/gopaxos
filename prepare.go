package gopaxos

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/liznear/gopaxos/proto"
)

// prepareLoop is the main loop for the prepare phase.
//
// In this loop, each node checks if it receives new commit messages from the leader. If
// yes, the leader is still alive. The node should stay as a follower. Otherwise, the node
// should start a new round of election by sending prepare messages to all peers.
func (p *paxos) prepareLoop(ctx context.Context) error {
	var (
		waitForNextCommit = 3 * p.commitInterval
	)
	for {
		if p.commitReceived.Swap(false) {
			select {
			case <-ctx.Done():
				return fmt.Errorf("paxos: stop prepare loop: %w", ctx.Err())
			case <-time.After(waitForNextCommit):
				continue
			}
		}
		p.logger.WithField("abn", p.activeBallot.Load()).Debugf("fail to receive commit in time, start election")

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-p.enterLeader:
			return nil
		case <-p.enterFollower:
			return nil
		}
	}
}

func (p *paxos) election(ctx context.Context) (bool, error) {
	pbn := nextPrepareBallot(p.id, p.activeBallot.Load(), p.maxPeersNumber)
	req := &proto.PrepareRequest{Ballot: pbn}
	resps := p.broadcast(ctx, func(ctx context.Context, t transport) (any, error) {
		return t.prepare(ctx, req)
	})

	votes := 1
	logs := []*log{p.log}
	for count := 0; count < len(p.peers); count++ {
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case resp := <-resps:
			if resp == nil {
				// Skip peers we fail to connect to
				continue
			}
			r := resp.(*proto.PrepareResponse)
			if r.ReplyType == proto.ReplyType_REPLY_TYPE_REJECT {
				p.logger.WithField("abn", r.Ballot).Debug("prepare rejected")
				p.activeBallot.Store(r.Ballot)
				return false, nil
			}
			votes += 1
			logs = append(logs, newLogWithInstances(r.Instances...))
			if votes > len(p.peers)/2 {
				break
			}
		}
	}
	p.activeBallot.Store(pbn)
	p.log = mergeLogs(pbn, logs)
	p.acceptCh <- p.log.insts
	p.enterLeader <- struct{}{}
	p.logger.WithField("abn", pbn).Debug("I'm the leader now")
	return false, nil
}

func nextPrepareBallot(id NodeID, abn, maxPeersNum int64) int64 {
	rounds := (abn + maxPeersNum) / (maxPeersNum + 1)
	return (rounds+1)*(maxPeersNum+1) + int64(id)
}

func mergeLogs(pbn int64, logs []*log) *log {
	if len(logs) == 0 {
		return &log{}
	}

	var base instanceID = math.MaxInt64
	var maxID instanceID
	instances := make(map[int64]*proto.Instance)
	for _, l := range logs {
		for _, inst := range l.insts {
			if inst == nil || inst.State == proto.State_STATE_MISSING {
				continue
			}
			if instanceID(inst.Id) < base {
				base = instanceID(inst.Id)
			}
			if instanceID(inst.Id) > maxID {
				maxID = instanceID(inst.Id)
			}
			old, ok := instances[inst.Id]
			if !ok {
				instances[inst.Id] = inst
				continue
			}
			if old.State == proto.State_STATE_COMMITTED {
				continue
			}
			if old.Ballot < inst.Ballot || (old.Ballot == inst.Ballot && inst.State == proto.State_STATE_COMMITTED) {
				instances[inst.Id] = inst
			}
		}
	}
	if base == math.MaxInt64 {
		return &log{}
	}

	insts := make([]*proto.Instance, maxID-base+1)
	for i := base; i <= maxID; i++ {
		if inst, ok := instances[int64(i)]; ok {
			inst.Ballot = pbn
			insts[i-base] = inst
		}
	}
	return &log{base, insts}
}
