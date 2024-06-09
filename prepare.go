package gopaxos

import (
	"context"
	"fmt"
	"math"
	"reflect"
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
		// Wait a while for the commit messages from a leader.
		// When a new node joins, it hasn't received a commit messages from the leader. We let it wait.
		select {
		case <-ctx.Done():
			return fmt.Errorf("paxos: stop prepare loop: %w", ctx.Err())
		case <-time.After(waitForNextCommit):
			if p.commitReceived.Swap(false) {
				continue
			}
		}

		p.logger.WithField("abn", p.activeBallot.Load()).Debugf("fail to receive commit in time, start election")
		win, err := p.election(ctx)
		if err != nil {
			return fmt.Errorf("paxos: fail during election: %w", err)
		}
		if !win {
			continue
		}

		// Now I'm the leader, we just block here until we become a follower again.
		select {
		case <-ctx.Done():
			return fmt.Errorf("paxos: stop prepare loop: %w", ctx.Err())
		case <-p.enterFollower:
			// do nothing
		}
	}
}

func (p *paxos) election(ctx context.Context) (bool, error) {
	pbn := nextPrepareBallot(p.id, p.activeBallot.Load(), p.maxNodesNumber)
	p.logger.WithField("abn", p.activeBallot.Load()).WithField("pbn", pbn).Debug("sending prepare requests")
	req := &proto.PrepareRequest{Ballot: pbn}
	resps := p.broadcast(ctx, func(ctx context.Context, t transport) (any, error) {
		return t.prepare(ctx, req)
	})

	votes := 1
	logs := []*log{p.log}
repliesLoop:
	for count := 0; count < len(p.peers); count++ {
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case resp := <-resps:
			if resp == nil || reflect.ValueOf(resp).IsNil() {
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
			p.logger.WithField("abn", p.activeBallot.Load()).WithField("pbn", pbn).WithField("votes", votes).Debug("Get votes")
			if votes > len(p.peers)/2 {
				break repliesLoop
			}
		}
	}
	if votes <= len(p.peers)/2 {
		return false, nil
	}

	p.log = mergeLogs(pbn, logs)
	if !p.log.empty() {
		p.acceptCh <- acceptPayload{pbn, [2]instanceID{p.log.base, p.log.nextInstanceID()}}
	}
	p.enterLeader <- struct{}{}
	// Update the pbn after updating all other status.
	p.activeBallot.Store(pbn)
	p.logger.WithField("abn", pbn).Debug("I'm the leader now")
	return true, nil
}

func (p *paxos) handlePrepare(_ context.Context, req *proto.PrepareRequest) (*proto.PrepareResponse, error) {
	p.logger.WithField("abn", p.activeBallot.Load()).WithField("req", req.String()).Debug("receiving prepare requests")
	if req.Ballot < p.activeBallot.Load() {
		return &proto.PrepareResponse{ReplyType: proto.ReplyType_REPLY_TYPE_REJECT, Ballot: p.activeBallot.Load()}, nil
	}
	old, updated := p.updateBallot(req.Ballot)
	if !updated {
		return &proto.PrepareResponse{ReplyType: proto.ReplyType_REPLY_TYPE_REJECT, Ballot: old}, nil
	}
	p.logger.WithField("abn", p.activeBallot.Load()).Debug("acknowledge prepare request")
	return &proto.PrepareResponse{ReplyType: proto.ReplyType_REPLY_TYPE_OK, Instances: p.log.insts}, nil
}

func nextPrepareBallot(id NodeID, abn, maxPeersNum int64) int64 {
	if abn == 0 {
		return int64(id)
	}
	prevRound := (abn + maxPeersNum - 1) / (maxPeersNum)
	round := prevRound + 1
	return (round-1)*maxPeersNum + int64(id)
}

// mergeLogs merges logs from different nodes when a node becomes a leader.
//
// The leader should merge logs from a majority of nodes to get the latest state.
// Logs from different nodes can have inconsistency, but any log instance with
// "COMMITTED" state should have the same value (even if the ballot is different), and
// it would appear in at least one of the logs (since we have a majority of nodes' logs).
// For any other log instance, we should choose the one with the highest ballot.
//
// The returned logs would have the ballot equal to pbn.
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
			// All these merged logs are marked as "IN_PROGRESS" since they are not committed yet.
			inst.State = proto.State_STATE_IN_PROGRESS
			insts[i-base] = inst
		}
	}
	return &log{base, insts}
}
