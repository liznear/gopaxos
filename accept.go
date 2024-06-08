package gopaxos

import (
	"context"
	"fmt"

	"github.com/liznear/gopaxos/proto"
)

func (p *paxos) acceptLoop(ctx context.Context) error {
	var (
		abn      int64
		isLeader bool
	)
acceptLoop:
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("paxos: stop accept loop as a follower: %w", ctx.Err())
		case <-p.enterLeader:
			// I'm the leader now. Need to start broadcasting accepted instances.
			abn, isLeader = p.currentBallot()
			if !isLeader {
				continue acceptLoop
			}
			p.logger.WithField("abn", abn).Debug("start accept loop")
		}

	innerLoop:
		for {
			select {
			case <-ctx.Done():
				return fmt.Errorf("paxos: stop accept loop as a leader: %w", ctx.Err())
			case payload := <-p.acceptCh:
				// Probably payloads from previous terms. Ignore them
				if payload.abn != abn {
					p.logger.WithField("abn", abn).WithField("accept_abn", payload.abn).Debug("ignore accept payload")
					continue innerLoop
				}
				// The instances should have been appended to the leader's log.
				// Broadcast the instances to all peers.
				accepted, err := p.broadcastAcceptedInstances(ctx, payload)
				if err != nil {
					return fmt.Errorf("paxos: fail to broadcast accepted instances: %w", err)
				}
				if !accepted {
					// No longer the leader.
					break innerLoop
				}
			}
		}
	}
}

func (p *paxos) broadcastAcceptedInstances(ctx context.Context, payload acceptPayload) (accepted bool, err error) {
	start, end := payload.instsRange[0]-p.log.base, payload.instsRange[1]-p.log.base
	if start < 0 || int(end) > len(p.log.insts) {
		p.logger.WithField("start", start).WithField("end", end).Error("invalid instance range")
		panic("invalid instance range")
	}
	if start == end {
		// Nothing to broadcast
		return true, nil
	}

	insts := p.log.insts[start:end]
	for _, inst := range insts {
		if inst.Ballot != payload.abn {
			p.logger.WithField("abn", payload.abn).WithField("inst", inst.Id).WithField("inst_abn", inst.Ballot).Error("instance has different ballot")
			panic("instance has different ballot")
		}
		if inst.State != proto.State_STATE_IN_PROGRESS {
			p.logger.WithField("abn", payload.abn).WithField("inst", inst.Id).WithField("state", inst.State.String()).Error("instance has wrong state")
			panic("instance has wrong state")
		}
	}

	req := &proto.AcceptRequest{Ballot: payload.abn, Instances: insts}
	resps := p.broadcast(ctx, func(ctx context.Context, t transport) (any, error) { return t.accept(ctx, req) })

	votes := 1
repliesLoop:
	for i := 0; i < len(p.peers); i++ {
		select {
		case <-ctx.Done():
			return
		case resp := <-resps:
			if resp == nil {
				// Skip peers we fail to connect to
				continue
			}
			r := resp.(*proto.AcceptResponse)
			if r.ReplyType == proto.ReplyType_REPLY_TYPE_REJECT {
				// Not leader anymore
				p.logger.WithField("old_abn", payload.abn).WithField("abn", r.Ballot).Debug("accept rejected")
				old, updated := p.updateBallot(r.Ballot)
				if !updated {
					p.logger.WithField("abn", old).WithField("replied_abn", r.Ballot).Debug("accept rejected, but not updated to new ballot")
				}
				return false, nil
			}

			votes += 1
			if votes > len(p.peers)/2 {
				break repliesLoop
			}
		}
	}

	// Reach quorum for the logs. Mark them as committed and also execute them.
	le := p.lastExecuted.Load()
	for i := start; i < end; i++ {
		inst := p.log.insts[i]
		inst.State = proto.State_STATE_COMMITTED
		if le+1 != inst.Id {
			// This is not the next log to be executed.
			if le+1 > inst.Id {
				p.logger.WithField("abn", payload.abn).WithField("inst", inst.Id).WithField("le", le).Error("instance has been executed, probably a new leader is re-replicating stuff")
			}
			if le+1 < inst.Id {
				p.logger.WithField("abn", payload.abn).WithField("inst", inst.Id).WithField("le", le).Error("there are gaps between this instance and the last executed instance")
			}
			continue
		}
		le += 1
		if err := p.onCommit.Execute(ctx, inst.Value); err != nil {
			p.logger.WithField("abn", payload.abn).WithField("inst", inst.Id).WithError(err).Error("fail to execute committed instance")
		}
	}
	p.lastExecuted.Store(le)
	return true, nil
}

type acceptPayload struct {
	abn int64
	// instsRange is the range of instances to be broadcasted.
	// left inclusive, right exclusive.
	instsRange [2]instanceID
}
