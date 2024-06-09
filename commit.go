package gopaxos

import (
	"context"
	"fmt"
	"time"

	"github.com/liznear/gopaxos/proto"
)

func (p *paxos) commitLoop(ctx context.Context) error {
	var (
		abn      int64
		isLeader bool
		timer    = time.NewTimer(p.commitInterval)
	)

	timer.Stop()
outerLoop:
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("paxos: stop commit loop as a follower: %w", ctx.Err())
		case <-p.enterLeader:
			abn, isLeader = p.currentBallot()
			if !isLeader {
				continue outerLoop
			}
			p.logger.WithField("abn", abn).Debug("I'm the leader now. Start committing loop")
			timer.Reset(p.commitInterval)
		}

	innerLoop:
		for {
			select {
			case <-ctx.Done():
				return fmt.Errorf("paxos: stop commit loop as a leader: %w", ctx.Err())
			case <-timer.C:
				committed, err := p.broadcastCommit(ctx, abn)
				if err != nil {
					return fmt.Errorf("paxos: fail to commit: %w", err)
				}
				if !committed {
					// No longer the leader
					timer.Stop()
					break innerLoop
				}
			}
		}
	}
}

func (p *paxos) broadcastCommit(ctx context.Context, abn int64) (committed bool, err error) {
	gle := p.globalLastExecuted.Load()
	le := p.lastExecuted.Load()
	p.log.trim(instanceID(gle))
	req := &proto.CommitRequest{Ballot: abn, LastExecuted: le, GlobalLastExecuted: gle}
	resps := p.broadcast(ctx, func(ctx context.Context, t transport) (any, error) {
		return t.commit(ctx, req)
	})

	counts := 0
	minLE := le
	for i := 0; i < len(p.peers); i++ {
		select {
		case <-ctx.Done():
			return false, fmt.Errorf("paxos: stop commit loop while waiting for response: %w", ctx.Err())
		case resp := <-resps:
			if resp == nil {
				// Skip peers we fail to connect to
				continue
			}
			r := resp.(*proto.CommitResponse)
			if r.ReplyType == proto.ReplyType_REPLY_TYPE_REJECT {
				_, _ = p.updateBallot(r.Ballot)
				p.logger.WithField("abn", r.Ballot).Debug("commit rejected, no longer the leader")
				return false, nil
			}

			counts += 1
			if r.LastExecuted < minLE {
				minLE = r.LastExecuted
			}
		}
	}
	if counts == len(p.peers) {
		p.globalLastExecuted.Store(minLE)
	}
	p.logger.WithField("abn", abn).WithField("gle", p.globalLastExecuted.Load()).Debug("commit success")
	return true, nil
}

func (p *paxos) handleCommit(_ context.Context, req *proto.CommitRequest) (*proto.CommitResponse, error) {
	old, _ := p.updateBallot(req.Ballot)
	if old > req.Ballot {
		return &proto.CommitResponse{ReplyType: proto.ReplyType_REPLY_TYPE_REJECT, Ballot: old}, nil
	}

	le := p.lastExecuted.Load()
	start := p.log.indexOf(instanceID(le))
	for i := start; i < len(p.log.insts); i++ {
		inst := p.log.insts[i]
		if inst == nil || inst.Id > req.LastExecuted {
			break
		}
		inst.State = proto.State_STATE_COMMITTED
		if err := p.onCommit.Execute(context.Background(), inst.Value); err != nil {
			p.logger.WithField("abn", req.Ballot).WithField("inst", inst.Id).WithError(err).Error("fail to execute committed instance")
		}
		le++
	}
	p.lastExecuted.Store(le)
	return &proto.CommitResponse{ReplyType: proto.ReplyType_REPLY_TYPE_OK, LastExecuted: p.lastExecuted.Load()}, nil
}
