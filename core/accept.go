package core

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/liznear/gopaxos/core/proto"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/status"
)

func (s *paxosState) acceptLoop(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case insts := <-s.acceptCh:
			abn, isLeader := s.currentActiveBallot()
			if !isLeader {
				slog.WarnContext(ctx, "receive instances as a follower, ignore.")
				continue
			}
			s.log.appendAsLeader(abn, insts)
			if err := s.sendToFollowers(ctx, abn, insts); err != nil {
				return fmt.Errorf("paxos: fail to send to followers: %w", err)
			}
		}
	}
}

func (s *paxosState) sendToFollowers(ctx context.Context, abn int64, insts []*proto.Instance) error {
	g, ctx := errgroup.WithContext(ctx)
	resps := make(chan *proto.AcceptResponse, len(s.peers))

	for _, peer := range s.peers {
		g.Go(func() error {
			resp, err := peer.transport.Accept(ctx, &proto.AcceptRequest{
				Ballot:    abn,
				Instances: insts,
			})
			if err != nil {
				slog.WarnContext(ctx, "fail to send accept", slog.Int("peer", int(peer.id)), slog.Any("code", status.Code(err)))
				resps <- nil
				return nil
			}
			resps <- resp
			return nil
		})
	}

	votes := 1
	for i := 0; i < len(s.peers); i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case resp := <-resps:
			if resp == nil {
				continue
			}
			if resp.ReplyType == proto.ReplyType_REPLY_TYPE_REJECT {
				s.updateBallot(resp.Ballot)
				break
			}
			votes += 1
			if votes >= (len(s.peers)+1)/2 {
				slog.DebugContext(ctx, fmt.Sprintf("Got %d votes, commiting now", votes), "abn", abn, "insts", insts)
				if len(insts) == 0 {
					return nil
				}
				return s.commit(ctx, insts[len(insts)-1].Id)
			}
		}
	}
	return nil
}

func (l *log) appendAsLeader(abn int64, insts []*proto.Instance) {
	id := l.base + instanceID(len(l.insts))
	for i, inst := range insts {
		inst.Id = int64(id) + int64(i)
		inst.State = proto.State_STATE_IN_PROGRESS
		inst.Ballot = abn
		l.insts = append(l.insts, inst)
	}
	slog.Debug("Appended as leader", "logs", l.String(), "abn", abn)
}

func (l *log) appendAsFollower(insts []*proto.Instance) {
	if len(insts) == 0 {
		return
	}
	first := insts[0].Id - int64(l.base)
	totalL := int(first) + len(insts)
	slog.Debug(fmt.Sprintf("Appending: growing to %d instances as follower", totalL))
	l.grow(totalL)
	slog.Debug(fmt.Sprintf("Appending %d instances as follower", len(insts)))
	for i, inst := range insts {
		l.insts[i+int(first)] = inst
		slog.Debug("Appending as follower", "value", string(inst.Value))
	}
	slog.Debug("Appended as follower", "logs", l.String())
}

func (l *log) commit(start, end int64) error {
	slog.Debug("Commit logs locally", "start", start, "end", end)
	if end < start {
		return nil
	}
	totalL := end - int64(l.base) + 1
	l.grow(int(totalL))

	for i := start; i < end; i++ {
		inst := l.insts[i]
		if inst == nil {
			continue
		}
		inst.State = proto.State_STATE_COMMITTED
	}
	slog.Debug("Logs committed locally", "start", start, "end", end, "logs", l.String())
	return nil
}

func (l *log) grow(length int) {
	capacity := cap(l.insts)
	slog.Debug("Growing log", "old", capacity)
	if capacity >= length {
		return
	}
	if capacity == 0 {
		capacity = 4
	}
	for capacity < length {
		capacity *= 2
	}
	old := l.insts
	slog.Debug("Growing log", "old", len(old), "new_cap", capacity, "new_len", length)
	l.insts = make([]*proto.Instance, length, capacity)
	copy(l.insts, old)
}

func (l *log) String() string {
	sb := strings.Builder{}
	sb.WriteString(fmt.Sprintf("Base: %d\n", l.base))
	for i, inst := range l.insts {
		if inst == nil || inst.State == proto.State_STATE_MISSING {
			sb.WriteString(fmt.Sprintf("%d[%d]: missing\n", int64(i)+int64(l.base), i))
			continue
		} else {
			sb.WriteString(fmt.Sprintf("%d[%d]: %s, bn=%d, value=%q\n", inst.Id, i, inst.State.String(), inst.Ballot, string(inst.Value)))
		}
	}
	return sb.String()
}

func (s *paxosState) HandleAccept(ctx context.Context, req *proto.AcceptRequest) *proto.AcceptResponse {
	slog.DebugContext(ctx, "Receive accept", "abn", s.activeBallot.Load(), "ballot", req.Ballot)

	abn, _ := s.updateBallot(req.Ballot)
	if abn > req.Ballot {
		return &proto.AcceptResponse{ReplyType: proto.ReplyType_REPLY_TYPE_REJECT, Ballot: abn}
	}
	s.log.appendAsFollower(req.Instances)
	return &proto.AcceptResponse{ReplyType: proto.ReplyType_REPLY_TYPE_OK, Ballot: abn}
}
