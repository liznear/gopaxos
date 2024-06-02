package core

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/liznear/gopaxos/core/proto"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/status"
	gproto "google.golang.org/protobuf/proto"
)

func (s *paxosState) prepareLoop(ctx context.Context) error {
	// Sleep for a while just in case there is already a leader.
	time.Sleep(time.Duration(2) * s.commitInterval)

	s.enterFollower <- struct{}{}
	for {
		// Check if the context has been cancelled.
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.enterFollower:
			// block until the node becomes a follower
		}

		slog.DebugContext(ctx, "I'm a follower now!!!", slog.Int64("abn", s.activeBallot.Load()))
	followerLoop:
		for {
			// Failure detector. If this node fail to receive a commit message (also used
			// as a heartbeat), it should break the follower loop and start a new election.
			if !s.commitReceived.Swap(false) {
				// Fail to receive a commit message in time. Start a new election.
				mergedLog, err := s.election(ctx)
				if err != nil {
					return fmt.Errorf("paxos: fail to elect: %w", err)
				}

				if abn, isLeader := s.currentActiveBallot(); !isLeader {
					slog.DebugContext(ctx, "Election failed", "new_abn", abn)
					// I'm not the leader. Let's continue the failure detection loop.
					time.Sleep(s.commitInterval)
					continue followerLoop
				}

				// I'm the leader. Send accept messages to all peers, then start
				// the next iteration, which should block at `<-s.enterFollower`.
				s.log = mergedLog
				s.acceptCh <- mergedLog.insts
				break followerLoop
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(3) * s.commitInterval):
				continue followerLoop
			}
		}
	}
}

func (s *paxosState) election(ctx context.Context) (log, error) {
	// TODO: Add prepare timeout
	g, ctx := errgroup.WithContext(ctx)
	pbn := nextPrepareBallot(s.id, s.activeBallot.Load(), s.maxPeersNum)
	req := &proto.PrepareRequest{Ballot: pbn}
	resps := make(chan *proto.PrepareResponse, len(s.peers))

	slog.DebugContext(ctx, "Start election", slog.Int64("pbn", pbn))

	for _, peer := range s.peers {
		peer := peer
		g.Go(func() error {
			resp, err := peer.transport.Prepare(ctx, req)
			if err != nil {
				slog.WarnContext(ctx, "paxos: fail to send prepare", slog.Int("peer", int(peer.id)), slog.Any("code", status.Code(err)))
				resps <- nil
			} else {
				resps <- resp
			}
			return nil
		})
	}

	// I've voted for myself.
	votes := 1
	logs := []log{s.log}
	for count := 0; count < len(s.peers); count++ {
		select {
		case <-ctx.Done():
			return log{}, ctx.Err()
		case resp := <-resps:
			if resp == nil {
				// Skip peers we fail to connect to.
				continue
			}
			if resp.ReplyType == proto.ReplyType_REPLY_TYPE_REJECT {
				slog.DebugContext(ctx, "Prepare rejected", slog.Int64("abn", resp.Ballot))
				// When I'm not the leader, this is the only place where ABN can be
				// updated.
				s.activeBallot.Store(resp.Ballot)
				return log{}, nil
			}
			// Someone accepts me. Check if quorum is reached.
			votes += 1
			logs = append(logs, newLog(resp.Instances))
			if votes > (1+len(s.peers))/2 {
				// Quorum reached, no need to wait for other replies.
				s.activeBallot.Store(pbn)
				s.enterLeader <- struct{}{}
				slog.DebugContext(ctx, "I'm the leader now!!!", slog.Int64("abn", pbn))
				return mergeLogs(pbn, logs), nil
			}
		}
	}
	return log{}, nil
}

func nextPrepareBallot(id NodeID, current int64, maxPeersNum int64) int64 {
	rounds := (current + maxPeersNum) / (maxPeersNum + 1)
	return (rounds+1)*(maxPeersNum+1) + int64(id)
}

func mergeLogs(ballot int64, logs []log) log {
	maxLen := 0
	for _, log := range logs {
		if len(log.insts) > maxLen {
			maxLen = len(log.insts)
		}
	}
	merged := make([]*proto.Instance, maxLen)

	for i := 0; i < maxLen; i++ {
		var inst *proto.Instance

		for _, log := range logs {
			if i >= len(log.insts) {
				continue
			}

			curr := log.insts[i]
			if curr.State == proto.State_STATE_COMMITTED {
				inst = curr
				break
			}
			if inst == nil || curr.Ballot > inst.Ballot {
				inst = curr
			}
		}

		if inst != nil {
			merged[i] = gproto.Clone(inst).(*proto.Instance)
			merged[i].Ballot = ballot
		}
	}
	return newLog(merged)
}

func (s *paxosState) HandlePrepare(ctx context.Context, req *proto.PrepareRequest) *proto.PrepareResponse {
	abn, _ := s.updateBallot(req.Ballot)
	slog.DebugContext(ctx, "Receive prepare", slog.Int64("new_abn", abn), slog.Int64("req_abn", req.Ballot))
	if abn > req.Ballot {
		return &proto.PrepareResponse{ReplyType: proto.ReplyType_REPLY_TYPE_REJECT, Ballot: abn}
	}
	return &proto.PrepareResponse{
		ReplyType: proto.ReplyType_REPLY_TYPE_OK,
		Ballot:    abn,
		Instances: s.log.insts,
	}
}
