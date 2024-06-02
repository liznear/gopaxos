package core

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/liznear/gopaxos/core/proto"
	"google.golang.org/grpc/status"
)

func (s *paxosState) commitLoop(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.enterLeader:
			slog.DebugContext(ctx, "Start committing", slog.Int64("abn", s.activeBallot.Load()))
			// do nothing
		}

		for {
			abn, isLeader := s.currentActiveBallot()
			slog.DebugContext(ctx, "Committing loop", slog.Int64("abn", abn), slog.Bool("is_leader", isLeader))
			if !isLeader {
				break
			}

			// It should be always safe to trim logs <= gle
			gle := instanceID(s.globalLastExecuted.Load())
			s.trim(gle)

			// TODO: make it configurable
			const commitTimeout = 5 * time.Second
			rctx, cancel := context.WithTimeout(ctx, commitTimeout)
			defer cancel()

			if err := s.broadcastCommit(rctx, abn); err != nil {
				return fmt.Errorf("paxos: fail to commit: %w", err)
			}

			// It is possible that we realized there is a new leader
			// after receiving replies for commits. This loop would
			// stop, and this whole `commitLoop` would block at
			// `<-s.enterLeader`.
			if abn, isLeader = s.currentActiveBallot(); !isLeader {
				slog.DebugContext(ctx, "No longer leader", slog.Int64("abn", abn), slog.Bool("is_leader", isLeader))
				break
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(s.commitInterval):
				continue
			}
		}
	}
}

func (s *paxosState) broadcastCommit(ctx context.Context, abn int64) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	req := &proto.CommitRequest{
		Ballot:             abn,
		LastExecuted:       s.lastExecuted.Load(),
		GlobalLastExecuted: s.globalLastExecuted.Load(),
	}
	resps := make(chan *proto.CommitResponse, len(s.peers))
	for _, peer := range s.peers {
		peer := peer
		go func() {
			resp, err := peer.transport.Commit(ctx, req)
			if err != nil {
				slog.WarnContext(ctx, "fail to broadcast commit", slog.Int("peer", int(peer.id)), slog.Any("code", status.Code(err)))
				resps <- nil
				return
			}
			resps <- resp
		}()
	}

	counts := 0
	minLastExecuted := s.lastExecuted.Load()
	for i := 0; i < len(s.peers); i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case resp := <-resps:
			if resp == nil {
				continue
			}

			if resp.ReplyType == proto.ReplyType_REPLY_TYPE_REJECT {
				slog.DebugContext(ctx, "Commit rejected", "abn", resp.Ballot)
				s.updateBallot(resp.Ballot)
				return nil
			}

			counts += 1
			if resp.LastExecuted < minLastExecuted {
				minLastExecuted = resp.LastExecuted
			}
		}
	}

	if counts == len(s.peers) {
		s.globalLastExecuted.Swap(minLastExecuted)
	}
	return nil
}

func (s *paxosState) trim(id instanceID) {
	slog.Debug(fmt.Sprintf("Trim to %d", id), "logs", s.log.String())
	le := instanceID(s.lastExecuted.Load())
	if id > le {
		slog.Error("trying to trim logs to id > self last executed id")
		id = le
	}
	s.log.trim(le)
}

// it assumes the caller knows what he is doing
func (l *log) trim(id instanceID) {
	if l.base > id {
		return
	}
	index := id - l.base
	l.base = id
	l.insts = l.insts[index:]
}

func (s *paxosState) commit(ctx context.Context, end int64) error {
	le := s.lastExecuted.Load()
	if err := s.log.commit(le+1, end); err != nil {
		return fmt.Errorf("fail to commit locally: %w", err)
	}

	index := le + int64(1) - int64(s.log.base)
	for int(index) < len(s.log.insts) {
		inst := s.log.insts[index]
		if inst == nil || inst.State == proto.State_STATE_MISSING {
			break
		}
		slog.DebugContext(ctx, "Executing", "index", index)
		if err := s.onCommit(ctx, inst.Value); err != nil {
			return fmt.Errorf("fail to execute: %w", err)
		}
		inst.State = proto.State_STATE_COMMITTED
		index++
	}
	s.lastExecuted.Store(index - 1)
	return nil
}

func (s *paxosState) HandleCommit(ctx context.Context, req *proto.CommitRequest) *proto.CommitResponse {
	slog.DebugContext(ctx, "Receive commit", "abn", s.activeBallot.Load(), "ballot", req.Ballot, "le", req.LastExecuted, "gle", req.GlobalLastExecuted, "sle", s.lastExecuted.Load())

	abn, _ := s.updateBallot(req.Ballot)
	if abn > req.Ballot {
		return &proto.CommitResponse{ReplyType: proto.ReplyType_REPLY_TYPE_REJECT, Ballot: abn}
	}
	s.commitReceived.Store(true)

	if err := s.commit(ctx, req.LastExecuted); err != nil {
		slog.Error("fail to commit: %v", err)
		os.Exit(1)
	}
	return &proto.CommitResponse{
		ReplyType:    proto.ReplyType_REPLY_TYPE_OK,
		LastExecuted: s.lastExecuted.Load(),
	}
}
