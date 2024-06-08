package gopaxos

import (
	"context"
	"fmt"
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
			return fmt.Errorf("paxos: stop accept loop: %w", ctx.Err())
		case <-p.enterLeader:
			// I'm the leader now. Need to start broadcasting accepted instances.
			abn, isLeader = p.currentBallot()
			if !isLeader {
				continue acceptLoop
			}
			p.logger.WithField("abn", abn).Debug("start accept loop")
		}
	}
}
