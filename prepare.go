package gopaxos

import "context"

func (p *paxos) prepareLoop(ctx context.Context) error {
	for {
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
