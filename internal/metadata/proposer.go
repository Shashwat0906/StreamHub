package metadata

import (
	"context"
	"errors"
)

// ErrNotLeader is returned by Propose on a node that is not the Raft leader.
var ErrNotLeader = errors.New("metadata: not the controller")

// Proposer commits commands to the metadata log. The Raft node implements
// it; the result is the value returned by Store.Apply on this node.
type Proposer interface {
	Propose(ctx context.Context, cmd Command) (Result, error)
	IsLeader() bool
	LeaderID() int32
}
