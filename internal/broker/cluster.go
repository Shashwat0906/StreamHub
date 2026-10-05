package broker

import (
	"context"
	"fmt"

	"github.com/Shashwat0906/StreamHub/internal/metadata"
	"github.com/Shashwat0906/StreamHub/internal/protocol"
)

// isFenced reports whether this broker must refuse leader writes because
// it lost contact with the controller. Standalone brokers are never fenced.
func (b *Broker) isFenced() bool { return false }

// dispatchExtra handles APIs added in later phases.
func (b *Broker) dispatchExtra(ctx context.Context, h protocol.RequestHeader, body []byte) protocol.Message {
	switch h.API {
	case protocol.APIInitProducerID:
		return b.handleInitProducerID(ctx)
	}
	return nil
}

// handleInitProducerID allocates a cluster-unique producer ID through the
// metadata log, so IDs are never reused even across controller changes.
func (b *Broker) handleInitProducerID(ctx context.Context) protocol.Message {
	if !b.proposer.IsLeader() {
		return &protocol.InitProducerIDResponse{Err: protocol.ErrNotController, ProducerID: -1}
	}
	res, err := b.proposer.Propose(ctx, metadata.Command{Type: metadata.CmdAllocateProducer})
	if err != nil {
		return &protocol.InitProducerIDResponse{Err: protocol.ErrNotController, ProducerID: -1}
	}
	if res.Err != protocol.ErrNone {
		return &protocol.InitProducerIDResponse{Err: res.Err, ProducerID: -1}
	}
	b.logger.Debug("allocated producer id", "pid", res.ProducerID)
	return &protocol.InitProducerIDResponse{ProducerID: res.ProducerID}
}

var _ = fmt.Sprintf
