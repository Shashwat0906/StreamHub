package broker

import (
	"context"

	"github.com/Shashwat0906/StreamHub/internal/metadata"
	"github.com/Shashwat0906/StreamHub/internal/protocol"
)

// dispatchExtra handles APIs added in later phases.
func (b *Broker) dispatchExtra(ctx context.Context, h protocol.RequestHeader, body []byte) protocol.Message {
	switch h.API {
	case protocol.APIInitProducerID:
		return b.handleInitProducerID(ctx)
	case protocol.APIRaftVote:
		req, err := decode[protocol.RaftVoteRequest](body)
		if err != nil || b.raft == nil {
			return &protocol.RaftVoteResponse{}
		}
		return b.raft.HandleVote(req)
	case protocol.APIRaftAppend:
		req, err := decode[protocol.RaftAppendRequest](body)
		if err != nil || b.raft == nil {
			return &protocol.RaftAppendResponse{}
		}
		return b.raft.HandleAppend(req)
	case protocol.APIBrokerHeartbeat:
		req, err := decode[protocol.BrokerHeartbeatRequest](body)
		if err != nil {
			return &protocol.BrokerHeartbeatResponse{Err: protocol.ErrInvalidRequest}
		}
		if b.controller == nil {
			return &protocol.BrokerHeartbeatResponse{Err: protocol.ErrNotController, ControllerID: -1}
		}
		return b.controller.onHeartbeat(ctx, req)
	case protocol.APIAlterISR:
		req, err := decode[protocol.AlterISRRequest](body)
		if err != nil {
			return badRequest(err)
		}
		if b.controller == nil {
			return &protocol.SimpleResponse{Err: protocol.ErrNotController}
		}
		return b.controller.onAlterISR(ctx, req)
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
