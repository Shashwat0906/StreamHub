package broker

import (
	"context"
	"fmt"
	"math/rand"
	"regexp"
	"strconv"
	"time"

	"github.com/Shashwat0906/StreamHub/internal/metadata"
	"github.com/Shashwat0906/StreamHub/internal/protocol"
)

// handle is the transport.Handler: decode, dispatch, encode.
func (b *Broker) handle(ctx context.Context, h protocol.RequestHeader, body []byte) protocol.Message {
	select {
	case <-b.initDone:
	case <-ctx.Done():
		return nil
	}
	start := time.Now()
	resp := b.dispatch(ctx, h, body)
	b.observeRequest(h.API, time.Since(start), resp)
	return resp
}

func decode[T any, PT interface {
	*T
	protocol.Message
}](body []byte) (PT, error) {
	m := PT(new(T))
	if err := protocol.Unmarshal(body, m); err != nil {
		return nil, err
	}
	return m, nil
}

func badRequest(err error) protocol.Message {
	return &protocol.SimpleResponse{Err: protocol.ErrInvalidRequest, ErrMsg: err.Error()}
}

func (b *Broker) dispatch(ctx context.Context, h protocol.RequestHeader, body []byte) protocol.Message {
	switch h.API {
	case protocol.APIProduce:
		req, err := decode[protocol.ProduceRequest](body)
		if err != nil {
			return &protocol.ProduceResponse{Err: protocol.ErrCorruptMessage, ErrMsg: err.Error()}
		}
		return b.handleProduce(ctx, req)
	case protocol.APIFetch:
		req, err := decode[protocol.FetchRequest](body)
		if err != nil {
			return &protocol.FetchResponse{Err: protocol.ErrInvalidRequest}
		}
		return b.handleFetch(ctx, req)
	case protocol.APIListOffsets:
		req, err := decode[protocol.ListOffsetsRequest](body)
		if err != nil {
			return &protocol.ListOffsetsResponse{Err: protocol.ErrInvalidRequest}
		}
		return b.handleListOffsets(req)
	case protocol.APIMetadata:
		req, err := decode[protocol.MetadataRequest](body)
		if err != nil {
			return &protocol.MetadataResponse{Err: protocol.ErrInvalidRequest}
		}
		return b.handleMetadata(req)
	case protocol.APICreateTopic:
		req, err := decode[protocol.CreateTopicRequest](body)
		if err != nil {
			return badRequest(err)
		}
		return b.handleCreateTopic(ctx, req)
	case protocol.APIDeleteTopic:
		req, err := decode[protocol.DeleteTopicRequest](body)
		if err != nil {
			return badRequest(err)
		}
		return b.handleDeleteTopic(ctx, req)
	default:
		if resp := b.dispatchExtra(ctx, h, body); resp != nil {
			return resp
		}
		return &protocol.SimpleResponse{Err: protocol.ErrInvalidRequest, ErrMsg: fmt.Sprintf("unsupported api %d", h.API)}
	}
}

// ---------------------------------------------------------------- produce

func (b *Broker) minISRFor(topic string) int {
	if t, ok := b.meta.Topic(topic); ok {
		return int(t.ConfigInt64("min.insync.replicas", int64(b.cfg.MinInsyncReplicas)))
	}
	return b.cfg.MinInsyncReplicas
}

func (b *Broker) partitionOrError(topic string, id int32) (*Partition, protocol.ErrorCode) {
	if p := b.replicas.get(topic, id); p != nil {
		return p, protocol.ErrNone
	}
	if _, ok := b.meta.Partition(topic, id); ok {
		return nil, protocol.ErrNotLeader // exists, but not hosted here
	}
	return nil, protocol.ErrUnknownTopicOrPartition
}

func (b *Broker) handleProduce(ctx context.Context, req *protocol.ProduceRequest) protocol.Message {
	resp := b.produce(ctx, req)
	if req.Acks == protocol.AcksNone {
		return nil // acks=0: the producer is not waiting for a reply
	}
	return resp
}

func (b *Broker) produce(ctx context.Context, req *protocol.ProduceRequest) *protocol.ProduceResponse {
	if len(req.Records) == 0 {
		return &protocol.ProduceResponse{Err: protocol.ErrInvalidRequest, ErrMsg: "empty batch"}
	}
	p, code := b.partitionOrError(req.Topic, req.Partition)
	if code != protocol.ErrNone {
		return &protocol.ProduceResponse{Err: code}
	}
	if b.isFenced() {
		// A broker that cannot reach the controller may already have been
		// replaced as leader. Refusing writes avoids acknowledging data on
		// a partition that has moved on (split brain).
		return &protocol.ProduceResponse{Err: protocol.ErrNotLeader, ErrMsg: "broker lost contact with controller"}
	}
	base, end, epoch, code, msg := p.appendAsLeader(req, b.minISRFor(req.Topic))
	b.metrics.recordsIn.Add(float64(len(req.Records)))
	if code != protocol.ErrNone {
		return &protocol.ProduceResponse{Err: code, ErrMsg: msg, BaseOffset: -1}
	}
	if req.Acks == protocol.AcksAll {
		timeout := time.Duration(req.TimeoutMs) * time.Millisecond
		if timeout <= 0 {
			timeout = 30 * time.Second
		}
		wctx, cancel := context.WithTimeout(ctx, timeout)
		code := p.waitForHW(wctx, end, epoch)
		cancel()
		if code != protocol.ErrNone {
			return &protocol.ProduceResponse{Err: code, BaseOffset: -1, ErrMsg: "batch not committed by ISR"}
		}
	}
	return &protocol.ProduceResponse{BaseOffset: base, LogStartOffset: p.log.StartOffset()}
}

// ---------------------------------------------------------------- fetch

func (b *Broker) handleFetch(ctx context.Context, req *protocol.FetchRequest) protocol.Message {
	maxWait := time.Duration(req.MaxWaitMs) * time.Millisecond
	if maxWait > 30*time.Second {
		maxWait = 30 * time.Second
	}
	deadline := time.Now().Add(maxWait)
	minBytes := int(req.MinBytes)
	if minBytes < 1 {
		minBytes = 1
	}

	// Long poll: register a waiter on every partition *before* reading, so
	// data that arrives between our read and our wait still wakes us.
	wake := make(chan struct{}, 1)
	var registered []*Partition
	for _, fp := range req.Partitions {
		if p := b.replicas.get(fp.Topic, fp.Partition); p != nil {
			p.addWaiter(wake)
			registered = append(registered, p)
		}
	}
	defer func() {
		for _, p := range registered {
			p.removeWaiter(wake)
		}
	}()

	for {
		resp, total, hasErr := b.fetchOnce(req)
		if total >= minBytes || hasErr || maxWait <= 0 || !time.Now().Before(deadline) {
			b.metrics.bytesOut.Add(float64(total))
			return resp
		}
		timer := time.NewTimer(time.Until(deadline))
		select {
		case <-wake:
		case <-timer.C:
		case <-ctx.Done():
		}
		timer.Stop()
		if ctx.Err() != nil {
			return resp
		}
	}
}

func (b *Broker) fetchOnce(req *protocol.FetchRequest) (*protocol.FetchResponse, int, bool) {
	resp := &protocol.FetchResponse{Partitions: make([]protocol.FetchPartitionResponse, 0, len(req.Partitions))}
	total := 0
	hasErr := false
	for _, fp := range req.Partitions {
		out := protocol.FetchPartitionResponse{Topic: fp.Topic, Partition: fp.Partition, HighWatermark: -1}
		p, code := b.partitionOrError(fp.Topic, fp.Partition)
		if code != protocol.ErrNone {
			out.Err = code
			hasErr = true
		} else {
			maxBytes := int(fp.MaxBytes)
			if maxBytes <= 0 {
				maxBytes = 1 << 20
			}
			r := p.read(req.ReplicaID, fp.FetchOffset, maxBytes, fp.CurrentLeaderEpoch)
			out.Err, out.Records = r.err, r.records
			out.HighWatermark, out.LogStartOffset, out.LogEndOffset, out.LeaderEpoch = r.hw, r.logStart, r.logEnd, r.epoch
			total += r.bytes
			if r.err != protocol.ErrNone {
				hasErr = true
			}
		}
		resp.Partitions = append(resp.Partitions, out)
	}
	return resp, total, hasErr
}

// ---------------------------------------------------------------- offsets

func (b *Broker) handleListOffsets(req *protocol.ListOffsetsRequest) protocol.Message {
	p, code := b.partitionOrError(req.Topic, req.Partition)
	if code != protocol.ErrNone {
		return &protocol.ListOffsetsResponse{Err: code, Offset: -1}
	}
	r, _, _, hw, _ := p.Snapshot()
	if r != roleLeader {
		return &protocol.ListOffsetsResponse{Err: protocol.ErrNotLeader, Offset: -1}
	}
	switch req.Timestamp {
	case protocol.OffsetEarliest:
		return &protocol.ListOffsetsResponse{Offset: p.log.StartOffset()}
	default:
		// "Latest" for a consumer is the high-watermark: records above it
		// are not readable yet.
		return &protocol.ListOffsetsResponse{Offset: hw}
	}
}

// ---------------------------------------------------------------- metadata

func (b *Broker) handleMetadata(req *protocol.MetadataRequest) protocol.Message {
	resp := &protocol.MetadataResponse{ControllerID: b.proposer.LeaderID()}
	for _, bm := range b.meta.Brokers() {
		resp.Brokers = append(resp.Brokers, protocol.BrokerInfo{ID: bm.ID, Addr: bm.Addr, Fenced: bm.Fenced})
	}
	var topics []metadata.TopicMeta
	if len(req.Topics) == 0 {
		topics = b.meta.Topics()
	} else {
		for _, name := range req.Topics {
			t, ok := b.meta.Topic(name)
			if !ok {
				resp.Topics = append(resp.Topics, protocol.TopicInfo{Name: name, Err: protocol.ErrUnknownTopicOrPartition})
				continue
			}
			topics = append(topics, t)
		}
	}
	for _, t := range topics {
		ti := protocol.TopicInfo{Name: t.Name, Configs: t.Configs}
		for _, p := range t.Partitions {
			ti.Partitions = append(ti.Partitions, protocol.PartitionInfo{
				ID: p.ID, Leader: p.Leader, LeaderEpoch: p.LeaderEpoch, Replicas: p.Replicas, ISR: p.ISR,
			})
		}
		resp.Topics = append(resp.Topics, ti)
	}
	return resp
}

// ---------------------------------------------------------------- admin

var topicNameRe = regexp.MustCompile(`^[a-zA-Z0-9._-]{1,200}$`)

// OffsetsTopic is the internal topic storing consumer-group offsets.
const OffsetsTopic = "__consumer_offsets"

var validTopicConfigs = map[string]bool{
	"retention.ms": true, "retention.bytes": true, "segment.bytes": true, "min.insync.replicas": true,
}

func (b *Broker) handleCreateTopic(ctx context.Context, req *protocol.CreateTopicRequest) protocol.Message {
	if !b.proposer.IsLeader() {
		return &protocol.SimpleResponse{Err: protocol.ErrNotController, ErrMsg: fmt.Sprintf("controller is %d", b.proposer.LeaderID())}
	}
	if !topicNameRe.MatchString(req.Name) {
		return &protocol.SimpleResponse{Err: protocol.ErrInvalidTopic, ErrMsg: "topic names use [a-zA-Z0-9._-], max 200 chars"}
	}
	for k, v := range req.Configs {
		if !validTopicConfigs[k] {
			return &protocol.SimpleResponse{Err: protocol.ErrInvalidRequest, ErrMsg: "unknown config " + k}
		}
		if _, err := strconv.ParseInt(v, 10, 64); err != nil {
			return &protocol.SimpleResponse{Err: protocol.ErrInvalidRequest, ErrMsg: "config " + k + " must be an integer"}
		}
	}
	res := b.createTopic(ctx, req.Name, req.Partitions, req.ReplicationFactor, req.Configs)
	return &protocol.SimpleResponse{Err: res.Err, ErrMsg: res.Msg}
}

// createTopic computes a replica assignment over live brokers and commits
// it through the metadata log. Only valid on the controller.
func (b *Broker) createTopic(ctx context.Context, name string, partitions int32, rf int16, configs map[string]string) metadata.Result {
	if partitions <= 0 {
		partitions = b.cfg.DefaultPartitions
	}
	if rf <= 0 {
		rf = b.cfg.DefaultReplicationFactor
	}
	live := b.meta.LiveBrokers()
	assign, err := metadata.AssignReplicas(live, int(partitions), int(rf), rand.Intn(max(1, len(live))))
	if err != nil {
		return metadata.Result{Err: protocol.ErrInvalidReplicationFactor, Msg: err.Error()}
	}
	res, err := b.proposer.Propose(ctx, metadata.Command{
		Type: metadata.CmdCreateTopic, Topic: name, Assignments: assign, Configs: configs,
	})
	if err != nil {
		return metadata.Result{Err: protocol.ErrNotController, Msg: err.Error()}
	}
	if res.Err == protocol.ErrNone {
		b.logger.Info("topic created", "topic", name, "partitions", partitions, "rf", rf, "assignment", assign)
	}
	return res
}

func (b *Broker) handleDeleteTopic(ctx context.Context, req *protocol.DeleteTopicRequest) protocol.Message {
	if !b.proposer.IsLeader() {
		return &protocol.SimpleResponse{Err: protocol.ErrNotController, ErrMsg: fmt.Sprintf("controller is %d", b.proposer.LeaderID())}
	}
	if req.Name == OffsetsTopic {
		return &protocol.SimpleResponse{Err: protocol.ErrInvalidTopic, ErrMsg: "cannot delete internal topic"}
	}
	res, err := b.proposer.Propose(ctx, metadata.Command{Type: metadata.CmdDeleteTopic, Topic: req.Name})
	if err != nil {
		return &protocol.SimpleResponse{Err: protocol.ErrNotController, ErrMsg: err.Error()}
	}
	if res.Err == protocol.ErrNone {
		b.logger.Info("topic deleted", "topic", req.Name)
	}
	return &protocol.SimpleResponse{Err: res.Err, ErrMsg: res.Msg}
}
