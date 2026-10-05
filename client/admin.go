package client

import (
	"context"
	"errors"
	"time"

	"github.com/Shashwat0906/StreamHub/internal/protocol"
)

// retry runs fn until it succeeds, returns a non-retriable error, or ctx
// expires. Metadata is refreshed between attempts because most retriable
// errors (NOT_LEADER, NOT_CONTROLLER, network) mean our view is stale.
func (c *Client) retry(ctx context.Context, fn func() error) error {
	var err error
	for attempt := 0; ; attempt++ {
		err = fn()
		if err == nil || !isRetriable(err) {
			return err
		}
		if ctx.Err() != nil {
			return err
		}
		c.log.Debug("retrying", "attempt", attempt, "err", err)
		if sleepCtx(ctx, backoff(attempt)) != nil {
			return err
		}
		c.RefreshMetadata(ctx) // best effort
	}
}

func withDefaultTimeout(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, d)
}

// TopicSpec describes a topic to create.
type TopicSpec struct {
	Name              string
	Partitions        int32
	ReplicationFactor int16
	Configs           map[string]string // retention.ms, retention.bytes, segment.bytes, min.insync.replicas
}

// CreateTopic creates a topic via the controller. It waits until every
// partition has a leader visible in metadata.
func (c *Client) CreateTopic(ctx context.Context, spec TopicSpec) error {
	ctx, cancel := withDefaultTimeout(ctx, 30*time.Second)
	defer cancel()
	attempted := false
	err := c.retry(ctx, func() error {
		addr, err := c.controllerAddr(ctx)
		if err != nil {
			return err
		}
		var resp protocol.SimpleResponse
		req := &protocol.CreateTopicRequest{Name: spec.Name, Partitions: spec.Partitions,
			ReplicationFactor: spec.ReplicationFactor, Configs: spec.Configs}
		if err := c.call(ctx, addr, protocol.APICreateTopic, req, &resp); err != nil {
			attempted = true
			return err
		}
		// A previous attempt may have committed before its reply was lost
		// (e.g. controller failover); "already exists" then means success.
		if resp.Err == protocol.ErrTopicAlreadyExists && attempted {
			return nil
		}
		if resp.Err.Retriable() {
			attempted = true
		}
		return resp.Err.AsError(resp.ErrMsg)
	})
	if err != nil {
		return err
	}
	return c.waitForLeaders(ctx, spec.Name)
}

func (c *Client) waitForLeaders(ctx context.Context, topic string) error {
	for attempt := 0; ; attempt++ {
		if err := c.RefreshMetadata(ctx); err == nil {
			c.mu.RLock()
			t, ok := c.topics[topic]
			c.mu.RUnlock()
			if ok {
				ready := true
				for _, p := range t.Partitions {
					if p.Leader < 0 {
						ready = false
					}
				}
				if ready {
					return nil
				}
			}
		}
		if err := sleepCtx(ctx, backoff(attempt)); err != nil {
			return errors.New("client: topic created but leaders not visible yet")
		}
	}
}

// DeleteTopic deletes a topic via the controller.
func (c *Client) DeleteTopic(ctx context.Context, name string) error {
	ctx, cancel := withDefaultTimeout(ctx, 30*time.Second)
	defer cancel()
	return c.retry(ctx, func() error {
		addr, err := c.controllerAddr(ctx)
		if err != nil {
			return err
		}
		var resp protocol.SimpleResponse
		if err := c.call(ctx, addr, protocol.APIDeleteTopic, &protocol.DeleteTopicRequest{Name: name}, &resp); err != nil {
			return err
		}
		return resp.Err.AsError(resp.ErrMsg)
	})
}

// ClusterInfo is the result of DescribeCluster.
type ClusterInfo struct {
	ControllerID int32
	Brokers      []protocol.BrokerInfo
	Topics       []protocol.TopicInfo
}

// DescribeCluster returns brokers, controller and all topics.
func (c *Client) DescribeCluster(ctx context.Context) (ClusterInfo, error) {
	ctx, cancel := withDefaultTimeout(ctx, 15*time.Second)
	defer cancel()
	var out ClusterInfo
	err := c.retry(ctx, func() error {
		for _, addr := range c.knownAddrs() {
			var resp protocol.MetadataResponse
			if err := c.call(ctx, addr, protocol.APIMetadata, &protocol.MetadataRequest{}, &resp); err != nil {
				continue
			}
			out = ClusterInfo{ControllerID: resp.ControllerID, Brokers: resp.Brokers, Topics: resp.Topics}
			return nil
		}
		return &protocol.Error{Code: protocol.ErrNetwork, Msg: "no broker reachable"}
	})
	return out, err
}

// ListOffsets returns the earliest (protocol.OffsetEarliest) or latest
// committed (protocol.OffsetLatest = high-watermark) offset of a partition.
func (c *Client) ListOffsets(ctx context.Context, topic string, partition int32, which int64) (int64, error) {
	ctx, cancel := withDefaultTimeout(ctx, 15*time.Second)
	defer cancel()
	var off int64
	err := c.retry(ctx, func() error {
		_, addr, _, err := c.leader(ctx, topic, partition)
		if err != nil {
			return err
		}
		var resp protocol.ListOffsetsResponse
		if err := c.call(ctx, addr, protocol.APIListOffsets, &protocol.ListOffsetsRequest{Topic: topic, Partition: partition, Timestamp: which}, &resp); err != nil {
			return err
		}
		off = resp.Offset
		return resp.Err.AsError("list offsets")
	})
	return off, err
}

// RawProduce sends one ProduceRequest to a specific broker address with no
// routing or retries. It exists for tools and failure tests.
func (c *Client) RawProduce(ctx context.Context, addr string, req *protocol.ProduceRequest) error {
	if req.Acks == protocol.AcksNone {
		return c.call(ctx, addr, protocol.APIProduce, req, nil)
	}
	var resp protocol.ProduceResponse
	if err := c.call(ctx, addr, protocol.APIProduce, req, &resp); err != nil {
		return err
	}
	return resp.Err.AsError(resp.ErrMsg)
}
