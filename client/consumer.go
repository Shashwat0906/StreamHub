package client

import (
	"context"
	"time"

	"github.com/Shashwat0906/StreamHub/internal/protocol"
)

// Record is a consumed message.
type Record struct {
	Topic     string
	Partition int32
	Offset    int64
	Timestamp time.Time
	Key       []byte
	Value     []byte
}

// OffsetReset decides where to start when there is no valid offset.
type OffsetReset int

const (
	ResetEarliest OffsetReset = iota
	ResetLatest
)

// FetchConfig tunes fetches.
type FetchConfig struct {
	MaxWait  time.Duration // long-poll wait when no data (default 500ms)
	MinBytes int32         // respond as soon as this much data is ready (default 1)
	MaxBytes int32         // per partition (default 1 MiB)
	Reset    OffsetReset   // on OFFSET_OUT_OF_RANGE / no committed offset
}

func (f *FetchConfig) setDefaults() {
	if f.MaxWait == 0 {
		f.MaxWait = 500 * time.Millisecond
	}
	if f.MinBytes == 0 {
		f.MinBytes = 1
	}
	if f.MaxBytes == 0 {
		f.MaxBytes = 1 << 20
	}
}

// PartitionConsumer reads one partition from a given offset, with no
// group coordination and no committed offsets. Useful for tools and tests.
type PartitionConsumer struct {
	c         *Client
	topic     string
	partition int32
	offset    int64
	cfg       FetchConfig
}

// ConsumePartition starts reading at offset. Pass protocol.OffsetEarliest
// or protocol.OffsetLatest to resolve the start from the broker.
func (c *Client) ConsumePartition(ctx context.Context, topic string, partition int32, offset int64, cfg FetchConfig) (*PartitionConsumer, error) {
	cfg.setDefaults()
	if offset < 0 {
		var err error
		offset, err = c.ListOffsets(ctx, topic, partition, offset)
		if err != nil {
			return nil, err
		}
	}
	return &PartitionConsumer{c: c, topic: topic, partition: partition, offset: offset, cfg: cfg}, nil
}

// Offset is the next offset Poll will read.
func (pc *PartitionConsumer) Offset() int64 { return pc.offset }

// SeekTo changes the next offset to read.
func (pc *PartitionConsumer) SeekTo(offset int64) { pc.offset = offset }

// Poll returns the next records (possibly none after MaxWait).
func (pc *PartitionConsumer) Poll(ctx context.Context) ([]Record, error) {
	var out []Record
	err := pc.c.retry(ctx, func() error {
		res, err := pc.c.fetchPartitions(ctx, pc.cfg, []fetchTarget{{pc.topic, pc.partition, pc.offset}})
		if err != nil {
			return err
		}
		r := res[0]
		if r.Err == protocol.ErrOffsetOutOfRange {
			which := protocol.OffsetEarliest
			if pc.cfg.Reset == ResetLatest {
				which = protocol.OffsetLatest
			}
			off, err := pc.c.ListOffsets(ctx, pc.topic, pc.partition, which)
			if err != nil {
				return err
			}
			pc.offset = off
			return nil
		}
		if r.Err != protocol.ErrNone {
			return r.Err.AsError("fetch")
		}
		out = r.Records
		if len(out) > 0 {
			pc.offset = out[len(out)-1].Offset + 1
		}
		return nil
	})
	return out, err
}

type fetchTarget struct {
	topic     string
	partition int32
	offset    int64
}

type fetchResult struct {
	target  fetchTarget
	Err     protocol.ErrorCode
	Records []Record
	HW      int64
}

// fetchPartitions sends one Fetch per leader broker and merges the results
// in target order. Requests to different leaders run in parallel.
func (c *Client) fetchPartitions(ctx context.Context, cfg FetchConfig, targets []fetchTarget) ([]fetchResult, error) {
	type group struct {
		addr string
		idx  []int
	}
	groups := map[int32]*group{}
	results := make([]fetchResult, len(targets))
	for i, t := range targets {
		results[i].target = t
		leaderID, addr, _, err := c.leader(ctx, t.topic, t.partition)
		if err != nil {
			results[i].Err = codeOf(err)
			continue
		}
		g := groups[leaderID]
		if g == nil {
			g = &group{addr: addr}
			groups[leaderID] = g
		}
		g.idx = append(g.idx, i)
	}
	type reply struct {
		g    *group
		resp protocol.FetchResponse
		err  error
	}
	ch := make(chan reply, len(groups))
	for _, g := range groups {
		go func(g *group) {
			req := &protocol.FetchRequest{
				ReplicaID: protocol.ReplicaIDConsumer,
				MaxWaitMs: int32(cfg.MaxWait / time.Millisecond),
				MinBytes:  cfg.MinBytes,
			}
			for _, i := range g.idx {
				t := targets[i]
				req.Partitions = append(req.Partitions, protocol.FetchPartition{
					Topic: t.topic, Partition: t.partition, FetchOffset: t.offset,
					MaxBytes: cfg.MaxBytes, CurrentLeaderEpoch: -1,
				})
			}
			rctx, cancel := context.WithTimeout(ctx, cfg.MaxWait+c.cfg.RequestTimeout)
			defer cancel()
			var resp protocol.FetchResponse
			err := c.call(rctx, g.addr, protocol.APIFetch, req, &resp)
			ch <- reply{g, resp, err}
		}(g)
	}
	var firstErr error
	for range groups {
		r := <-ch
		if r.err != nil {
			for _, i := range r.g.idx {
				results[i].Err = protocol.ErrNetwork
			}
			if firstErr == nil {
				firstErr = r.err
			}
			continue
		}
		for j, pr := range r.resp.Partitions {
			if j >= len(r.g.idx) {
				break
			}
			i := r.g.idx[j]
			results[i].Err = pr.Err
			results[i].HW = pr.HighWatermark
			for _, rec := range pr.Records {
				results[i].Records = append(results[i].Records, Record{
					Topic: pr.Topic, Partition: pr.Partition, Offset: rec.Offset,
					Timestamp: time.UnixMilli(rec.Timestamp), Key: rec.Key, Value: rec.Value,
				})
			}
		}
	}
	// A single-target fetch that failed at the network level is returned
	// as an error so the caller's retry loop refreshes metadata.
	if len(targets) == 1 && results[0].Err == protocol.ErrNetwork && firstErr != nil {
		return nil, firstErr
	}
	if len(targets) == 1 && results[0].Err.Retriable() {
		return nil, results[0].Err.AsError("fetch")
	}
	return results, nil
}
