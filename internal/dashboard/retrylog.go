package dashboard

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/Shashwat0906/StreamHub/client"
	"github.com/Shashwat0906/StreamHub/internal/protocol"
)

// RetryLogTopic stores one record per DLQ retry, so "already retried" and
// redrive counts survive dashboard restarts (they used to live only in
// memory). Internal topic: hidden from the UI like __consumer_offsets.
const RetryLogTopic = "__dlq_retries"

type retryMark struct {
	ID          string `json:"id"`          // DLQ entry that was re-published
	Fingerprint string `json:"fingerprint"` // topic/key/payload of the re-published message
	RetryCount  int    `json:"retryCount"`  // redrives of this message so far
	NewMessage  string `json:"newMessage"`  // where it was re-published
	At          int64  `json:"at"`
}

// loadRetryLog replays __dlq_retries into memory once per dashboard
// process. It succeeds trivially when the topic does not exist yet.
func (d *demo) loadRetryLog(ctx context.Context) error {
	d.retryLogMu.Lock()
	defer d.retryLogMu.Unlock()
	if d.retryLogLoaded {
		return nil
	}
	if _, err := d.g.client.Topic(ctx, RetryLogTopic); err != nil {
		var pe *protocol.Error
		if errors.As(err, &pe) && pe.Code == protocol.ErrUnknownTopicOrPartition {
			d.retryLogLoaded = true // nothing retried yet
			return nil
		}
		return err
	}
	end, err := d.g.client.ListOffsets(ctx, RetryLogTopic, 0, protocol.OffsetLatest)
	if err != nil {
		return err
	}
	start, err := d.g.client.ListOffsets(ctx, RetryLogTopic, 0, protocol.OffsetEarliest)
	if err != nil {
		return err
	}
	if start < end {
		pc, err := d.g.client.ConsumePartition(ctx, RetryLogTopic, 0, start, client.FetchConfig{MaxWait: 50 * time.Millisecond})
		if err != nil {
			return err
		}
		for pc.Offset() < end {
			recs, err := pc.Poll(ctx)
			if err != nil {
				return err
			}
			if len(recs) == 0 {
				break
			}
			for _, r := range recs {
				var m retryMark
				if json.Unmarshal(r.Value, &m) != nil || m.ID == "" {
					continue
				}
				d.mu.Lock()
				d.retried[m.ID] = true
				if m.RetryCount > d.priorRetr[m.Fingerprint] {
					d.priorRetr[m.Fingerprint] = m.RetryCount
				}
				d.mu.Unlock()
			}
		}
	}
	d.retryLogLoaded = true
	return nil
}

// recordRetry appends a retry mark (acks=all). It is written after the
// re-publish succeeded; without transactions there is a small window in
// which the message is re-published but the mark is not yet durable (a
// crash there would allow one extra retry of the same entry).
func (d *demo) recordRetry(ctx context.Context, m retryMark) error {
	if err := d.ensureTopic(ctx, RetryLogTopic); err != nil {
		return err
	}
	b, _ := json.Marshal(m)
	res := d.g.produce(ctx, ProduceRequest{Topic: RetryLogTopic, Key: m.ID, Value: string(b), Acks: "all"})
	if res.Status != "acknowledged" {
		return fmt.Errorf("retry log write failed: %s", res.Error)
	}
	return nil
}
