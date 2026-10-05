package client

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Shashwat0906/StreamHub/internal/protocol"
	"github.com/Shashwat0906/StreamHub/internal/storage"
)

// ProducerConfig configures a Producer.
type ProducerConfig struct {
	// Acks: protocol.AcksNone (0), AcksLeader (1) or AcksAll (-1, default).
	Acks int16
	// AcksSet must be true to use Acks=0 (the zero value means "default").
	AcksSet bool
	// BatchMaxRecords / BatchMaxBytes close a batch early when reached.
	BatchMaxRecords int
	BatchMaxBytes   int
	// Linger is how long a lane waits for more records before sending a
	// non-full batch. Higher = bigger batches, more latency.
	Linger time.Duration
	// DeliveryTimeout bounds the total time (including retries) to deliver
	// one batch.
	DeliveryTimeout time.Duration
	// RequestTimeout is the per-attempt broker timeout (acks=all wait).
	RequestTimeout time.Duration
	// Idempotent enables producer IDs + sequence numbers so retried batches
	// are not written twice.
	Idempotent bool
	// Partitioner defaults to DefaultPartitioner.
	Partitioner Partitioner
	// QueueSize per partition lane (backpressure).
	QueueSize int
}

func (c *ProducerConfig) setDefaults() {
	if !c.AcksSet && c.Acks == 0 {
		c.Acks = protocol.AcksAll
	}
	if c.BatchMaxRecords == 0 {
		c.BatchMaxRecords = 1000
	}
	if c.BatchMaxBytes == 0 {
		c.BatchMaxBytes = 512 << 10
	}
	if c.Linger == 0 {
		c.Linger = 5 * time.Millisecond
	}
	if c.DeliveryTimeout == 0 {
		c.DeliveryTimeout = 60 * time.Second
	}
	if c.RequestTimeout == 0 {
		c.RequestTimeout = 10 * time.Second
	}
	if c.Partitioner == nil {
		c.Partitioner = &DefaultPartitioner{}
	}
	if c.QueueSize == 0 {
		c.QueueSize = 10000
	}
}

// Message is a record to produce.
type Message struct {
	Topic string
	// Partition is used only when ManualPartition is true; otherwise the
	// partitioner chooses (hash of Key, or round-robin when Key is nil).
	Partition       int32
	ManualPartition bool
	Key             []byte
	Value           []byte
	Timestamp       time.Time // zero = broker time
}

// Delivery is the outcome of one message.
type Delivery struct {
	Topic     string
	Partition int32
	Offset    int64 // -1 when unknown (acks=0, or duplicate of an older batch)
	Err       error
}

// ErrProducerClosed is returned for sends after Close.
var ErrProducerClosed = errors.New("client: producer closed")

// Producer batches messages per partition and sends them with retries.
//
// Ordering: each partition has one "lane" goroutine that sends at most one
// batch at a time and retries it until it succeeds or times out, so records
// of a partition are written in send order.
type Producer struct {
	c   *Client
	cfg ProducerConfig

	mu      sync.Mutex
	lanes   map[laneKey]*lane
	closed  bool
	pidMu   sync.Mutex
	pid     int64 // -1 if not idempotent / not yet initialised
	pidGen  int   // bumps when pid is reset; lanes reset sequences
	wg      sync.WaitGroup
	pending sync.WaitGroup
}

type laneKey struct {
	topic     string
	partition int32
}

type pendingMsg struct {
	msg  Message
	done func(Delivery)
}

type lane struct {
	key    laneKey
	in     chan pendingMsg
	seq    int32 // next sequence number (idempotence)
	pidGen int
}

// NewProducer creates a producer on top of a shared Client.
func (c *Client) NewProducer(cfg ProducerConfig) (*Producer, error) {
	cfg.setDefaults()
	if cfg.Idempotent && cfg.Acks != protocol.AcksAll {
		return nil, errors.New("client: idempotence requires acks=all")
	}
	return &Producer{c: c, cfg: cfg, lanes: map[laneKey]*lane{}, pid: -1}, nil
}

// producerID returns the current producer ID, allocating one from the
// controller on first use.
func (p *Producer) producerID(ctx context.Context) (int64, int, error) {
	if !p.cfg.Idempotent {
		return -1, 0, nil
	}
	p.pidMu.Lock()
	defer p.pidMu.Unlock()
	if p.pid >= 0 {
		return p.pid, p.pidGen, nil
	}
	var pid int64
	err := p.c.retry(ctx, func() error {
		addr, err := p.c.controllerAddr(ctx)
		if err != nil {
			return err
		}
		var resp protocol.InitProducerIDResponse
		if err := p.c.call(ctx, addr, protocol.APIInitProducerID, &protocol.InitProducerIDRequest{}, &resp); err != nil {
			return err
		}
		pid = resp.ProducerID
		return resp.Err.AsError("init producer id")
	})
	if err != nil {
		return -1, 0, err
	}
	p.pid = pid
	return pid, p.pidGen, nil
}

// resetProducerID abandons the current ID after a batch failed for good:
// its sequence numbers now have a gap the broker would reject forever.
func (p *Producer) resetProducerID(gen int) {
	p.pidMu.Lock()
	defer p.pidMu.Unlock()
	if p.pidGen == gen {
		p.pid = -1
		p.pidGen++
	}
}

// Send enqueues a message; done (may be nil) is called once with the
// outcome, from a producer goroutine.
func (p *Producer) Send(ctx context.Context, m Message, done func(Delivery)) error {
	if done == nil {
		done = func(Delivery) {}
	}
	part := m.Partition
	if !m.ManualPartition {
		n, err := p.c.Partitions(ctx, m.Topic)
		if err != nil {
			return err
		}
		part = p.cfg.Partitioner.Partition(m.Topic, m.Key, n)
	}
	m.Partition = part
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return ErrProducerClosed
	}
	ln := p.lanes[laneKey{m.Topic, part}]
	if ln == nil {
		ln = &lane{key: laneKey{m.Topic, part}, in: make(chan pendingMsg, p.cfg.QueueSize)}
		p.lanes[ln.key] = ln
		p.wg.Add(1)
		go p.runLane(ln)
	}
	p.pending.Add(1)
	p.mu.Unlock()
	wrapped := func(d Delivery) { done(d); p.pending.Done() }
	select {
	case ln.in <- pendingMsg{msg: m, done: wrapped}:
		return nil
	case <-ctx.Done():
		p.pending.Done()
		return ctx.Err()
	}
}

// SendSync sends one message and waits for its delivery.
func (p *Producer) SendSync(ctx context.Context, m Message) Delivery {
	ch := make(chan Delivery, 1)
	if err := p.Send(ctx, m, func(d Delivery) { ch <- d }); err != nil {
		return Delivery{Topic: m.Topic, Partition: m.Partition, Offset: -1, Err: err}
	}
	select {
	case d := <-ch:
		return d
	case <-ctx.Done():
		return Delivery{Topic: m.Topic, Partition: m.Partition, Offset: -1, Err: ctx.Err()}
	}
}

// Flush waits until every message sent so far has been delivered or failed.
func (p *Producer) Flush(ctx context.Context) error {
	done := make(chan struct{})
	go func() { p.pending.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close flushes and stops all lanes.
func (p *Producer) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	for _, ln := range p.lanes {
		close(ln.in)
	}
	p.mu.Unlock()
	p.wg.Wait()
	return nil
}

// runLane collects records into batches and sends them one at a time.
func (p *Producer) runLane(ln *lane) {
	defer p.wg.Done()
	var batch []pendingMsg
	bytes := 0
	for {
		// Block for the first record of a batch.
		first, ok := <-ln.in
		if !ok {
			return
		}
		batch = append(batch[:0], first)
		bytes = len(first.msg.Key) + len(first.msg.Value)
		linger := time.NewTimer(p.cfg.Linger)
	fill:
		for len(batch) < p.cfg.BatchMaxRecords && bytes < p.cfg.BatchMaxBytes {
			select {
			case m, ok := <-ln.in:
				if !ok {
					break fill
				}
				batch = append(batch, m)
				bytes += len(m.msg.Key) + len(m.msg.Value)
			case <-linger.C:
				break fill
			}
		}
		linger.Stop()
		p.sendBatch(ln, batch)
	}
}

// sendBatch delivers one batch, retrying retriable errors until the
// delivery timeout. With idempotence the retry reuses the same sequence
// numbers, so a batch the broker already wrote is not written again.
func (p *Producer) sendBatch(ln *lane, batch []pendingMsg) {
	ctx, cancel := context.WithTimeout(context.Background(), p.cfg.DeliveryTimeout)
	defer cancel()

	recs := make([]storage.Record, len(batch))
	for i, m := range batch {
		var ts int64
		if !m.msg.Timestamp.IsZero() {
			ts = m.msg.Timestamp.UnixMilli()
		}
		recs[i] = storage.Record{Key: m.msg.Key, Value: m.msg.Value, Timestamp: ts}
	}

	pid, gen, err := p.producerID(ctx)
	if err != nil {
		p.finish(ln, batch, -1, err)
		return
	}
	if gen != ln.pidGen {
		ln.pidGen, ln.seq = gen, 0 // new producer ID: sequences restart at 0
	}
	baseSeq := int32(-1)
	if pid >= 0 {
		baseSeq = ln.seq
	}

	var base int64 = -1
	err = p.c.retry(ctx, func() error {
		_, addr, _, err := p.c.leader(ctx, ln.key.topic, ln.key.partition)
		if err != nil {
			return err
		}
		req := &protocol.ProduceRequest{
			Acks: p.cfg.Acks, TimeoutMs: int32(p.cfg.RequestTimeout / time.Millisecond),
			Topic: ln.key.topic, Partition: ln.key.partition,
			ProducerID: pid, BaseSequence: baseSeq, Records: recs,
		}
		if p.cfg.Acks == protocol.AcksNone {
			return p.c.call(ctx, addr, protocol.APIProduce, req, nil)
		}
		rctx, rcancel := context.WithTimeout(ctx, p.cfg.RequestTimeout+5*time.Second)
		defer rcancel()
		var resp protocol.ProduceResponse
		if err := p.c.call(rctx, addr, protocol.APIProduce, req, &resp); err != nil {
			return err
		}
		switch resp.Err {
		case protocol.ErrNone:
			base = resp.BaseOffset
			return nil
		case protocol.ErrDuplicateSequence:
			return nil // already written earlier; offset unknown
		}
		return resp.Err.AsError(resp.ErrMsg)
	})
	if pid >= 0 {
		if err == nil {
			ln.seq += int32(len(batch))
		} else {
			// The batch may or may not have been written; either way our
			// sequence space is now ambiguous. Start a fresh producer ID.
			p.resetProducerID(gen)
		}
	}
	p.finish(ln, batch, base, err)
}

func (p *Producer) finish(ln *lane, batch []pendingMsg, base int64, err error) {
	for i, m := range batch {
		d := Delivery{Topic: ln.key.topic, Partition: ln.key.partition, Offset: -1, Err: err}
		if err == nil && base >= 0 {
			d.Offset = base + int64(i)
		}
		if err != nil {
			d.Err = fmt.Errorf("produce %s-%d: %w", ln.key.topic, ln.key.partition, err)
		}
		m.done(d)
	}
}
