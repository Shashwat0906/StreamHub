package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Shashwat0906/StreamHub/client"
	"github.com/Shashwat0906/StreamHub/internal/protocol"
)

func runPerf(args []string) error {
	sub, rest, err := subcommand(args, "produce", "consume")
	if err != nil {
		return err
	}
	if sub == "produce" {
		return perfProduce(rest)
	}
	return perfConsume(rest)
}

// perfProduce sends N records of a fixed size as fast as the producer
// allows (bounded by --inflight outstanding records) and reports
// throughput and per-record delivery latency (send call → ack).
func perfProduce(args []string) error {
	fs := flag.NewFlagSet("perf produce", flag.ExitOnError)
	bs := bootstrapFlag(fs)
	topic := fs.String("topic", "perf", "topic (must exist)")
	n := fs.Int("records", 100000, "number of records")
	size := fs.Int("size", 100, "value size in bytes")
	acks := fs.String("acks", "all", "0, 1 or all")
	idem := fs.Bool("idempotent", false, "idempotent producer (acks=all only)")
	linger := fs.Duration("linger", 5*time.Millisecond, "batch linger")
	batch := fs.Int("batch", 1000, "max records per batch")
	inflight := fs.Int("inflight", 50000, "max records sent but not yet acknowledged")
	keyed := fs.Bool("keyed", false, "use random keys (hash partitioning) instead of round-robin")
	fs.Parse(args)

	cfg := client.ProducerConfig{Linger: *linger, BatchMaxRecords: *batch, Idempotent: *idem, QueueSize: *inflight}
	switch *acks {
	case "0":
		cfg.Acks, cfg.AcksSet = protocol.AcksNone, true
	case "1":
		cfg.Acks, cfg.AcksSet = protocol.AcksLeader, true
	case "all", "-1":
		cfg.Acks, cfg.AcksSet = protocol.AcksAll, true
	default:
		return fmt.Errorf("bad --acks %q", *acks)
	}
	c, err := newClient(*bs)
	if err != nil {
		return err
	}
	defer c.Close()
	ctx, cancel := signalContext()
	defer cancel()
	if _, err := c.Partitions(ctx, *topic); err != nil {
		return fmt.Errorf("topic %q: %w (create it first)", *topic, err)
	}
	p, err := c.NewProducer(cfg)
	if err != nil {
		return err
	}
	defer p.Close()

	value := make([]byte, *size)
	rand.Read(value)
	lat := make([]time.Duration, *n)
	var failed atomic.Int64
	sem := make(chan struct{}, *inflight)
	var wg sync.WaitGroup
	start := time.Now()
	for i := 0; i < *n; i++ {
		sem <- struct{}{}
		wg.Add(1)
		m := client.Message{Topic: *topic, Value: value}
		if *keyed {
			m.Key = []byte(fmt.Sprintf("k%d", rand.Intn(1<<20)))
		}
		sent := time.Now()
		idx := i
		if err := p.Send(ctx, m, func(d client.Delivery) {
			lat[idx] = time.Since(sent)
			if d.Err != nil {
				failed.Add(1)
			}
			<-sem
			wg.Done()
		}); err != nil {
			return err
		}
	}
	wg.Wait()
	elapsed := time.Since(start)

	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	pct := func(q float64) time.Duration { return lat[min(len(lat)-1, int(q*float64(len(lat))))] }
	mb := float64(*n) * float64(*size) / (1 << 20)
	fmt.Printf("produce: records=%d size=%dB acks=%s idempotent=%v failed=%d\n", *n, *size, *acks, *idem, failed.Load())
	fmt.Printf("  elapsed=%.2fs  throughput=%.0f records/s  %.2f MiB/s (values only)\n",
		elapsed.Seconds(), float64(*n)/elapsed.Seconds(), mb/elapsed.Seconds())
	fmt.Printf("  latency p50=%v p95=%v p99=%v max=%v\n",
		pct(0.50).Round(time.Microsecond), pct(0.95).Round(time.Microsecond),
		pct(0.99).Round(time.Microsecond), lat[len(lat)-1].Round(time.Microsecond))
	if failed.Load() > 0 {
		return fmt.Errorf("%d records failed", failed.Load())
	}
	return nil
}

// perfConsume reads N records from all partitions (from earliest) with one
// partition consumer per partition and reports throughput.
func perfConsume(args []string) error {
	fs := flag.NewFlagSet("perf consume", flag.ExitOnError)
	bs := bootstrapFlag(fs)
	topic := fs.String("topic", "perf", "topic")
	n := fs.Int("records", 100000, "stop after this many records")
	maxBytes := fs.Int("fetch-bytes", 1<<20, "max bytes per partition per fetch")
	fs.Parse(args)

	c, err := newClient(*bs)
	if err != nil {
		return err
	}
	defer c.Close()
	ctx, cancel := signalContext()
	defer cancel()
	np, err := c.Partitions(ctx, *topic)
	if err != nil {
		return err
	}
	var total, bytes atomic.Int64
	start := time.Now()
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	var wg sync.WaitGroup
	errCh := make(chan error, np)
	for part := 0; part < np; part++ {
		pc, err := c.ConsumePartition(ctx, *topic, int32(part), protocol.OffsetEarliest,
			client.FetchConfig{MaxWait: 100 * time.Millisecond, MaxBytes: int32(*maxBytes)})
		if err != nil {
			return err
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			for runCtx.Err() == nil {
				recs, err := pc.Poll(runCtx)
				if err != nil {
					if runCtx.Err() == nil {
						errCh <- err
					}
					return
				}
				for _, r := range recs {
					bytes.Add(int64(len(r.Value) + len(r.Key)))
				}
				if total.Add(int64(len(recs))) >= int64(*n) {
					stop()
				}
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)
	select {
	case err := <-errCh:
		return err
	default:
	}
	if total.Load() < int64(*n) {
		return errors.New("interrupted before reading all records")
	}
	fmt.Printf("consume: records=%d partitions=%d elapsed=%.2fs throughput=%.0f records/s %.2f MiB/s\n",
		total.Load(), np, elapsed.Seconds(), float64(total.Load())/elapsed.Seconds(),
		float64(bytes.Load())/(1<<20)/elapsed.Seconds())
	return nil
}
