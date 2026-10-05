package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/Shashwat0906/StreamHub/client"
	"github.com/Shashwat0906/StreamHub/internal/protocol"
)

func bootstrapFlag(fs *flag.FlagSet) *string {
	def := os.Getenv("STREAMHUB_BOOTSTRAP")
	if def == "" {
		def = "localhost:9092"
	}
	return fs.String("bootstrap", def, "comma-separated broker addresses")
}

func newClient(bootstrap string) (*client.Client, error) {
	return client.New(client.Config{Bootstrap: strings.Split(bootstrap, ","), ClientID: "streamhub-cli"})
}

// signalContext is cancelled on Ctrl-C.
func signalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}

type kvFlag map[string]string

func (k kvFlag) String() string { return fmt.Sprint(map[string]string(k)) }
func (k kvFlag) Set(s string) error {
	kv := strings.SplitN(s, "=", 2)
	if len(kv) != 2 {
		return fmt.Errorf("want key=value, got %q", s)
	}
	k[kv[0]] = kv[1]
	return nil
}

// ---------------------------------------------------------------- topic

func runTopic(args []string) error {
	sub, rest, err := subcommand(args, "create", "delete", "list", "describe")
	if err != nil {
		return err
	}
	fs := flag.NewFlagSet("topic "+sub, flag.ExitOnError)
	bs := bootstrapFlag(fs)
	name := fs.String("name", "", "topic name")
	parts := fs.Int("partitions", 3, "number of partitions (create)")
	rf := fs.Int("replication-factor", 0, "replication factor (create; 0 = broker default)")
	configs := kvFlag{}
	fs.Var(configs, "config", "topic config key=value (repeatable): retention.ms, retention.bytes, segment.bytes, min.insync.replicas")
	fs.Parse(rest)

	c, err := newClient(*bs)
	if err != nil {
		return err
	}
	defer c.Close()
	ctx, cancel := signalContext()
	defer cancel()

	switch sub {
	case "create":
		if *name == "" {
			return errors.New("--name is required")
		}
		if err := c.CreateTopic(ctx, client.TopicSpec{Name: *name, Partitions: int32(*parts), ReplicationFactor: int16(*rf), Configs: configs}); err != nil {
			return err
		}
		fmt.Printf("created topic %q\n", *name)
	case "delete":
		if *name == "" {
			return errors.New("--name is required")
		}
		if err := c.DeleteTopic(ctx, *name); err != nil {
			return err
		}
		fmt.Printf("deleted topic %q\n", *name)
	case "list":
		info, err := c.DescribeCluster(ctx)
		if err != nil {
			return err
		}
		for _, t := range info.Topics {
			fmt.Printf("%s\t%d partitions\n", t.Name, len(t.Partitions))
		}
	case "describe":
		info, err := c.DescribeCluster(ctx)
		if err != nil {
			return err
		}
		printTopics(info, *name)
	}
	return nil
}

func printTopics(info client.ClusterInfo, only string) {
	tw := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "TOPIC\tPARTITION\tLEADER\tEPOCH\tREPLICAS\tISR")
	for _, t := range info.Topics {
		if only != "" && t.Name != only {
			continue
		}
		for _, p := range t.Partitions {
			fmt.Fprintf(tw, "%s\t%d\t%d\t%d\t%v\t%v\n", t.Name, p.ID, p.Leader, p.LeaderEpoch, p.Replicas, p.ISR)
		}
	}
	tw.Flush()
}

// ---------------------------------------------------------------- cluster

func runCluster(args []string) error {
	_, rest, err := subcommand(args, "describe")
	if err != nil {
		return err
	}
	fs := flag.NewFlagSet("cluster describe", flag.ExitOnError)
	bs := bootstrapFlag(fs)
	fs.Parse(rest)
	c, err := newClient(*bs)
	if err != nil {
		return err
	}
	defer c.Close()
	ctx, cancel := signalContext()
	defer cancel()
	info, err := c.DescribeCluster(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("controller: broker %d\n\n", info.ControllerID)
	tw := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "BROKER\tADDRESS\tSTATUS")
	for _, b := range info.Brokers {
		status := "alive"
		if b.Fenced {
			status = "fenced"
		}
		fmt.Fprintf(tw, "%d\t%s\t%s\n", b.ID, b.Addr, status)
	}
	tw.Flush()
	fmt.Println()
	printTopics(info, "")
	return nil
}

// ---------------------------------------------------------------- produce

func runProduce(args []string) error {
	fs := flag.NewFlagSet("produce", flag.ExitOnError)
	bs := bootstrapFlag(fs)
	topic := fs.String("topic", "", "topic")
	key := fs.String("key", "", "record key (single record mode)")
	value := fs.String("value", "", "record value (single record mode); if empty, read lines from stdin")
	sep := fs.String("key-separator", "", "stdin mode: split each line into key<sep>value")
	partition := fs.Int("partition", -1, "explicit partition (-1 = partitioner)")
	acks := fs.String("acks", "all", "0, 1 or all")
	idem := fs.Bool("idempotent", true, "enable idempotent producer (requires acks=all)")
	fs.Parse(args)
	if *topic == "" {
		return errors.New("--topic is required")
	}
	cfg := client.ProducerConfig{}
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
	cfg.Idempotent = *idem && cfg.Acks == protocol.AcksAll

	c, err := newClient(*bs)
	if err != nil {
		return err
	}
	defer c.Close()
	p, err := c.NewProducer(cfg)
	if err != nil {
		return err
	}
	defer p.Close()
	ctx, cancel := signalContext()
	defer cancel()

	mk := func(k, v string) client.Message {
		m := client.Message{Topic: *topic, Value: []byte(v)}
		if k != "" {
			m.Key = []byte(k)
		}
		if *partition >= 0 {
			m.Partition, m.ManualPartition = int32(*partition), true
		}
		return m
	}
	if *value != "" {
		d := p.SendSync(ctx, mk(*key, *value))
		if d.Err != nil {
			return d.Err
		}
		fmt.Printf("%s-%d@%d\n", d.Topic, d.Partition, d.Offset)
		return nil
	}

	var failed atomic.Int64
	sent := 0
	sc := bufio.NewScanner(os.Stdin)
	sc.Buffer(make([]byte, 1<<20), 16<<20)
	for sc.Scan() {
		line := sc.Text()
		k, v := *key, line
		if *sep != "" {
			if i := strings.Index(line, *sep); i >= 0 {
				k, v = line[:i], line[i+len(*sep):]
			}
		}
		sent++
		if err := p.Send(ctx, mk(k, v), func(d client.Delivery) {
			if d.Err != nil {
				failed.Add(1)
				fmt.Fprintln(os.Stderr, "delivery failed:", d.Err)
			}
		}); err != nil {
			return err
		}
	}
	if err := p.Flush(ctx); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "sent %d records, %d failed\n", sent, failed.Load())
	if failed.Load() > 0 {
		return fmt.Errorf("%d records failed", failed.Load())
	}
	return nil
}

// ---------------------------------------------------------------- consume

func runConsume(args []string) error {
	fs := flag.NewFlagSet("consume", flag.ExitOnError)
	bs := bootstrapFlag(fs)
	topic := fs.String("topic", "", "topic")
	group := fs.String("group", "", "consumer group (commits offsets, shares partitions)")
	partition := fs.Int("partition", -1, "single partition to read (no group); -1 = all partitions")
	from := fs.String("from", "earliest", "earliest, latest or an offset (no-group mode / no committed offset)")
	strategy := fs.String("strategy", "range", "group assignment strategy: range or roundrobin")
	count := fs.Int("count", 0, "exit after this many records (0 = run until Ctrl-C)")
	idle := fs.Duration("idle-timeout", 0, "exit after this long without records (0 = never)")
	format := fs.String("format", "%k\t%v", "output format: %t topic %p partition %o offset %k key %v value %T timestamp")
	fs.Parse(args)
	if *topic == "" {
		return errors.New("--topic is required")
	}
	c, err := newClient(*bs)
	if err != nil {
		return err
	}
	defer c.Close()
	ctx, cancel := signalContext()
	defer cancel()

	n := 0
	lastData := time.Now()
	print := func(r client.Record) bool {
		out := strings.NewReplacer(
			"%t", r.Topic, "%p", strconv.Itoa(int(r.Partition)), "%o", strconv.FormatInt(r.Offset, 10),
			"%k", string(r.Key), "%v", string(r.Value), "%T", r.Timestamp.Format(time.RFC3339Nano),
		).Replace(*format)
		fmt.Println(out)
		n++
		return *count > 0 && n >= *count
	}
	idleExpired := func(got int) bool {
		if got > 0 {
			lastData = time.Now()
			return false
		}
		return *idle > 0 && time.Since(lastData) > *idle
	}

	if *group != "" {
		return consumeGroup(ctx, c, *group, *topic, *strategy, *from, print, idleExpired)
	}

	reset := client.ResetEarliest
	start := protocol.OffsetEarliest
	switch *from {
	case "earliest":
	case "latest":
		start, reset = protocol.OffsetLatest, client.ResetLatest
	default:
		v, err := strconv.ParseInt(*from, 10, 64)
		if err != nil {
			return fmt.Errorf("bad --from %q", *from)
		}
		start = v
	}
	parts := []int32{int32(*partition)}
	if *partition < 0 {
		np, err := c.Partitions(ctx, *topic)
		if err != nil {
			return err
		}
		parts = parts[:0]
		for i := 0; i < np; i++ {
			parts = append(parts, int32(i))
		}
	}
	var pcs []*client.PartitionConsumer
	for _, p := range parts {
		pc, err := c.ConsumePartition(ctx, *topic, p, start, client.FetchConfig{Reset: reset, MaxWait: 200 * time.Millisecond})
		if err != nil {
			return err
		}
		pcs = append(pcs, pc)
	}
	for ctx.Err() == nil {
		got := 0
		for _, pc := range pcs {
			recs, err := pc.Poll(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return nil
				}
				return err
			}
			got += len(recs)
			sort.Slice(recs, func(i, j int) bool { return recs[i].Offset < recs[j].Offset })
			for _, r := range recs {
				if print(r) {
					return nil
				}
			}
		}
		if idleExpired(got) {
			return nil
		}
	}
	return nil
}
