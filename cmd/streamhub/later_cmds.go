package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/Shashwat0906/StreamHub/client"
)

func runGroup(args []string) error {
	sub, rest, err := subcommand(args, "list", "describe")
	if err != nil {
		return err
	}
	fs := flag.NewFlagSet("group "+sub, flag.ExitOnError)
	bs := bootstrapFlag(fs)
	name := fs.String("group", "", "group id (describe)")
	fs.Parse(rest)
	c, err := newClient(*bs)
	if err != nil {
		return err
	}
	defer c.Close()
	ctx, cancel := signalContext()
	defer cancel()

	switch sub {
	case "list":
		groups, err := c.ListGroups(ctx)
		if err != nil {
			return err
		}
		tw := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
		fmt.Fprintln(tw, "GROUP\tSTATE")
		for _, g := range groups {
			fmt.Fprintf(tw, "%s\t%s\n", g.Group, g.State)
		}
		tw.Flush()
	case "describe":
		if *name == "" {
			return errors.New("--group is required")
		}
		info, err := c.DescribeGroup(ctx, *name)
		if err != nil {
			return err
		}
		fmt.Printf("group %s: state=%s strategy=%s generation=%d members=%d\n\n",
			info.Group, info.State, info.Strategy, info.Generation, len(info.Members))
		tw := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
		fmt.Fprintln(tw, "MEMBER\tCLIENT\tASSIGNMENT")
		for _, m := range info.Members {
			var parts []string
			for _, tp := range m.Assignment {
				parts = append(parts, fmt.Sprintf("%s%v", tp.Topic, tp.Partitions))
			}
			fmt.Fprintf(tw, "%s\t%s\t%s\n", m.MemberID, m.ClientID, strings.Join(parts, " "))
		}
		tw.Flush()
		fmt.Println()
		tw = tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
		fmt.Fprintln(tw, "TOPIC\tPARTITION\tCOMMITTED\tEND\tLAG")
		for _, o := range info.Offsets {
			fmt.Fprintf(tw, "%s\t%d\t%d\t%d\t%d\n", o.Topic, o.Partition, o.Committed, o.End, o.Lag)
		}
		tw.Flush()
	}
	return nil
}

// consumeGroup consumes as a group member, committing after each batch is
// printed (at-least-once).
func consumeGroup(ctx context.Context, c *client.Client, group, topic, strategy, from string,
	print func(client.Record) bool, idle func(int) bool) error {
	reset := client.ResetEarliest
	if from == "latest" {
		reset = client.ResetLatest
	}
	gc, err := c.NewGroupConsumer(client.GroupConfig{
		Group: group, Topics: []string{topic}, Strategy: strategy,
		Fetch: client.FetchConfig{Reset: reset, MaxWait: 500 * time.Millisecond},
	})
	if err != nil {
		return err
	}
	defer func() {
		cctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		gc.Close(cctx)
	}()
	for ctx.Err() == nil {
		recs, err := gc.Poll(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		done := false
		for i, r := range recs {
			if print(r) {
				done = true
				// Do not commit records we stop before printing: rewind each
				// partition to its first unprinted record.
				rewound := map[int32]bool{}
				for _, rest := range recs[i+1:] {
					if !rewound[rest.Partition] {
						gc.Seek(rest.Topic, rest.Partition, rest.Offset)
						rewound[rest.Partition] = true
					}
				}
				break
			}
		}
		if len(recs) > 0 {
			if err := gc.Commit(ctx); err != nil {
				fmt.Fprintln(os.Stderr, "commit failed (records may be redelivered):", err)
			}
		}
		if done || idle(len(recs)) {
			return nil
		}
	}
	return nil
}
