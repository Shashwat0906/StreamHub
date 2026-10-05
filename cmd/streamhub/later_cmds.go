package main

import (
	"context"
	"errors"

	"github.com/Shashwat0906/StreamHub/client"
)

func runGroup(args []string) error { return errors.New("consumer groups arrive in Phase 4") }
func runPerf(args []string) error  { return errors.New("perf tool arrives in Phase 6") }

func consumeGroup(ctx context.Context, c *client.Client, group, topic, strategy, from string,
	print func(client.Record) bool, idle func(int) bool) error {
	return errors.New("consumer groups arrive in Phase 4")
}
