package client_test

import (
	"context"
	"fmt"

	"github.com/Shashwat0906/StreamHub/client"
)

// This example mirrors the README. It is compiled (not run) by go test.
func Example() {
	ctx := context.Background()
	process := func(client.Record) {}

	c, _ := client.New(client.Config{Bootstrap: []string{"127.0.0.1:9092"}})
	defer c.Close()

	c.CreateTopic(ctx, client.TopicSpec{Name: "orders", Partitions: 3, ReplicationFactor: 3})

	p, _ := c.NewProducer(client.ProducerConfig{Idempotent: true}) // acks=all by default
	d := p.SendSync(ctx, client.Message{Topic: "orders", Key: []byte("customer-42"), Value: []byte("paid")})
	fmt.Println(d.Partition, d.Offset, d.Err)

	gc, _ := c.NewGroupConsumer(client.GroupConfig{Group: "billing", Topics: []string{"orders"}})
	for {
		recs, err := gc.Poll(ctx)
		if err != nil {
			break
		}
		for _, r := range recs {
			process(r)
		}
		gc.Commit(ctx) // commit after processing = at-least-once
	}
	gc.Close(ctx)
}
