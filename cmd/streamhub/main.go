// Command streamhub runs a StreamHub broker and provides admin, produce
// and consume tooling.
//
//	streamhub broker  --id 1 --listen :9092 --data-dir ./data/1 [--peers 1=h:9092,2=h:9093,3=h:9094]
//	streamhub topic   create|delete|list|describe ...
//	streamhub produce --topic t [--key k] [--value v | stdin lines]
//	streamhub consume --topic t [--group g | --partition p] [--from earliest|latest|N]
//	streamhub cluster describe
//	streamhub group   list|describe ...
//	streamhub perf    produce|consume ...
package main

import (
	"fmt"
	"os"
	"strings"
)

func usage() {
	fmt.Fprint(os.Stderr, `streamhub - a Kafka-inspired distributed message broker

Usage:
  streamhub broker   [flags]                 run a broker
  streamhub topic    create|delete|list|describe [flags]
  streamhub produce  [flags]                 produce records (args or stdin)
  streamhub consume  [flags]                 consume records
  streamhub cluster  describe [flags]        show brokers, controller, partitions
  streamhub group    list|describe [flags]   consumer groups
  streamhub perf     produce|consume [flags] load generator

Client commands read the bootstrap list from --bootstrap or $STREAMHUB_BOOTSTRAP
(default localhost:9092). Run "streamhub <command> -h" for flags.
`)
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	cmd, args := os.Args[1], os.Args[2:]
	var err error
	switch cmd {
	case "broker":
		err = runBroker(args)
	case "topic":
		err = runTopic(args)
	case "produce":
		err = runProduce(args)
	case "consume":
		err = runConsume(args)
	case "cluster":
		err = runCluster(args)
	case "group":
		err = runGroup(args)
	case "perf":
		err = runPerf(args)
	case "-h", "--help", "help":
		usage()
		return
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n", cmd)
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

// subcommand splits "topic create ..." into ("create", rest).
func subcommand(args []string, valid ...string) (string, []string, error) {
	if len(args) == 0 || strings.HasPrefix(args[0], "-") {
		return "", nil, fmt.Errorf("expected one of: %s", strings.Join(valid, ", "))
	}
	for _, v := range valid {
		if args[0] == v {
			return v, args[1:], nil
		}
	}
	return "", nil, fmt.Errorf("unknown subcommand %q (expected one of: %s)", args[0], strings.Join(valid, ", "))
}
