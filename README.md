# StreamHub

A Kafka-inspired distributed message broker written from scratch in Go,
using only the standard library.

Producers publish to topics. Topics are split into partitions, and each
partition is a replicated, append-only commit log spread across brokers.
Consumers read independently, alone or in consumer groups with committed
offsets. Cluster metadata is managed by a small Raft quorum (KRaft-style).

```
producer ──Produce(acks=all)──▶ leader ◀──Fetch── followers (ISR)
                                  │
consumer group ──Fetch (long poll)┘   controller = Raft leader (metadata log)
```

* **Design:** [`PLAN.md`](PLAN.md) covers architecture, wire protocol, on-disk format and failure matrix.
* **Concepts explained simply:** [`docs/LEARNING.md`](docs/LEARNING.md) (interview prep).
* **Benchmarks:** [`docs/BENCHMARKS.md`](docs/BENCHMARKS.md).
* **Status:** [`PROGRESS.md`](PROGRESS.md) records what was done and tested in each phase.

## Web dashboard

```bash
go build -o bin/streamhub ./cmd/streamhub
bin/streamhub dashboard --managed     # launches 3 brokers + demo traffic → http://127.0.0.1:8090
```

A React + TypeScript + Tailwind console fed live over Server-Sent Events. It covers:
overview, brokers, topics and partitions, produce, live message stream,
consumer groups, dead letter queue, and a failure-simulation page. On that page you
can SIGKILL a broker and watch leader election, ISR changes, rebalancing and recovery
step by step. Every value comes from the running cluster; see
[`docs/DASHBOARD.md`](docs/DASHBOARD.md), which also lists what the backend cannot support.

![Failure simulation](docs/images/failure-incident.png)

## Features

| Area | What exists |
|---|---|
| Storage | Segmented append-only log, sparse offset index, CRC32-C per record, crash recovery (torn-tail truncation, index rebuild), retention by size/time, leader-epoch cache |
| Topics | Create/delete, partition count, replication factor, per-topic configs (`retention.ms`, `retention.bytes`, `segment.bytes`, `min.insync.replicas`, `max.message.bytes`) |
| Partitioning | FNV-1a key hashing; round-robin for keyless messages |
| Replication | Leader/follower with follower fetchers, ISR shrink/expand, high-watermark, leader-epoch truncation, acks 0/1/all, `min.insync.replicas`, no unclean election |
| Metadata | Raft (election, replication, durable state, check-quorum); controller = Raft leader; heartbeats, session fencing, broker self-fencing, preferred-leader rebalancing, controlled shutdown |
| Producer | Batching per partition, linger, retries with backoff, idempotence (producer ID + sequence numbers) |
| Consumer | Pull-based fetch with long polling, offset reset policy |
| Groups | Join/heartbeat/leave, range and round-robin assignment, eager rebalancing, generation fencing, offsets stored in replicated `__consumer_offsets` |
| Interfaces | Custom binary protocol over TCP, Go client library, CLI |
| Observability | `log/slog` JSON logs, Prometheus `/metrics`, `/healthz`, `/readyz`, JSON `/v1/state` |
| Dashboard | React/TypeScript/Tailwind web console over SSE, failure simulation, DLQ (see docs/DASHBOARD.md) |

## Guarantees

**Provided**

* Ordering within a partition.
* No loss of `acks=all` writes while at least one in-sync replica survives
  (`min.insync.replicas` ≥ 2 recommended; unclean leader election is never done).
* Consumers only see committed data (below the high-watermark).
* At-least-once delivery for consumer groups.
* No duplicates from producer retries when the idempotent producer is on
  (per partition, for the life of the producer ID).

**Not provided**

* Exactly-once or transactions; ordering across partitions.
* Durability if every replica loses power at the same moment, unless brokers
  run with `--fsync`. Like Kafka, the default relies on replication.
* Log compaction (so `__consumer_offsets` grows without bound), TLS,
  authentication, quotas, Raft snapshots and dynamic membership.

## Quick start

Requires Go 1.24+.

### A local 3-broker cluster (separate processes)

```bash
scripts/cluster.sh start          # builds bin/streamhub, starts brokers on :9092-9094
export STREAMHUB_BOOTSTRAP=127.0.0.1:9092,127.0.0.1:9093,127.0.0.1:9094

bin/streamhub topic create --name orders --partitions 3 --replication-factor 3
seq 1 100 | awk '{print "customer"($1%5)":order-"$1}' | bin/streamhub produce --topic orders --key-separator :
bin/streamhub consume --topic orders --group billing --idle-timeout 3s --format '%p@%o %k=%v'
bin/streamhub group describe --group billing      # members, assignment, lag
bin/streamhub cluster describe                    # brokers, controller, leaders, ISR

scripts/cluster.sh kill 2         # SIGKILL a broker and watch failover
bin/streamhub cluster describe
scripts/cluster.sh restart 2
scripts/cluster.sh clean          # stop and delete data
```

Metrics and health: `curl localhost:8081/metrics`, `curl localhost:8081/healthz`
(brokers 1–3 use HTTP ports 8081–8083).

### Docker

```bash
docker compose up -d --build
docker compose exec broker1 /streamhub topic create --name demo --partitions 3 --replication-factor 3 --bootstrap broker1:9092
docker compose exec broker1 /streamhub cluster describe --bootstrap broker1:9092
docker compose down -v
```

Brokers advertise their compose hostnames, so run clients through
`docker compose exec`. The dashboard is at http://localhost:8090 (attached mode). More detail is in the comment at the top of `docker-compose.yml`.

### Single broker

```bash
go build -o bin/streamhub ./cmd/streamhub
bin/streamhub broker --id 1 --listen 127.0.0.1:9092 --http 127.0.0.1:8080 --data-dir ./data
```

Without `--peers` the broker runs standalone, with a local metadata log and no Raft.

## Using the Go client

```go
c, _ := client.New(client.Config{Bootstrap: []string{"127.0.0.1:9092"}})
defer c.Close()

c.CreateTopic(ctx, client.TopicSpec{Name: "orders", Partitions: 3, ReplicationFactor: 3})

p, _ := c.NewProducer(client.ProducerConfig{Idempotent: true}) // acks=all by default
d := p.SendSync(ctx, client.Message{Topic: "orders", Key: []byte("customer-42"), Value: []byte("paid")})
fmt.Println(d.Partition, d.Offset, d.Err)

gc, _ := c.NewGroupConsumer(client.GroupConfig{Group: "billing", Topics: []string{"orders"}})
for {
	recs, err := gc.Poll(ctx)
	if err != nil { break }
	for _, r := range recs { process(r) }
	gc.Commit(ctx) // commit after processing = at-least-once
}
gc.Close(ctx)
```

## CLI

```
streamhub broker   --id N --listen host:port [--peers 1=h:p,2=h:p,3=h:p] [--fsync] ...
streamhub topic    create|delete|list|describe --name T [--partitions N] [--replication-factor N] [--config k=v]
streamhub produce  --topic T [--key K --value V | stdin lines] [--key-separator :] [--acks 0|1|all]
streamhub consume  --topic T [--group G | --partition P] [--from earliest|latest|N] [--count N]
streamhub cluster  describe
streamhub group    list | describe --group G
streamhub perf     produce|consume --topic T --records N [--size B] [--acks ...] [--inflight N]
streamhub healthcheck --http host:port
streamhub dashboard [--managed | --bootstrap h:p,...] [--listen 127.0.0.1:8090]
```

## Testing

```bash
go test -race ./...          # everything (integration suite takes about 2-3 minutes)
go test -race ./internal/integration -run Failover -v
go test ./internal/storage -run XXX -fuzz FuzzDecodeRecord -fuzztime 30s
```

* **Unit tests:** log segments, index, CRC, recovery, retention, epoch cache, protocol codec, Raft (in-memory network with partitions), metadata state machine, partitioner, assignment strategies, idempotent sequence checks, metrics output.
* **Integration tests:** real brokers in-process with real TCP and disk.
  * 1-broker and 3-broker clusters.
  * Replication, ISR, acks, consumer groups, retention, idempotence and metrics.
* **Failure tests:**
  * Kill the leader mid-produce, verifying no acks=all record is lost.
  * A network partition isolating the leader.
  * Controller failover.
  * A rolling restart.
  * A crash during a write (torn record).
  * A consumer crash with uncommitted work: records are redelivered and none are lost.
  * A coordinator failover, verifying committed offsets survive.
* **Real processes:** the same kill-the-leader scenario was run with `SIGKILL` against `scripts/cluster.sh`. 3,000,000 records were produced, and 3,000,000 unique records with 0 missing were consumed afterwards. See PROGRESS.md.

## Project layout

```
cmd/streamhub/        CLI and broker entry point
client/               Go client: metadata, admin, producer, consumers, groups
internal/storage/     commit log: records, segments, index, recovery, retention, epochs
internal/protocol/    wire format, messages, error codes
internal/transport/   TCP server, connection pool, fault injection
internal/raft/        Raft for the metadata quorum
internal/metadata/    deterministic metadata state machine
internal/broker/      broker: handlers, replica manager, fetchers, controller, groups
internal/metrics/     Prometheus text exposition
internal/dashboard/    dashboard backend (snapshot collector, SSE, demo consumers, DLQ, process manager) + embedded UI
internal/testutil/    in-process cluster harness
internal/integration/ end-to-end and failure tests
web/                  dashboard source (React + TypeScript + Tailwind, d3 charts)
scripts/cluster.sh    local 3-process cluster
docs/                 LEARNING.md, BENCHMARKS.md, DASHBOARD.md
```

## Known limitations

* **No log compaction.** `__consumer_offsets` keeps every commit, and retention is disabled for it.
* **Raft has no snapshots.** The metadata log grows forever and is replayed in full on restart.
* **Idempotence state is rebuilt on startup** by scanning each partition log: O(log size) at startup.
* **Consumer-group assignment is computed by the coordinator.** Kafka uses a client-side leader and SyncGroup instead. Rebalancing is eager, not cooperative.
* **Follower metadata is eventually consistent.** Clients retry, and `CreateTopic` waits until every broker sees the new topic.
* **Benchmarks were run on one 2-vCPU VM** with all brokers on loopback. See docs/BENCHMARKS.md.
