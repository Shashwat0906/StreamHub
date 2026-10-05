# StreamHub — Progress

Only results that were actually run are reported here.

| Phase | Status |
|---|---|
| 0 — Plan | done |
| 1 — Single-node broker | done |
| 2 — Cluster metadata (Raft) | not started |
| 3 — Replication | not started |
| 4 — Consumer groups | not started |
| 5 — Idempotence, retention, metrics, hardening | not started |
| 6 — Benchmarks & docs | not started |

## How to run (current state)

```bash
go build -o bin/streamhub ./cmd/streamhub
bin/streamhub broker --id 1 --listen 127.0.0.1:9092 --http 127.0.0.1:8080 --data-dir ./data/1
bin/streamhub topic create --name demo --partitions 2
echo "k1:hello" | bin/streamhub produce --topic demo --key-separator :
bin/streamhub consume --topic demo --count 1 --format '%p@%o %k=%v'
curl localhost:8080/metrics ; curl localhost:8080/healthz
go test -race ./...
```

## Phase 1 — single-node broker (done)

Done:
* `internal/storage`: segmented append-only log, sparse offset index,
  CRC32-C per record, crash recovery (truncates torn/corrupt tail, rebuilds
  bad indexes), truncation, retention by size/time, leader-epoch cache.
* `internal/protocol`: binary framing + codec for **all** planned APIs.
* `internal/transport`: TCP server, connection pool, fault-injection hooks.
* `internal/metadata`: deterministic metadata state machine (brokers,
  topics, partitions, leader/ISR rules, producer-ID allocation) driven by a
  local command log in standalone mode.
* `internal/broker`: Produce (acks 0/1/all), Fetch with long polling,
  ListOffsets, Metadata, CreateTopic, DeleteTopic, InitProducerId;
  Prometheus `/metrics`, `/healthz`, `/readyz`; JSON structured logs.
* `client`: metadata cache, admin API, batching producer with retries,
  partition consumer; hash (FNV-1a) and round-robin partitioning.
* `cmd/streamhub`: `broker`, `topic create|delete|list|describe`,
  `produce`, `consume`, `cluster describe`.

Tests run (`go test -race ./...`, all passing):
* storage: record round-trip, CRC detects every single-byte flip, reads
  across segments at every offset, reopen, torn-write recovery, bad-CRC
  recovery, corrupt-index rebuild, truncation, sequential-offset check,
  retention by size and by time, epoch cache, index lookup.
* protocol: encode/decode round-trip of every message, frame parsing,
  garbage rejection.
* transport: pooling, context deadlines, fault-injection block/heal.
* metadata: balanced assignment, leader election from ISR on fencing,
  no unclean election, stale-epoch AlterISR rejected, deterministic replay.
* integration (real TCP + disk): produce/consume with key placement and
  per-key ordering, acks 0/1/all, long-poll wake-up latency, restart keeps
  topics and data, create-topic validation, delete removes data.
* Manual CLI smoke test against a separate broker process (create topic,
  produce from stdin, consume, `/healthz`, `/metrics`).

Bug found and fixed during the smoke test: an undecodable response was
classified as a network error, so the producer retried it for its whole
delivery timeout. Decode errors are now non-retriable.

Not done yet: multi-broker cluster, replication, consumer groups.
