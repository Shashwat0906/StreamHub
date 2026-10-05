# StreamHub — Progress

Only results that were actually run are reported here.

| Phase | Status |
|---|---|
| 0 — Plan | done |
| 1 — Single-node broker | done |
| 2 — Cluster metadata (Raft) | done |
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

## Phase 2 — cluster metadata (done)

Done:
* `internal/raft`: leader election (randomised timeouts, election
  restriction), log replication with fast conflict back-off, commit only of
  current-term entries, leader no-op on election, **check-quorum** (an
  isolated leader steps down), durable term/vote (fsync + atomic rename)
  and log (CRC per entry, torn-tail recovery).
* Every broker is a Raft voter; the Raft leader is the active controller.
  Metadata commands are applied by every broker, so all brokers converge to
  the same image (verified in tests).
* Controller: broker heartbeats → register/unfence; missed session →
  `FenceBroker` command → deterministic leader election from the ISR in
  the state machine. A newly elected controller grants every broker a full
  session before fencing anyone.
* Broker self-fencing: a broker without a successful controller heartbeat
  for 3/4 of the session timeout refuses leader writes.
* Requests arriving during broker startup wait until it is fully wired
  (found by the race detector).
* `scripts/cluster.sh`: 3 separate broker processes, separate data dirs and
  ports, `start|stop|status|kill N|restart N|clean`.

Tests run (`go test -race`, all passing, integration suite run 3× in a row):
* raft (in-memory network): election + replication, leader failover and
  restart catch-up from disk, isolated leader cannot commit and steps down,
  divergent uncommitted entry overwritten after heal, single node, WAL
  torn-tail recovery.
* integration (3 in-process brokers, real TCP): cluster forms, partitions
  spread over all brokers, identical metadata on every broker, client
  routes to leaders; controller failover (new controller, dead broker
  fenced, topic creation still works, dead broker excluded from placement,
  rejoin); RF=1 partition goes offline when its broker dies and comes back
  with its data when the broker returns; full cluster restart keeps topics.
* Manual run of `scripts/cluster.sh` with 3 OS processes: create topic,
  produce, `SIGKILL` the controller → another broker became controller,
  restart → all 3 ready again.

Known limitation found: follower metadata is eventually consistent, so a
client may briefly not see a just-created topic on some brokers (clients
retry; tests poll).

Not done yet: replication of partition data (RF>1 topics have followers in
metadata but they do not fetch yet), consumer groups.
