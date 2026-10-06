# StreamHub — Progress

Only results that were actually run are reported here.

| Phase | Status |
|---|---|
| 0 — Plan | done |
| 1 — Single-node broker | done |
| 2 — Cluster metadata (Raft) | done |
| 3 — Replication | done |
| 4 — Consumer groups | done |
| 5 — Idempotence, retention, metrics, hardening | done |
| 6 — Benchmarks & docs | done |
| 7 — Web dashboard | done |

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

## Phase 3 — replication (done)

Done:
* Follower fetchers: one goroutine per leader broker, multi-partition
  long-poll Fetch with the follower's replica ID and leader epoch.
* High-watermark = min LEO over the (maximal) ISR; consumers read only
  below it; follower HW = min(leader HW, own LEO); HW checkpointed.
* ISR shrink (follower not caught up within `replica.lag.time`) and expand
  (follower reached HW and the current leader epoch) via `AlterISR` to the
  controller, fenced by leader epoch. Kafka's "caught up" rule (compare to
  the leader LEO at the previous fetch) so busy followers are not dropped.
* acks: 0 (no response), 1 (leader append), all (HW passes the batch and
  `|ISR| >= min.insync.replicas`, else `NOT_ENOUGH_REPLICAS`).
* Leader-epoch truncation: a replica that becomes follower calls
  `OffsetForLeaderEpoch` and cuts its divergent tail (KIP-101/279 rule).
* Followers reset to the leader's log start if retention deleted what they need.

Tests run (`go test -race`, all passing; integration suite run 3× in a row):
* all replicas byte-identical (offset, epoch, value) after 500 acks=all
  records; ISR shrinks when a follower dies, acks=all continues with 2/3,
  follower restarts, catches up and rejoins; `NOT_ENOUGH_REPLICAS` when
  ISR < min.insync while acks=1 still succeeds.
* **Kill leader mid-produce** (in-process): one run logged
  `acked=412 stored=412 lost=0 duplicates=0 old_leader=3 new_leader=1`.
* **Network partition isolating the leader**: acks=all to the isolated
  leader was never acknowledged; it accepted 20 acks=1 writes before
  self-fencing, and after healing it truncated them (0 survived), all
  acks=all records present, replicas identical.
* Rolling restart of all 3 brokers while producing: 390/390 records, ISR
  back to 3 after each restart.
* Crash during write on a follower (torn record appended to its segment):
  recovery cut it, the follower re-replicated, 150/150 records in order.

Real-process test (3 OS processes via `scripts/cluster.sh`): leader
`SIGKILL`ed 1 s into producing 3,000,000 records (acks=all, idempotent):
producer reported 0 failures; consuming afterwards returned 3,000,000
records, 3,000,000 unique, 0 missing; restarted broker caught up
(LEO = HW = 3,000,000 on all three).

**Bug found by that run and fixed**: `Log.Read` continued into the next
segment when a segment stopped early because of the byte budget, skipping
records (the consumer saw ~600k of 3M offsets). Followers read through the
same path and `AppendReplicated` only checked that offsets increase, so a
follower could store a log with holes. Fixed both (read stops at the
segment; replicated batches must be contiguous) and added regression tests
that fail on the old code.

## Phase 4 — consumer groups (done)

Done:
* Internal topic `__consumer_offsets` (8 partitions, RF = min(3, live
  brokers), created on first FindCoordinator). Coordinator for a group =
  leader of partition `fnv(group) % 8`.
* Coordinator: JoinGroup (blocks until the rebalance completes), Heartbeat,
  LeaveGroup, session expiry, rebalance timeout (members that do not rejoin
  are removed), initial join delay so consumers starting together cause one
  rebalance, eager "stop-the-world" rebalancing, **server-side** assignment
  (range or round-robin; Kafka does it client-side via SyncGroup).
* Committed offsets are records in `__consumer_offsets` written with
  acks=all; a new coordinator rebuilds its cache by reading the partition.
  Commits from a stale generation or unknown member are rejected
  (zombie fencing).
* Client `GroupConsumer`: join/rejoin, heartbeat goroutine, commit before
  rejoining and on close, positions from committed offsets or reset policy.
  CLI: `consume --group`, `group list`, `group describe` (members,
  assignment, committed offset, end offset, lag).

Tests run (`go test -race`, all passing, full suite run 3× then 2×):
* unit: range and round-robin (exact layout, completeness, no duplicates,
  balance, mixed subscriptions, determinism).
* integration: 2 members split 6 partitions 3/3; third member → 2/2/2 and
  generation bump; graceful leave → 3/3 (~0.3 s, no session wait); lag 0 and
  committed offsets sum to records produced.
* **member crash with uncommitted work**: one run logged "a processed 100
  records without committing; after its crash 100 records were
  redelivered, 0 lost"; the survivor resumed exactly at the committed
  offsets (asserted per partition).
* zombie commit with an old generation → ILLEGAL_GENERATION; unknown member
  → UNKNOWN_MEMBER_ID; round-robin across two topics; **coordinator broker
  killed**: consumer moved to the new coordinator, 300/300 records, 0
  duplicates (committed offsets survived).
* Real processes: two `consume --group` CLIs split 4 partitions 2/2, read
  1000 distinct records, lag 0; 50 more produced → lag 50 → group resumed
  and read exactly 50.

Bugs found and fixed in this phase:
1. **acks=all could be acknowledged with ISR below min.insync.replicas**
   if the ISR shrank while the write waited for the HW (flaky
   `TestNotEnoughReplicas`). The leader now re-checks after the HW wait and
   returns NOT_ENOUGH_REPLICAS_AFTER_APPEND, like Kafka.
2. A `produce` right after `topic create` could hit a broker that had not
   applied the topic yet. CreateTopic now waits until every broker sees it,
   and topic lookups retry unknown topics briefly.
3. My first version of the crash test passed without checking anything
   (its wait condition was already true); rewritten so it requires the
   survivor to re-read the uncommitted range.

## Phase 5 — idempotence, retention, metrics, hardening (done)

Done:
* Idempotent producer end to end: producer IDs allocated through the Raft
  log (never reused), per-partition sequence numbers, broker de-duplicates
  retries (returns the original offset), rejects gaps
  (OUT_OF_ORDER_SEQUENCE). State is rebuilt from `producerId/sequence`
  stored in every record, so it survives restarts and leader failover. On a
  permanently failed batch the client switches to a new producer ID.
* Retention by size and time per topic (`retention.bytes`, `retention.ms`,
  `segment.bytes`), never deleting the active segment or data above the
  high-watermark; followers reset to the leader's log start when retention
  removed what they need.
* `max.message.bytes` (default 1 MiB) → MESSAGE_TOO_LARGE.
* Preferred-leader rebalancing: the controller moves leadership back to
  `Replicas[0]` when it is alive and in the ISR (load balancing after
  failures).
* Controlled shutdown: a graceful stop fences the broker first so leaders
  move immediately; `Kill` stays abrupt for crash tests.
* Observability: Prometheus `/metrics` (request counts by API and error,
  latency histograms, records in, fetch bytes, ISR shrinks/expands, fencings,
  rebalances, per-partition LEO/HW/size/ISR size/under-replicated,
  is_controller, leader count), `/healthz`, `/readyz` (503 when fenced),
  JSON `slog` logs.
* Fuzz tests for the record decoder and every request decoder.

Tests run (`go test -race`, full suite 3× in a row, all passing):
* sequence-check unit tests (in order, exact retry, suffix retry, gap,
  overlap, evicted window), state rebuilt from a log.
* integration: retry deduplicated + gap rejected + non-idempotent retry
  duplicated (log content asserted exactly); dedupe after **leader
  failover** (retry against the new leader returns the original offset, 2
  records not 4); dedupe after restart; retention by size on all three
  replicas (one run: log start moved to 864 of 1000) and consumer reset to
  earliest; retention by time keeps the active segment; follower down while
  leader deleted segments → resets to leader log start and catches up to
  820; message too large; preferred leader restored after its broker
  restarts; `/metrics` contains the expected series, `/healthz` JSON,
  `/readyz` → 503 for an isolated broker and back to 200 after healing;
  controlled shutdown (one run: leadership already moved when the graceful
  stop returned, with a 10 s session timeout).
* Fuzzing: `FuzzDecodeRecord` ~438k execs / 20 s, `FuzzDecodeRequests`
  ~325k execs / 20 s, no crashes (bounded runs, not exhaustive).

## Phase 6 — benchmarks, docs, packaging (done)

Done:
* `streamhub perf produce|consume` load generator (throughput, per-record
  latency percentiles).
* `docs/BENCHMARKS.md`: storage micro-benchmarks (3 runs) and end-to-end
  runs on the 3-process cluster: acks=all/1, RF 3/1, 100 B/1 KiB, consume,
  one-at-a-time latency, `--fsync`, plus 3 repeats for variance. All on one
  2-vCPU VM, which the document states prominently.
* `docs/LEARNING.md`: every component explained simply, walkthroughs and
  interview Q&A.
* `README.md`: features, guarantees, quick start, client example (compiled
  as `client/example_test.go`), CLI, testing, limitations.
* `Dockerfile` (multi-stage → `FROM scratch`, 12 MB) and
  `docker-compose.yml` (3 brokers, healthchecks via `streamhub healthcheck`).
* `.github/workflows/ci.yml` (gofmt, vet, `go test -race`).
* `scripts/cluster.sh` now waits for brokers to exit on stop, and accepts
  extra broker flags via `STREAMHUB_BROKER_ARGS`.

What was run:
* All benchmarks listed in docs/BENCHMARKS.md, with the exact output.
* README quick start executed verbatim against the 3-process cluster
  (create, produce, group consume, describe, `SIGKILL` broker 2 → shown as
  fenced, restart, `/healthz`, `/metrics`).
* docker-compose: the Docker daemon could be started in this environment,
  but **Docker Hub is blocked here**, so the multi-stage `Dockerfile` (which
  pulls `golang:1.24-alpine`) could **not** be built. To still test the
  compose file, an equivalent `FROM scratch` image was built from the
  locally compiled static binary and started with
  `docker compose up -d --no-build`: all 3 containers became healthy,
  a RF=3 topic was created, 5000 records produced, the `broker2` container
  was stopped, 1000 more produced, all 6000 consumed back unique, and after
  `docker compose start broker2` it rejoined every ISR.
  (Later superseded: the real multi-stage build was run in phase 7, see
  below.)
* CI workflow: written but **not run** (no GitHub Actions runner here).

## Final verification

* `gofmt -l .` clean, `go vet ./...` clean.
* `go test -race ./...` passes (see the commit for the run used).

## Bugs found by testing (all fixed, with regression tests)

1. Log read across a segment boundary skipped the rest of the segment when
   the byte budget ran out (found by a 3M-record real-process run).
2. Replicated appends only checked increasing offsets, so a follower could
   store holes (found while analysing bug 1).
3. acks=all could be acknowledged with ISR < min.insync.replicas if the
   ISR shrank during the wait (flaky test).
4. Requests arriving during broker startup saw half-initialised state
   (race detector).
5. Undecodable responses were retried as network errors for the whole
   delivery timeout (CLI smoke test).
6. A command right after `topic create` could hit a broker that had not
   applied the topic yet (real-process CLI run).
7. `scripts/cluster.sh stop` did not wait for exit, so an immediate
   `start` could fail on busy ports (benchmark run).

## Phase 7 — web dashboard (done)

Done (details and screenshots in `docs/DASHBOARD.md`):
* `streamhub dashboard`: Go backend-for-frontend that aggregates a cluster
  snapshot every second (metadata, group descriptions, each broker's new
  `/v1/state` JSON endpoint), diffs snapshots into timeline events and
  pushes both to the browser over Server-Sent Events; second SSE stream
  for live messages; REST for produce, topics, DLQ retry, demo consumers,
  traffic and broker control.
* Managed mode launches and owns 3 broker processes (Kill = SIGKILL,
  Graceful stop = SIGTERM/controlled shutdown, Restart); attached mode
  monitors any cluster; docker-compose runs it next to the brokers.
* React 19 + TypeScript + Tailwind v4 UI, d3 charts, embedded in the Go
  binary: Overview, Broker Cluster (+ details drawer), Topics & Partitions
  (tree + table), Produce, Live Stream, Consumer Groups, Dead Letter Queue,
  Failure Simulation (six-step incident tracker, leadership matrix,
  timeline), dark/light theme, responsive layout, toasts, loading/empty/
  error states.
* Broker changes: advertised HTTP address in cluster metadata,
  `/v1/state`, consumed-records and produce-error counters, HTTP started
  before registration (requests wait for init).
* DLQ: demo consumers dead-letter failing records to `<topic>.dlq`; retry
  state persisted in `__dlq_retries`.

What was run:
* `go test -race ./...` including new `internal/dashboard` tests: snapshot
  diff unit tests, an end-to-end API test against a real 3-broker
  in-process cluster, and 3 regression tests (below). Each regression test
  was checked to fail on the old code.
* UI (first pass): `npm run typecheck` and `npm run build` via the
  offline package store, typecheck against local React type shims.
* UI (after npm registry access was granted): real `npm install` (80
  packages, 0 vulnerabilities reported); `npm run typecheck` against the
  real `@types/react` 19.3.0 / TypeScript 5.9.3 → exit 0 with no errors; a
  planted type error was caught (TS2322, exit 2), proving the check is live;
  `npm run build` with the npm toolchain (React 19.3.0, esbuild 0.25.12)
  → this bundle is now the committed one, and `package-lock.json` is
  committed (CI uses `npm ci`). Re-checked in Chromium: all 9 routes, dark
  and light, no console errors.
* Managed mode with 3 real broker processes, exercised through the API and
  through a real browser (Playwright + Chromium): every page screenshotted
  in dark, light and mobile widths with no console errors; clicked Kill on
  a broker and captured the incident (broker unavailable → leader failure
  → election → ISR shrink → rebalance), clicked Restart and captured
  recovery and, ~30 s later, leadership moving back; DLQ Retry clicked in
  the UI (entry marked retried, re-failed message shows 1/3).
* Docker, first pass (Docker Hub blocked at the time): compose stack run
  with `--no-build` on a locally built scratch image; `docker compose kill
  broker2` → dashboard showed fenced/leader lost/elected/ISR shrink; after
  `start`, all ISRs 3.
* Docker, real build (after registry access was granted, 2026-10-06):
  * `docker build --no-cache -t streamhub:local .` with the multi-stage
    `Dockerfile` (`golang:1.24-alpine` → `scratch`) succeeded in ~22 s
    (go build step 14.5 s, "no module dependencies to download"); image
    size 12 MB. `docker compose build --no-cache` also succeeded (~17 s).
  * `docker compose down -v && docker compose up -d --build`: broker1–3
    healthy (compose healthcheck) within ~6 s, dashboard on :8090.
  * Broker-failure test through the dashboard API (attached mode):
    topic `payments` (6 partitions, RF 3, min ISR 2), 2 demo consumers in
    group `settlement`, traffic generator at 50 msg/s acks=all.
    Broker 2 was the Raft controller and led payments-1, payments-4 and
    __consumer_offsets-0/3/6. `docker compose kill broker2` (SIGKILL) at
    ~04:48:55 UTC. Events recorded by the dashboard (UTC):
    * 04:48:56.2 broker 2 unavailable;
    * 04:48:57.2 controller moved from broker 2 to broker 3 (Raft election);
    * 04:49:02.4 broker 2 fenced (session expired), leader lost and new
      leader elected (epoch 1) for all 5 partitions it led (payments-1 → 3,
      payments-4 → 1), every ISR shrank to 2 replicas;
    * 0 offline partitions; traffic kept flowing (~50 msg/s produced during
      the outage, 0 errors reported to the traffic generator); the group
      stayed Stable with both members.
    `docker compose start broker2` at ~04:49:14 UTC:
    * 04:49:15.2 broker 2 reachable, 04:49:16.2 re-registered (unfenced),
      04:49:17.2 all 14 ISRs back to 3 replicas;
    * 04:49:19.2 leadership moved back to preferred broker 2 (epoch 2) for
      the 5 partitions.
  * Brokers answered 2 Produce requests with NOT_LEADER (broker 1 and
    broker 3 metrics) around the leadership changes; the producer retried
    them, so the generator reported 0 errors.
  * Data check after stopping traffic: generator `sent` = 6495; sum of
    payments high-watermarks = 6495; `streamhub consume` of the whole topic
    read 6495 records with 6495 unique `orderId`s (0 missing, 0
    duplicates); group lag 0, dashboard "consumed" = 6495.

Bugs found while verifying (all fixed):
1. A crashed demo consumer kept reporting "running" (the dying loop
   overwrote the terminal state). Regression test added.
2. If writing to the DLQ failed, the demo consumer still committed past the
   record, so it was neither processed nor dead-lettered (lost). It now
   rewinds and retries the record.
3. During a coordinator failover a group vanished from the snapshot, so
   "messages consumed" dropped to 0 and jumped back, drawing a fake
   throughput spike. The last known view is now carried forward (flagged
   stale) for up to 30 s. Regression test: consumed total never decreases.
4. DLQ "already retried" state lived only in memory; a dashboard restart
   allowed retrying the same entry again. Now persisted in
   `__dlq_retries`. Regression test with a second, fresh dashboard.
5. Live stream labelled ~25 % of traffic-generator records "committed to
   ISR" instead of their ack, because the consumer saw them before the
   producer callback ran; it now waits (one shared 300 ms budget per
   fetched batch) for the ack.
6. UI: chart time axis showed mm:ss (read as a clock time), the DLQ table
   overflowed and hid the Retry button, a broker name wrapped, a dropdown
   truncated. Fixed and re-screenshotted.
7. Docs/UI wording: preferred-leader moves are now shown as step 5
   (reassignment), not as a failover election.

Not supported by the backend (shown honestly in the UI, listed in
docs/DASHBOARD.md): built-in DLQ, message IDs/headers, ack status for
records not produced through the dashboard, broker failure buttons outside
managed mode, crashing consumers not run by the dashboard, network
partition simulation, replica reassignment.

