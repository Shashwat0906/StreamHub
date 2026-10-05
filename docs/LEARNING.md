# StreamHub explained simply

This guide explains every moving part of StreamHub in plain language, with
pointers to the code, so you can explain it in an interview. Each section
ends with the one or two sentences you would actually say out loud.

---

## 1. The big picture

A **message broker** sits between programs that produce data (orders,
clicks, logs) and programs that consume it. Producers do not talk to
consumers directly; they append messages to a **topic**, and consumers read
the topic at their own pace.

StreamHub works like Kafka:

* A **topic** is split into **partitions** (topic `orders` → `orders-0`,
  `orders-1`, `orders-2`). Partitions are the unit of parallelism and of
  ordering.
* Each partition is an **append-only log** stored on disk.
* Each partition has copies (**replicas**) on several brokers. One is the
  **leader** (handles reads and writes); the others are **followers** that
  copy the leader.
* A **controller** keeps track of brokers, topics and who leads what.
* Consumers in a **consumer group** split the partitions between them and
  remember their progress with **committed offsets**.

> *"StreamHub is a Kafka-style broker: topics are split into partitions,
> each partition is a replicated append-only log with one leader, a
> Raft-based controller manages metadata, and consumer groups share
> partitions and commit offsets."*

---

## 2. The commit log (`internal/storage`)

### What it is
A partition is just a file you only ever **append** to. Every message gets
a number, its **offset**: the first message is 0, the next is 1, and so on.
Nothing is ever modified in place. Consumers read "from offset N".

Why append-only? Sequential disk writes are very fast, readers never block
writers, and "where am I?" is a single number.

### Segments
One giant file would be hard to clean up, so the log is split into
**segments**: `00000000000000000000.log`, `00000000000000004096.log`, ...
The file name is the **base offset** (first offset inside). Only the newest
("active") segment is written; when it reaches `segment.bytes` it is
**rolled** (fsync'd and closed) and a new one starts.
Code: `storage/log.go` (`appendLocked`, `rollLocked`).

### Record format and CRC
Each record on disk is: `length | crc | offset | timestamp | leaderEpoch |
producerId | sequence | key | value`. The **CRC32-C checksum** covers
everything after it, so a half-written record or a flipped bit is detected
on read. Code: `storage/record.go`.

### Sparse index
To read offset 1,000,000 we must find *where* it is inside a segment file.
Each segment has a small `.index` file with entries
`(relativeOffset → byte position)`, but only one entry every ~4 KB of data
(that is what **sparse** means). To find an offset: binary-search the index
for the closest entry before it, then scan forward at most ~4 KB.
Tiny index, fast lookups. Code: `storage/index.go`.

### Crash recovery
If the broker crashes mid-write, the last segment may end with half a
record. On startup we scan the **last** segment record by record, checking
length, CRC and that offsets keep increasing. At the first bad record we
**truncate** the file there and rebuild the index. Older segments were
fsync'd when they were rolled, so they are trusted.
Code: `storage/segment.go` (`recoverFull`, `scan`).

### Retention
Logs cannot grow forever. A background job deletes whole **old segments**
when the partition is bigger than `retention.bytes` or the segment's newest
message is older than `retention.ms`. It never deletes the active segment
or anything above the high-watermark. Deleting moves the **log start
offset** forward; asking for an older offset returns `OFFSET_OUT_OF_RANGE`
and the consumer resets.

> *"Each partition is a segmented append-only log. Records carry a CRC; a
> sparse index maps offsets to file positions; on restart we scan the last
> segment and truncate any torn tail; retention deletes whole old segments."*

---

## 3. Topics, partitions and partitioning

* More partitions = more parallelism (more consumers can read at once,
  leaders spread across brokers).
* **Ordering is guaranteed only within a partition.**
* The producer picks the partition:
  * **with a key** → `hash(key) % partitions` (FNV-1a). Same key → same
    partition → per-key ordering (all events of customer 42 stay in order).
  * **without a key** → round-robin, for even load.

Replica placement (`metadata.AssignReplicas`) spreads leaders and followers
round-robin over brokers so no broker leads everything.

> *"Partitions give parallelism; order holds per partition. Keyed messages
> are hashed so a key always lands in one partition; keyless ones go
> round-robin."*

---

## 4. Replication: leader, followers, ISR, high-watermark

### Leader and followers
For each partition, producers and consumers talk only to the **leader**.
Followers continuously send **Fetch** requests to the leader ("give me
records from offset X") and append what they receive with the **same
offsets**. Code: `broker/fetcher.go`.

### LEO and high-watermark (HW)
* **LEO (log end offset)**: the next offset a replica would write. Each
  replica has its own.
* The leader learns each follower's LEO from its fetch requests (asking
  for offset X means "I have everything below X").
* **High-watermark = the smallest LEO among the in-sync replicas.** Every
  record below the HW exists on all in-sync replicas: it is **committed**.
* **Consumers can only read below the HW.** Otherwise a consumer could read
  a record that is then lost in a failover, and see history "un-happen".

Example: leader LEO=10, follower A LEO=8, follower B LEO=9 → HW=8.
Consumers see offsets 0–7.

### ISR (in-sync replicas)
The **ISR** is the set of replicas that are keeping up. A follower that has
not caught up within `replica.lag.time` (e.g. it crashed or is slow) is
**removed** from the ISR, so the HW no longer waits for it. When it catches
up again it is **added back**. The leader asks the controller to change the
ISR (`AlterISR`), so the ISR is stored in replicated metadata.
Code: `broker/fetcher.go` (`isrChange`, `checkISR`).

Subtle detail (Kafka's rule): under constant load a healthy follower is
always a few records behind. So "caught up" means the follower reached the
leader's LEO *as of its previous fetch*, not the live LEO.

### acks
| acks | Leader replies when... | Can lose acknowledged data? |
|---|---|---|
| 0 | never replies | yes, freely |
| 1 | it appended locally | yes, if the leader dies before followers copy it |
| all | the HW passed the batch, **and** ISR ≥ `min.insync.replicas` | no, while one ISR member survives |

With RF=3 and `min.insync.replicas=2`, acks=all survives the loss of any
one broker. If the ISR drops below 2, producers get `NOT_ENOUGH_REPLICAS`
instead of a false promise. The leader re-checks this after waiting for the
HW (`NOT_ENOUGH_REPLICAS_AFTER_APPEND`); a test caught us without that
check.

> *"Followers fetch from the leader; the high-watermark is the minimum LEO
> over the in-sync replicas, so anything below it is on every ISR member.
> Consumers only read below the HW, and acks=all waits for the HW plus a
> minimum ISR size."*

---

## 5. Leader election and leader epochs

### Electing a new leader
When a broker dies, the controller notices (missed heartbeats, section 6),
**fences** it, and for each partition it led picks the first live replica
**in the ISR** as new leader. Because ISR members have everything below
the HW, no committed (acks=all) record is lost.

**Unclean election is disabled**: if no ISR member is alive, the partition
goes **offline** rather than electing an out-of-date replica and silently
losing data. It comes back when the last ISR member returns.

### Leader epoch
Every leadership change increments the partition's **leader epoch**. Every
record stores the epoch of the leader that wrote it. The epoch is used for
**fencing**: requests carrying an old epoch (from a deposed leader, or from
a follower with stale metadata) are rejected.

### Truncation after failover (the tricky part)
An old leader may hold records that never reached the followers (they were
never committed). When it comes back as a follower, its log has **diverged**
from the new leader's. It must cut that tail before fetching, otherwise the
same offset would hold different data on different replicas.

How: each replica keeps a **leader-epoch cache** ("epoch 3 started at
offset 120, epoch 5 at 410"). The returning replica asks the new leader:
*"where does my latest epoch end in your log?"* (`OffsetForLeaderEpoch`)
and truncates exactly there. This is Kafka's KIP-101 fix; simply truncating
to the old high-watermark can lose committed data in some edge cases.
Code: `storage/epoch.go`, `broker/fetcher.go` (`truncateForLeader`).

In our network-partition test, an isolated leader accepted 20 acks=1
writes; after the network healed it truncated all 20. That is exactly
"acks=1 can lose data", demonstrated.

> *"Only ISR members can become leader, so committed data survives. Leader
> epochs fence stale leaders, and a returning ex-leader asks the new leader
> where its epoch ends and truncates its divergent tail."*

---

## 6. The controller and Raft (`internal/raft`, `internal/metadata`)

### Why we need consensus
Someone must decide "broker 2 is dead, broker 3 now leads orders-1". If two
nodes made conflicting decisions (**split brain**), two leaders could accept
writes. The decision-maker must therefore be agreed on by a **majority**.

### Raft in one paragraph
Nodes are followers, candidates or leaders. If a follower hears nothing
from a leader for a random **election timeout**, it becomes a candidate,
increments the **term** and asks for votes. A node votes at most once per
term, and only for a candidate whose log is at least as up to date as its
own. Majority wins. The leader appends commands to its log and replicates
them; an entry is **committed** once a majority stores it. Every node
applies committed entries in the same order.

### How StreamHub uses it (KRaft-style)
* Every broker is a Raft voter. **The Raft leader is the active
  controller.**
* Every metadata change is a command in the Raft log: register broker,
  fence broker, create/delete topic, change ISR, elect leader, allocate
  producer ID.
* **Every broker applies the same log**, so every broker has the same
  metadata image. That is how brokers learn which partitions they lead.
* Leader election for *partitions* happens inside the deterministic apply
  function, so all brokers reach the same decision.

### Liveness and fencing
* Brokers send `BrokerHeartbeat` to the controller every ~0.5 s.
* If the controller hears nothing for the **session timeout** (3 s), it
  commits `FenceBroker`, which moves leadership away.
* **Self-fencing**: a broker that cannot reach the controller stops
  accepting writes after 3/4 of the session timeout, *before* the
  controller replaces it. This avoids two leaders accepting writes.
* **Check-quorum**: a Raft leader that cannot reach a majority steps down,
  so an isolated node cannot keep acting as controller.

### Honest limitations
No snapshots (the metadata log grows forever), static membership, no
pre-vote. With 3 brokers, losing 2 stops all metadata changes.

> *"Metadata lives in a Raft log replicated across the brokers; the Raft
> leader is the controller. Liveness comes from heartbeats with a session
> timeout; dead brokers are fenced by a committed command, and isolated
> brokers fence themselves."*

---

## 7. Producers: batching, retries, idempotence (`client/producer.go`)

* **Batching**: the producer keeps one queue ("lane") per partition and
  sends many records per request (up to 1000 records, or after `linger`).
  Fewer, bigger requests = much higher throughput.
* **Ordering**: each lane has at most **one batch in flight**, so a retry
  can never overtake a later batch.
* **Retries**: on retriable errors (`NOT_LEADER`, timeout, network) the
  producer refreshes metadata and resends.

### The duplicate problem
If the leader writes a batch but the reply is lost, the producer retries
and the batch is written **twice**. That is plain at-least-once.

### Idempotent producer
* The producer gets a unique **producer ID** (allocated through Raft, so
  never reused).
* Each record of a partition gets a **sequence number** 0, 1, 2, ...
* The broker remembers, per producer, the last sequence it wrote:
  * next expected sequence → append;
  * already seen → **don't write again**, reply with the original offset;
  * a gap → `OUT_OF_ORDER_SEQUENCE` (a batch went missing).
* Sequence numbers are stored inside each record, so a follower that
  becomes leader rebuilds the same state by scanning its log. Tested: a
  retry sent to the *new* leader after a failover was de-duplicated.

Limits: per partition, per producer session. Not across producer restarts,
not across partitions, and no transactions.

> *"Batching per partition gives throughput, one batch in flight per
> partition keeps order, and producer IDs plus per-partition sequence
> numbers let the broker drop retried duplicates."*

---

## 8. Consumers, long polling and consumer groups

### Pull-based fetch with long polling
Consumers **pull** (`Fetch` from offset X). If no data is available the
broker **holds the request** (up to `maxWait`) and answers as soon as data
arrives. This avoids busy-polling and keeps latency low. Implementation: the
fetch registers a waiter on each partition, and appends or HW changes wake
it. Code: `broker/handlers.go` (`handleFetch`).

### Consumer groups
A **consumer group** is a set of consumers sharing work: each partition is
assigned to exactly **one** member of the group. Add consumers to scale out
(up to the number of partitions).

* The **group coordinator** is a broker: the leader of partition
  `hash(group) % 8` of the internal topic `__consumer_offsets`.
* Members send `JoinGroup`, then `Heartbeat` regularly.
* **Rebalance**: when someone joins, leaves, or stops heartbeating, the
  coordinator moves the group to `PreparingRebalance`. Heartbeat replies
  say "rebalance in progress"; members **commit their progress and
  rejoin**. When all have rejoined (or the rebalance timeout passes), the
  coordinator computes a new assignment, bumps the **generation** number and
  replies to everyone.
* Assignment strategies (`broker/assign.go`):
  * **range**: per topic, contiguous blocks. 7 partitions, 3 members →
    [0,1,2] [3,4] [5,6].
  * **round-robin**: deal all partitions out one by one, balanced across
    topics.

### Committed offsets
A consumer **commits** "next offset to read" for each partition. Commits
are records written to `__consumer_offsets` with **acks=all**, so they are
replicated and survive a coordinator crash; the new coordinator reloads
them. A commit from an old **generation** is rejected
(`ILLEGAL_GENERATION`): a "zombie" consumer that was kicked out (say, after
a long GC pause) cannot overwrite the new owner's progress.

### At-least-once delivery
The consumer processes records, *then* commits. If it crashes after
processing but before committing, whoever gets the partition resumes from
the last commit and **those records are delivered again**. Tested: a
member processed 100 records without committing and crashed; exactly 100
were redelivered and 0 were lost.

> *"Consumers pull with long polling. A group coordinator assigns each
> partition to one member, rebalances on membership change, and stores
> committed offsets durably in a replicated internal topic, giving
> at-least-once delivery."*

---

## 9. Guarantees: say exactly this

**Provided**
* Order within a partition.
* No loss of acks=all writes while one ISR member survives
  (`min.insync.replicas=2`, no unclean election).
* Consumers never see uncommitted data (they read below the HW).
* At-least-once for consumer groups.
* No duplicates from producer retries with the idempotent producer.

**Not provided**
* Exactly-once / transactions, ordering across partitions.
* Durability if every replica loses power at once, unless `--fsync`.
* Log compaction, TLS/auth, quotas, dynamic Raft membership.

---

## 10. Walkthroughs

### A produce with acks=all
1. The client hashes the key → partition 1 → looks up its leader (broker 2)
   in cached metadata.
2. Broker 2 checks: leader? not fenced? ISR ≥ min ISR? sequence number OK?
3. It appends to its log with its leader epoch; the LEO moves.
4. Followers' pending long-poll fetches wake up; they append and fetch
   again with the new offset, which tells the leader their new LEO.
5. The HW advances to the minimum ISR LEO; the waiting produce request sees
   HW ≥ batch end, re-checks the ISR size, and replies with the base offset.

### The leader dies
1. Heartbeats stop; after the session timeout the controller commits
   `FenceBroker(2)`.
2. Every broker applies it: the dead broker leaves the ISR, the first live
   ISR replica becomes leader, and the leader epoch goes up.
3. The new leader's replica manager switches to leader; the other follower
   truncates via `OffsetForLeaderEpoch` and fetches from the new leader.
4. The producer gets `NOT_LEADER` / network errors, refreshes metadata and
   retries; idempotence drops any batch that had in fact been written.
5. When broker 2 returns it is unfenced, truncates its divergent tail,
   catches up, rejoins the ISR, and later gets its preferred leadership back.

---

## 11. Likely interview questions

**Why can't consumers read up to the leader's LEO?** Records above the HW
are not on every ISR member yet; after a failover they may disappear.

**What is the difference between LEO and HW?** LEO is per replica (where it
will write next). HW is per partition (what every in-sync replica has).

**Why not elect any live replica?** An out-of-sync replica may lack
committed records; electing it loses acknowledged data. We prefer
unavailability over silent loss.

**How do you avoid split brain?** Raft majority for the controller; leader
epochs on every partition; brokers self-fence when cut off; Raft
check-quorum.

**Why is a rebalance "stop the world"?** We use eager rebalancing: everyone
gives up partitions and rejoins. Kafka also has incremental cooperative
rebalancing, which we do not implement.

**How would you get exactly-once?** Transactions: atomic writes across
partitions plus committing consumer offsets in the same transaction (Kafka
EOS). Not implemented here.

**What did testing find?** (1) A log read across a segment boundary could
skip the rest of a segment, found by a 3-million-record end-to-end run.
(2) acks=all could be acknowledged with too small an ISR if the ISR shrank
while waiting, found by a flaky test. (3) Requests arriving during startup
saw half-initialised state, found by Go's race detector.
