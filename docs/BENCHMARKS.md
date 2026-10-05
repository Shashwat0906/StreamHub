# StreamHub — Benchmarks

Every number below comes from a run recorded while building the project
(Phase 6). Nothing is extrapolated. Treat them as **"what this code did on
this machine"**, not as a comparison with Kafka or as a capacity plan.

## Environment

| | |
|---|---|
| Machine | cloud VM, **2 vCPU** Intel Xeon @ 2.10 GHz, 7.8 GiB RAM, virtio disk |
| OS / Go | Linux, Go 1.24.7 |
| Cluster | 3 broker **processes** on the same VM (`scripts/cluster.sh`), loopback networking |
| Load generator | `streamhub perf produce/consume`, also on the same VM |

Important caveat: three brokers *and* the load generator share 2 vCPUs, and
"network" is loopback. CPU is the bottleneck in every end-to-end run;
replication traffic does not cross a real NIC. Real multi-machine numbers
would differ (in either direction) and were **not** measured.

## 1. Storage micro-benchmarks

`go test ./internal/storage -bench . -benchtime 3s -count 3`

| Benchmark | Run 1 | Run 2 | Run 3 |
|---|---|---|---|
| `BenchmarkAppend` (batches of 100 × 100-byte values, no fsync) | 320.28 MB/s | 323.69 MB/s | 316.23 MB/s |
| `BenchmarkRead` (64 KiB reads from a 100k-record log) | 500.79 MB/s | 502.48 MB/s | 502.36 MB/s |

An encoded 100-byte record is 148 bytes (48 bytes of header incl. CRC), so
~320 MB/s ≈ 2.1–2.2 M records/s appended. Both benchmarks hit the OS page
cache; neither measures the disk.

## 2. End-to-end produce / consume (3-process cluster)

Producer settings unless noted: linger 5 ms, ≤1000 records per batch, up to
50,000 records outstanding (`--inflight`), round-robin partitioning, 3
partitions. Latency is per record, from `Send` to acknowledgement, so under
full load it is dominated by **queueing in the producer**, not by the
broker.

| Scenario | Records | Throughput | p50 / p95 / p99 latency |
|---|---|---|---|
| A. RF=3, acks=all, idempotent, 100 B | 500,000 | 252,632 rec/s (24.1 MiB/s) | 189 / 239 / 260 ms |
| A, repeat 1 | 500,000 | 241,560 rec/s (23.0 MiB/s) | 256 / 324 / 335 ms |
| A, repeat 2 | 500,000 | 276,865 rec/s (26.4 MiB/s) | 160 / 220 / 229 ms |
| A, repeat 3 | 500,000 | 303,267 rec/s (28.9 MiB/s) | 146 / 228 / 248 ms |
| B. RF=3, acks=1, 100 B | 500,000 | 339,350 rec/s (32.4 MiB/s) | 130 / 194 / 209 ms |
| C. RF=1, acks=1, 100 B | 500,000 | 709,339 rec/s (67.7 MiB/s) | 59 / 99 / 127 ms |
| D. RF=3, acks=all, idempotent, 1 KiB | 200,000 | 66,478 rec/s (64.9 MiB/s) | 711 / 888 / 913 ms |
| G. as A but brokers run with `--fsync` (fsync every append) | 200,000 | 199,275 rec/s (19.0 MiB/s) | 230 / 304 / 318 ms |

Consumer:

| Scenario | Records | Throughput |
|---|---|---|
| E. read RF=3 topic from earliest, 3 partition consumers | 1,000,000 | 1,048,296 rec/s (100.0 MiB/s) |

### Latency without queueing

One record at a time (`--inflight 1 --linger 0s`), so each number is a full
round trip: client → leader append → followers fetch → high-watermark
advances → ack.

| Scenario | Records | p50 / p95 / p99 / max |
|---|---|---|
| F. RF=3, acks=all | 2,000 | 5.6 / 6.0 / 6.3 / 7.7 ms |
| H. RF=3, acks=all, `--fsync` | 500 | 6.5 / 7.1 / 8.1 / 24.9 ms |

### Reading the results

* acks=all vs acks=1 (A vs B) costs ~20–30 % throughput here: the leader
  must wait for both followers to fetch before the high-watermark moves.
* RF=3 vs RF=1 (B vs C) roughly halves throughput, which is expected when
  three replicas share two CPUs: every byte is written three times and
  shipped twice over loopback.
* Run-to-run variance on the same scenario was about ±12 % (A and its
  repeats: 241k–303k rec/s).
* `--fsync` cost little on this VM (G vs A, H vs F). Virtual disks often
  acknowledge fsync from a host cache, so do **not** conclude that fsync is
  cheap on real hardware.
* The ~5.6 ms acks=all round trip is mostly the replication protocol: a
  record is committed only after a follower's *next* fetch reports the new
  log end offset to the leader.

## How to reproduce

```bash
scripts/cluster.sh start
export STREAMHUB_BOOTSTRAP=127.0.0.1:9092,127.0.0.1:9093,127.0.0.1:9094
bin/streamhub topic create --name rf3 --partitions 3 --replication-factor 3
bin/streamhub perf produce --topic rf3 --records 500000 --size 100 --acks all --idempotent
bin/streamhub perf consume --topic rf3 --records 500000
bin/streamhub perf produce --topic rf3 --records 2000 --size 100 --acks all --inflight 1 --linger 0s
scripts/cluster.sh clean
# fsync variant:
STREAMHUB_BROKER_ARGS="--fsync" scripts/cluster.sh start

go test ./internal/storage -run XXX -bench . -benchtime 3s -count 3
```
