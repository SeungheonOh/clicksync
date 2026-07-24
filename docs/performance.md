# Throughput and performance plan

## 1. Goal

Insertion must not be the normal bottleneck. With healthy local ClickHouse,
end-to-end sync should be limited by the slowest required relay, network
bandwidth, or decode/normalization—not avoidable per-batch database
round-trips.

The legacy service is reported around 100 blocks/second after optimization.
The clean-slate local replay target is at least 1,000 representative
blocks/second and a publication capacity at least twice the measured strict
multi-relay intake rate on the same host.

Blocks/second alone is not sufficient because era and transaction density
vary. Every result also reports raw MiB/second and fact rows/second.

The current relay path keeps one N2N connection per relay. Its internal
ChainSync driver uses gOuroboros's public mux, protocol, decoder, and message
types without a dependency fork. It permits at most 100 outstanding
`RequestNext` messages, writes one request at a time, and refills from 99 to
100 after every completed callback.
ChainSync ingress and the sequential BlockFetch worker run independently.

In an actual two-relay run writing to ClickHouse, this path sustained about
545-585 agreed and published blocks/second, compared with the earlier roughly
100 blocks/second. The measured run had no reconnects or mismatches, and a
packet capture showed one 20-request refill write about every 29-31 ms. Public
relay and block-span variability make this an observed range, not a fixed
throughput guarantee.

A later, denser span averaged 23.6 kB/block and ran near 380 blocks/second at
about 72 Mbit/second per relay. That byte rate predicts 380 blocks/second,
while ChainSync headers remained ahead of bodies and the limiting BlockFetch
worker stayed about 95% active. This ties the lower block count directly to
relay body bandwidth and block size rather than insertion or an empty header
pipeline.

## 2. Performance invariants

Ordinary roll-forward must have:

- zero ClickHouse `SELECT` statements;
- zero per-block inserts;
- zero `dataset_manifest` writes;
- zero successful fact readbacks;
- zero UTxO/source/datum existence lookups;
- at most one insert per populated fact table per microbatch;
- one final adoption insert per microbatch;
- concurrent independent fact-table inserts;
- explicitly synchronous client inserts (`async_insert=0`);
- bounded overlap among network, normalization, and publication.

Rollback and ambiguous commit recovery may read ClickHouse because they are
exception paths.

## 3. Instrumentation

The always-on runtime metrics stay deliberately small: lifetime agreement and
publication rates/counters, average agreement/normalization/publication
duration, and current/high-water agreed-queue occupancy. Relay sessions also
report header/body rates, range timing and duty, prepared-range overlap, and
current/high-water bounded queues. These are focused session counters, not a
general worker-utilization subsystem.

For a focused benchmark or bottleneck investigation, measure these phases
independently with benchmark hooks, Go profiles, and ClickHouse diagnostics:

1. relay download and raw callback;
2. raw digest;
3. agreement wait;
4. decode;
5. normalize;
6. batch wait/fill;
7. each fact table insert;
8. full fact-insert wall time;
9. adoption insert;
10. end-to-end durable acknowledgement.

Also record:

- blocks, raw bytes, fact rows;
- blocks per range and publication batch;
- queue current/high-water items and bytes;
- worker busy time;
- ClickHouse parts created and merge activity;
- process CPU, RSS, allocations, and GC;
- ClickHouse CPU, memory, written rows/bytes, and query count.

These detailed measurements are a performance-test protocol, not an
always-on runtime metrics contract.

## 4. Benchmark datasets

### Fixture microbenchmarks

Use the real licensed legacy fixtures for each era to compare parser behavior
and catch pathological allocations.

### Representative replay

Use at least 100,000 real consecutive Conway-era blocks when available. The
run has:

- a warm-up prefix excluded from totals;
- fixed worker/queue/batch settings;
- a fresh ClickHouse database;
- no public-relay variability;
- the same normalized result digest between compared configurations.

If the repository cannot redistribute the range, the benchmark harness reads
an operator-provided local CBOR stream and documents its start/end points.
The current repository has no fixed consecutive local corpus; see
[performance-results.md](performance-results.md) for both the completed
publication-only benchmark and the variable public-relay replay.

### Live relay run

Run at least two independently operated public relays with strict all-of-N
agreement. Report aggregate agreed bytes, agreement wait, exact relay
identities, and the session-local per-relay fetch rates. Do not use the public
run alone to judge insertion capacity.

## 5. Acceptance gates

### Normalization

- Concurrent decode/normalize capacity is at least twice live agreed intake.
- Reorder queues do not remain at their high-water mark in the local replay.
- No block is decoded more than once in the ordinary agreement path.

### Publication

- Sustained local replay is at least 1,000 representative blocks/second.
- Publication-only capacity is at least twice measured live agreed intake.
- No ordinary roll-forward `SELECT` appears under the Clicksync query user.
- Insert request count matches populated tables plus one adoption per batch.
- Median publication batch is not range-size-limited during historical
  backfill unless byte/row limits require it.

### End to end

- A 100,000-block run completes without unbounded RSS growth.
- Queue occupancy shows continued overlap rather than serialized network and
  database phases.
- Shutdown commits the final complete prefix.
- Restart resumes without republishing the committed prefix.

If a numeric gate fails, the result must include phase timings and identify
the limiting resource. It is not acceptable to raise unbounded queues or drop
agreement to obtain a headline rate.

## 6. Tuning sequence

Tune one dimension at a time:

1. normalizer worker count;
2. raw/reorder window;
3. BlockFetch range size;
4. publication block/row/byte/age limits;
5. ClickHouse connection pool;
6. table insert concurrency.

Retest output equality, crash visibility, memory, parts, and rollback after
each change.

## 7. Initial settings to validate

```text
ChainSync outstanding:        100 requests
BlockFetch range:             512 blocks
BlockFetch receive queue:     512 messages
per-relay event queue:        256
agreed/reorder window:        256 blocks / 256 MiB
normalizer workers:           GOMAXPROCS
publication max blocks:       1,024
publication max raw bytes:    128 MiB
publication max fact rows:    2,000,000
publication max age:          1 second
ClickHouse open conns:        16
```

These are starting hypotheses, not permanent magic numbers. Configurable
defaults may change only with benchmark evidence. The ChainSync maximum is
fixed by the protocol.

## 8. Expected improvement sources

Compared with the legacy path, the rewrite removes:

- two history-growing source-state queries per reference chunk;
- existing-datum queries;
- full fact verification queries;
- manifest reads/writes and evidence transitions;
- sequential waiting across all fact-table inserts;
- repeated decoding/validation work across relay observations.

It adds full body bandwidth for every required relay. That is intentional:
network is the chosen trust cost, and the target architecture makes this cost
visible instead of paying hidden validation and insertion costs.
