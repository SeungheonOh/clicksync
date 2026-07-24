# ClickSync sync-speed investigation and verdict

Date: 2026-07-24

Repository state reviewed: `7cebb059a1ff47c168c6a74a8103facd3c6c85ae`

Measured ClickHouse version: `26.3.12.3`

## Verdict

The demonstrated bottleneck is primarily ClickSync's ClickHouse publication
path, not an inability to obtain blocks from P2P peers.

The first change to test is explicitly disabling ClickHouse asynchronous
inserts for the ClickSync writer. ClickHouse 26.3 enables them by default, but
ClickSync already batches rows client-side and waits after every ordered
insert. During the observed sync, all 18,414 async insert requests caused
18,414 separate flushes. No requests were combined, while the adaptive
timeouts accumulated 406 seconds during 2,713 seconds of active sync time.

The second change to test is increasing `CLICKSYNC_QUEUE_CAPACITY` from `4` to
its existing validated maximum of `32`. That permits a complete 32-body
BlockFetch range to wait behind database publication instead of stalling after
four decoded blocks.

Increasing the P2P range from 32 to 64 is not the recommended first change.
With the current four-event queue and blocking publication path, a larger
range would encounter the same backpressure. It would also require code
changes because both the header range and queue are currently capped at 32,
and a slower range risks reaching the one-second publication-age limit.

The safer route to a roughly 64-block database batch is:

1. disable unproductive async inserts;
2. increase the decoded-event queue to 32;
3. retain validated 32-block P2P ranges; and
4. let one publication batch accumulate two ranges when its row, byte,
   rollback, and age limits permit.

Only test a 64-block P2P range if instrumentation later proves that range
request overhead remains important after database backpressure is reduced.

## What was measured

The evidence came from the local ClickHouse `system.query_log`,
`system.asynchronous_insert_log`, table metadata, and `EXPLAIN indexes=1`.
No sync run was started or altered for this investigation.

The captured sample was an Alonzo-era partial-history run made on the same
sync, publication, and storage implementation reviewed here. Subsequent
repository commits changed the configured boundary and normalization details,
not the measured performance paths.

Three active intervals published 58,178 blocks:

| Run | Blocks | Active seconds | Blocks/s | Batches | Blocks/batch |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 16,801 | 719.5 | 23.35 | 564 | 29.79 |
| 2 | 16,288 | 795.6 | 20.47 | 597 | 27.28 |
| 3 | 25,089 | 1,198.3 | 20.94 | 892 | 28.13 |
| **Weighted total** | **58,178** | **2,713.4** | **21.44** | **2,053** | **28.34** |

Batch-size evidence:

- median, p90, and p99 batch size: 32 blocks;
- 1,580 of 2,053 batches (77%) contained exactly 32 blocks;
- the publication maximum is 256 blocks, 32 MiB, 1,000,000 fact rows, or one
  second, whichever limit is reached first
  ([publication limits](../internal/publication/publication.go#L100));
- the batch timer currently flushes after 500 ms
  ([handler timer](../internal/ingest/handler.go#L27));
- the N2N header range and queues are capped at 32, while the event queue
  defaults to 4
  ([configuration](../internal/config/config.go#L159),
  [N2N queues](../internal/n2n/client.go#L111)).

Receiving full 32-block ranges in 77% of batches indicates that P2P peers were
generally supplying data. It does not prove that P2P and decoding are free:
server query time accounted for roughly 46% of active runtime, while the rest
included network transfer, CBOR decoding, normalization, checkpoints,
application work, and unmeasured waiting. The database path is the first
demonstrated constraint and also propagates backpressure into P2P.

The measurement is host-specific. Other containers were active on the same
16-core/32-thread host, and ClickSync currently exposes no phase timings or
queue-occupancy metrics. These results identify optimization targets but are
not a universal throughput benchmark.

## Finding 1: default async inserts add delay without batching

ClickSync does not explicitly set `async_insert` on its native ClickHouse
connection, so it inherits the server default
([connection settings](../internal/store/db.go#L23)). ClickHouse 26.3.12.3
reported:

```text
async_insert = 1
wait_for_async_insert = 1
```

The async insert log showed:

| Table | Insert requests | Flushes | Requests/flush | Average adaptive timeout |
| --- | ---: | ---: | ---: | ---: |
| `dataset_manifest` | 2,908 | 2,908 | 1.00 | 44.8 ms |
| `transactions` | 2,046 | 2,046 | 1.00 | 59.4 ms |
| `outputs` | 2,046 | 2,046 | 1.00 | 49.6 ms |
| all ClickSync tables | 18,414 | 18,414 | 1.00 | 22.1 ms |

The lack of coalescing follows from the current design: one writer inserts
fact tables sequentially, verifies them, inserts adoption rows, and then
advances the manifest
([ordered publication](../internal/publication/publication.go#L501)).
With `wait_for_async_insert=1`, the writer cannot submit the next compatible
insert while the current request waits to flush.

The largest server-reported query costs were:

| Query family | Calls | Aggregate query duration |
| --- | ---: | ---: |
| insert `dataset_manifest` | 2,909 | 482.05 s |
| insert `transactions` | 2,046 | 139.69 s |
| insert `outputs` | 2,046 | 130.78 s |
| insert `transaction_metadata` | 2,005 | 49.32 s |

The manifest is especially expensive because every physical batch appends a
full, constrained manifest row. Of 2,924 manifest revisions, 2,053 (70.2%)
were `physical_adoption` revisions—one per batch.

ClickHouse's current guidance says synchronous inserts are appropriate when a
client can batch and async inserts shift batching to the server. It also says
`wait_for_async_insert=1` acknowledges only after the buffer is flushed.
ClickSync already implements client-side batching and needs acknowledgement
before adoption:

- [Selecting an insert strategy](https://clickhouse.com/docs/best-practices/selecting-an-insert-strategy)
- [ClickHouse 26.3: async inserts enabled by default](https://clickhouse.com/blog/clickhouse-release-26-03)

### Recommended experiment

Run the same frozen block interval twice on fresh databases:

1. current defaults;
2. `async_insert=0` explicitly for the ClickSync connection or ingest user.

Keep peer, queue, header range, host load, ClickHouse version, and storage
constant. Compare blocks/s, publication latency, insert flush count, created
parts, merges, CPU time, and final fact digests. Include shutdown, restart,
lost-response, and rollback tests.

Synchronous inserts retain acknowledgement and backpressure. Do not replace
the current mode with `wait_for_async_insert=0`: fire-and-forget insertion
could let verification or adoption race facts that have not reached storage.

## Finding 2: database publication stalls P2P intake

`CLICKSYNC_QUEUE_CAPACITY` controls:

- the decoded chain-event channel;
- the BlockFetch receive queue;
- the ChainSync pipeline limit; and
- the ChainSync receive queue
  ([N2N configuration](../internal/n2n/client.go#L823)).

The default is 4 and the validated maximum is 32. While publication holds the
handler mutex and performs database work, only four decoded events can wait in
the application channel. A 32-block BlockFetch range therefore reaches
application backpressure early.

This is why P2P and database time are not fully separable: slow publication
prevents the P2P callback from delivering more bodies. A larger P2P range
does not remove that dependency.

### Recommended experiment

After isolating the async-insert result, repeat the same benchmark with:

```text
CLICKSYNC_QUEUE_CAPACITY=32
CLICKSYNC_HEADER_BATCH_SIZE=32
```

This preserves the existing maximum range and validation behavior. Measure
queue high-water marks and process RSS because the cost is additional bounded
decoded-block memory. Test this separately from `async_insert=0` so the
individual gain can be attributed.

## Finding 3: use multiple 32-block ranges per publication

ClickHouse recommends at least 1,000 rows per synchronous insert and ideally
10,000–100,000 because every insert carries part and index overhead.
ClickSync already uses the Native protocol and LZ4
([connection](../internal/store/db.go#L31)), which match ClickHouse's
[insert guidance](https://clickhouse.com/docs/best-practices/selecting-an-insert-strategy).
The remaining issue is effective batch fill.

At 32 blocks, high-volume `inputs` and `outputs` inserts are reasonably sized,
but `blocks`, `chain_events`, and especially the one-row manifest revision
remain small. Verification also performs count and content-digest reads for
every physical batch
([batch verification](../internal/store/publication.go#L1262),
[content verification](../internal/store/verification.go#L128)).

### Recommended design

Keep each BlockFetch range at 32 initially. Permit a backfill publication to
accumulate multiple completed and verified ranges. Make the flush age
configurable and test a 64-block publication target while retaining all
existing row, byte, block, rollback, and one-second age limits.

For this sample, perfect 64-block fill would reduce physical-adoption batches
from 2,053 to about 909. At the observed 165.7 ms average manifest insert
duration, avoiding 1,144 manifest writes represents roughly 190 seconds of
server query time before counting fewer fact inserts, verification cycles,
parts, and merges. This is a directional estimate, not a promised wall-clock
gain.

### Why not immediately fetch 64 bodies per P2P range?

- Both configuration validation and the N2N client currently cap the range at
  32, so 64 is not an operational setting change.
- The event queue defaults to 4, so the larger range would still stop behind
  database publication.
- The range-starting ChainSync callback remains blocked until the full
  BlockFetch range completes. A larger range extends that protocol-critical
  section.
- The first staged block starts a 500 ms flush timer and publication rejects
  a batch older than one second. A slow 64-body range reduces the remaining
  timing margin.
- Two separately verified 32-block ranges give the database the same
  64-block publication opportunity with less change to rollback and
  range-validation behavior.

## Finding 4: source-UTxO lookups will worsen with history size

Each publication resolves referenced outputs in chunks of 256 because the Go
driver expands array parameters into SQL text
([lookup chunking](../internal/store/publication.go#L19)). The two lookup
families performed:

| Lookup | Calls | Rows read | Selected marks/call | Aggregate query duration | User CPU |
| --- | ---: | ---: | ---: | ---: | ---: |
| output facts | 5,571 | 7.59 billion | 175.7 | 102.19 s | 753.7 s |
| active consumptions | 5,571 | 6.85 billion | 154.0 | 94.19 s | 636.7 s |

User CPU is summed across ClickHouse query threads and can exceed wall time.
The scaling signal is rows read: average output lookup reads rose from about
216,000 rows early in the sample to 1.84 million near the end; input lookups
rose from about 199,000 to 1.54 million.

The table sort orders already align with the lookup predicates
([input/output schema](../migrations/001_initial.sql#L505)).
`EXPLAIN indexes=1` confirmed primary-key use, but ClickHouse's sparse primary
index reads complete granules. The default granule contains 8,192 rows, and
the measured calls read roughly 7,750–7,980 rows per selected mark. Random
transaction hashes spread each 256-key query across many granules and active
parts.

ClickHouse documents this behavior in
[Primary indexes](https://clickhouse.com/docs/primary-indexes).

### Recommended prototypes

1. Send all microbatch references as a typed external table instead of
   expanding 256-element array chunks. The repository already uses external
   tables for expected-content verification
   ([example](../internal/store/verification.go#L65)). This can reduce query
   count and repeated granule reads without increasing `max_query_size`.
2. On a representative copy, benchmark `inputs` and `outputs` with
   `index_granularity` values such as 512 and 256. Smaller granules improve
   point-lookup precision but increase primary-index and mark-file storage,
   insertion work, and metadata.
3. Use `EXPLAIN indexes=1`, selected marks, read rows/bytes, storage size, and
   end-to-end publication time as acceptance evidence. Another projection
   with the same ordering is unlikely to help because the base primary key is
   already selected.

## Lower-priority work

- Add ingestion metrics for header-range fill, BlockFetch duration, decoded
  queue occupancy, normalization duration, batch flush reason/size/rows/bytes,
  and every database phase.
- After reducing rows read, test concurrent output-fact and consumption
  lookups. They are independent, but both use substantial CPU and may contend.
- Only then consider parallel fact-table inserts and verification reads.
  Facts are inert until adoption, but the ordered fault-injection and crash
  semantics require a fresh audit.
- Cache immutable schema column types used by content verification. This
  removes many round trips but accounted for only 0.16 seconds of server
  execution in the sample.

## Changes not recommended

- Do not reduce the two-operator corroboration threshold; that trades
  correctness for speed and affects only periodic checkpoints.
- Do not remove exact fact verification or publish adoption before facts are
  confirmed.
- Do not use `wait_for_async_insert=0`.
- Do not raise `max_query_size` as the only UTxO lookup fix; it avoids a query
  limit but does not address sparse-granule reads.
- Do not assume more peers will parallelize ordered chain ingestion.
  Secondary peers provide trust evidence, while one selected primary supplies
  the contiguous body stream.
- Do not increase the P2P range to 64 before removing publication
  backpressure and measuring range-request overhead.

## Recommended order

1. Add phase metrics to the benchmark harness without changing publication
   semantics.
2. A/B `async_insert=0`.
3. A/B queue capacity 4 versus 32.
4. Use the winning settings to test a 64-block publication batch made from
   multiple 32-block P2P ranges.
5. Only if P2P range overhead is then material, prototype a 64-block range and
   re-audit timeout, rollback, memory, and range-validation behavior.
6. Prototype external-table source references and smaller lookup granules.
7. Re-profile before attempting database write/read concurrency.

The likely short-term improvement comes from removing async buffering that
does not batch and allowing more bounded overlap between P2P delivery and
publication. Substantially faster long-history sync requires larger effective
publication batches and a source-UTxO lookup path whose rows read do not grow
rapidly with accumulated history.
