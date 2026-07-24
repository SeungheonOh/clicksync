# Single-connection BlockFetch throughput plan

Status: architecture approved for implementation

Scope: one N2N connection per configured relay; no multi-connection mode

## 1. Decision

Clicksync will keep exactly one node-to-node TCP connection to each configured
relay. That connection will carry ChainSync and BlockFetch concurrently through
Ouroboros mini-protocol multiplexing.

The implementation order is:

1. decouple BlockFetch range length from gOuroboros receive-queue length;
2. measure larger ranges on the same actual historical block span;
3. move BlockFetch out of the ChainSync callback;
4. prepare one bounded range of headers while the current range body streams;
5. issue sequential BlockFetch ranges back-to-back on the same connection.

There will be no worker pool, connection striping, duplicate connection per
operator, or configuration for multiple BlockFetch connections.

## 2. Why the current process stops near 100 blocks/second

### 2.1 Publication is already independent

The relay sessions, agreement producer, ordered normalizers, batch builder, and
ClickHouse publisher already run independently. A raw block can wait for
publication only after every bounded downstream queue fills.

The live run does not show that condition:

| Signal | Live observation |
|---|---:|
| Agreed rate | about 99-101 blocks/second |
| Agreed queue current | 0 blocks between bursts |
| Agreed queue high-water | 71 blocks |
| Normalization average | about 0.50 ms/block |
| Publication average | about 40-50 ms/batch |
| Reconnects / disagreements | 0 / 0 |

Published blocks repeatedly catch agreed blocks. ClickHouse and normalization
are therefore not holding BlockFetch at the current rate.

### 2.2 ChainSync currently waits for each complete BlockFetch range

The current relay session has this control flow:

```text
ChainSync callback
  -> append one header
  -> after 512 headers, call flushPending()
       -> request BlockFetch range
       -> wait for BatchDone
  -> return from ChainSync callback
  -> permit more ChainSync responses
```

`onRollForward` calls `flushPending` synchronously. `flushPending` does not
return until the BlockFetch callback has received the complete range and
`BatchDone`. gOuroboros does not tell its ChainSync loop to continue until the
callback returns.

Consequently the connection alternates between:

```text
collect headers -> stream bodies -> collect headers -> stream bodies
```

It does not overlap the two independent mini-protocols.

### 2.3 Actual relay timestamps expose the bubble

`blocks.observed_at` is stamped from the latest raw callback among the required
relays. It measures when a block became available for strict agreement, before
normalization or insertion.

For the last 100 complete, aligned 512-block ranges in one uninterrupted live
attempt:

| Measurement | Result |
|---|---:|
| Average 512-block body stream | 1,229.9 ms |
| Median body stream | 1,180.1 ms |
| p95 body stream | 2,224.3 ms |
| Average gap to the next range's first body | 3,950.7 ms |
| Effective rate | 98.8 blocks/second |
| Body-only no-gap projection | 416.3 blocks/second |

The projection is not a promised result. Continuous operation will be limited
by the slower of header production and body delivery. It does show that the
current body stream can deliver much faster than the end-to-end rate and then
sits without another ready request for most of the cycle.

### 2.4 The host network is not the present ceiling

During the same live sync:

| Measurement | Result |
|---|---:|
| Clicksync container ingress | about 25.9 Mbit/s |
| Host Ethernet link | 1,000 Mbit/s, full duplex |
| Host interface errors / drops | 0 / 0 |
| Independent 50 MB WAN probe | about 605 Mbit/s |
| Clicksync CPU | low single-digit percentage of one core |

The independent probe ran while Clicksync remained live. It does not prove
that every route to every relay can sustain 605 Mbit/s, but it rules out the
host's access link as the reason Clicksync is receiving only about 26 Mbit/s.

After the scheduling bubble is removed, a relay, route, or single TCP flow may
become the next ceiling. Clicksync will measure and report that ceiling; it
will not open more connections to bypass it.

## 3. Correcting the 512 assumption

`blockfetch.MaxRecvQueueSize == 512` is a gOuroboros receive-message queue
limit. It is not a maximum number of blocks in `MsgRequestRange`.

A BlockFetch range request contains only a start point and end point. The
protocol state remains in `Streaming` until `BatchDone`, and the receive loop
drains messages continuously. A range may therefore contain more than 512
blocks.

Clicksync currently conflates these independent limits:

```text
protocol receive queue: capacity for messages waiting for callbacks
BlockFetch range:        number of consecutive headers covered by one request
```

The implementation will separate them:

```text
CLICKSYNC_PROTOCOL_QUEUE_SIZE        default 512, hard maximum 512
CLICKSYNC_BLOCKFETCH_RANGE_BLOCKS    initial default selected by live A/B
```

The first live comparison will use 512, 1,024, 2,048, 4,096, and 8,192 blocks.
The initial application hard cap will be 8,192. This is a memory and rollback
responsiveness bound, not a protocol limit.

The old `CLICKSYNC_HEADER_BATCH_SIZE` name will be removed rather than kept as
an ambiguous alias.

## 4. Required invariants

The refactor must preserve all of these:

1. Exactly one N2N connection exists per configured relay.
2. Each logical relay remains exactly one vote in strict agreement.
3. Only one BlockFetch range is active on a connection at a time.
4. ChainSync header order is never changed.
5. A raw callback is paired with the expected header ordinal and block type.
6. Relay zero alone retains raw CBOR; other relays hash and discard it.
7. Forward and rollback events leave a session in exact ChainSync order.
8. All header, range, raw-byte, and output buffering is bounded.
9. Backpressure eventually reaches the ChainSync callback and the socket.
10. Any protocol/count/order failure cancels the relay session and lets the
    existing outer attempt restart from durable state.
11. No ledger, body, signature, transaction, or semantic validation is added.

## 5. Target architecture

```text
one N2N TCP connection per relay
  |
  +-- ChainSync mini-protocol
  |     |
  |     +-- ordered callbacks
  |           - append contiguous headers
  |           - flush at range size or tip
  |           - enqueue rollback after pending headers
  |                         |
  |                         v
  |                 one ordered job slot
  |                         |
  +-- BlockFetch mini-protocol
        |
        +-- one dedicated sequential fetch loop
              - request prepared range
              - emit raw block events
              - wait for BatchDone
              - immediately take next prepared range
                         |
                         v
                 existing relay event queue
                         |
                         v
           agreement -> normalize -> ClickHouse
```

“Dedicated worker” means one goroutine responsible for the one BlockFetch
client on the existing connection. It does not mean an additional connection
or concurrent range request.

### 5.1 ChainSync callback

gOuroboros invokes one ChainSync client's message callbacks in protocol order.
Those callbacks therefore remain the single owner of the pending header slice.
No header-coordinator goroutine or intermediate token queue is needed.

The job type is:

```go
type sessionJob struct {
    kind     jobKind // fetch or rollback
    headers  []expectedHeader
    rollback model.Point
    tip      model.Point
}
```

For a roll forward the callback:

- derives the point and block type;
- appends one expected header;
- detaches the pending slice into an immutable fetch job when the configured
  range is full or the header reaches the advertised tip;
- sends that job to the one-slot FIFO; and
- returns without calling or waiting for BlockFetch.

For a rollback the callback first enqueues any pending partial fetch job, then
enqueues the rollback job. FIFO insertion is cancellation-aware. Blocking on a
full slot is intentional bounded backpressure.

The callback never shares a mutable header slice with the fetch loop. It
detaches the completed slice and starts a fresh one before returning.

### 5.2 Sequential BlockFetch loop

One new goroutine consumes the ordered job FIFO and is the sole caller of
`GetBlockRange`.

For each fetch job it:

1. installs the expected header slice as the active range;
2. requests the inclusive first-to-last range;
3. receives raw callbacks in order;
4. compares callback count and block type with the expected ordinal;
5. hashes the exact raw bytes;
6. retains bytes only for relay zero;
7. emits the relay event through the existing bounded channel;
8. waits for `BatchDone`;
9. clears the active range;
10. immediately consumes the already prepared next job.

For a rollback job it emits the rollback event immediately. Since fetch and
rollback jobs use one FIFO and one consumer, no result reordering layer or
barrier coordinator is required.

Calling `GetBlockRange` concurrently or from `BatchDoneFunc` is forbidden.
gOuroboros serializes the BlockFetch client with a busy lock, and the protocol
allows a new request only after returning to `Idle`.

### 5.3 Buffer bounds

Only headers are prefetched; raw bodies continue to stream through the existing
byte budget.

Initial bounds:

| Buffer | Bound |
|---|---:|
| Pending callback headers | one configured range |
| Ordered jobs | one range/rollback job |
| Active BlockFetch jobs | one range |
| Relay output | existing 256 events |
| Retained raw bytes | existing shared 256 MiB |

At most roughly three ranges of small header metadata can exist between the
ChainSync callback and active fetch. There is never an unbounded historical
header list and never a full-range raw-body cache.

## 6. Ordering and rollback behavior

### 6.1 Normal historical forward flow

```text
ChainSync builds job N+1
        concurrently with
BlockFetch streaming job N

BatchDone(N)
  -> request job N+1 immediately
  -> ChainSync builds job N+2
```

Because there is one fetch loop, no result reordering layer is required.

### 6.2 Rollback barrier

Suppose ChainSync produces:

```text
headers A -> headers B -> rollback R -> headers C
```

The ordered callback queue produces:

```text
fetch(A) -> fetch(B) -> emit(R) -> fetch(C)
```

The rollback cannot overtake a body range whose headers preceded it. Work
after the rollback cannot be requested before the barrier is emitted.

If a pre-rollback range is no longer available from the relay, the attempt
fails. Clicksync closes the connection and restarts from its durable
intersection candidates. It does not guess, skip, or retry an ambiguous
partial range.

At the live tip, partial ranges are small, so normal shallow rollbacks do not
wait behind an 8,192-block historical request.

### 6.3 Failure and shutdown

Any of these cancels the complete relay session:

- ChainSync or BlockFetch disconnect;
- `NoBlocks` for a requested expected range;
- wrong callback count or block type;
- callback without an active job;
- queue byte-bound violation;
- callback queue or output failure.

Cancellation closes the one connection, unblocks both mini-protocols, releases
reserved raw bytes, and allows the existing runner retry policy to resume from
durable state. There is no in-session range retry.

## 7. Minimal performance instrumentation

The implementation needs enough evidence to select a range size and prove that
the bubble is gone, without adding a metrics framework.

Per relay, the periodic structured progress log will expose:

- headers received;
- pending-header count and job-slot current/high-water;
- BlockFetch ranges and body blocks/bytes;
- average range size;
- average body-stream duration;
- average gap from one `BatchDone` to the next request's `StartBatch`;
- prepared-job availability when the fetch loop becomes idle;
- BlockFetch active duty cycle;
- callback time blocked on downstream backpressure.

Interpretation:

| Observation | Limiter |
|---|---|
| No prepared job when BlockFetch becomes idle | ChainSync/header production |
| Prepared job exists but request-start gap is high | local scheduler bug |
| BlockFetch duty near 100%, low host ingress | relay/path/single-flow ceiling |
| Relay/agreed queues remain full | downstream backpressure |
| Agreed queue drains and publisher stays caught up | relay intake |

## 8. Work items

### WI-01: Separate range length from receive-queue length

Requirements:

- Replace `HeaderBatchSize` with `BlockFetchRangeBlocks`.
- Remove the false `range <= blockfetch.MaxRecvQueueSize` rule.
- Keep protocol receive queue validation in `1..512`.
- Add an application range bound in `1..8192`.
- Keep the current default at 512 until actual-data comparison completes.

Verification:

- Configuration accepts a 4,096-block range with a 512-message receive queue.
- Configuration still rejects a receive queue above 512.
- One actual relay returns a complete range larger than 512 with exact callback
  count and order.

### WI-02: Add phase timing before changing control flow

Requirements:

- Time header collection, request-to-`StartBatch`, body streaming, inter-range
  gap, and downstream callback blocking.
- Attribute observations by configured relay index.
- Add no per-block logging.

Verification:

- Reproduce the existing roughly 1.23-second stream / 3.95-second next-body
  gap on an actual relay span.
- Reconcile range block/byte totals with existing agreed counters.

### WI-03: Test larger ranges on the current single connection

Run the production sync path against the same reset historical ClickHouse span
for range sizes:

```text
512, 1024, 2048, 4096, 8192
```

Record:

- agreed blocks/second;
- range phase timings and active duty;
- ingress Mbit/s;
- RSS and pending-header/job-slot high-water;
- reconnects, `NoBlocks`, mismatches, and rollbacks;
- published blocks/second and publication lag.

Selection rule:

- choose the smallest range within 5% of the best sustained rate;
- reject a range that causes protocol failures or unacceptable shutdown and
  rollback responsiveness;
- stop here if the larger range already keeps BlockFetch continuously active
  and saturates the observed relay/path ceiling.

### WI-04: Decouple ChainSync and sequential BlockFetch

Requirements:

- ChainSync callbacks create and enqueue ordered immutable range and rollback
  jobs; they never invoke or wait for BlockFetch.
- One fetch loop owns the one BlockFetch client.
- One prepared range is allowed while one range streams.
- Existing raw-byte ownership and relay event interfaces remain unchanged.
- Session cancellation owns and terminates all new goroutines.

Verification:

- While range N streams, range N+1 becomes prepared.
- When N reaches `BatchDone`, N+1 is requested without waiting for another
  range of headers.
- Output points and digests exactly match the pre-refactor implementation.
- The next-request gap is no longer the dominant portion of the cycle.

### WI-05: Preserve barriers, retry, and bounded memory

Requirements:

- Partial headers flush before a rollback.
- A rollback is emitted only after every preceding range event.
- No post-rollback range is requested early.
- Cancellation releases every retained-byte reservation.
- A failed range cancels the attempt instead of being silently skipped.

Verification:

- One focused deterministic test covers forward range, partial range, rollback
  barrier, and resumed forward order.
- One focused cancellation test covers an active range plus a prepared range
  and verifies all goroutines and byte reservations terminate.
- Do not add semantic block-validation tests or a large mock matrix.

### WI-06: Actual-relay acceptance and default selection

Use the real configured IOG and Cardano Foundation relays. Reset the disposable
ClickHouse dataset to the same fixed historical point for each candidate so
block-size distribution is identical.

Required run:

- at least 100,000 consecutive actual blocks;
- strict two-relay agreement;
- no gaps, duplicate block numbers, duplicate hashes, or duplicate active
  publication IDs;
- no mismatches or reconnect loops;
- published blocks remain caught up after final flush;
- ClickHouse insertion remains faster than agreed intake;
- bounded RSS and queue high-water;
- graceful restart from the resulting durable tip.

Performance result:

- report sustained agreed and published blocks/second;
- report body-stream duty cycle and remaining idle reason;
- report actual ingress Mbit/s against the independently measured host
  capacity;
- set the default range to the smallest near-best value.

If the final single connection is continuously busy and the relay/path is the
remaining limit, document that result and stop. Multiple N2N connections are
not a permitted follow-up.

## 9. Test policy

Testing stays deliberately small:

1. existing unit and store tests continue to pass;
2. add only the two concurrency/barrier tests named in WI-05;
3. use actual relay replays for throughput and wire behavior;
4. use ClickHouse integrity queries for final end-to-end correctness;
5. do not build a synthetic relay simulator or a combinatorial scheduler test
   suite.

The performance work is successful only when actual relay data improves. A
microbenchmark alone cannot satisfy any throughput acceptance item.

## 10. Acceptance gates

Correctness gates:

- exactly one N2N connection per configured relay;
- exact event order through rollbacks;
- identical point/type/raw-digest sequence before and after the refactor;
- no unbounded buffer;
- existing restart and ClickHouse authority behavior unchanged.

Performance gates:

- a prepared job produces a back-to-back next range request;
- body-delivery gaps are no longer dominated by header collection;
- agreed and published rates remain equal over a completed run;
- CPU, ClickHouse, and queues do not replace the network as bottleneck;
- chosen range is supported by actual same-span A/B evidence.

The 416.3 blocks/second no-gap figure is a diagnostic upper projection from the
measured span, not a release requirement. The achieved rate will be the slower
of continuous ChainSync header production and continuous BlockFetch body
delivery on the single relay connection.

## Appendix A: live range timing query

The following query measures complete aligned ranges from the first block of a
known uninterrupted attempt. Replace `10906462` with that attempt's first block
number.

```sql
WITH
    10906462 AS base,
    grouped AS
    (
        SELECT
            intDiv(block_number - base, 512) AS range_id,
            count() AS blocks,
            min(observed_at) AS first_seen,
            max(observed_at) AS last_seen
        FROM blocks
        WHERE block_number >= base
        GROUP BY range_id
    ),
    timed AS
    (
        SELECT
            *,
            (toUnixTimestamp64Micro(last_seen) -
             toUnixTimestamp64Micro(first_seen)) / 1000. AS range_ms,
            (toUnixTimestamp64Micro(first_seen) -
             toUnixTimestamp64Micro(
                 lagInFrame(last_seen, 1, last_seen) OVER
                 (
                     ORDER BY range_id
                     ROWS BETWEEN UNBOUNDED PRECEDING
                              AND UNBOUNDED FOLLOWING
                 )
             )) / 1000. AS gap_ms
        FROM grouped
    ),
    samples AS
    (
        SELECT *
        FROM timed
        WHERE blocks = 512 AND range_id > 0
        ORDER BY range_id DESC
        LIMIT 100
    )
SELECT
    count() AS ranges,
    round(avg(range_ms), 1) AS avg_stream_ms,
    round(quantileExact(0.5)(range_ms), 1) AS median_stream_ms,
    round(quantileExact(0.95)(range_ms), 1) AS p95_stream_ms,
    round(avg(gap_ms), 1) AS avg_inter_range_gap_ms,
    round(512000. / (avg(range_ms) + avg(gap_ms)), 1)
        AS effective_blocks_s,
    round(512000. / avg(range_ms), 1)
        AS no_gap_projection_blocks_s
FROM samples;
```
