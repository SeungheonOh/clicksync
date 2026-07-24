# Single-connection BlockFetch throughput plan

Status: implemented and measured with actual relays

Scope: one N2N connection per configured relay; no multi-connection mode

## 1. Decision

Clicksync keeps exactly one node-to-node TCP connection to each configured
relay. That connection carries ChainSync and BlockFetch concurrently through
Ouroboros mini-protocol multiplexing.

The implemented shape is:

1. decouple BlockFetch range length from gOuroboros receive-queue length;
2. maintain a bounded sliding ChainSync `RequestNext` window;
3. make ChainSync callbacks enqueue-only;
4. batch the ordered header stream in a dedicated range builder;
5. issue sequential BlockFetch ranges from an independent worker;
6. measure range sizes on the same actual historical block span.

There is no worker pool, connection striping, duplicate connection per
operator, or configuration for multiple BlockFetch connections.

## 2. Baseline: why the previous process stopped near 100 blocks/second

### 2.1 Publication is already independent

The relay sessions, agreement producer, ordered normalizers, batch builder, and
ClickHouse publisher already run independently. A raw block can wait for
publication only after every bounded downstream queue fills.

The baseline live run did not show that condition:

| Signal | Live observation |
|---|---:|
| Agreed rate | about 99-101 blocks/second |
| Agreed queue current | 0 blocks between bursts |
| Agreed queue high-water | 71 blocks |
| Normalization average | about 0.50 ms/block |
| Publication average | about 40-50 ms/batch |
| Reconnects / disagreements | 0 / 0 |

Published blocks repeatedly caught agreed blocks. ClickHouse and normalization
were therefore not holding BlockFetch at the baseline rate.

### 2.2 ChainSync waited for each complete BlockFetch range

The baseline relay session had this control flow:

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
baseline body stream could deliver much faster than the end-to-end rate and then
sits without another ready request for most of the cycle.

### 2.4 The host network was not the baseline ceiling

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
become the next ceiling. Clicksync measures that boundary but does not open
more connections to bypass it.

### 2.5 ChainSync request scheduling

The baseline used gOuroboros's general ChainSync client with a configured
pipeline limit of 100. Its sender advanced in cohorts of at most 20, yielding
an effective request cadence close to the observed 100 headers/second on these
relay paths.

Clicksync now owns a small ChainSync driver in
`internal/relay/chainsync_client.go`. It uses the public gOuroboros mux,
`protocol.Protocol`, state/message decoders, and ChainSync message types; no
gOuroboros fork or module-cache patch is involved. The driver has an exact
hard maximum of 100 outstanding `RequestNext` messages.

Initial and refill writes contain one request. Every completed callback
briefly lowers the outstanding count from 100 to 99, then immediately restores
it to 100. ChainSync ingress and the BlockFetch loop are independent workers
on the same muxed N2N connection.

Response liveness uses one ticker-backed watchdog per ChainSync client.
Callbacks update its deadline without allocating, stopping, or creating a
runtime timer for every header.

## 3. Correcting the 512 assumption

`blockfetch.MaxRecvQueueSize == 512` is a gOuroboros receive-message queue
limit. It is not a maximum number of blocks in `MsgRequestRange`.

A BlockFetch range request contains only a start point and end point. The
protocol state remains in `Streaming` until `BatchDone`, and the receive loop
drains messages continuously. A range may therefore contain more than 512
blocks.

The baseline implementation conflated these independent limits:

```text
protocol receive queue: capacity for messages waiting for callbacks
BlockFetch range:        number of consecutive headers covered by one request
```

The implementation separates them:

```text
CLICKSYNC_BLOCKFETCH_RANGE_BLOCKS    default 512, application maximum 8192
CLICKSYNC_BLOCKFETCH_QUEUE_SIZE      default 512, hard maximum 512
```

The ChainSync receive queue and outstanding-request window always use
gOuroboros's supported maximum of 100.

Range-size comparisons may use 512, 1,024, 2,048, 4,096, and 8,192 blocks.
The 8,192 application cap is a memory and rollback-responsiveness bound, not a
protocol limit.

The old `CLICKSYNC_HEADER_BATCH_SIZE` name was removed rather than retained as
an ambiguous alias.

## 4. Required invariants

The implementation preserves all of these:

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

## 5. Implemented architecture

```text
one N2N TCP connection per relay
  |
  +-- ChainSync mini-protocol
  |     - internal sliding RequestNext window
  |     - at most 100 outstanding
  |     - one request per initial/refill write
  |     |
  |     +-- ordered callbacks
  |           - convert point
  |           - enqueue header or rollback
  |                         |
  |                         v
  |                 bounded event FIFO
  |                         |
  |                         v
  |                 one range builder
  |                   - batch headers
  |                   - flush at range size or tip
  |                   - preserve rollback barriers
  |                         |
  |                         v
  |                 one prepared fetch slot
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

### 5.1 Pipelined ChainSync ingress

`internal/relay/chainsync_client.go` owns the ChainSync request schedule. It is
an independent internal driver built from gOuroboros's public mux, protocol,
decoder, and message APIs, rather than a fork of the dependency.

The outstanding window is fixed at the protocol maximum of 100. Startup fills
it with single encoded `MsgRequestNext` writes. Each completed roll-forward or
rollback callback immediately sends one replacement request, taking the
window from 99 back to 100. The earlier batch-20 live run emitted one
20-request refill write about every 29-31 ms.

For a roll forward the callback:

- derives the point and block type;
- sends one compact header event to a bounded FIFO; and
- returns without batching, calling BlockFetch, or waiting for publication.

Rollback callbacks enqueue to the same FIFO. The FIFO is sized to one complete
ChainSync request pipeline, so callbacks normally return immediately. If all
downstream stages fall behind, its cancellation-aware send deliberately
propagates TCP backpressure.

### 5.2 Range builder

A single range-builder goroutine consumes the ChainSync FIFO. It is the sole
owner of the mutable pending header slice. It:

1. appends forward headers in callback order;
2. detaches an immutable range when the configured size is reached or the
   advertised tip arrives;
3. flushes a partial range before a rollback;
4. enqueues the rollback after that partial range; and
5. continues batching post-rollback headers only after the barrier.

The range builder and fetch worker communicate through one prepared-job slot.
One slot is sufficient to keep sequential BlockFetch busy and prevents
unbounded header prefetch.

### 5.3 Sequential BlockFetch loop

One goroutine consumes the ordered job FIFO and is the sole caller of
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

### 5.4 Buffer bounds

Only headers are prefetched; raw bodies continue to stream through the existing
byte budget.

Bounds and defaults:

| Buffer | Bound |
|---|---:|
| Outstanding ChainSync requests | fixed at 100 |
| ChainSync callback events | 100 |
| Pending range-builder headers | one configured range |
| Prepared fetch jobs | one range/rollback |
| Active BlockFetch range | one |
| Relay output | existing 256 events |
| Retained raw bytes | existing shared 256 MiB |

There is never an unbounded historical header list or a full-range raw-body
cache.

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

The implementation exposes enough evidence to select a range size and check
whether the bubble remains, without adding a general metrics framework.

Per relay, the structured fetch-progress log exposes:

- headers received;
- ChainSync FIFO, pending-header, and fetch-slot current/high-water;
- BlockFetch ranges and body blocks/bytes;
- average range size;
- average body-stream duration;
- average inter-range idle time and `GetBlockRange` call time;
- prepared-job availability when the fetch loop becomes idle;
- BlockFetch active duty cycle;
- callback time blocked on downstream backpressure.

gOuroboros does not expose a `StartBatch` callback. The measured
`get_range_wait_avg` is wall time around `GetBlockRange`, which returns after
the library handles `StartBatch`; it is the nearest available proxy, not an
exact wire timestamp.

Interpretation:

| Observation | Limiter |
|---|---|
| No prepared job when BlockFetch becomes idle | ChainSync/header production |
| Prepared job exists but inter-range idle is high | local scheduling |
| Inter-range idle is low but `GetBlockRange` wait is high | relay/path/single-flow response |
| BlockFetch duty near 100%, low host ingress | relay/path/single-flow ceiling |
| Relay/agreed queues remain full | downstream backpressure |
| Agreed queue drains and publisher stays caught up | relay intake |

## 8. Work items and repeatable checks

WI-01, WI-02, WI-04, and WI-05 describe the implemented path. WI-03 and WI-06
remain the repeatable procedure for changing defaults or comparing relay
conditions.

### WI-01: Separate range length from receive-queue length

Requirements:

- Replace `HeaderBatchSize` with `BlockFetchRangeBlocks`.
- Remove the false `range <= blockfetch.MaxRecvQueueSize` rule.
- Keep protocol receive queue validation in `1..512`.
- Add an application range bound in `1..8192`.
- Keep the default at 512 unless an actual-data comparison supports a change.

Verification:

- Configuration accepts a 4,096-block range with a 512-message receive queue.
- Configuration still rejects a receive queue above 512.
- One actual relay returns a complete range larger than 512 with exact callback
  count and order.

### WI-02: Add phase timing before changing control flow

Requirements:

- Time header collection, the `GetBlockRange` call, body streaming, inter-range
  idle, and downstream callback blocking.
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

- Use the internal ChainSync driver with at most 100 outstanding
  `RequestNext` messages and one request per write.
- Refill one request after each completed callback.
- ChainSync callbacks enqueue ordered header and rollback events only.
- One range builder owns batching and rollback barriers.
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

An initial optimized run using the two configured relays and ClickHouse
sustained about 545-585 agreed and published blocks/second, compared with the
earlier roughly 100 blocks/second. It recorded no reconnects or mismatches. A
packet capture showed a 20-request ChainSync refill write about every 29-31 ms.
This result demonstrates the scheduling improvement on that run; it does not
replace the fixed-span procedure when changing range defaults.

The fixed-span range trial retained the 512-block default. Repeated 512 and
2,048 runs over the same 32,768 blocks reversed relative order as public-relay
conditions moved (425.48 then 519.90 blocks/second at 512; 481.07 then 410.30
at 2,048). The decisive phase metric was overlap: 512 sometimes prepared the
next range and kept the limiting BlockFetch worker near 91% duty, while 2,048
never prepared the next range and one relay fell near 65% duty. An 8,000-block
actual-relay canary fell near 184-208 blocks/second and caused a keepalive
reconnect after exhausting its header backlog. Larger ranges are supported,
but are not a throughput optimization on the measured single connection.

A second 8,000-block canary waited for two complete ranges (16,000 headers)
before starting BlockFetch. The preloaded second range was ready immediately,
but the following nine transitions all waited for headers. Its same-span rate
was 378.10 blocks/second; over 80,000 blocks it sustained 398.31 including
prefill and 406.20 after the reserve drained. The slower relay spent only 54.2%
of wall time fetching and averaged 7.19 seconds idle between ranges. Initial
backlog changes burst timing, not the steady-state header/body flow rate.

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

The 416.3 blocks/second no-gap figure above was a diagnostic projection from
the baseline span, not a fixed ceiling or release requirement. The optimized
two-relay run sustained about 545-585 agreed and published blocks/second with
no reconnects or mismatches; different block spans and public-relay conditions
make the two figures directional rather than a controlled A/B result.

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
