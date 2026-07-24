# Clicksync architecture

## 1. Overview

Clicksync is one Go process with five bounded stages:

```text
N raw relay sessions
        |
        v
strict ordered agreement
        |
        v
parallel decode + normalize
        |
        v
ordered microbatch builder
        |
        v
parallel ClickHouse fact inserts
        |
        v
single adoption commit
```

Rollback events travel through the same ordered stream. They act as barriers:
all earlier agreed blocks are resolved first, pending uncommitted descendants
are discarded, and committed descendants are invalidated before later blocks
can proceed.

## 2. Packages

The intended package boundaries are:

| Package | Responsibility |
|---|---|
| `internal/config` | Parse immutable environment configuration and reject unusable process settings. |
| `internal/model` | SQL-facing facts, points, relay identity, agreed events, and publication batches. |
| `internal/relay` | One gOuroboros ChainSync + raw BlockFetch session per relay. No cross-peer policy. |
| `internal/agreement` | Align one next event from every relay and compare it. |
| `internal/normalize` | Decode one agreed raw block and project SQL facts. |
| `internal/pipeline` | Concurrent normalization, ordered delivery, bounded queues, and rollback barriers. |
| `internal/store` | Migration, initialization, event recovery, fact inserts, adoption, and rollback. |
| `internal/syncer` | Restart loop and composition of relay, agreement, pipeline, and store. |
| `internal/writerlock` | Process-lifetime single-host advisory lock. |
| `cmd/clicksync` | `migrate`, `sync`, and `status` commands. |

Packages communicate through small interfaces defined by their consumers.
There is no generic engine, plugin layer, trust controller, evidence service,
or multi-stage authority abstraction.

## 3. Relay session

Each configured relay gets an equivalent persistent node-to-node connection.
There is no primary relay in the agreement protocol. Relay index zero is only
the retained-body source after equality is known.

A relay session:

1. negotiates node-to-node with the configured network magic;
2. finds one of the supplied durable intersection candidates;
3. starts ChainSync from that exact point;
4. accumulates up to `header_batch_size` roll-forward headers;
5. requests the matching BlockFetch range;
6. uses gOuroboros's raw block callback;
7. computes a content digest without decoding or validating the block;
8. emits ordered `Forward` or `Rollback` events to its bounded channel.

The raw BlockFetch callback is intentional. It avoids decoding every block N
times and avoids gOuroboros block-body validation. Only relay zero retains a
copy of the raw bytes after hashing; other relays discard bytes immediately.

The session still associates each raw callback with the corresponding
ChainSync header position so agreement compares:

```text
event kind
slot
header hash
block type
SHA-256(domain || block type || exact raw block CBOR)
```

This association is protocol bookkeeping, not a claim that the header or body
is valid.

## 4. Strict agreement

Configuration requires at least two relays with distinct normalized
endpoints. Every configured relay is required.

For each stream ordinal, the agreement stage reads one event from each relay:

### Roll forward

Agreement succeeds only when every event is roll-forward and all compared
fields are byte-identical. The agreed event retains:

- the point;
- block type;
- content digest;
- raw CBOR from relay zero;
- ordered configured relay identities and actual remote addresses;
- observation time.

### Rollback

Agreement succeeds only when every event is rollback and every target point
is identical. The target must be the configured start point, a pending point,
or a known committed point in the canonical rollback window.

### Difference, disconnect, lag, or intersection mismatch

No attempt is made to decide which relay is correct. For an event difference,
disconnect, or lag, the current relay set is cancelled, uncommitted events are
discarded, bounded diagnostics are logged, and the complete set is retried
after capped exponential backoff.

An initial intersection mismatch is handled before event agreement begins.
The candidate list is ordered newest to oldest. If the relays select different
canonical candidates, the runner retries every relay with the suffix beginning
at the oldest selected candidate. This restriction belongs only to the current
durable snapshot; a committed snapshot change discards it.

Once every relay selects the same point, an older selection is not treated as
permission to replay the already committed suffix. The runner first commits a
rollback from the durable tip to that unanimous point. Forward streaming starts
only after that rollback succeeds. Any identical descendants received
afterward are explicit re-adoptions of already invalidated publications, never
duplicate active publications layered over the old committed suffix.

A slow relay naturally backpressures its own channel and then the other
sessions through the bounded agreement window. This is expected: strict
all-of-N makes the slowest required relay part of the network boundary.

## 5. Hash definition

The per-block agreement digest is:

```text
SHA-256(
  "cardano-clicksync/raw-block/v1" ||
  uint64_be(block_type) ||
  uint64_be(raw_length) ||
  raw_block_cbor
)
```

The point and event kind are compared separately. Length is included to make
the framing unambiguous. A segment digest used for logs or provenance is the
SHA-256 of the ordered per-block digest sequence with the same style of domain
separation.

Raw equality would also work; a fixed digest avoids retaining N copies and
makes comparison cost predictable. Collision resistance is an explicit trust
assumption.

## 6. Decode and normalization

Only an agreed retained copy is decoded:

```go
ledger.NewBlockFromCbor(
    blockType,
    raw,
    common.VerifyConfig{SkipBodyHashValidation: true},
)
```

Normalization workers run concurrently. Each job carries a monotonically
increasing in-memory sequence. Results are reordered before publication, so
ClickHouse always sees chain order.

Normalization derives values needed by the schema:

- block and transaction identifiers;
- phase-2-valid flow selection;
- effective regular/collateral inputs and outputs;
- assets and mint deltas;
- datum bodies and observations;
- withdrawals;
- redeemer data and transaction-local targets;
- metadata.

Deriving an identifier that is not explicitly stored on the wire is required
parsing. It must not be followed by a redundant comparison pass.

Optional enrichment failures become `NULL`/`none` when raw facts remain
unambiguous. Failure to decode a required structure or represent a required
numeric value stops before adoption and is reported as unsupported data.

## 7. Bounded pipeline

Every queue has both an item bound and, where raw bytes are retained, a byte
bound.

Initial defaults:

| Boundary | Default |
|---|---:|
| gOuroboros BlockFetch receive queue | 512 messages |
| headers per BlockFetch range | 512 initially; this is not a protocol maximum |
| per-relay emitted events | 256 |
| agreed raw blocks | 256 blocks / 256 MiB |
| concurrent normalizers | `GOMAXPROCS` |
| normalization reorder window | 256 blocks / 256 MiB |
| pending publication | 1,024 blocks / 128 MiB / 2,000,000 fact rows / 1 second |

Configuration may lower these values. Raising hard limits requires benchmark
and memory evidence.

The approved single-connection throughput refactor separates receive-queue
capacity from BlockFetch range length and overlaps ChainSync header prefetch
with sequential BlockFetch on the same N2N connection. Multiple connections
per relay are explicitly out of scope. See
[Single-connection BlockFetch throughput plan](single-connection-blockfetch-plan.md).

Backpressure is end-to-end. When ClickHouse is slower, publication fills,
then normalization fills, then agreement fills, then BlockFetch stops reading.
No stage drops an agreed roll-forward silently.

## 8. Publication

A publication microbatch contains a contiguous ordered block prefix.

For each block the writer assigns a fresh `publication_id`. It reserves a
contiguous adoption event range above every raw identifier seen at startup.
Identifiers from failed attempts are never reused.

Publication order:

1. Assert the writer lock.
2. Prepare table-oriented native batches.
3. Insert each populated fact table concurrently.
4. Wait for every fact insert to succeed.
5. Assert the writer lock again.
6. Insert one adoption batch into `chain_events`.
7. Acknowledge the microbatch to the pipeline.

Facts without adoption are inert. There is no successful fact readback and no
manifest update.

The ClickHouse connection explicitly uses synchronous insert behavior:

```text
async_insert = 0
wait_for_async_insert = 1
```

If the adoption insert returns an error, the writer performs one exact
readback for the reserved event range:

- exact complete rows mean committed;
- no rows mean not committed;
- partial or conflicting rows are fatal and require operator inspection;
- a readback failure makes commit state indeterminate and stops the process.

## 9. Dataset initialization and restart

`dataset` is immutable and written once. It records schema identity, network,
and configured start point. Startup requires exactly one matching logical row
after initialization.

Current state is reconstructed from committed authority:

- the largest committed adoption or rollback event is the durable snapshot;
- its resulting point is the durable tip;
- recent active publications build a bounded canonical rollback window;
- allocation high-water marks include orphan facts and uncommitted event
  remnants, so identifiers are never reused.

Ordinary restart reads occur before relay sessions start. No state query is
performed per roll-forward batch.

Intersection candidates are the durable tip, geometrically spaced recent
canonical ancestors, and the configured start point, ordered newest to oldest.
Every relay session must accept one candidate. If sessions select different
candidates, the complete group is restarted with the canonical suffix that
begins at the oldest selected candidate. The suffix remains forced only while
the durable snapshot is unchanged.

When all sessions later select that same older point, the runner commits a
rollback to it before constructing the agreement and forward pipeline. The
rollback invalidates the former committed descendants using the normal
header-last store protocol. The relay streams therefore resume from the new
durable tip; any repeated descendant is a valid re-adoption after invalidation,
not a second active copy of a still-committed suffix.

## 10. Rollback

Rollback behavior preserves the sound legacy visibility pattern. The
pre-stream intersection reconciliation described above uses the same store
operation and must commit before any new forward event is accepted.

When unanimous rollback reaches the ordered pipeline:

1. Wait for all earlier normalization jobs.
2. Stop the publication age timer.
3. Remove pending uncommitted descendants after the target.
4. If the target is now the durable tip, emit no database event.
5. Otherwise resolve committed descendants from the in-memory canonical
   window, falling back to a bounded startup-style database query.
6. Reserve one rollback event and ID.
7. Insert one invalidation membership row per descendant into `chain_events`.
8. Insert one `rollbacks` header last.
9. Treat invalidations as effective only when that exact header exists.
10. Truncate the canonical window and resume later roll-forwards.

An error after invalidations but before the header leaves inert rows. Restart
allocates above the abandoned event and can retry with a new event. An
ambiguous header insert is resolved through exact readback.

Rollback depth is bounded (default 2,160 committed blocks). A target below the
configured dataset start or outside the recoverable canonical chain stops the
process.

## 11. Shutdown

On SIGINT or SIGTERM, a normal graceful shutdown:

1. stops accepting new relay events;
2. cancels relay sessions;
3. drains already agreed normalization jobs;
4. flushes the final complete publication batch;
5. closes ClickHouse;
6. releases the writer lock.

A single configurable shutdown deadline covers draining and final commit.
Cancellation during an uncertain adoption or rollback commit still performs
the bounded commit readback when it can finish within that deadline.

Go cannot forcibly terminate a goroutine or library operation that ignores
context cancellation. `ErrShutdownTimeout` therefore means clean completion
could not be proven, not that every in-process operation was killed. It is a
process-terminal result: the CLI deliberately does not close ClickHouse or
release the advisory lock underneath potentially abandoned work, logs the
error, and immediately calls `os.Exit`. The operating system then closes those
process resources.

## 12. Observability

Structured logs and an in-process metrics snapshot deliberately expose a small
operational set:

- lifetime attempts and whole-set reconnects;
- lifetime agreed blocks and raw bytes;
- lifetime committed blocks, publication batches/rows, and rollbacks;
- agreement call/mismatch and normalized-block counts;
- current and high-water agreed-queue items and retained bytes;
- lifetime-average agreed and published blocks/second;
- average agreement wait, normalization time, and publication-batch time.

These values show whether intake is below publication, whether the agreed
queue is accumulating, where agreement/normalization/publication is spending
time, and whether commits are progressing. They identify the broad bottleneck without
building per-relay throughput/timing or reorder/worker-utilization
instrumentation. Focused benchmarks, Go profiles, and ClickHouse query logs
provide deeper decode or insert detail when needed.

Logs never include raw block CBOR or database credentials.

## 13. Trust and failure statement

This architecture detects a relay returning bytes different from another
configured relay at the same ordered position. It does not detect:

- all relays agreeing on invalid data;
- colluding relays;
- relays sharing the same faulty upstream;
- a cryptographic hash collision;
- semantic parser bugs shared by all workers;
- ClickHouse corruption after successful acknowledgement.

Those limitations are part of the product contract, not hidden exceptional
cases.
