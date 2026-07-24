# Implementation work items

Every work item includes its production requirements, required tests, and exit
criteria. Work may be delegated only at these package boundaries or a smaller
bounded subset.

Implementation status (2026-07-24): WI-01 through WI-10 are implemented and
verified. Unit, race, vet, fixture, ClickHouse integration, container,
query-audit, generated-fact publication, and a live strict two-relay replay of
100,000 consecutive real Conway blocks are complete.

## WI-01: Repository, module, configuration, and commands

Dependencies: none

Requirements:

- Create one Go module named `cardano-clicksync`.
- Pin a tested gOuroboros release and ClickHouse Go driver.
- Support `clicksync migrate`, `clicksync sync`, and `clicksync status`.
- Parse ClickHouse, network magic, relay list, start point, queue/batch,
  timeout, and worker settings from environment.
- Require at least two distinct relay endpoints.
- Use strict all-configured-relays agreement; expose no threshold setting.
- Acquire a process-lifetime single-host `flock` for `sync`.
- Handle SIGINT/SIGTERM with one bounded shutdown deadline.
- Keep credentials out of logs and status.

Tests:

- valid/default configuration;
- malformed endpoints, duplicate endpoints, fewer than two relays;
- malformed start points and invalid numeric/duration bounds;
- unknown command and command-specific configuration isolation;
- two-process lock exclusion and release after process death;
- cancellation and shutdown deadline.

Exit:

- the binary builds;
- help/usage describes only implemented commands;
- no legacy package or configuration name is present.

## WI-02: Schema and store foundation

Dependencies: WI-01

Requirements:

- Implement the schema in [schema.md](schema.md).
- Embed deterministic migration SQL and schema hash.
- Open ClickHouse with native protocol, LZ4, sufficient pooled connections,
  `async_insert=0`, and acknowledged inserts.
- Initialize or verify the immutable dataset row.
- Compute publication/event high-water marks across raw tables.
- Derive committed snapshot and current tip without a manifest.
- Load a bounded canonical rollback window and intersection candidates.

Tests:

- migration statement splitting and idempotence;
- schema contract/golden descriptor;
- fresh initialization and exact restart;
- conflicting dataset identity rejection;
- high-water marks include orphan fact/event rows;
- latest adoption and latest rollback tip derivation;
- active canonical window after rollback and re-adoption;
- ClickHouse integration test under an explicit environment/build tag.

Exit:

- a fresh ClickHouse database initializes;
- restart needs no mutable manifest;
- ordinary publication APIs require no state reads.

## WI-03: Fact model and non-validating normalizer

Dependencies: WI-01; schema types from WI-02

Requirements:

- Port the useful SQL-facing types from legacy `internal/model/facts.go`.
- Port era parsing from legacy `internal/normalize/bundle.go`.
- Decode only after relay agreement with body-hash validation skipped.
- Preserve effective phase-2 flow projection and transaction context facts.
- Remove transaction-ID revalidation, duplicate/state rules, source
  resolution, and deep publication validation.
- Treat optional address/script enrichment as nullable where raw facts remain
  representable.
- Never persist full block/transaction/witness/script CBOR.
- Retain exact datum, redeemer-data, and metadata CBOR.

Tests:

- real Byron EBB/main, Shelley, Allegra, Mary, Alonzo, Babbage, Conway, and
  any supported newer-era fixtures;
- valid and phase-2-invalid collateral flow;
- assets and mint/burn;
- regular/collateral/reference inputs;
- collateral return;
- inline/hash/witness datums;
- withdrawals;
- all supported redeemer purposes;
- metadata maps and binary values;
- malformed required CBOR and numeric overflow fail before publication;
- formerly rejected duplicate or unresolved source references normalize as
  observations;
- fixture assertion that no forbidden raw CBOR field exists in the model.

Benchmarks:

- decode plus normalize representative blocks in parallel;
- allocation profile for large Conway blocks.

Exit:

- fixture coverage passes;
- no normalizer call performs UTxO/database access;
- no second semantic validation pass exists.

## WI-04: Raw gOuroboros relay session

Dependencies: WI-01 and shared event types from WI-03

Requirements:

- One session owns one gOuroboros connection.
- Use ChainSync for ordered points and BlockFetch for raw ranges.
- Use `BlockRawFunc`; do not decode in the relay session.
- Disable gOuroboros block-body validation.
- Accumulate configurable header ranges and associate raw callbacks by order.
- Hash exact raw CBOR with the documented domain.
- Retain raw bytes only for relay index zero.
- Emit bounded forward/rollback events with actual negotiated peer metadata.
- Cancellation must unblock protocol callbacks and queue sends.

Tests:

- intersection accepted/not found;
- ordered single and multi-block ranges;
- range end at tip;
- raw callback digest and source retention policy;
- missing, extra, or reordered protocol callbacks fail the session;
- rollback event ordering;
- disconnect, timeout, DNS/connection error classification;
- backpressure and cancellation without goroutine leaks;
- opt-in live handshake/range test.

Exit:

- two independent sessions can stream the same fixture without decoding;
- race tests pass.

## WI-05: Strict relay agreement

Dependencies: WI-04

Requirements:

- Consume exactly one next event from every configured session.
- Require identical event kind.
- Require identical point, block type, raw length, and content digest for a
  forward event.
- Require identical target point for rollback.
- Preserve configured-order relay attribution.
- Forward only relay-zero raw bytes after all comparisons pass.
- Return a typed difference containing bounded per-relay diagnostics.
- Never vote, quarantine, skip, or publish a partial agreement.

Tests:

- 2-of-2 and 3-of-3 identical forward events;
- digest, type, point, length, and event-kind disagreement;
- identical unanimous rollback;
- different rollback targets;
- one closed/error channel;
- fast relay bounded behind a slow relay;
- raw bytes are absent from diagnostics and non-source events;
- no output is emitted before the last required event arrives.

Exit:

- property/fuzz tests cannot produce an agreed output from non-identical
  inputs;
- mismatch causes no decode or store call.

## WI-06: Ordered parallel processing and batching

Dependencies: WI-03 and WI-05

Requirements:

- Assign ordered sequence numbers to agreed events.
- Decode/normalize roll-forwards with a bounded worker pool.
- Reorder results before the batch builder.
- Treat rollback as a barrier after every preceding job.
- Bound both items and retained raw bytes.
- Build contiguous microbatches by block, byte, row, and age limits.
- Flush on limits, rollback, shutdown, and live-tip age.
- Report current/high-water agreed-queue occupancy and average normalization
  duration through the lightweight runtime snapshot; use focused benchmarks
  for deeper reorder and worker-utilization detail.

Tests:

- intentionally out-of-order workers still produce ordered blocks;
- worker failure cancels later publication;
- byte and item bounds backpressure;
- current/high-water queue and average-normalization metric accounting;
- each flush reason and exact boundary;
- timer races and shutdown;
- rollback waits for preceding work and prevents following work overtaking;
- no goroutine/timer leak under repeated cancellation;
- race suite.

Exit:

- a fixture replay keeps stable order under randomized worker delay;
- maximum retained memory follows configured bounds.

## WI-07: High-throughput ClickHouse publication

Dependencies: WI-02, WI-03, and WI-06

Requirements:

- Allocate one publication ID per block and ordered adoption events.
- Convert each microbatch into at most one native batch per populated table.
- Insert independent fact tables concurrently.
- Commit visibility with one final adoption insert.
- Perform no successful fact readback.
- Perform no datum existence lookup.
- On adoption error, read back only the exact expected event range.
- Do not reuse IDs after any failed attempt.
- Assert writer lock before fact work and immediately before adoption.

Tests:

- every table mapping, nullable value, binary hash, and array;
- one physical send per populated table for a multi-block batch;
- concurrent fact insert scheduling;
- every individual table failure prevents adoption;
- orphan fact rows remain invisible;
- exact complete/absent/partial/conflicting adoption readback;
- lock loss prevents adoption;
- retry after failure uses new identifiers;
- integration test with ClickHouse query log proving zero ordinary
  roll-forward `SELECT` queries.

Benchmarks:

- publisher with representative normalized blocks;
- rows/second, blocks/second, parts created, insert calls, and p50/p95 batch
  duration.

Exit:

- the writer satisfies the publication crash table in `schema.md`;
- publisher capacity meets `performance.md` or produces phase evidence for a
  documented blocker.

## WI-08: Rollback and recovery

Dependencies: WI-02, WI-05, WI-06, and WI-07

Requirements:

- Maintain a bounded canonical `(point, publication_id, event_seq)` window.
- Truncate pending uncommitted descendants without database writes.
- Resolve committed descendants only from the canonical chain.
- Insert invalidations first and one rollback header last.
- Make headerless invalidations inert.
- Resolve an ambiguous rollback header by exact readback.
- Reject targets below the dataset start or beyond maximum depth.
- Support later re-adoption using new publication IDs.
- Restart correctly after every rollback crash cut.

Tests:

- rollback wholly inside pending batch;
- rollback to durable tip;
- rollback of one and maximum-depth descendants;
- target not canonical, below start, and too deep;
- failure before/during/after invalidations and before/after header;
- exact uncertain header readback;
- restart with orphan invalidations;
- rollback then identical block re-adoption;
- current membership and tip after each case.

Exit:

- all rollback crash cuts converge after restart;
- no rollback path depends on a manifest or trust evidence state machine.

## WI-09: Runtime supervision and observability

Dependencies: WI-04 through WI-08

Requirements:

- Open all required sessions from one durable candidate set.
- Restart the whole set after transport failure or disagreement.
- If startup intersections differ, retry the whole set with the canonical
  suffix beginning at the oldest selected candidate.
- Before streaming from a unanimous older intersection, commit a rollback to
  that point so the former committed suffix is invalidated before any
  descendant can be re-adopted.
- Use capped exponential backoff reset after useful progress.
- Discard all uncommitted in-memory data before restart.
- Never rotate only one relay into a primary role.
- Emit periodic concise JSON progress with lifetime agreement/publication
  rates, attempt/reconnect/commit/row counters, agreement calls/mismatches,
  normalized blocks, average agreement/normalize/publish durations, and
  current/high-water agreed-queue levels.
- Do not add per-relay throughput/timing or reorder/worker-utilization metrics
  to the runtime snapshot.
- Treat a shutdown deadline as process-terminal if clean completion cannot be
  proven; do not close the store or release the writer lock before process
  exit in that case.
- Implement `status` from immutable dataset identity and committed events.

Tests:

- startup, progress, disconnect, mismatch, and successful restart;
- progress counter, row, mismatch, stage-average, and queue high-water
  accounting;
- differing intersection selections converge through the oldest selected
  canonical suffix;
- unanimous selection of an older point commits rollback before streaming;
- no publication from a failed attempt after restart begins;
- context cancellation during connection, agreement, decoding, publication,
  and backoff;
- shutdown deadline bounds an operation that ignores cancellation;
- log redaction;
- status on empty, adopted, rolled-back, and re-adopted datasets.

Exit:

- a deterministic fake-relay end-to-end run covers sync, restart, rollback,
  and re-adoption;
- shutdown is clean under the race detector.

## WI-10: Integration, performance, and final audit

Dependencies: all prior work

Requirements:

- Run formatting, vet, unit, race, fixture, and ClickHouse integration suites.
- Build a container and minimal Compose setup without unrelated services.
- Run the performance protocol in [performance.md](performance.md).
- Compare implementation against every requirement and non-goal.
- Record supported eras, trust limitations, and measured throughput honestly.
- Keep the legacy repository untouched.

Tests/evidence:

- `go test ./...`, `go test -race ./...`, and `go vet ./...`: complete;
- deterministic schema hash: complete;
- container build and offline command smoke test: complete;
- 100,352-block generated-fact ClickHouse publication replay: complete;
- ClickHouse query-log audit: complete;
- restart and rollback fault matrix: complete in unit/integration coverage;
- 100,000-consecutive-block real-CBOR end-to-end replay: complete from two
  public mainnet relays;
- live strict two-relay run: complete, with exact range/identifier continuity
  and measured stage timings.

Exit:

- no critical/high correctness or data-visibility finding remains;
- final documentation matches code and measured behavior.
