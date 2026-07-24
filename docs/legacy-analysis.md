# Legacy Clicksync analysis

Reviewed repository: `/home/sho/io/clicksync`  
Checked-in baseline: `2572ccb`  
Review date: 2026-07-24

The legacy working tree contained extensive uncommitted validation-removal and
header-certificate experiments. Those files were inspected for lessons but
are not treated as a stable source baseline. This rewrite reads the legacy
repository only; it does not modify or clean that worktree.

## 1. Scale and concentration

The checked-in repository contains about 74,000 lines of Go. The largest
production control-plane files are:

| Area | Representative files | Problem |
|---|---|---|
| Corroboration | `internal/syncer/supervisor.go` | About 2,300 lines plus about 3,200 lines of tests for checkpoints, evidence, quarantine, trust transitions, and suffix rules. |
| Manifest | `internal/store/manifest.go`, `manifest_record.go`, `trust.go`, `rollback_manifest.go` | Thousands of lines serializing and validating a mutable logical state machine in append-only ClickHouse rows. |
| Publication | `internal/publication/publication.go`, `internal/store/publication.go`, `verification.go` | UTxO lookups, duplicate/state checks, content digests, successful-insert readback, and crash-stage orchestration. |
| Query authority | `clickout/internal/clickhouse/authority_*` | A second large implementation was needed to interpret and revalidate the manifest/evidence protocol. |

The actual fact model is small: `internal/model/facts.go` is about 170 lines.
The era-aware normalizer is sizeable because Cardano eras are sizeable, but
its job is direct and cohesive.

## 2. Legacy data flow

The checked-in path is:

```text
one selected primary relay
  -> ChainSync header ranges
  -> BlockFetch and structural checks
  -> normalize
  -> stage microbatch
  -> historical UTxO/output reads
  -> deep publication validation
  -> nine sequential fact inserts
  -> full successful-batch fact readback
  -> adoption insert
  -> full dataset_manifest append

secondary relays
  -> startup/periodic/rollback singleton probes
  -> peer observation rows
  -> trust evidence state machine
  -> physical/effective/servable manifest heads
```

That design does not compare every collected body. It validates a primary
body stream and periodically corroborates points. This explains why removing
validation from the old design led to increasingly elaborate header
certificates: primary-only bodies still needed a separate content-binding
argument.

The clean-slate design removes that premise. Every relay supplies every raw
block, so exact body agreement is the content-binding mechanism.

## 3. Demonstrated throughput costs

The legacy `docs/sync-speed-verdict.md` captured an Alonzo-era run at roughly
21 blocks/second and identified publication as the first demonstrated
bottleneck. A later optimized version is reported by the project owner at
roughly 100 blocks/second.

The measured legacy costs were:

- one large `dataset_manifest` insert per physical batch;
- nine fact tables inserted sequentially;
- ClickHouse asynchronous insert enabled while the client also waited after
  every insert, producing no server-side coalescing;
- successful fact rows read back for counts and full content digests;
- historical output and active-consumption lookups for every batch;
- increasingly broad sparse-primary-index reads as history grew;
- a small decoded-event queue propagating database stalls into BlockFetch.

The replacement removes all five hot-path read families:

1. source output resolution;
2. active consumption resolution;
3. existing datum-body lookup;
4. successful fact count/content verification;
5. manifest head transition/reconciliation reads.

## 4. Keep, simplify, or delete

| Legacy area | Decision | Rationale |
|---|---|---|
| `internal/model/facts.go` | Port and minimally change | Go types align well with ClickHouse facts. Remove claims that require local resolution; add relay content identity. |
| `internal/normalize/bundle.go` | Port, then simplify | Era parsing, effective phase-2 flow projection, assets, datums, redeemers, and metadata are valuable. Delete ledger-validity rejection and redundant hash verification. |
| Real block fixtures | Reuse | They cover Byron through current supported eras and are licensed for this purpose. |
| gOuroboros direct N2N setup | Reuse concepts and small helpers | Handshake, ChainSync, BlockFetch, cancellation, and raw range ordering are proven. Rewrite around raw callbacks and multiple symmetric sessions. |
| `internal/syncer/supervisor.go` | Delete | Checkpoint probes, primary/secondary roles, quarantine, trust finalization, evidence attempts, and suffix policy disappear. |
| `internal/model/observation.go` | Delete | Canonical observation IDs/digests exist only for the removed evidence state machine. |
| Deep publication validator | Delete | The normalized model is written directly. Only schema representability remains at append boundaries. |
| UTxO resolver | Delete | It is state validation and the largest scaling read path. |
| Successful fact readback | Delete | A successful synchronous insert is the persistence acknowledgement. |
| Fact-before-adoption ordering | Keep | It makes partial fact attempts invisible without transactions across tables. |
| Adoption uncertainty readback | Keep | A network error can make the final commit result ambiguous. |
| Append-only rollback membership plus final header | Keep and simplify | The semantics are sound, rare, and outside the ordinary hot path. |
| `dataset_manifest` | Delete | Dataset identity is immutable; current tip and membership derive from committed events. |
| Writer `flock` | Keep | ClickHouse does not provide a cross-table transactional single-writer fence. |
| Writer audit heartbeat | Delete | The OS lock is authority; heartbeat rows add a second, non-authoritative state model. |
| Clickout module | Out of scope | This repository produces the schema contract; query tooling can consume it independently. |

## 5. Validation removed

The new collector does not perform:

- block-body hash validation;
- header signature, KES, VRF, slot-leader, or chain-selection validation;
- transaction-ID comparison against separately rehashed body CBOR;
- duplicate input, overlapping role, double-spend, or already-spent checks;
- source-output existence or active-state checks;
- value conservation, fee, collateral, mint, withdrawal, or script checks;
- address-network rejection when the raw address can still be stored;
- fact bundle semantic revalidation after normalization;
- successful ClickHouse fact count or content readback.

## 6. Work that necessarily remains

Some checks are required to operate software safely and are not claims about
Cardano validity:

- gOuroboros must decode protocol framing and ChainSync headers.
- Configured network magic and relay endpoints must be usable.
- All configured relays must provide the same event kind and exact content
  digest before progress.
- Raw CBOR must decode into a supported gOuroboros era type.
- Required values must fit their ClickHouse types.
- Parallel SQL arrays must have matching lengths.
- Required enum values must have a schema representation.
- A block is all-or-nothing; unsupported required semantics stop before any
  adoption for that block.
- Publication must not violate its own batch, ordering, or writer-lock
  invariants.

These are transport, parsing, and representation boundaries—not independent
ledger validation.

## 7. Key conclusions

1. The old manifest is not needed for correctness. Immutable dataset identity
   belongs in one initialization row; current state belongs in committed
   chain events.
2. The old corroboration complexity follows from asymmetric body sourcing.
   Symmetric full-body collection makes corroboration one ordered hash
   comparison.
3. The old append-only fact/adoption and rollback visibility ideas are worth
   retaining.
4. The biggest throughput wins come from deleting reads, eliminating the
   per-batch manifest write, using larger batches, and overlapping independent
   fact-table inserts.
5. The new trust statement is weaker than validation and stronger than a
   single relay: unanimous byte agreement is evidence of common observation,
   not proof of ledger truth.

