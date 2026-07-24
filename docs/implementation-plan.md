# Cardano Clicksync implementation plan

Status: implementation, local verification, and live two-relay replay complete  
Legacy baseline reviewed: `/home/sho/io/clicksync` at `2572ccb`  
Target repository: `/home/sho/io/cardano-clicksync`

## 1. Purpose

Cardano Clicksync is a fast, non-validating Cardano fact collector. It follows
the same chain from multiple independently operated relays, requires every
configured relay to return the same ordered raw blocks, decodes one agreed
copy, and writes compact facts to ClickHouse.

The service does not decide whether a Cardano block or transaction is valid.
Its trust claim is deliberately narrow:

> Every configured relay supplied the same raw block bytes in the same
> ChainSync position.

This rewrite keeps the useful parts of the legacy system—the fact model,
era-aware gOuroboros parsing, append-only visibility boundary, and rollback
membership semantics—while removing the control plane that tried to turn
Clicksync into a partial validator.

## 2. Primary requirements

1. Use Go and `github.com/blinklabs-io/gouroboros`.
2. Connect directly to at least two configured Cardano node-to-node relays.
3. Download blocks from all configured relays concurrently.
4. Compare every ordered relay event before making it publishable.
5. For roll-forward events, compare the ChainSync point, block type, and a
   SHA-256 digest of the exact raw block CBOR.
6. Decode and normalize only one retained copy after agreement.
7. Perform no consensus, ledger-state, UTxO-state, script, witness, signature,
   fee, value-conservation, or duplicate-spend validation.
8. Insert normalized facts in large table-oriented ClickHouse batches.
9. Keep facts invisible until a final adoption insert succeeds.
10. Preserve append-only rollback and re-adoption behavior.
11. Keep ordinary roll-forward free of ClickHouse reads.
12. Bound all network, decode, reorder, and publication queues.
13. Expose lightweight agreement/publication rates, commit/retry counters, and
    current agreed-queue occupancy. Use focused benchmarks and profiles—not a
    permanent per-stage metrics framework—to separate decode from ClickHouse
    when downstream work is limiting.

## 3. Explicit non-goals

- Choosing the correct Cardano fork.
- Tolerating a minority relay through voting or quorum policy.
- Quarantining, scoring, or automatically replacing a divergent relay.
- Maintaining local UTxO or ledger state.
- Rechecking block-body hashes already covered by raw-byte relay agreement.
- Rechecking transaction IDs against transaction-body CBOR.
- Reading inserted fact rows back after successful ClickHouse inserts.
- Maintaining a mutable dataset manifest or evidence state machine.
- Providing Clickout/query APIs in this repository.
- Preserving source compatibility with legacy internal Go packages.

The first version uses strict all-of-N agreement. Operators who want 2-of-3
behavior should run exactly the two relays they are prepared to trust until a
separate, justified design replaces this rule.

## 4. Design documents

- [Legacy analysis](legacy-analysis.md) records what was inspected and the
  keep/rewrite/delete decisions.
- [Architecture](architecture.md) defines processes, data flow, agreement,
  backpressure, restart, and rollback behavior.
- [Schema and publication contract](schema.md) defines ClickHouse authority
  and the replacement for `dataset_manifest`.
- [Work items](work-items.md) defines implementation requirements, tests,
  dependencies, and completion criteria for each unit of work.
- [Performance plan](performance.md) defines measurement, budgets, and
  throughput acceptance.

These documents are the implementation contract. Material deviations require
updating the relevant document and its affected tests in the same change.

## 5. System boundary

Inputs:

- node-to-node ChainSync and BlockFetch from configured relays;
- immutable environment configuration;
- an optional existing ClickHouse dataset created by this implementation.

Outputs:

- normalized block, transaction, input, output, datum, withdrawal, redeemer,
  and metadata facts;
- append-only adoption and rollback membership events;
- concise relay-agreement provenance;
- structured operational logs and status output.

No raw block, transaction, witness, auxiliary-data, or script CBOR is retained.
The only stored CBOR remains datum bodies, redeemer data, and transaction
metadata required by the fact contract.

## 6. Delivery order

1. Establish the module, configuration, schema, and model contracts.
2. Port and simplify normalization with real-era fixture coverage.
3. Implement one raw gOuroboros relay session.
4. Implement strict multi-relay event agreement.
5. Implement ordered concurrent decode/normalize.
6. Implement table-oriented ClickHouse publication and recovery.
7. Implement rollback, restart, and runtime wiring.
8. Run unit, fixture, integration, race, failure, and performance gates.
9. Reconcile code and documentation, then report measured limitations.

Subagents may implement bounded work only after this document set exists.
No more than ten subagents may be created for the whole rewrite.

## 7. Implementation and verification status

As of 2026-07-24, the clean-slate implementation and its local verification
are complete:

- all commands, bounded relay agreement, normalizers, ordered batching,
  append-only publication, rollback/recovery, restart supervision, and
  process-lifetime writer fencing are implemented;
- unit, fixture, vet, race, tagged ClickHouse integration, container-build,
  and Compose-configuration checks pass;
- real fixtures cover Byron main/EBB through Dijkstra;
- a 100,352-block generated-fact publication benchmark measured 87,402
  blocks/second and 786,614 fact rows/second against local ClickHouse;
- the successful publication query-log audit observed zero `SELECT` queries;
- a fresh strict two-relay mainnet dataset committed 100,000 consecutive real
  Conway blocks and 12,552,027 fact rows with exact identifier/range
  continuity;
- an uninterrupted 81,408-block portion measured 104.69 committed
  blocks/second, with zero mismatches/reconnects and a queue that drained
  between relay bursts.

The 87,402-block/second benchmark remains deliberately publication-only. The
104.69-block/second result is end-to-end but depends on public-relay and
network behavior, so it is not presented as a deterministic local benchmark.
Both scopes are recorded in [performance-results.md](performance-results.md).

## 8. Completion contract

The implemented rewrite satisfies the following code and local-data
requirements:

- `go test ./...` and `go test -race ./...` pass.
- Real block fixtures from every supported era normalize successfully.
- Agreement tests prove that no mismatched, missing, reordered, or
  single-relay block reaches the decoder.
- A fact-table failure leaves no adoption event.
- An ambiguous adoption response is resolved by exact event readback.
- Restart ignores orphan fact rows and resumes from the latest committed tip.
- A unanimous rollback invalidates exactly the committed descendants and
  re-adoption works.
- Ordinary roll-forward issues zero ClickHouse `SELECT` statements.
- ClickHouse asynchronous insertion is explicitly disabled for this writer.
- The 100,352-block generated-fact ClickHouse publication benchmark is run and
  reported without presenting it as real-CBOR or live-network throughput.
- Publication capacity exceeds the 1,000-block/second local write-path gate.
  It exceeds measured strict two-relay intake by more than 800x on this host.
- A 100,000-block real-CBOR strict two-relay replay has exact contiguous
  adoption/publication identifiers and no bad relay provenance.
- No legacy manifest, corroboration/trust engine, UTxO resolver, fact
  verification pass, or compatibility implementation remains.
