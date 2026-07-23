# Architecture and DB Sync parity plan

## Decision

The first Cardano provider uses Ogmios 7 rather than modifying `cardano-db-sync` or implementing Ouroboros from scratch.

The current [Cardano DB Sync 13.7.2.1 release](https://github.com/IntersectMBO/cardano-db-sync/releases/tag/13.7.2.1) has a sound high-level shape: a pipelined local Chain Sync client, a bounded event queue, ledger application/snapshots, era normalization, batched database writes, and separate off-chain workers. Its PostgreSQL boundary is not narrow, though. Surrogate-ID lookup, insertion ordering, caches, foreign keys, Hasql sessions, and rollback deletion are spread through its universal insert modules. Replacing `cardano-db` in place would retain most of that complexity while fighting ClickHouse's storage model.

The reusable conceptual seams in DB Sync are:

| DB Sync | clicksync |
| --- | --- |
| `Sync.hs`: local Chain Sync, intersection, pipelining | `OgmiosChainSource`: Ogmios JSON-RPC Chain Sync |
| `DbEvent.hs`: bounded ordered apply/rollback events | Ogmios sequential handler with configurable in-flight requests |
| `Generic.Block` / `Generic.Tx.Types`: era normalization | `normalizeBlock`: provider JSON to immutable fact rows |
| `Database.hs`: batch/rollback coordination | `Ingestor`: ordered logical event sequence and publish barriers |
| PostgreSQL normalized schema and cascading deletes | ClickHouse fact tables plus canonicality/rollback logs |
| consensus ledger state and snapshots | planned direct-ledger provider |
| off-chain pool/vote fetchers | deferred independent workers |

DB Sync connects to the local node over a Unix socket and maintains an internal ledger copy; its own overview is in the [project README](https://github.com/IntersectMBO/cardano-db-sync/blob/13.7.2.1/Readme.md). Ogmios already provides the same `findIntersection` / `nextBlock` Chain Sync cursor and current era codecs over a stable JSON boundary. The [Ogmios local Chain Sync guide](https://github.com/CardanoSolutions/ogmios/blob/v7.0.0/docs/content/mini-protocols/local-chain-sync.md) documents that cursor and intersection behavior.

This is an incremental choice, not a permanent restriction. `ChainSource` is transport-neutral. A future Haskell provider can reuse `ouroboros-network`, `ouroboros-consensus`, `cardano-ledger`, and DB Sync's pure normalization seams without importing its PostgreSQL layer.

## Data model

Facts are immutable and always carry `network_magic` and their containing `block_hash`:

- `blocks`
- `transactions`
- `tx_inputs`
- `tx_outputs`
- `output_assets`
- `mint_assets`

Retries can leave duplicate fact attempts. Fact tables therefore use `ReplacingMergeTree(ingest_seq)`, keep every logical identity in one partition, and authoritative views use `FINAL`.

Fork selection is separate:

- `chain_events` appends `is_canonical=true` after every successful block fact publication.
- A rollback appends `is_canonical=false` for every currently active descendant.
- `rollbacks` records the raw target, prior tip, reason, depth, writer, and logical sequence.
- Every invalidation row has a `rollback_id`. It is ignored until the matching `rollbacks` header exists.

All rows in one rollback share one `event_seq`, making the rollback one logical snapshot boundary. Re-adoption appends a newer `true`, so a table of blocks that were *ever* rolled back would be incorrect.

### Why the UTxO query is a double set difference

For a selected canonical chain:

```text
current UTxO
  = outputs produced by canonical blocks
    − inputs consumed by canonical blocks
```

Suppose block `A` produces `x`, and block `B` spends `x` and produces `y`.

```text
canonical A,B:  {x,y} − {x} = {y}
rollback B:     {x}   − {}  = {x}
```

Rolling back `B` must remove `y` *and resurrect `x`*. Subtracting only outputs from rolled-back blocks misses the resurrection. `current_utxos` first restricts both output and spend facts to canonical blocks, then uses a left anti-join on `(network_magic, source_tx_hash, source_output_index)`.

Phase-2 validity is encoded on the facts before this query:

- Valid transaction: ordinary inputs are consumed and ordinary outputs are produced.
- Invalid transaction: collateral inputs are consumed and only collateral-return output is produced.
- Reference inputs are never consumed.
- Non-applied rows remain queryable for forensic/block-level analysis.

## Crash and retry model

ClickHouse does not provide a normal transaction across these tables. Visibility is therefore controlled by small append-only commit records.

Roll forward:

1. Allocate a monotonic logical `event_seq` per block from the maximum already stored.
2. Buffer a bounded number of consecutive blocks and insert each fact table once for the batch.
3. Append all corresponding `chain_events(is_canonical=true)` markers last.
4. Canonical views only see the batch after step 3.

Rollback:

1. Resolve the rollback point and count its exact current canonical descendants in ClickHouse.
2. Allocate one logical sequence and one `rollback_id`.
3. Use server-side `INSERT ... SELECT` to append all descendant `chain_events(is_canonical=false, rollback_id=...)` without materializing them in the application.
4. Insert the `rollbacks` header last.
5. Canonical status ignores step 3 until step 4 exists.

If a response is lost, reconnect/intersection derives state from ClickHouse, not process memory. Retried fact identities collapse under `FINAL`; duplicate canonicality rows carry the same result. An insert attempt that never received its commit event remains invisible.

ClickHouse 26.3 enables asynchronous inserts by default. The client explicitly disables them for these writes so an HTTP success means the part is visible before the next publication step.

## Query and engine choices

The source of truth uses append-oriented MergeTree engines. It deliberately avoids mutations, `VersionedCollapsingMergeTree`, and incremental materialized views joined to canonical state.

- `ReplacingMergeTree` is used only to make idempotent fact/manifests exact under `FINAL`.
- `chain_events` is a plain `MergeTree`, retaining the audit sequence.
- `argMax(is_canonical, event_seq)` resolves the current state of each block.
- `tx_outputs` is ordered by network and address for the dominant address lookup.
- inputs are ordered by the output reference they consume; assets are ordered by policy/name.
- `current_utxos_by_address(network_magic,address)` narrows output, spend, and canonicality reads to one address's candidate references; `current_utxos` remains the global analytical view.

An incremental ClickHouse materialized view reacts only to inserts into its source. A rollback inserted into another table would not make it revisit historical outputs, so such a view cannot be the canonical UTxO authority. ClickHouse's own [materialized-view guidance](https://clickhouse.com/blog/common-getting-started-issues-with-clickhouse) calls out this source-block trigger model. The query-time anti-join remains authoritative; rebuildable explicit delta caches can be added later.

## Current scope and parity roadmap

### Implemented

- Chain Sync resume, roll forward, roll backward, reconnect.
- Byron, Shelley, Allegra, Mary, Alonzo, Babbage, Conway, and Dijkstra block shapes provided by Ogmios 7.
- Dijkstra sub-transactions.
- Blocks, transactions, inputs, outputs, output assets, mint/burn.
- Metadata/certificate/withdrawal/proposal/vote JSON retention on transaction rows.
- Optional transaction CBOR retention when Ogmios is started with `--include-transaction-cbor`.
- Correct canonical UTxO semantics for indexed chain outputs and rollback audit views.

### Next: chain-data completeness

1. Seed and validate Byron and Shelley genesis distributions as permanently canonical synthetic facts. Chain Sync does not emit them; DB Sync explicitly creates artificial genesis blocks/transactions for this reason.
2. Normalize datums, scripts, redeemers, metadata labels, withdrawals, certificates, governance proposals/votes, and Dijkstra account/sub-ledger structures into dedicated analytical tables.
3. Add block time/epoch derivation from validated genesis configurations.
4. Add explicit stream generations and snapshot sequence tokens for reindexing, failover, and pagination that cannot straddle a rollback.
5. Add rebuildable address/asset balance delta caches after measuring real mainnet query plans.

### Then: ledger provider

Full DB Sync parity requires applying the Cardano ledger, not merely decoding blocks:

- rewards and epoch stake;
- pool distributions and epoch snapshots;
- ADA pots and deposits;
- ratification/enactment and other governance state;
- validated ledger-state snapshots at restart/rollback points;
- genesis delegation and staking state.

The direct provider should reuse the official Haskell consensus/ledger libraries and preserve the existing `ChainSource` plus normalized fact interfaces. It should not reimplement Ouroboros wire protocols or transplant DB Sync's PostgreSQL insertion layer.

### Separate workers

Off-chain pool metadata, vote metadata, and monitoring/metrics do not belong in the ordered chain commit path. They can be independent append-only workers keyed by immutable on-chain references.

## Operational constraints

- Exactly one writer is supported per `(database, network_magic)`. `event_seq` is allocated from stored maxima, not a distributed sequencer.
- Rollbacks deeper than the configured guard fail before canonicality changes; normal membership insertion stays server-side regardless of depth.
- Use a new database for full reindexing until stream generations are implemented.
- EBB blocks do not contain a Chain Sync slot in the Ogmios block shape. They are stored with `slot = NULL` and are not offered as restart intersection candidates.
- Restart intersections include the latest security-window points densely plus bucketed points across the complete stored history; `origin` is always the final fallback.
- Configuration requires the intersection candidate count to cover every permitted rollback point plus a historical bucket.
- The `current_*` views are exact, not precomputed. Large public deployments should benchmark filters and add measured projections/delta caches rather than hiding mutable state behind an incorrect materialized view.
- ClickHouse 24.8+ is the supported floor for the official JavaScript client; the supplied deployment pins 26.3 LTS.
