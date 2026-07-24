# Clicksync schema and snapshot contract

Status: implementation contract
Database: `clicksync`
Migration: `migrations/001_initial.sql`

This is the only ingestion/query boundary shared by Clicksync and Clickout.
Clickout must not import the root Go module.

## Binary and numeric representation

- Block, transaction, datum, and CBOR content hashes are raw 32-byte
  `FixedString(32)` values.
- Policy, script, and credential hashes are raw 28-byte `FixedString(28)`
  values.
- Addresses, reward accounts, asset names, datum/redeemer data, metadata-map
  CBOR, peer names, and target identities are binary-safe `String` values.
- Lovelace and produced asset quantities are `UInt64`.
- Mint and burn quantities are non-zero `Int64`.
- Parallel asset arrays are sorted lexicographically by `(policy_id,
  asset_name)` before insertion and always have equal lengths.
- The only persisted CBOR columns are `datum_bodies.datum_cbor`,
  `redeemers.data_cbor`, and `transaction_metadata.metadata_cbor`.

The writer rejects overflows, malformed lengths, unsorted arrays, unknown
eras/purposes, unresolved redeemer pointers, or a missing exact CBOR fragment
before it inserts any fact row.

## Publication identity

`blocks.publication_id` identifies one immutable fact bundle. Every
publication-scoped fact carries this identifier and its containing block
number. A fact bundle is not visible merely because its rows exist.

For roll-forward event `E`, the writer inserts and verifies all fact rows,
then inserts exactly one:

```text
chain_events(E, publication_id, event_kind='adoption', active=true)
```

That adoption row is the commit record. An incomplete attempt with no
adoption row is invisible.

For rollback event `E`, the writer first inserts one invalidation membership
row per affected publication:

```text
chain_events(E, publication_id, event_kind='invalidation', active=false,
             rollback_id=R)
```

It then inserts `rollbacks(R, E, ...)` last. Invalidation rows without that
matching header are inert. Re-adoption appends a later adoption event for the
same complete publication.

## Canonical snapshot

A reader captures the largest committed event once:

```sql
SELECT max(event_seq)
FROM
(
    SELECT event_seq
    FROM clicksync.chain_events
    WHERE event_kind = 'adoption'

    UNION ALL

    SELECT event_seq
    FROM clicksync.rollbacks
);
```

Origin with no committed event is snapshot `0`. Every page and traversal
frontier keeps the captured number.

At snapshot `{snapshot}`, committed membership events are:

```sql
SELECT publication_id, event_seq, active
FROM clicksync.chain_events
WHERE event_seq <= {snapshot:UInt64}
  AND event_kind = 'adoption'

UNION ALL

SELECT ce.publication_id, ce.event_seq, ce.active
FROM clicksync.chain_events AS ce
INNER JOIN clicksync.rollbacks AS rb
    ON ce.rollback_id = rb.rollback_id
   AND ce.event_seq = rb.event_seq
WHERE ce.event_seq <= {snapshot:UInt64}
  AND ce.event_kind = 'invalidation';
```

The active publication set is the last committed membership state per
publication:

```sql
SELECT publication_id
FROM committed_membership
GROUP BY publication_id
HAVING argMax(active, event_seq);
```

That whole-set expression defines semantics and is used for offline
reconciliation/fixtures. It is not the normal point, transaction, BFS, or
writer source-resolution plan. Those paths first use the relevant fact-table
order key to collect candidate `publication_id` values, add a
`publication_id IN (...)` restriction to the committed-membership query, and
filter only that bounded candidate set. A traversal batches this restriction
for each frontier. This preserves the same snapshot result without grouping
unrelated chain history on every lookup.

Facts join that set by `publication_id`. Readers do not use `FINAL`.
`dataset_manifest` and `writer_audit` are cache/operational tables; readers
select their latest rows with `argMax(..., revision)`. The manifest is
reconciled from committed adoption/rollback records after restart and never
defines snapshot authority.

The committed-membership query intentionally uses an `INNER JOIN` for
rollback membership instead of testing nullable output from a default
ClickHouse `LEFT JOIN`. This makes headerless invalidations inert regardless
of the server's `join_use_nulls` setting.

Synthetic genesis facts use a normal immutable publication and adoption
event. They are never selected for rollback membership.

## Allocation and content-addressed datum restart rules

At startup the sole writer allocates above every raw persisted identifier,
including invisible crash remnants. `publication_id` is greater than the
maximum found in `blocks`, all publication-scoped fact tables, and
`chain_events`. `event_seq` is greater than the maximum found in raw
`chain_events` and `rollbacks`. Neither allocation uses only the manifest or
committed snapshot.

`datum_bodies` is a plain `MergeTree`, not a versioned replacing table. Before
deduplicating an existing hash the writer verifies that every physical row has
the expected length, `hash(CBOR)`, and byte-identical body. It inserts a body
only when no row exists. A lost insert response may leave byte-identical
physical duplicates; readers use minimum/`argMin` semantics:

```sql
SELECT
    datum_hash,
    argMin(datum_cbor, (first_publication_id, first_seen_at)) AS datum_cbor,
    min(first_publication_id) AS first_publication_id,
    min(first_seen_at) AS first_seen_at
FROM clicksync.datum_bodies
WHERE datum_hash = {datum_hash:FixedString(32)}
GROUP BY datum_hash;
```

No query claims a `ReplacingMergeTree` version preserves the first sighting.

## Query-facing order keys

| Table | Leading order key | Intended access |
|---|---|---|
| `blocks` | `(block_hash, publication_id)` | point and publication lookup |
| `chain_events` | `(publication_id, event_seq)` | snapshot membership |
| `rollbacks` | `(event_seq, rollback_id)` | committed event and audit |
| `transactions` | `(tx_hash, publication_id, tx_order)` | transaction lookup |
| `inputs` | `(source_tx_hash, source_output_index, ...)` | active spend lookup |
| `outputs` | `(tx_hash, output_index, publication_id)` | UTxO and produced outputs |
| `datum_bodies` | `datum_hash` | content-addressed datum lookup |
| `datum_observations` | `(datum_hash, publication_id, ...)` | active provenance |
| `withdrawals` | `(tx_hash, publication_id, body_ordinal)` | transaction context |
| `redeemers` | `(tx_hash, publication_id, purpose, redeemer_index)` | transaction context |
| `transaction_metadata` | `(tx_hash, publication_id)` | transaction metadata |
| `peer_observations` | `(observed_tip_hash, observed_at, ...)` | checkpoint agreement |

`inputs` deliberately favors forward source-spend traversal and `outputs`
favors UTxO/producing-transaction access. Reverse input-by-consuming-
transaction and address history are measured before adding projections.

## Completeness and trust

The only trust value is
`peer_observed_structurally_verified`. It means the selected peer data passed
structural checks; it does not claim independent consensus or ledger
validation.

`complete_history=true` is schema-constrained to Origin start plus successful
deterministic genesis seeding. Intersection/tail datasets remain incomplete.
Clickout must return snapshot, trust mode, and completeness with every result.

## Writer authority and audit

`writer_audit` records owner, build, process, activation, heartbeat, and
graceful release history. It has no expiry or fencing semantics and is never
consulted as authority. A plain ClickHouse MergeTree table does not provide
atomic compare-and-swap/unique-row semantics, and an audit insert plus fact
publication cannot form one transaction.

Every supported writer instead shares and holds the single-host
`clicksync-state` advisory `flock` for process lifetime. A killed process can
therefore leave its last audit row looking active; the released operating
system lock, not that stale row, permits the next process to start.
Multi-host and separately mounted writers fail closed; there is no force or
stale-takeover mode.
