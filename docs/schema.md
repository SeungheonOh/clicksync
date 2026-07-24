# ClickHouse schema and publication contract

Status: implementation contract  
Database: `clicksync`

## 1. Principles

- Preserve the useful legacy fact-table shape and binary representations.
- Delete `dataset_manifest`.
- Delete validation/evidence fields whose claims are no longer made.
- Keep one append-only visibility mechanism.
- Keep rollback header-last semantics.
- Make ordinary roll-forward write-only until an insert result is ambiguous.
- Do not retain raw blocks, full transactions, witnesses, or scripts.

## 2. Binary representation

- Cardano 32-byte hashes use `FixedString(32)`.
- Cardano 28-byte hashes use `FixedString(28)`.
- Raw addresses, reward accounts, asset names, and retained compact CBOR use
  binary-safe `String`.
- Lovelace and produced quantities use `UInt64`.
- Mint/burn deltas use `Int64`.
- Parallel asset arrays are sorted by `(policy_id, asset_name)` and have equal
  lengths.
- Timestamps use `DateTime64(6, 'UTC')`.

## 3. Tables

### 3.1 `dataset`

One immutable logical row:

```text
dataset_id UUID
schema_hash FixedString(32)
network_magic UInt32
network_name LowCardinality(String)
start_origin Bool
start_slot Nullable(UInt64)
start_hash Nullable(FixedString(32))
start_block_number Nullable(UInt64)
start_is_byron_ebb Bool
created_at DateTime64(6, 'UTC')
source_build String
```

There is no revision, current tip, trust state, evidence state, pending write,
or rollback state. Startup reads all rows and accepts only byte-equivalent
replays of one logical identity. A conflicting identity fails closed.

### 3.2 `blocks`

The legacy block fact columns remain where they describe observed data:

```text
publication_id UInt64
block_hash FixedString(32)
parent_hash Nullable(FixedString(32))
slot UInt64
block_number UInt64
era LowCardinality(String)
block_type Int16
transaction_count UInt32
input_count UInt32
output_count UInt32
datum_observation_count UInt32
withdrawal_count UInt32
redeemer_count UInt32
metadata_count UInt32
synthetic Bool
content_hash FixedString(32)
relay_hosts Array(String)
relay_addresses Array(String)
relay_operators Array(String)
n2n_versions Array(UInt16)
network_magic UInt32
observed_at DateTime64(6, 'UTC')
inserted_at DateTime64(6, 'UTC')
```

Removed:

- `body_hash_verified`;
- `transaction_hashes_verified`;
- `facts_digest`;
- singular source fields that implied one authoritative relay.

The four relay arrays have equal lengths and preserve configuration order.
`content_hash` is the raw-block agreement digest, not a Cardano block-body
validation result.

### 3.3 `transactions`

Preserve the legacy compact transaction facts:

- publication and block identity;
- transaction hash/order and optional nested identity;
- era and phase-2 validity;
- regular/collateral/genesis flow kind;
- declared and directly encoded effective fee information;
- mint deltas and whether they are applied;
- input/output/context counts.

No field claims local ledger validation.

### 3.4 `inputs`

Store:

```text
publication_id
block_number
tx_hash
tx_order
source_tx_hash
source_output_index
body_ordinal
role
is_consumed
```

Delete `source_is_resolved`. Clicksync never queries historical outputs during
ingestion. Consumers determine source availability at their chosen snapshot.

### 3.5 `outputs`

Preserve:

- output reference and body ordinal;
- regular/collateral-return/genesis kind;
- raw address and optional payment credential;
- lovelace and native assets;
- datum kind/hash;
- optional reference-script hash/language.

Address-derived credential fields are optional enrichment. The raw address is
the authoritative stored observation.

### 3.6 `datum_bodies`

Make datum bodies publication-scoped:

```text
publication_id UInt64
block_number UInt64
datum_hash FixedString(32)
datum_cbor String
byte_length UInt32
observed_at DateTime64(6, 'UTC')
```

This deliberately allows duplicate bodies across publications and removes the
steady-state existing-datum lookup. Consumers group by hash and may audit
conflicts offline.

### 3.7 Remaining context tables

Preserve the useful legacy shapes for:

- `datum_observations`;
- `withdrawals`;
- `redeemers`;
- `transaction_metadata`.

Redeemer resolution means transaction-local pointer interpretation only.
Cross-transaction script lookup belongs to consumers.

### 3.8 `chain_events`

`chain_events` is append-only publication membership:

```text
event_seq UInt64
publication_id UInt64
event_kind Enum8('adoption' = 1, 'invalidation' = 2)
active Bool
rollback_id Nullable(UUID)
block_hash FixedString(32)
slot UInt64
block_number UInt64
is_byron_ebb Bool
writer_id UUID
recorded_at DateTime64(6, 'UTC')
```

Adoption rows are authoritative immediately. Invalidation rows are
authoritative only when joined to the exact committed `rollbacks` header.

### 3.9 `rollbacks`

Keep one compact header:

```text
rollback_id UUID
event_seq UInt64
rollback_to_origin Bool
rollback_to_slot Nullable(UInt64)
rollback_to_hash Nullable(FixedString(32))
rollback_to_block_number Nullable(UInt64)
rollback_to_is_byron_ebb Bool
old_tip_slot Nullable(UInt64)
old_tip_hash Nullable(FixedString(32))
old_tip_block_number Nullable(UInt64)
old_tip_is_byron_ebb Bool
old_tip_event_seq UInt64
depth UInt32
relay_hosts Array(String)
relay_addresses Array(String)
relay_operators Array(String)
reason String
writer_id UUID
recorded_at DateTime64(6, 'UTC')
```

Removed are check IDs, agreement groups, attempts, evidence counts/digests,
thresholds, and pending manifest state. The unanimous ordered rollback event
is the evidence.

## 4. Publication authority

### Roll forward

For a batch of publications:

1. insert all fact rows;
2. insert all adoption rows in one native `chain_events` batch;
3. consider the prefix committed only after that insert succeeds or exact
   readback proves the complete expected rows.

### Rollback

For descendants `D` and rollback `R`:

1. insert all invalidation rows for `D` in one native batch;
2. insert the single `rollbacks(R)` header;
3. only the header makes matching invalidations effective.

### Re-adoption

Re-adoption writes a new immutable publication and a later adoption event.
This avoids hot-path fact existence reads. Rollbacks are rare enough that the
small duplicate storage cost is preferable to another stateful path.

## 5. Snapshot semantics

The committed snapshot is:

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
)
```

Committed membership up to snapshot `S` is:

```sql
SELECT publication_id, event_seq, active
FROM clicksync.chain_events
WHERE event_kind = 'adoption'
  AND event_seq <= S

UNION ALL

SELECT ce.publication_id, ce.event_seq, ce.active
FROM clicksync.chain_events AS ce
INNER JOIN clicksync.rollbacks AS rb
    ON rb.rollback_id = ce.rollback_id
   AND rb.event_seq = ce.event_seq
WHERE ce.event_kind = 'invalidation'
  AND ce.event_seq <= S
```

The active state for one publication is its last committed membership event.
Queries should first identify candidate publication IDs from fact-table order
keys, then resolve membership for only those candidates.

## 6. Current tip

The latest committed action determines the tip:

- latest adoption: the adoption row with the largest event sequence;
- latest rollback: the rollback target from the header.

There is no cached manifest head. `status` and startup derive this value using
queries ordered by the leading event key.

## 7. Identifier allocation

The sole writer initializes high-water marks above:

- every `publication_id` in every fact table and `chain_events`;
- every raw `event_seq` in `chain_events` and `rollbacks`.

This includes invisible remnants. IDs are process-local monotonic counters
after startup and are never reused.

## 8. Engines and order keys

Fact tables use `MergeTree` and partition by million-block ranges where they
carry block numbers. Leading order keys remain query-oriented:

| Table | Leading order key |
|---|---|
| `blocks` | `(block_hash, publication_id)` |
| `transactions` | `(tx_hash, publication_id, tx_order)` |
| `inputs` | `(source_tx_hash, source_output_index, publication_id)` |
| `outputs` | `(tx_hash, output_index, publication_id)` |
| `datum_bodies` | `(datum_hash, publication_id)` |
| `datum_observations` | `(datum_hash, publication_id)` |
| `withdrawals` | `(tx_hash, publication_id, body_ordinal)` |
| `redeemers` | `(tx_hash, publication_id, purpose, redeemer_index)` |
| `transaction_metadata` | `(tx_hash, publication_id)` |
| `chain_events` | `(event_seq, event_kind, publication_id)` with a publication projection |
| `rollbacks` | `(event_seq, rollback_id)` |

The schema uses no `ReplacingMergeTree` as a substitute for commit semantics
and readers do not depend on `FINAL`.

## 9. Crash cases

| Crash point | Durable meaning |
|---|---|
| During fact inserts | Orphan facts are invisible. |
| After facts, before adoption | Orphan facts are invisible. |
| Adoption response lost | Exact event readback decides. |
| After adoption | Batch is committed; no post-adoption manifest work exists. |
| During invalidation insert | No rollback header, so all remnants are inert. |
| After invalidations, before rollback header | Invalidations are inert. |
| Rollback response lost | Exact header and membership readback decides. |
| After rollback header | Rollback is committed; no post-header manifest work exists. |

