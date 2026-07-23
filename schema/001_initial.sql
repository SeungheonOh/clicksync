CREATE TABLE IF NOT EXISTS __DATABASE__.blocks
(
    network_magic UInt32,
    block_hash String,
    previous_block_hash Nullable(String),
    slot Nullable(UInt64),
    block_number UInt64,
    era LowCardinality(String),
    block_type LowCardinality(String),
    size_bytes Nullable(UInt64),
    tx_count UInt32,
    issuer_vkey Nullable(String),
    ingest_seq UInt64,
    inserted_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(ingest_seq)
PARTITION BY (network_magic, intDiv(block_number, 1000000))
ORDER BY (network_magic, block_hash);

CREATE TABLE IF NOT EXISTS __DATABASE__.transactions
(
    network_magic UInt32,
    block_hash String,
    block_number UInt64,
    tx_hash String,
    tx_index UInt32,
    parent_tx_hash Nullable(String),
    subtx_index Nullable(UInt32),
    is_valid Bool,
    is_applied Bool,
    fee_lovelace Nullable(UInt64),
    invalid_before Nullable(UInt64),
    invalid_after Nullable(UInt64),
    metadata_json Nullable(String) CODEC(ZSTD(3)),
    certificates_json Nullable(String) CODEC(ZSTD(3)),
    withdrawals_json Nullable(String) CODEC(ZSTD(3)),
    proposals_json Nullable(String) CODEC(ZSTD(3)),
    votes_json Nullable(String) CODEC(ZSTD(3)),
    tx_cbor Nullable(String) CODEC(ZSTD(3)),
    ingest_seq UInt64,
    inserted_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(ingest_seq)
PARTITION BY (network_magic, intDiv(block_number, 1000000))
ORDER BY (network_magic, tx_hash, block_hash);

CREATE TABLE IF NOT EXISTS __DATABASE__.tx_inputs
(
    network_magic UInt32,
    block_hash String,
    block_number UInt64,
    tx_hash String,
    input_kind LowCardinality(String),
    input_index UInt32,
    source_tx_hash String,
    source_output_index UInt32,
    is_consumed Bool,
    ingest_seq UInt64,
    inserted_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(ingest_seq)
PARTITION BY (network_magic, intDiv(block_number, 1000000))
ORDER BY
(
    network_magic,
    source_tx_hash,
    source_output_index,
    block_hash,
    tx_hash,
    input_kind,
    input_index
);

CREATE TABLE IF NOT EXISTS __DATABASE__.tx_outputs
(
    network_magic UInt32,
    block_hash String,
    block_number UInt64,
    tx_hash String,
    output_index UInt32,
    output_kind LowCardinality(String),
    address String CODEC(ZSTD(3)),
    lovelace UInt64,
    datum_hash Nullable(String),
    inline_datum Nullable(String) CODEC(ZSTD(3)),
    reference_script_json Nullable(String) CODEC(ZSTD(3)),
    is_produced Bool,
    ingest_seq UInt64,
    inserted_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(ingest_seq)
PARTITION BY (network_magic, intDiv(block_number, 1000000))
-- Address is first because address UTxO/balance lookups are the dominant
-- provider query. The full natural identity still participates in replacement.
ORDER BY (network_magic, address, tx_hash, output_index, block_hash);

CREATE TABLE IF NOT EXISTS __DATABASE__.output_assets
(
    network_magic UInt32,
    block_hash String,
    block_number UInt64,
    tx_hash String,
    output_index UInt32,
    policy_id String,
    asset_name String,
    quantity UInt64,
    is_produced Bool,
    ingest_seq UInt64,
    inserted_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(ingest_seq)
PARTITION BY (network_magic, intDiv(block_number, 1000000))
ORDER BY
(
    network_magic,
    policy_id,
    asset_name,
    tx_hash,
    output_index,
    block_hash
);

CREATE TABLE IF NOT EXISTS __DATABASE__.mint_assets
(
    network_magic UInt32,
    block_hash String,
    block_number UInt64,
    tx_hash String,
    policy_id String,
    asset_name String,
    quantity Int64,
    is_applied Bool,
    ingest_seq UInt64,
    inserted_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(ingest_seq)
PARTITION BY (network_magic, intDiv(block_number, 1000000))
ORDER BY (network_magic, policy_id, asset_name, tx_hash, block_hash);

-- This is the publish manifest. A block is query-visible only after its facts
-- have been inserted and an is_canonical=true event has been appended here.
CREATE TABLE IF NOT EXISTS __DATABASE__.chain_events
(
    network_magic UInt32,
    event_seq UInt64,
    block_hash String,
    slot Nullable(UInt64),
    block_number UInt64,
    is_canonical Bool,
    rollback_id Nullable(UUID),
    writer_id UUID,
    observed_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY (network_magic, intDiv(event_seq, 10000000))
ORDER BY (network_magic, block_hash, event_seq);

-- One audit row per ChainSync rollback. The invalidated block membership is
-- represented by chain_events rows sharing rollback_id.
CREATE TABLE IF NOT EXISTS __DATABASE__.rollbacks
(
    network_magic UInt32,
    rollback_id UUID,
    rollback_to_hash Nullable(String),
    rollback_to_slot Nullable(UInt64),
    old_tip_hash Nullable(String),
    old_tip_slot Nullable(UInt64),
    depth UInt32,
    event_seq UInt64,
    reason LowCardinality(String),
    writer_id UUID,
    observed_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(event_seq)
PARTITION BY network_magic
ORDER BY (network_magic, rollback_id);

CREATE VIEW IF NOT EXISTS __DATABASE__.canonical_block_status AS
SELECT
    e.network_magic AS network_magic,
    e.block_hash AS block_hash,
    argMax(e.slot, e.event_seq) AS slot,
    argMax(e.block_number, e.event_seq) AS block_number,
    max(e.event_seq) AS canonical_event_seq,
    argMax(e.is_canonical, e.event_seq) AS is_canonical
FROM __DATABASE__.chain_events AS e
-- A rollback header is the commit marker for its invalidation rows. A crash
-- before that last insert leaves those rows inert and safe to retry.
WHERE e.rollback_id IS NULL
   OR (e.network_magic, assumeNotNull(e.rollback_id)) IN
      (SELECT network_magic, rollback_id FROM __DATABASE__.rollbacks FINAL)
GROUP BY e.network_magic, e.block_hash
HAVING is_canonical = true;

CREATE VIEW IF NOT EXISTS __DATABASE__.current_chain AS
SELECT
    b.network_magic AS network_magic,
    b.block_hash AS block_hash,
    b.previous_block_hash AS previous_block_hash,
    b.slot AS slot,
    b.block_number AS block_number,
    b.era AS era,
    b.block_type AS block_type,
    b.size_bytes AS size_bytes,
    b.tx_count AS tx_count,
    b.issuer_vkey AS issuer_vkey,
    s.canonical_event_seq AS canonical_event_seq
FROM (SELECT * FROM __DATABASE__.blocks FINAL) AS b
INNER JOIN __DATABASE__.canonical_block_status AS s
    ON b.network_magic = s.network_magic AND b.block_hash = s.block_hash;

CREATE VIEW IF NOT EXISTS __DATABASE__.current_transactions AS
SELECT t.*
FROM (SELECT * FROM __DATABASE__.transactions FINAL) AS t
INNER JOIN __DATABASE__.canonical_block_status AS c
    ON t.network_magic = c.network_magic AND t.block_hash = c.block_hash;

-- A rolled-back output disappears because its producing block leaves the
-- canonical set. A rolled-back spend disappears too, thereby resurrecting the
-- output it had consumed.
CREATE VIEW IF NOT EXISTS __DATABASE__.current_utxos AS
WITH
    produced AS
    (
        SELECT o.*
        FROM (SELECT * FROM __DATABASE__.tx_outputs FINAL) AS o
        INNER JOIN __DATABASE__.canonical_block_status AS c
            ON o.network_magic = c.network_magic AND o.block_hash = c.block_hash
        WHERE o.is_produced = true
    ),
    consumed AS
    (
        SELECT DISTINCT
            i.network_magic AS network_magic,
            i.source_tx_hash AS source_tx_hash,
            i.source_output_index AS source_output_index
        FROM (SELECT * FROM __DATABASE__.tx_inputs FINAL) AS i
        INNER JOIN __DATABASE__.canonical_block_status AS c
            ON i.network_magic = c.network_magic AND i.block_hash = c.block_hash
        WHERE i.is_consumed = true
    )
SELECT o.*
FROM produced AS o
LEFT ANTI JOIN consumed AS i
    ON o.network_magic = i.network_magic
    AND o.tx_hash = i.source_tx_hash
    AND o.output_index = i.source_output_index;

-- Provider fast path. Unlike filtering current_utxos after its global
-- anti-join, this parameterized view restricts producer status, spend rows,
-- and spender status to the output references owned by one address.
CREATE VIEW IF NOT EXISTS __DATABASE__.current_utxos_by_address AS
WITH
    address_outputs AS
    (
        SELECT *
        FROM (SELECT * FROM __DATABASE__.tx_outputs FINAL)
        WHERE network_magic = {network_magic:UInt32}
          AND address = {address:String}
          AND is_produced = true
    ),
    canonical_producers AS
    (
        SELECT block_hash
        FROM __DATABASE__.canonical_block_status
        WHERE network_magic = {network_magic:UInt32}
          AND block_hash IN (SELECT block_hash FROM address_outputs)
    ),
    candidate_outputs AS
    (
        SELECT *
        FROM address_outputs
        WHERE block_hash IN (SELECT block_hash FROM canonical_producers)
    ),
    candidate_refs AS
    (
        SELECT tx_hash, output_index
        FROM candidate_outputs
    ),
    candidate_spends AS
    (
        SELECT
            source_tx_hash,
            source_output_index,
            block_hash
        FROM (SELECT * FROM __DATABASE__.tx_inputs FINAL)
        WHERE network_magic = {network_magic:UInt32}
          AND is_consumed = true
          AND (source_tx_hash, source_output_index) IN
              (SELECT tx_hash, output_index FROM candidate_refs)
    ),
    canonical_spenders AS
    (
        SELECT block_hash
        FROM __DATABASE__.canonical_block_status
        WHERE network_magic = {network_magic:UInt32}
          AND block_hash IN (SELECT block_hash FROM candidate_spends)
    ),
    consumed_refs AS
    (
        SELECT DISTINCT source_tx_hash, source_output_index
        FROM candidate_spends
        WHERE block_hash IN (SELECT block_hash FROM canonical_spenders)
    )
SELECT o.*
FROM candidate_outputs AS o
LEFT ANTI JOIN consumed_refs AS i
    ON o.tx_hash = i.source_tx_hash
    AND o.output_index = i.source_output_index;

CREATE VIEW IF NOT EXISTS __DATABASE__.current_utxo_assets AS
SELECT a.*
FROM (SELECT * FROM __DATABASE__.output_assets FINAL) AS a
INNER JOIN __DATABASE__.current_utxos AS u
    ON a.network_magic = u.network_magic
    AND a.block_hash = u.block_hash
    AND a.tx_hash = u.tx_hash
    AND a.output_index = u.output_index
WHERE a.is_produced = true;

CREATE VIEW IF NOT EXISTS __DATABASE__.address_balances AS
SELECT
    network_magic,
    address,
    sum(lovelace) AS lovelace,
    count() AS utxo_count
FROM __DATABASE__.current_utxos
GROUP BY network_magic, address;
