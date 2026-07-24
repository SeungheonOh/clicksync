CREATE DATABASE IF NOT EXISTS clicksync;

CREATE TABLE IF NOT EXISTS clicksync.dataset
(
    dataset_id UUID,
    schema_hash FixedString(32),
    network_magic UInt32,
    network_name LowCardinality(String),
    start_origin Bool,
    start_slot Nullable(UInt64),
    start_hash Nullable(FixedString(32)),
    start_block_number Nullable(UInt64),
    start_is_byron_ebb Bool,
    created_at DateTime64(6, 'UTC'),
    source_build String,
    CONSTRAINT dataset_start_point CHECK
        (start_origin AND isNull(start_slot) AND isNull(start_hash) AND isNull(start_block_number) AND NOT start_is_byron_ebb)
        OR
        (NOT start_origin AND isNotNull(start_slot) AND isNotNull(start_hash) AND isNotNull(start_block_number))
)
ENGINE = MergeTree
ORDER BY dataset_id;

CREATE TABLE IF NOT EXISTS clicksync.blocks
(
    publication_id UInt64,
    block_hash FixedString(32),
    parent_hash Nullable(FixedString(32)),
    slot UInt64,
    block_number UInt64,
    era LowCardinality(String),
    block_type Int16,
    transaction_count UInt32,
    input_count UInt32,
    output_count UInt32,
    datum_observation_count UInt32,
    withdrawal_count UInt32,
    redeemer_count UInt32,
    metadata_count UInt32,
    synthetic Bool,
    content_hash FixedString(32),
    relay_hosts Array(String),
    relay_addresses Array(String),
    relay_operators Array(String),
    n2n_versions Array(UInt16),
    network_magic UInt32,
    observed_at DateTime64(6, 'UTC'),
    inserted_at DateTime64(6, 'UTC'),
    INDEX publication_idx publication_id TYPE minmax GRANULARITY 1,
    PROJECTION blocks_by_publication
    (
        SELECT publication_id, block_hash, _part_offset
        ORDER BY publication_id
    ),
    CONSTRAINT blocks_relay_arrays CHECK
        length(relay_hosts) = length(relay_addresses)
        AND length(relay_hosts) = length(relay_operators)
        AND length(relay_hosts) = length(n2n_versions)
)
ENGINE = MergeTree
PARTITION BY intDiv(block_number, 1000000)
ORDER BY (block_hash, publication_id);

CREATE TABLE IF NOT EXISTS clicksync.transactions
(
    publication_id UInt64,
    block_number UInt64,
    tx_hash FixedString(32),
    tx_order UInt32,
    parent_tx_hash Nullable(FixedString(32)),
    subtransaction_index Nullable(UInt32),
    era LowCardinality(String),
    phase2_valid Bool,
    flow_kind Enum8('regular' = 1, 'collateral' = 2, 'genesis' = 3),
    declared_fee_lovelace Nullable(UInt64),
    effective_fee_lovelace Nullable(UInt64),
    mint_is_applied Bool,
    mint_policy_ids Array(FixedString(28)),
    mint_asset_names Array(String) CODEC(ZSTD(3)),
    mint_quantities Array(Int64) CODEC(ZSTD(3)),
    regular_input_count UInt32,
    collateral_input_count UInt32,
    reference_input_count UInt32,
    produced_output_count UInt32,
    withdrawal_count UInt32,
    redeemer_count UInt32,
    metadata_present Bool,
    datum_observation_count UInt32,
    INDEX publication_idx publication_id TYPE minmax GRANULARITY 1,
    CONSTRAINT transactions_mint_arrays CHECK
        length(mint_policy_ids) = length(mint_asset_names)
        AND length(mint_policy_ids) = length(mint_quantities)
)
ENGINE = MergeTree
PARTITION BY intDiv(block_number, 1000000)
ORDER BY (tx_hash, publication_id, tx_order);

CREATE TABLE IF NOT EXISTS clicksync.inputs
(
    publication_id UInt64,
    block_number UInt64,
    tx_hash FixedString(32),
    tx_order UInt32,
    source_tx_hash FixedString(32),
    source_output_index UInt32,
    body_ordinal UInt32,
    role Enum8('regular' = 1, 'collateral' = 2, 'reference' = 3),
    is_consumed Bool,
    INDEX publication_idx publication_id TYPE minmax GRANULARITY 1,
    PROJECTION inputs_by_consuming_tx
    (
        SELECT tx_hash, publication_id, body_ordinal, _part_offset
        ORDER BY (tx_hash, publication_id, body_ordinal)
    )
)
ENGINE = MergeTree
PARTITION BY intDiv(block_number, 1000000)
ORDER BY (source_tx_hash, source_output_index, publication_id);

CREATE TABLE IF NOT EXISTS clicksync.outputs
(
    publication_id UInt64,
    block_number UInt64,
    tx_hash FixedString(32),
    tx_order UInt32,
    output_index UInt32,
    body_ordinal UInt32,
    output_kind Enum8('regular' = 1, 'collateral_return' = 2, 'genesis' = 3),
    address String CODEC(ZSTD(3)),
    address_hash UInt64 MATERIALIZED sipHash64(address),
    payment_credential_kind Nullable(Enum8('none' = 0, 'key' = 1, 'script' = 2)),
    payment_credential_hash Nullable(FixedString(28)),
    lovelace UInt64,
    asset_policy_ids Array(FixedString(28)),
    asset_names Array(String) CODEC(ZSTD(3)),
    asset_quantities Array(UInt64) CODEC(ZSTD(3)),
    datum_kind Enum8('none' = 1, 'hash' = 2, 'inline' = 3),
    datum_hash Nullable(FixedString(32)),
    reference_script_hash Nullable(FixedString(28)),
    reference_script_language Nullable(String),
    INDEX publication_idx publication_id TYPE minmax GRANULARITY 1,
    PROJECTION outputs_by_address_hash
    (
        SELECT address_hash, _part_offset
        ORDER BY address_hash
    ),
    PROJECTION outputs_by_payment_credential
    (
        SELECT payment_credential_kind, payment_credential_hash, _part_offset
        ORDER BY (payment_credential_kind, payment_credential_hash)
    ),
    CONSTRAINT outputs_asset_arrays CHECK
        length(asset_policy_ids) = length(asset_names)
        AND length(asset_policy_ids) = length(asset_quantities)
)
ENGINE = MergeTree
PARTITION BY intDiv(block_number, 1000000)
ORDER BY (tx_hash, output_index, publication_id);

CREATE TABLE IF NOT EXISTS clicksync.datum_bodies
(
    publication_id UInt64,
    block_number UInt64,
    datum_hash FixedString(32),
    datum_cbor String CODEC(ZSTD(3)),
    byte_length UInt32,
    observed_at DateTime64(6, 'UTC'),
    INDEX publication_idx publication_id TYPE minmax GRANULARITY 1,
    CONSTRAINT datum_bodies_length CHECK byte_length = length(datum_cbor)
)
ENGINE = MergeTree
PARTITION BY intDiv(block_number, 1000000)
ORDER BY (datum_hash, publication_id);

CREATE TABLE IF NOT EXISTS clicksync.datum_observations
(
    publication_id UInt64,
    block_number UInt64,
    datum_hash FixedString(32),
    tx_hash FixedString(32),
    tx_order UInt32,
    source_kind Enum8('inline_output' = 1, 'witness' = 2),
    source_ordinal UInt32,
    output_index Nullable(UInt32),
    INDEX publication_idx publication_id TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
PARTITION BY intDiv(block_number, 1000000)
ORDER BY (datum_hash, publication_id);

CREATE TABLE IF NOT EXISTS clicksync.withdrawals
(
    publication_id UInt64,
    block_number UInt64,
    tx_hash FixedString(32),
    tx_order UInt32,
    body_ordinal UInt32,
    reward_account String,
    lovelace UInt64,
    is_applied Bool,
    credential_kind Nullable(Enum8('key' = 1, 'script' = 2)),
    credential_hash Nullable(FixedString(28)),
    INDEX publication_idx publication_id TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
PARTITION BY intDiv(block_number, 1000000)
ORDER BY (tx_hash, publication_id, body_ordinal);

CREATE TABLE IF NOT EXISTS clicksync.redeemers
(
    publication_id UInt64,
    block_number UInt64,
    tx_hash FixedString(32),
    tx_order UInt32,
    raw_purpose_tag UInt8,
    purpose Enum8(
        'spend' = 1,
        'mint' = 2,
        'reward' = 3,
        'certificate' = 4,
        'vote' = 5,
        'proposal' = 6
    ),
    redeemer_index UInt32,
    data_cbor String CODEC(ZSTD(3)),
    data_byte_length UInt32,
    data_hash FixedString(32),
    ex_units_memory UInt64,
    ex_units_steps UInt64,
    is_applied Bool,
    target_tx_hash Nullable(FixedString(32)),
    target_output_index Nullable(UInt32),
    target_policy_id Nullable(FixedString(28)),
    target_reward_account Nullable(String),
    target_body_ordinal Nullable(UInt32),
    target_identity Nullable(String),
    resolved_script_hash Nullable(FixedString(28)),
    INDEX publication_idx publication_id TYPE minmax GRANULARITY 1,
    CONSTRAINT redeemers_data_length CHECK data_byte_length = length(data_cbor)
)
ENGINE = MergeTree
PARTITION BY intDiv(block_number, 1000000)
ORDER BY (tx_hash, publication_id, purpose, redeemer_index);

CREATE TABLE IF NOT EXISTS clicksync.transaction_metadata
(
    publication_id UInt64,
    block_number UInt64,
    tx_hash FixedString(32),
    tx_order UInt32,
    labels Array(UInt64),
    metadata_cbor String CODEC(ZSTD(3)),
    byte_length UInt32,
    content_hash FixedString(32),
    INDEX publication_idx publication_id TYPE minmax GRANULARITY 1,
    CONSTRAINT transaction_metadata_length CHECK byte_length = length(metadata_cbor)
)
ENGINE = MergeTree
PARTITION BY intDiv(block_number, 1000000)
ORDER BY (tx_hash, publication_id);

CREATE TABLE IF NOT EXISTS clicksync.chain_events
(
    event_seq UInt64,
    publication_id UInt64,
    event_kind Enum8('adoption' = 1, 'invalidation' = 2),
    active Bool,
    rollback_id Nullable(UUID),
    block_hash FixedString(32),
    slot UInt64,
    block_number UInt64,
    is_byron_ebb Bool,
    writer_id UUID,
    recorded_at DateTime64(6, 'UTC'),
    PROJECTION chain_events_by_publication
    (
        SELECT publication_id, event_seq, event_kind, active, rollback_id, is_byron_ebb, _part_offset
        ORDER BY (publication_id, event_seq)
    ),
    CONSTRAINT chain_events_kind CHECK
        (event_kind = 'adoption' AND active AND isNull(rollback_id))
        OR
        (event_kind = 'invalidation' AND NOT active AND isNotNull(rollback_id))
)
ENGINE = MergeTree
PARTITION BY intDiv(event_seq, 1000000)
ORDER BY (event_seq, event_kind, publication_id);

CREATE TABLE IF NOT EXISTS clicksync.rollbacks
(
    rollback_id UUID,
    event_seq UInt64,
    rollback_to_origin Bool,
    rollback_to_slot Nullable(UInt64),
    rollback_to_hash Nullable(FixedString(32)),
    rollback_to_block_number Nullable(UInt64),
    rollback_to_is_byron_ebb Bool,
    old_tip_slot Nullable(UInt64),
    old_tip_hash Nullable(FixedString(32)),
    old_tip_block_number Nullable(UInt64),
    old_tip_is_byron_ebb Bool,
    old_tip_event_seq UInt64,
    depth UInt32,
    relay_hosts Array(String),
    relay_addresses Array(String),
    relay_operators Array(String),
    reason String,
    writer_id UUID,
    recorded_at DateTime64(6, 'UTC'),
    CONSTRAINT rollbacks_target CHECK
        (rollback_to_origin AND isNull(rollback_to_slot) AND isNull(rollback_to_hash) AND isNull(rollback_to_block_number) AND NOT rollback_to_is_byron_ebb)
        OR
        (NOT rollback_to_origin AND isNotNull(rollback_to_slot) AND isNotNull(rollback_to_hash) AND isNotNull(rollback_to_block_number)),
    CONSTRAINT rollbacks_old_tip CHECK
        (isNull(old_tip_slot) AND isNull(old_tip_hash) AND isNull(old_tip_block_number) AND NOT old_tip_is_byron_ebb)
        OR
        (isNotNull(old_tip_slot) AND isNotNull(old_tip_hash) AND isNotNull(old_tip_block_number)),
    CONSTRAINT rollbacks_relay_arrays CHECK
        length(relay_hosts) = length(relay_addresses)
        AND length(relay_hosts) = length(relay_operators)
)
ENGINE = MergeTree
PARTITION BY intDiv(event_seq, 1000000)
ORDER BY (event_seq, rollback_id);
