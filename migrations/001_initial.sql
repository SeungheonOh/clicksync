CREATE DATABASE IF NOT EXISTS clicksync;

CREATE TABLE IF NOT EXISTS clicksync.dataset_manifest
(
    manifest_key UInt8 DEFAULT 1,
    revision UInt64,
    dataset_id UUID,
    network_magic UInt32,
    network_name String,
    byron_genesis_id FixedString(32),
    byron_genesis_json_hash FixedString(32),
    shelley_genesis_id FixedString(32),
    shelley_genesis_json_hash FixedString(32),
    start_kind Enum8('origin' = 1, 'intersection' = 2),
    start_slot Nullable(UInt64),
    start_hash Nullable(FixedString(32)),
    start_block_number Nullable(UInt64),
    start_is_byron_ebb Bool,
    genesis_seeded Bool,
    complete_history Bool,
    trust_mode String,
    committed_event_seq UInt64,
    committed_tip_origin Bool,
    committed_tip_slot Nullable(UInt64),
    committed_tip_hash Nullable(FixedString(32)),
    committed_tip_block_number Nullable(UInt64),
    committed_tip_is_byron_ebb Bool,
    writer_id Nullable(UUID),
    writer_build String,
    source_build String,
    created_at DateTime64(6, 'UTC'),
    updated_at DateTime64(6, 'UTC'),
    CONSTRAINT dataset_manifest_singleton CHECK manifest_key = 1,
    CONSTRAINT dataset_manifest_trust CHECK trust_mode = 'peer_observed_structurally_verified',
    CONSTRAINT dataset_manifest_start_point CHECK
        (start_kind = 'origin' AND isNull(start_slot) AND isNull(start_hash) AND isNull(start_block_number) AND NOT start_is_byron_ebb)
        OR
        (start_kind = 'intersection' AND isNotNull(start_slot) AND isNotNull(start_hash) AND isNotNull(start_block_number)),
    CONSTRAINT dataset_manifest_tip CHECK
        (committed_tip_origin AND isNull(committed_tip_slot) AND isNull(committed_tip_hash) AND isNull(committed_tip_block_number) AND NOT committed_tip_is_byron_ebb)
        OR
        (NOT committed_tip_origin AND isNotNull(committed_tip_slot) AND isNotNull(committed_tip_hash) AND isNotNull(committed_tip_block_number)),
    CONSTRAINT dataset_manifest_completeness CHECK complete_history = (start_kind = 'origin' AND genesis_seeded)
)
ENGINE = ReplacingMergeTree(revision)
ORDER BY manifest_key;

CREATE TABLE IF NOT EXISTS clicksync.blocks
(
    publication_id UInt64,
    block_hash FixedString(32),
    parent_hash Nullable(FixedString(32)),
    slot UInt64,
    block_number UInt64,
    era String,
    block_type Int16,
    transaction_count UInt32,
    input_count UInt32,
    output_count UInt32,
    datum_observation_count UInt32,
    withdrawal_count UInt32,
    redeemer_count UInt32,
    metadata_count UInt32,
    synthetic Bool,
    source_peer String,
    source_address String,
    source_operator String,
    n2n_version UInt16,
    network_magic UInt32,
    body_hash_verified Bool,
    transaction_hashes_verified Bool,
    facts_digest FixedString(32),
    writer_id UUID,
    observed_at DateTime64(6, 'UTC'),
    inserted_at DateTime64(6, 'UTC'),
    INDEX publication_idx publication_id TYPE minmax GRANULARITY 1,
    CONSTRAINT blocks_structural_verification CHECK synthetic OR (body_hash_verified AND transaction_hashes_verified)
)
ENGINE = MergeTree
PARTITION BY intDiv(block_number, 1000000)
ORDER BY (block_hash, publication_id);

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
    PROJECTION by_event_seq
    (
        SELECT event_seq, event_kind, publication_id, active, rollback_id, is_byron_ebb
        ORDER BY (event_seq, event_kind, publication_id)
    ),
    CONSTRAINT chain_events_kind CHECK
        (event_kind = 'adoption' AND active AND isNull(rollback_id))
        OR
        (event_kind = 'invalidation' AND NOT active AND isNotNull(rollback_id))
)
ENGINE = MergeTree
PARTITION BY intDiv(event_seq, 1000000)
ORDER BY (publication_id, event_seq);

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
    depth UInt32,
    reason String,
    observed_peers Array(String),
    observed_operators Array(String),
    corroboration_required UInt16,
    agreement_group Nullable(UUID),
    writer_id UUID,
    recorded_at DateTime64(6, 'UTC'),
    CONSTRAINT rollbacks_point CHECK
        (rollback_to_origin AND isNull(rollback_to_slot) AND isNull(rollback_to_hash) AND isNull(rollback_to_block_number) AND NOT rollback_to_is_byron_ebb)
        OR
        (NOT rollback_to_origin AND isNotNull(rollback_to_slot) AND isNotNull(rollback_to_hash) AND isNotNull(rollback_to_block_number)),
    CONSTRAINT rollbacks_old_tip CHECK
        (isNull(old_tip_slot) AND isNull(old_tip_hash) AND isNull(old_tip_block_number) AND NOT old_tip_is_byron_ebb)
        OR
        (isNotNull(old_tip_slot) AND isNotNull(old_tip_hash) AND isNotNull(old_tip_block_number)),
    CONSTRAINT rollbacks_peers CHECK
        length(observed_peers) = length(observed_operators)
        AND length(observed_peers) > 0
        AND length(arrayDistinct(observed_operators)) >= corroboration_required
)
ENGINE = MergeTree
PARTITION BY intDiv(event_seq, 1000000)
ORDER BY (event_seq, rollback_id);

CREATE TABLE IF NOT EXISTS clicksync.transactions
(
    publication_id UInt64,
    block_number UInt64,
    tx_hash FixedString(32),
    tx_order UInt32,
    parent_tx_hash Nullable(FixedString(32)),
    subtransaction_index Nullable(UInt32),
    era String,
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
        AND length(mint_policy_ids) = length(mint_quantities),
    CONSTRAINT transactions_mint_nonzero CHECK arrayAll(quantity -> quantity != 0, mint_quantities),
    CONSTRAINT transactions_genesis CHECK
        (flow_kind != 'genesis')
        OR
        (phase2_valid AND isNull(parent_tx_hash) AND isNull(subtransaction_index))
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
    source_is_resolved Bool,
    INDEX publication_idx publication_id TYPE minmax GRANULARITY 1,
    PROJECTION inputs_by_consuming_tx
    (
        SELECT tx_hash, publication_id, body_ordinal, _part_offset
        ORDER BY (tx_hash, publication_id, body_ordinal)
    ),
    CONSTRAINT inputs_reference_not_consumed CHECK role != 'reference' OR NOT is_consumed
)
ENGINE = MergeTree
PARTITION BY intDiv(block_number, 1000000)
ORDER BY (source_tx_hash, source_output_index, publication_id, tx_hash, body_ordinal);

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
    payment_credential_kind Enum8('none' = 0, 'key' = 1, 'script' = 2),
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
    CONSTRAINT outputs_asset_arrays CHECK
        length(asset_policy_ids) = length(asset_names)
        AND length(asset_policy_ids) = length(asset_quantities),
    CONSTRAINT outputs_asset_nonzero CHECK arrayAll(quantity -> quantity != 0, asset_quantities),
    CONSTRAINT outputs_payment_credential CHECK
        (payment_credential_kind = 'none' AND isNull(payment_credential_hash))
        OR
        (payment_credential_kind IN ('key', 'script') AND isNotNull(payment_credential_hash)),
    CONSTRAINT outputs_datum CHECK
        (datum_kind = 'none' AND isNull(datum_hash))
        OR
        (datum_kind IN ('hash', 'inline') AND isNotNull(datum_hash)),
    CONSTRAINT outputs_reference_script CHECK
        (isNull(reference_script_hash) AND isNull(reference_script_language))
        OR
        (isNotNull(reference_script_hash) AND isNotNull(reference_script_language))
)
ENGINE = MergeTree
PARTITION BY intDiv(block_number, 1000000)
ORDER BY (tx_hash, output_index, publication_id);

CREATE TABLE IF NOT EXISTS clicksync.datum_bodies
(
    datum_hash FixedString(32),
    datum_cbor String CODEC(ZSTD(3)),
    byte_length UInt32,
    content_hash FixedString(32),
    first_publication_id UInt64,
    first_seen_at DateTime64(6, 'UTC'),
    INDEX first_publication_idx first_publication_id TYPE minmax GRANULARITY 1,
    CONSTRAINT datum_bodies_length CHECK byte_length = length(datum_cbor),
    CONSTRAINT datum_bodies_content_hash CHECK datum_hash = content_hash
)
ENGINE = MergeTree
ORDER BY (datum_hash, first_publication_id);

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
ORDER BY (datum_hash, publication_id, tx_hash, source_kind, source_ordinal);

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
    credential_kind Enum8('key' = 1, 'script' = 2),
    credential_hash FixedString(28),
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
    resolution_status Enum8('resolved' = 1),
    target_tx_hash Nullable(FixedString(32)),
    target_output_index Nullable(UInt32),
    target_policy_id Nullable(FixedString(28)),
    target_reward_account Nullable(String),
    target_body_ordinal Nullable(UInt32),
    target_identity Nullable(String),
    resolved_script_hash Nullable(FixedString(28)),
    INDEX publication_idx publication_id TYPE minmax GRANULARITY 1,
    CONSTRAINT redeemers_data_length CHECK data_byte_length = length(data_cbor),
    CONSTRAINT redeemers_spend_target CHECK
        purpose != 'spend'
        OR
        (isNotNull(target_tx_hash) AND isNotNull(target_output_index)),
    CONSTRAINT redeemers_mint_target CHECK purpose != 'mint' OR isNotNull(target_policy_id),
    CONSTRAINT redeemers_reward_target CHECK purpose != 'reward' OR isNotNull(target_reward_account),
    CONSTRAINT redeemers_ordinal_target CHECK
        purpose NOT IN ('certificate', 'vote', 'proposal')
        OR
        isNotNull(target_body_ordinal)
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
    CONSTRAINT transaction_metadata_length CHECK byte_length = length(metadata_cbor),
    CONSTRAINT transaction_metadata_labels CHECK labels = arraySort(labels)
)
ENGINE = MergeTree
PARTITION BY intDiv(block_number, 1000000)
ORDER BY (tx_hash, publication_id);

CREATE TABLE IF NOT EXISTS clicksync.peer_observations
(
    observation_id UUID,
    observation_digest FixedString(32),
    observation_kind Enum8(
        'checkpoint' = 1,
        'source_change' = 2,
        'rollback' = 3,
        'disagreement' = 4
    ),
    peer_host String,
    peer_address String,
    operator_label String,
    n2n_version UInt16,
    network_magic UInt32,
    observed_tip_slot UInt64,
    observed_tip_hash FixedString(32),
    observed_tip_block_number UInt64,
    checkpoint_slot Nullable(UInt64),
    checkpoint_hash Nullable(FixedString(32)),
    checkpoint_block_number Nullable(UInt64),
    checkpoint_is_byron_ebb Nullable(Bool),
    agreement_group Nullable(UUID),
    selected_body_source Bool,
    body_hash_verified Bool,
    point_verified Bool,
    parent_verified Bool,
    result Enum8('agreed' = 1, 'disagreed' = 2, 'unavailable' = 3, 'quarantined' = 4),
    reason String,
    observed_at DateTime64(6, 'UTC'),
    CONSTRAINT peer_observations_checkpoint CHECK
        (isNull(checkpoint_slot) AND isNull(checkpoint_hash) AND isNull(checkpoint_block_number) AND isNull(checkpoint_is_byron_ebb))
        OR
        (isNotNull(checkpoint_slot) AND isNotNull(checkpoint_hash) AND isNotNull(checkpoint_block_number) AND isNotNull(checkpoint_is_byron_ebb))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(observed_at)
ORDER BY (observed_tip_hash, observed_at, peer_host, observation_id);

CREATE TABLE IF NOT EXISTS clicksync.writer_audit
(
    dataset_id UUID,
    revision UInt64,
    owner_id UUID,
    state Enum8('active' = 1, 'released' = 2),
    build_id String,
    hostname String,
    process_id UInt32,
    acquired_at DateTime64(6, 'UTC'),
    heartbeat_at DateTime64(6, 'UTC'),
    released_at Nullable(DateTime64(6, 'UTC')),
    release_reason String,
    lock_path String,
    CONSTRAINT writer_audit_time CHECK acquired_at <= heartbeat_at,
    CONSTRAINT writer_audit_state CHECK
        (state = 'active' AND isNull(released_at))
        OR
        (state = 'released' AND isNotNull(released_at))
)
ENGINE = ReplacingMergeTree(revision)
ORDER BY dataset_id;
