CREATE DATABASE IF NOT EXISTS clicksync;

CREATE TABLE IF NOT EXISTS clicksync.dataset_manifest
(
    manifest_key UInt8 DEFAULT 1,
    revision UInt64,
    transition_id UUID,
    transition_kind String,
    previous_row_digest Nullable(FixedString(32)),
    row_digest FixedString(32),
    dataset_id UUID,
    schema_contract_hash FixedString(32),
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
    trust_status Enum8(
        'agreed' = 1,
        'checking' = 2,
        'unavailable' = 3,
        'disputed' = 4
    ),
    trust_basis Enum8(
        'official_genesis' = 1,
        'sampled_peer' = 2,
        'partial_boundary' = 3,
        'primary_only' = 4
    ),
    check_id Nullable(UUID),
    agreement_group Nullable(UUID),
    check_attempt UInt32,
    corroboration_required UInt16,
    corroboration_confirmed UInt16,
    checkpoint_interval UInt64,
    primary_suffix UInt64,
    disagreement Bool,
    trust_reason String,
    check_started_at Nullable(DateTime64(6, 'UTC')),
    check_completed_at Nullable(DateTime64(6, 'UTC')),
    evidence_state Enum8('none' = 1, 'open' = 2, 'frozen' = 3),
    evidence_count UInt32,
    evidence_digest Nullable(FixedString(32)),
    pending_evidence_observation_id Nullable(UUID),
    pending_evidence_observation_digest Nullable(FixedString(32)),
    pending_evidence_payload String,
    pending_evidence_writer_id Nullable(UUID),
    pending_evidence_reserved_at Nullable(DateTime64(6, 'UTC')),
    checked_event_seq Nullable(UInt64),
    checked_point_origin Nullable(Bool),
    checked_point_slot Nullable(UInt64),
    checked_point_hash Nullable(FixedString(32)),
    checked_point_block_number Nullable(UInt64),
    checked_point_is_byron_ebb Nullable(Bool),
    last_agreed_event_seq Nullable(UInt64),
    last_agreed_point_origin Nullable(Bool),
    last_agreed_point_slot Nullable(UInt64),
    last_agreed_point_hash Nullable(FixedString(32)),
    last_agreed_point_block_number Nullable(UInt64),
    last_agreed_point_is_byron_ebb Nullable(Bool),
    last_agreed_at Nullable(DateTime64(6, 'UTC')),
    last_agreed_check_id Nullable(UUID),
    last_agreed_agreement_group Nullable(UUID),
    last_agreed_check_attempt UInt32,
    last_agreed_corroboration_required UInt16,
    last_agreed_corroboration_confirmed UInt16,
    last_agreed_checked_event_seq Nullable(UInt64),
    last_agreed_checked_point_origin Nullable(Bool),
    last_agreed_checked_point_slot Nullable(UInt64),
    last_agreed_checked_point_hash Nullable(FixedString(32)),
    last_agreed_checked_point_block_number Nullable(UInt64),
    last_agreed_checked_point_is_byron_ebb Nullable(Bool),
    last_agreed_evidence_count UInt32,
    last_agreed_evidence_digest Nullable(FixedString(32)),
    servable_floor_event_seq UInt64,
    servable_floor_origin Bool,
    servable_floor_slot Nullable(UInt64),
    servable_floor_hash Nullable(FixedString(32)),
    servable_floor_block_number Nullable(UInt64),
    servable_floor_is_byron_ebb Bool,
    servable_floor_permanent Bool,
    physical_event_seq UInt64,
    physical_tip_origin Bool,
    physical_tip_slot Nullable(UInt64),
    physical_tip_hash Nullable(FixedString(32)),
    physical_tip_block_number Nullable(UInt64),
    physical_tip_is_byron_ebb Bool,
    effective_event_seq UInt64,
    effective_tip_origin Bool,
    effective_tip_slot Nullable(UInt64),
    effective_tip_hash Nullable(FixedString(32)),
    effective_tip_block_number Nullable(UInt64),
    effective_tip_is_byron_ebb Bool,
    servable Bool,
    visibility_generation UInt64,
    pending_rollback_state Enum8(
        'none' = 0,
        'reserved' = 1,
        'invalidations_written' = 2
    ),
    pending_rollback_id Nullable(UUID),
    pending_rollback_event_seq Nullable(UInt64),
    pending_rollback_to_origin Nullable(Bool),
    pending_rollback_to_slot Nullable(UInt64),
    pending_rollback_to_hash Nullable(FixedString(32)),
    pending_rollback_to_block_number Nullable(UInt64),
    pending_rollback_to_is_byron_ebb Nullable(Bool),
    pending_rollback_old_physical_event_seq Nullable(UInt64),
    pending_rollback_old_physical_origin Nullable(Bool),
    pending_rollback_old_physical_slot Nullable(UInt64),
    pending_rollback_old_physical_hash Nullable(FixedString(32)),
    pending_rollback_old_physical_block_number Nullable(UInt64),
    pending_rollback_old_physical_is_byron_ebb Nullable(Bool),
    pending_rollback_depth Nullable(UInt32),
    pending_rollback_reason String,
    pending_rollback_observed_peers Array(String),
    pending_rollback_observed_operators Array(String),
    pending_rollback_required Nullable(UInt16),
    pending_rollback_check_id Nullable(UUID),
    pending_rollback_agreement_group Nullable(UUID),
    pending_rollback_check_attempt Nullable(UInt32),
    pending_rollback_checked_event_seq Nullable(UInt64),
    pending_rollback_evidence_count Nullable(UInt32),
    pending_rollback_evidence_digest Nullable(FixedString(32)),
    pending_rollback_writer_id Nullable(UUID),
    pending_rollback_started_at Nullable(DateTime64(6, 'UTC')),
    writer_id Nullable(UUID),
    writer_build String,
    source_build String,
    created_at DateTime64(6, 'UTC'),
    updated_at DateTime64(6, 'UTC'),
    CONSTRAINT dataset_manifest_singleton CHECK manifest_key = 1,
    CONSTRAINT dataset_manifest_revision CHECK revision > 0,
    CONSTRAINT dataset_manifest_trust CHECK trust_mode = 'peer_observed_structurally_verified',
    CONSTRAINT dataset_manifest_start_point CHECK
        (start_kind = 'origin' AND isNull(start_slot) AND isNull(start_hash) AND isNull(start_block_number) AND NOT start_is_byron_ebb)
        OR
        (start_kind = 'intersection' AND isNotNull(start_slot) AND isNotNull(start_hash) AND isNotNull(start_block_number)),
    CONSTRAINT dataset_manifest_checked_point CHECK
        multiIf(
            isNull(checked_point_origin),
                isNull(checked_event_seq) AND isNull(checked_point_slot) AND isNull(checked_point_hash) AND isNull(checked_point_block_number) AND isNull(checked_point_is_byron_ebb),
            isNotNull(checked_point_origin) AND assumeNotNull(checked_point_origin),
                isNotNull(checked_event_seq) AND isNull(checked_point_slot) AND isNull(checked_point_hash) AND isNull(checked_point_block_number) AND isNotNull(checked_point_is_byron_ebb) AND NOT assumeNotNull(checked_point_is_byron_ebb),
            isNotNull(checked_event_seq) AND isNotNull(checked_point_slot) AND isNotNull(checked_point_hash) AND isNotNull(checked_point_block_number) AND isNotNull(checked_point_is_byron_ebb)
        ),
    CONSTRAINT dataset_manifest_last_agreed_point CHECK
        multiIf(
            isNull(last_agreed_point_origin),
                isNull(last_agreed_event_seq) AND isNull(last_agreed_point_slot) AND isNull(last_agreed_point_hash) AND isNull(last_agreed_point_block_number) AND isNull(last_agreed_point_is_byron_ebb) AND isNull(last_agreed_at),
            isNotNull(last_agreed_point_origin) AND assumeNotNull(last_agreed_point_origin),
                isNotNull(last_agreed_event_seq) AND isNull(last_agreed_point_slot) AND isNull(last_agreed_point_hash) AND isNull(last_agreed_point_block_number) AND isNotNull(last_agreed_point_is_byron_ebb) AND NOT assumeNotNull(last_agreed_point_is_byron_ebb) AND isNotNull(last_agreed_at),
            isNotNull(last_agreed_event_seq) AND isNotNull(last_agreed_point_slot) AND isNotNull(last_agreed_point_hash) AND isNotNull(last_agreed_point_block_number) AND isNotNull(last_agreed_point_is_byron_ebb) AND isNotNull(last_agreed_at)
        ),
    CONSTRAINT dataset_manifest_last_agreed_evidence CHECK
        (
            isNull(last_agreed_check_id)
            AND isNull(last_agreed_agreement_group)
            AND last_agreed_check_attempt = 0
            AND last_agreed_corroboration_required = 0
            AND last_agreed_corroboration_confirmed = 0
            AND isNull(last_agreed_checked_event_seq)
            AND isNull(last_agreed_checked_point_origin)
            AND isNull(last_agreed_checked_point_slot)
            AND isNull(last_agreed_checked_point_hash)
            AND isNull(last_agreed_checked_point_block_number)
            AND isNull(last_agreed_checked_point_is_byron_ebb)
            AND last_agreed_evidence_count = 0
            AND isNull(last_agreed_evidence_digest)
            AND (
                isNull(last_agreed_event_seq)
                OR
                (
                    servable_floor_permanent
                    AND servable_floor_origin
                    AND assumeNotNull(last_agreed_event_seq) = servable_floor_event_seq
                    AND assumeNotNull(last_agreed_point_origin)
                )
            )
        )
        OR
        (
            isNotNull(last_agreed_event_seq)
            AND isNotNull(last_agreed_check_id)
            AND isNotNull(last_agreed_agreement_group)
            AND last_agreed_check_attempt > 0
            AND last_agreed_corroboration_required >= 2
            AND last_agreed_corroboration_confirmed >= last_agreed_corroboration_required
            AND last_agreed_corroboration_confirmed <= last_agreed_evidence_count
            AND isNotNull(last_agreed_checked_event_seq)
            AND isNotNull(last_agreed_checked_point_origin)
            AND isNotNull(last_agreed_checked_point_is_byron_ebb)
            AND isNotNull(last_agreed_evidence_digest)
        ),
    CONSTRAINT dataset_manifest_floor_point CHECK
        (servable_floor_origin AND isNull(servable_floor_slot) AND isNull(servable_floor_hash) AND isNull(servable_floor_block_number) AND NOT servable_floor_is_byron_ebb)
        OR
        (NOT servable_floor_origin AND isNotNull(servable_floor_slot) AND isNotNull(servable_floor_hash) AND isNotNull(servable_floor_block_number)),
    CONSTRAINT dataset_manifest_physical_tip CHECK
        (physical_tip_origin AND isNull(physical_tip_slot) AND isNull(physical_tip_hash) AND isNull(physical_tip_block_number) AND NOT physical_tip_is_byron_ebb)
        OR
        (NOT physical_tip_origin AND isNotNull(physical_tip_slot) AND isNotNull(physical_tip_hash) AND isNotNull(physical_tip_block_number)),
    CONSTRAINT dataset_manifest_effective_tip CHECK
        (effective_tip_origin AND isNull(effective_tip_slot) AND isNull(effective_tip_hash) AND isNull(effective_tip_block_number) AND NOT effective_tip_is_byron_ebb)
        OR
        (NOT effective_tip_origin AND isNotNull(effective_tip_slot) AND isNotNull(effective_tip_hash) AND isNotNull(effective_tip_block_number)),
    CONSTRAINT dataset_manifest_pending_rollback CHECK
        multiIf(
            pending_rollback_state = 'none',
                isNull(pending_rollback_id)
                AND isNull(pending_rollback_event_seq)
                AND isNull(pending_rollback_to_origin)
                AND isNull(pending_rollback_to_slot)
                AND isNull(pending_rollback_to_hash)
                AND isNull(pending_rollback_to_block_number)
                AND isNull(pending_rollback_to_is_byron_ebb)
                AND isNull(pending_rollback_old_physical_event_seq)
                AND isNull(pending_rollback_old_physical_origin)
                AND isNull(pending_rollback_old_physical_slot)
                AND isNull(pending_rollback_old_physical_hash)
            AND isNull(pending_rollback_old_physical_block_number)
            AND isNull(pending_rollback_old_physical_is_byron_ebb)
            AND isNull(pending_rollback_depth)
            AND pending_rollback_reason = ''
            AND empty(pending_rollback_observed_peers)
            AND empty(pending_rollback_observed_operators)
            AND isNull(pending_rollback_required)
            AND isNull(pending_rollback_check_id)
            AND isNull(pending_rollback_agreement_group)
            AND isNull(pending_rollback_check_attempt)
            AND isNull(pending_rollback_checked_event_seq)
            AND isNull(pending_rollback_evidence_count)
            AND isNull(pending_rollback_evidence_digest)
            AND isNull(pending_rollback_writer_id)
            AND isNull(pending_rollback_started_at),
            isNotNull(pending_rollback_id)
            AND isNotNull(pending_rollback_event_seq)
            AND isNotNull(pending_rollback_to_origin)
            AND multiIf(
                assumeNotNull(pending_rollback_to_origin),
                    isNull(pending_rollback_to_slot)
                    AND isNull(pending_rollback_to_hash)
                    AND isNull(pending_rollback_to_block_number)
                    AND isNotNull(pending_rollback_to_is_byron_ebb)
                    AND NOT assumeNotNull(pending_rollback_to_is_byron_ebb),
                isNotNull(pending_rollback_to_slot)
                    AND isNotNull(pending_rollback_to_hash)
                    AND isNotNull(pending_rollback_to_block_number)
                    AND isNotNull(pending_rollback_to_is_byron_ebb)
            )
            AND isNotNull(pending_rollback_old_physical_event_seq)
            AND isNotNull(pending_rollback_old_physical_origin)
            AND multiIf(
                assumeNotNull(pending_rollback_old_physical_origin),
                    isNull(pending_rollback_old_physical_slot)
                    AND isNull(pending_rollback_old_physical_hash)
                    AND isNull(pending_rollback_old_physical_block_number)
                    AND isNotNull(pending_rollback_old_physical_is_byron_ebb)
                    AND NOT assumeNotNull(pending_rollback_old_physical_is_byron_ebb),
                isNotNull(pending_rollback_old_physical_slot)
                    AND isNotNull(pending_rollback_old_physical_hash)
                    AND isNotNull(pending_rollback_old_physical_block_number)
                    AND isNotNull(pending_rollback_old_physical_is_byron_ebb)
            )
            AND isNotNull(pending_rollback_depth)
            AND pending_rollback_reason != ''
            AND evidence_state = 'frozen'
            AND length(pending_rollback_observed_peers) = length(pending_rollback_observed_operators)
            AND isNotNull(pending_rollback_required)
            AND assumeNotNull(pending_rollback_required) >= 2
            AND length(arrayDistinct(arrayMap(item -> lowerUTF8(trim(item)), pending_rollback_observed_operators))) >= assumeNotNull(pending_rollback_required)
            AND isNotNull(pending_rollback_check_id)
            AND isNotNull(pending_rollback_agreement_group)
            AND isNotNull(pending_rollback_check_attempt)
            AND assumeNotNull(pending_rollback_check_attempt) > 0
            AND isNotNull(pending_rollback_checked_event_seq)
            AND isNotNull(pending_rollback_evidence_count)
            AND assumeNotNull(pending_rollback_evidence_count) = evidence_count
            AND assumeNotNull(pending_rollback_evidence_count) >= assumeNotNull(pending_rollback_required)
            AND isNotNull(pending_rollback_evidence_digest)
            AND assumeNotNull(pending_rollback_evidence_digest) = assumeNotNull(evidence_digest)
            AND isNotNull(pending_rollback_writer_id)
            AND isNotNull(pending_rollback_started_at)
        ),
    CONSTRAINT dataset_manifest_check_identity CHECK
        (trust_status != 'checking')
        OR
        (isNotNull(check_id) AND isNotNull(agreement_group) AND isNotNull(checked_event_seq) AND isNotNull(checked_point_origin) AND isNotNull(check_started_at)),
    CONSTRAINT dataset_manifest_evidence_state CHECK
        (
            evidence_state = 'none'
            AND evidence_count = 0
            AND isNull(evidence_digest)
            AND isNull(pending_evidence_observation_id)
            AND isNull(pending_evidence_observation_digest)
            AND pending_evidence_payload = ''
            AND isNull(pending_evidence_writer_id)
            AND isNull(pending_evidence_reserved_at)
        )
        OR
        (
            evidence_state IN ('open', 'frozen')
            AND isNotNull(check_id)
            AND isNotNull(agreement_group)
            AND isNotNull(evidence_digest)
            AND (
                (
                    isNull(pending_evidence_observation_id)
                    AND isNull(pending_evidence_observation_digest)
                    AND pending_evidence_payload = ''
                    AND isNull(pending_evidence_writer_id)
                    AND isNull(pending_evidence_reserved_at)
                )
                OR
                (
                    evidence_state = 'open'
                    AND isNotNull(pending_evidence_observation_id)
                    AND isNotNull(pending_evidence_observation_digest)
                    AND pending_evidence_payload != ''
                    AND isNotNull(pending_evidence_writer_id)
                    AND isNotNull(pending_evidence_reserved_at)
                )
            )
        ),
    CONSTRAINT dataset_manifest_corroboration CHECK corroboration_confirmed <= evidence_count,
    CONSTRAINT dataset_manifest_cadence CHECK checkpoint_interval = 512 AND primary_suffix <= 767,
    CONSTRAINT dataset_manifest_completeness CHECK complete_history = (start_kind = 'origin' AND genesis_seeded)
)
ENGINE = MergeTree
ORDER BY (manifest_key, revision)
SETTINGS index_granularity = 64;

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
    PROJECTION blocks_by_publication
    (
        SELECT publication_id, block_hash, _part_offset
        ORDER BY publication_id
    ),
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
ORDER BY (event_kind, event_seq, publication_id);

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
    reason String,
    observed_peers Array(String),
    observed_operators Array(String),
    corroboration_required UInt16,
    check_id UUID,
    agreement_group Nullable(UUID),
    check_attempt UInt32,
    checked_event_seq UInt64,
    evidence_count UInt32,
    evidence_digest FixedString(32),
    writer_id UUID,
    recorded_at DateTime64(6, 'UTC'),
    PROJECTION rollbacks_by_id
    (
        SELECT rollback_id, event_seq, _part_offset
        ORDER BY (rollback_id, event_seq)
    ),
    CONSTRAINT rollbacks_point CHECK
        (rollback_to_origin AND isNull(rollback_to_slot) AND isNull(rollback_to_hash) AND isNull(rollback_to_block_number) AND NOT rollback_to_is_byron_ebb)
        OR
        (NOT rollback_to_origin AND isNotNull(rollback_to_slot) AND isNotNull(rollback_to_hash) AND isNotNull(rollback_to_block_number)),
    CONSTRAINT rollbacks_old_tip CHECK
        (isNull(old_tip_slot) AND isNull(old_tip_hash) AND isNull(old_tip_block_number) AND NOT old_tip_is_byron_ebb)
        OR
        (isNotNull(old_tip_slot) AND isNotNull(old_tip_hash) AND isNotNull(old_tip_block_number)),
    CONSTRAINT rollbacks_check_identity CHECK
        check_id != toUUID('00000000-0000-0000-0000-000000000000')
        AND isNotNull(agreement_group)
        AND assumeNotNull(agreement_group) != toUUID('00000000-0000-0000-0000-000000000000')
        AND check_attempt > 0
        AND corroboration_required >= 2
        AND evidence_count >= corroboration_required,
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
    PROJECTION outputs_by_payment_credential
    (
        SELECT payment_credential_kind, payment_credential_hash, _part_offset
        ORDER BY (payment_credential_kind, payment_credential_hash)
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
    evidence_identity FixedString(32),
    observation_kind Enum8(
        'checkpoint' = 1,
        'source_change' = 2,
        'rollback' = 3,
        'disagreement' = 4
    ),
    peer_host String,
    peer_address String,
    operator_label String,
    operator_key String MATERIALIZED lowerUTF8(trim(operator_label)),
    n2n_version UInt16,
    network_magic UInt32,
    observed_tip_slot UInt64,
    observed_tip_hash FixedString(32),
    observed_tip_block_number UInt64,
    checkpoint_slot Nullable(UInt64),
    checkpoint_hash Nullable(FixedString(32)),
    checkpoint_block_number Nullable(UInt64),
    checkpoint_is_byron_ebb Nullable(Bool),
    check_id UUID,
    agreement_group UUID,
    check_attempt UInt32,
    evidence_ordinal UInt32,
    proof_method Enum8(
        'none' = 1,
        'chain_sync_singleton' = 2,
        'boundary_singleton_block_fetch' = 3,
        'follow_block_fetch' = 4,
        'paired_chain_sync_singleton' = 5
    ),
    corroboration_required UInt16,
    checked_event_seq UInt64,
    checked_point_origin Bool,
    checked_point_slot Nullable(UInt64),
    checked_point_hash Nullable(FixedString(32)),
    checked_point_block_number Nullable(UInt64),
    checked_point_is_byron_ebb Bool,
    selected_body_source Bool,
    body_hash_verified Bool,
    point_verified Bool,
    parent_verified Bool,
    result Enum8('agreed' = 1, 'disagreed' = 2, 'unavailable' = 3, 'quarantined' = 4),
    reason String,
    observed_at DateTime64(6, 'UTC'),
    PROJECTION peer_observations_by_logical_id
    (
        SELECT check_id, observation_id, evidence_ordinal, observation_digest, _part_offset
        ORDER BY (check_id, observation_id, evidence_ordinal)
    ),
    CONSTRAINT peer_observations_checkpoint CHECK
        (isNull(checkpoint_slot) AND isNull(checkpoint_hash) AND isNull(checkpoint_block_number) AND isNull(checkpoint_is_byron_ebb))
        OR
        (isNotNull(checkpoint_slot) AND isNotNull(checkpoint_hash) AND isNotNull(checkpoint_block_number) AND isNotNull(checkpoint_is_byron_ebb)),
    CONSTRAINT peer_observations_check_identity CHECK
        check_id != toUUID('00000000-0000-0000-0000-000000000000')
        AND agreement_group != toUUID('00000000-0000-0000-0000-000000000000')
        AND check_attempt > 0
        AND corroboration_required >= 2
        AND operator_key != ''
        AND multiIf(
            observation_kind = 'source_change', evidence_ordinal = 0,
            evidence_ordinal > 0
        )
        AND multiIf(
            observation_kind = 'source_change',
                proof_method = 'none'
                AND result IN ('unavailable', 'quarantined'),
            observation_kind = 'checkpoint',
                multiIf(
                    proof_method = 'chain_sync_singleton', true,
                    proof_method = 'boundary_singleton_block_fetch',
                        result = 'agreed' AND body_hash_verified,
                    proof_method = 'follow_block_fetch',
                        result = 'agreed'
                        AND selected_body_source
                        AND body_hash_verified,
                    false
                ),
            observation_kind = 'disagreement',
                proof_method = 'chain_sync_singleton'
                AND result != 'agreed',
            observation_kind = 'rollback',
                multiIf(
                    proof_method = 'paired_chain_sync_singleton', true,
                    proof_method = 'follow_block_fetch',
                        result = 'agreed'
                        AND selected_body_source
                        AND body_hash_verified,
                    false
                ),
            false
        ),
    CONSTRAINT peer_observations_proof_flags CHECK
        multiIf(
            proof_method = 'none',
                NOT selected_body_source
                AND NOT body_hash_verified
                AND NOT point_verified
                AND NOT parent_verified,
            proof_method IN ('chain_sync_singleton', 'paired_chain_sync_singleton'),
                multiIf(
                    result = 'agreed',
                        NOT selected_body_source
                        AND NOT body_hash_verified
                        AND point_verified
                        AND NOT parent_verified,
                    NOT selected_body_source
                    AND NOT body_hash_verified
                    AND NOT point_verified
                    AND NOT parent_verified
                ),
            proof_method = 'boundary_singleton_block_fetch',
                result = 'agreed'
                AND body_hash_verified
                AND point_verified
                AND NOT parent_verified,
            proof_method = 'follow_block_fetch',
                result = 'agreed'
                AND selected_body_source
                AND body_hash_verified
                AND point_verified
                AND NOT parent_verified,
            false
        )
        AND NOT (observation_kind = 'disagreement' AND result = 'agreed'),
    CONSTRAINT peer_observations_checked_point CHECK
        (
            checked_point_origin
            AND isNull(checked_point_slot)
            AND isNull(checked_point_hash)
            AND isNull(checked_point_block_number)
            AND NOT checked_point_is_byron_ebb
        )
        OR
        (
            NOT checked_point_origin
            AND isNotNull(checked_point_slot)
            AND isNotNull(checked_point_hash)
            AND isNotNull(checked_point_block_number)
        )
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(observed_at)
ORDER BY (check_id, evidence_ordinal, observation_id);

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
