package clickhouse

const snapshotTipSQL = `
SELECT max(event_seq)
FROM
(
    SELECT event_seq
    FROM chain_events
    WHERE event_kind = 'adoption'

    UNION ALL

    SELECT event_seq
    FROM rollbacks
)`

const snapshotAtBlockSQL = `
SELECT count(), max(ce.event_seq)
FROM chain_events AS ce
INNER JOIN blocks AS b ON ce.publication_id = b.publication_id
WHERE ce.event_kind = 'adoption'
  AND b.block_hash = ?`

const snapshotPinnedSQL = `
SELECT
    max(committed_tip),
    countIf(event_seq = ?)
FROM
(
    SELECT event_seq, max(event_seq) OVER () AS committed_tip
    FROM
    (
        SELECT event_seq
        FROM chain_events
        WHERE event_kind = 'adoption'

        UNION ALL

        SELECT event_seq
        FROM rollbacks
    )
)`

const manifestSQL = `
SELECT
    count(),
    argMax(complete_history, revision),
    argMax(trust_mode, revision)
FROM dataset_manifest
WHERE manifest_key = 1`

const activePublicationsCTE = `
WITH committed_membership AS
(
    SELECT publication_id, event_seq, active
    FROM chain_events
    WHERE event_seq <= ?
      AND event_kind = 'adoption'

    UNION ALL

    SELECT ce.publication_id, ce.event_seq, ce.active
    FROM chain_events AS ce
    INNER JOIN rollbacks AS rb
        ON ce.rollback_id = rb.rollback_id
       AND ce.event_seq = rb.event_seq
    WHERE ce.event_seq <= ?
      AND ce.event_kind = 'invalidation'
),
active_publications AS
(
    SELECT publication_id
    FROM committed_membership
    GROUP BY publication_id
    HAVING argMax(active, event_seq)
)`

const outputColumns = `
    o.tx_hash,
    o.output_index,
    b.block_hash,
    o.block_number,
    o.output_kind,
    o.address,
    o.lovelace,
    o.asset_policy_ids,
    o.asset_names,
    o.asset_quantities,
    o.datum_kind,
    o.datum_hash,
    o.reference_script_hash,
    o.reference_script_language`

const outputByRefSQL = activePublicationsCTE + `
SELECT` + outputColumns + `
FROM outputs AS o
INNER JOIN active_publications AS ap ON o.publication_id = ap.publication_id
INNER JOIN blocks AS b ON o.publication_id = b.publication_id
WHERE o.tx_hash = ?
  AND o.output_index = ?
LIMIT 2`

const spendByRefSQL = activePublicationsCTE + `
SELECT DISTINCT i.tx_hash
FROM inputs AS i
INNER JOIN active_publications AS ap ON i.publication_id = ap.publication_id
WHERE i.source_tx_hash = ?
  AND i.source_output_index = ?
  AND i.is_consumed
LIMIT 2`

const transactionHeaderSQL = activePublicationsCTE + `
SELECT
    t.tx_hash,
    b.block_hash,
    t.block_number,
    t.tx_order,
    t.parent_tx_hash,
    t.subtransaction_index,
    t.era,
    t.phase2_valid,
    t.flow_kind,
    t.declared_fee_lovelace,
    t.effective_fee_lovelace,
    t.mint_is_applied,
    t.mint_policy_ids,
    t.mint_asset_names,
    t.mint_quantities
FROM transactions AS t
INNER JOIN active_publications AS ap ON t.publication_id = ap.publication_id
INNER JOIN blocks AS b ON t.publication_id = b.publication_id
WHERE t.tx_hash = ?
LIMIT 2`

const inputsByTxSQL = activePublicationsCTE + `
SELECT
    i.source_tx_hash,
    i.source_output_index,
    i.tx_hash,
    i.role,
    i.body_ordinal,
    i.is_consumed,
    i.source_is_resolved
FROM inputs AS i
INNER JOIN active_publications AS ap ON i.publication_id = ap.publication_id
WHERE i.tx_hash = ?
ORDER BY i.body_ordinal`

const outputsByTxSQL = activePublicationsCTE + `
SELECT` + outputColumns + `
FROM outputs AS o
INNER JOIN active_publications AS ap ON o.publication_id = ap.publication_id
INNER JOIN blocks AS b ON o.publication_id = b.publication_id
WHERE o.tx_hash = ?
ORDER BY o.body_ordinal, o.output_index`

const datumBodySQL = `
SELECT
    argMin(datum_cbor, (first_publication_id, first_seen_at)),
    argMin(byte_length, (first_publication_id, first_seen_at)),
    argMin(content_hash, (first_publication_id, first_seen_at)),
    uniqExact((datum_cbor, byte_length, content_hash))
FROM datum_bodies
WHERE datum_hash = ?
GROUP BY datum_hash`

const datumObservationsSQL = activePublicationsCTE + `
SELECT
    d.datum_hash,
    d.tx_hash,
    b.block_hash,
    d.source_kind
FROM datum_observations AS d
INNER JOIN active_publications AS ap ON d.publication_id = ap.publication_id
INNER JOIN blocks AS b ON d.publication_id = b.publication_id
WHERE d.datum_hash = ?
ORDER BY d.block_number, d.tx_order, d.source_kind, d.source_ordinal`

const withdrawalsByTxSQL = activePublicationsCTE + `
SELECT
    w.tx_hash,
    w.reward_account,
    w.lovelace,
    w.body_ordinal,
    w.is_applied,
    w.credential_hash
FROM withdrawals AS w
INNER JOIN active_publications AS ap ON w.publication_id = ap.publication_id
WHERE w.tx_hash = ?
ORDER BY w.body_ordinal`

const metadataByTxSQL = activePublicationsCTE + `
SELECT
    m.tx_hash,
    m.labels,
    m.metadata_cbor,
    m.byte_length,
    m.content_hash
FROM transaction_metadata AS m
INNER JOIN active_publications AS ap ON m.publication_id = ap.publication_id
WHERE m.tx_hash = ?
LIMIT 2`

const redeemersByTxSQL = activePublicationsCTE + `
SELECT
    r.tx_hash,
    r.raw_purpose_tag,
    r.purpose,
    r.redeemer_index,
    r.data_cbor,
    r.data_byte_length,
    r.data_hash,
    r.ex_units_memory,
    r.ex_units_steps,
    r.is_applied,
    r.resolution_status,
    r.target_tx_hash,
    r.target_output_index,
    r.target_policy_id,
    r.target_reward_account,
    r.target_body_ordinal,
    r.target_identity,
    r.resolved_script_hash
FROM redeemers AS r
INNER JOIN active_publications AS ap ON r.publication_id = ap.publication_id
WHERE r.tx_hash = ?
ORDER BY r.purpose, r.redeemer_index`
