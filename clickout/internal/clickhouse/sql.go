package clickhouse

// targetedFactSQL deliberately finds the small fact set first. Only the
// publication IDs represented by those candidates are looked up in the
// append-only event log. This avoids an O(all chain history) active-publication
// CTE on every point read and BFS frontier expansion.
func targetedFactSQL(candidate, result string) string {
	return `
WITH
    toUInt64(?) AS snapshot_event,
    toUInt64(?) AS publication_watermark,
    fact_candidates AS
    (
` + candidate + `
    ),
    candidate_publications AS
    (
        SELECT DISTINCT publication_id
        FROM fact_candidates
    ),
    candidate_blocks AS
    (
        SELECT
            publication_id,
            block_hash,
            block_number,
            era,
            transaction_count,
            synthetic,
            toUInt8(1) AS present
        FROM blocks
        WHERE publication_id IN
            (SELECT publication_id FROM candidate_publications)
          AND publication_id <= publication_watermark
    ),
    candidate_invalidations AS
    (
        SELECT
            ce.publication_id,
            ce.event_seq,
            ce.active,
            assumeNotNull(ce.rollback_id) AS rollback_id
        FROM chain_events AS ce
        WHERE ce.publication_id IN
            (SELECT publication_id FROM candidate_publications)
          AND ce.event_seq <= snapshot_event
          AND ce.event_kind = 'invalidation'
    ),
    candidate_rollback_headers AS
    (
        SELECT rb.rollback_id, rb.event_seq
        FROM rollbacks AS rb
        WHERE (rb.rollback_id, rb.event_seq) IN
        (
            SELECT rollback_id, event_seq
            FROM candidate_invalidations
        )
    ),
    committed_candidate_membership AS
    (
        SELECT ce.publication_id, ce.event_seq, ce.active
        FROM chain_events AS ce
        WHERE ce.publication_id IN
            (SELECT publication_id FROM candidate_publications)
          AND ce.event_seq <= snapshot_event
          AND ce.event_kind = 'adoption'

        UNION ALL

        SELECT ci.publication_id, ci.event_seq, ci.active
        FROM candidate_rollback_headers AS rb
        INNER JOIN candidate_invalidations AS ci
            ON rb.rollback_id = ci.rollback_id
           AND rb.event_seq = ci.event_seq
    ),
    active_candidate_publications AS
    (
        SELECT publication_id
        FROM committed_candidate_membership
        GROUP BY publication_id
        HAVING argMax(active, event_seq) = 1
    )
` + result
}

const outputColumns = `
    o.tx_hash,
    o.output_index,
    o.body_ordinal,
    ifNull(b.block_hash, toFixedString('', 32)),
    o.block_number,
    o.output_kind,
    o.address,
    o.payment_credential_kind,
    o.payment_credential_hash,
    o.lovelace,
    o.asset_policy_ids,
    o.asset_names,
    o.asset_quantities,
    o.datum_kind,
    o.datum_hash,
    o.reference_script_hash,
    o.reference_script_language`

var outputByRefSQL = targetedFactSQL(`
        SELECT *
        FROM outputs
        WHERE tx_hash = ?
          AND output_index = ?
          AND publication_id <= publication_watermark
`, `
SELECT`+outputColumns+`,
    toUInt8(
        ifNull(b.present, toUInt8(0)) = 1
        AND ifNull(b.block_number, toUInt64(0)) = o.block_number
        AND o.tx_order < ifNull(b.transaction_count, toUInt32(0))
    ),
    ifNull(b.era, ''),
    ifNull(b.synthetic, false)
FROM fact_candidates AS o
INNER JOIN active_candidate_publications AS ap
    ON o.publication_id = ap.publication_id
LEFT JOIN candidate_blocks AS b ON o.publication_id = b.publication_id
LIMIT 2`)

var usesByRefSQL = targetedFactSQL(`
        SELECT *
        FROM inputs
        WHERE source_tx_hash = ?
          AND source_output_index = ?
          AND publication_id <= publication_watermark
`, `
SELECT
    i.source_tx_hash,
    i.source_output_index,
    i.tx_hash,
    ifNull(b.block_hash, toFixedString('', 32)),
    i.block_number,
    i.role,
    i.body_ordinal,
    i.is_consumed,
    i.source_is_resolved,
    toUInt8(
        ifNull(b.present, toUInt8(0)) = 1
        AND ifNull(b.block_number, toUInt64(0)) = i.block_number
        AND i.tx_order < ifNull(b.transaction_count, toUInt32(0))
    )
FROM fact_candidates AS i
INNER JOIN active_candidate_publications AS ap
    ON i.publication_id = ap.publication_id
LEFT JOIN candidate_blocks AS b ON i.publication_id = b.publication_id
ORDER BY
    i.block_number,
    i.tx_hash,
    multiIf(i.role = 'regular', 0, i.role = 'collateral', 1, i.role = 'reference', 2, 3),
    i.body_ordinal,
    i.source_tx_hash,
    i.source_output_index,
    i.publication_id
LIMIT 10001`)

var spendByRefSQL = targetedFactSQL(`
        SELECT *
        FROM inputs
        WHERE source_tx_hash = ?
          AND source_output_index = ?
          AND is_consumed
          AND publication_id <= publication_watermark
`, `
SELECT DISTINCT
    i.tx_hash,
    toUInt8(
        ifNull(b.present, toUInt8(0)) = 1
        AND ifNull(b.block_number, toUInt64(0)) = i.block_number
        AND i.tx_order < ifNull(b.transaction_count, toUInt32(0))
    )
FROM fact_candidates AS i
INNER JOIN active_candidate_publications AS ap
    ON i.publication_id = ap.publication_id
LEFT JOIN candidate_blocks AS b ON i.publication_id = b.publication_id
ORDER BY i.tx_hash
LIMIT 2`)

const datumBodySQL = `
SELECT
    argMin(datum_cbor, (first_publication_id, first_seen_at)),
    argMin(byte_length, (first_publication_id, first_seen_at)),
    argMin(content_hash, (first_publication_id, first_seen_at)),
    uniqExact((datum_cbor, byte_length, content_hash))
FROM datum_bodies
WHERE datum_hash = ?
GROUP BY datum_hash`

var datumObservationsSQL = targetedFactSQL(`
        SELECT *
        FROM datum_observations
        WHERE datum_hash = ?
          AND publication_id <= publication_watermark
`, `
SELECT
    d.datum_hash,
    d.tx_hash,
    ifNull(b.block_hash, toFixedString('', 32)),
    d.source_kind,
    toUInt8(
        ifNull(b.present, toUInt8(0)) = 1
        AND ifNull(b.block_number, toUInt64(0)) = d.block_number
        AND d.tx_order < ifNull(b.transaction_count, toUInt32(0))
    ),
    ifNull(b.era, ''),
    ifNull(b.synthetic, false)
FROM fact_candidates AS d
INNER JOIN active_candidate_publications AS ap
    ON d.publication_id = ap.publication_id
LEFT JOIN candidate_blocks AS b ON d.publication_id = b.publication_id
ORDER BY d.block_number, d.tx_order, d.source_kind, d.source_ordinal`)
