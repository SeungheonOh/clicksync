-- Returns the total lovelace in currently unspent effective-chain UTxOs
-- represented by this dataset. When dataset_manifest.complete_history = false
-- (as in the current Alonzo-start run), this is not the complete mainnet UTxO total.
WITH
    (
        SELECT argMax((effective_event_seq, complete_history), revision)
        FROM clicksync.dataset_manifest
        WHERE manifest_key = 1
    ) AS latest_manifest,
    tupleElement(latest_manifest, 1) AS snapshot_event,
    tupleElement(latest_manifest, 2) AS complete_history,
    (
        SELECT argMax(publication_id, (event_seq, publication_id))
        FROM clicksync.chain_events
        WHERE event_kind = 'adoption'
          AND event_seq <= snapshot_event
    ) AS publication_watermark,
    committed_membership AS
    (
        SELECT publication_id, event_seq, active
        FROM clicksync.chain_events
        WHERE event_kind = 'adoption'
          AND event_seq <= snapshot_event

        UNION ALL

        SELECT ce.publication_id, ce.event_seq, ce.active
        FROM clicksync.chain_events AS ce
        INNER JOIN clicksync.rollbacks AS rb
            ON rb.rollback_id = assumeNotNull(ce.rollback_id)
           AND rb.event_seq = ce.event_seq
        WHERE ce.event_kind = 'invalidation'
          AND ce.event_seq <= snapshot_event
    ),
    active_publications AS
    (
        SELECT publication_id
        FROM committed_membership
        GROUP BY publication_id
        HAVING argMax(active, event_seq) = 1
    ),
    active_outputs AS
    (
        SELECT
            o.tx_hash,
            o.output_index,
            o.lovelace
        FROM clicksync.outputs AS o
        INNER JOIN active_publications AS ap USING (publication_id)
        WHERE o.publication_id <= publication_watermark
    ),
    active_spends AS
    (
        SELECT DISTINCT
            i.source_tx_hash,
            i.source_output_index
        FROM clicksync.inputs AS i
        INNER JOIN active_publications AS ap USING (publication_id)
        WHERE i.publication_id <= publication_watermark
          AND i.is_consumed
    )
SELECT
    snapshot_event,
    publication_watermark,
    complete_history,
    count() AS unspent_output_count,
    sum(toUInt128(o.lovelace)) AS total_lovelace
FROM active_outputs AS o
LEFT ANTI JOIN active_spends AS s
    ON s.source_tx_hash = o.tx_hash
   AND s.source_output_index = o.output_index;
