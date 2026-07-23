-- Destructive fixture for an otherwise empty, disposable `clicksync`
-- database. It proves the canonical commit/snapshot rule used by Clickout.
TRUNCATE TABLE clicksync.chain_events;
TRUNCATE TABLE clicksync.rollbacks;

INSERT INTO clicksync.chain_events
(
    event_seq,
    publication_id,
    event_kind,
    active,
    rollback_id,
    block_hash,
    slot,
    block_number,
    is_byron_ebb,
    writer_id,
    recorded_at
)
VALUES
(
    1,
    7,
    'adoption',
    true,
    NULL,
    unhex(repeat('11', 32)),
    10,
    10,
    false,
    toUUID('00000000-0000-0000-0000-000000000001'),
    now64(6)
);

INSERT INTO clicksync.chain_events
(
    event_seq,
    publication_id,
    event_kind,
    active,
    rollback_id,
    block_hash,
    slot,
    block_number,
    is_byron_ebb,
    writer_id,
    recorded_at
)
VALUES
(
    2,
    7,
    'invalidation',
    false,
    toUUID('00000000-0000-0000-0000-000000000002'),
    unhex(repeat('11', 32)),
    10,
    10,
    false,
    toUUID('00000000-0000-0000-0000-000000000001'),
    now64(6)
);

SELECT throwIf(
    (
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
    ) != 1,
    'headerless invalidation advanced committed snapshot'
);

SELECT throwIf(
    (
        SELECT argMax(active, event_seq)
        FROM
        (
            SELECT publication_id, event_seq, active
            FROM clicksync.chain_events
            WHERE event_seq <= 2
              AND event_kind = 'adoption'
            UNION ALL
            SELECT ce.publication_id, ce.event_seq, ce.active
            FROM clicksync.chain_events AS ce
            INNER JOIN clicksync.rollbacks AS rb
                ON ce.rollback_id = rb.rollback_id
               AND ce.event_seq = rb.event_seq
            WHERE ce.event_seq <= 2
              AND ce.event_kind = 'invalidation'
        )
        WHERE publication_id = 7
    ) != true,
    'headerless invalidation changed active membership'
);

INSERT INTO clicksync.rollbacks
(
    rollback_id,
    event_seq,
    rollback_to_origin,
    rollback_to_slot,
    rollback_to_hash,
    rollback_to_block_number,
    rollback_to_is_byron_ebb,
    old_tip_slot,
    old_tip_hash,
    old_tip_block_number,
    old_tip_is_byron_ebb,
    depth,
    reason,
    observed_peers,
    observed_operators,
    corroboration_required,
    agreement_group,
    writer_id,
    recorded_at
)
VALUES
(
    toUUID('00000000-0000-0000-0000-000000000002'),
    2,
    true,
    NULL,
    NULL,
    NULL,
    false,
    10,
    unhex(repeat('11', 32)),
    10,
    false,
    1,
    'contract fixture',
    ['peer-a', 'peer-b'],
    ['operator-a', 'operator-b'],
    2,
    toUUID('00000000-0000-0000-0000-000000000003'),
    toUUID('00000000-0000-0000-0000-000000000001'),
    now64(6)
);

SELECT throwIf(
    (
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
    ) != 2,
    'rollback header did not advance committed snapshot'
);

SELECT throwIf(
    (
        SELECT argMax(active, event_seq)
        FROM
        (
            SELECT publication_id, event_seq, active
            FROM clicksync.chain_events
            WHERE event_seq <= 2
              AND event_kind = 'adoption'
            UNION ALL
            SELECT ce.publication_id, ce.event_seq, ce.active
            FROM clicksync.chain_events AS ce
            INNER JOIN clicksync.rollbacks AS rb
                ON ce.rollback_id = rb.rollback_id
               AND ce.event_seq = rb.event_seq
            WHERE ce.event_seq <= 2
              AND ce.event_kind = 'invalidation'
        )
        WHERE publication_id = 7
    ) != false,
    'committed rollback did not invalidate publication'
);
