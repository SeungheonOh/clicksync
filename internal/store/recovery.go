package store

import (
	"context"
	"errors"
	"fmt"
)

const latestAdoptionSQL = `
SELECT event_seq, publication_id, block_hash, slot, block_number, is_byron_ebb
FROM clicksync.chain_events
PREWHERE event_kind = 'adoption'
ORDER BY event_seq DESC, publication_id
LIMIT 2`

const latestRollbackSQL = `
SELECT
    event_seq,
    rollback_to_origin,
    rollback_to_slot,
    rollback_to_hash,
    rollback_to_block_number,
    rollback_to_is_byron_ebb
FROM clicksync.rollbacks
ORDER BY event_seq DESC, rollback_id
LIMIT 2`

const canonicalWindowSQL = `
SELECT
    publication_id,
    tupleElement(latest, 1) AS event_seq,
    tupleElement(latest, 3) AS block_hash,
    tupleElement(latest, 4) AS slot,
    tupleElement(latest, 5) AS block_number,
    tupleElement(latest, 6) AS is_byron_ebb
FROM
(
    SELECT
        publication_id,
        argMax(
            tuple(event_seq, active, block_hash, slot, block_number, is_byron_ebb),
            event_seq
        ) AS latest
    FROM
    (
        SELECT
            publication_id, event_seq, active, block_hash, slot, block_number,
            is_byron_ebb
        FROM clicksync.chain_events
        WHERE event_kind = 'adoption'
          AND event_seq <= ?

        UNION ALL

        SELECT
            ce.publication_id, ce.event_seq, ce.active, ce.block_hash, ce.slot,
            ce.block_number, ce.is_byron_ebb
        FROM clicksync.chain_events AS ce
        INNER JOIN clicksync.rollbacks AS rb
            ON rb.rollback_id = ce.rollback_id
           AND rb.event_seq = ce.event_seq
        WHERE ce.event_kind = 'invalidation'
          AND ce.event_seq <= ?
    )
    GROUP BY publication_id
)
WHERE tupleElement(latest, 2)
ORDER BY event_seq DESC, publication_id
LIMIT ?`

type latestAction struct {
	found       bool
	eventSeq    uint64
	publication uint64
	point       Point
	kind        string
}

func (d *DB) State(ctx context.Context, maximumDepth uint32) (State, error) {
	if maximumDepth == 0 {
		return State{}, errors.New("canonical window depth must be non-zero")
	}
	identity, err := d.initializedIdentity()
	if err != nil {
		return State{}, err
	}
	return d.stateForIdentity(ctx, identity, maximumDepth)
}

// Inspect reconstructs read-only status without initializing a dataset or
// allocator. The boolean is false only when no dataset row exists.
func (d *DB) Inspect(
	ctx context.Context,
	maximumDepth uint32,
) (State, bool, error) {
	if d == nil || d.conn == nil {
		return State{}, false, errorsNewNilDB()
	}
	if maximumDepth == 0 {
		return State{}, false, errors.New("canonical window depth must be non-zero")
	}
	identities, err := d.loadDataset(ctx)
	if err != nil {
		return State{}, false, err
	}
	if len(identities) == 0 {
		return State{}, false, nil
	}
	identity := identities[0]
	for _, replay := range identities[1:] {
		if !sameDatasetIdentity(identity, replay) {
			return State{}, false, errors.New(
				"dataset contains conflicting immutable identities",
			)
		}
	}
	if err := identityUsesCurrentSchema(identity); err != nil {
		return State{}, false, err
	}
	state, err := d.stateForIdentity(ctx, identity, maximumDepth)
	if err != nil {
		return State{}, false, err
	}
	return state, true, nil
}

func (d *DB) stateForIdentity(
	ctx context.Context,
	identity DatasetIdentity,
	maximumDepth uint32,
) (State, error) {
	action, err := d.latestCommittedAction(ctx)
	if err != nil {
		return State{}, err
	}
	state := State{
		Dataset: identity,
		Tip:     identity.Start,
	}
	if action.found {
		state.Snapshot = action.eventSeq
		state.Tip = action.point
	}
	state.Canonical, err = d.loadCanonicalWindow(
		ctx,
		state.Snapshot,
		uint64(maximumDepth)+1,
	)
	if err != nil {
		return State{}, err
	}
	if err := validateRecoveredState(state); err != nil {
		return State{}, err
	}
	state.Intersections = intersectionCandidates(state)
	return state, nil
}

func (d *DB) CommittedSnapshot(ctx context.Context) (uint64, error) {
	if _, err := d.initializedIdentity(); err != nil {
		return 0, err
	}
	action, err := d.latestCommittedAction(ctx)
	if err != nil || !action.found {
		return 0, err
	}
	return action.eventSeq, nil
}

func (d *DB) CurrentTip(ctx context.Context) (Point, error) {
	identity, err := d.initializedIdentity()
	if err != nil {
		return Point{}, err
	}
	action, err := d.latestCommittedAction(ctx)
	if err != nil {
		return Point{}, err
	}
	if !action.found {
		return identity.Start, nil
	}
	return action.point, nil
}

func (d *DB) Intersections(
	ctx context.Context,
	maximumDepth uint32,
) ([]Point, error) {
	state, err := d.State(ctx, maximumDepth)
	if err != nil {
		return nil, err
	}
	return append([]Point(nil), state.Intersections...), nil
}

func (d *DB) initializedIdentity() (DatasetIdentity, error) {
	if d == nil {
		return DatasetIdentity{}, errorsNewNilDB()
	}
	d.initializeMu.Lock()
	defer d.initializeMu.Unlock()
	if d.identity == nil || d.allocator == nil {
		return DatasetIdentity{}, errors.New("store is not initialized")
	}
	return *d.identity, nil
}

func (d *DB) latestCommittedAction(ctx context.Context) (latestAction, error) {
	adoption, err := d.latestAdoption(ctx)
	if err != nil {
		return latestAction{}, err
	}
	rollback, err := d.latestRollback(ctx)
	if err != nil {
		return latestAction{}, err
	}
	if adoption.found && rollback.found && adoption.eventSeq == rollback.eventSeq {
		return latestAction{}, fmt.Errorf(
			"%w: event %d is both an adoption and rollback",
			ErrCommitConflict,
			adoption.eventSeq,
		)
	}
	if rollback.found && (!adoption.found || rollback.eventSeq > adoption.eventSeq) {
		return rollback, nil
	}
	return adoption, nil
}

func (d *DB) latestAdoption(ctx context.Context) (latestAction, error) {
	result, err := d.conn.Query(ctx, latestAdoptionSQL)
	if err != nil {
		return latestAction{}, fmt.Errorf("read latest adoption: %w", err)
	}
	defer result.Close()
	var latest latestAction
	for result.Next() {
		var (
			action    latestAction
			hashBytes []byte
		)
		if err := result.Scan(
			&action.eventSeq,
			&action.publication,
			&hashBytes,
			&action.point.Slot,
			&action.point.BlockNumber,
			&action.point.IsByronEBB,
		); err != nil {
			return latestAction{}, fmt.Errorf("scan latest adoption: %w", err)
		}
		hash, err := hash32(hashBytes)
		if err != nil {
			return latestAction{}, fmt.Errorf("latest adoption hash: %w", err)
		}
		action.found = true
		action.kind = "adoption"
		action.point.Hash = hash
		if !validPoint(action.point) || action.eventSeq == 0 ||
			action.publication == 0 {
			return latestAction{}, fmt.Errorf(
				"%w: latest adoption has invalid identity",
				ErrCommitConflict,
			)
		}
		if !latest.found {
			latest = action
			continue
		}
		if action.eventSeq == latest.eventSeq {
			return latestAction{}, fmt.Errorf(
				"%w: duplicate latest adoption event %d",
				ErrCommitConflict,
				action.eventSeq,
			)
		}
		break
	}
	if err := result.Err(); err != nil {
		return latestAction{}, fmt.Errorf("iterate latest adoption: %w", err)
	}
	return latest, nil
}

func (d *DB) latestRollback(ctx context.Context) (latestAction, error) {
	result, err := d.conn.Query(ctx, latestRollbackSQL)
	if err != nil {
		return latestAction{}, fmt.Errorf("read latest rollback: %w", err)
	}
	defer result.Close()
	var latest latestAction
	for result.Next() {
		var (
			action      latestAction
			origin      bool
			slot        *uint64
			hashBytes   []byte
			blockNumber *uint64
			isByronEBB  bool
		)
		if err := result.Scan(
			&action.eventSeq,
			&origin,
			&slot,
			&hashBytes,
			&blockNumber,
			&isByronEBB,
		); err != nil {
			return latestAction{}, fmt.Errorf("scan latest rollback: %w", err)
		}
		point, err := scanPoint(
			origin,
			slot,
			hashBytes,
			blockNumber,
			isByronEBB,
		)
		if err != nil {
			return latestAction{}, fmt.Errorf("latest rollback target: %w", err)
		}
		action.found = true
		action.kind = "rollback"
		action.point = point
		if action.eventSeq == 0 {
			return latestAction{}, fmt.Errorf(
				"%w: latest rollback has zero event identity",
				ErrCommitConflict,
			)
		}
		if !latest.found {
			latest = action
			continue
		}
		if action.eventSeq == latest.eventSeq {
			return latestAction{}, fmt.Errorf(
				"%w: duplicate latest rollback event %d",
				ErrCommitConflict,
				action.eventSeq,
			)
		}
		break
	}
	if err := result.Err(); err != nil {
		return latestAction{}, fmt.Errorf("iterate latest rollback: %w", err)
	}
	return latest, nil
}

func (d *DB) loadCanonicalWindow(
	ctx context.Context,
	snapshot uint64,
	limit uint64,
) ([]CanonicalBlock, error) {
	if snapshot == 0 {
		return nil, nil
	}
	result, err := d.conn.Query(
		ctx,
		canonicalWindowSQL,
		snapshot,
		snapshot,
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("read canonical rollback window: %w", err)
	}
	defer result.Close()
	window := make([]CanonicalBlock, 0, limit)
	seenPublications := make(map[uint64]struct{})
	for result.Next() {
		var (
			block     CanonicalBlock
			hashBytes []byte
		)
		if err := result.Scan(
			&block.PublicationID,
			&block.EventSeq,
			&hashBytes,
			&block.Point.Slot,
			&block.Point.BlockNumber,
			&block.Point.IsByronEBB,
		); err != nil {
			return nil, fmt.Errorf("scan canonical rollback window: %w", err)
		}
		hash, err := hash32(hashBytes)
		if err != nil {
			return nil, fmt.Errorf("canonical block hash: %w", err)
		}
		block.Point.Hash = hash
		if block.PublicationID == 0 || block.EventSeq == 0 ||
			block.EventSeq > snapshot || !validPoint(block.Point) {
			return nil, fmt.Errorf(
				"%w: canonical membership row has invalid identity",
				ErrCommitConflict,
			)
		}
		if _, duplicate := seenPublications[block.PublicationID]; duplicate {
			return nil, fmt.Errorf(
				"%w: canonical membership duplicates publication %d",
				ErrCommitConflict,
				block.PublicationID,
			)
		}
		if len(window) > 0 && block.EventSeq >= window[len(window)-1].EventSeq {
			return nil, fmt.Errorf(
				"%w: canonical membership is not newest-first",
				ErrCommitConflict,
			)
		}
		seenPublications[block.PublicationID] = struct{}{}
		window = append(window, block)
	}
	if err := result.Err(); err != nil {
		return nil, fmt.Errorf("iterate canonical rollback window: %w", err)
	}
	return window, nil
}

func validateRecoveredState(state State) error {
	if state.Snapshot == 0 {
		if len(state.Canonical) != 0 || state.Tip != state.Dataset.Start {
			return fmt.Errorf(
				"%w: empty snapshot differs from dataset start",
				ErrCommitConflict,
			)
		}
		return nil
	}
	if len(state.Canonical) == 0 {
		if state.Tip != state.Dataset.Start {
			return fmt.Errorf(
				"%w: committed tip has no active canonical publication",
				ErrCommitConflict,
			)
		}
		return nil
	}
	if state.Canonical[0].Point != state.Tip {
		return fmt.Errorf(
			"%w: latest committed action tip differs from active canonical tip",
			ErrCommitConflict,
		)
	}
	return nil
}

func intersectionCandidates(state State) []Point {
	const dense = 32
	const geometricSamples = 32
	candidates := make([]Point, 0, dense+geometricSamples+1)
	seen := make(map[Point]struct{})
	appendPoint := func(point Point) {
		if _, duplicate := seen[point]; duplicate {
			return
		}
		seen[point] = struct{}{}
		candidates = append(candidates, point)
	}
	for index := 0; index < len(state.Canonical) && index < dense; index++ {
		appendPoint(state.Canonical[index].Point)
	}
	for distance, sample := dense*2, 0; distance <= len(state.Canonical) && sample < geometricSamples; distance, sample = distance*2, sample+1 {
		appendPoint(state.Canonical[distance-1].Point)
	}
	if len(candidates) == 0 && state.Tip != state.Dataset.Start {
		appendPoint(state.Tip)
	}
	appendPoint(state.Dataset.Start)
	return candidates
}
