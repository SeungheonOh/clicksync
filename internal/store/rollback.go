package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

const insertInvalidationsSQL = `INSERT INTO clicksync.chain_events
(
    event_seq, publication_id, event_kind, active, rollback_id, block_hash,
    slot, block_number, is_byron_ebb, writer_id, recorded_at
)`

const invalidationReadbackSQL = `
SELECT
    publication_id, event_kind, active, rollback_id, block_hash, slot,
    block_number, is_byron_ebb, writer_id, recorded_at
FROM clicksync.chain_events
WHERE event_seq = ?
  AND event_kind = 'invalidation'
ORDER BY publication_id`

const insertRollbackSQL = `INSERT INTO clicksync.rollbacks
(
    rollback_id, event_seq, rollback_to_origin, rollback_to_slot,
    rollback_to_hash, rollback_to_block_number, rollback_to_is_byron_ebb,
    old_tip_slot, old_tip_hash, old_tip_block_number, old_tip_is_byron_ebb,
    old_tip_event_seq, depth, relay_hosts, relay_addresses, relay_operators,
    reason, writer_id, recorded_at
)`

const rollbackReadbackSQL = `
SELECT
    rollback_id,
    rollback_to_origin,
    rollback_to_slot,
    rollback_to_hash,
    rollback_to_block_number,
    rollback_to_is_byron_ebb,
    old_tip_slot,
    old_tip_hash,
    old_tip_block_number,
    old_tip_is_byron_ebb,
    old_tip_event_seq,
    depth,
    relay_hosts,
    relay_addresses,
    relay_operators,
    reason,
    writer_id,
    recorded_at
FROM clicksync.rollbacks
WHERE event_seq = ?
ORDER BY rollback_id`

type membershipReadback uint8

const (
	membershipAbsent membershipReadback = iota
	membershipPartial
	membershipComplete
)

func (d *DB) Rollback(
	ctx context.Context,
	lock Lock,
	request RollbackRequest,
) (RollbackCommit, error) {
	if lock == nil {
		return RollbackCommit{}, errors.New("rollback requires the writer lock")
	}
	if request.MaximumDepth == 0 {
		return RollbackCommit{}, fmt.Errorf(
			"%w: maximum rollback depth must be non-zero",
			ErrInvalidRollback,
		)
	}
	if strings.TrimSpace(request.Reason) == "" {
		return RollbackCommit{}, fmt.Errorf(
			"%w: rollback reason is empty",
			ErrInvalidRollback,
		)
	}
	if err := validateRelays(request.Relays); err != nil {
		return RollbackCommit{}, err
	}

	d.operationMu.Lock()
	defer d.operationMu.Unlock()
	if err := lock.AssertHeld(); err != nil {
		return RollbackCommit{}, fmt.Errorf("rollback writer lock is not held: %w", err)
	}
	state, err := d.State(ctx, request.MaximumDepth)
	if err != nil {
		return RollbackCommit{}, err
	}
	target, descendants, err := resolveRollback(state, request)
	if err != nil {
		return RollbackCommit{}, err
	}
	if target == state.Tip {
		return RollbackCommit{
			To:             target,
			OldTip:         state.Tip,
			OldTipEventSeq: state.Snapshot,
			Committed:      true,
			Noop:           true,
		}, nil
	}

	_, allocator, err := d.initializedStore()
	if err != nil {
		return RollbackCommit{}, err
	}
	eventSeq, err := allocator.reserveEvents(1)
	if err != nil {
		return RollbackCommit{}, err
	}
	record := rollback{
		RollbackID:     uuid.New(),
		EventSeq:       eventSeq,
		To:             target,
		OldTip:         state.Tip,
		OldTipEventSeq: state.Snapshot,
		Descendants:    append([]CanonicalBlock(nil), descendants...),
		Relays:         append([]Relay(nil), request.Relays...),
		Reason:         request.Reason,
		WriterID:       d.writerID,
		RecordedAt:     d.now(),
	}
	commit := rollbackResult(record)

	invalidationErr := d.insertInvalidations(ctx, record)
	if invalidationErr != nil {
		readbackCtx, cancel := uncertaintyContext(ctx)
		status, readbackErr := d.invalidationsCommitted(readbackCtx, record)
		cancel()
		switch {
		case readbackErr != nil:
			return commit, errors.Join(
				ErrNotCommitted,
				fmt.Errorf("rollback invalidation insert: %w", invalidationErr),
				fmt.Errorf("rollback invalidation exact readback: %w", readbackErr),
			)
		case status != membershipComplete:
			return commit, errors.Join(
				ErrNotCommitted,
				fmt.Errorf("rollback invalidation insert: %w", invalidationErr),
				fmt.Errorf(
					"rollback invalidations read back as %s",
					status,
				),
			)
		default:
			commit.ResolvedUncertain = true
		}
	}
	if err := lock.AssertHeld(); err != nil {
		return commit, fmt.Errorf(
			"rollback writer lock was lost before header: %w",
			err,
		)
	}
	headerErr := d.insertRollbackHeader(ctx, record)
	if headerErr == nil {
		commit.Committed = true
		return commit, nil
	}

	readbackCtx, cancel := uncertaintyContext(ctx)
	defer cancel()
	headerFound, readbackErr := d.rollbackHeaderCommitted(readbackCtx, record)
	switch {
	case readbackErr != nil:
		if errors.Is(readbackErr, ErrCommitConflict) {
			return commit, errors.Join(headerErr, readbackErr)
		}
		return commit, errors.Join(
			ErrCommitIndeterminate,
			fmt.Errorf("rollback header insert: %w", headerErr),
			fmt.Errorf("rollback header exact readback: %w", readbackErr),
		)
	case !headerFound:
		return commit, errors.Join(
			ErrNotCommitted,
			fmt.Errorf("rollback header insert: %w", headerErr),
		)
	}
	status, membershipErr := d.invalidationsCommitted(readbackCtx, record)
	if membershipErr != nil {
		return commit, errors.Join(
			ErrCommitIndeterminate,
			fmt.Errorf("rollback header insert: %w", headerErr),
			fmt.Errorf("committed rollback membership readback: %w", membershipErr),
		)
	}
	if status != membershipComplete {
		return commit, fmt.Errorf(
			"%w: rollback header exists with %s invalidation membership",
			ErrCommitConflict,
			status,
		)
	}
	commit.Committed = true
	commit.ResolvedUncertain = true
	return commit, nil
}

func resolveRollback(
	state State,
	request RollbackRequest,
) (Point, []CanonicalBlock, error) {
	target, targetIndex, targetInWindow, err := resolveRollbackTarget(
		state,
		request.To,
	)
	if err != nil {
		return Point{}, nil, err
	}
	if target == state.Tip {
		return target, nil, nil
	}
	if targetInWindow {
		if uint32(targetIndex) > request.MaximumDepth {
			return Point{}, nil, fmt.Errorf(
				"%w: rollback depth %d exceeds maximum %d",
				ErrInvalidRollback,
				targetIndex,
				request.MaximumDepth,
			)
		}
		return target, append([]CanonicalBlock(nil), state.Canonical[:targetIndex]...), nil
	}
	// A partial-history boundary or Origin is not necessarily represented by
	// a publication. State loads maximumDepth+1 rows, so a longer result proves
	// that this terminal target is too deep.
	if target != state.Dataset.Start {
		return Point{}, nil, fmt.Errorf(
			"%w: target is not on the canonical chain",
			ErrInvalidRollback,
		)
	}
	if len(state.Canonical) > int(request.MaximumDepth) {
		return Point{}, nil, fmt.Errorf(
			"%w: rollback to dataset start exceeds maximum depth %d",
			ErrInvalidRollback,
			request.MaximumDepth,
		)
	}
	return target, append([]CanonicalBlock(nil), state.Canonical...), nil
}

func resolveRollbackTarget(
	state State,
	requested Point,
) (Point, int, bool, error) {
	if requested.Origin {
		if !state.Dataset.Start.Origin {
			return Point{}, 0, false, fmt.Errorf(
				"%w: rollback below the dataset start is forbidden",
				ErrInvalidRollback,
			)
		}
		return state.Dataset.Start, 0, false, nil
	}
	if requested.Hash == ([32]byte{}) {
		return Point{}, 0, false, fmt.Errorf(
			"%w: rollback target hash is empty",
			ErrInvalidRollback,
		)
	}
	for index, block := range state.Canonical {
		if sameWirePoint(requested, block.Point) {
			if err := suppliedPointMetadataMatches(requested, block.Point); err != nil {
				return Point{}, 0, false, err
			}
			return block.Point, index, true, nil
		}
	}
	if sameWirePoint(requested, state.Dataset.Start) {
		if err := suppliedPointMetadataMatches(requested, state.Dataset.Start); err != nil {
			return Point{}, 0, false, err
		}
		return state.Dataset.Start, 0, false, nil
	}
	return Point{}, 0, false, fmt.Errorf(
		"%w: rollback target is not in the bounded canonical chain",
		ErrInvalidRollback,
	)
}

func sameWirePoint(left, right Point) bool {
	if left.Origin || right.Origin {
		return left.Origin == right.Origin
	}
	return left.Slot == right.Slot && left.Hash == right.Hash
}

func suppliedPointMetadataMatches(supplied, canonical Point) error {
	if supplied.BlockNumber != 0 && supplied.BlockNumber != canonical.BlockNumber {
		return fmt.Errorf(
			"%w: rollback target block number conflicts with canonical metadata",
			ErrInvalidRollback,
		)
	}
	if supplied.IsByronEBB && !canonical.IsByronEBB {
		return fmt.Errorf(
			"%w: rollback target EBB flag conflicts with canonical metadata",
			ErrInvalidRollback,
		)
	}
	return nil
}

func (d *DB) insertInvalidations(
	ctx context.Context,
	record rollback,
) error {
	if len(record.Descendants) == 0 {
		return errors.New("cannot insert empty rollback invalidations")
	}
	batch, err := d.conn.PrepareBatch(ctx, insertInvalidationsSQL)
	if err != nil {
		return err
	}
	for _, descendant := range record.Descendants {
		if err := batch.Append(
			record.EventSeq,
			descendant.PublicationID,
			"invalidation",
			false,
			record.RollbackID,
			bytes32(descendant.Point.Hash),
			descendant.Point.Slot,
			descendant.Point.BlockNumber,
			descendant.Point.IsByronEBB,
			record.WriterID,
			record.RecordedAt,
		); err != nil {
			_ = batch.Abort()
			return err
		}
	}
	return batch.Send()
}

func (d *DB) invalidationsCommitted(
	ctx context.Context,
	record rollback,
) (membershipReadback, error) {
	result, err := d.conn.Query(ctx, invalidationReadbackSQL, record.EventSeq)
	if err != nil {
		return membershipAbsent, err
	}
	defer result.Close()
	expected := make(map[uint64]Point, len(record.Descendants))
	for _, descendant := range record.Descendants {
		if _, duplicate := expected[descendant.PublicationID]; duplicate {
			return membershipAbsent, fmt.Errorf(
				"%w: rollback descendants duplicate publication %d",
				ErrCommitConflict,
				descendant.PublicationID,
			)
		}
		expected[descendant.PublicationID] = descendant.Point
	}
	seen := make(map[uint64]struct{}, len(expected))
	for result.Next() {
		var (
			publicationID uint64
			eventKind     string
			active        bool
			rollbackID    *uuid.UUID
			hashBytes     []byte
			slot          uint64
			blockNumber   uint64
			isByronEBB    bool
			writerID      uuid.UUID
			recordedAt    time.Time
		)
		if err := result.Scan(
			&publicationID,
			&eventKind,
			&active,
			&rollbackID,
			&hashBytes,
			&slot,
			&blockNumber,
			&isByronEBB,
			&writerID,
			&recordedAt,
		); err != nil {
			return membershipAbsent, err
		}
		point, ok := expected[publicationID]
		if !ok || rollbackID == nil {
			return membershipAbsent, fmt.Errorf(
				"%w: rollback invalidation contains an unexpected publication",
				ErrCommitConflict,
			)
		}
		if _, duplicate := seen[publicationID]; duplicate {
			return membershipAbsent, fmt.Errorf(
				"%w: rollback invalidation duplicates a publication",
				ErrCommitConflict,
			)
		}
		if eventKind != "invalidation" ||
			active ||
			*rollbackID != record.RollbackID ||
			!bytes.Equal(hashBytes, point.Hash[:]) ||
			slot != point.Slot ||
			blockNumber != point.BlockNumber ||
			isByronEBB != point.IsByronEBB ||
			writerID != record.WriterID ||
			!recordedAt.Equal(record.RecordedAt) {
			return membershipAbsent, fmt.Errorf(
				"%w: rollback invalidation differs from expected row",
				ErrCommitConflict,
			)
		}
		seen[publicationID] = struct{}{}
	}
	if err := result.Err(); err != nil {
		return membershipAbsent, err
	}
	switch {
	case len(seen) == 0:
		return membershipAbsent, nil
	case len(seen) == len(expected):
		return membershipComplete, nil
	default:
		return membershipPartial, nil
	}
}

func (d *DB) insertRollbackHeader(
	ctx context.Context,
	record rollback,
) error {
	batch, err := d.conn.PrepareBatch(ctx, insertRollbackSQL)
	if err != nil {
		return err
	}
	toSlot, toHash, toNumber := nullablePoint(record.To)
	oldSlot, oldHash, oldNumber := nullablePoint(record.OldTip)
	hosts, addresses, operators := relayArrays(record.Relays)
	if err := batch.Append(
		record.RollbackID,
		record.EventSeq,
		record.To.Origin,
		toSlot,
		toHash,
		toNumber,
		record.To.IsByronEBB,
		oldSlot,
		oldHash,
		oldNumber,
		record.OldTip.IsByronEBB,
		record.OldTipEventSeq,
		uint32(len(record.Descendants)),
		hosts,
		addresses,
		operators,
		record.Reason,
		record.WriterID,
		record.RecordedAt,
	); err != nil {
		_ = batch.Abort()
		return err
	}
	return batch.Send()
}

func (d *DB) rollbackHeaderCommitted(
	ctx context.Context,
	record rollback,
) (bool, error) {
	result, err := d.conn.Query(ctx, rollbackReadbackSQL, record.EventSeq)
	if err != nil {
		return false, err
	}
	defer result.Close()
	found := false
	for result.Next() {
		if found {
			return false, fmt.Errorf(
				"%w: rollback event %d has duplicate headers",
				ErrCommitConflict,
				record.EventSeq,
			)
		}
		found = true
		var (
			rollbackID uuid.UUID
			toOrigin   bool
			toSlot     *uint64
			toHash     []byte
			toNumber   *uint64
			toEBB      bool
			oldSlot    *uint64
			oldHash    []byte
			oldNumber  *uint64
			oldEBB     bool
			oldEvent   uint64
			depth      uint32
			hosts      []string
			addresses  []string
			operators  []string
			reason     string
			writer     uuid.UUID
			recordedAt time.Time
		)
		if err := result.Scan(
			&rollbackID,
			&toOrigin,
			&toSlot,
			&toHash,
			&toNumber,
			&toEBB,
			&oldSlot,
			&oldHash,
			&oldNumber,
			&oldEBB,
			&oldEvent,
			&depth,
			&hosts,
			&addresses,
			&operators,
			&reason,
			&writer,
			&recordedAt,
		); err != nil {
			return false, err
		}
		to, err := scanPoint(toOrigin, toSlot, toHash, toNumber, toEBB)
		if err != nil {
			return false, fmt.Errorf("%w: rollback target: %v", ErrCommitConflict, err)
		}
		old, err := scanPoint(oldSlot == nil, oldSlot, oldHash, oldNumber, oldEBB)
		if err != nil {
			return false, fmt.Errorf("%w: rollback old tip: %v", ErrCommitConflict, err)
		}
		expectedHosts, expectedAddresses, expectedOperators := relayArrays(record.Relays)
		if rollbackID != record.RollbackID ||
			to != record.To ||
			old != record.OldTip ||
			oldEvent != record.OldTipEventSeq ||
			depth != uint32(len(record.Descendants)) ||
			!equalStrings(hosts, expectedHosts) ||
			!equalStrings(addresses, expectedAddresses) ||
			!equalStrings(operators, expectedOperators) ||
			reason != record.Reason ||
			writer != record.WriterID ||
			!recordedAt.Equal(record.RecordedAt) {
			return false, fmt.Errorf(
				"%w: rollback header differs from expected row",
				ErrCommitConflict,
			)
		}
	}
	if err := result.Err(); err != nil {
		return false, err
	}
	return found, nil
}

func rollbackResult(record rollback) RollbackCommit {
	return RollbackCommit{
		RollbackID:     record.RollbackID,
		EventSeq:       record.EventSeq,
		To:             record.To,
		OldTip:         record.OldTip,
		OldTipEventSeq: record.OldTipEventSeq,
		Descendants:    append([]CanonicalBlock(nil), record.Descendants...),
	}
}

func nullablePoint(point Point) (any, any, any) {
	if point.Origin {
		return nil, nil, nil
	}
	return point.Slot, bytes32(point.Hash), point.BlockNumber
}

func relayArrays(relays []Relay) ([]string, []string, []string) {
	hosts := make([]string, len(relays))
	addresses := make([]string, len(relays))
	operators := make([]string, len(relays))
	for index, relay := range relays {
		hosts[index] = relay.Host
		addresses[index] = relay.Address
		operators[index] = relay.Operator
	}
	return hosts, addresses, operators
}

func validateRelays(relays []Relay) error {
	if len(relays) < 2 {
		return fmt.Errorf(
			"%w: rollback has fewer than two agreeing relays",
			ErrInvalidRollback,
		)
	}
	for index, relay := range relays {
		if strings.TrimSpace(relay.Host) == "" ||
			strings.TrimSpace(relay.Address) == "" ||
			strings.TrimSpace(relay.Operator) == "" {
			return fmt.Errorf(
				"%w: rollback relay %d identity is incomplete",
				ErrInvalidRollback,
				index,
			)
		}
	}
	return nil
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func (status membershipReadback) String() string {
	switch status {
	case membershipAbsent:
		return "absent"
	case membershipPartial:
		return "partial"
	case membershipComplete:
		return "complete"
	default:
		return "unknown"
	}
}
