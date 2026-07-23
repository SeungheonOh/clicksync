package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"

	pcommon "github.com/blinklabs-io/gouroboros/protocol/common"

	"clicksync/internal/n2n"
	"clicksync/internal/publication"
)

const (
	denseIntersectionEvents     = uint64(32)
	maxGeometricIntersectionAge = 32
)

type intersectionEvent struct {
	eventSeq      uint64
	publicationID uint64
	point         publication.Point
}

// IntersectionCandidates returns a bounded dense recent tail plus
// geometrically older committed points. Every fact-backed point is resolved
// against committed membership only for the bounded publication ID set.
// Origin or the exact partial-history boundary is appended as the terminal
// candidate without scanning block history.
func (d *DB) IntersectionCandidates(ctx context.Context) ([]n2n.ChainPoint, error) {
	record, found, err := d.loadAuthoritativeManifest(ctx)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, errors.New("dataset manifest is not initialized")
	}
	snapshot := record.Effective.EventSeq
	identity := manifestIdentityFromRecord(record)
	tip, err := d.committedTip(ctx, snapshot)
	if err != nil {
		return nil, err
	}
	if snapshot == 0 {
		return terminalIntersection(identity.Start), nil
	}

	events, err := d.recentIntersectionEvents(ctx, snapshot)
	if err != nil {
		return nil, err
	}
	for distance, samples := denseIntersectionEvents, 0; distance < snapshot && samples < maxGeometricIntersectionAge; distance, samples = distance*2, samples+1 {
		target := snapshot - distance
		event, found, err := d.intersectionEventAtOrBefore(ctx, target)
		if err != nil {
			return nil, err
		}
		if found {
			events = append(events, event)
		}
		if distance > ^uint64(0)/2 {
			break
		}
	}

	byPublication := make(map[uint64]intersectionEvent, len(events))
	publicationIDs := make([]uint64, 0, len(events))
	for _, event := range events {
		previous, exists := byPublication[event.publicationID]
		if exists && previous.eventSeq >= event.eventSeq {
			continue
		}
		if !exists {
			publicationIDs = append(publicationIDs, event.publicationID)
		}
		byPublication[event.publicationID] = event
	}
	activeIDs, err := d.activeCandidatePublications(ctx, snapshot, publicationIDs)
	if err != nil {
		return nil, err
	}

	points := make([]intersectionEvent, 0, len(activeIDs)+2)
	if !tip.Origin {
		points = append(points, intersectionEvent{eventSeq: snapshot, point: tip})
	}
	for _, publicationID := range activeIDs {
		event, ok := byPublication[publicationID]
		if !ok || event.point.Slot == 0 {
			continue
		}
		points = append(points, event)
	}
	sort.Slice(points, func(left, right int) bool {
		if points[left].point.Slot != points[right].point.Slot {
			return points[left].point.Slot > points[right].point.Slot
		}
		return points[left].eventSeq > points[right].eventSeq
	})

	ret := make([]n2n.ChainPoint, 0, len(points)+1)
	seen := make(map[[40]byte]struct{}, len(points))
	for _, candidate := range points {
		point := candidate.point
		if point.Origin || point.Hash == ([32]byte{}) {
			continue
		}
		var key [40]byte
		copy(key[:8], uint64Bytes(point.Slot))
		copy(key[8:], point.Hash[:])
		if _, duplicate := seen[key]; duplicate {
			continue
		}
		if len(ret) > 0 && point.Slot == ret[len(ret)-1].Point.Slot {
			newer := ret[len(ret)-1]
			if newer.IsByronEBB ||
				!point.IsByronEBB ||
				newer.BlockNumber != point.BlockNumber+1 {
				return nil, fmt.Errorf(
					"equal-slot candidates are not a Byron EBB/successor pair at slot %d",
					point.Slot,
				)
			}
		}
		if len(ret) > 0 {
			newer := ret[len(ret)-1]
			if point.Slot > newer.Point.Slot ||
				point.BlockNumber > newer.BlockNumber {
				return nil, fmt.Errorf(
					"committed candidates are not newest-to-oldest at slot %d height %d",
					point.Slot,
					point.BlockNumber,
				)
			}
			if point.Slot < newer.Point.Slot &&
				point.BlockNumber == newer.BlockNumber &&
				(!newer.IsByronEBB || point.IsByronEBB) {
				return nil, fmt.Errorf(
					"equal-height candidates are not a Byron EBB/predecessor pair at height %d",
					point.BlockNumber,
				)
			}
		}
		seen[key] = struct{}{}
		ret = append(ret, chainPointFromPublication(point))
	}

	ret, err = appendTerminalIntersection(ret, identity.Start)
	if err != nil {
		return nil, err
	}
	return ret, nil
}

func (d *DB) recentIntersectionEvents(
	ctx context.Context,
	snapshot uint64,
) ([]intersectionEvent, error) {
	const query = `
SELECT event_seq, publication_id, slot, block_hash, block_number, is_byron_ebb
FROM clicksync.chain_events
WHERE event_kind = 'adoption'
  AND event_seq <= ?
ORDER BY event_seq DESC
LIMIT ?`
	rows, err := d.conn.Query(ctx, query, snapshot, denseIntersectionEvents)
	if err != nil {
		return nil, fmt.Errorf("query dense intersection events: %w", err)
	}
	defer rows.Close()
	ret := make([]intersectionEvent, 0, denseIntersectionEvents)
	for rows.Next() {
		event, err := scanIntersectionEvent(rows.Scan)
		if err != nil {
			return nil, err
		}
		ret = append(ret, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate dense intersection events: %w", err)
	}
	return ret, nil
}

func (d *DB) intersectionEventAtOrBefore(
	ctx context.Context,
	eventSeq uint64,
) (intersectionEvent, bool, error) {
	const query = `
SELECT event_seq, publication_id, slot, block_hash, block_number, is_byron_ebb
FROM clicksync.chain_events
WHERE event_kind = 'adoption'
  AND event_seq <= ?
ORDER BY event_seq DESC
LIMIT 1`
	rows, err := d.conn.Query(ctx, query, eventSeq)
	if err != nil {
		return intersectionEvent{}, false, fmt.Errorf("query geometric intersection event: %w", err)
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return intersectionEvent{}, false, fmt.Errorf("iterate geometric intersection event: %w", err)
		}
		return intersectionEvent{}, false, nil
	}
	event, err := scanIntersectionEvent(rows.Scan)
	if err != nil {
		return intersectionEvent{}, false, err
	}
	return event, true, nil
}

func scanIntersectionEvent(
	scan func(...any) error,
) (intersectionEvent, error) {
	var (
		eventSeq      uint64
		publicationID uint64
		slot          uint64
		hashBytes     []byte
		blockNumber   uint64
		isByronEBB    bool
	)
	if err := scan(
		&eventSeq,
		&publicationID,
		&slot,
		&hashBytes,
		&blockNumber,
		&isByronEBB,
	); err != nil {
		return intersectionEvent{}, fmt.Errorf("scan intersection event: %w", err)
	}
	hash, err := hash32(hashBytes)
	if err != nil {
		return intersectionEvent{}, err
	}
	return intersectionEvent{
		eventSeq:      eventSeq,
		publicationID: publicationID,
		point: publication.Point{
			Slot:        slot,
			Hash:        hash,
			BlockNumber: blockNumber,
			IsByronEBB:  isByronEBB,
		},
	}, nil
}

func terminalIntersection(start publication.Point) []n2n.ChainPoint {
	if start.Origin {
		return []n2n.ChainPoint{n2n.NewChainPointOrigin()}
	}
	return []n2n.ChainPoint{chainPointFromPublication(start)}
}

func appendTerminalIntersection(
	candidates []n2n.ChainPoint,
	start publication.Point,
) ([]n2n.ChainPoint, error) {
	terminal := terminalIntersection(start)[0]
	if len(candidates) == 0 {
		return append(candidates, terminal), nil
	}
	previous := candidates[len(candidates)-1]
	if previous.Point.Slot == terminal.Point.Slot &&
		bytes.Equal(previous.Point.Hash, terminal.Point.Hash) {
		if previous.BlockNumber != terminal.BlockNumber ||
			previous.IsByronEBB != terminal.IsByronEBB {
			return nil, errors.New("terminal boundary metadata conflicts with committed candidate")
		}
		return candidates, nil
	}
	if len(terminal.Point.Hash) == 0 {
		return append(candidates, terminal), nil
	}
	if terminal.Point.Slot > previous.Point.Slot {
		return nil, errors.New("partial-history boundary is newer than committed candidates")
	}
	if terminal.BlockNumber > previous.BlockNumber {
		return nil, errors.New("partial-history boundary height is newer than committed candidates")
	}
	if terminal.Point.Slot == previous.Point.Slot {
		if previous.IsByronEBB ||
			!terminal.IsByronEBB ||
			previous.BlockNumber != terminal.BlockNumber+1 {
			return nil, errors.New(
				"equal-slot partial boundary is not the Byron EBB parent of its successor",
			)
		}
	} else if terminal.BlockNumber == previous.BlockNumber &&
		(!previous.IsByronEBB || terminal.IsByronEBB) {
		return nil, errors.New(
			"equal-height partial boundary is not the predecessor of a Byron EBB",
		)
	}
	return append(candidates, terminal), nil
}

func chainPointFromPublication(point publication.Point) n2n.ChainPoint {
	wirePoint := pcommon.NewPoint(point.Slot, bytes.Clone(point.Hash[:]))
	if point.IsByronEBB {
		return n2n.NewByronEBBChainPoint(wirePoint, point.BlockNumber)
	}
	return n2n.NewChainPoint(wirePoint, point.BlockNumber)
}

func uint64Bytes(value uint64) []byte {
	return []byte{
		byte(value >> 56),
		byte(value >> 48),
		byte(value >> 40),
		byte(value >> 32),
		byte(value >> 24),
		byte(value >> 16),
		byte(value >> 8),
		byte(value),
	}
}
