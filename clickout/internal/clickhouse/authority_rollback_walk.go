package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/google/uuid"
)

const authorityBlockHashCandidatesSQL = `
SELECT publication_id
FROM blocks
PREWHERE block_hash = ?
WHERE (NOT ? OR publication_id > ?)
ORDER BY block_hash, publication_id
LIMIT 9`

const authorityPublicationMembershipSQL = `
SELECT
    event_seq, publication_id, event_kind, active, rollback_id, block_hash,
    slot, block_number, is_byron_ebb, writer_id, recorded_at
FROM chain_events
PREWHERE publication_id = ?
  AND event_seq <= ?
WHERE (NOT ? OR event_seq > ?)
ORDER BY publication_id, event_seq, event_kind, rollback_id
LIMIT 9`

type authorityPhysicalMembershipRow struct {
	EventSeq      uint64     `ch:"event_seq"`
	PublicationID uint64     `ch:"publication_id"`
	EventKind     string     `ch:"event_kind"`
	Active        bool       `ch:"active"`
	RollbackID    *uuid.UUID `ch:"rollback_id"`
	BlockHash     string     `ch:"block_hash"`
	Slot          uint64     `ch:"slot"`
	BlockNumber   uint64     `ch:"block_number"`
	IsByronEBB    bool       `ch:"is_byron_ebb"`
	WriterID      uuid.UUID  `ch:"writer_id"`
	RecordedAt    time.Time  `ch:"recorded_at"`
}

type authorityActiveBlock struct {
	PublicationID    uint64
	AdoptionEventSeq uint64
	Point            authorityPoint
	ParentHash       *authorityHash
	Synthetic        bool
}

type authorityRollbackDescendant struct {
	PublicationID uint64
	Point         authorityPoint
}

func decodeAuthorityBlockHashCandidatePage(
	publicationIDs []uint64,
) (publicationID uint64, found bool, err error) {
	defer func() {
		err = invalidAuthorityError(err)
	}()
	if len(publicationIDs) == 0 {
		return 0, false, nil
	}
	if len(publicationIDs) > 9 {
		return 0, false, errors.New("block-hash candidate page exceeds LIMIT 9")
	}
	for index, publicationID := range publicationIDs {
		if publicationID == 0 {
			return 0, false, errors.New("block-hash candidate publication is zero")
		}
		if index > 0 && publicationID < publicationIDs[index-1] {
			return 0, false, errors.New("block-hash candidate page is not ordered")
		}
	}
	first := publicationIDs[0]
	replays := 0
	for _, publicationID := range publicationIDs {
		if publicationID != first {
			break
		}
		replays++
	}
	if replays >= 9 {
		return 0, false, errors.New(
			"block-hash candidate has at least nine physical rows",
		)
	}
	return first, true, nil
}

func authorityRollbackWalkPhaseLimits() phaseLimits {
	return hydrationPhaseLimits(9)
}

func (store *Store) nextAuthorityBlockHashCandidate(
	ctx context.Context,
	hash authorityHash,
	cursorSet bool,
	cursor uint64,
) (uint64, bool, error) {
	queryCtx, finish := store.instrumentPhase(
		ctx,
		"authority_block_hash_candidates",
		authorityRollbackWalkPhaseLimits(),
	)
	defer finish()
	rows, err := store.conn.Query(
		queryCtx,
		authorityBlockHashCandidatesSQL,
		string(hash[:]),
		cursorSet,
		cursor,
	)
	if err != nil {
		return 0, false, mapQueryError("authority_block_hash_candidates", err)
	}
	defer rows.Close()
	publicationIDs := make([]uint64, 0, 9)
	for rows.Next() {
		var publicationID uint64
		if err := rows.Scan(&publicationID); err != nil {
			return 0, false, fmt.Errorf("scan block-hash candidate: %w", err)
		}
		publicationIDs = append(publicationIDs, publicationID)
	}
	if err := rows.Err(); err != nil {
		return 0, false, fmt.Errorf("iterate block-hash candidates: %w", err)
	}
	return decodeAuthorityBlockHashCandidatePage(publicationIDs)
}

func sameAuthorityPhysicalMembershipRow(
	left authorityPhysicalMembershipRow,
	right authorityPhysicalMembershipRow,
) bool {
	if !left.RecordedAt.Equal(right.RecordedAt) {
		return false
	}
	left.RecordedAt = time.Time{}
	right.RecordedAt = time.Time{}
	return reflect.DeepEqual(left, right)
}

func decodeAuthorityPhysicalMembershipRow(
	row authorityPhysicalMembershipRow,
	publicationID uint64,
	snapshot uint64,
) (point authorityPoint, err error) {
	defer func() {
		err = invalidAuthorityError(err)
	}()
	if row.EventSeq == 0 ||
		row.EventSeq > snapshot ||
		row.PublicationID == 0 ||
		row.PublicationID != publicationID ||
		row.WriterID == uuid.Nil {
		return authorityPoint{}, errors.New(
			"physical membership event/publication/writer identity is invalid",
		)
	}
	if row.RecordedAt.IsZero() ||
		row.RecordedAt != normalizeAuthorityTime(row.RecordedAt) {
		return authorityPoint{}, errors.New(
			"physical membership time is not UTC-microcanonical",
		)
	}
	switch row.EventKind {
	case "adoption":
		if !row.Active || row.RollbackID != nil {
			return authorityPoint{}, errors.New(
				"physical adoption membership has invalid shape",
			)
		}
	case "invalidation":
		if row.Active ||
			row.RollbackID == nil ||
			*row.RollbackID == uuid.Nil {
			return authorityPoint{}, errors.New(
				"physical invalidation membership has invalid shape",
			)
		}
	default:
		return authorityPoint{}, fmt.Errorf(
			"unknown physical membership kind %q",
			row.EventKind,
		)
	}
	hash, err := fixedAuthorityHash(row.BlockHash)
	if err != nil {
		return authorityPoint{}, fmt.Errorf("physical membership block hash: %w", err)
	}
	point = authorityPoint{
		Slot:        row.Slot,
		Hash:        hash,
		BlockNumber: row.BlockNumber,
		IsByronEBB:  row.IsByronEBB,
	}
	if err := validateAuthorityPoint("physical membership point", point); err != nil {
		return authorityPoint{}, err
	}
	return point, nil
}

func decodeAuthorityMembershipEventPage(
	rows []authorityPhysicalMembershipRow,
	publicationID uint64,
	snapshot uint64,
) (
	membership authorityPhysicalMembershipRow,
	point authorityPoint,
	found bool,
	err error,
) {
	defer func() {
		err = invalidAuthorityError(err)
	}()
	if len(rows) == 0 {
		return authorityPhysicalMembershipRow{}, authorityPoint{}, false, nil
	}
	if len(rows) > 9 {
		return authorityPhysicalMembershipRow{}, authorityPoint{}, false,
			errors.New("physical membership page exceeds LIMIT 9")
	}
	for index, row := range rows {
		if row.PublicationID != publicationID ||
			row.EventSeq > snapshot ||
			(index > 0 && row.EventSeq < rows[index-1].EventSeq) {
			return authorityPhysicalMembershipRow{}, authorityPoint{}, false,
				errors.New("physical membership page is outside its stable key")
		}
	}
	eventSeq := rows[0].EventSeq
	replays := 0
	var first authorityPhysicalMembershipRow
	for _, row := range rows {
		if row.EventSeq != eventSeq {
			break
		}
		replays++
		if replays >= 9 {
			return authorityPhysicalMembershipRow{}, authorityPoint{}, false,
				errors.New(
					"physical membership event has at least nine rows",
				)
		}
		decoded, err := decodeAuthorityPhysicalMembershipRow(
			row,
			publicationID,
			snapshot,
		)
		if err != nil {
			return authorityPhysicalMembershipRow{}, authorityPoint{}, false, err
		}
		if replays == 1 {
			first = row
			point = decoded
			continue
		}
		if !sameAuthorityPhysicalMembershipRow(first, row) {
			return authorityPhysicalMembershipRow{}, authorityPoint{}, false,
				errors.New(
					"physical membership event rows conflict",
				)
		}
	}
	return first, point, true, nil
}

func (store *Store) nextAuthorityMembershipEvent(
	ctx context.Context,
	publicationID uint64,
	snapshot uint64,
	cursorSet bool,
	cursor uint64,
) (authorityPhysicalMembershipRow, authorityPoint, bool, error) {
	queryCtx, finish := store.instrumentPhase(
		ctx,
		"authority_publication_membership",
		authorityRollbackWalkPhaseLimits(),
	)
	defer finish()
	rows, err := store.conn.Query(
		queryCtx,
		authorityPublicationMembershipSQL,
		publicationID,
		snapshot,
		cursorSet,
		cursor,
	)
	if err != nil {
		return authorityPhysicalMembershipRow{}, authorityPoint{}, false,
			mapQueryError("authority_publication_membership", err)
	}
	defer rows.Close()
	physical := make([]authorityPhysicalMembershipRow, 0, 9)
	for rows.Next() {
		var row authorityPhysicalMembershipRow
		if err := rows.ScanStruct(&row); err != nil {
			return authorityPhysicalMembershipRow{}, authorityPoint{}, false,
				fmt.Errorf("scan physical membership: %w", err)
		}
		physical = append(physical, row)
	}
	if err := rows.Err(); err != nil {
		return authorityPhysicalMembershipRow{}, authorityPoint{}, false,
			fmt.Errorf("iterate physical membership: %w", err)
	}
	return decodeAuthorityMembershipEventPage(physical, publicationID, snapshot)
}

func membershipMatchesAdoption(
	membership authorityPhysicalMembershipRow,
	adoption authorityPhysicalAdoptionRow,
) bool {
	return membership.EventKind == "adoption" &&
		membership.EventSeq == adoption.EventSeq &&
		membership.PublicationID == adoption.PublicationID &&
		membership.Active == adoption.Active &&
		membership.RollbackID == nil &&
		adoption.RollbackID == nil &&
		membership.BlockHash == adoption.BlockHash &&
		membership.Slot == adoption.Slot &&
		membership.BlockNumber == adoption.BlockNumber &&
		membership.IsByronEBB == adoption.IsByronEBB &&
		membership.WriterID == adoption.WriterID &&
		membership.RecordedAt.Equal(adoption.RecordedAt)
}

func validateAuthorityHistoricalSynthetic(
	record authorityRecord,
	adoption authorityPhysicalAdoptionRow,
	block authorityPhysicalBlockRow,
	point authorityPoint,
) (err error) {
	defer func() {
		err = invalidAuthorityError(err)
	}()
	if adoption.EventSeq != 1 ||
		record.Start != (authorityPoint{Origin: true}) ||
		!record.GenesisSeeded ||
		!record.CompleteHistory ||
		point.Slot != 0 ||
		point.BlockNumber != 0 ||
		point.IsByronEBB ||
		point.Hash != record.ByronGenesisID ||
		block.ParentHash != nil ||
		block.Era != "Byron" ||
		block.BlockType != -1 ||
		!block.Synthetic {
		return errors.New(
			"historical synthetic adoption differs from immutable genesis",
		)
	}
	return nil
}

func validateAuthorityHistoricalInvalidation(
	membership authorityPhysicalMembershipRow,
	point authorityPoint,
	blockPoint authorityPoint,
	header authorityPhysicalRollbackRow,
	headerFound bool,
) (err error) {
	defer func() {
		err = invalidAuthorityError(err)
	}()
	if !headerFound ||
		membership.EventKind != "invalidation" ||
		membership.RollbackID == nil ||
		*membership.RollbackID != header.RollbackID ||
		membership.EventSeq != header.EventSeq ||
		membership.PublicationID == 0 ||
		point != blockPoint ||
		membership.WriterID != header.WriterID ||
		!membership.RecordedAt.Equal(header.RecordedAt) {
		return errors.New(
			"historical invalidation differs from exact rollback header/block",
		)
	}
	return nil
}

type authorityMembershipLifecycle struct {
	any              bool
	adoptionSeen     bool
	invalidationSeen bool
}

func (lifecycle *authorityMembershipLifecycle) observe(kind string) (err error) {
	defer func() {
		err = invalidAuthorityError(err)
	}()
	lifecycle.any = true
	switch kind {
	case "adoption":
		if lifecycle.adoptionSeen || lifecycle.invalidationSeen {
			return errors.New(
				"publication lifecycle has a second/reactivation adoption",
			)
		}
		lifecycle.adoptionSeen = true
	case "invalidation":
		if !lifecycle.adoptionSeen || lifecycle.invalidationSeen {
			return errors.New(
				"publication lifecycle has invalid invalidation order/count",
			)
		}
		lifecycle.invalidationSeen = true
	default:
		return fmt.Errorf("unknown publication lifecycle kind %q", kind)
	}
	return nil
}

func (lifecycle authorityMembershipLifecycle) active() (
	active bool,
	err error,
) {
	defer func() {
		err = invalidAuthorityError(err)
	}()
	if !lifecycle.any {
		return false, nil
	}
	if !lifecycle.adoptionSeen {
		return false, errors.New("publication lifecycle has no adoption")
	}
	return !lifecycle.invalidationSeen, nil
}

type authorityPublicationLifecycleSummary struct {
	Adoption      authorityPhysicalMembershipRow
	AdoptionFound bool
	Active        bool
}

type authorityPublicationLifecycleReaders struct {
	nextMembership func(
		context.Context,
		uint64,
		uint64,
		bool,
		uint64,
	) (authorityPhysicalMembershipRow, authorityPoint, bool, error)
	loadAdoption func(
		context.Context,
		uint64,
	) (authorityPhysicalAdoptionRow, authorityPoint, bool, error)
	loadRollback func(
		context.Context,
		uint64,
	) (
		authorityPhysicalRollbackRow,
		authorityPoint,
		authorityPoint,
		authorityHash,
		bool,
		error,
	)
}

func walkAuthorityPublicationLifecycle(
	ctx context.Context,
	record authorityRecord,
	snapshot uint64,
	block authorityPhysicalBlockRow,
	blockPoint authorityPoint,
	readers authorityPublicationLifecycleReaders,
) (authorityPublicationLifecycleSummary, error) {
	var (
		cursorSet bool
		cursor    uint64
		lifecycle authorityMembershipLifecycle
		summary   authorityPublicationLifecycleSummary
	)
	for {
		if err := ctx.Err(); err != nil {
			return authorityPublicationLifecycleSummary{}, err
		}
		membership, point, found, err := readers.nextMembership(
			ctx,
			block.PublicationID,
			snapshot,
			cursorSet,
			cursor,
		)
		if err != nil {
			return authorityPublicationLifecycleSummary{}, err
		}
		if !found {
			break
		}
		if err := lifecycle.observe(membership.EventKind); err != nil {
			return authorityPublicationLifecycleSummary{}, err
		}
		switch membership.EventKind {
		case "adoption":
			adoption, adoptionPoint, adoptionFound, err :=
				readers.loadAdoption(ctx, membership.EventSeq)
			if err != nil {
				return authorityPublicationLifecycleSummary{}, err
			}
			if !adoptionFound ||
				!membershipMatchesAdoption(membership, adoption) {
				return authorityPublicationLifecycleSummary{},
					invalidAuthorityError(
						errors.New(
							"historical membership differs from exact adoption header",
						),
					)
			}
			if err := validateAuthorityAdoptionBlockIdentity(
				adoption,
				adoptionPoint,
				block,
				blockPoint,
			); err != nil {
				return authorityPublicationLifecycleSummary{}, err
			}
			if block.Synthetic {
				if err := validateAuthorityHistoricalSynthetic(
					record,
					adoption,
					block,
					blockPoint,
				); err != nil {
					return authorityPublicationLifecycleSummary{}, err
				}
			}
			summary.Adoption = membership
			summary.AdoptionFound = true
		case "invalidation":
			header, _, _, _, headerFound, err :=
				readers.loadRollback(ctx, membership.EventSeq)
			if err != nil {
				return authorityPublicationLifecycleSummary{}, err
			}
			if err := validateAuthorityHistoricalInvalidation(
				membership,
				point,
				blockPoint,
				header,
				headerFound,
			); err != nil {
				return authorityPublicationLifecycleSummary{}, err
			}
		}
		cursorSet = true
		cursor = membership.EventSeq
	}
	active, err := lifecycle.active()
	if err != nil {
		return authorityPublicationLifecycleSummary{}, err
	}
	summary.Active = active
	return summary, nil
}

func (store *Store) authorityPublicationLifecycleAt(
	ctx context.Context,
	record authorityRecord,
	snapshot uint64,
	block authorityPhysicalBlockRow,
	blockPoint authorityPoint,
) (authorityPublicationLifecycleSummary, error) {
	return walkAuthorityPublicationLifecycle(
		ctx,
		record,
		snapshot,
		block,
		blockPoint,
		authorityPublicationLifecycleReaders{
			nextMembership: store.nextAuthorityMembershipEvent,
			loadAdoption:   store.loadAuthorityPhysicalAdoption,
			loadRollback:   store.loadAuthorityPhysicalRollback,
		},
	)
}

func (store *Store) authorityPublicationActiveAt(
	ctx context.Context,
	record authorityRecord,
	snapshot uint64,
	block authorityPhysicalBlockRow,
	blockPoint authorityPoint,
) (bool, error) {
	summary, err := store.authorityPublicationLifecycleAt(
		ctx,
		record,
		snapshot,
		block,
		blockPoint,
	)
	if err != nil {
		return false, err
	}
	return summary.Active, nil
}

func validateAuthorityCurrentAdoptionLifecycleSummary(
	adoption authorityPhysicalAdoptionRow,
	summary authorityPublicationLifecycleSummary,
) (err error) {
	defer func() {
		err = invalidAuthorityError(err)
	}()
	if !summary.Active ||
		!summary.AdoptionFound ||
		!membershipMatchesAdoption(summary.Adoption, adoption) {
		return errors.New(
			"current physical adoption is not the sole active exact publication lifecycle",
		)
	}
	return nil
}

func (store *Store) validateAuthorityCurrentAdoptionLifecycle(
	ctx context.Context,
	record authorityRecord,
	adoption authorityPhysicalAdoptionRow,
	block authorityPhysicalBlockRow,
	blockPoint authorityPoint,
) error {
	summary, err := store.authorityPublicationLifecycleAt(
		ctx,
		record,
		record.Physical.EventSeq,
		block,
		blockPoint,
	)
	if err != nil {
		return err
	}
	return validateAuthorityCurrentAdoptionLifecycleSummary(adoption, summary)
}

func decodeAuthorityBlockParent(
	row authorityPhysicalBlockRow,
) (parentHash *authorityHash, err error) {
	defer func() {
		err = invalidAuthorityError(err)
	}()
	if row.ParentHash == nil {
		return nil, nil
	}
	parent, err := fixedAuthorityHash(*row.ParentHash)
	if err != nil {
		return nil, err
	}
	if parent == (authorityHash{}) {
		return nil, errors.New("authority block parent is zero")
	}
	return &parent, nil
}

func (store *Store) loadAuthorityActiveBlockByHash(
	ctx context.Context,
	record authorityRecord,
	snapshot uint64,
	hash authorityHash,
) (authorityActiveBlock, bool, error) {
	var (
		cursorSet bool
		cursor    uint64
		active    *authorityActiveBlock
	)
	for {
		if err := ctx.Err(); err != nil {
			return authorityActiveBlock{}, false, err
		}
		publicationID, found, err := store.nextAuthorityBlockHashCandidate(
			ctx,
			hash,
			cursorSet,
			cursor,
		)
		if err != nil {
			return authorityActiveBlock{}, false, err
		}
		if !found {
			break
		}
		block, point, blockFound, err := store.loadAuthorityPhysicalBlock(
			ctx,
			publicationID,
		)
		if err != nil {
			return authorityActiveBlock{}, false, err
		}
		if !blockFound || point.Hash != hash {
			return authorityActiveBlock{}, false, invalidAuthorityError(
				errors.New(
					"block-hash candidate differs from exact publication",
				),
			)
		}
		lifecycle, err := store.authorityPublicationLifecycleAt(
			ctx,
			record,
			snapshot,
			block,
			point,
		)
		if err != nil {
			return authorityActiveBlock{}, false, err
		}
		if lifecycle.Active {
			if !lifecycle.AdoptionFound ||
				lifecycle.Adoption.EventSeq == 0 ||
				lifecycle.Adoption.PublicationID != block.PublicationID {
				return authorityActiveBlock{}, false, invalidAuthorityError(
					errors.New(
						"active block publication lost its sole adoption event",
					),
				)
			}
			if active != nil {
				return authorityActiveBlock{}, false, invalidAuthorityError(
					errors.New(
						"block hash has multiple active publications",
					),
				)
			}
			parent, err := decodeAuthorityBlockParent(block)
			if err != nil {
				return authorityActiveBlock{}, false, err
			}
			value := authorityActiveBlock{
				PublicationID:    block.PublicationID,
				AdoptionEventSeq: lifecycle.Adoption.EventSeq,
				Point:            point,
				ParentHash:       parent,
				Synthetic:        block.Synthetic,
			}
			active = &value
		}
		cursorSet = true
		cursor = publicationID
	}
	if active == nil {
		return authorityActiveBlock{}, false, nil
	}
	return *active, true, nil
}

// validateAuthorityRollbackOldTipHead is intentionally the lightweight,
// non-recursive proof used while reconstructing a rollback's own old-active
// chain. Serving-authority Effective heads use validateAuthorityExactHeadArtifacts
// instead, which additionally proves frozen evidence and the full invalidation
// set.
func (store *Store) validateAuthorityRollbackOldTipHead(
	ctx context.Context,
	record authorityRecord,
	eventSeq uint64,
	expected authorityPoint,
) error {
	if eventSeq == 0 {
		if expected != record.Start {
			return invalidAuthorityError(
				errors.New(
					"event-zero old physical tip differs from manifest start",
				),
			)
		}
		return nil
	}
	adoption, adoptionPoint, adoptionFound, err :=
		store.loadAuthorityPhysicalAdoption(ctx, eventSeq)
	if err != nil {
		return err
	}
	rollback, rollbackTo, _, _, rollbackFound, err :=
		store.loadAuthorityPhysicalRollback(ctx, eventSeq)
	if err != nil {
		return err
	}
	if adoptionFound == rollbackFound {
		return invalidAuthorityError(
			errors.New(
				"old physical event does not have exactly one adoption/rollback header",
			),
		)
	}
	if rollbackFound {
		if rollback.EventSeq != eventSeq || rollbackTo != expected {
			return invalidAuthorityError(
				errors.New(
					"old physical rollback header differs from exact tip",
				),
			)
		}
		return nil
	}
	block, blockPoint, found, err := store.loadAuthorityPhysicalBlock(
		ctx,
		adoption.PublicationID,
	)
	if err != nil {
		return err
	}
	if !found {
		return invalidAuthorityError(
			errors.New("old physical adoption has no exact block"),
		)
	}
	if err := validateAuthorityAdoptionBlockIdentity(
		adoption,
		adoptionPoint,
		block,
		blockPoint,
	); err != nil {
		return err
	}
	active, err := store.authorityPublicationActiveAt(
		ctx,
		record,
		eventSeq,
		block,
		blockPoint,
	)
	if err != nil {
		return err
	}
	if !active {
		return invalidAuthorityError(
			errors.New("old physical adoption is not active at its event"),
		)
	}
	if block.Synthetic {
		if err := validateAuthorityHistoricalSynthetic(
			record,
			adoption,
			block,
			blockPoint,
		); err != nil {
			return err
		}
		if !expected.Origin {
			return invalidAuthorityError(
				errors.New(
					"synthetic old physical adoption does not map to Origin",
				),
			)
		}
		return nil
	}
	if adoptionPoint != expected {
		return invalidAuthorityError(
			errors.New("old physical adoption differs from exact tip"),
		)
	}
	return nil
}

type authorityActiveBlockResolver func(
	context.Context,
	uint64,
	authorityHash,
) (authorityActiveBlock, bool, error)

type authorityRollbackDescendantVisitor func(
	authorityRollbackDescendant,
) error

func validateAuthorityParentProgress(
	parent authorityActiveBlock,
	child authorityActiveBlock,
) (err error) {
	defer func() {
		err = invalidAuthorityError(err)
	}()
	// Publications are allocated from a monotonic high-water and a contiguous
	// batch reserves them parent-to-child before facts are written.
	if parent.PublicationID >= child.PublicationID {
		return errors.New(
			"active parent publication does not precede its child",
		)
	}
	return validateAuthorityParentPointProgress(parent.Point, child.Point)
}

func validateAuthorityParentPointProgress(
	parent authorityPoint,
	child authorityPoint,
) (err error) {
	defer func() {
		err = invalidAuthorityError(err)
	}()
	switch {
	case child.IsByronEBB:
		if child.BlockNumber != parent.BlockNumber ||
			child.Slot <= parent.Slot {
			return errors.New("Byron EBB parent/child progress is invalid")
		}
	default:
		if child.BlockNumber == 0 ||
			child.BlockNumber-1 != parent.BlockNumber ||
			(parent.IsByronEBB && child.Slot < parent.Slot) ||
			(!parent.IsByronEBB && child.Slot <= parent.Slot) {
			return errors.New("ordinary parent/child progress is invalid")
		}
	}
	return nil
}

func walkAuthorityRollbackDescendants(
	ctx context.Context,
	record authorityRecord,
	header authorityPhysicalRollbackRow,
	to authorityPoint,
	oldTip authorityPoint,
	resolve authorityActiveBlockResolver,
	visit authorityRollbackDescendantVisitor,
) (uint32, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if to.Origin && !record.Start.Origin {
		return 0, invalidAuthorityError(
			errors.New(
				"Origin rollback is below the partial-history start",
			),
		)
	}
	if header.Depth == 0 {
		if oldTip != to {
			return 0, invalidAuthorityError(
				errors.New(
					"depth-zero rollback old tip differs from target",
				),
			)
		}
		return 0, nil
	}
	if oldTip == to || oldTip.Origin {
		return 0, invalidAuthorityError(
			errors.New("positive-depth rollback has an invalid old tip"),
		)
	}
	current, found, err := resolve(
		ctx,
		header.OldTipEventSeq,
		oldTip.Hash,
	)
	if err != nil {
		return 0, err
	}
	if !found || current.Point != oldTip {
		return 0, invalidAuthorityError(
			errors.New(
				"rollback old tip is not one exact active block",
			),
		)
	}
	var depth uint32
	for {
		if err := ctx.Err(); err != nil {
			return depth, err
		}
		if current.Synthetic {
			if !to.Origin {
				return depth, invalidAuthorityError(
					errors.New(
						"rollback target is below synthetic genesis",
					),
				)
			}
			if depth != header.Depth {
				return depth, invalidAuthorityError(
					errors.New(
						"synthetic exclusion differs from rollback depth",
					),
				)
			}
			return depth, nil
		}
		if current.Point == to {
			if depth != header.Depth {
				return depth, invalidAuthorityError(
					errors.New(
						"rollback reached target at a different depth",
					),
				)
			}
			return depth, nil
		}
		if depth == header.Depth {
			return depth, invalidAuthorityError(
				errors.New(
					"rollback depth ended before the exact target",
				),
			)
		}
		descendant := authorityRollbackDescendant{
			PublicationID: current.PublicationID,
			Point:         current.Point,
		}
		if visit != nil {
			if err := visit(descendant); err != nil {
				return depth, err
			}
		}
		depth++
		if current.ParentHash == nil {
			if !to.Origin || !record.Start.Origin {
				return depth, invalidAuthorityError(
					errors.New(
						"active parent chain ends before rollback target",
					),
				)
			}
			if depth != header.Depth {
				return depth, invalidAuthorityError(
					errors.New(
						"parentless Origin adjacency differs from rollback depth",
					),
				)
			}
			return depth, nil
		}
		if !to.Origin &&
			to == record.Start &&
			*current.ParentHash == to.Hash {
			if err := validateAuthorityParentPointProgress(
				to,
				current.Point,
			); err != nil {
				return depth, fmt.Errorf(
					"manifest-start parent/child progress: %w",
					err,
				)
			}
			if depth != header.Depth {
				return depth, invalidAuthorityError(
					errors.New(
						"manifest-start adjacency differs from rollback depth",
					),
				)
			}
			return depth, nil
		}
		parent, parentFound, err := resolve(
			ctx,
			header.OldTipEventSeq,
			*current.ParentHash,
		)
		if err != nil {
			return depth, err
		}
		if !parentFound {
			return depth, invalidAuthorityError(
				errors.New(
					"active parent chain ends before rollback target",
				),
			)
		}
		if err := validateAuthorityParentProgress(parent, current); err != nil {
			return depth, err
		}
		current = parent
	}
}

func authorityDepthZeroNeedsActiveBlock(
	record authorityRecord,
	to authorityPoint,
	oldTip authorityPoint,
) (needsActive bool, err error) {
	defer func() {
		err = invalidAuthorityError(err)
	}()
	if oldTip != to {
		return false, errors.New(
			"depth-zero rollback old tip differs from target",
		)
	}
	if to.Origin && !record.Start.Origin {
		return false, errors.New(
			"Origin rollback is below the partial-history start",
		)
	}
	return !oldTip.Origin && oldTip != record.Start, nil
}

func (store *Store) reconstructAuthorityRollbackDescendants(
	ctx context.Context,
	record authorityRecord,
	header authorityPhysicalRollbackRow,
	to authorityPoint,
	oldTip authorityPoint,
	visit authorityRollbackDescendantVisitor,
) (uint32, error) {
	if to.Origin && !record.Start.Origin {
		return 0, invalidAuthorityError(
			errors.New(
				"Origin rollback is below the partial-history start",
			),
		)
	}
	if err := store.validateAuthorityRollbackOldTipHead(
		ctx,
		record,
		header.OldTipEventSeq,
		oldTip,
	); err != nil {
		return 0, fmt.Errorf("validate rollback old physical tip: %w", err)
	}
	if header.Depth == 0 {
		needsActive, err := authorityDepthZeroNeedsActiveBlock(
			record,
			to,
			oldTip,
		)
		if err != nil {
			return 0, err
		}
		if needsActive {
			block, found, err := store.loadAuthorityActiveBlockByHash(
				ctx,
				record,
				header.OldTipEventSeq,
				oldTip.Hash,
			)
			if err != nil {
				return 0, err
			}
			if !found || block.Point != oldTip {
				return 0, invalidAuthorityError(
					errors.New(
						"depth-zero rollback tip is not exactly active",
					),
				)
			}
		}
		return 0, nil
	}
	return walkAuthorityRollbackDescendants(
		ctx,
		record,
		header,
		to,
		oldTip,
		func(
			resolveCtx context.Context,
			snapshot uint64,
			hash authorityHash,
		) (authorityActiveBlock, bool, error) {
			return store.loadAuthorityActiveBlockByHash(
				resolveCtx,
				record,
				snapshot,
				hash,
			)
		},
		visit,
	)
}
