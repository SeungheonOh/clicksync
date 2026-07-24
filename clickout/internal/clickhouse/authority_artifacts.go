package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/google/uuid"
)

const authorityPhysicalAdoptionSQL = `
SELECT
    event_seq, publication_id, active, rollback_id, block_hash, slot,
    block_number, is_byron_ebb, writer_id, recorded_at
FROM chain_events
PREWHERE event_kind = 'adoption'
  AND event_seq = ?
ORDER BY event_kind, event_seq, publication_id
LIMIT 9`

const authorityPhysicalBlockSQL = `
SELECT
    publication_id, block_hash, parent_hash, slot, block_number, era,
    block_type, synthetic, facts_digest, writer_id, inserted_at
FROM blocks
PREWHERE publication_id = ?
ORDER BY publication_id, block_hash
LIMIT 9`

const authorityPhysicalRollbackSQL = `
SELECT
    rollback_id, event_seq,
    rollback_to_origin, rollback_to_slot, rollback_to_hash,
    rollback_to_block_number, rollback_to_is_byron_ebb,
    old_tip_slot, old_tip_hash, old_tip_block_number, old_tip_is_byron_ebb,
    old_tip_event_seq, depth, reason, observed_peers, observed_operators,
    corroboration_required, check_id, agreement_group, check_attempt,
    checked_event_seq, evidence_count, evidence_digest, writer_id, recorded_at
FROM rollbacks
PREWHERE event_seq = ?
ORDER BY event_seq, rollback_id
LIMIT 9`

type authorityPhysicalAdoptionRow struct {
	EventSeq      uint64     `ch:"event_seq"`
	PublicationID uint64     `ch:"publication_id"`
	Active        bool       `ch:"active"`
	RollbackID    *uuid.UUID `ch:"rollback_id"`
	BlockHash     string     `ch:"block_hash"`
	Slot          uint64     `ch:"slot"`
	BlockNumber   uint64     `ch:"block_number"`
	IsByronEBB    bool       `ch:"is_byron_ebb"`
	WriterID      uuid.UUID  `ch:"writer_id"`
	RecordedAt    time.Time  `ch:"recorded_at"`
}

type authorityPhysicalBlockRow struct {
	PublicationID uint64    `ch:"publication_id"`
	BlockHash     string    `ch:"block_hash"`
	ParentHash    *string   `ch:"parent_hash"`
	Slot          uint64    `ch:"slot"`
	BlockNumber   uint64    `ch:"block_number"`
	Era           string    `ch:"era"`
	BlockType     int16     `ch:"block_type"`
	Synthetic     bool      `ch:"synthetic"`
	FactsDigest   string    `ch:"facts_digest"`
	WriterID      uuid.UUID `ch:"writer_id"`
	InsertedAt    time.Time `ch:"inserted_at"`
}

type authorityPhysicalRollbackRow struct {
	RollbackID            uuid.UUID  `ch:"rollback_id"`
	EventSeq              uint64     `ch:"event_seq"`
	ToOrigin              bool       `ch:"rollback_to_origin"`
	ToSlot                *uint64    `ch:"rollback_to_slot"`
	ToHash                *string    `ch:"rollback_to_hash"`
	ToBlockNumber         *uint64    `ch:"rollback_to_block_number"`
	ToIsByronEBB          bool       `ch:"rollback_to_is_byron_ebb"`
	OldTipSlot            *uint64    `ch:"old_tip_slot"`
	OldTipHash            *string    `ch:"old_tip_hash"`
	OldTipBlockNumber     *uint64    `ch:"old_tip_block_number"`
	OldTipIsByronEBB      bool       `ch:"old_tip_is_byron_ebb"`
	OldTipEventSeq        uint64     `ch:"old_tip_event_seq"`
	Depth                 uint32     `ch:"depth"`
	Reason                string     `ch:"reason"`
	ObservedPeers         []string   `ch:"observed_peers"`
	ObservedOperators     []string   `ch:"observed_operators"`
	CorroborationRequired uint16     `ch:"corroboration_required"`
	CheckID               uuid.UUID  `ch:"check_id"`
	AgreementGroup        *uuid.UUID `ch:"agreement_group"`
	CheckAttempt          uint32     `ch:"check_attempt"`
	CheckedEventSeq       uint64     `ch:"checked_event_seq"`
	EvidenceCount         uint32     `ch:"evidence_count"`
	EvidenceDigest        string     `ch:"evidence_digest"`
	WriterID              uuid.UUID  `ch:"writer_id"`
	RecordedAt            time.Time  `ch:"recorded_at"`
}

func sameAuthorityPhysicalAdoptionRow(
	left authorityPhysicalAdoptionRow,
	right authorityPhysicalAdoptionRow,
) bool {
	if !left.RecordedAt.Equal(right.RecordedAt) {
		return false
	}
	left.RecordedAt = time.Time{}
	right.RecordedAt = time.Time{}
	return reflect.DeepEqual(left, right)
}

func decodeAuthorityPhysicalAdoptionRows(
	rows []authorityPhysicalAdoptionRow,
	eventSeq uint64,
) (
	decoded authorityPhysicalAdoptionRow,
	point authorityPoint,
	found bool,
	err error,
) {
	defer func() {
		err = invalidAuthorityError(err)
	}()
	if eventSeq == 0 {
		return authorityPhysicalAdoptionRow{}, authorityPoint{}, false, errors.New(
			"physical adoption request has event zero",
		)
	}
	if len(rows) == 0 {
		return authorityPhysicalAdoptionRow{}, authorityPoint{}, false, nil
	}
	if len(rows) >= 9 {
		return authorityPhysicalAdoptionRow{}, authorityPoint{}, false, errors.New(
			"physical adoption has at least nine rows",
		)
	}
	var row authorityPhysicalAdoptionRow
	for index, physical := range rows {
		decoded, err := decodeAuthorityPhysicalAdoptionRow(physical, eventSeq)
		if err != nil {
			return authorityPhysicalAdoptionRow{}, authorityPoint{}, false, err
		}
		if index == 0 {
			row = physical
			point = decoded
			continue
		}
		if !sameAuthorityPhysicalAdoptionRow(row, physical) {
			return authorityPhysicalAdoptionRow{}, authorityPoint{}, false, errors.New(
				"physical adoption rows conflict",
			)
		}
	}
	return row, point, true, nil
}

func decodeAuthorityPhysicalAdoptionRow(
	row authorityPhysicalAdoptionRow,
	eventSeq uint64,
) (point authorityPoint, err error) {
	defer func() {
		err = invalidAuthorityError(err)
	}()
	if row.EventSeq == 0 || row.EventSeq != eventSeq ||
		row.PublicationID == 0 || !row.Active || row.RollbackID != nil {
		return authorityPoint{}, errors.New(
			"physical adoption has invalid exact event shape",
		)
	}
	if row.WriterID == uuid.Nil {
		return authorityPoint{}, errors.New("physical adoption writer ID is zero")
	}
	if row.RecordedAt.IsZero() ||
		row.RecordedAt != normalizeAuthorityTime(row.RecordedAt) {
		return authorityPoint{}, errors.New(
			"physical adoption recorded time is not UTC-microcanonical",
		)
	}
	hash, err := fixedAuthorityHash(row.BlockHash)
	if err != nil {
		return authorityPoint{}, fmt.Errorf(
			"physical adoption block hash: %w",
			err,
		)
	}
	point = authorityPoint{
		Slot:        row.Slot,
		Hash:        hash,
		BlockNumber: row.BlockNumber,
		IsByronEBB:  row.IsByronEBB,
	}
	if err := validateAuthorityPoint("physical adoption point", point); err != nil {
		return authorityPoint{}, err
	}
	return point, nil
}

func authorityPhysicalAdoptionPhaseLimits() phaseLimits {
	return hydrationPhaseLimits(9)
}

func (store *Store) loadAuthorityPhysicalAdoption(
	ctx context.Context,
	eventSeq uint64,
) (authorityPhysicalAdoptionRow, authorityPoint, bool, error) {
	queryCtx, finish := store.instrumentPhase(
		ctx,
		"authority_physical_adoption",
		authorityPhysicalAdoptionPhaseLimits(),
	)
	defer finish()
	rows, err := store.conn.Query(
		queryCtx,
		authorityPhysicalAdoptionSQL,
		eventSeq,
	)
	if err != nil {
		return authorityPhysicalAdoptionRow{}, authorityPoint{}, false,
			mapQueryError("authority_physical_adoption", err)
	}
	defer rows.Close()
	physical := make([]authorityPhysicalAdoptionRow, 0, 9)
	for rows.Next() {
		var row authorityPhysicalAdoptionRow
		if err := rows.ScanStruct(&row); err != nil {
			return authorityPhysicalAdoptionRow{}, authorityPoint{}, false,
				fmt.Errorf("scan physical adoption: %w", err)
		}
		physical = append(physical, row)
	}
	if err := rows.Err(); err != nil {
		return authorityPhysicalAdoptionRow{}, authorityPoint{}, false,
			fmt.Errorf("iterate physical adoption: %w", err)
	}
	return decodeAuthorityPhysicalAdoptionRows(physical, eventSeq)
}

func sameAuthorityPhysicalBlockRow(
	left authorityPhysicalBlockRow,
	right authorityPhysicalBlockRow,
) bool {
	if !left.InsertedAt.Equal(right.InsertedAt) {
		return false
	}
	left.InsertedAt = time.Time{}
	right.InsertedAt = time.Time{}
	return reflect.DeepEqual(left, right)
}

func decodeAuthorityPhysicalBlockRows(
	rows []authorityPhysicalBlockRow,
	publicationID uint64,
) (
	decoded authorityPhysicalBlockRow,
	point authorityPoint,
	found bool,
	err error,
) {
	defer func() {
		err = invalidAuthorityError(err)
	}()
	if publicationID == 0 {
		return authorityPhysicalBlockRow{}, authorityPoint{}, false, errors.New(
			"physical block request has publication zero",
		)
	}
	if len(rows) == 0 {
		return authorityPhysicalBlockRow{}, authorityPoint{}, false, nil
	}
	if len(rows) >= 9 {
		return authorityPhysicalBlockRow{}, authorityPoint{}, false, errors.New(
			"physical block has at least nine rows",
		)
	}
	var row authorityPhysicalBlockRow
	for index, physical := range rows {
		decoded, err := decodeAuthorityPhysicalBlockRow(physical, publicationID)
		if err != nil {
			return authorityPhysicalBlockRow{}, authorityPoint{}, false, err
		}
		if index == 0 {
			row = physical
			point = decoded
			continue
		}
		if !sameAuthorityPhysicalBlockRow(row, physical) {
			return authorityPhysicalBlockRow{}, authorityPoint{}, false, errors.New(
				"physical block rows conflict",
			)
		}
	}
	return row, point, true, nil
}

func decodeAuthorityPhysicalBlockRow(
	row authorityPhysicalBlockRow,
	publicationID uint64,
) (point authorityPoint, err error) {
	defer func() {
		err = invalidAuthorityError(err)
	}()
	if row.PublicationID == 0 || row.PublicationID != publicationID {
		return authorityPoint{}, errors.New(
			"physical block differs from exact publication",
		)
	}
	if row.WriterID == uuid.Nil {
		return authorityPoint{}, errors.New("physical block writer ID is zero")
	}
	if row.InsertedAt.IsZero() ||
		row.InsertedAt != normalizeAuthorityTime(row.InsertedAt) {
		return authorityPoint{}, errors.New(
			"physical block inserted time is not UTC-microcanonical",
		)
	}
	if strings.TrimSpace(row.Era) == "" {
		return authorityPoint{}, errors.New("physical block era is empty")
	}
	hash, err := fixedAuthorityHash(row.BlockHash)
	if err != nil {
		return authorityPoint{}, fmt.Errorf("physical block hash: %w", err)
	}
	point = authorityPoint{
		Slot:        row.Slot,
		Hash:        hash,
		BlockNumber: row.BlockNumber,
		IsByronEBB:  row.Era == "Byron" && row.BlockType == 0,
	}
	if err := validateAuthorityPoint("physical block point", point); err != nil {
		return authorityPoint{}, err
	}
	if row.ParentHash != nil {
		parent, err := fixedAuthorityHash(*row.ParentHash)
		if err != nil {
			return authorityPoint{}, fmt.Errorf("physical block parent hash: %w", err)
		}
		if parent == (authorityHash{}) {
			return authorityPoint{}, errors.New("physical block parent hash is zero")
		}
	}
	digest, err := fixedAuthorityHash(row.FactsDigest)
	if err != nil {
		return authorityPoint{}, fmt.Errorf("physical block facts digest: %w", err)
	}
	if digest == (authorityHash{}) {
		return authorityPoint{}, errors.New("physical block facts digest is zero")
	}
	return point, nil
}

func authorityPhysicalBlockPhaseLimits() phaseLimits {
	return hydrationPhaseLimits(9)
}

func (store *Store) loadAuthorityPhysicalBlock(
	ctx context.Context,
	publicationID uint64,
) (authorityPhysicalBlockRow, authorityPoint, bool, error) {
	queryCtx, finish := store.instrumentPhase(
		ctx,
		"authority_physical_block",
		authorityPhysicalBlockPhaseLimits(),
	)
	defer finish()
	rows, err := store.conn.Query(
		queryCtx,
		authorityPhysicalBlockSQL,
		publicationID,
	)
	if err != nil {
		return authorityPhysicalBlockRow{}, authorityPoint{}, false,
			mapQueryError("authority_physical_block", err)
	}
	defer rows.Close()
	physical := make([]authorityPhysicalBlockRow, 0, 9)
	for rows.Next() {
		var row authorityPhysicalBlockRow
		if err := rows.ScanStruct(&row); err != nil {
			return authorityPhysicalBlockRow{}, authorityPoint{}, false,
				fmt.Errorf("scan physical block: %w", err)
		}
		physical = append(physical, row)
	}
	if err := rows.Err(); err != nil {
		return authorityPhysicalBlockRow{}, authorityPoint{}, false,
			fmt.Errorf("iterate physical block: %w", err)
	}
	return decodeAuthorityPhysicalBlockRows(physical, publicationID)
}

func validateAuthorityPhysicalAdoptionMapping(
	record authorityRecord,
	adoption authorityPhysicalAdoptionRow,
	adoptionPoint authorityPoint,
	block authorityPhysicalBlockRow,
	blockPoint authorityPoint,
) (err error) {
	defer func() {
		err = invalidAuthorityError(err)
	}()
	if err := validateAuthorityAdoptionBlockIdentity(
		adoption,
		adoptionPoint,
		block,
		blockPoint,
	); err != nil {
		return err
	}
	if record.Physical.EventSeq == 0 ||
		adoption.EventSeq != record.Physical.EventSeq {
		return errors.New(
			"physical adoption event differs from manifest physical event",
		)
	}
	if block.Synthetic {
		if record.Physical != (authorityHead{
			EventSeq: 1,
			Point:    authorityPoint{Origin: true},
		}) ||
			record.Start != (authorityPoint{Origin: true}) ||
			!record.GenesisSeeded ||
			!record.CompleteHistory ||
			adoptionPoint.Slot != 0 ||
			adoptionPoint.BlockNumber != 0 ||
			adoptionPoint.IsByronEBB ||
			adoptionPoint.Hash != record.ByronGenesisID ||
			block.ParentHash != nil ||
			block.Era != "Byron" ||
			block.BlockType != -1 {
			return errors.New(
				"synthetic physical adoption differs from exact manifest genesis",
			)
		}
		return nil
	}
	if record.Physical.Point != adoptionPoint {
		return errors.New(
			"ordinary physical adoption differs from manifest physical point",
		)
	}
	return nil
}

func validateAuthorityAdoptionBlockIdentity(
	adoption authorityPhysicalAdoptionRow,
	adoptionPoint authorityPoint,
	block authorityPhysicalBlockRow,
	blockPoint authorityPoint,
) (err error) {
	defer func() {
		err = invalidAuthorityError(err)
	}()
	if adoption.EventSeq == 0 ||
		adoption.PublicationID == 0 ||
		adoption.PublicationID != block.PublicationID {
		return errors.New(
			"physical adoption differs from its exact block publication",
		)
	}
	if adoption.WriterID != block.WriterID ||
		!adoption.RecordedAt.Equal(block.InsertedAt) {
		return errors.New(
			"physical adoption differs from block writer/time provenance",
		)
	}
	if err := validateAuthorityPoint("physical adoption point", adoptionPoint); err != nil {
		return err
	}
	if err := validateAuthorityPoint("physical block point", blockPoint); err != nil {
		return err
	}
	if adoptionPoint != blockPoint {
		return errors.New(
			"physical adoption differs from its exact block point",
		)
	}
	return nil
}

func sameAuthorityPhysicalRollbackRow(
	left authorityPhysicalRollbackRow,
	right authorityPhysicalRollbackRow,
) bool {
	if !left.RecordedAt.Equal(right.RecordedAt) {
		return false
	}
	left.RecordedAt = time.Time{}
	right.RecordedAt = time.Time{}
	return reflect.DeepEqual(left, right)
}

func decodeAuthorityPhysicalRollbackRows(
	rows []authorityPhysicalRollbackRow,
	eventSeq uint64,
) (
	decoded authorityPhysicalRollbackRow,
	toPoint authorityPoint,
	oldTipPoint authorityPoint,
	evidenceDigest authorityHash,
	found bool,
	err error,
) {
	defer func() {
		err = invalidAuthorityError(err)
	}()
	if eventSeq == 0 {
		return authorityPhysicalRollbackRow{}, authorityPoint{}, authorityPoint{},
			authorityHash{}, false, errors.New(
				"physical rollback request has event zero",
			)
	}
	if len(rows) == 0 {
		return authorityPhysicalRollbackRow{}, authorityPoint{}, authorityPoint{},
			authorityHash{}, false, nil
	}
	if len(rows) >= 9 {
		return authorityPhysicalRollbackRow{}, authorityPoint{}, authorityPoint{},
			authorityHash{}, false, errors.New(
				"physical rollback has at least nine rows",
			)
	}
	var (
		row    authorityPhysicalRollbackRow
		to     authorityPoint
		oldTip authorityPoint
		digest authorityHash
	)
	for index, physical := range rows {
		decodedTo, decodedOld, decodedDigest, err :=
			decodeAuthorityPhysicalRollbackRow(physical, eventSeq)
		if err != nil {
			return authorityPhysicalRollbackRow{}, authorityPoint{},
				authorityPoint{}, authorityHash{}, false, err
		}
		if index == 0 {
			row = physical
			to = decodedTo
			oldTip = decodedOld
			digest = decodedDigest
			continue
		}
		if !sameAuthorityPhysicalRollbackRow(row, physical) {
			return authorityPhysicalRollbackRow{}, authorityPoint{},
				authorityPoint{}, authorityHash{}, false, errors.New(
					"physical rollback rows conflict",
				)
		}
	}
	return row, to, oldTip, digest, true, nil
}

func decodeAuthorityPhysicalRollbackRow(
	row authorityPhysicalRollbackRow,
	eventSeq uint64,
) (
	toPoint authorityPoint,
	oldTipPoint authorityPoint,
	evidenceDigest authorityHash,
	err error,
) {
	defer func() {
		err = invalidAuthorityError(err)
	}()
	if row.RollbackID == uuid.Nil ||
		row.EventSeq == 0 ||
		row.EventSeq != eventSeq ||
		row.EventSeq <= row.OldTipEventSeq {
		return authorityPoint{}, authorityPoint{}, authorityHash{}, errors.New(
			"physical rollback has invalid exact event identity",
		)
	}
	if row.CheckID == uuid.Nil ||
		row.AgreementGroup == nil ||
		*row.AgreementGroup == uuid.Nil ||
		row.WriterID == uuid.Nil ||
		row.CheckAttempt == 0 {
		return authorityPoint{}, authorityPoint{}, authorityHash{}, errors.New(
			"physical rollback authority UUID/attempt is incomplete",
		)
	}
	if row.RecordedAt.IsZero() ||
		row.RecordedAt != normalizeAuthorityTime(row.RecordedAt) {
		return authorityPoint{}, authorityPoint{}, authorityHash{}, errors.New(
			"physical rollback recorded time is not UTC-microcanonical",
		)
	}
	if strings.TrimSpace(row.Reason) == "" ||
		row.CorroborationRequired < 2 ||
		row.EvidenceCount < uint32(row.CorroborationRequired) ||
		row.EvidenceCount > 65535 ||
		len(row.ObservedOperators) < int(row.CorroborationRequired) ||
		uint32(len(row.ObservedOperators)) > row.EvidenceCount ||
		len(row.ObservedPeers) != len(row.ObservedOperators) {
		return authorityPoint{}, authorityPoint{}, authorityHash{}, errors.New(
			"physical rollback count/observer shape is invalid",
		)
	}
	operators := make(map[string]struct{}, len(row.ObservedOperators))
	for index, label := range row.ObservedOperators {
		operator := strings.ToLower(strings.TrimSpace(label))
		peer := strings.TrimSpace(row.ObservedPeers[index])
		if operator == "" || peer == "" {
			return authorityPoint{}, authorityPoint{}, authorityHash{}, errors.New(
				"physical rollback observer is not canonical",
			)
		}
		if _, duplicate := operators[operator]; duplicate {
			return authorityPoint{}, authorityPoint{}, authorityHash{}, errors.New(
				"physical rollback repeats a normalized operator",
			)
		}
		operators[operator] = struct{}{}
	}
	to, err := authorityPointFromDB(
		row.ToOrigin,
		row.ToSlot,
		row.ToHash,
		row.ToBlockNumber,
		row.ToIsByronEBB,
	)
	if err != nil {
		return authorityPoint{}, authorityPoint{}, authorityHash{}, fmt.Errorf(
			"physical rollback target: %w",
			err,
		)
	}
	if err := validateAuthorityPoint("physical rollback target", to); err != nil {
		return authorityPoint{}, authorityPoint{}, authorityHash{}, err
	}
	oldTip, err := authorityPointFromDB(
		row.OldTipSlot == nil,
		row.OldTipSlot,
		row.OldTipHash,
		row.OldTipBlockNumber,
		row.OldTipIsByronEBB,
	)
	if err != nil {
		return authorityPoint{}, authorityPoint{}, authorityHash{}, fmt.Errorf(
			"physical rollback old tip: %w",
			err,
		)
	}
	if err := validateAuthorityPoint("physical rollback old tip", oldTip); err != nil {
		return authorityPoint{}, authorityPoint{}, authorityHash{}, err
	}
	digest, err := fixedAuthorityHash(row.EvidenceDigest)
	if err != nil {
		return authorityPoint{}, authorityPoint{}, authorityHash{}, fmt.Errorf(
			"physical rollback evidence digest: %w",
			err,
		)
	}
	if digest == (authorityHash{}) {
		return authorityPoint{}, authorityPoint{}, authorityHash{}, errors.New(
			"physical rollback evidence digest is zero",
		)
	}
	return to, oldTip, digest, nil
}

func authorityPhysicalRollbackPhaseLimits() phaseLimits {
	return hydrationPhaseLimits(9)
}

func (store *Store) loadAuthorityPhysicalRollback(
	ctx context.Context,
	eventSeq uint64,
) (authorityPhysicalRollbackRow, authorityPoint, authorityPoint, authorityHash, bool, error) {
	queryCtx, finish := store.instrumentPhase(
		ctx,
		"authority_physical_rollback",
		authorityPhysicalRollbackPhaseLimits(),
	)
	defer finish()
	rows, err := store.conn.Query(
		queryCtx,
		authorityPhysicalRollbackSQL,
		eventSeq,
	)
	if err != nil {
		return authorityPhysicalRollbackRow{}, authorityPoint{}, authorityPoint{},
			authorityHash{}, false,
			mapQueryError("authority_physical_rollback", err)
	}
	defer rows.Close()
	physical := make([]authorityPhysicalRollbackRow, 0, 9)
	for rows.Next() {
		var row authorityPhysicalRollbackRow
		if err := rows.ScanStruct(&row); err != nil {
			return authorityPhysicalRollbackRow{}, authorityPoint{},
				authorityPoint{}, authorityHash{}, false,
				fmt.Errorf("scan physical rollback: %w", err)
		}
		physical = append(physical, row)
	}
	if err := rows.Err(); err != nil {
		return authorityPhysicalRollbackRow{}, authorityPoint{}, authorityPoint{},
			authorityHash{}, false, fmt.Errorf(
				"iterate physical rollback: %w",
				err,
			)
	}
	return decodeAuthorityPhysicalRollbackRows(physical, eventSeq)
}

func validateAuthorityPendingRollbackHeader(
	pending authorityPendingRollback,
	row authorityPhysicalRollbackRow,
	to authorityPoint,
	oldTip authorityPoint,
	digest authorityHash,
) (err error) {
	defer func() {
		err = invalidAuthorityError(err)
	}()
	if pending.ID != authorityUUID(row.RollbackID) ||
		pending.EventSeq != row.EventSeq ||
		pending.To != to ||
		pending.OldPhysical != (authorityHead{
			EventSeq: row.OldTipEventSeq,
			Point:    oldTip,
		}) ||
		pending.Depth != row.Depth ||
		pending.Reason != row.Reason ||
		!reflect.DeepEqual(pending.Peers, row.ObservedPeers) ||
		!reflect.DeepEqual(pending.Operators, row.ObservedOperators) ||
		pending.Required != row.CorroborationRequired ||
		pending.CheckID != authorityUUID(row.CheckID) ||
		row.AgreementGroup == nil ||
		pending.Group != authorityUUID(*row.AgreementGroup) ||
		pending.CheckAttempt != row.CheckAttempt ||
		pending.CheckedEventSeq != row.CheckedEventSeq ||
		pending.EvidenceCount != row.EvidenceCount ||
		pending.EvidenceDigest != digest ||
		pending.WriterID != authorityUUID(row.WriterID) ||
		!pending.StartedAt.Equal(row.RecordedAt) {
		return errors.New(
			"physical rollback header differs from exact pending authority",
		)
	}
	return nil
}

type authorityRollbackEvidenceBinding struct {
	Commitment authorityEvidenceCommitment
	Outcome    authorityEvidenceOutcome
	Agreed     map[string]string
}

func bindAuthorityRollbackEvidence(
	rows []authorityObservationRow,
	checkID [16]byte,
	group [16]byte,
	attempt uint32,
	required uint16,
	checked authorityHead,
) (binding authorityRollbackEvidenceBinding, err error) {
	defer func() {
		err = invalidAuthorityError(err)
	}()
	commitment, outcome, err := bindAuthorityEvidenceSet(
		rows,
		checkID,
		group,
		attempt,
		required,
		checked,
	)
	if err != nil {
		return authorityRollbackEvidenceBinding{}, err
	}
	agreed := make(map[string]string, outcome.Confirmed)
	for _, physical := range commitment.Rows {
		if physical.Observation.Result != "agreed" {
			continue
		}
		peer := physical.Observation.PeerHost
		if strings.TrimSpace(peer) == "" {
			return authorityRollbackEvidenceBinding{}, errors.New(
				"agreed evidence peer is empty",
			)
		}
		agreed[physical.OperatorKey] = peer
	}
	if len(agreed) != int(outcome.Confirmed) {
		return authorityRollbackEvidenceBinding{}, errors.New(
			"agreed evidence operator map has wrong cardinality",
		)
	}
	return authorityRollbackEvidenceBinding{
		Commitment: commitment,
		Outcome:    outcome,
		Agreed:     agreed,
	}, nil
}

func validateAuthorityRollbackObserverMap(
	peers []string,
	operators []string,
	agreed map[string]string,
) (err error) {
	defer func() {
		err = invalidAuthorityError(err)
	}()
	if len(peers) != len(agreed) ||
		len(operators) != len(agreed) {
		return errors.New(
			"rollback observers differ from canonical agreed evidence map",
		)
	}
	seen := make(map[string]struct{}, len(operators))
	for index, label := range operators {
		operator := strings.ToLower(strings.TrimSpace(label))
		peer := strings.TrimSpace(peers[index])
		if operator == "" || peer == "" {
			return errors.New(
				"rollback observer identity is empty after normalization",
			)
		}
		if _, duplicate := seen[operator]; duplicate {
			return errors.New(
				"rollback observers repeat a normalized operator",
			)
		}
		seen[operator] = struct{}{}
		evidencePeer, exists := agreed[operator]
		if !exists || evidencePeer != peer {
			return errors.New(
				"rollback observer pair differs from canonical agreed evidence",
			)
		}
	}
	return nil
}

func validateAuthorityFinalizedRollbackHeader(
	record authorityRecord,
	row authorityPhysicalRollbackRow,
	to authorityPoint,
	digest authorityHash,
	evidenceRows []authorityObservationRow,
) (err error) {
	defer func() {
		err = invalidAuthorityError(err)
	}()
	head := authorityHead{EventSeq: row.EventSeq, Point: to}
	if record.Physical != head ||
		record.LastAgreed == nil ||
		*record.LastAgreed != head ||
		record.LastAgreedAt == nil ||
		!record.LastAgreedAt.Equal(row.RecordedAt) ||
		record.LastAgreedEvidence == nil {
		return errors.New(
			"finalized rollback header differs from physical/last-agreed authority",
		)
	}
	reference := *record.LastAgreedEvidence
	if reference.Required < 2 ||
		reference.Confirmed < reference.Required ||
		uint32(reference.Confirmed) > reference.Count ||
		reference.Digest == (authorityHash{}) ||
		row.CheckID != uuid.UUID(reference.CheckID) ||
		row.AgreementGroup == nil ||
		*row.AgreementGroup != uuid.UUID(reference.Group) ||
		row.CheckAttempt != reference.Attempt ||
		row.CorroborationRequired != reference.Required ||
		row.CheckedEventSeq != reference.Checked.EventSeq ||
		to != reference.Checked.Point ||
		row.EvidenceCount != reference.Count ||
		digest != reference.Digest {
		return errors.New(
			"finalized rollback header differs from last-agreed evidence reference",
		)
	}
	evidence, err := bindAuthorityRollbackEvidence(
		evidenceRows,
		reference.CheckID,
		reference.Group,
		reference.Attempt,
		reference.Required,
		reference.Checked,
	)
	if err != nil {
		return err
	}
	if evidence.Commitment.Count != reference.Count ||
		evidence.Commitment.Digest != reference.Digest ||
		evidence.Outcome.Confirmed != reference.Confirmed ||
		evidence.Outcome.Disagreement {
		return errors.New(
			"last-agreed evidence rows differ from immutable reference",
		)
	}
	if err := validateAuthorityRollbackObserverMap(
		row.ObservedPeers,
		row.ObservedOperators,
		evidence.Agreed,
	); err != nil {
		return fmt.Errorf("finalized rollback observers: %w", err)
	}
	return nil
}
