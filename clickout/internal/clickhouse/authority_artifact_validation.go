package clickhouse

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
)

type authorityArtifactKind string

const (
	authorityArtifactAdoption     authorityArtifactKind = "adoption"
	authorityArtifactRollback     authorityArtifactKind = "rollback"
	authorityArtifactInvalidation authorityArtifactKind = "invalidation"
)

const authorityAdoptionAtEventProbeSQL = `
SELECT event_seq
FROM chain_events
PREWHERE event_kind = 'adoption'
  AND event_seq = ?
ORDER BY event_kind, event_seq, publication_id
LIMIT 1`

const authorityRollbackAtEventProbeSQL = `
SELECT event_seq
FROM rollbacks
PREWHERE event_seq = ?
ORDER BY event_seq, rollback_id
LIMIT 1`

const authorityInvalidationAtEventProbeSQL = `
SELECT event_seq
FROM chain_events
PREWHERE event_kind = 'invalidation'
  AND event_seq = ?
ORDER BY event_kind, event_seq, publication_id
LIMIT 1`

const authorityAdoptionAfterProbeSQL = `
SELECT event_seq
FROM chain_events
PREWHERE event_kind = 'adoption'
  AND event_seq > ?
ORDER BY event_kind, event_seq, publication_id
LIMIT 1`

const authorityRollbackAfterProbeSQL = `
SELECT event_seq
FROM rollbacks
PREWHERE event_seq > ?
ORDER BY event_seq, rollback_id
LIMIT 1`

const authorityInvalidationAfterProbeSQL = `
SELECT event_seq
FROM chain_events
PREWHERE event_kind = 'invalidation'
  AND event_seq > ?
ORDER BY event_kind, event_seq, publication_id
LIMIT 1`

const authorityRollbackBetweenProbeSQL = `
SELECT event_seq
FROM rollbacks
PREWHERE event_seq > ?
  AND event_seq < ?
ORDER BY event_seq, rollback_id
LIMIT 1`

const authorityInvalidationBetweenProbeSQL = `
SELECT event_seq
FROM chain_events
PREWHERE event_kind = 'invalidation'
  AND event_seq > ?
  AND event_seq < ?
ORDER BY event_kind, event_seq, publication_id
LIMIT 1`

func authorityArtifactProbePhaseLimits() phaseLimits {
	return hydrationPhaseLimits(1)
}

func (store *Store) runAuthorityArtifactProbe(
	ctx context.Context,
	phase string,
	query string,
	args ...any,
) (uint64, bool, error) {
	if err := ctx.Err(); err != nil {
		return 0, false, err
	}
	queryCtx, finish := store.instrumentPhase(
		ctx,
		phase,
		authorityArtifactProbePhaseLimits(),
	)
	defer finish()
	rows, err := store.conn.Query(queryCtx, query, args...)
	if err != nil {
		return 0, false, mapQueryError(phase, err)
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return 0, false, fmt.Errorf("iterate %s: %w", phase, err)
		}
		return 0, false, nil
	}
	var eventSeq uint64
	if err := rows.Scan(&eventSeq); err != nil {
		return 0, false, fmt.Errorf("scan %s: %w", phase, err)
	}
	if rows.Next() {
		return 0, false, invalidAuthorityError(
			fmt.Errorf("%s returned more than LIMIT 1", phase),
		)
	}
	if err := rows.Err(); err != nil {
		return 0, false, fmt.Errorf("iterate %s: %w", phase, err)
	}
	return eventSeq, true, nil
}

type authorityArtifactAtProbe func(
	context.Context,
	authorityArtifactKind,
	uint64,
) (bool, error)

func (store *Store) probeAuthorityArtifactAt(
	ctx context.Context,
	kind authorityArtifactKind,
	eventSeq uint64,
) (bool, error) {
	var query string
	switch kind {
	case authorityArtifactAdoption:
		query = authorityAdoptionAtEventProbeSQL
	case authorityArtifactRollback:
		query = authorityRollbackAtEventProbeSQL
	case authorityArtifactInvalidation:
		query = authorityInvalidationAtEventProbeSQL
	default:
		return false, invalidAuthorityError(
			fmt.Errorf("unknown authority artifact kind %q", kind),
		)
	}
	foundEvent, found, err := store.runAuthorityArtifactProbe(
		ctx,
		"authority_"+string(kind)+"_at_event",
		query,
		eventSeq,
	)
	if err != nil || !found {
		return found, err
	}
	if foundEvent != eventSeq {
		return false, invalidAuthorityError(
			errors.New(
				"authority exact-event probe returned a different event",
			),
		)
	}
	return true, nil
}

type authorityArtifactRangeProbe func(
	context.Context,
	authorityArtifactKind,
	uint64,
	*uint64,
) (uint64, bool, error)

func (store *Store) probeAuthorityArtifactRange(
	ctx context.Context,
	kind authorityArtifactKind,
	lowerExclusive uint64,
	upperExclusive *uint64,
) (uint64, bool, error) {
	var (
		query string
		args  []any
	)
	switch {
	case upperExclusive == nil && kind == authorityArtifactAdoption:
		query = authorityAdoptionAfterProbeSQL
		args = []any{lowerExclusive}
	case upperExclusive == nil && kind == authorityArtifactRollback:
		query = authorityRollbackAfterProbeSQL
		args = []any{lowerExclusive}
	case upperExclusive == nil && kind == authorityArtifactInvalidation:
		query = authorityInvalidationAfterProbeSQL
		args = []any{lowerExclusive}
	case upperExclusive != nil && kind == authorityArtifactRollback:
		if *upperExclusive <= lowerExclusive {
			return 0, false, invalidAuthorityError(
				errors.New(
					"authority rollback range probe is empty/reversed",
				),
			)
		}
		query = authorityRollbackBetweenProbeSQL
		args = []any{lowerExclusive, *upperExclusive}
	case upperExclusive != nil && kind == authorityArtifactInvalidation:
		if *upperExclusive <= lowerExclusive {
			return 0, false, invalidAuthorityError(
				errors.New(
					"authority invalidation range probe is empty/reversed",
				),
			)
		}
		query = authorityInvalidationBetweenProbeSQL
		args = []any{lowerExclusive, *upperExclusive}
	default:
		return 0, false, invalidAuthorityError(
			fmt.Errorf(
				"unsupported authority artifact range probe %q",
				kind,
			),
		)
	}
	foundEvent, found, err := store.runAuthorityArtifactProbe(
		ctx,
		"authority_"+string(kind)+"_range",
		query,
		args...,
	)
	if err != nil || !found {
		return foundEvent, found, err
	}
	if foundEvent <= lowerExclusive ||
		(upperExclusive != nil && foundEvent >= *upperExclusive) {
		return 0, false, invalidAuthorityError(
			errors.New(
				"authority range probe returned an event outside its range",
			),
		)
	}
	return foundEvent, true, nil
}

type authorityArtifactReaders struct {
	loadAdoption func(
		context.Context,
		uint64,
	) (authorityPhysicalAdoptionRow, authorityPoint, bool, error)
	loadBlock func(
		context.Context,
		uint64,
	) (authorityPhysicalBlockRow, authorityPoint, bool, error)
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
	loadEvidence func(
		context.Context,
		[16]byte,
	) ([]authorityObservationRow, error)
	validateAdoption func(
		authorityRecord,
		authorityPhysicalAdoptionRow,
		authorityPoint,
		authorityPhysicalBlockRow,
		authorityPoint,
	) error
	validateAdoptionLifecycle func(
		context.Context,
		authorityRecord,
		authorityPhysicalAdoptionRow,
		authorityPhysicalBlockRow,
		authorityPoint,
	) error
	validateFinalizedRollback func(
		authorityRecord,
		authorityPhysicalRollbackRow,
		authorityPoint,
		authorityHash,
		[]authorityObservationRow,
	) error
	validateInvalidations func(
		context.Context,
		authorityRecord,
		authorityPhysicalRollbackRow,
		authorityPoint,
		authorityPoint,
	) (bool, error)
	probeAt authorityArtifactAtProbe
}

func (store *Store) authorityArtifactValidationReaders() authorityArtifactReaders {
	return authorityArtifactReaders{
		loadAdoption:              store.loadAuthorityPhysicalAdoption,
		loadBlock:                 store.loadAuthorityPhysicalBlock,
		loadRollback:              store.loadAuthorityPhysicalRollback,
		loadEvidence:              store.loadAuthorityObservationRows,
		validateAdoption:          validateAuthorityPhysicalAdoptionMapping,
		validateAdoptionLifecycle: store.validateAuthorityCurrentAdoptionLifecycle,
		validateFinalizedRollback: validateAuthorityFinalizedRollbackHeader,
		validateInvalidations:     store.validateAuthorityRollbackInvalidations,
		probeAt:                   store.probeAuthorityArtifactAt,
	}
}

func validateAuthorityExactHeadArtifacts(
	ctx context.Context,
	record authorityRecord,
	head authorityHead,
	readers authorityArtifactReaders,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	eventSeq := head.EventSeq
	if eventSeq == 0 {
		if head != (authorityHead{
			EventSeq: 0,
			Point:    record.Start,
		}) {
			return invalidAuthorityError(
				errors.New(
					"event-zero authority differs from immutable start",
				),
			)
		}
		return nil
	}

	// The existing current-head validators are deliberately reused against a
	// projection whose Physical coordinate is the exact head being proved.
	// This keeps current and historical Effective artifact semantics identical
	// without weakening the finalized-Physical wrapper.
	projected := record
	projected.Physical = head

	adoption, adoptionPoint, adoptionFound, err :=
		readers.loadAdoption(ctx, eventSeq)
	if err != nil {
		return err
	}
	rollback, rollbackTo, oldTip, digest, rollbackFound, err :=
		readers.loadRollback(ctx, eventSeq)
	if err != nil {
		return err
	}
	if adoptionFound == rollbackFound {
		return invalidAuthorityError(
			errors.New(
				"authority event does not have exactly one adoption/rollback header",
			),
		)
	}
	if adoptionFound {
		block, blockPoint, found, err := readers.loadBlock(
			ctx,
			adoption.PublicationID,
		)
		if err != nil {
			return err
		}
		if !found {
			return invalidAuthorityError(
				errors.New(
					"authority adoption has no exact block publication",
				),
			)
		}
		if err := readers.validateAdoption(
			projected,
			adoption,
			adoptionPoint,
			block,
			blockPoint,
		); err != nil {
			return err
		}
		if err := readers.validateAdoptionLifecycle(
			ctx,
			projected,
			adoption,
			block,
			blockPoint,
		); err != nil {
			return err
		}
		foundInvalidation, err := readers.probeAt(
			ctx,
			authorityArtifactInvalidation,
			eventSeq,
		)
		if err != nil {
			return err
		}
		if foundInvalidation {
			return invalidAuthorityError(
				errors.New(
					"authority adoption event has a same-event invalidation",
				),
			)
		}
		return nil
	}

	evidence, err := readers.loadEvidence(
		ctx,
		authorityUUID(rollback.CheckID),
	)
	if err != nil {
		return err
	}
	if err := readers.validateFinalizedRollback(
		projected,
		rollback,
		rollbackTo,
		digest,
		evidence,
	); err != nil {
		return err
	}
	complete, err := readers.validateInvalidations(
		ctx,
		projected,
		rollback,
		rollbackTo,
		oldTip,
	)
	if err != nil {
		return err
	}
	if !complete {
		return invalidAuthorityError(
			errors.New(
				"authority rollback has no exact invalidation set",
			),
		)
	}
	return nil
}

func validateAuthorityCurrentPhysicalArtifacts(
	ctx context.Context,
	record authorityRecord,
	readers authorityArtifactReaders,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	for _, kind := range []authorityArtifactKind{
		authorityArtifactAdoption,
		authorityArtifactRollback,
		authorityArtifactInvalidation,
	} {
		found, err := readers.probeAt(ctx, kind, 0)
		if err != nil {
			return err
		}
		if found {
			return invalidAuthorityError(
				fmt.Errorf(
					"authority contains a %s artifact at reserved event zero",
					kind,
				),
			)
		}
	}
	return validateAuthorityExactHeadArtifacts(
		ctx,
		record,
		record.Physical,
		readers,
	)
}

func validateAuthorityHistoricalEffectiveArtifacts(
	ctx context.Context,
	record authorityRecord,
	readers authorityArtifactReaders,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if record.Effective == record.Physical {
		return nil
	}
	return validateAuthorityExactHeadArtifacts(
		ctx,
		record,
		record.Effective,
		readers,
	)
}

func authorityPhysicalRollbackFromPending(
	pending authorityPendingRollback,
) authorityPhysicalRollbackRow {
	row := authorityPhysicalRollbackRow{
		RollbackID:            uuid.UUID(pending.ID),
		EventSeq:              pending.EventSeq,
		ToOrigin:              pending.To.Origin,
		ToIsByronEBB:          pending.To.IsByronEBB,
		OldTipIsByronEBB:      pending.OldPhysical.Point.IsByronEBB,
		OldTipEventSeq:        pending.OldPhysical.EventSeq,
		Depth:                 pending.Depth,
		Reason:                pending.Reason,
		ObservedPeers:         append([]string(nil), pending.Peers...),
		ObservedOperators:     append([]string(nil), pending.Operators...),
		CorroborationRequired: pending.Required,
		CheckID:               uuid.UUID(pending.CheckID),
		CheckAttempt:          pending.CheckAttempt,
		CheckedEventSeq:       pending.CheckedEventSeq,
		EvidenceCount:         pending.EvidenceCount,
		EvidenceDigest:        string(pending.EvidenceDigest[:]),
		WriterID:              uuid.UUID(pending.WriterID),
		RecordedAt:            pending.StartedAt,
	}
	group := uuid.UUID(pending.Group)
	row.AgreementGroup = &group
	if !pending.To.Origin {
		slot := pending.To.Slot
		hash := string(pending.To.Hash[:])
		number := pending.To.BlockNumber
		row.ToSlot = &slot
		row.ToHash = &hash
		row.ToBlockNumber = &number
	}
	if !pending.OldPhysical.Point.Origin {
		slot := pending.OldPhysical.Point.Slot
		hash := string(pending.OldPhysical.Point.Hash[:])
		number := pending.OldPhysical.Point.BlockNumber
		row.OldTipSlot = &slot
		row.OldTipHash = &hash
		row.OldTipBlockNumber = &number
	}
	return row
}

func validateAuthorityPendingRollbackReservationState(
	record authorityRecord,
	pending authorityPendingRollback,
) (err error) {
	defer func() {
		err = invalidAuthorityError(err)
	}()
	checked := authorityHead{
		EventSeq: pending.CheckedEventSeq,
		Point:    pending.To,
	}
	if record.TrustStatus != "checking" ||
		record.CheckCompletedAt != nil ||
		record.CorroborationConfirmed != 0 ||
		record.Disagreement ||
		record.CheckID == nil ||
		*record.CheckID != pending.CheckID ||
		record.AgreementGroup == nil ||
		*record.AgreementGroup != pending.Group ||
		record.CheckAttempt != pending.CheckAttempt ||
		record.CorroborationRequired != pending.Required ||
		record.Checked == nil ||
		*record.Checked != checked {
		return errors.New(
			"pending rollback differs from the exact incomplete current check",
		)
	}
	if record.EvidenceState != "frozen" ||
		record.PendingEvidenceWrite != nil ||
		record.EvidenceDigest == nil ||
		record.EvidenceCount != pending.EvidenceCount ||
		*record.EvidenceDigest != pending.EvidenceDigest {
		return errors.New(
			"pending rollback differs from the frozen current evidence commitment",
		)
	}
	return nil
}

func validateAuthorityPendingRollbackEvidence(
	pending authorityPendingRollback,
	rows []authorityObservationRow,
) (err error) {
	defer func() {
		err = invalidAuthorityError(err)
	}()
	checked := authorityHead{
		EventSeq: pending.CheckedEventSeq,
		Point:    pending.To,
	}
	evidence, err := bindAuthorityRollbackEvidence(
		rows,
		pending.CheckID,
		pending.Group,
		pending.CheckAttempt,
		pending.Required,
		checked,
	)
	if err != nil {
		return fmt.Errorf("bind pending rollback evidence: %w", err)
	}
	if evidence.Commitment.Count != pending.EvidenceCount ||
		evidence.Commitment.Digest != pending.EvidenceDigest {
		return errors.New(
			"pending rollback evidence differs from the frozen commitment",
		)
	}
	if evidence.Outcome.Disagreement ||
		evidence.Outcome.Confirmed < pending.Required {
		return errors.New(
			"pending rollback lacks canonical independent agreement threshold",
		)
	}
	if err := validateAuthorityRollbackObserverMap(
		pending.Peers,
		pending.Operators,
		evidence.Agreed,
	); err != nil {
		return fmt.Errorf("pending rollback observers: %w", err)
	}
	return nil
}

func validateAuthorityPendingRollbackArtifacts(
	ctx context.Context,
	record authorityRecord,
	readers authorityArtifactReaders,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if record.PendingRollback == nil {
		return invalidAuthorityError(
			errors.New(
				"pending rollback artifact validation has no reservation",
			),
		)
	}
	pending := *record.PendingRollback
	if pending.OldPhysical != record.Physical ||
		pending.EventSeq <= record.Physical.EventSeq {
		return invalidAuthorityError(
			errors.New(
				"pending rollback is not anchored above the physical authority",
			),
		)
	}
	switch pending.State {
	case "reserved", "invalidations_written":
	default:
		return invalidAuthorityError(
			fmt.Errorf("unknown pending rollback state %q", pending.State),
		)
	}
	if err := validateAuthorityPendingRollbackReservationState(
		record,
		pending,
	); err != nil {
		return err
	}
	evidence, err := readers.loadEvidence(ctx, pending.CheckID)
	if err != nil {
		return err
	}
	if err := validateAuthorityPendingRollbackEvidence(
		pending,
		evidence,
	); err != nil {
		return err
	}

	synthetic := authorityPhysicalRollbackFromPending(pending)
	_, to, oldTip, digest, _, err := decodeAuthorityPhysicalRollbackRows(
		[]authorityPhysicalRollbackRow{synthetic},
		pending.EventSeq,
	)
	if err != nil {
		return fmt.Errorf("decode pending rollback authority: %w", err)
	}
	if err := validateAuthorityPendingRollbackHeader(
		pending,
		synthetic,
		to,
		oldTip,
		digest,
	); err != nil {
		return err
	}

	header, headerTo, headerOldTip, headerDigest, headerFound, err :=
		readers.loadRollback(ctx, pending.EventSeq)
	if err != nil {
		return err
	}
	if headerFound {
		if err := validateAuthorityPendingRollbackHeader(
			pending,
			header,
			headerTo,
			headerOldTip,
			headerDigest,
		); err != nil {
			return err
		}
	}
	if pending.State == "reserved" && headerFound {
		return invalidAuthorityError(
			errors.New(
				"reserved pending rollback already has a physical header",
			),
		)
	}

	complete, err := readers.validateInvalidations(
		ctx,
		record,
		synthetic,
		to,
		oldTip,
	)
	if err != nil {
		return err
	}
	if pending.State == "invalidations_written" && !complete {
		return invalidAuthorityError(
			errors.New(
				"pending invalidations-written rollback lacks its exact set",
			),
		)
	}
	return nil
}

func validateAuthorityArtifactBarriers(
	ctx context.Context,
	record authorityRecord,
	probe authorityArtifactRangeProbe,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if probe == nil {
		return invalidAuthorityError(
			errors.New("authority artifact barrier has a nil probe"),
		)
	}
	physical := record.Physical.EventSeq
	if record.PendingRollback == nil {
		for _, kind := range []authorityArtifactKind{
			authorityArtifactRollback,
			authorityArtifactInvalidation,
		} {
			eventSeq, found, err := probe(ctx, kind, physical, nil)
			if err != nil {
				return err
			}
			if found {
				return invalidAuthorityError(
					fmt.Errorf(
						"unreserved %s artifact at event %d above physical event %d",
						kind,
						eventSeq,
						physical,
					),
				)
			}
		}
		return nil
	}

	pending := record.PendingRollback
	if pending.OldPhysical != record.Physical ||
		pending.EventSeq <= physical {
		return invalidAuthorityError(
			errors.New(
				"pending rollback barrier is not anchored above physical authority",
			),
		)
	}
	if eventSeq, found, err := probe(
		ctx,
		authorityArtifactAdoption,
		physical,
		nil,
	); err != nil {
		return err
	} else if found {
		return invalidAuthorityError(
			fmt.Errorf(
				"adoption artifact at event %d above pending physical anchor %d",
				eventSeq,
				physical,
			),
		)
	}

	upper := pending.EventSeq
	for _, kind := range []authorityArtifactKind{
		authorityArtifactRollback,
		authorityArtifactInvalidation,
	} {
		eventSeq, found, err := probe(ctx, kind, physical, &upper)
		if err != nil {
			return err
		}
		if found {
			return invalidAuthorityError(
				fmt.Errorf(
					"unreserved %s artifact at gap event %d between physical %d and pending %d",
					kind,
					eventSeq,
					physical,
					pending.EventSeq,
				),
			)
		}
		eventSeq, found, err = probe(
			ctx,
			kind,
			pending.EventSeq,
			nil,
		)
		if err != nil {
			return err
		}
		if found {
			return invalidAuthorityError(
				fmt.Errorf(
					"unreserved %s artifact at event %d above pending event %d",
					kind,
					eventSeq,
					pending.EventSeq,
				),
			)
		}
	}
	return nil
}

func (store *Store) validateAuthorityPhysicalArtifacts(
	ctx context.Context,
	record authorityRecord,
) error {
	readers := store.authorityArtifactValidationReaders()
	if err := validateAuthorityCurrentPhysicalArtifacts(
		ctx,
		record,
		readers,
	); err != nil {
		return fmt.Errorf("validate current physical authority: %w", err)
	}
	if record.PendingRollback != nil {
		if err := validateAuthorityPendingRollbackArtifacts(
			ctx,
			record,
			readers,
		); err != nil {
			return fmt.Errorf("validate pending rollback authority: %w", err)
		}
	}
	if err := validateAuthorityArtifactBarriers(
		ctx,
		record,
		store.probeAuthorityArtifactRange,
	); err != nil {
		return fmt.Errorf("validate authority artifact barriers: %w", err)
	}
	return nil
}

func (store *Store) validateAuthorityRecordArtifacts(
	ctx context.Context,
	record authorityRecord,
) error {
	if err := store.validateAuthorityPhysicalArtifacts(ctx, record); err != nil {
		return err
	}
	if err := validateAuthorityHistoricalEffectiveArtifacts(
		ctx,
		record,
		store.authorityArtifactValidationReaders(),
	); err != nil {
		return fmt.Errorf("validate historical effective authority: %w", err)
	}
	return nil
}
