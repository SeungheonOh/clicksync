package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"clicksync/internal/model"
	"clicksync/internal/n2n"
	"clicksync/internal/publication"
	"clicksync/internal/syncer"
)

func (d *DB) rejectUnreservedRollbackArtifactsAfter(
	ctx context.Context,
	physicalEventSeq uint64,
) error {
	for _, probe := range []struct {
		kind  string
		query string
	}{
		{
			kind: "rollback header",
			query: `SELECT event_seq
FROM clicksync.rollbacks
PREWHERE event_seq > ?
ORDER BY event_seq, rollback_id
LIMIT 1`,
		},
		{
			kind: "rollback invalidation",
			query: `SELECT event_seq
FROM clicksync.chain_events
PREWHERE event_kind = 'invalidation'
  AND event_seq > ?
ORDER BY event_kind, event_seq, publication_id
LIMIT 1`,
		},
	} {
		rows, err := d.conn.Query(ctx, probe.query, physicalEventSeq)
		if err != nil {
			return fmt.Errorf("probe unreserved %s: %w", probe.kind, err)
		}
		if rows.Next() {
			var eventSeq uint64
			if err := rows.Scan(&eventSeq); err != nil {
				rows.Close()
				return fmt.Errorf("scan unreserved %s: %w", probe.kind, err)
			}
			rows.Close()
			return fmt.Errorf(
				"ordinary reconciliation found unreserved %s at event %d after physical event %d",
				probe.kind,
				eventSeq,
				physicalEventSeq,
			)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return fmt.Errorf("iterate unreserved %s probe: %w", probe.kind, err)
		}
		if err := rows.Close(); err != nil {
			return fmt.Errorf("close unreserved %s probe: %w", probe.kind, err)
		}
	}
	return nil
}

func (d *DB) validateCurrentPhysicalRollbackArtifacts(
	ctx context.Context,
	record manifestRecord,
) error {
	eventSeq := record.Physical.EventSeq
	adoption, adoptionFound, err := d.committedAdoptionHeader(ctx, eventSeq)
	if err != nil {
		return err
	}
	_, rollbackFound, err := d.committedRollbackPoint(ctx, eventSeq)
	if err != nil {
		return err
	}
	if adoptionFound && rollbackFound {
		return errors.New("current physical event has both adoption and rollback headers")
	}
	if adoptionFound {
		adoptionPoint := publication.Point{
			Slot:        adoption.Slot,
			Hash:        adoption.Hash,
			BlockNumber: adoption.BlockNumber,
			IsByronEBB:  adoption.IsByronEBB,
		}
		synthetic, err := d.validateAdoptedBlockIdentity(ctx, adoption)
		if err != nil {
			return err
		}
		if adoption.EventSeq != eventSeq ||
			(!synthetic && adoptionPoint != record.Physical.Point) ||
			(synthetic && !record.Physical.Point.Origin) {
			return errors.New("current physical adoption differs from manifest head")
		}
		return d.rejectInvalidationAtEvent(ctx, eventSeq)
	}
	if !rollbackFound {
		if eventSeq == 0 {
			return d.rejectInvalidationAtEvent(ctx, eventSeq)
		}
		return errors.New("current manifest physical event has no exact physical header")
	}
	commit, err := d.rollbackCommitAtEvent(ctx, eventSeq)
	if err != nil {
		return err
	}
	if commit.To != record.Physical.Point ||
		record.LastAgreed == nil ||
		*record.LastAgreed != record.Physical ||
		record.LastAgreedEvidence == nil {
		return errors.New("current rollback header lacks exact manifest agreement authority")
	}
	reference := record.LastAgreedEvidence
	if commit.CheckID == nil ||
		commit.AgreementGroup == nil ||
		*commit.CheckID != reference.CheckID ||
		*commit.AgreementGroup != reference.Group ||
		commit.CheckAttempt != reference.Attempt ||
		commit.CorroborationRequired != reference.Required ||
		commit.CheckedEventSeq != reference.Checked.EventSeq ||
		commit.EvidenceCount != reference.Count ||
		commit.EvidenceDigest != reference.Digest {
		return errors.New("current rollback header differs from last-agreed evidence authority")
	}
	var descendants []publication.Descendant
	if commit.Depth != 0 {
		descendants, err = d.ActiveDescendants(
			ctx,
			commit.OldEventSeq,
			commit.To,
			commit.Depth,
		)
		if err != nil {
			return fmt.Errorf("reconstruct finalized rollback descendants: %w", err)
		}
		if len(descendants) != int(commit.Depth) ||
			descendants[0].Point != commit.OldTip {
			return errors.New("finalized rollback descendants differ from physical header")
		}
	}
	committed, err := d.invalidationsCommitted(ctx, commit, descendants)
	if err != nil {
		return fmt.Errorf("validate finalized rollback invalidations: %w", err)
	}
	if !committed {
		return errors.New("finalized rollback is missing exact invalidations")
	}
	return nil
}

func (d *DB) validatePendingRollbackArtifacts(
	ctx context.Context,
	pending manifestPendingRollback,
) error {
	adoptions, err := d.conn.Query(ctx, `
SELECT event_seq
FROM clicksync.chain_events
PREWHERE event_kind = 'adoption'
  AND event_seq >= ?
ORDER BY event_kind, event_seq, publication_id
LIMIT 1`, pending.EventSeq)
	if err != nil {
		return err
	}
	if adoptions.Next() {
		var eventSeq uint64
		if err := adoptions.Scan(&eventSeq); err != nil {
			adoptions.Close()
			return err
		}
		adoptions.Close()
		return fmt.Errorf(
			"pending rollback conflicts with adoption at event %d",
			eventSeq,
		)
	}
	if err := adoptions.Err(); err != nil {
		adoptions.Close()
		return err
	}
	adoptions.Close()
	commit := rollbackCommitFromPending(pending)
	descendants, err := d.reconstructPendingRollbackDescendants(ctx, commit)
	if err != nil {
		return err
	}
	invalidations, err := d.invalidationsCommitted(ctx, commit, descendants)
	if err != nil {
		return err
	}
	if !invalidations {
		if pending.State == "invalidations_written" {
			return errors.New("pending invalidation-stage rollback lacks exact invalidations")
		}
		if err := d.rejectInvalidationAtEvent(ctx, pending.EventSeq); err != nil {
			return errors.New("reserved rollback has a partial invalidation set")
		}
	}
	header, err := d.RollbackCommitted(ctx, commit)
	if err != nil {
		return err
	}
	switch pending.State {
	case "reserved":
		if header {
			return errors.New("reserved rollback has a header before invalidation marker")
		}
	case "invalidations_written":
		if !invalidations {
			return errors.New("rollback invalidation marker precedes exact invalidations")
		}
	default:
		return fmt.Errorf("unknown pending rollback state %q", pending.State)
	}
	return nil
}

func (d *DB) rejectInvalidationAtEvent(ctx context.Context, eventSeq uint64) error {
	rows, err := d.conn.Query(ctx, `
SELECT event_seq
FROM clicksync.chain_events
PREWHERE event_kind = 'invalidation'
  AND event_seq = ?
ORDER BY event_kind, event_seq, publication_id
LIMIT 1`, eventSeq)
	if err != nil {
		return fmt.Errorf("probe same-event rollback invalidation: %w", err)
	}
	defer rows.Close()
	if rows.Next() {
		return fmt.Errorf(
			"ordinary reconciliation found rollback invalidation at adoption event %d",
			eventSeq,
		)
	}
	return rows.Err()
}

func (d *DB) rollbackCommitAtEvent(
	ctx context.Context,
	eventSeq uint64,
) (publication.RollbackCommit, error) {
	const query = `
SELECT
    rollback_id, rollback_to_origin, rollback_to_slot, rollback_to_hash,
    rollback_to_block_number, rollback_to_is_byron_ebb,
    old_tip_slot, old_tip_hash, old_tip_block_number, old_tip_is_byron_ebb,
    old_tip_event_seq, depth, reason, observed_peers, observed_operators,
    corroboration_required, check_id, agreement_group, check_attempt,
    checked_event_seq, evidence_count, evidence_digest, writer_id, recorded_at
FROM clicksync.rollbacks
PREWHERE event_seq = ?
ORDER BY event_seq, rollback_id
LIMIT 1`
	rows, err := d.conn.Query(ctx, query, eventSeq)
	if err != nil {
		return publication.RollbackCommit{}, err
	}
	defer rows.Close()
	if !rows.Next() {
		return publication.RollbackCommit{}, errors.New("rollback header disappeared")
	}
	var (
		rollbackID     uuid.UUID
		toOrigin       bool
		toSlot         *uint64
		toHash         []byte
		toNumber       *uint64
		toEBB          bool
		oldSlot        *uint64
		oldHash        []byte
		oldNumber      *uint64
		oldEBB         bool
		oldEvent       uint64
		depth          uint32
		reason         string
		peers          []string
		operators      []string
		required       uint16
		checkID        uuid.UUID
		group          *uuid.UUID
		attempt        uint32
		checkedEvent   uint64
		evidenceCount  uint32
		evidenceDigest []byte
		writer         uuid.UUID
		recordedAt     time.Time
	)
	if err := rows.Scan(
		&rollbackID, &toOrigin, &toSlot, &toHash, &toNumber, &toEBB,
		&oldSlot, &oldHash, &oldNumber, &oldEBB, &oldEvent, &depth,
		&reason, &peers, &operators, &required, &checkID, &group, &attempt,
		&checkedEvent, &evidenceCount, &evidenceDigest, &writer, &recordedAt,
	); err != nil {
		return publication.RollbackCommit{}, err
	}
	to, err := rollbackHeaderPoint(toOrigin, toSlot, toHash, toNumber, toEBB)
	if err != nil {
		return publication.RollbackCommit{}, err
	}
	old, err := rollbackHeaderPoint(oldSlot == nil, oldSlot, oldHash, oldNumber, oldEBB)
	if err != nil {
		return publication.RollbackCommit{}, err
	}
	digest, err := hash32(evidenceDigest)
	if err != nil {
		return publication.RollbackCommit{}, err
	}
	check := [16]byte(checkID)
	if group == nil {
		return publication.RollbackCommit{}, errors.New("rollback header has no agreement group")
	}
	agreement := [16]byte(*group)
	commit := publication.RollbackCommit{
		RollbackID:            [16]byte(rollbackID),
		EventSeq:              eventSeq,
		To:                    to,
		OldTip:                old,
		OldEventSeq:           oldEvent,
		Depth:                 depth,
		Reason:                reason,
		ObservedPeers:         peers,
		ObservedOperators:     operators,
		CorroborationRequired: required,
		CheckID:               &check,
		AgreementGroup:        &agreement,
		CheckAttempt:          attempt,
		CheckedEventSeq:       checkedEvent,
		EvidenceCount:         evidenceCount,
		EvidenceDigest:        digest,
		WriterID:              [16]byte(writer),
		RecordedAt:            recordedAt,
	}
	found, err := d.RollbackCommitted(ctx, commit)
	if err != nil {
		return publication.RollbackCommit{}, err
	}
	if !found {
		return publication.RollbackCommit{}, errors.New("rollback header disappeared during validation")
	}
	return commit, nil
}

func rollbackHeaderPoint(
	origin bool,
	slot *uint64,
	hash []byte,
	number *uint64,
	isByronEBB bool,
) (publication.Point, error) {
	if origin {
		if slot != nil || len(hash) != 0 || number != nil || isByronEBB {
			return publication.Point{}, errors.New("Origin rollback point has invalid shape")
		}
		return publication.Point{Origin: true}, nil
	}
	if slot == nil || number == nil || len(hash) != len(model.Hash32{}) {
		return publication.Point{}, errors.New("rollback point is incomplete")
	}
	value, err := hash32(hash)
	if err != nil {
		return publication.Point{}, err
	}
	return publication.Point{
		Slot:        *slot,
		Hash:        value,
		BlockNumber: *number,
		IsByronEBB:  isByronEBB,
	}, nil
}

func (d *DB) ReserveRollbackManifest(
	ctx context.Context,
	authority publication.Lock,
	commit publication.RollbackCommit,
	writerBuild string,
) (publication.RollbackCommit, error) {
	if authority == nil {
		return commit, errors.New("rollback reservation requires the real writer flock")
	}
	if err := authority.AssertHeld(); err != nil {
		return commit, fmt.Errorf("rollback reservation flock is not held: %w", err)
	}
	d.evidenceMu.Lock()
	defer d.evidenceMu.Unlock()
	if err := d.recoverPendingEvidenceWriteLocked(
		ctx,
		authority,
		commit.WriterID,
		writerBuild,
	); err != nil {
		return commit, err
	}
	if commit.CheckID == nil || *commit.CheckID == ([16]byte{}) ||
		commit.AgreementGroup == nil || *commit.AgreementGroup == ([16]byte{}) ||
		commit.CheckAttempt == 0 {
		return commit, errors.New("rollback reservation requires the exact trust check/group/attempt")
	}
	latest, found, err := d.loadLatestManifestRecord(ctx)
	if err != nil {
		return commit, err
	}
	if !found {
		return commit, errors.New("dataset manifest is not initialized")
	}
	check, err := rollbackCheckFromCommit(commit)
	if err != nil {
		return commit, err
	}
	if !manifestCheckMatches(latest, check) || latest.TrustStatus != "checking" {
		return commit, errors.New("rollback reservation differs from authoritative checking identity")
	}
	frozen, err := d.freezeTrustEvidenceLocked(
		ctx,
		authority,
		check,
		commit.RecordedAt,
		commit.WriterID,
		writerBuild,
		"evidence_frozen",
	)
	if err != nil {
		return commit, fmt.Errorf("freeze rollback evidence: %w", err)
	}
	commit.EvidenceCount = frozen.Count
	commit.EvidenceDigest = frozen.Digest
	if _, err := d.validateRollbackCommitEvidence(ctx, commit); err != nil {
		return commit, fmt.Errorf("validate rollback reservation evidence: %w", err)
	}
	verified, err := d.readTrustEvidenceCommitment(ctx, check)
	if err != nil {
		return commit, err
	}
	if verified != frozen {
		return commit, errors.New("rollback evaluator rows differ from frozen evidence commitment")
	}
	err = d.transitionManifest(
		ctx,
		authority,
		"rollback_reserved",
		commit.RecordedAt,
		func(latest manifestRecord) (bool, error) {
			if latest.PendingRollback == nil {
				return false, nil
			}
			if pendingMatchesCommit(*latest.PendingRollback, commit) {
				return true, nil
			}
			return false, errors.New("a different rollback reservation is already pending")
		},
		func(next *manifestRecord) error {
			if next.PendingRollback != nil {
				return errors.New("cannot overwrite a pending rollback reservation")
			}
			if next.TrustStatus != "checking" ||
				next.Checked == nil ||
				next.Checked.Point != commit.To ||
				next.CheckID == nil ||
				*next.CheckID != *commit.CheckID ||
				next.AgreementGroup == nil ||
				*next.AgreementGroup != *commit.AgreementGroup ||
				next.CheckAttempt != commit.CheckAttempt ||
				next.Checked.EventSeq != commit.CheckedEventSeq ||
				next.CorroborationRequired != commit.CorroborationRequired {
				return errors.New(
					"rollback reservation is not bound to the exact checking target/group/threshold",
				)
			}
			if commit.EventSeq <= next.Physical.EventSeq ||
				commit.OldEventSeq != next.Physical.EventSeq ||
				commit.OldTip != next.Physical.Point {
				return errors.New("rollback reservation is not anchored after the physical head")
			}
			next.PendingRollback = pendingRollbackFromCommit(commit, next.Physical)
			next.Effective = manifestClamp(*next)
			next.Servable = next.LastAgreed != nil || next.ServableFloorPermanent
			next.TrustReason = "corroborated rollback reserved before physical invalidations"
			next.WriterBuild = writerBuild
			return nil
		},
	)
	return commit, err
}

func (d *DB) MarkRollbackInvalidations(
	ctx context.Context,
	authority publication.Lock,
	commit publication.RollbackCommit,
	writerBuild string,
) error {
	return d.transitionManifest(
		ctx,
		authority,
		"rollback_invalidations_written",
		commit.RecordedAt,
		func(latest manifestRecord) (bool, error) {
			if latest.PendingRollback == nil ||
				!pendingMatchesCommit(*latest.PendingRollback, commit) {
				return false, errors.New("rollback invalidation marker does not match reservation")
			}
			return latest.PendingRollback.State == "invalidations_written", nil
		},
		func(next *manifestRecord) error {
			if next.PendingRollback == nil ||
				!pendingMatchesCommit(*next.PendingRollback, commit) ||
				next.PendingRollback.State != "reserved" {
				return errors.New("rollback invalidation transition lacks exact reserved predecessor")
			}
			next.PendingRollback.State = "invalidations_written"
			next.TrustReason = "rollback invalidations written; header/finalization pending"
			next.WriterBuild = writerBuild
			return nil
		},
	)
}

func (d *DB) FinalizeRollbackManifest(
	ctx context.Context,
	authority publication.Lock,
	commit publication.RollbackCommit,
	writerBuild string,
) error {
	committed, err := d.RollbackCommitted(ctx, commit)
	if err != nil {
		return err
	}
	if !committed {
		return errors.New("rollback header is not durably committed")
	}
	evidence, err := d.validateRollbackCommitEvidence(ctx, commit)
	if err != nil {
		return fmt.Errorf("revalidate rollback finalization evidence: %w", err)
	}
	return d.transitionManifest(
		ctx,
		authority,
		"rollback_finalized",
		commit.RecordedAt,
		func(latest manifestRecord) (bool, error) {
			if latest.PendingRollback == nil &&
				latest.Physical.EventSeq == commit.EventSeq &&
				latest.Physical.Point == commit.To &&
				latest.TrustStatus == "agreed" {
				return true, nil
			}
			return false, nil
		},
		func(next *manifestRecord) error {
			if next.PendingRollback == nil ||
				!pendingMatchesCommit(*next.PendingRollback, commit) ||
				next.PendingRollback.State != "invalidations_written" {
				return errors.New("rollback finalization lacks exact invalidation-stage reservation")
			}
			next.PendingRollback = nil
			next.Physical = manifestHead{EventSeq: commit.EventSeq, Point: commit.To}
			next.Effective = next.Physical
			next.TrustStatus = "agreed"
			next.TrustBasis = "sampled_peer"
			next.CorroborationConfirmed = evidence.Confirmed
			next.Disagreement = false
			next.TrustReason = "corroborated rollback header committed"
			completed := manifestTime(commit.RecordedAt)
			next.CheckCompletedAt = &completed
			// Checked remains the pre-rollback adoption/boundary event that
			// the persisted peer evidence hashed. LastAgreed is the newly
			// committed rollback-header event at that same point.
			agreed := next.Physical
			next.LastAgreed = &agreed
			next.LastAgreedAt = &completed
			next.LastAgreedEvidence = &manifestEvidenceReference{
				CheckID:   *next.CheckID,
				Group:     *next.AgreementGroup,
				Attempt:   next.CheckAttempt,
				Required:  next.CorroborationRequired,
				Confirmed: evidence.Confirmed,
				Checked:   *next.Checked,
				Count:     commit.EvidenceCount,
				Digest:    commit.EvidenceDigest,
			}
			next.Servable = true
			next.PrimarySuffix = 0
			next.WriterBuild = writerBuild
			return nil
		},
	)
}

// RecoverPendingRollback completes the exact append-only transaction reserved
// in the manifest. It never allocates a replacement identity and is safe to
// rerun after every insert/readback/manifest cut.
func (d *DB) RecoverPendingRollback(
	ctx context.Context,
	authority publication.Lock,
	writerBuild string,
) error {
	if authority == nil {
		return errors.New("pending rollback recovery requires the real writer flock")
	}
	if err := authority.AssertHeld(); err != nil {
		return fmt.Errorf("pending rollback recovery flock is not held: %w", err)
	}
	latest, found, err := d.loadLatestManifestRecord(ctx)
	if err != nil {
		return err
	}
	if !found {
		return errors.New("dataset manifest is not initialized")
	}
	if latest.PendingRollback == nil {
		return nil
	}
	commit := rollbackCommitFromPending(*latest.PendingRollback)
	if _, err := d.validateRollbackCommitEvidence(ctx, commit); err != nil {
		return fmt.Errorf("revalidate pending rollback evidence: %w", err)
	}
	descendants, err := d.reconstructPendingRollbackDescendants(ctx, commit)
	if err != nil {
		return err
	}
	if len(descendants) > 0 {
		if err := d.InsertInvalidations(ctx, commit, descendants); err != nil {
			return fmt.Errorf("recover rollback invalidations: %w", err)
		}
	}
	if err := d.MarkRollbackInvalidations(
		ctx,
		authority,
		commit,
		writerBuild,
	); err != nil {
		return err
	}
	committed, err := d.RollbackCommitted(ctx, commit)
	if err != nil {
		return err
	}
	if !committed {
		insertErr := d.InsertRollbackHeader(ctx, commit)
		committed, verifyErr := d.RollbackCommitted(ctx, commit)
		if verifyErr != nil {
			return errors.Join(
				insertErr,
				fmt.Errorf("verify recovered rollback header: %w", verifyErr),
			)
		}
		if !committed {
			if insertErr != nil {
				return fmt.Errorf("recover rollback header: %w", insertErr)
			}
			return errors.New("recovered rollback header is absent after successful insert")
		}
	}
	if err := authority.AssertHeld(); err != nil {
		return fmt.Errorf("pending rollback recovery flock was lost: %w", err)
	}
	return d.FinalizeRollbackManifest(ctx, authority, commit, writerBuild)
}

func (d *DB) reconstructPendingRollbackDescendants(
	ctx context.Context,
	commit publication.RollbackCommit,
) ([]publication.Descendant, error) {
	if commit.Depth == 0 {
		tip, err := d.committedTip(ctx, commit.OldEventSeq)
		if err != nil {
			return nil, fmt.Errorf("resolve depth-zero pending rollback tip: %w", err)
		}
		if tip != commit.To || commit.OldTip != commit.To {
			return nil, errors.New("depth-zero rollback reservation no longer matches its exact tip")
		}
		return nil, nil
	}
	descendants, err := d.ActiveDescendants(
		ctx,
		commit.OldEventSeq,
		commit.To,
		commit.Depth,
	)
	if err != nil {
		return nil, fmt.Errorf("reconstruct pending rollback descendants: %w", err)
	}
	if len(descendants) != int(commit.Depth) ||
		descendants[0].Point != commit.OldTip {
		return nil, errors.New("reconstructed descendants differ from pending rollback reservation")
	}
	return descendants, nil
}

func (d *DB) validateRollbackCommitEvidence(
	ctx context.Context,
	commit publication.RollbackCommit,
) (trustEvidenceResult, error) {
	check, err := rollbackCheckFromCommit(commit)
	if err != nil {
		return trustEvidenceResult{}, err
	}
	latest, found, err := d.loadLatestManifestRecord(ctx)
	if err != nil {
		return trustEvidenceResult{}, err
	}
	if !found ||
		!manifestCheckMatches(latest, check) ||
		latest.EvidenceState != "frozen" ||
		latest.PendingEvidenceWrite != nil ||
		latest.EvidenceDigest == nil {
		return trustEvidenceResult{}, errors.New(
			"rollback evidence is not the exact frozen manifest check",
		)
	}
	if commit.EvidenceCount != latest.EvidenceCount ||
		commit.EvidenceDigest != *latest.EvidenceDigest {
		return trustEvidenceResult{}, errors.New(
			"rollback commit differs from the frozen evidence commitment",
		)
	}
	commitment, err := d.readTrustEvidenceCommitment(ctx, check)
	if err != nil {
		return trustEvidenceResult{}, err
	}
	if commitment.Count != latest.EvidenceCount ||
		commitment.Digest != *latest.EvidenceDigest {
		return trustEvidenceResult{}, errors.New(
			"rollback evidence differs from frozen manifest commitment",
		)
	}
	evidence, err := d.readTrustEvidence(ctx, check)
	if err != nil {
		return trustEvidenceResult{}, err
	}
	if evidence.Disagreement || evidence.Confirmed < commit.CorroborationRequired {
		return trustEvidenceResult{}, errors.New(
			"rollback commit lacks canonical independent agreement threshold",
		)
	}
	if len(commit.ObservedPeers) != len(commit.ObservedOperators) ||
		len(commit.ObservedOperators) != len(evidence.Agreed) {
		return trustEvidenceResult{}, errors.New(
			"rollback observer arrays differ from canonical agreed evidence set",
		)
	}
	seen := make(map[string]struct{}, len(commit.ObservedOperators))
	for index, label := range commit.ObservedOperators {
		operator := strings.ToLower(strings.TrimSpace(label))
		peer := strings.TrimSpace(commit.ObservedPeers[index])
		if operator == "" || peer == "" {
			return trustEvidenceResult{}, errors.New(
				"rollback observer identity is empty after normalization",
			)
		}
		if _, duplicate := seen[operator]; duplicate {
			return trustEvidenceResult{}, errors.New(
				"rollback observer arrays contain a duplicate normalized operator",
			)
		}
		seen[operator] = struct{}{}
		if evidencePeer, agreed := evidence.Agreed[operator]; !agreed ||
			evidencePeer != peer {
			return trustEvidenceResult{}, errors.New(
				"rollback peer/operator pair is not canonical agreed evidence",
			)
		}
	}
	return evidence, nil
}

func pendingRollbackFromCommit(
	commit publication.RollbackCommit,
	oldPhysical manifestHead,
) *manifestPendingRollback {
	group := [16]byte{}
	if commit.AgreementGroup != nil {
		group = *commit.AgreementGroup
	}
	return &manifestPendingRollback{
		State:           "reserved",
		ID:              commit.RollbackID,
		EventSeq:        commit.EventSeq,
		To:              commit.To,
		OldPhysical:     oldPhysical,
		Depth:           commit.Depth,
		Reason:          commit.Reason,
		Peers:           append([]string(nil), commit.ObservedPeers...),
		Operators:       append([]string(nil), commit.ObservedOperators...),
		Required:        commit.CorroborationRequired,
		CheckID:         *commit.CheckID,
		Group:           group,
		CheckAttempt:    commit.CheckAttempt,
		CheckedEventSeq: commit.CheckedEventSeq,
		EvidenceCount:   commit.EvidenceCount,
		EvidenceDigest:  commit.EvidenceDigest,
		WriterID:        commit.WriterID,
		StartedAt:       manifestTime(commit.RecordedAt),
	}
}

func pendingMatchesCommit(
	pending manifestPendingRollback,
	commit publication.RollbackCommit,
) bool {
	if commit.AgreementGroup == nil {
		return false
	}
	return pending.ID == commit.RollbackID &&
		pending.EventSeq == commit.EventSeq &&
		pending.To == commit.To &&
		pending.OldPhysical.Point == commit.OldTip &&
		pending.OldPhysical.EventSeq == commit.OldEventSeq &&
		pending.Depth == commit.Depth &&
		pending.Reason == commit.Reason &&
		equalStrings(pending.Peers, commit.ObservedPeers) &&
		equalStrings(pending.Operators, commit.ObservedOperators) &&
		pending.Required == commit.CorroborationRequired &&
		commit.CheckID != nil &&
		pending.CheckID == *commit.CheckID &&
		pending.Group == *commit.AgreementGroup &&
		pending.CheckAttempt == commit.CheckAttempt &&
		pending.CheckedEventSeq == commit.CheckedEventSeq &&
		pending.EvidenceCount == commit.EvidenceCount &&
		pending.EvidenceDigest == commit.EvidenceDigest &&
		pending.WriterID == commit.WriterID &&
		pending.StartedAt.Equal(manifestTime(commit.RecordedAt))
}

func rollbackCommitFromPending(pending manifestPendingRollback) publication.RollbackCommit {
	checkID := pending.CheckID
	group := pending.Group
	return publication.RollbackCommit{
		RollbackID:            pending.ID,
		EventSeq:              pending.EventSeq,
		To:                    pending.To,
		OldTip:                pending.OldPhysical.Point,
		OldEventSeq:           pending.OldPhysical.EventSeq,
		Depth:                 pending.Depth,
		Reason:                pending.Reason,
		ObservedPeers:         append([]string(nil), pending.Peers...),
		ObservedOperators:     append([]string(nil), pending.Operators...),
		CorroborationRequired: pending.Required,
		CheckID:               &checkID,
		AgreementGroup:        &group,
		CheckAttempt:          pending.CheckAttempt,
		CheckedEventSeq:       pending.CheckedEventSeq,
		EvidenceCount:         pending.EvidenceCount,
		EvidenceDigest:        pending.EvidenceDigest,
		WriterID:              pending.WriterID,
		RecordedAt:            pending.StartedAt,
	}
}

func rollbackCheckFromCommit(
	commit publication.RollbackCommit,
) (syncer.CheckIdentity, error) {
	if commit.CheckID == nil || commit.AgreementGroup == nil {
		return syncer.CheckIdentity{}, errors.New("rollback commit lacks exact trust identity")
	}
	point := n2n.NewChainPointOrigin()
	if !commit.To.Origin {
		point = chainPointFromPublication(commit.To)
	}
	return syncer.CheckIdentity{
		ID:              *commit.CheckID,
		AgreementGroup:  *commit.AgreementGroup,
		Attempt:         commit.CheckAttempt,
		Required:        commit.CorroborationRequired,
		CheckedEventSeq: commit.CheckedEventSeq,
		CheckedPoint:    point,
	}, nil
}
