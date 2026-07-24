package store

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/google/uuid"

	"clicksync/internal/model"
	"clicksync/internal/n2n"
	"clicksync/internal/publication"
	"clicksync/internal/syncer"
)

// One trust attempt cannot legitimately contain more unique normalized
// operators than the UInt16 protocol threshold space. The extra query row is
// solely a fail-closed corruption/duplicate guard; it is not a runtime or
// cumulative evidence retention limit.
const maxTrustEvidenceRows = math.MaxUint16
const maxTrustEvidencePhysicalRows = maxTrustEvidenceRows*manifestDuplicateLimit + 1

type trustEvidenceCommitment struct {
	Count        uint32
	Digest       model.Hash32
	PrefixDigest model.Hash32
}

func emptyTrustEvidenceCommitment() trustEvidenceCommitment {
	empty := sha256.Sum256([]byte("clicksync-trust-evidence-set\x00"))
	return trustEvidenceCommitment{
		Digest:       empty,
		PrefixDigest: empty,
	}
}

func (d *DB) readTrustEvidenceCommitment(
	ctx context.Context,
	check syncer.CheckIdentity,
) (trustEvidenceCommitment, error) {
	const shapeProbe = `
SELECT 1
FROM clicksync.peer_observations
PREWHERE check_id = ?
WHERE (observation_kind = 'source_change' AND evidence_ordinal != 0)
   OR (observation_kind != 'source_change' AND evidence_ordinal = 0)
ORDER BY check_id, evidence_ordinal, observation_id
LIMIT 1`
	probe, err := d.conn.Query(ctx, shapeProbe, uuid.UUID(check.ID))
	if err != nil {
		return trustEvidenceCommitment{}, fmt.Errorf(
			"probe trust evidence ordinal shape: %w",
			err,
		)
	}
	if probe.Next() {
		probe.Close()
		return trustEvidenceCommitment{}, errors.New(
			"trust evidence contains diagnostic/authoritative ordinal corruption",
		)
	}
	if err := probe.Err(); err != nil {
		probe.Close()
		return trustEvidenceCommitment{}, err
	}
	probe.Close()
	const query = `
SELECT
    evidence_ordinal,
    observation_id,
    observation_digest,
    observation_kind,
    agreement_group,
    check_attempt
FROM clicksync.peer_observations
PREWHERE check_id = ?
WHERE observation_kind != 'source_change'
  AND evidence_ordinal > 0
ORDER BY check_id, evidence_ordinal, observation_id
LIMIT ?`
	rows, err := d.conn.Query(
		ctx,
		query,
		uuid.UUID(check.ID),
		maxTrustEvidencePhysicalRows,
	)
	if err != nil {
		return trustEvidenceCommitment{}, fmt.Errorf(
			"query trust evidence commitment: %w",
			err,
		)
	}
	defer rows.Close()
	hasher := sha256.New()
	_, _ = hasher.Write([]byte("clicksync-trust-evidence-set\x00"))
	var (
		physicalRows     uint64
		lastOrdinal      uint32
		lastID           uuid.UUID
		lastDigest       model.Hash32
		lastPrefixDigest model.Hash32
		duplicates       uint8
		uniqueIDs        = make(map[uuid.UUID]uint32)
	)
	for rows.Next() {
		physicalRows++
		if physicalRows == maxTrustEvidencePhysicalRows {
			return trustEvidenceCommitment{}, errors.New(
				"trust evidence exceeds bounded physical replay space",
			)
		}
		var (
			ordinal uint32
			id      uuid.UUID
			raw     []byte
			kind    string
			group   uuid.UUID
			attempt uint32
		)
		if err := rows.Scan(
			&ordinal,
			&id,
			&raw,
			&kind,
			&group,
			&attempt,
		); err != nil {
			return trustEvidenceCommitment{}, fmt.Errorf(
				"scan trust evidence commitment: %w",
				err,
			)
		}
		digest, err := hash32(raw)
		if err != nil {
			return trustEvidenceCommitment{}, err
		}
		if group != uuid.UUID(check.AgreementGroup) ||
			attempt != check.Attempt ||
			(kind != "source_change" && ordinal == 0) ||
			(kind == "source_change" && ordinal != 0) {
			return trustEvidenceCommitment{}, errors.New(
				"eligible evidence row differs from exact check identity",
			)
		}
		if kind == "source_change" {
			continue
		}
		if ordinal == lastOrdinal {
			if id != lastID || digest != lastDigest {
				return trustEvidenceCommitment{}, errors.New(
					"one evidence ordinal has conflicting physical rows",
				)
			}
			if duplicates == manifestDuplicateLimit {
				return trustEvidenceCommitment{}, errors.New(
					"evidence ordinal has at least nine physical rows",
				)
			}
			duplicates++
			continue
		}
		if ordinal != lastOrdinal+1 {
			return trustEvidenceCommitment{}, fmt.Errorf(
				"eligible evidence ordinal gap: got %d after %d",
				ordinal,
				lastOrdinal,
			)
		}
		if previous, exists := uniqueIDs[id]; exists {
			return trustEvidenceCommitment{}, fmt.Errorf(
				"observation ID is assigned to evidence ordinals %d and %d",
				previous,
				ordinal,
			)
		}
		uniqueIDs[id] = ordinal
		lastOrdinal = ordinal
		lastID = id
		lastDigest = digest
		duplicates = 1
		prefix := hasher.Sum(nil)
		var encodedOrdinal [4]byte
		binary.BigEndian.PutUint32(encodedOrdinal[:], ordinal)
		_, _ = hasher.Write(encodedOrdinal[:])
		_, _ = hasher.Write(id[:])
		_, _ = hasher.Write(digest[:])
		copy(lastPrefixDigest[:], prefix)
	}
	if err := rows.Err(); err != nil {
		return trustEvidenceCommitment{}, fmt.Errorf(
			"iterate trust evidence commitment: %w",
			err,
		)
	}
	if lastOrdinal > maxTrustEvidenceRows {
		return trustEvidenceCommitment{}, errors.New(
			"trust evidence unique ordinal space exceeds UInt16 protocol cardinality",
		)
	}
	var digest model.Hash32
	copy(digest[:], hasher.Sum(nil))
	if lastOrdinal == 0 {
		lastPrefixDigest = digest
	}
	return trustEvidenceCommitment{
		Count:        lastOrdinal,
		Digest:       digest,
		PrefixDigest: lastPrefixDigest,
	}, nil
}

func (d *DB) InsertPeerObservations(
	ctx context.Context,
	authority publication.Lock,
	observations []model.PeerObservation,
) error {
	if len(observations) == 0 {
		return nil
	}
	diagnostics := make([]model.PeerObservation, 0, len(observations))
	for _, observation := range observations {
		if observation.Kind == "source_change" {
			if observation.EvidenceOrdinal != 0 {
				return errors.New("diagnostic observation carries an evidence ordinal")
			}
			diagnostics = append(diagnostics, observation)
			continue
		}
		if authority == nil {
			return errors.New("eligible trust evidence requires the real writer flock")
		}
		if err := authority.AssertHeld(); err != nil {
			return fmt.Errorf("eligible trust evidence flock is not held: %w", err)
		}
		d.evidenceMu.Lock()
		if err := d.recoverPendingEvidenceWriteLocked(
			ctx,
			authority,
			[16]byte{},
			"",
		); err != nil {
			d.evidenceMu.Unlock()
			return err
		}
		if observation.EvidenceOrdinal != 0 {
			d.evidenceMu.Unlock()
			return errors.New("caller assigned authoritative evidence ordinal")
		}
		found, err := d.logicalObservationCommitted(ctx, observation)
		if err != nil {
			d.evidenceMu.Unlock()
			return err
		}
		if found {
			d.evidenceMu.Unlock()
			continue
		}
		if err := d.reserveEvidenceWrite(ctx, authority, observation); err != nil {
			d.evidenceMu.Unlock()
			return err
		}
		if err := d.recoverPendingEvidenceWriteLocked(
			ctx,
			authority,
			[16]byte{},
			"",
		); err != nil {
			d.evidenceMu.Unlock()
			return err
		}
		d.evidenceMu.Unlock()
	}
	return d.insertPeerObservationRows(ctx, diagnostics)
}

func (d *DB) logicalObservationCommitted(
	ctx context.Context,
	observation model.PeerObservation,
) (bool, error) {
	const query = `
SELECT evidence_ordinal, observation_digest
FROM clicksync.peer_observations
PREWHERE check_id = ?
WHERE observation_id = ?
ORDER BY check_id, observation_id, evidence_ordinal
LIMIT 9`
	rows, err := d.conn.Query(
		ctx,
		query,
		uuid.UUID(observation.CheckID),
		uuid.UUID(observation.ID),
	)
	if err != nil {
		return false, fmt.Errorf("query logical observation replay: %w", err)
	}
	defer rows.Close()
	var (
		found   bool
		ordinal uint32
		digest  model.Hash32
		count   uint8
	)
	for rows.Next() {
		count++
		if count > manifestDuplicateLimit {
			return false, errors.New(
				"logical observation replay has at least nine physical rows",
			)
		}
		var (
			rowOrdinal uint32
			rawDigest  []byte
		)
		if err := rows.Scan(&rowOrdinal, &rawDigest); err != nil {
			return false, fmt.Errorf("scan logical observation replay: %w", err)
		}
		rowDigest, err := hash32(rawDigest)
		if err != nil {
			return false, err
		}
		if !found {
			found = true
			ordinal = rowOrdinal
			digest = rowDigest
		} else if ordinal != rowOrdinal || digest != rowDigest {
			return false, errors.New(
				"logical observation replay has conflicting ordinal/digest rows",
			)
		}
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("iterate logical observation replay: %w", err)
	}
	if !found {
		return false, nil
	}
	if ordinal == 0 {
		return false, errors.New("eligible logical observation has zero ordinal")
	}
	expected := observation
	expected.EvidenceOrdinal = ordinal
	if err := model.FinalizePeerObservationIdentity(&expected); err != nil {
		return false, err
	}
	expectedDigest, err := model.PeerObservationDigest(expected)
	if err != nil {
		return false, err
	}
	if expected.ID != observation.ID || expectedDigest != digest {
		return false, errors.New(
			"logical observation replay differs from canonical admitted row",
		)
	}
	return true, nil
}

func (d *DB) reserveEvidenceWrite(
	ctx context.Context,
	authority publication.Lock,
	observation model.PeerObservation,
) error {
	return d.transitionManifest(
		ctx,
		authority,
		"evidence_write_reserved",
		observation.ObservedAt,
		func(latest manifestRecord) (bool, error) {
			if latest.PendingEvidenceWrite == nil {
				return false, nil
			}
			if latest.PendingEvidenceWrite.Observation.ID == observation.ID &&
				latest.PendingEvidenceWrite.Observation.CheckID == observation.CheckID {
				return true, nil
			}
			return false, errors.New("a different evidence write is already reserved")
		},
		func(next *manifestRecord) error {
			if next.EvidenceState != "open" ||
				next.PendingEvidenceWrite != nil ||
				next.TrustStatus != "checking" ||
				next.CheckID == nil ||
				*next.CheckID != observation.CheckID ||
				next.AgreementGroup == nil ||
				*next.AgreementGroup != observation.AgreementGroup ||
				next.CheckAttempt != observation.CheckAttempt {
				return errors.New(
					"eligible observation does not target the exact latest open check",
				)
			}
			if next.EvidenceCount == math.MaxUint16 {
				return errors.New("trust evidence ordinal space exhausted")
			}
			admitted := observation
			admitted.EvidenceOrdinal = next.EvidenceCount + 1
			if err := model.FinalizePeerObservationIdentity(&admitted); err != nil {
				return err
			}
			if admitted.ID != observation.ID ||
				admitted.EvidenceIdentity != observation.EvidenceIdentity {
				return errors.New(
					"writer-assigned ordinal changed logical observation identity",
				)
			}
			digest, err := model.PeerObservationDigest(admitted)
			if err != nil {
				return err
			}
			payload, err := canonicalPendingEvidencePayload(admitted)
			if err != nil {
				return err
			}
			if next.WriterID == nil {
				return errors.New("open trust check has no writer attribution")
			}
			next.PendingEvidenceWrite = &manifestPendingEvidenceWrite{
				Observation: admitted,
				Digest:      digest,
				Payload:     payload,
				WriterID:    *next.WriterID,
				ReservedAt:  manifestTime(observation.ObservedAt),
			}
			next.TrustReason = "eligible evidence write durably reserved"
			return nil
		},
	)
}

func (d *DB) RecoverPendingEvidenceWrite(
	ctx context.Context,
	authority publication.Lock,
	recoveryWriterID [16]byte,
	recoveryWriterBuild string,
) error {
	d.evidenceMu.Lock()
	defer d.evidenceMu.Unlock()
	return d.recoverPendingEvidenceWriteLocked(
		ctx,
		authority,
		recoveryWriterID,
		recoveryWriterBuild,
	)
}

func (d *DB) recoverPendingEvidenceWriteLocked(
	ctx context.Context,
	authority publication.Lock,
	recoveryWriterID [16]byte,
	recoveryWriterBuild string,
) error {
	if authority == nil {
		return errors.New("pending evidence recovery requires the real writer flock")
	}
	if err := authority.AssertHeld(); err != nil {
		return fmt.Errorf("pending evidence recovery flock is not held: %w", err)
	}
	latest, found, err := d.loadLatestManifestRecord(ctx)
	if err != nil {
		return err
	}
	if !found {
		return errors.New("dataset manifest is not initialized")
	}
	if latest.PendingEvidenceWrite == nil {
		return nil
	}
	pending := *latest.PendingEvidenceWrite
	// Assert immediately before the irreversible physical insert. The earlier
	// assertion protects entry to recovery, but the lock may have been lost
	// while the manifest reservation was read and validated.
	if err := authority.AssertHeld(); err != nil {
		return fmt.Errorf(
			"pending evidence recovery flock was lost before physical replay: %w",
			err,
		)
	}
	if err := d.insertPeerObservationRows(
		ctx,
		[]model.PeerObservation{pending.Observation},
	); err != nil {
		return fmt.Errorf("replay reserved evidence write: %w", err)
	}
	check, err := checkIdentityFromManifest(
		latest,
		latest.Checked != nil && latest.Checked.EventSeq == latest.Physical.EventSeq,
	)
	if err != nil {
		return err
	}
	commitment, err := d.readTrustEvidenceCommitment(ctx, check)
	if err != nil {
		return err
	}
	if commitment.Count != pending.Observation.EvidenceOrdinal {
		return errors.New(
			"reserved evidence replay did not produce the next contiguous ordinal",
		)
	}
	if err := authority.AssertHeld(); err != nil {
		return fmt.Errorf(
			"pending evidence recovery flock was lost before manifest commit: %w",
			err,
		)
	}
	return d.transitionManifest(
		ctx,
		authority,
		"evidence_write_committed",
		pending.ReservedAt,
		func(latest manifestRecord) (bool, error) {
			if latest.PendingEvidenceWrite == nil {
				return latest.EvidenceCount == commitment.Count &&
					latest.EvidenceDigest != nil &&
					*latest.EvidenceDigest == commitment.Digest, nil
			}
			return false, nil
		},
		func(next *manifestRecord) error {
			if next.PendingEvidenceWrite == nil ||
				next.PendingEvidenceWrite.Observation.ID != pending.Observation.ID ||
				next.PendingEvidenceWrite.Digest != pending.Digest ||
				next.PendingEvidenceWrite.Payload != pending.Payload {
				return errors.New(
					"pending evidence reservation changed during recovery",
				)
			}
			next.PendingEvidenceWrite = nil
			next.EvidenceCount = commitment.Count
			next.EvidenceDigest = &commitment.Digest
			next.TrustReason = "eligible evidence write durably committed"
			if recoveryWriterID != ([16]byte{}) {
				writer := recoveryWriterID
				next.WriterID = &writer
				next.WriterBuild = recoveryWriterBuild
			}
			return nil
		},
	)
}

func (d *DB) freezeTrustEvidence(
	ctx context.Context,
	authority publication.Lock,
	check syncer.CheckIdentity,
	at time.Time,
	writerID [16]byte,
	writerBuild string,
	transitionKind string,
) (trustEvidenceCommitment, error) {
	d.evidenceMu.Lock()
	defer d.evidenceMu.Unlock()
	return d.freezeTrustEvidenceLocked(
		ctx,
		authority,
		check,
		at,
		writerID,
		writerBuild,
		transitionKind,
	)
}

func (d *DB) freezeTrustEvidenceLocked(
	ctx context.Context,
	authority publication.Lock,
	check syncer.CheckIdentity,
	at time.Time,
	writerID [16]byte,
	writerBuild string,
	transitionKind string,
) (trustEvidenceCommitment, error) {
	var frozen trustEvidenceCommitment
	err := d.transitionManifest(
		ctx,
		authority,
		transitionKind,
		at,
		func(latest manifestRecord) (bool, error) {
			if !manifestCheckMatches(latest, check) {
				return false, errors.New("evidence freeze check differs from latest manifest")
			}
			if latest.PendingEvidenceWrite != nil {
				return false, errors.New("cannot freeze evidence with a pending write")
			}
			commitment, err := d.readTrustEvidenceCommitment(ctx, check)
			if err != nil {
				return false, err
			}
			frozen = commitment
			if latest.EvidenceCount != commitment.Count ||
				latest.EvidenceDigest == nil ||
				*latest.EvidenceDigest != commitment.Digest {
				return false, errors.New(
					"manifest cumulative evidence commitment differs from physical rows",
				)
			}
			return latest.EvidenceState == "frozen", nil
		},
		func(next *manifestRecord) error {
			if next.EvidenceState != "open" ||
				next.PendingEvidenceWrite != nil {
				return errors.New("only a quiescent open evidence set can freeze")
			}
			next.EvidenceState = "frozen"
			next.EvidenceCount = frozen.Count
			next.EvidenceDigest = &frozen.Digest
			next.TrustReason = "canonical eligible evidence set frozen"
			writer := writerID
			next.WriterID = &writer
			next.WriterBuild = writerBuild
			return nil
		},
	)
	return frozen, err
}

func (d *DB) validateManifestEvidenceCommitment(
	ctx context.Context,
	record manifestRecord,
) error {
	var currentCommitment *trustEvidenceCommitment
	if record.CheckID != nil {
		check, err := checkIdentityFromManifest(
			record,
			record.Checked != nil && record.Checked.EventSeq == record.Physical.EventSeq,
		)
		if err != nil {
			return err
		}
		commitment, err := d.readTrustEvidenceCommitment(ctx, check)
		if err != nil {
			return err
		}
		currentCommitment = &commitment
		if record.EvidenceDigest == nil ||
			record.EvidenceState == "none" {
			return errors.New("manifest peer check has no durable evidence commitment")
		}
		currentMatches := record.EvidenceCount == commitment.Count &&
			*record.EvidenceDigest == commitment.Digest
		if !currentMatches && record.PendingEvidenceWrite != nil &&
			commitment.Count == record.EvidenceCount+1 &&
			record.PendingEvidenceWrite.Observation.EvidenceOrdinal == commitment.Count &&
			commitment.PrefixDigest == *record.EvidenceDigest {
			replay := record.PendingEvidenceWrite.Observation
			replay.EvidenceOrdinal = 0
			found, err := d.logicalObservationCommitted(ctx, replay)
			if err != nil {
				return err
			}
			currentMatches = found
		}
		if !currentMatches {
			return errors.New(
				"manifest evidence commitment differs from committed prefix or exact pending row",
			)
		}
	}
	if record.LastAgreedEvidence == nil {
		return nil
	}
	reference := *record.LastAgreedEvidence
	var commitment trustEvidenceCommitment
	if record.CheckID != nil &&
		*record.CheckID == reference.CheckID &&
		currentCommitment != nil {
		commitment = *currentCommitment
	} else {
		check := checkIdentityFromEvidenceReference(reference)
		var err error
		commitment, err = d.readTrustEvidenceCommitment(ctx, check)
		if err != nil {
			return fmt.Errorf("validate last-agreed evidence commitment: %w", err)
		}
	}
	if commitment.Count != reference.Count ||
		commitment.Digest != reference.Digest {
		return errors.New(
			"last-agreed authority differs from its immutable evidence commitment",
		)
	}
	return nil
}

func checkIdentityFromEvidenceReference(
	reference manifestEvidenceReference,
) syncer.CheckIdentity {
	point := n2n.NewChainPointOrigin()
	if !reference.Checked.Point.Origin {
		point = chainPointFromPublication(reference.Checked.Point)
	}
	return syncer.CheckIdentity{
		ID:              reference.CheckID,
		AgreementGroup:  reference.Group,
		Attempt:         reference.Attempt,
		Required:        reference.Required,
		CheckedEventSeq: reference.Checked.EventSeq,
		CheckedPoint:    point,
	}
}

func (d *DB) BeginTrustCheck(
	ctx context.Context,
	authority publication.Lock,
	expected *n2n.ChainPoint,
	required int,
	at time.Time,
	writerID [16]byte,
	writerBuild string,
) (syncer.CheckIdentity, error) {
	if authority == nil {
		return syncer.CheckIdentity{}, errors.New("trust check requires the real writer flock")
	}
	if expected == nil {
		return syncer.CheckIdentity{}, errors.New("trust check requires an exact candidate event-point")
	}
	if required < 2 || required > math.MaxUint16 {
		return syncer.CheckIdentity{}, fmt.Errorf("invalid trust corroboration threshold %d", required)
	}
	expectedPoint, err := publicationPointFromChain(*expected)
	if err != nil {
		return syncer.CheckIdentity{}, err
	}
	if authority == nil {
		return syncer.CheckIdentity{}, errors.New(
			"begin trust check requires the real writer flock",
		)
	}
	d.evidenceMu.Lock()
	defer d.evidenceMu.Unlock()
	if err := d.recoverPendingEvidenceWriteLocked(
		ctx,
		authority,
		writerID,
		writerBuild,
	); err != nil {
		return syncer.CheckIdentity{}, err
	}
	latestBeforeBegin, found, err := d.loadLatestManifestRecord(ctx)
	if err != nil {
		return syncer.CheckIdentity{}, err
	}
	if !found {
		return syncer.CheckIdentity{}, errors.New("dataset manifest is not initialized")
	}
	if latestBeforeBegin.PendingRollback != nil {
		return syncer.CheckIdentity{}, errors.New(
			"cannot begin a new trust check while rollback recovery is pending",
		)
	}
	reuseOpen := latestBeforeBegin.TrustStatus == "checking" &&
		latestBeforeBegin.EvidenceState == "open" &&
		latestBeforeBegin.PendingEvidenceWrite == nil &&
		latestBeforeBegin.EvidenceCount == 0 &&
		latestBeforeBegin.Checked != nil &&
		latestBeforeBegin.Checked.Point == expectedPoint &&
		latestBeforeBegin.CorroborationRequired == uint16(required)
	if latestBeforeBegin.TrustStatus == "checking" && !reuseOpen {
		oldCheck, err := checkIdentityFromManifest(
			latestBeforeBegin,
			latestBeforeBegin.Checked != nil &&
				latestBeforeBegin.Checked.EventSeq == latestBeforeBegin.Physical.EventSeq,
		)
		if err != nil {
			return syncer.CheckIdentity{}, err
		}
		if _, err := d.freezeTrustEvidenceLocked(
			ctx,
			authority,
			oldCheck,
			at,
			writerID,
			writerBuild,
			"trust_superseded",
		); err != nil {
			return syncer.CheckIdentity{}, fmt.Errorf(
				"freeze superseded trust attempt: %w",
				err,
			)
		}
	}

	var returned syncer.CheckIdentity
	err = d.transitionManifest(
		ctx,
		authority,
		"trust_check_started",
		at,
		func(latest manifestRecord) (bool, error) {
			eventSeq, physical, err := d.manifestEventForCandidate(
				ctx,
				latest,
				expectedPoint,
			)
			if err != nil {
				return false, err
			}
			candidate := manifestHead{EventSeq: eventSeq, Point: expectedPoint}
			if latest.TrustStatus == "checking" &&
				latest.EvidenceState == "open" &&
				latest.PendingEvidenceWrite == nil &&
				latest.Checked != nil &&
				*latest.Checked == candidate &&
				latest.CorroborationRequired == uint16(required) {
				hasEvidence, err := d.trustAttemptHasEvidence(
					ctx,
					latest.CheckID,
					latest.CheckAttempt,
				)
				if err != nil {
					return false, err
				}
				if !hasEvidence {
					returned, err = checkIdentityFromManifest(latest, physical)
					return true, err
				}
			}
			return false, nil
		},
		func(next *manifestRecord) error {
			eventSeq, physical, err := d.manifestEventForCandidate(
				ctx,
				*next,
				expectedPoint,
			)
			if err != nil {
				return err
			}
			sameGroup := next.TrustStatus == "checking" &&
				next.CheckID != nil &&
				next.AgreementGroup != nil
			if sameGroup {
				if next.EvidenceState != "frozen" {
					return errors.New(
						"previous trust attempt was not frozen before supersession",
					)
				}
				if next.CheckAttempt == math.MaxUint32 {
					return errors.New("manifest trust check attempt space exhausted")
				}
				checkID, err := uuid.NewRandom()
				if err != nil {
					return fmt.Errorf("generate trust attempt check ID: %w", err)
				}
				check := [16]byte(checkID)
				next.CheckID = &check
				next.CheckAttempt++
			} else {
				checkID, err := uuid.NewRandom()
				if err != nil {
					return fmt.Errorf("generate trust check ID: %w", err)
				}
				groupID, err := uuid.NewRandom()
				if err != nil {
					return fmt.Errorf("generate trust agreement group: %w", err)
				}
				check := [16]byte(checkID)
				group := [16]byte(groupID)
				next.CheckID = &check
				next.AgreementGroup = &group
				next.CheckAttempt = 1
				next.VisibilityGeneration++
			}
			checked := manifestHead{EventSeq: eventSeq, Point: expectedPoint}
			next.TrustStatus = "checking"
			next.TrustBasis = "sampled_peer"
			next.CorroborationRequired = uint16(required)
			next.CorroborationConfirmed = 0
			next.Disagreement = false
			next.TrustReason = "exact candidate membership check in progress"
			started := manifestTime(at)
			next.CheckStartedAt = &started
			next.CheckCompletedAt = nil
			empty := emptyTrustEvidenceCommitment()
			next.EvidenceState = "open"
			next.EvidenceCount = 0
			next.EvidenceDigest = &empty.Digest
			next.PendingEvidenceWrite = nil
			next.Checked = &checked
			next.Effective = manifestClamp(*next)
			next.Servable = next.LastAgreed != nil || next.ServableFloorPermanent
			writer := writerID
			next.WriterID = &writer
			next.WriterBuild = writerBuild
			returned, err = checkIdentityFromManifest(*next, physical)
			return err
		},
	)
	if err != nil {
		return syncer.CheckIdentity{}, err
	}
	if returned.ID != ([16]byte{}) {
		return returned, nil
	}
	latest, found, err := d.loadLatestManifestRecord(ctx)
	if err != nil {
		return syncer.CheckIdentity{}, err
	}
	if !found {
		return syncer.CheckIdentity{}, errors.New("manifest disappeared after trust check transition")
	}
	physical := latest.Checked != nil && *latest.Checked == latest.Physical
	return checkIdentityFromManifest(latest, physical)
}

func (d *DB) FinalizeTrustCheck(
	ctx context.Context,
	authority publication.Lock,
	check syncer.CheckIdentity,
	forceDispute bool,
	reason string,
	at time.Time,
	writerID [16]byte,
	writerBuild string,
) (syncer.TrustResolution, error) {
	if authority == nil {
		return syncer.TrustResolution{}, errors.New("trust finalization requires the real writer flock")
	}
	d.evidenceMu.Lock()
	if err := d.recoverPendingEvidenceWriteLocked(
		ctx,
		authority,
		writerID,
		writerBuild,
	); err != nil {
		d.evidenceMu.Unlock()
		return syncer.TrustResolution{}, err
	}
	frozen, err := d.freezeTrustEvidenceLocked(
		ctx,
		authority,
		check,
		at,
		writerID,
		writerBuild,
		"evidence_frozen",
	)
	if err != nil {
		d.evidenceMu.Unlock()
		return syncer.TrustResolution{}, err
	}
	evidence, evidenceErr := d.readTrustEvidence(ctx, check)
	verifiedCommitment, commitmentErr := d.readTrustEvidenceCommitment(ctx, check)
	d.evidenceMu.Unlock()
	if commitmentErr != nil {
		return syncer.TrustResolution{}, commitmentErr
	}
	if verifiedCommitment != frozen {
		return syncer.TrustResolution{}, errors.New(
			"trust evaluator rows differ from the frozen evidence commitment",
		)
	}
	if evidenceErr != nil {
		forceDispute = true
		reason = "malformed trust evidence: " + evidenceErr.Error()
	}
	status := "unavailable"
	if forceDispute || evidence.Disagreement {
		status = "disputed"
		if reason == "" {
			reason = evidence.Reason
		}
	} else if evidence.Confirmed >= check.Required {
		status = "agreed"
	}
	if reason == "" {
		switch status {
		case "agreed":
			reason = "independent operators agreed on exact event-point"
		case "unavailable":
			reason = "insufficient independent operators were reachable"
		case "disputed":
			reason = "independent evidence conflicted on exact event-point"
		}
	}
	var resolution syncer.TrustResolution
	err = d.transitionManifest(
		ctx,
		authority,
		"trust_"+status,
		at,
		func(latest manifestRecord) (bool, error) {
			if manifestCheckMatches(latest, check) && latest.TrustStatus == status &&
				latest.CheckCompletedAt != nil {
				resolution = syncer.TrustResolution{
					Status:    latest.TrustStatus,
					Confirmed: latest.CorroborationConfirmed,
					Required:  latest.CorroborationRequired,
					Servable:  latest.Servable,
				}
				return true, nil
			}
			return false, nil
		},
		func(next *manifestRecord) error {
			if next.TrustStatus != "checking" || !manifestCheckMatches(*next, check) {
				return errors.New("trust finalization does not match the authoritative checking row")
			}
			if next.EvidenceState != "frozen" ||
				next.PendingEvidenceWrite != nil {
				return errors.New("trust finalization requires frozen committed evidence")
			}
			primarySuffix := uint64(0)
			if status == "agreed" && *next.Checked != next.Physical {
				var err error
				primarySuffix, err = d.validatedPrimarySuffixAfterCheckedAncestor(
					ctx,
					*next,
					check,
				)
				if err != nil {
					return err
				}
			}
			next.TrustStatus = status
			next.CorroborationConfirmed = evidence.Confirmed
			next.Disagreement = status == "disputed"
			next.TrustReason = reason
			completed := manifestTime(at)
			next.CheckCompletedAt = &completed
			switch status {
			case "agreed":
				agreed := *next.Checked
				next.LastAgreed = &agreed
				next.LastAgreedAt = &completed
				next.LastAgreedEvidence = &manifestEvidenceReference{
					CheckID:   *next.CheckID,
					Group:     *next.AgreementGroup,
					Attempt:   next.CheckAttempt,
					Required:  next.CorroborationRequired,
					Confirmed: evidence.Confirmed,
					Checked:   *next.Checked,
					Count:     next.EvidenceCount,
					Digest:    *next.EvidenceDigest,
				}
				next.Effective = next.Physical
				next.Servable = true
				next.PrimarySuffix = primarySuffix
				if primarySuffix == 0 {
					next.TrustBasis = "sampled_peer"
				} else {
					next.TrustBasis = "primary_only"
				}
			case "unavailable":
				if next.LastAgreed != nil || next.ServableFloorPermanent {
					next.Effective = next.Physical
					next.Servable = true
				} else {
					next.Effective = next.ServableFloor
					next.Servable = false
				}
			case "disputed":
				next.Effective = manifestClamp(*next)
				next.Servable = next.LastAgreed != nil || next.ServableFloorPermanent
			}
			writer := writerID
			next.WriterID = &writer
			next.WriterBuild = writerBuild
			resolution = syncer.TrustResolution{
				Status:    status,
				Confirmed: evidence.Confirmed,
				Required:  check.Required,
				Servable:  next.Servable,
			}
			return nil
		},
	)
	return resolution, err
}

// validatedPrimarySuffixAfterCheckedAncestor permits a periodic check that
// began at the physical head to finish after later adoptions committed. The
// sampled point remains the authority anchor; only its exact active physical
// descendants form the primary-only suffix. An older candidate selected for a
// rollback cannot use this path.
func (d *DB) validatedPrimarySuffixAfterCheckedAncestor(
	ctx context.Context,
	record manifestRecord,
	check syncer.CheckIdentity,
) (uint64, error) {
	if record.Checked == nil || !check.Physical {
		return 0, errors.New(
			"older agreed candidate requires rollback reservation before trust release",
		)
	}
	if !manifestPointBefore(record.Checked.Point, record.Physical.Point) {
		return 0, errors.New(
			"advanced physical head is not strictly after its checked ancestor",
		)
	}
	remote, err := d.remoteAdoptionsBetween(
		ctx,
		record.Checked.EventSeq,
		record.Physical.EventSeq,
	)
	if err != nil {
		return 0, fmt.Errorf(
			"count primary suffix after checked ancestor: %w",
			err,
		)
	}
	if remote == 0 || remote > manifestMaximumSuffix {
		return 0, fmt.Errorf(
			"checked ancestor has invalid primary suffix length %d",
			remote,
		)
	}
	descendants, err := d.ActiveDescendants(
		ctx,
		record.Physical.EventSeq,
		record.Checked.Point,
		uint32(remote),
	)
	if err != nil {
		return 0, fmt.Errorf(
			"validate active primary suffix after checked ancestor: %w",
			err,
		)
	}
	if uint64(len(descendants)) != remote {
		return 0, fmt.Errorf(
			"active primary suffix has %d descendants but %d remote adoptions",
			len(descendants),
			remote,
		)
	}
	return remote, nil
}

type trustEvidenceResult struct {
	Confirmed    uint16
	Disagreement bool
	Reason       string
	Agreed       map[string]string
}

func (d *DB) readTrustEvidence(
	ctx context.Context,
	check syncer.CheckIdentity,
) (trustEvidenceResult, error) {
	const query = `
SELECT
    observation_id,
    observation_digest,
    evidence_identity,
    observation_kind,
    peer_host,
    peer_address,
    operator_label,
    operator_key,
    n2n_version,
    network_magic,
    observed_tip_slot,
    observed_tip_hash,
    observed_tip_block_number,
    checkpoint_slot,
    checkpoint_hash,
    checkpoint_block_number,
    checkpoint_is_byron_ebb,
    check_id,
    agreement_group,
    check_attempt,
    evidence_ordinal,
    proof_method,
    corroboration_required,
    checked_event_seq,
    checked_point_origin,
    checked_point_slot,
    checked_point_hash,
    checked_point_block_number,
    checked_point_is_byron_ebb,
    selected_body_source,
    body_hash_verified,
    point_verified,
    parent_verified,
    result,
    reason,
    observed_at
FROM clicksync.peer_observations
PREWHERE check_id = ?
WHERE observation_kind != 'source_change'
  AND evidence_ordinal > 0
ORDER BY check_id, evidence_ordinal, observation_id
LIMIT ?`
	rows, err := d.conn.Query(
		ctx,
		query,
		uuid.UUID(check.ID),
		maxTrustEvidencePhysicalRows,
	)
	if err != nil {
		return trustEvidenceResult{}, fmt.Errorf("query trust check evidence: %w", err)
	}
	defer rows.Close()
	byOperator := make(map[string]string)
	byID := make(map[[16]byte]model.Hash32)
	byIDCount := make(map[[16]byte]uint8)
	byEvidence := make(map[model.Hash32][16]byte)
	result := trustEvidenceResult{Agreed: make(map[string]string)}
	rowCount := 0
	for rows.Next() {
		rowCount++
		if rowCount == maxTrustEvidencePhysicalRows {
			return trustEvidenceResult{}, fmt.Errorf(
				"trust attempt exceeds bounded physical evidence replay space %d",
				maxTrustEvidencePhysicalRows-1,
			)
		}
		var (
			observationID        uuid.UUID
			digest               []byte
			evidenceID           []byte
			kind                 string
			peerHost             string
			peerAddress          string
			operatorLabel        string
			operator             string
			n2nVersion           uint16
			networkMagic         uint32
			tipSlot              uint64
			tipHashBytes         []byte
			tipNumber            uint64
			checkpointSlot       *uint64
			checkpointHash       *string
			checkpointNumber     *uint64
			checkpointIsByronEBB *bool
			checkID              uuid.UUID
			group                uuid.UUID
			attempt              uint32
			ordinal              uint32
			proofMethod          string
			required             uint16
			eventSeq             uint64
			origin               bool
			slot                 *uint64
			hash                 *string
			blockNumber          *uint64
			isByronEBB           bool
			selectedBodySource   bool
			bodyHashVerified     bool
			pointVerified        bool
			parentVerified       bool
			outcome              string
			reason               string
			observedAt           time.Time
		)
		if err := rows.Scan(
			&observationID,
			&digest,
			&evidenceID,
			&kind,
			&peerHost,
			&peerAddress,
			&operatorLabel,
			&operator,
			&n2nVersion,
			&networkMagic,
			&tipSlot,
			&tipHashBytes,
			&tipNumber,
			&checkpointSlot,
			&checkpointHash,
			&checkpointNumber,
			&checkpointIsByronEBB,
			&checkID,
			&group,
			&attempt,
			&ordinal,
			&proofMethod,
			&required,
			&eventSeq,
			&origin,
			&slot,
			&hash,
			&blockNumber,
			&isByronEBB,
			&selectedBodySource,
			&bodyHashVerified,
			&pointVerified,
			&parentVerified,
			&outcome,
			&reason,
			&observedAt,
		); err != nil {
			return trustEvidenceResult{}, err
		}
		evidenceHash, err := hash32(evidenceID)
		if err != nil {
			return trustEvidenceResult{}, err
		}
		tipHash, err := hash32(tipHashBytes)
		if err != nil {
			return trustEvidenceResult{}, err
		}
		checkpointHashValue, err := hashPointer(checkpointHash)
		if err != nil {
			return trustEvidenceResult{}, err
		}
		checkedHashPointer, err := hashPointer(hash)
		if err != nil {
			return trustEvidenceResult{}, err
		}
		identity := [16]byte(observationID)
		row := model.PeerObservation{
			ID:                     identity,
			EvidenceIdentity:       evidenceHash,
			Kind:                   kind,
			PeerHost:               peerHost,
			PeerAddress:            peerAddress,
			Operator:               operatorLabel,
			N2NVersion:             n2nVersion,
			NetworkMagic:           networkMagic,
			TipSlot:                tipSlot,
			TipHash:                tipHash,
			TipBlockNumber:         tipNumber,
			CheckpointSlot:         checkpointSlot,
			CheckpointHash:         checkpointHashValue,
			CheckpointBlockNumber:  checkpointNumber,
			CheckpointIsByronEBB:   checkpointIsByronEBB,
			CheckID:                [16]byte(checkID),
			AgreementGroup:         [16]byte(group),
			CheckAttempt:           attempt,
			EvidenceOrdinal:        ordinal,
			ProofMethod:            proofMethod,
			CorroborationRequired:  required,
			CheckedEventSeq:        eventSeq,
			CheckedPointOrigin:     origin,
			CheckedPointSlot:       slot,
			CheckedPointHash:       checkedHashPointer,
			CheckedBlockNumber:     blockNumber,
			CheckedPointIsByronEBB: isByronEBB,
			SelectedBodySource:     selectedBodySource,
			BodyHashVerified:       bodyHashVerified,
			PointVerified:          pointVerified,
			ParentVerified:         parentVerified,
			Result:                 outcome,
			Reason:                 reason,
			ObservedAt:             observedAt,
		}
		if err := model.VerifyPeerObservationIdentity(row, digest); err != nil {
			return trustEvidenceResult{}, err
		}
		point, err := pointFromDB(origin, slot, hash, blockNumber, isByronEBB)
		if err != nil {
			return trustEvidenceResult{}, err
		}
		if checkID != uuid.UUID(check.ID) ||
			group != uuid.UUID(check.AgreementGroup) ||
			attempt != check.Attempt ||
			(kind != "source_change" && ordinal == 0) ||
			(kind == "source_change" && ordinal != 0) ||
			required != check.Required ||
			eventSeq != check.CheckedEventSeq ||
			point != publicationPointUnchecked(check.CheckedPoint) ||
			strings.TrimSpace(operator) == "" {
			return trustEvidenceResult{}, errors.New("observation check identity differs from manifest check")
		}
		normalizedOperator := strings.ToLower(strings.TrimSpace(operatorLabel))
		if operator != normalizedOperator {
			return trustEvidenceResult{}, errors.New(
				"materialized operator identity differs from canonical label",
			)
		}
		recomputedDigest, err := model.PeerObservationDigest(row)
		if err != nil {
			return trustEvidenceResult{}, err
		}
		if previous, exists := byID[identity]; exists {
			if previous != recomputedDigest {
				return trustEvidenceResult{}, errors.New(
					"conflicting persisted rows share one observation ID",
				)
			}
			if previousID, exists := byEvidence[evidenceHash]; !exists ||
				previousID != identity {
				return trustEvidenceResult{}, errors.New(
					"duplicate observation has inconsistent evidence identity",
				)
			}
			if byIDCount[identity] == 8 {
				return trustEvidenceResult{}, errors.New(
					"peer observation exceeds bounded identical replay duplicates",
				)
			}
			byIDCount[identity]++
			continue
		}
		if previousID, exists := byEvidence[evidenceHash]; exists &&
			previousID != identity {
			return trustEvidenceResult{}, errors.New(
				"conflicting persisted rows share one evidence identity",
			)
		}
		byID[identity] = recomputedDigest
		if len(byID) > maxTrustEvidenceRows {
			return trustEvidenceResult{}, errors.New(
				"trust attempt exceeds UInt16 unique evidence cardinality",
			)
		}
		byIDCount[identity] = 1
		byEvidence[evidenceHash] = identity
		eligible, err := validateTrustEvidenceProvenance(row, point)
		if err != nil {
			return trustEvidenceResult{}, err
		}
		if !eligible {
			continue
		}
		if previous, exists := byOperator[operator]; exists {
			if previous != outcome {
				return trustEvidenceResult{}, errors.New(
					"one normalized operator emitted multiple outcomes",
				)
			}
			return trustEvidenceResult{}, errors.New(
				"one normalized operator emitted multiple distinct evidence rows",
			)
		}
		byOperator[operator] = outcome
		if outcome == "disagreed" || outcome == "quarantined" {
			result.Disagreement = true
			result.Reason = "one or more operators rejected the exact point"
		} else if outcome == "agreed" {
			result.Agreed[operator] = peerHost
		}
	}
	if err := rows.Err(); err != nil {
		return trustEvidenceResult{}, err
	}
	for _, outcome := range byOperator {
		if outcome == "agreed" {
			result.Confirmed++
		}
	}
	return result, nil
}

func validateTrustEvidenceProvenance(
	row model.PeerObservation,
	checked publication.Point,
) (bool, error) {
	switch row.Kind {
	case "source_change":
		if row.ProofMethod != syncer.ObservationProofNone {
			return false, errors.New("diagnostic source-change row has an authoritative proof method")
		}
		if row.Result != "unavailable" && row.Result != "quarantined" {
			return false, errors.New("diagnostic source-change row has an authoritative result")
		}
		if row.SelectedBodySource ||
			row.BodyHashVerified ||
			row.PointVerified ||
			row.ParentVerified {
			return false, errors.New("diagnostic source-change row carries proof/source flags")
		}
		return false, nil
	case "checkpoint", "disagreement":
		if row.Kind == "disagreement" && row.Result == "agreed" {
			return false, errors.New("disagreement evidence claims agreement")
		}
		if row.ProofMethod != syncer.ObservationProofChainSyncSingleton &&
			row.ProofMethod != syncer.ObservationProofBoundarySingletonFetch &&
			(row.Kind != "checkpoint" ||
				row.ProofMethod != syncer.ObservationProofFollowBlockFetch) {
			return false, errors.New("checkpoint evidence has an invalid proof method")
		}
	case "rollback":
		if row.ProofMethod != syncer.ObservationProofFollowBlockFetch &&
			row.ProofMethod != syncer.ObservationProofPairedChainSyncSingleton {
			return false, errors.New("rollback evidence has an invalid proof method")
		}
	default:
		return false, fmt.Errorf(
			"unknown peer evidence observation kind %q",
			row.Kind,
		)
	}
	if row.NetworkMagic != mainnetMagic ||
		strings.TrimSpace(row.PeerHost) == "" ||
		strings.TrimSpace(row.Operator) == "" {
		return false, errors.New("trust evidence has invalid network or peer/operator provenance")
	}
	if checked.Origin {
		if row.CheckpointSlot != nil ||
			row.CheckpointHash != nil ||
			row.CheckpointBlockNumber != nil ||
			row.CheckpointIsByronEBB != nil {
			return false, errors.New("Origin trust evidence checkpoint is not null-shaped")
		}
	} else if row.CheckpointSlot == nil ||
		row.CheckpointHash == nil ||
		row.CheckpointBlockNumber == nil ||
		row.CheckpointIsByronEBB == nil ||
		*row.CheckpointSlot != checked.Slot ||
		*row.CheckpointHash != checked.Hash ||
		*row.CheckpointBlockNumber != checked.BlockNumber ||
		*row.CheckpointIsByronEBB != checked.IsByronEBB {
		return false, errors.New("trust evidence checkpoint differs from exact checked point")
	}
	if row.Result == "agreed" {
		if strings.TrimSpace(row.PeerAddress) == "" ||
			row.N2NVersion == 0 ||
			row.TipHash == (model.Hash32{}) ||
			!row.PointVerified ||
			row.TipBlockNumber < checked.BlockNumber ||
			row.TipSlot < checked.Slot ||
			row.Kind == "disagreement" {
			return false, errors.New("agreed trust evidence lacks verified session/tip provenance")
		}
	}
	switch row.ProofMethod {
	case syncer.ObservationProofChainSyncSingleton,
		syncer.ObservationProofPairedChainSyncSingleton:
		if row.Result == "agreed" {
			if !row.PointVerified ||
				row.SelectedBodySource ||
				row.BodyHashVerified ||
				row.ParentVerified {
				return false, errors.New("singleton agreement is not exact point-only proof")
			}
		} else if row.SelectedBodySource ||
			row.BodyHashVerified ||
			row.PointVerified ||
			row.ParentVerified {
			return false, errors.New("failed singleton attempt carries verification/source flags")
		}
	case syncer.ObservationProofFollowBlockFetch:
		if row.Result != "agreed" ||
			!row.SelectedBodySource ||
			!row.BodyHashVerified ||
			!row.PointVerified ||
			row.ParentVerified {
			return false, errors.New("Follow BlockFetch evidence has an invalid proof shape")
		}
	case syncer.ObservationProofBoundarySingletonFetch:
		if row.Result != "agreed" ||
			!row.BodyHashVerified ||
			!row.PointVerified ||
			row.ParentVerified {
			return false, errors.New("boundary singleton BlockFetch evidence has an invalid proof shape")
		}
	}
	return true, nil
}

func (d *DB) trustAttemptHasEvidence(
	ctx context.Context,
	checkID *[16]byte,
	attempt uint32,
) (bool, error) {
	if checkID == nil {
		return false, nil
	}
	rows, err := d.conn.Query(
		ctx,
		`SELECT 1
FROM clicksync.peer_observations
PREWHERE check_id = ?
WHERE check_attempt = ?
  AND observation_kind != 'source_change'
  AND evidence_ordinal > 0
ORDER BY check_id, evidence_ordinal, observation_id
LIMIT 1`,
		uuid.UUID(*checkID),
		attempt,
	)
	if err != nil {
		return false, fmt.Errorf("probe current trust attempt evidence: %w", err)
	}
	defer rows.Close()
	return rows.Next(), rows.Err()
}

func (d *DB) manifestEventForCandidate(
	ctx context.Context,
	latest manifestRecord,
	point publication.Point,
) (uint64, bool, error) {
	if point == latest.Physical.Point {
		return latest.Physical.EventSeq, true, nil
	}
	if point.Origin &&
		latest.ServableFloorPermanent &&
		latest.ServableFloor.Point.Origin {
		return latest.ServableFloor.EventSeq,
			latest.ServableFloor.EventSeq == latest.Physical.EventSeq,
			nil
	}
	if point == latest.Start {
		return 0, false, nil
	}
	if point.Origin {
		return 0, false, errors.New("Origin is not this dataset's boundary")
	}
	active, err := d.activeBlockByPoint(ctx, latest.Physical.EventSeq, point)
	if err != nil {
		return 0, false, fmt.Errorf(
			"resolve active publication for exact trust candidate: %w",
			err,
		)
	}
	const query = `
SELECT event_seq
FROM clicksync.chain_events
WHERE publication_id = ?
  AND event_kind = 'adoption'
  AND event_seq <= ?
ORDER BY event_seq
LIMIT 2`
	rows, err := d.conn.Query(
		ctx,
		query,
		active.PublicationID,
		latest.Physical.EventSeq,
	)
	if err != nil {
		return 0, false, fmt.Errorf("resolve manifest event for exact candidate: %w", err)
	}
	defer rows.Close()
	var events []uint64
	for rows.Next() {
		var event uint64
		if err := rows.Scan(&event); err != nil {
			return 0, false, err
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return 0, false, err
	}
	if len(events) == 0 {
		return 0, false, errors.New("exact trust candidate has no physical adoption event")
	}
	if len(events) > 1 {
		return 0, false, errors.New(
			"active trust candidate publication has multiple adoption events",
		)
	}
	return events[0], false, nil
}

func manifestClamp(record manifestRecord) manifestHead {
	if record.LastAgreed != nil {
		return *record.LastAgreed
	}
	return record.ServableFloor
}

func manifestCheckMatches(record manifestRecord, check syncer.CheckIdentity) bool {
	return record.CheckID != nil &&
		record.AgreementGroup != nil &&
		*record.CheckID == check.ID &&
		*record.AgreementGroup == check.AgreementGroup &&
		record.CheckAttempt == check.Attempt &&
		record.CorroborationRequired == check.Required &&
		record.Checked != nil &&
		record.Checked.EventSeq == check.CheckedEventSeq &&
		record.Checked.Point == publicationPointUnchecked(check.CheckedPoint)
}

func checkIdentityFromManifest(
	record manifestRecord,
	physical bool,
) (syncer.CheckIdentity, error) {
	if record.CheckID == nil || record.AgreementGroup == nil || record.Checked == nil {
		return syncer.CheckIdentity{}, errors.New("authoritative checking row lacks check identity")
	}
	point := n2n.NewChainPointOrigin()
	if !record.Checked.Point.Origin {
		point = chainPointFromPublication(record.Checked.Point)
	}
	return syncer.CheckIdentity{
		ID:              *record.CheckID,
		AgreementGroup:  *record.AgreementGroup,
		Attempt:         record.CheckAttempt,
		Required:        record.CorroborationRequired,
		CheckedEventSeq: record.Checked.EventSeq,
		CheckedPoint:    point,
		Physical:        physical,
	}, nil
}

func publicationPointFromChain(point n2n.ChainPoint) (publication.Point, error) {
	if len(point.Point.Hash) == 0 {
		if point.Point.Slot != 0 || point.BlockNumber != 0 || point.IsByronEBB {
			return publication.Point{}, errors.New("Origin trust candidate carries block metadata")
		}
		return publication.Point{Origin: true}, nil
	}
	if len(point.Point.Hash) != 32 {
		return publication.Point{}, fmt.Errorf("trust candidate hash has %d bytes", len(point.Point.Hash))
	}
	var hash model.Hash32
	copy(hash[:], point.Point.Hash)
	if hash == (model.Hash32{}) {
		return publication.Point{}, errors.New("trust candidate hash is zero")
	}
	return publication.Point{
		Slot:        point.Point.Slot,
		Hash:        hash,
		BlockNumber: point.BlockNumber,
		IsByronEBB:  point.IsByronEBB,
	}, nil
}

func publicationPointUnchecked(point n2n.ChainPoint) publication.Point {
	ret, _ := publicationPointFromChain(point)
	return ret
}

func stringPointer(value []byte) *string {
	if len(value) == 0 {
		return nil
	}
	ret := string(value)
	return &ret
}
