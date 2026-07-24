package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

// One check can contain at most 65,535 logical observations. Each logical
// observation may have up to eight identical physical replay rows; the final
// row is a fail-closed overflow sentinel.
const authorityEvidenceReadLimit = uint64(524281)

const authorityEvidenceShapeSQL = `
SELECT 1
FROM peer_observations
PREWHERE check_id = ?
WHERE (observation_kind = 'source_change' AND evidence_ordinal != 0)
   OR (observation_kind != 'source_change' AND evidence_ordinal = 0)
ORDER BY check_id, evidence_ordinal, observation_id
LIMIT 1`

const authorityEvidenceSQL = `
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
FROM peer_observations
PREWHERE check_id = ?
WHERE observation_kind != 'source_change'
  AND evidence_ordinal > 0
ORDER BY check_id, evidence_ordinal, observation_id
LIMIT 524281`

type authorityObservationRow struct {
	Observation authorityObservation
	OperatorKey string
	Digest      authorityHash
}

type authorityObservationDBRow struct {
	ObservationID      uuid.UUID `ch:"observation_id"`
	ObservationDigest  string    `ch:"observation_digest"`
	EvidenceIdentity   string    `ch:"evidence_identity"`
	Kind               string    `ch:"observation_kind"`
	PeerHost           string    `ch:"peer_host"`
	PeerAddress        string    `ch:"peer_address"`
	Operator           string    `ch:"operator_label"`
	OperatorKey        string    `ch:"operator_key"`
	N2NVersion         uint16    `ch:"n2n_version"`
	NetworkMagic       uint32    `ch:"network_magic"`
	TipSlot            uint64    `ch:"observed_tip_slot"`
	TipHash            string    `ch:"observed_tip_hash"`
	TipBlockNumber     uint64    `ch:"observed_tip_block_number"`
	CheckpointSlot     *uint64   `ch:"checkpoint_slot"`
	CheckpointHash     *string   `ch:"checkpoint_hash"`
	CheckpointNumber   *uint64   `ch:"checkpoint_block_number"`
	CheckpointIsByron  *bool     `ch:"checkpoint_is_byron_ebb"`
	CheckID            uuid.UUID `ch:"check_id"`
	AgreementGroup     uuid.UUID `ch:"agreement_group"`
	CheckAttempt       uint32    `ch:"check_attempt"`
	EvidenceOrdinal    uint32    `ch:"evidence_ordinal"`
	ProofMethod        string    `ch:"proof_method"`
	Required           uint16    `ch:"corroboration_required"`
	CheckedEventSeq    uint64    `ch:"checked_event_seq"`
	CheckedOrigin      bool      `ch:"checked_point_origin"`
	CheckedSlot        *uint64   `ch:"checked_point_slot"`
	CheckedHash        *string   `ch:"checked_point_hash"`
	CheckedBlockNumber *uint64   `ch:"checked_point_block_number"`
	CheckedIsByronEBB  bool      `ch:"checked_point_is_byron_ebb"`
	SelectedBodySource bool      `ch:"selected_body_source"`
	BodyHashVerified   bool      `ch:"body_hash_verified"`
	PointVerified      bool      `ch:"point_verified"`
	ParentVerified     bool      `ch:"parent_verified"`
	Result             string    `ch:"result"`
	Reason             string    `ch:"reason"`
	ObservedAt         time.Time `ch:"observed_at"`
}

func (raw authorityObservationDBRow) row() (
	row authorityObservationRow,
	err error,
) {
	defer func() {
		err = invalidAuthorityError(err)
	}()
	if raw.OperatorKey != strings.ToLower(strings.TrimSpace(raw.Operator)) {
		return authorityObservationRow{}, errors.New(
			"operator key differs from canonical operator label",
		)
	}
	digest, err := fixedAuthorityHash(raw.ObservationDigest)
	if err != nil {
		return authorityObservationRow{}, fmt.Errorf("observation digest: %w", err)
	}
	evidenceIdentity, err := fixedAuthorityHash(raw.EvidenceIdentity)
	if err != nil {
		return authorityObservationRow{}, fmt.Errorf("evidence identity: %w", err)
	}
	tipHash, err := fixedAuthorityHash(raw.TipHash)
	if err != nil {
		return authorityObservationRow{}, fmt.Errorf("observed tip hash: %w", err)
	}
	checkpointHash, err := optionalAuthorityHash(raw.CheckpointHash)
	if err != nil {
		return authorityObservationRow{}, fmt.Errorf("checkpoint hash: %w", err)
	}
	checkedHash, err := optionalAuthorityHash(raw.CheckedHash)
	if err != nil {
		return authorityObservationRow{}, fmt.Errorf("checked point hash: %w", err)
	}
	observation := authorityObservation{
		ID:                     authorityUUID(raw.ObservationID),
		EvidenceIdentity:       evidenceIdentity,
		Kind:                   raw.Kind,
		PeerHost:               raw.PeerHost,
		PeerAddress:            raw.PeerAddress,
		Operator:               raw.Operator,
		N2NVersion:             raw.N2NVersion,
		NetworkMagic:           raw.NetworkMagic,
		TipSlot:                raw.TipSlot,
		TipHash:                tipHash,
		TipBlockNumber:         raw.TipBlockNumber,
		CheckpointSlot:         raw.CheckpointSlot,
		CheckpointHash:         checkpointHash,
		CheckpointBlockNumber:  raw.CheckpointNumber,
		CheckpointIsByronEBB:   raw.CheckpointIsByron,
		CheckID:                authorityUUID(raw.CheckID),
		AgreementGroup:         authorityUUID(raw.AgreementGroup),
		CheckAttempt:           raw.CheckAttempt,
		EvidenceOrdinal:        raw.EvidenceOrdinal,
		ProofMethod:            raw.ProofMethod,
		CorroborationRequired:  raw.Required,
		CheckedEventSeq:        raw.CheckedEventSeq,
		CheckedPointOrigin:     raw.CheckedOrigin,
		CheckedPointSlot:       raw.CheckedSlot,
		CheckedPointHash:       checkedHash,
		CheckedBlockNumber:     raw.CheckedBlockNumber,
		CheckedPointIsByronEBB: raw.CheckedIsByronEBB,
		SelectedBodySource:     raw.SelectedBodySource,
		BodyHashVerified:       raw.BodyHashVerified,
		PointVerified:          raw.PointVerified,
		ParentVerified:         raw.ParentVerified,
		Result:                 raw.Result,
		Reason:                 raw.Reason,
		ObservedAt:             normalizeAuthorityTime(raw.ObservedAt),
	}
	if err := verifyAuthorityObservation(observation, digest); err != nil {
		return authorityObservationRow{}, err
	}
	return authorityObservationRow{
		Observation: observation,
		OperatorKey: raw.OperatorKey,
		Digest:      digest,
	}, nil
}

func authorityEvidencePhaseLimits() phaseLimits {
	limits := defaultPhaseLimits()
	limits.MaxRowsToRead = defaultMaxRowsToRead
	limits.MaxResultRows = authorityEvidenceReadLimit
	return limits
}

func (store *Store) loadAuthorityObservationRows(
	ctx context.Context,
	checkID [16]byte,
) ([]authorityObservationRow, error) {
	if checkID == ([16]byte{}) {
		return nil, invalidAuthorityError(
			errors.New("authority evidence check ID is zero"),
		)
	}
	queryCtx, finish := store.instrumentPhase(
		ctx,
		"authority_evidence",
		authorityEvidencePhaseLimits(),
	)
	defer finish()
	probe, err := store.conn.Query(
		queryCtx,
		authorityEvidenceShapeSQL,
		uuid.UUID(checkID),
	)
	if err != nil {
		return nil, mapQueryError("authority_evidence", err)
	}
	if probe.Next() {
		_ = probe.Close()
		return nil, invalidAuthorityError(
			errors.New(
				"authority evidence contains diagnostic/authoritative ordinal corruption",
			),
		)
	}
	if err := probe.Err(); err != nil {
		_ = probe.Close()
		return nil, err
	}
	if err := probe.Close(); err != nil {
		return nil, err
	}
	rows, err := store.conn.Query(
		queryCtx,
		authorityEvidenceSQL,
		uuid.UUID(checkID),
	)
	if err != nil {
		return nil, mapQueryError("authority_evidence", err)
	}
	defer rows.Close()

	result := make([]authorityObservationRow, 0)
	for rows.Next() {
		if uint64(len(result))+1 == authorityEvidenceReadLimit {
			return nil, invalidAuthorityError(
				fmt.Errorf(
					"authority evidence exceeds bounded physical replay space %d",
					authorityEvidenceReadLimit-1,
				),
			)
		}
		var raw authorityObservationDBRow
		if err := rows.ScanStruct(&raw); err != nil {
			return nil, fmt.Errorf("scan authority evidence: %w", err)
		}
		row, err := raw.row()
		if err != nil {
			return nil, fmt.Errorf("convert authority evidence: %w", err)
		}
		if row.Observation.CheckID != checkID {
			return nil, invalidAuthorityError(
				errors.New(
					"authority evidence row differs from exact check ID",
				),
			)
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}
