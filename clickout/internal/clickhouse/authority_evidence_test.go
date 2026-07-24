package clickhouse

import (
	"context"
	"crypto/sha256"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestAuthorityEvidenceOwnedSemanticTaxonomy(t *testing.T) {
	t.Parallel()
	if _, err := (&Store{}).loadAuthorityObservationRows(
		context.Background(),
		[16]byte{},
	); !errors.Is(err, ErrInvalidDataset) {
		t.Fatalf("zero-check semantic error = %v", err)
	}
	if _, err := (authorityObservationDBRow{}).row(); !errors.Is(
		err,
		ErrInvalidDataset,
	) {
		t.Fatalf("row-conversion semantic error = %v", err)
	}
}

func TestAuthorityEvidenceReaderCompilesWithRawPhysicalRows(t *testing.T) {
	t.Parallel()
	var load func(
		*Store,
		context.Context,
		[16]byte,
	) ([]authorityObservationRow, error) = (*Store).loadAuthorityObservationRows
	if load == nil {
		t.Fatal("authority evidence reader is nil")
	}
}

func TestAuthorityEvidenceQueryIsExactAndBounded(t *testing.T) {
	t.Parallel()
	const shapeExpected = `
SELECT 1
FROM peer_observations
PREWHERE check_id = ?
WHERE (observation_kind = 'source_change' AND evidence_ordinal != 0)
   OR (observation_kind != 'source_change' AND evidence_ordinal = 0)
ORDER BY check_id, evidence_ordinal, observation_id
LIMIT 1`
	const expected = `
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
	if authorityEvidenceShapeSQL != shapeExpected {
		t.Fatalf("authority evidence shape SQL changed:\n%s", authorityEvidenceShapeSQL)
	}
	if authorityEvidenceSQL != expected {
		t.Fatalf("authority evidence SQL changed:\n%s", authorityEvidenceSQL)
	}
	if strings.Contains(authorityEvidenceSQL, "clicksync.") ||
		strings.Contains(authorityEvidenceSQL, " FINAL") ||
		strings.Contains(authorityEvidenceSQL, "argMax(") ||
		strings.Contains(authorityEvidenceSQL, "count(") {
		t.Fatalf("authority evidence SQL is not a standalone raw read:\n%s", authorityEvidenceSQL)
	}
	if authorityEvidenceReadLimit != 65535*manifestDuplicateLimit+1 {
		t.Fatalf("authority evidence sentinel limit = %d", authorityEvidenceReadLimit)
	}
}

func TestAuthorityEvidenceUsesDedicatedPhaseLimits(t *testing.T) {
	t.Parallel()
	limits := authorityEvidencePhaseLimits()
	if limits.MaxResultRows < authorityEvidenceReadLimit {
		t.Fatalf("max result rows = %d", limits.MaxResultRows)
	}
	if limits.MaxRowsToRead == 0 || limits.MaxRowsToRead > 2_000_000 {
		t.Fatalf("max rows to read = %d", limits.MaxRowsToRead)
	}
	if limits.MaxResultRows == defaultMaxResultRows {
		t.Fatal("authority evidence reused the ordinary result-row cap")
	}
	settings := settingsForPhase(limits, 30*time.Second)
	if settings["max_result_rows"] != authorityEvidenceReadLimit {
		t.Fatalf("driver max_result_rows = %#v", settings["max_result_rows"])
	}
	if value, ok := settings["max_rows_to_read"].(uint64); !ok ||
		value == 0 || value > 2_000_000 {
		t.Fatalf("driver max_rows_to_read = %#v", settings["max_rows_to_read"])
	}
}

func TestAuthorityObservationDBRowConversion(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, time.July, 23, 10, 11, 12, 345678901, time.FixedZone("test", -5*60*60))
	checkID := authorityFill16(0x31)
	group := authorityFill16(0x32)
	slot := uint64(501)
	number := uint64(41)
	isByron := false
	pointHash := authorityFill32(0x33)
	observation := authorityObservation{
		Kind:                   "checkpoint",
		PeerHost:               "relay.example",
		PeerAddress:            "192.0.2.31:3001",
		Operator:               "operator-a",
		N2NVersion:             15,
		NetworkMagic:           764824073,
		TipSlot:                510,
		TipHash:                authorityFill32(0x34),
		TipBlockNumber:         42,
		CheckpointSlot:         &slot,
		CheckpointHash:         &pointHash,
		CheckpointBlockNumber:  &number,
		CheckpointIsByronEBB:   &isByron,
		CheckID:                checkID,
		AgreementGroup:         group,
		CheckAttempt:           2,
		EvidenceOrdinal:        7,
		ProofMethod:            "chain_sync_singleton",
		CorroborationRequired:  2,
		CheckedEventSeq:        99,
		CheckedPointSlot:       &slot,
		CheckedPointHash:       &pointHash,
		CheckedBlockNumber:     &number,
		CheckedPointIsByronEBB: isByron,
		PointVerified:          true,
		Result:                 "agreed",
		Reason:                 "verified",
		ObservedAt:             at,
	}
	if err := finalizeAuthorityObservationIdentity(&observation); err != nil {
		t.Fatal(err)
	}
	encoded, err := canonicalAuthorityObservationPayload(observation)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256([]byte(encoded))
	checkpointHash := string(observation.CheckpointHash[:])
	checkedHash := string(observation.CheckedPointHash[:])
	raw := authorityObservationDBRow{
		ObservationID:      uuid.UUID(observation.ID),
		ObservationDigest:  string(digest[:]),
		EvidenceIdentity:   string(observation.EvidenceIdentity[:]),
		Kind:               observation.Kind,
		PeerHost:           observation.PeerHost,
		PeerAddress:        observation.PeerAddress,
		Operator:           observation.Operator,
		OperatorKey:        strings.ToLower(strings.TrimSpace(observation.Operator)),
		N2NVersion:         observation.N2NVersion,
		NetworkMagic:       observation.NetworkMagic,
		TipSlot:            observation.TipSlot,
		TipHash:            string(observation.TipHash[:]),
		TipBlockNumber:     observation.TipBlockNumber,
		CheckpointSlot:     observation.CheckpointSlot,
		CheckpointHash:     &checkpointHash,
		CheckpointNumber:   observation.CheckpointBlockNumber,
		CheckpointIsByron:  observation.CheckpointIsByronEBB,
		CheckID:            uuid.UUID(observation.CheckID),
		AgreementGroup:     uuid.UUID(observation.AgreementGroup),
		CheckAttempt:       observation.CheckAttempt,
		EvidenceOrdinal:    observation.EvidenceOrdinal,
		ProofMethod:        observation.ProofMethod,
		Required:           observation.CorroborationRequired,
		CheckedEventSeq:    observation.CheckedEventSeq,
		CheckedOrigin:      observation.CheckedPointOrigin,
		CheckedSlot:        observation.CheckedPointSlot,
		CheckedHash:        &checkedHash,
		CheckedBlockNumber: observation.CheckedBlockNumber,
		CheckedIsByronEBB:  observation.CheckedPointIsByronEBB,
		SelectedBodySource: observation.SelectedBodySource,
		BodyHashVerified:   observation.BodyHashVerified,
		PointVerified:      observation.PointVerified,
		ParentVerified:     observation.ParentVerified,
		Result:             observation.Result,
		Reason:             observation.Reason,
		ObservedAt:         at,
	}
	converted, err := raw.row()
	if err != nil {
		t.Fatal(err)
	}
	observation.ObservedAt = normalizeAuthorityTime(observation.ObservedAt)
	if !reflect.DeepEqual(converted.Observation, observation) ||
		converted.OperatorKey != raw.OperatorKey ||
		converted.Digest != authorityHash(digest) {
		t.Fatalf("converted row differs:\n got %#v\nwant %#v", converted, observation)
	}

	raw.ObservationDigest = "short"
	if _, err := raw.row(); err == nil {
		t.Fatal("short FixedString digest was accepted")
	}
	raw.ObservationDigest = string(digest[:])
	raw.OperatorKey = "operator-b"
	if _, err := raw.row(); err == nil {
		t.Fatal("noncanonical operator key was accepted")
	}
	raw.OperatorKey = strings.ToLower(strings.TrimSpace(raw.Operator))
	wrongIdentity := authorityFill32(0x99)
	raw.EvidenceIdentity = string(wrongIdentity[:])
	if _, err := raw.row(); err == nil {
		t.Fatal("noncanonical evidence identity was accepted")
	}
}
