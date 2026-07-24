package clickhouse

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	_ "embed"

	"github.com/google/uuid"
)

//go:embed testdata/schema_contract.txt
var schemaContractDescriptor string

type authorityHash [32]byte

func (value authorityHash) String() string {
	return hex.EncodeToString(value[:])
}

type authorityPoint struct {
	Origin      bool
	Slot        uint64
	Hash        authorityHash
	BlockNumber uint64
	IsByronEBB  bool
}

type authorityHead struct {
	EventSeq uint64
	Point    authorityPoint
}

type authorityObservation struct {
	ID                     [16]byte
	EvidenceIdentity       authorityHash
	Kind                   string
	PeerHost               string
	PeerAddress            string
	Operator               string
	N2NVersion             uint16
	NetworkMagic           uint32
	TipSlot                uint64
	TipHash                authorityHash
	TipBlockNumber         uint64
	CheckpointSlot         *uint64
	CheckpointHash         *authorityHash
	CheckpointBlockNumber  *uint64
	CheckpointIsByronEBB   *bool
	CheckID                [16]byte
	AgreementGroup         [16]byte
	CheckAttempt           uint32
	EvidenceOrdinal        uint32
	ProofMethod            string
	CorroborationRequired  uint16
	CheckedEventSeq        uint64
	CheckedPointOrigin     bool
	CheckedPointSlot       *uint64
	CheckedPointHash       *authorityHash
	CheckedBlockNumber     *uint64
	CheckedPointIsByronEBB bool
	SelectedBodySource     bool
	BodyHashVerified       bool
	PointVerified          bool
	ParentVerified         bool
	Result                 string
	Reason                 string
	ObservedAt             time.Time
}

type authorityPendingEvidence struct {
	Observation authorityObservation
	Digest      authorityHash
	Payload     string
	WriterID    [16]byte
	ReservedAt  time.Time
}

type authorityEvidenceReference struct {
	CheckID   [16]byte
	Group     [16]byte
	Attempt   uint32
	Required  uint16
	Confirmed uint16
	Checked   authorityHead
	Count     uint32
	Digest    authorityHash
}

type authorityPendingRollback struct {
	State           string
	ID              [16]byte
	EventSeq        uint64
	To              authorityPoint
	OldPhysical     authorityHead
	Depth           uint32
	Reason          string
	Peers           []string
	Operators       []string
	Required        uint16
	CheckID         [16]byte
	Group           [16]byte
	CheckAttempt    uint32
	CheckedEventSeq uint64
	EvidenceCount   uint32
	EvidenceDigest  authorityHash
	WriterID        [16]byte
	StartedAt       time.Time
}

// authorityRecord uses the producer's exact exported field names. The
// canonical row digest is SHA-256 over encoding/json's sorted object form with
// RowDigest removed, so these names are part of the storage contract.
type authorityRecord struct {
	ManifestKey       uint8
	Revision          uint64
	TransitionID      [16]byte
	TransitionKind    string
	PreviousRowDigest *authorityHash
	RowDigest         authorityHash

	DatasetID              [16]byte
	SchemaContractHash     authorityHash
	NetworkMagic           uint32
	NetworkName            string
	ByronGenesisID         authorityHash
	ByronGenesisJSONHash   authorityHash
	ShelleyGenesisID       authorityHash
	ShelleyGenesisJSONHash authorityHash
	Start                  authorityPoint
	GenesisSeeded          bool
	CompleteHistory        bool
	TrustMode              string

	TrustStatus            string
	TrustBasis             string
	CheckID                *[16]byte
	AgreementGroup         *[16]byte
	CheckAttempt           uint32
	CorroborationRequired  uint16
	CorroborationConfirmed uint16
	CheckpointInterval     uint64
	PrimarySuffix          uint64
	Disagreement           bool
	TrustReason            string
	CheckStartedAt         *time.Time
	CheckCompletedAt       *time.Time
	EvidenceState          string
	EvidenceCount          uint32
	EvidenceDigest         *authorityHash
	PendingEvidenceWrite   *authorityPendingEvidence
	Checked                *authorityHead
	LastAgreed             *authorityHead
	LastAgreedAt           *time.Time
	LastAgreedEvidence     *authorityEvidenceReference
	ServableFloor          authorityHead
	ServableFloorPermanent bool
	Physical               authorityHead
	Effective              authorityHead
	Servable               bool
	VisibilityGeneration   uint64
	PendingRollback        *authorityPendingRollback

	WriterID    *[16]byte
	WriterBuild string
	SourceBuild string
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// authorityDBRow deliberately mirrors every dataset_manifest column. A
// producer-side semantic field therefore cannot be silently absent from the
// Clickout digest verifier.
type authorityDBRow struct {
	ManifestKey            uint8     `ch:"manifest_key"`
	Revision               uint64    `ch:"revision"`
	TransitionID           uuid.UUID `ch:"transition_id"`
	TransitionKind         string    `ch:"transition_kind"`
	PreviousRowDigest      *string   `ch:"previous_row_digest"`
	RowDigest              string    `ch:"row_digest"`
	DatasetID              uuid.UUID `ch:"dataset_id"`
	SchemaContractHash     string    `ch:"schema_contract_hash"`
	NetworkMagic           uint32    `ch:"network_magic"`
	NetworkName            string    `ch:"network_name"`
	ByronGenesisID         string    `ch:"byron_genesis_id"`
	ByronGenesisJSONHash   string    `ch:"byron_genesis_json_hash"`
	ShelleyGenesisID       string    `ch:"shelley_genesis_id"`
	ShelleyGenesisJSONHash string    `ch:"shelley_genesis_json_hash"`
	StartKind              string    `ch:"start_kind"`
	StartSlot              *uint64   `ch:"start_slot"`
	StartHash              *string   `ch:"start_hash"`
	StartBlockNumber       *uint64   `ch:"start_block_number"`
	StartIsByronEBB        bool      `ch:"start_is_byron_ebb"`
	GenesisSeeded          bool      `ch:"genesis_seeded"`
	CompleteHistory        bool      `ch:"complete_history"`
	TrustMode              string    `ch:"trust_mode"`

	TrustStatus            string     `ch:"trust_status"`
	TrustBasis             string     `ch:"trust_basis"`
	CheckID                *uuid.UUID `ch:"check_id"`
	AgreementGroup         *uuid.UUID `ch:"agreement_group"`
	CheckAttempt           uint32     `ch:"check_attempt"`
	CorroborationRequired  uint16     `ch:"corroboration_required"`
	CorroborationConfirmed uint16     `ch:"corroboration_confirmed"`
	CheckpointInterval     uint64     `ch:"checkpoint_interval"`
	PrimarySuffix          uint64     `ch:"primary_suffix"`
	Disagreement           bool       `ch:"disagreement"`
	TrustReason            string     `ch:"trust_reason"`
	CheckStartedAt         *time.Time `ch:"check_started_at"`
	CheckCompletedAt       *time.Time `ch:"check_completed_at"`
	EvidenceState          string     `ch:"evidence_state"`
	EvidenceCount          uint32     `ch:"evidence_count"`
	EvidenceDigest         *string    `ch:"evidence_digest"`
	PendingEvidenceID      *uuid.UUID `ch:"pending_evidence_observation_id"`
	PendingEvidenceDigest  *string    `ch:"pending_evidence_observation_digest"`
	PendingEvidencePayload string     `ch:"pending_evidence_payload"`
	PendingEvidenceWriter  *uuid.UUID `ch:"pending_evidence_writer_id"`
	PendingEvidenceAt      *time.Time `ch:"pending_evidence_reserved_at"`

	CheckedEventSeq             *uint64    `ch:"checked_event_seq"`
	CheckedPointOrigin          *bool      `ch:"checked_point_origin"`
	CheckedPointSlot            *uint64    `ch:"checked_point_slot"`
	CheckedPointHash            *string    `ch:"checked_point_hash"`
	CheckedPointBlockNumber     *uint64    `ch:"checked_point_block_number"`
	CheckedPointIsByronEBB      *bool      `ch:"checked_point_is_byron_ebb"`
	LastAgreedEventSeq          *uint64    `ch:"last_agreed_event_seq"`
	LastAgreedPointOrigin       *bool      `ch:"last_agreed_point_origin"`
	LastAgreedPointSlot         *uint64    `ch:"last_agreed_point_slot"`
	LastAgreedPointHash         *string    `ch:"last_agreed_point_hash"`
	LastAgreedPointBlockNumber  *uint64    `ch:"last_agreed_point_block_number"`
	LastAgreedPointIsByronEBB   *bool      `ch:"last_agreed_point_is_byron_ebb"`
	LastAgreedAt                *time.Time `ch:"last_agreed_at"`
	LastAgreedCheckID           *uuid.UUID `ch:"last_agreed_check_id"`
	LastAgreedAgreementGroup    *uuid.UUID `ch:"last_agreed_agreement_group"`
	LastAgreedCheckAttempt      uint32     `ch:"last_agreed_check_attempt"`
	LastAgreedRequired          uint16     `ch:"last_agreed_corroboration_required"`
	LastAgreedConfirmed         uint16     `ch:"last_agreed_corroboration_confirmed"`
	LastAgreedCheckedEventSeq   *uint64    `ch:"last_agreed_checked_event_seq"`
	LastAgreedCheckedOrigin     *bool      `ch:"last_agreed_checked_point_origin"`
	LastAgreedCheckedSlot       *uint64    `ch:"last_agreed_checked_point_slot"`
	LastAgreedCheckedHash       *string    `ch:"last_agreed_checked_point_hash"`
	LastAgreedCheckedNumber     *uint64    `ch:"last_agreed_checked_point_block_number"`
	LastAgreedCheckedIsByronEBB *bool      `ch:"last_agreed_checked_point_is_byron_ebb"`
	LastAgreedEvidenceCount     uint32     `ch:"last_agreed_evidence_count"`
	LastAgreedEvidenceDigest    *string    `ch:"last_agreed_evidence_digest"`

	ServableFloorEventSeq    uint64  `ch:"servable_floor_event_seq"`
	ServableFloorOrigin      bool    `ch:"servable_floor_origin"`
	ServableFloorSlot        *uint64 `ch:"servable_floor_slot"`
	ServableFloorHash        *string `ch:"servable_floor_hash"`
	ServableFloorBlockNumber *uint64 `ch:"servable_floor_block_number"`
	ServableFloorIsByronEBB  bool    `ch:"servable_floor_is_byron_ebb"`
	ServableFloorPermanent   bool    `ch:"servable_floor_permanent"`
	PhysicalEventSeq         uint64  `ch:"physical_event_seq"`
	PhysicalTipOrigin        bool    `ch:"physical_tip_origin"`
	PhysicalTipSlot          *uint64 `ch:"physical_tip_slot"`
	PhysicalTipHash          *string `ch:"physical_tip_hash"`
	PhysicalTipBlockNumber   *uint64 `ch:"physical_tip_block_number"`
	PhysicalTipIsByronEBB    bool    `ch:"physical_tip_is_byron_ebb"`
	EffectiveEventSeq        uint64  `ch:"effective_event_seq"`
	EffectiveTipOrigin       bool    `ch:"effective_tip_origin"`
	EffectiveTipSlot         *uint64 `ch:"effective_tip_slot"`
	EffectiveTipHash         *string `ch:"effective_tip_hash"`
	EffectiveTipBlockNumber  *uint64 `ch:"effective_tip_block_number"`
	EffectiveTipIsByronEBB   bool    `ch:"effective_tip_is_byron_ebb"`
	Servable                 bool    `ch:"servable"`
	VisibilityGeneration     uint64  `ch:"visibility_generation"`

	PendingRollbackState                  string     `ch:"pending_rollback_state"`
	PendingRollbackID                     *uuid.UUID `ch:"pending_rollback_id"`
	PendingRollbackEventSeq               *uint64    `ch:"pending_rollback_event_seq"`
	PendingRollbackToOrigin               *bool      `ch:"pending_rollback_to_origin"`
	PendingRollbackToSlot                 *uint64    `ch:"pending_rollback_to_slot"`
	PendingRollbackToHash                 *string    `ch:"pending_rollback_to_hash"`
	PendingRollbackToBlockNumber          *uint64    `ch:"pending_rollback_to_block_number"`
	PendingRollbackToIsByronEBB           *bool      `ch:"pending_rollback_to_is_byron_ebb"`
	PendingRollbackOldPhysicalEventSeq    *uint64    `ch:"pending_rollback_old_physical_event_seq"`
	PendingRollbackOldPhysicalOrigin      *bool      `ch:"pending_rollback_old_physical_origin"`
	PendingRollbackOldPhysicalSlot        *uint64    `ch:"pending_rollback_old_physical_slot"`
	PendingRollbackOldPhysicalHash        *string    `ch:"pending_rollback_old_physical_hash"`
	PendingRollbackOldPhysicalBlockNumber *uint64    `ch:"pending_rollback_old_physical_block_number"`
	PendingRollbackOldPhysicalIsByronEBB  *bool      `ch:"pending_rollback_old_physical_is_byron_ebb"`
	PendingRollbackDepth                  *uint32    `ch:"pending_rollback_depth"`
	PendingRollbackReason                 string     `ch:"pending_rollback_reason"`
	PendingRollbackObservedPeers          []string   `ch:"pending_rollback_observed_peers"`
	PendingRollbackObservedOperators      []string   `ch:"pending_rollback_observed_operators"`
	PendingRollbackRequired               *uint16    `ch:"pending_rollback_required"`
	PendingRollbackCheckID                *uuid.UUID `ch:"pending_rollback_check_id"`
	PendingRollbackAgreementGroup         *uuid.UUID `ch:"pending_rollback_agreement_group"`
	PendingRollbackCheckAttempt           *uint32    `ch:"pending_rollback_check_attempt"`
	PendingRollbackCheckedEventSeq        *uint64    `ch:"pending_rollback_checked_event_seq"`
	PendingRollbackEvidenceCount          *uint32    `ch:"pending_rollback_evidence_count"`
	PendingRollbackEvidenceDigest         *string    `ch:"pending_rollback_evidence_digest"`
	PendingRollbackWriterID               *uuid.UUID `ch:"pending_rollback_writer_id"`
	PendingRollbackStartedAt              *time.Time `ch:"pending_rollback_started_at"`

	WriterID    *uuid.UUID `ch:"writer_id"`
	WriterBuild string     `ch:"writer_build"`
	SourceBuild string     `ch:"source_build"`
	CreatedAt   time.Time  `ch:"created_at"`
	UpdatedAt   time.Time  `ch:"updated_at"`
}

func expectedSchemaContract() authorityHash {
	return sha256.Sum256([]byte(schemaContractDescriptor))
}

func fixedAuthorityHash(value string) (authorityHash, error) {
	if len(value) != 32 {
		return authorityHash{}, fmt.Errorf("FixedString(32) length is %d", len(value))
	}
	var result authorityHash
	copy(result[:], value)
	return result, nil
}

func optionalAuthorityHash(value *string) (*authorityHash, error) {
	if value == nil {
		return nil, nil
	}
	result, err := fixedAuthorityHash(*value)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

func authorityUUID(value uuid.UUID) [16]byte {
	var result [16]byte
	copy(result[:], value[:])
	return result
}

func optionalAuthorityUUID(value *uuid.UUID) *[16]byte {
	if value == nil {
		return nil
	}
	result := authorityUUID(*value)
	return &result
}

func authorityPointFromDB(
	origin bool,
	slot *uint64,
	hash *string,
	number *uint64,
	isByronEBB bool,
) (authorityPoint, error) {
	if origin {
		if slot != nil || hash != nil || number != nil || isByronEBB {
			return authorityPoint{}, errors.New("Origin point carries non-Origin fields")
		}
		return authorityPoint{Origin: true}, nil
	}
	if slot == nil || hash == nil || number == nil {
		return authorityPoint{}, errors.New("non-Origin point is incomplete")
	}
	decoded, err := fixedAuthorityHash(*hash)
	if err != nil {
		return authorityPoint{}, err
	}
	return authorityPoint{
		Slot:        *slot,
		Hash:        decoded,
		BlockNumber: *number,
		IsByronEBB:  isByronEBB,
	}, nil
}

func optionalAuthorityHead(
	event *uint64,
	origin *bool,
	slot *uint64,
	hash *string,
	number *uint64,
	isByronEBB *bool,
) (*authorityHead, error) {
	if event == nil && origin == nil && slot == nil && hash == nil &&
		number == nil && isByronEBB == nil {
		return nil, nil
	}
	if event == nil || origin == nil || isByronEBB == nil {
		return nil, errors.New("optional event-point is incomplete")
	}
	point, err := authorityPointFromDB(*origin, slot, hash, number, *isByronEBB)
	if err != nil {
		return nil, err
	}
	return &authorityHead{EventSeq: *event, Point: point}, nil
}

func (raw authorityDBRow) record() (authorityRecord, error) {
	previous, err := optionalAuthorityHash(raw.PreviousRowDigest)
	if err != nil {
		return authorityRecord{}, err
	}
	rowDigest, err := fixedAuthorityHash(raw.RowDigest)
	if err != nil {
		return authorityRecord{}, err
	}
	contract, err := fixedAuthorityHash(raw.SchemaContractHash)
	if err != nil {
		return authorityRecord{}, err
	}
	evidenceDigest, err := optionalAuthorityHash(raw.EvidenceDigest)
	if err != nil {
		return authorityRecord{}, err
	}
	byronID, err := fixedAuthorityHash(raw.ByronGenesisID)
	if err != nil {
		return authorityRecord{}, err
	}
	byronJSON, err := fixedAuthorityHash(raw.ByronGenesisJSONHash)
	if err != nil {
		return authorityRecord{}, err
	}
	shelleyID, err := fixedAuthorityHash(raw.ShelleyGenesisID)
	if err != nil {
		return authorityRecord{}, err
	}
	shelleyJSON, err := fixedAuthorityHash(raw.ShelleyGenesisJSONHash)
	if err != nil {
		return authorityRecord{}, err
	}
	if raw.StartKind != "origin" && raw.StartKind != "intersection" {
		return authorityRecord{}, fmt.Errorf("unknown start kind %q", raw.StartKind)
	}
	start, err := authorityPointFromDB(
		raw.StartKind == "origin",
		raw.StartSlot,
		raw.StartHash,
		raw.StartBlockNumber,
		raw.StartIsByronEBB,
	)
	if err != nil {
		return authorityRecord{}, err
	}
	checked, err := optionalAuthorityHead(
		raw.CheckedEventSeq,
		raw.CheckedPointOrigin,
		raw.CheckedPointSlot,
		raw.CheckedPointHash,
		raw.CheckedPointBlockNumber,
		raw.CheckedPointIsByronEBB,
	)
	if err != nil {
		return authorityRecord{}, err
	}
	lastAgreed, err := optionalAuthorityHead(
		raw.LastAgreedEventSeq,
		raw.LastAgreedPointOrigin,
		raw.LastAgreedPointSlot,
		raw.LastAgreedPointHash,
		raw.LastAgreedPointBlockNumber,
		raw.LastAgreedPointIsByronEBB,
	)
	if err != nil {
		return authorityRecord{}, err
	}
	lastChecked, err := optionalAuthorityHead(
		raw.LastAgreedCheckedEventSeq,
		raw.LastAgreedCheckedOrigin,
		raw.LastAgreedCheckedSlot,
		raw.LastAgreedCheckedHash,
		raw.LastAgreedCheckedNumber,
		raw.LastAgreedCheckedIsByronEBB,
	)
	if err != nil {
		return authorityRecord{}, err
	}
	floor, err := authorityPointFromDB(
		raw.ServableFloorOrigin,
		raw.ServableFloorSlot,
		raw.ServableFloorHash,
		raw.ServableFloorBlockNumber,
		raw.ServableFloorIsByronEBB,
	)
	if err != nil {
		return authorityRecord{}, err
	}
	physical, err := authorityPointFromDB(
		raw.PhysicalTipOrigin,
		raw.PhysicalTipSlot,
		raw.PhysicalTipHash,
		raw.PhysicalTipBlockNumber,
		raw.PhysicalTipIsByronEBB,
	)
	if err != nil {
		return authorityRecord{}, err
	}
	effective, err := authorityPointFromDB(
		raw.EffectiveTipOrigin,
		raw.EffectiveTipSlot,
		raw.EffectiveTipHash,
		raw.EffectiveTipBlockNumber,
		raw.EffectiveTipIsByronEBB,
	)
	if err != nil {
		return authorityRecord{}, err
	}
	record := authorityRecord{
		ManifestKey:            raw.ManifestKey,
		Revision:               raw.Revision,
		TransitionID:           authorityUUID(raw.TransitionID),
		TransitionKind:         raw.TransitionKind,
		PreviousRowDigest:      previous,
		RowDigest:              rowDigest,
		DatasetID:              authorityUUID(raw.DatasetID),
		SchemaContractHash:     contract,
		NetworkMagic:           raw.NetworkMagic,
		NetworkName:            raw.NetworkName,
		ByronGenesisID:         byronID,
		ByronGenesisJSONHash:   byronJSON,
		ShelleyGenesisID:       shelleyID,
		ShelleyGenesisJSONHash: shelleyJSON,
		Start:                  start,
		GenesisSeeded:          raw.GenesisSeeded,
		CompleteHistory:        raw.CompleteHistory,
		TrustMode:              raw.TrustMode,
		TrustStatus:            raw.TrustStatus,
		TrustBasis:             raw.TrustBasis,
		CheckID:                optionalAuthorityUUID(raw.CheckID),
		AgreementGroup:         optionalAuthorityUUID(raw.AgreementGroup),
		CheckAttempt:           raw.CheckAttempt,
		CorroborationRequired:  raw.CorroborationRequired,
		CorroborationConfirmed: raw.CorroborationConfirmed,
		CheckpointInterval:     raw.CheckpointInterval,
		PrimarySuffix:          raw.PrimarySuffix,
		Disagreement:           raw.Disagreement,
		TrustReason:            raw.TrustReason,
		CheckStartedAt:         raw.CheckStartedAt,
		CheckCompletedAt:       raw.CheckCompletedAt,
		EvidenceState:          raw.EvidenceState,
		EvidenceCount:          raw.EvidenceCount,
		EvidenceDigest:         evidenceDigest,
		Checked:                checked,
		LastAgreed:             lastAgreed,
		LastAgreedAt:           raw.LastAgreedAt,
		ServableFloor:          authorityHead{EventSeq: raw.ServableFloorEventSeq, Point: floor},
		ServableFloorPermanent: raw.ServableFloorPermanent,
		Physical:               authorityHead{EventSeq: raw.PhysicalEventSeq, Point: physical},
		Effective:              authorityHead{EventSeq: raw.EffectiveEventSeq, Point: effective},
		Servable:               raw.Servable,
		VisibilityGeneration:   raw.VisibilityGeneration,
		WriterID:               optionalAuthorityUUID(raw.WriterID),
		WriterBuild:            raw.WriterBuild,
		SourceBuild:            raw.SourceBuild,
		CreatedAt:              raw.CreatedAt,
		UpdatedAt:              raw.UpdatedAt,
	}
	if raw.LastAgreedCheckID == nil {
		if raw.LastAgreedAgreementGroup != nil ||
			raw.LastAgreedCheckAttempt != 0 ||
			raw.LastAgreedRequired != 0 ||
			raw.LastAgreedConfirmed != 0 ||
			lastChecked != nil ||
			raw.LastAgreedEvidenceCount != 0 ||
			raw.LastAgreedEvidenceDigest != nil {
			return authorityRecord{}, errors.New("absent last-agreed evidence carries fields")
		}
	} else {
		digest, err := optionalAuthorityHash(raw.LastAgreedEvidenceDigest)
		if err != nil {
			return authorityRecord{}, err
		}
		if raw.LastAgreedAgreementGroup == nil || lastChecked == nil || digest == nil {
			return authorityRecord{}, errors.New("last-agreed evidence is incomplete")
		}
		record.LastAgreedEvidence = &authorityEvidenceReference{
			CheckID:   authorityUUID(*raw.LastAgreedCheckID),
			Group:     authorityUUID(*raw.LastAgreedAgreementGroup),
			Attempt:   raw.LastAgreedCheckAttempt,
			Required:  raw.LastAgreedRequired,
			Confirmed: raw.LastAgreedConfirmed,
			Checked:   *lastChecked,
			Count:     raw.LastAgreedEvidenceCount,
			Digest:    *digest,
		}
	}
	if raw.PendingEvidenceID == nil {
		if raw.PendingEvidenceDigest != nil ||
			raw.PendingEvidencePayload != "" ||
			raw.PendingEvidenceWriter != nil ||
			raw.PendingEvidenceAt != nil {
			return authorityRecord{}, errors.New("absent pending evidence carries fields")
		}
	} else {
		digest, err := optionalAuthorityHash(raw.PendingEvidenceDigest)
		if err != nil {
			return authorityRecord{}, err
		}
		if digest == nil || raw.PendingEvidencePayload == "" ||
			raw.PendingEvidenceWriter == nil || raw.PendingEvidenceAt == nil {
			return authorityRecord{}, errors.New("pending evidence is incomplete")
		}
		observation, err := decodeAuthorityObservation(raw.PendingEvidencePayload)
		if err != nil {
			return authorityRecord{}, err
		}
		if observation.ID != authorityUUID(*raw.PendingEvidenceID) {
			return authorityRecord{}, errors.New("pending evidence payload ID mismatch")
		}
		if err := verifyAuthorityObservation(observation, *digest); err != nil {
			return authorityRecord{}, err
		}
		record.PendingEvidenceWrite = &authorityPendingEvidence{
			Observation: observation,
			Digest:      *digest,
			Payload:     raw.PendingEvidencePayload,
			WriterID:    authorityUUID(*raw.PendingEvidenceWriter),
			ReservedAt:  *raw.PendingEvidenceAt,
		}
	}
	if raw.PendingRollbackState == "none" {
		if raw.PendingRollbackID != nil ||
			raw.PendingRollbackEventSeq != nil ||
			raw.PendingRollbackToOrigin != nil ||
			raw.PendingRollbackToSlot != nil ||
			raw.PendingRollbackToHash != nil ||
			raw.PendingRollbackToBlockNumber != nil ||
			raw.PendingRollbackToIsByronEBB != nil ||
			raw.PendingRollbackOldPhysicalEventSeq != nil ||
			raw.PendingRollbackOldPhysicalOrigin != nil ||
			raw.PendingRollbackOldPhysicalSlot != nil ||
			raw.PendingRollbackOldPhysicalHash != nil ||
			raw.PendingRollbackOldPhysicalBlockNumber != nil ||
			raw.PendingRollbackOldPhysicalIsByronEBB != nil ||
			raw.PendingRollbackDepth != nil ||
			raw.PendingRollbackReason != "" ||
			len(raw.PendingRollbackObservedPeers) != 0 ||
			len(raw.PendingRollbackObservedOperators) != 0 ||
			raw.PendingRollbackRequired != nil ||
			raw.PendingRollbackCheckID != nil ||
			raw.PendingRollbackAgreementGroup != nil ||
			raw.PendingRollbackCheckAttempt != nil ||
			raw.PendingRollbackCheckedEventSeq != nil ||
			raw.PendingRollbackEvidenceCount != nil ||
			raw.PendingRollbackEvidenceDigest != nil ||
			raw.PendingRollbackWriterID != nil ||
			raw.PendingRollbackStartedAt != nil {
			return authorityRecord{}, errors.New("absent pending rollback carries fields")
		}
	} else {
		if raw.PendingRollbackState != "reserved" &&
			raw.PendingRollbackState != "invalidations_written" {
			return authorityRecord{}, errors.New("unknown pending rollback state")
		}
		required := raw.PendingRollbackRequired
		if raw.PendingRollbackID == nil ||
			raw.PendingRollbackEventSeq == nil ||
			raw.PendingRollbackToOrigin == nil ||
			raw.PendingRollbackToIsByronEBB == nil ||
			raw.PendingRollbackOldPhysicalEventSeq == nil ||
			raw.PendingRollbackOldPhysicalOrigin == nil ||
			raw.PendingRollbackOldPhysicalIsByronEBB == nil ||
			raw.PendingRollbackDepth == nil ||
			required == nil ||
			raw.PendingRollbackCheckID == nil ||
			raw.PendingRollbackAgreementGroup == nil ||
			raw.PendingRollbackCheckAttempt == nil ||
			raw.PendingRollbackCheckedEventSeq == nil ||
			raw.PendingRollbackEvidenceCount == nil ||
			raw.PendingRollbackEvidenceDigest == nil ||
			raw.PendingRollbackWriterID == nil ||
			raw.PendingRollbackStartedAt == nil {
			return authorityRecord{}, errors.New("pending rollback is incomplete")
		}
		to, err := authorityPointFromDB(
			*raw.PendingRollbackToOrigin,
			raw.PendingRollbackToSlot,
			raw.PendingRollbackToHash,
			raw.PendingRollbackToBlockNumber,
			*raw.PendingRollbackToIsByronEBB,
		)
		if err != nil {
			return authorityRecord{}, err
		}
		old, err := authorityPointFromDB(
			*raw.PendingRollbackOldPhysicalOrigin,
			raw.PendingRollbackOldPhysicalSlot,
			raw.PendingRollbackOldPhysicalHash,
			raw.PendingRollbackOldPhysicalBlockNumber,
			*raw.PendingRollbackOldPhysicalIsByronEBB,
		)
		if err != nil {
			return authorityRecord{}, err
		}
		digest, err := fixedAuthorityHash(*raw.PendingRollbackEvidenceDigest)
		if err != nil {
			return authorityRecord{}, err
		}
		record.PendingRollback = &authorityPendingRollback{
			State:           raw.PendingRollbackState,
			ID:              authorityUUID(*raw.PendingRollbackID),
			EventSeq:        *raw.PendingRollbackEventSeq,
			To:              to,
			OldPhysical:     authorityHead{EventSeq: *raw.PendingRollbackOldPhysicalEventSeq, Point: old},
			Depth:           *raw.PendingRollbackDepth,
			Reason:          raw.PendingRollbackReason,
			Peers:           append([]string(nil), raw.PendingRollbackObservedPeers...),
			Operators:       append([]string(nil), raw.PendingRollbackObservedOperators...),
			Required:        *required,
			CheckID:         authorityUUID(*raw.PendingRollbackCheckID),
			Group:           authorityUUID(*raw.PendingRollbackAgreementGroup),
			CheckAttempt:    *raw.PendingRollbackCheckAttempt,
			CheckedEventSeq: *raw.PendingRollbackCheckedEventSeq,
			EvidenceCount:   *raw.PendingRollbackEvidenceCount,
			EvidenceDigest:  digest,
			WriterID:        authorityUUID(*raw.PendingRollbackWriterID),
			StartedAt:       *raw.PendingRollbackStartedAt,
		}
	}
	normalizeAuthorityTimes(&record)
	return record, nil
}
