package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"

	"clicksync/internal/model"
	"clicksync/internal/publication"
	"clicksync/migrations"
)

const (
	manifestKey              = uint8(1)
	manifestLatestReadLimit  = 9
	manifestDuplicateLimit   = 8
	manifestCheckpointBlocks = uint64(512)
	manifestMaximumSuffix    = uint64(767)
	boundedManifestHeadQuery = `
SELECT *
FROM clicksync.dataset_manifest
PREWHERE manifest_key = 1
ORDER BY revision DESC
LIMIT 9`
)

type manifestHead struct {
	EventSeq uint64
	Point    publication.Point
}

type manifestPendingRollback struct {
	State           string
	ID              [16]byte
	EventSeq        uint64
	To              publication.Point
	OldPhysical     manifestHead
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
	EvidenceDigest  model.Hash32
	WriterID        [16]byte
	StartedAt       time.Time
}

type manifestPendingEvidenceWrite struct {
	Observation model.PeerObservation
	Digest      model.Hash32
	Payload     string
	WriterID    [16]byte
	ReservedAt  time.Time
}

type manifestEvidenceReference struct {
	CheckID   [16]byte
	Group     [16]byte
	Attempt   uint32
	Required  uint16
	Confirmed uint16
	Checked   manifestHead
	Count     uint32
	Digest    model.Hash32
}

// manifestRecord is the complete authoritative dataset state. It deliberately
// mirrors every dataset_manifest column: the digest helper therefore cannot
// accidentally omit a persisted semantic field.
type manifestRecord struct {
	ManifestKey       uint8
	Revision          uint64
	TransitionID      [16]byte
	TransitionKind    string
	PreviousRowDigest *model.Hash32
	RowDigest         model.Hash32

	DatasetID              [16]byte
	SchemaContractHash     model.Hash32
	NetworkMagic           uint32
	NetworkName            string
	ByronGenesisID         model.Hash32
	ByronGenesisJSONHash   model.Hash32
	ShelleyGenesisID       model.Hash32
	ShelleyGenesisJSONHash model.Hash32
	Start                  publication.Point
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
	EvidenceDigest         *model.Hash32
	PendingEvidenceWrite   *manifestPendingEvidenceWrite
	Checked                *manifestHead
	LastAgreed             *manifestHead
	LastAgreedAt           *time.Time
	LastAgreedEvidence     *manifestEvidenceReference
	ServableFloor          manifestHead
	ServableFloorPermanent bool
	Physical               manifestHead
	Effective              manifestHead
	Servable               bool
	VisibilityGeneration   uint64
	PendingRollback        *manifestPendingRollback

	WriterID    *[16]byte
	WriterBuild string
	SourceBuild string
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// manifestDBRow is intentionally flat so clickhouse-go's name-based
// ScanStruct/AppendStruct can enforce a one-to-one column mapping.
type manifestDBRow struct {
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

func manifestCanonicalPayload(row manifestRecord) ([]byte, error) {
	encoded, err := json.Marshal(row)
	if err != nil {
		return nil, fmt.Errorf("marshal manifest row: %w", err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &fields); err != nil {
		return nil, fmt.Errorf("decode manifest canonical fields: %w", err)
	}
	delete(fields, "RowDigest")
	return json.Marshal(fields)
}

func finalizeManifestRecord(row *manifestRecord) error {
	row.ManifestKey = manifestKey
	normalizeManifestTimes(row)

	transitionSeed := *row
	transitionSeed.TransitionID = [16]byte{}
	payload, err := manifestCanonicalPayload(transitionSeed)
	if err != nil {
		return err
	}
	transitionHash := sha256.Sum256(append(
		[]byte("clicksync-manifest-transition\x00"),
		payload...,
	))
	copy(row.TransitionID[:], transitionHash[:16])
	// Set UUID version/variant bits while retaining deterministic content.
	row.TransitionID[6] = (row.TransitionID[6] & 0x0f) | 0x50
	row.TransitionID[8] = (row.TransitionID[8] & 0x3f) | 0x80

	payload, err = manifestCanonicalPayload(*row)
	if err != nil {
		return err
	}
	row.RowDigest = sha256.Sum256(payload)
	return nil
}

func verifyManifestRecord(row manifestRecord) error {
	if row.ManifestKey != manifestKey {
		return fmt.Errorf("manifest key %d is not the singleton key", row.ManifestKey)
	}
	if row.Revision == 0 {
		return errors.New("manifest revision is zero")
	}
	if row.SchemaContractHash != migrations.ContractHash {
		return errors.New("manifest schema contract hash differs from embedded canonical contract")
	}
	if row.DatasetID == ([16]byte{}) ||
		row.NetworkMagic == 0 ||
		row.NetworkName == "" ||
		row.ByronGenesisID == (model.Hash32{}) ||
		row.ByronGenesisJSONHash == (model.Hash32{}) ||
		row.ShelleyGenesisID == (model.Hash32{}) ||
		row.ShelleyGenesisJSONHash == (model.Hash32{}) {
		return errors.New("manifest immutable dataset identity is incomplete")
	}
	switch row.TransitionKind {
	case "initialize",
		"physical_adoption",
		"physical_rollback",
		"physical_genesis",
		"physical_reconcile",
		"genesis_complete",
		"trust_check_started",
		"trust_agreed",
		"trust_unavailable",
		"trust_disputed",
		"trust_superseded",
		"evidence_write_reserved",
		"evidence_write_committed",
		"evidence_frozen",
		"bootstrap_agreed",
		"rollback_reserved",
		"rollback_invalidations_written",
		"rollback_finalized",
		"rollback_recovered":
	default:
		return fmt.Errorf("unknown manifest transition kind %q", row.TransitionKind)
	}
	if row.TrustMode != "peer_observed_structurally_verified" {
		return fmt.Errorf("unknown manifest trust mode %q", row.TrustMode)
	}
	if row.CheckpointInterval != manifestCheckpointBlocks {
		return fmt.Errorf("manifest checkpoint interval %d is not %d", row.CheckpointInterval, manifestCheckpointBlocks)
	}
	if row.PrimarySuffix > manifestMaximumSuffix {
		return fmt.Errorf("manifest primary suffix %d exceeds %d", row.PrimarySuffix, manifestMaximumSuffix)
	}
	if uint32(row.CorroborationConfirmed) > row.EvidenceCount {
		return errors.New("manifest confirmed corroboration exceeds committed evidence count")
	}
	if row.CompleteHistory != (row.Start.Origin && row.GenesisSeeded) {
		return errors.New("manifest completeness does not match start/genesis state")
	}
	switch row.TrustBasis {
	case "official_genesis", "sampled_peer", "partial_boundary", "primary_only":
	default:
		return fmt.Errorf("unknown manifest trust basis %q", row.TrustBasis)
	}
	if row.TrustBasis == "primary_only" &&
		(row.PrimarySuffix == 0 ||
			row.LastAgreed == nil ||
			(row.LastAgreedEvidence == nil && !row.ServableFloorPermanent)) {
		return errors.New(
			"primary-only manifest lacks a sampled/genesis anchor and suffix",
		)
	}
	switch row.TrustStatus {
	case "agreed":
		if row.Disagreement {
			return errors.New("agreed manifest carries disagreement")
		}
		if !row.Servable || row.Effective != row.Physical {
			return errors.New("agreed manifest must serve its physical head")
		}
		if row.CheckID != nil &&
			row.CorroborationConfirmed < row.CorroborationRequired {
			return errors.New("sampled agreement has insufficient corroboration")
		}
		if row.TrustBasis == "official_genesis" {
			if row.CheckID != nil ||
				row.CorroborationRequired != 0 ||
				row.CorroborationConfirmed != 0 {
				return errors.New("official genesis agreement carries peer-check state")
			}
		} else if row.TrustBasis != "primary_only" &&
			(row.TrustBasis != "sampled_peer" ||
				row.CheckID == nil ||
				row.CorroborationRequired < 2 ||
				row.CorroborationConfirmed < row.CorroborationRequired ||
				row.Checked == nil ||
				row.LastAgreed == nil ||
				!manifestAgreementEvidenceMatches(row)) {
			return errors.New("peer-derived agreement lacks threshold check evidence")
		}
	case "unavailable":
		if row.Disagreement {
			return errors.New("unavailable manifest carries disagreement")
		}
		if row.Servable {
			if row.Effective != row.Physical {
				return errors.New("servable unavailable manifest must expose its physical head")
			}
		} else if row.LastAgreed != nil ||
			row.Effective != row.ServableFloor ||
			row.ServableFloor.EventSeq != 0 {
			return errors.New("unservable bootstrap manifest is not clamped to its event-zero boundary")
		}
	case "checking", "disputed":
		if row.CheckID == nil {
			return fmt.Errorf("%s manifest has no check identity", row.TrustStatus)
		}
		expectedClamp := row.ServableFloor
		if row.LastAgreed != nil {
			expectedClamp = *row.LastAgreed
		}
		if manifestPointBefore(row.Physical.Point, expectedClamp.Point) {
			expectedClamp = row.Physical
		}
		if row.Effective != expectedClamp {
			return fmt.Errorf(
				"%s manifest effective head is not its agreed/local clamp",
				row.TrustStatus,
			)
		}
		expectedServable := row.LastAgreed != nil || row.ServableFloorPermanent
		if row.Servable != expectedServable {
			return fmt.Errorf(
				"%s manifest servability does not match its safe floor",
				row.TrustStatus,
			)
		}
		if row.TrustStatus == "checking" && row.Disagreement {
			return errors.New("checking manifest already carries disagreement")
		}
		if row.TrustStatus == "disputed" && !row.Disagreement {
			return errors.New("disputed manifest lacks disagreement")
		}
	default:
		return fmt.Errorf("unknown manifest trust status %q", row.TrustStatus)
	}
	if row.Effective.EventSeq > row.Physical.EventSeq {
		return errors.New("manifest effective event is newer than physical event")
	}
	if manifestPointBefore(row.Effective.Point, row.ServableFloor.Point) {
		return errors.New("manifest effective point is below its servable floor")
	}
	if row.ServableFloorPermanent &&
		(!row.Start.Origin || !row.GenesisSeeded || !row.ServableFloor.Point.Origin) {
		return errors.New("only verified official genesis can be a permanent floor")
	}
	if !row.Start.Origin &&
		(row.ServableFloorPermanent ||
			row.ServableFloor.EventSeq != 0 ||
			row.ServableFloor.Point != row.Start) {
		return errors.New("partial-history manifest lost its nonpermanent event-zero boundary")
	}
	if row.Start.Origin && !row.GenesisSeeded && row.Servable {
		return errors.New("Origin manifest is servable before exact genesis seeding")
	}
	if row.Start.Origin && row.GenesisSeeded &&
		(!row.ServableFloorPermanent || !row.ServableFloor.Point.Origin) {
		return errors.New("seeded Origin manifest lacks its permanent official floor")
	}
	if (row.CheckID == nil) != (row.AgreementGroup == nil) {
		return errors.New("manifest check ID and agreement group nullability differ")
	}
	if row.CheckID == nil {
		if row.Checked != nil ||
			row.CheckAttempt != 0 ||
			row.CorroborationRequired != 0 ||
			row.CorroborationConfirmed != 0 ||
			row.CheckStartedAt != nil ||
			row.CheckCompletedAt != nil ||
			row.EvidenceState != "none" ||
			row.EvidenceCount != 0 ||
			row.EvidenceDigest != nil ||
			row.PendingEvidenceWrite != nil {
			return errors.New("manifest without a check identity carries check state")
		}
	} else {
		if *row.CheckID == ([16]byte{}) ||
			*row.AgreementGroup == ([16]byte{}) ||
			row.Checked == nil ||
			row.CheckAttempt == 0 ||
			row.CorroborationRequired < 2 ||
			row.CheckStartedAt == nil {
			return errors.New("manifest check state is incomplete")
		}
		if (row.EvidenceState != "open" && row.EvidenceState != "frozen") ||
			row.EvidenceDigest == nil {
			return errors.New("manifest peer check lacks durable evidence state/commitment")
		}
		if row.TrustStatus == "checking" {
			if row.CheckCompletedAt != nil {
				return errors.New("checking manifest already has a completion time")
			}
		} else if row.CheckCompletedAt == nil {
			return errors.New("completed check state lacks a completion time")
		} else if row.EvidenceState != "frozen" {
			return errors.New("completed check evidence is not frozen")
		}
		if row.PendingEvidenceWrite != nil {
			pending := row.PendingEvidenceWrite
			if row.EvidenceState != "open" ||
				pending.Observation.CheckID != *row.CheckID ||
				pending.Observation.AgreementGroup != *row.AgreementGroup ||
				pending.Observation.CheckAttempt != row.CheckAttempt ||
				pending.Observation.EvidenceOrdinal != row.EvidenceCount+1 ||
				pending.Observation.Kind == "source_change" ||
				pending.WriterID == ([16]byte{}) {
				return errors.New("pending evidence write differs from exact open check")
			}
			canonical, err := canonicalPendingEvidencePayload(pending.Observation)
			if err != nil {
				return err
			}
			digest, err := model.PeerObservationDigest(pending.Observation)
			if err != nil {
				return err
			}
			if canonical != pending.Payload ||
				digest != pending.Digest {
				return errors.New(
					"pending evidence payload/digest is not canonical",
				)
			}
			if err := model.VerifyPeerObservationIdentity(
				pending.Observation,
				pending.Digest[:],
			); err != nil {
				return fmt.Errorf("pending evidence identity is invalid: %w", err)
			}
			eligible, err := validateTrustEvidenceProvenance(
				pending.Observation,
				pendingEvidenceCheckedPoint(pending.Observation),
			)
			if err != nil {
				return err
			}
			if !eligible ||
				pending.Observation.CheckedEventSeq != row.Checked.EventSeq ||
				pendingEvidenceCheckedPoint(pending.Observation) != row.Checked.Point ||
				pending.Observation.CorroborationRequired != row.CorroborationRequired {
				return errors.New(
					"pending evidence payload is not eligible for the exact manifest check",
				)
			}
		}
	}
	if row.LastAgreed == nil {
		if row.LastAgreedAt != nil || row.LastAgreedEvidence != nil {
			return errors.New("manifest last-agreed time lacks an event-point")
		}
	} else if row.LastAgreedAt == nil {
		return errors.New("manifest last-agreed event-point lacks a time")
	} else if row.LastAgreedEvidence == nil {
		if !row.ServableFloorPermanent ||
			!row.ServableFloor.Point.Origin ||
			*row.LastAgreed != row.ServableFloor {
			return errors.New(
				"peer-derived last-agreed head lacks its immutable evidence reference",
			)
		}
	} else {
		reference := row.LastAgreedEvidence
		if reference.CheckID == ([16]byte{}) ||
			reference.Group == ([16]byte{}) ||
			reference.Attempt == 0 ||
			reference.Required < 2 ||
			reference.Confirmed < reference.Required ||
			uint32(reference.Confirmed) > reference.Count ||
			reference.Digest == (model.Hash32{}) ||
			reference.Checked.Point != row.LastAgreed.Point ||
			reference.Checked.EventSeq > row.LastAgreed.EventSeq {
			return errors.New("manifest last-agreed evidence reference is invalid")
		}
		if row.TrustStatus == "agreed" &&
			row.TrustBasis != "official_genesis" &&
			!currentEvidenceMatchesReference(row, *reference) {
			return errors.New(
				"agreed manifest current evidence differs from last-agreed authority",
			)
		}
	}
	for _, candidate := range []struct {
		name  string
		point publication.Point
	}{
		{name: "start", point: row.Start},
		{name: "servable floor", point: row.ServableFloor.Point},
		{name: "physical head", point: row.Physical.Point},
		{name: "effective head", point: row.Effective.Point},
	} {
		if err := validateManifestPoint(candidate.name, candidate.point); err != nil {
			return err
		}
	}
	if row.Checked != nil {
		if err := validateManifestPoint("checked point", row.Checked.Point); err != nil {
			return err
		}
		if row.Checked.EventSeq > row.Physical.EventSeq {
			return errors.New("checked event is newer than physical event")
		}
	}
	if row.LastAgreed != nil {
		if err := validateManifestPoint("last-agreed point", row.LastAgreed.Point); err != nil {
			return err
		}
		if row.LastAgreed.EventSeq > row.Physical.EventSeq {
			return errors.New("last-agreed event is newer than physical event")
		}
	}
	if row.Revision == 1 && row.PreviousRowDigest != nil {
		return errors.New("initial manifest row has a predecessor digest")
	}
	if row.Revision > 1 && row.PreviousRowDigest == nil {
		return errors.New("non-initial manifest row has no predecessor digest")
	}
	if row.PendingRollback == nil {
		// The SQL representation uses the explicit enum value "none".
	} else if row.PendingRollback.ID == ([16]byte{}) ||
		row.PendingRollback.State == "" ||
		row.PendingRollback.State == "none" {
		return errors.New("manifest pending rollback identity/state is incomplete")
	} else if row.PendingRollback.State != "reserved" &&
		row.PendingRollback.State != "invalidations_written" {
		return fmt.Errorf("unknown pending rollback state %q", row.PendingRollback.State)
	} else if row.PendingRollback.EventSeq <= row.Physical.EventSeq ||
		row.PendingRollback.OldPhysical != row.Physical {
		return errors.New("pending rollback reservation is not anchored to the physical head")
	} else if row.PendingRollback.Reason == "" ||
		row.PendingRollback.Required < 2 ||
		row.PendingRollback.CheckID == ([16]byte{}) ||
		row.PendingRollback.Group == ([16]byte{}) ||
		row.PendingRollback.CheckAttempt == 0 ||
		row.PendingRollback.EvidenceCount < uint32(row.PendingRollback.Required) ||
		row.PendingRollback.EvidenceDigest == (model.Hash32{}) ||
		row.PendingRollback.EvidenceCount != row.EvidenceCount ||
		row.EvidenceDigest == nil ||
		row.PendingRollback.EvidenceDigest != *row.EvidenceDigest ||
		row.PendingRollback.WriterID == ([16]byte{}) ||
		len(row.PendingRollback.Peers) != len(row.PendingRollback.Operators) ||
		len(row.PendingRollback.Operators) < int(row.PendingRollback.Required) {
		return errors.New("pending rollback corroboration reservation is incomplete")
	} else if row.EvidenceState != "frozen" ||
		row.PendingEvidenceWrite != nil {
		return errors.New("pending rollback evidence is not durably frozen")
	} else if err := validateManifestPoint(
		"pending rollback target",
		row.PendingRollback.To,
	); err != nil {
		return err
	} else if err := validateManifestPoint(
		"pending rollback old physical point",
		row.PendingRollback.OldPhysical.Point,
	); err != nil {
		return err
	}
	expected := row
	if err := finalizeManifestRecord(&expected); err != nil {
		return err
	}
	if expected.TransitionID != row.TransitionID {
		return errors.New("manifest transition ID does not match canonical content")
	}
	if expected.RowDigest != row.RowDigest {
		return errors.New("manifest row digest does not match canonical content")
	}
	return nil
}

func pendingEvidenceCheckedPoint(
	observation model.PeerObservation,
) publication.Point {
	if observation.CheckedPointOrigin {
		return publication.Point{Origin: true}
	}
	if observation.CheckedPointSlot == nil ||
		observation.CheckedPointHash == nil ||
		observation.CheckedBlockNumber == nil {
		return publication.Point{}
	}
	return publication.Point{
		Slot:        *observation.CheckedPointSlot,
		Hash:        *observation.CheckedPointHash,
		BlockNumber: *observation.CheckedBlockNumber,
		IsByronEBB:  observation.CheckedPointIsByronEBB,
	}
}

func manifestAgreementEvidenceMatches(row manifestRecord) bool {
	if row.Checked == nil || row.LastAgreed == nil {
		return false
	}
	if *row.Checked == *row.LastAgreed {
		return true
	}
	return row.Checked.Point == row.LastAgreed.Point &&
		row.Checked.EventSeq < row.LastAgreed.EventSeq &&
		row.LastAgreed.EventSeq <= row.Physical.EventSeq
}

func currentEvidenceMatchesReference(
	row manifestRecord,
	reference manifestEvidenceReference,
) bool {
	return row.CheckID != nil &&
		row.AgreementGroup != nil &&
		row.Checked != nil &&
		row.EvidenceDigest != nil &&
		*row.CheckID == reference.CheckID &&
		*row.AgreementGroup == reference.Group &&
		row.CheckAttempt == reference.Attempt &&
		row.CorroborationRequired == reference.Required &&
		row.CorroborationConfirmed == reference.Confirmed &&
		*row.Checked == reference.Checked &&
		row.EvidenceCount == reference.Count &&
		*row.EvidenceDigest == reference.Digest
}

func validateManifestPoint(name string, point publication.Point) error {
	if point.Origin {
		if point.Hash != (model.Hash32{}) ||
			point.Slot != 0 ||
			point.BlockNumber != 0 ||
			point.IsByronEBB {
			return fmt.Errorf("%s Origin carries non-Origin fields", name)
		}
		return nil
	}
	if point.Hash == (model.Hash32{}) {
		return fmt.Errorf("%s has a zero block hash", name)
	}
	return nil
}

func normalizeManifestTimes(row *manifestRecord) {
	row.CreatedAt = manifestTime(row.CreatedAt)
	row.UpdatedAt = manifestTime(row.UpdatedAt)
	normalizeOptionalTime(&row.CheckStartedAt)
	normalizeOptionalTime(&row.CheckCompletedAt)
	normalizeOptionalTime(&row.LastAgreedAt)
	if row.PendingEvidenceWrite != nil {
		row.PendingEvidenceWrite.ReservedAt = manifestTime(
			row.PendingEvidenceWrite.ReservedAt,
		)
		row.PendingEvidenceWrite.Observation.ObservedAt = manifestTime(
			row.PendingEvidenceWrite.Observation.ObservedAt,
		)
	}
	if row.PendingRollback != nil {
		row.PendingRollback.StartedAt = manifestTime(row.PendingRollback.StartedAt)
	}
}

func normalizeOptionalTime(value **time.Time) {
	if *value == nil {
		return
	}
	normalized := manifestTime(**value)
	*value = &normalized
}

func manifestTime(value time.Time) time.Time {
	return value.UTC().Truncate(time.Microsecond)
}

func (d *DB) loadLatestManifestRecord(
	ctx context.Context,
) (manifestRecord, bool, error) {
	rows, err := d.conn.Query(ctx, boundedManifestHeadQuery)
	if err != nil {
		return manifestRecord{}, false, fmt.Errorf("query bounded manifest head: %w", err)
	}
	defer rows.Close()
	records := make([]manifestRecord, 0, manifestLatestReadLimit)
	for rows.Next() {
		var raw manifestDBRow
		if err := rows.ScanStruct(&raw); err != nil {
			return manifestRecord{}, false, fmt.Errorf("scan manifest row: %w", err)
		}
		record, err := raw.manifestRecord()
		if err != nil {
			return manifestRecord{}, false, err
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return manifestRecord{}, false, fmt.Errorf("iterate bounded manifest head: %w", err)
	}
	return validateBoundedManifestRows(records)
}

func (d *DB) loadAuthoritativeManifest(
	ctx context.Context,
) (manifestRecord, bool, error) {
	return loadStableAuthoritativeManifest(
		ctx,
		d.loadLatestManifestRecord,
		d.validateAuthoritativeManifestSnapshot,
	)
}

func loadStableAuthoritativeManifest(
	ctx context.Context,
	load func(context.Context) (manifestRecord, bool, error),
	validate func(context.Context, manifestRecord) error,
) (manifestRecord, bool, error) {
	for {
		if err := ctx.Err(); err != nil {
			return manifestRecord{}, false, err
		}
		record, found, err := load(ctx)
		if err != nil || !found {
			return record, found, err
		}
		validationErr := validate(ctx, record)
		latest, stillFound, reloadErr := load(ctx)
		if reloadErr != nil {
			return manifestRecord{}, true, reloadErr
		}
		if !stillFound {
			return manifestRecord{}, true, errors.New(
				"dataset manifest disappeared during authoritative validation",
			)
		}
		if latest.Revision != record.Revision ||
			latest.RowDigest != record.RowDigest {
			continue
		}
		if validationErr != nil {
			return manifestRecord{}, true, validationErr
		}
		return record, true, nil
	}
}

func (d *DB) validateAuthoritativeManifestSnapshot(
	ctx context.Context,
	record manifestRecord,
) error {
	if err := d.validateManifestEvidenceCommitment(ctx, record); err != nil {
		return fmt.Errorf("validate authoritative evidence commitment: %w", err)
	}
	if err := d.validateCurrentPhysicalRollbackArtifacts(ctx, record); err != nil {
		return fmt.Errorf("validate authoritative physical event: %w", err)
	}
	barrier := record.Physical.EventSeq
	if pending := record.PendingRollback; pending != nil {
		if err := d.validatePendingRollbackArtifacts(ctx, *pending); err != nil {
			return fmt.Errorf("validate authoritative pending rollback: %w", err)
		}
		barrier = pending.EventSeq
	}
	if err := d.rejectUnreservedRollbackArtifactsAfter(ctx, barrier); err != nil {
		return err
	}
	return nil
}

func validateBoundedManifestRows(
	records []manifestRecord,
) (manifestRecord, bool, error) {
	if len(records) == 0 {
		return manifestRecord{}, false, nil
	}
	latest := records[0]
	if err := verifyManifestRecord(latest); err != nil {
		return manifestRecord{}, false, fmt.Errorf("invalid latest manifest row: %w", err)
	}
	latestBytes, err := json.Marshal(latest)
	if err != nil {
		return manifestRecord{}, false, err
	}
	duplicates := 0
	index := 0
	for index < len(records) && records[index].Revision == latest.Revision {
		duplicates++
		if duplicates > manifestDuplicateLimit ||
			(duplicates == manifestLatestReadLimit && len(records) == manifestLatestReadLimit) {
			return manifestRecord{}, false, errors.New("manifest latest revision has at least nine physical rows")
		}
		if err := verifyManifestRecord(records[index]); err != nil {
			return manifestRecord{}, false, fmt.Errorf("invalid duplicate manifest head: %w", err)
		}
		encoded, err := json.Marshal(records[index])
		if err != nil {
			return manifestRecord{}, false, err
		}
		if !bytes.Equal(encoded, latestBytes) {
			return manifestRecord{}, false, errors.New("manifest latest revision has conflicting physical rows")
		}
		index++
	}
	if latest.Revision == 1 {
		if index != len(records) {
			return manifestRecord{}, false, errors.New(
				"initial manifest revision has impossible lower history",
			)
		}
		return latest, true, nil
	}
	if index == len(records) {
		return manifestRecord{}, false, errors.New("bounded manifest head is missing its predecessor")
	}
	predecessorRevision := latest.Revision - 1
	if records[index].Revision != predecessorRevision {
		return manifestRecord{}, false, fmt.Errorf(
			"manifest revision %d predecessor is %d, not %d",
			latest.Revision,
			records[index].Revision,
			predecessorRevision,
		)
	}
	for index < len(records) && records[index].Revision == predecessorRevision {
		predecessor := records[index]
		if err := verifyManifestRecord(predecessor); err != nil {
			return manifestRecord{}, false, fmt.Errorf("invalid manifest predecessor: %w", err)
		}
		if latest.PreviousRowDigest == nil || predecessor.RowDigest != *latest.PreviousRowDigest {
			return manifestRecord{}, false, errors.New("manifest predecessor digest does not match latest row")
		}
		if !sameManifestImmutableIdentity(latest, predecessor) {
			return manifestRecord{}, false, errors.New("manifest immutable identity changed between revisions")
		}
		index++
	}
	return latest, true, nil
}

func sameManifestImmutableIdentity(left, right manifestRecord) bool {
	return left.DatasetID == right.DatasetID &&
		left.SchemaContractHash == right.SchemaContractHash &&
		left.NetworkMagic == right.NetworkMagic &&
		left.NetworkName == right.NetworkName &&
		left.ByronGenesisID == right.ByronGenesisID &&
		left.ByronGenesisJSONHash == right.ByronGenesisJSONHash &&
		left.ShelleyGenesisID == right.ShelleyGenesisID &&
		left.ShelleyGenesisJSONHash == right.ShelleyGenesisJSONHash &&
		left.Start == right.Start &&
		left.TrustMode == right.TrustMode &&
		left.CreatedAt.Equal(right.CreatedAt)
}

func (d *DB) appendManifestRecord(
	ctx context.Context,
	row manifestRecord,
) error {
	if row.Revision == 0 || row.Revision == math.MaxUint64 {
		return errors.New("invalid manifest append revision")
	}
	if err := finalizeManifestRecord(&row); err != nil {
		return err
	}
	if err := verifyManifestRecord(row); err != nil {
		return fmt.Errorf("refuse invalid manifest append: %w", err)
	}
	raw, err := manifestDBRowFromRecord(row)
	if err != nil {
		return err
	}
	insertCtx := clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{
		"insert_deduplication_token": hex.EncodeToString(row.RowDigest[:]),
	}))
	batch, insertErr := d.conn.PrepareBatch(
		insertCtx,
		"INSERT INTO clicksync.dataset_manifest",
	)
	if insertErr == nil {
		insertErr = batch.AppendStruct(&raw)
		if insertErr == nil {
			insertErr = batch.Send()
		} else {
			_ = batch.Abort()
		}
	}
	latest, found, readErr := d.loadLatestManifestRecord(ctx)
	if readErr != nil {
		if insertErr != nil {
			return errors.Join(
				fmt.Errorf("append manifest revision %d: %w", row.Revision, insertErr),
				fmt.Errorf("verify uncertain manifest append: %w", readErr),
			)
		}
		return fmt.Errorf("verify manifest append: %w", readErr)
	}
	if !found {
		return fmt.Errorf("manifest revision %d is absent after append: %w", row.Revision, insertErr)
	}
	if latest.Revision > row.Revision {
		return fmt.Errorf("manifest advanced to later revision %d while appending %d", latest.Revision, row.Revision)
	}
	if latest.Revision == row.Revision && latest.RowDigest == row.RowDigest {
		return nil
	}
	if latest.Revision == row.Revision {
		return fmt.Errorf("manifest revision %d exists with conflicting content", row.Revision)
	}
	if insertErr != nil {
		return fmt.Errorf("append manifest revision %d: %w", row.Revision, insertErr)
	}
	return fmt.Errorf("manifest append revision %d was not visible after successful insert", row.Revision)
}

func appendManifestTransition(
	latest manifestRecord,
	kind string,
	at time.Time,
	mutate func(*manifestRecord) error,
) (manifestRecord, error) {
	if latest.Revision == math.MaxUint64 {
		return manifestRecord{}, errors.New("dataset manifest revision space exhausted")
	}
	next := latest
	next.Revision++
	next.TransitionKind = kind
	previous := latest.RowDigest
	next.PreviousRowDigest = &previous
	next.TransitionID = [16]byte{}
	next.RowDigest = model.Hash32{}
	next.UpdatedAt = manifestTime(at)
	if err := mutate(&next); err != nil {
		return manifestRecord{}, err
	}
	if !sameManifestImmutableIdentity(latest, next) {
		return manifestRecord{}, errors.New("manifest transition changed immutable identity")
	}
	if err := finalizeManifestRecord(&next); err != nil {
		return manifestRecord{}, err
	}
	return next, nil
}

func (d *DB) transitionManifest(
	ctx context.Context,
	authority publication.Lock,
	kind string,
	at time.Time,
	alreadyApplied func(manifestRecord) (bool, error),
	mutate func(*manifestRecord) error,
) error {
	if authority == nil {
		return errors.New("manifest transition requires the real writer flock")
	}
	d.manifestMu.Lock()
	defer d.manifestMu.Unlock()

	if err := authority.AssertHeld(); err != nil {
		return fmt.Errorf("manifest transition flock is not held: %w", err)
	}
	latest, found, err := d.loadLatestManifestRecord(ctx)
	if err != nil {
		return err
	}
	if !found {
		return errors.New("dataset manifest is not initialized")
	}
	if alreadyApplied != nil {
		applied, err := alreadyApplied(latest)
		if err != nil {
			return err
		}
		if applied {
			return nil
		}
	}
	next, err := appendManifestTransition(latest, kind, at, mutate)
	if err != nil {
		return err
	}
	if err := authority.AssertHeld(); err != nil {
		return fmt.Errorf("manifest transition flock was lost before append: %w", err)
	}
	return d.appendManifestRecord(ctx, next)
}

func pointDBValues(point publication.Point) (
	origin bool,
	slot *uint64,
	hash *string,
	number *uint64,
	isByronEBB bool,
) {
	origin = point.Origin
	if point.Origin {
		return
	}
	slotValue := point.Slot
	hashValue := string(point.Hash[:])
	numberValue := point.BlockNumber
	slot = &slotValue
	hash = &hashValue
	number = &numberValue
	isByronEBB = point.IsByronEBB
	return
}

func optionalHeadDBValues(head *manifestHead) (
	eventSeq *uint64,
	origin *bool,
	slot *uint64,
	hash *string,
	number *uint64,
	isByronEBB *bool,
) {
	if head == nil {
		return
	}
	eventValue := head.EventSeq
	originValue, slot, hash, number, ebbValue := pointDBValues(head.Point)
	eventSeq = &eventValue
	origin = &originValue
	isByronEBB = &ebbValue
	return
}

func pointFromDB(
	origin bool,
	slot *uint64,
	hash *string,
	number *uint64,
	isByronEBB bool,
) (publication.Point, error) {
	if origin {
		if slot != nil || hash != nil || number != nil || isByronEBB {
			return publication.Point{}, errors.New("Origin point carries non-Origin fields")
		}
		return publication.Point{Origin: true}, nil
	}
	if slot == nil || hash == nil || number == nil {
		return publication.Point{}, errors.New("non-Origin point is incomplete")
	}
	decodedHash, err := fixedHash(*hash)
	if err != nil {
		return publication.Point{}, err
	}
	return publication.Point{
		Slot:        *slot,
		Hash:        decodedHash,
		BlockNumber: *number,
		IsByronEBB:  isByronEBB,
	}, nil
}

func optionalHeadFromDB(
	eventSeq *uint64,
	origin *bool,
	slot *uint64,
	hash *string,
	number *uint64,
	isByronEBB *bool,
) (*manifestHead, error) {
	if eventSeq == nil && origin == nil && slot == nil && hash == nil &&
		number == nil && isByronEBB == nil {
		return nil, nil
	}
	if eventSeq == nil || origin == nil || isByronEBB == nil {
		return nil, errors.New("optional manifest event-point is incomplete")
	}
	point, err := pointFromDB(*origin, slot, hash, number, *isByronEBB)
	if err != nil {
		return nil, err
	}
	return &manifestHead{EventSeq: *eventSeq, Point: point}, nil
}

func fixedHash(value string) (model.Hash32, error) {
	if len(value) != 32 {
		return model.Hash32{}, fmt.Errorf("manifest FixedString(32) length is %d", len(value))
	}
	var result model.Hash32
	copy(result[:], value)
	return result, nil
}

func hashPointer(value *string) (*model.Hash32, error) {
	if value == nil {
		return nil, nil
	}
	hash, err := fixedHash(*value)
	if err != nil {
		return nil, err
	}
	return &hash, nil
}

func canonicalPendingEvidencePayload(
	observation model.PeerObservation,
) (string, error) {
	observation.ObservedAt = manifestTime(observation.ObservedAt)
	encoded, err := json.Marshal(observation)
	if err != nil {
		return "", fmt.Errorf("encode pending evidence payload: %w", err)
	}
	return string(encoded), nil
}

func decodePendingEvidencePayload(
	payload string,
) (model.PeerObservation, error) {
	decoder := json.NewDecoder(bytes.NewBufferString(payload))
	decoder.DisallowUnknownFields()
	var observation model.PeerObservation
	if err := decoder.Decode(&observation); err != nil {
		return model.PeerObservation{}, fmt.Errorf(
			"decode pending evidence payload: %w",
			err,
		)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err == nil {
		return model.PeerObservation{}, errors.New(
			"pending evidence payload has trailing JSON",
		)
	}
	canonical, err := canonicalPendingEvidencePayload(observation)
	if err != nil {
		return model.PeerObservation{}, err
	}
	if canonical != payload {
		return model.PeerObservation{}, errors.New(
			"pending evidence payload is not canonical JSON",
		)
	}
	return observation, nil
}

func uuid16(value uuid.UUID) [16]byte {
	var result [16]byte
	copy(result[:], value[:])
	return result
}

func uuid16Pointer(value *uuid.UUID) *[16]byte {
	if value == nil {
		return nil
	}
	result := uuid16(*value)
	return &result
}

func uuidPointer(value *[16]byte) *uuid.UUID {
	if value == nil {
		return nil
	}
	result := uuid.UUID(*value)
	return &result
}

func manifestDBRowFromRecord(record manifestRecord) (manifestDBRow, error) {
	startKind := "intersection"
	startOrigin, startSlot, startHash, startNumber, startEBB :=
		pointDBValues(record.Start)
	if startOrigin {
		startKind = "origin"
	}
	checkedEvent, checkedOrigin, checkedSlot, checkedHash, checkedNumber, checkedEBB :=
		optionalHeadDBValues(record.Checked)
	agreedEvent, agreedOrigin, agreedSlot, agreedHash, agreedNumber, agreedEBB :=
		optionalHeadDBValues(record.LastAgreed)
	var (
		lastEvidenceCheckedEvent  *uint64
		lastEvidenceCheckedOrigin *bool
		lastEvidenceCheckedSlot   *uint64
		lastEvidenceCheckedHash   *string
		lastEvidenceCheckedNumber *uint64
		lastEvidenceCheckedEBB    *bool
	)
	if record.LastAgreedEvidence != nil {
		checked := record.LastAgreedEvidence.Checked
		lastEvidenceCheckedEvent,
			lastEvidenceCheckedOrigin,
			lastEvidenceCheckedSlot,
			lastEvidenceCheckedHash,
			lastEvidenceCheckedNumber,
			lastEvidenceCheckedEBB = optionalHeadDBValues(&checked)
	}
	floorOrigin, floorSlot, floorHash, floorNumber, floorEBB :=
		pointDBValues(record.ServableFloor.Point)
	physicalOrigin, physicalSlot, physicalHash, physicalNumber, physicalEBB :=
		pointDBValues(record.Physical.Point)
	effectiveOrigin, effectiveSlot, effectiveHash, effectiveNumber, effectiveEBB :=
		pointDBValues(record.Effective.Point)

	raw := manifestDBRow{
		ManifestKey:                 record.ManifestKey,
		Revision:                    record.Revision,
		TransitionID:                uuid.UUID(record.TransitionID),
		TransitionKind:              record.TransitionKind,
		RowDigest:                   string(record.RowDigest[:]),
		DatasetID:                   uuid.UUID(record.DatasetID),
		SchemaContractHash:          string(record.SchemaContractHash[:]),
		NetworkMagic:                record.NetworkMagic,
		NetworkName:                 record.NetworkName,
		ByronGenesisID:              string(record.ByronGenesisID[:]),
		ByronGenesisJSONHash:        string(record.ByronGenesisJSONHash[:]),
		ShelleyGenesisID:            string(record.ShelleyGenesisID[:]),
		ShelleyGenesisJSONHash:      string(record.ShelleyGenesisJSONHash[:]),
		StartKind:                   startKind,
		StartSlot:                   startSlot,
		StartHash:                   startHash,
		StartBlockNumber:            startNumber,
		StartIsByronEBB:             startEBB,
		GenesisSeeded:               record.GenesisSeeded,
		CompleteHistory:             record.CompleteHistory,
		TrustMode:                   record.TrustMode,
		TrustStatus:                 record.TrustStatus,
		TrustBasis:                  record.TrustBasis,
		CheckID:                     uuidPointer(record.CheckID),
		AgreementGroup:              uuidPointer(record.AgreementGroup),
		CheckAttempt:                record.CheckAttempt,
		CorroborationRequired:       record.CorroborationRequired,
		CorroborationConfirmed:      record.CorroborationConfirmed,
		CheckpointInterval:          record.CheckpointInterval,
		PrimarySuffix:               record.PrimarySuffix,
		Disagreement:                record.Disagreement,
		TrustReason:                 record.TrustReason,
		CheckStartedAt:              record.CheckStartedAt,
		CheckCompletedAt:            record.CheckCompletedAt,
		EvidenceState:               record.EvidenceState,
		EvidenceCount:               record.EvidenceCount,
		CheckedEventSeq:             checkedEvent,
		CheckedPointOrigin:          checkedOrigin,
		CheckedPointSlot:            checkedSlot,
		CheckedPointHash:            checkedHash,
		CheckedPointBlockNumber:     checkedNumber,
		CheckedPointIsByronEBB:      checkedEBB,
		LastAgreedEventSeq:          agreedEvent,
		LastAgreedPointOrigin:       agreedOrigin,
		LastAgreedPointSlot:         agreedSlot,
		LastAgreedPointHash:         agreedHash,
		LastAgreedPointBlockNumber:  agreedNumber,
		LastAgreedPointIsByronEBB:   agreedEBB,
		LastAgreedAt:                record.LastAgreedAt,
		LastAgreedCheckedEventSeq:   lastEvidenceCheckedEvent,
		LastAgreedCheckedOrigin:     lastEvidenceCheckedOrigin,
		LastAgreedCheckedSlot:       lastEvidenceCheckedSlot,
		LastAgreedCheckedHash:       lastEvidenceCheckedHash,
		LastAgreedCheckedNumber:     lastEvidenceCheckedNumber,
		LastAgreedCheckedIsByronEBB: lastEvidenceCheckedEBB,
		ServableFloorEventSeq:       record.ServableFloor.EventSeq,
		ServableFloorOrigin:         floorOrigin,
		ServableFloorSlot:           floorSlot,
		ServableFloorHash:           floorHash,
		ServableFloorBlockNumber:    floorNumber,
		ServableFloorIsByronEBB:     floorEBB,
		ServableFloorPermanent:      record.ServableFloorPermanent,
		PhysicalEventSeq:            record.Physical.EventSeq,
		PhysicalTipOrigin:           physicalOrigin,
		PhysicalTipSlot:             physicalSlot,
		PhysicalTipHash:             physicalHash,
		PhysicalTipBlockNumber:      physicalNumber,
		PhysicalTipIsByronEBB:       physicalEBB,
		EffectiveEventSeq:           record.Effective.EventSeq,
		EffectiveTipOrigin:          effectiveOrigin,
		EffectiveTipSlot:            effectiveSlot,
		EffectiveTipHash:            effectiveHash,
		EffectiveTipBlockNumber:     effectiveNumber,
		EffectiveTipIsByronEBB:      effectiveEBB,
		Servable:                    record.Servable,
		VisibilityGeneration:        record.VisibilityGeneration,
		PendingRollbackState:        "none",
		WriterID:                    uuidPointer(record.WriterID),
		WriterBuild:                 record.WriterBuild,
		SourceBuild:                 record.SourceBuild,
		CreatedAt:                   record.CreatedAt,
		UpdatedAt:                   record.UpdatedAt,
	}
	if record.PreviousRowDigest != nil {
		value := string(record.PreviousRowDigest[:])
		raw.PreviousRowDigest = &value
	}
	if record.EvidenceDigest != nil {
		value := string(record.EvidenceDigest[:])
		raw.EvidenceDigest = &value
	}
	if record.LastAgreedEvidence != nil {
		reference := record.LastAgreedEvidence
		raw.LastAgreedCheckID = uuidPointer(&reference.CheckID)
		raw.LastAgreedAgreementGroup = uuidPointer(&reference.Group)
		raw.LastAgreedCheckAttempt = reference.Attempt
		raw.LastAgreedRequired = reference.Required
		raw.LastAgreedConfirmed = reference.Confirmed
		raw.LastAgreedEvidenceCount = reference.Count
		value := string(reference.Digest[:])
		raw.LastAgreedEvidenceDigest = &value
	}
	if record.PendingEvidenceWrite != nil {
		pending := record.PendingEvidenceWrite
		raw.PendingEvidenceID = uuidPointer(&pending.Observation.ID)
		value := string(pending.Digest[:])
		raw.PendingEvidenceDigest = &value
		raw.PendingEvidencePayload = pending.Payload
		raw.PendingEvidenceWriter = uuidPointer(&pending.WriterID)
		reservedAt := pending.ReservedAt
		raw.PendingEvidenceAt = &reservedAt
	}
	if record.PendingRollback != nil {
		pending := record.PendingRollback
		toOrigin, toSlot, toHash, toNumber, toEBB := pointDBValues(pending.To)
		oldOrigin, oldSlot, oldHash, oldNumber, oldEBB :=
			pointDBValues(pending.OldPhysical.Point)
		pendingEvent := pending.EventSeq
		oldEvent := pending.OldPhysical.EventSeq
		raw.PendingRollbackState = pending.State
		raw.PendingRollbackID = uuidPointer(&pending.ID)
		raw.PendingRollbackEventSeq = &pendingEvent
		raw.PendingRollbackToOrigin = &toOrigin
		raw.PendingRollbackToSlot = toSlot
		raw.PendingRollbackToHash = toHash
		raw.PendingRollbackToBlockNumber = toNumber
		raw.PendingRollbackToIsByronEBB = &toEBB
		raw.PendingRollbackOldPhysicalEventSeq = &oldEvent
		raw.PendingRollbackOldPhysicalOrigin = &oldOrigin
		raw.PendingRollbackOldPhysicalSlot = oldSlot
		raw.PendingRollbackOldPhysicalHash = oldHash
		raw.PendingRollbackOldPhysicalBlockNumber = oldNumber
		raw.PendingRollbackOldPhysicalIsByronEBB = &oldEBB
		depth := pending.Depth
		required := pending.Required
		raw.PendingRollbackDepth = &depth
		raw.PendingRollbackReason = pending.Reason
		raw.PendingRollbackObservedPeers = append([]string(nil), pending.Peers...)
		raw.PendingRollbackObservedOperators = append([]string(nil), pending.Operators...)
		raw.PendingRollbackRequired = &required
		raw.PendingRollbackCheckID = uuidPointer(&pending.CheckID)
		raw.PendingRollbackAgreementGroup = uuidPointer(&pending.Group)
		checkAttempt := pending.CheckAttempt
		checkedEventSeq := pending.CheckedEventSeq
		raw.PendingRollbackCheckAttempt = &checkAttempt
		raw.PendingRollbackCheckedEventSeq = &checkedEventSeq
		evidenceCount := pending.EvidenceCount
		raw.PendingRollbackEvidenceCount = &evidenceCount
		evidenceDigest := string(pending.EvidenceDigest[:])
		raw.PendingRollbackEvidenceDigest = &evidenceDigest
		raw.PendingRollbackWriterID = uuidPointer(&pending.WriterID)
		started := pending.StartedAt
		raw.PendingRollbackStartedAt = &started
	}
	return raw, nil
}

func (raw manifestDBRow) manifestRecord() (manifestRecord, error) {
	previousDigest, err := hashPointer(raw.PreviousRowDigest)
	if err != nil {
		return manifestRecord{}, fmt.Errorf("decode previous manifest digest: %w", err)
	}
	rowDigest, err := fixedHash(raw.RowDigest)
	if err != nil {
		return manifestRecord{}, fmt.Errorf("decode manifest row digest: %w", err)
	}
	evidenceDigest, err := hashPointer(raw.EvidenceDigest)
	if err != nil {
		return manifestRecord{}, fmt.Errorf("decode manifest evidence digest: %w", err)
	}
	contractHash, err := fixedHash(raw.SchemaContractHash)
	if err != nil {
		return manifestRecord{}, fmt.Errorf("decode schema contract hash: %w", err)
	}
	byronID, err := fixedHash(raw.ByronGenesisID)
	if err != nil {
		return manifestRecord{}, err
	}
	byronJSON, err := fixedHash(raw.ByronGenesisJSONHash)
	if err != nil {
		return manifestRecord{}, err
	}
	shelleyID, err := fixedHash(raw.ShelleyGenesisID)
	if err != nil {
		return manifestRecord{}, err
	}
	shelleyJSON, err := fixedHash(raw.ShelleyGenesisJSONHash)
	if err != nil {
		return manifestRecord{}, err
	}
	startOrigin := raw.StartKind == "origin"
	if !startOrigin && raw.StartKind != "intersection" {
		return manifestRecord{}, fmt.Errorf("unknown manifest start kind %q", raw.StartKind)
	}
	start, err := pointFromDB(
		startOrigin,
		raw.StartSlot,
		raw.StartHash,
		raw.StartBlockNumber,
		raw.StartIsByronEBB,
	)
	if err != nil {
		return manifestRecord{}, fmt.Errorf("decode manifest start: %w", err)
	}
	checked, err := optionalHeadFromDB(
		raw.CheckedEventSeq,
		raw.CheckedPointOrigin,
		raw.CheckedPointSlot,
		raw.CheckedPointHash,
		raw.CheckedPointBlockNumber,
		raw.CheckedPointIsByronEBB,
	)
	if err != nil {
		return manifestRecord{}, fmt.Errorf("decode checked manifest point: %w", err)
	}
	lastAgreed, err := optionalHeadFromDB(
		raw.LastAgreedEventSeq,
		raw.LastAgreedPointOrigin,
		raw.LastAgreedPointSlot,
		raw.LastAgreedPointHash,
		raw.LastAgreedPointBlockNumber,
		raw.LastAgreedPointIsByronEBB,
	)
	if err != nil {
		return manifestRecord{}, fmt.Errorf("decode last-agreed manifest point: %w", err)
	}
	lastAgreedChecked, err := optionalHeadFromDB(
		raw.LastAgreedCheckedEventSeq,
		raw.LastAgreedCheckedOrigin,
		raw.LastAgreedCheckedSlot,
		raw.LastAgreedCheckedHash,
		raw.LastAgreedCheckedNumber,
		raw.LastAgreedCheckedIsByronEBB,
	)
	if err != nil {
		return manifestRecord{}, fmt.Errorf(
			"decode last-agreed evidence checked point: %w",
			err,
		)
	}
	lastAgreedEvidenceDigest, err := hashPointer(raw.LastAgreedEvidenceDigest)
	if err != nil {
		return manifestRecord{}, fmt.Errorf(
			"decode last-agreed evidence digest: %w",
			err,
		)
	}
	floorPoint, err := pointFromDB(
		raw.ServableFloorOrigin,
		raw.ServableFloorSlot,
		raw.ServableFloorHash,
		raw.ServableFloorBlockNumber,
		raw.ServableFloorIsByronEBB,
	)
	if err != nil {
		return manifestRecord{}, fmt.Errorf("decode servable floor: %w", err)
	}
	physicalPoint, err := pointFromDB(
		raw.PhysicalTipOrigin,
		raw.PhysicalTipSlot,
		raw.PhysicalTipHash,
		raw.PhysicalTipBlockNumber,
		raw.PhysicalTipIsByronEBB,
	)
	if err != nil {
		return manifestRecord{}, fmt.Errorf("decode physical manifest head: %w", err)
	}
	effectivePoint, err := pointFromDB(
		raw.EffectiveTipOrigin,
		raw.EffectiveTipSlot,
		raw.EffectiveTipHash,
		raw.EffectiveTipBlockNumber,
		raw.EffectiveTipIsByronEBB,
	)
	if err != nil {
		return manifestRecord{}, fmt.Errorf("decode effective manifest head: %w", err)
	}

	record := manifestRecord{
		ManifestKey:            raw.ManifestKey,
		Revision:               raw.Revision,
		TransitionID:           uuid16(raw.TransitionID),
		TransitionKind:         raw.TransitionKind,
		PreviousRowDigest:      previousDigest,
		RowDigest:              rowDigest,
		DatasetID:              uuid16(raw.DatasetID),
		SchemaContractHash:     contractHash,
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
		CheckID:                uuid16Pointer(raw.CheckID),
		AgreementGroup:         uuid16Pointer(raw.AgreementGroup),
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
		ServableFloor:          manifestHead{EventSeq: raw.ServableFloorEventSeq, Point: floorPoint},
		ServableFloorPermanent: raw.ServableFloorPermanent,
		Physical:               manifestHead{EventSeq: raw.PhysicalEventSeq, Point: physicalPoint},
		Effective:              manifestHead{EventSeq: raw.EffectiveEventSeq, Point: effectivePoint},
		Servable:               raw.Servable,
		VisibilityGeneration:   raw.VisibilityGeneration,
		WriterID:               uuid16Pointer(raw.WriterID),
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
			lastAgreedChecked != nil ||
			raw.LastAgreedEvidenceCount != 0 ||
			lastAgreedEvidenceDigest != nil {
			return manifestRecord{}, errors.New(
				"absent last-agreed evidence reference carries fields",
			)
		}
	} else {
		if raw.LastAgreedAgreementGroup == nil ||
			lastAgreedChecked == nil ||
			lastAgreedEvidenceDigest == nil {
			return manifestRecord{}, errors.New(
				"last-agreed evidence reference is incomplete",
			)
		}
		record.LastAgreedEvidence = &manifestEvidenceReference{
			CheckID:   uuid16(*raw.LastAgreedCheckID),
			Group:     uuid16(*raw.LastAgreedAgreementGroup),
			Attempt:   raw.LastAgreedCheckAttempt,
			Required:  raw.LastAgreedRequired,
			Confirmed: raw.LastAgreedConfirmed,
			Checked:   *lastAgreedChecked,
			Count:     raw.LastAgreedEvidenceCount,
			Digest:    *lastAgreedEvidenceDigest,
		}
	}
	if raw.PendingEvidenceID == nil {
		if raw.PendingEvidenceDigest != nil ||
			raw.PendingEvidencePayload != "" ||
			raw.PendingEvidenceWriter != nil ||
			raw.PendingEvidenceAt != nil {
			return manifestRecord{}, errors.New(
				"manifest absent pending evidence write carries payload fields",
			)
		}
	} else {
		if raw.PendingEvidenceDigest == nil ||
			raw.PendingEvidencePayload == "" ||
			raw.PendingEvidenceWriter == nil ||
			raw.PendingEvidenceAt == nil {
			return manifestRecord{}, errors.New(
				"manifest pending evidence write is incomplete",
			)
		}
		digest, err := fixedHash(*raw.PendingEvidenceDigest)
		if err != nil {
			return manifestRecord{}, fmt.Errorf(
				"decode pending evidence digest: %w",
				err,
			)
		}
		observation, err := decodePendingEvidencePayload(
			raw.PendingEvidencePayload,
		)
		if err != nil {
			return manifestRecord{}, err
		}
		if observation.ID != uuid16(*raw.PendingEvidenceID) {
			return manifestRecord{}, errors.New(
				"pending evidence payload ID differs from manifest reservation",
			)
		}
		if err := model.VerifyPeerObservationIdentity(
			observation,
			digest[:],
		); err != nil {
			return manifestRecord{}, fmt.Errorf(
				"verify pending evidence payload: %w",
				err,
			)
		}
		record.PendingEvidenceWrite = &manifestPendingEvidenceWrite{
			Observation: observation,
			Digest:      digest,
			Payload:     raw.PendingEvidencePayload,
			WriterID:    uuid16(*raw.PendingEvidenceWriter),
			ReservedAt:  *raw.PendingEvidenceAt,
		}
	}
	if raw.PendingRollbackState == "none" {
		if raw.PendingRollbackID != nil ||
			raw.PendingRollbackEventSeq != nil ||
			raw.PendingRollbackStartedAt != nil {
			return manifestRecord{}, errors.New("manifest none pending rollback carries identity")
		}
	} else {
		if raw.PendingRollbackID == nil ||
			raw.PendingRollbackEventSeq == nil ||
			raw.PendingRollbackOldPhysicalEventSeq == nil ||
			raw.PendingRollbackDepth == nil ||
			raw.PendingRollbackRequired == nil ||
			raw.PendingRollbackCheckID == nil ||
			raw.PendingRollbackAgreementGroup == nil ||
			raw.PendingRollbackCheckAttempt == nil ||
			raw.PendingRollbackCheckedEventSeq == nil ||
			raw.PendingRollbackEvidenceCount == nil ||
			raw.PendingRollbackEvidenceDigest == nil ||
			raw.PendingRollbackWriterID == nil ||
			raw.PendingRollbackStartedAt == nil {
			return manifestRecord{}, errors.New("manifest pending rollback is incomplete")
		}
		to, err := optionalPointFromDB(
			raw.PendingRollbackToOrigin,
			raw.PendingRollbackToSlot,
			raw.PendingRollbackToHash,
			raw.PendingRollbackToBlockNumber,
			raw.PendingRollbackToIsByronEBB,
		)
		if err != nil {
			return manifestRecord{}, fmt.Errorf("decode pending rollback target: %w", err)
		}
		old, err := optionalPointFromDB(
			raw.PendingRollbackOldPhysicalOrigin,
			raw.PendingRollbackOldPhysicalSlot,
			raw.PendingRollbackOldPhysicalHash,
			raw.PendingRollbackOldPhysicalBlockNumber,
			raw.PendingRollbackOldPhysicalIsByronEBB,
		)
		if err != nil {
			return manifestRecord{}, fmt.Errorf("decode pending rollback old head: %w", err)
		}
		evidenceDigest, err := fixedHash(*raw.PendingRollbackEvidenceDigest)
		if err != nil {
			return manifestRecord{}, fmt.Errorf(
				"decode pending rollback evidence digest: %w",
				err,
			)
		}
		record.PendingRollback = &manifestPendingRollback{
			State:           raw.PendingRollbackState,
			ID:              uuid16(*raw.PendingRollbackID),
			EventSeq:        *raw.PendingRollbackEventSeq,
			To:              to,
			OldPhysical:     manifestHead{EventSeq: *raw.PendingRollbackOldPhysicalEventSeq, Point: old},
			Depth:           *raw.PendingRollbackDepth,
			Reason:          raw.PendingRollbackReason,
			Peers:           append([]string(nil), raw.PendingRollbackObservedPeers...),
			Operators:       append([]string(nil), raw.PendingRollbackObservedOperators...),
			Required:        *raw.PendingRollbackRequired,
			CheckID:         uuid16(*raw.PendingRollbackCheckID),
			Group:           uuid16(*raw.PendingRollbackAgreementGroup),
			CheckAttempt:    *raw.PendingRollbackCheckAttempt,
			CheckedEventSeq: *raw.PendingRollbackCheckedEventSeq,
			EvidenceCount:   *raw.PendingRollbackEvidenceCount,
			EvidenceDigest:  evidenceDigest,
			WriterID:        uuid16(*raw.PendingRollbackWriterID),
			StartedAt:       *raw.PendingRollbackStartedAt,
		}
	}
	normalizeManifestTimes(&record)
	return record, nil
}

func optionalPointFromDB(
	origin *bool,
	slot *uint64,
	hash *string,
	number *uint64,
	isByronEBB *bool,
) (publication.Point, error) {
	if origin == nil || isByronEBB == nil {
		return publication.Point{}, errors.New("optional point origin/type is absent")
	}
	return pointFromDB(*origin, slot, hash, number, *isByronEBB)
}
