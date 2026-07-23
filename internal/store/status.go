package store

import (
	"context"
	"encoding/hex"
	"errors"
	"time"

	"clicksync/internal/publication"
)

type DatasetStatus struct {
	Initialized            bool                     `json:"initialized"`
	ManifestRevision       uint64                   `json:"manifest_revision"`
	TransitionID           string                   `json:"transition_id"`
	TransitionKind         string                   `json:"transition_kind"`
	RowDigest              string                   `json:"row_digest"`
	DatasetID              string                   `json:"dataset_id"`
	SchemaContractHash     string                   `json:"schema_contract_hash"`
	NetworkName            string                   `json:"network_name"`
	NetworkMagic           uint32                   `json:"network_magic"`
	Start                  StatusPoint              `json:"start"`
	GenesisSeeded          bool                     `json:"genesis_seeded"`
	CompleteHistory        bool                     `json:"complete_history"`
	Physical               StatusHead               `json:"physical"`
	Effective              StatusHead               `json:"effective"`
	Servable               bool                     `json:"servable"`
	TrustStatus            string                   `json:"trust_status"`
	TrustBasis             string                   `json:"trust_basis"`
	TrustReason            string                   `json:"trust_reason"`
	CheckID                string                   `json:"check_id,omitempty"`
	AgreementGroup         string                   `json:"agreement_group,omitempty"`
	CheckAttempt           uint32                   `json:"check_attempt"`
	CheckStartedAt         *time.Time               `json:"check_started_at,omitempty"`
	CheckCompletedAt       *time.Time               `json:"check_completed_at,omitempty"`
	EvidenceState          string                   `json:"evidence_state"`
	EvidenceCount          uint32                   `json:"evidence_count"`
	EvidenceDigest         string                   `json:"evidence_digest,omitempty"`
	PendingEvidence        *PendingEvidenceStatus   `json:"pending_evidence,omitempty"`
	Checked                *StatusHead              `json:"checked,omitempty"`
	LastAgreed             *StatusHead              `json:"last_agreed,omitempty"`
	LastAgreedAt           *time.Time               `json:"last_agreed_at,omitempty"`
	LastAgreedEvidence     *EvidenceReferenceStatus `json:"last_agreed_evidence,omitempty"`
	CorroborationRequired  uint16                   `json:"corroboration_required"`
	CorroborationConfirmed uint16                   `json:"corroboration_confirmed"`
	CheckpointInterval     uint64                   `json:"checkpoint_interval"`
	PrimarySuffix          uint64                   `json:"primary_suffix"`
	LagEvents              uint64                   `json:"lag_events"`
	LagBlocks              uint64                   `json:"lag_blocks"`
	VisibilityGeneration   uint64                   `json:"visibility_generation"`
	PendingRollback        *PendingRollbackStatus   `json:"pending_rollback,omitempty"`
	WriterID               string                   `json:"writer_id,omitempty"`
	WriterBuild            string                   `json:"writer_build"`
	SourceBuild            string                   `json:"source_build"`
}

type PendingEvidenceStatus struct {
	State             string    `json:"state"`
	CheckID           string    `json:"check_id"`
	AgreementGroup    string    `json:"agreement_group"`
	CheckAttempt      uint32    `json:"check_attempt"`
	ObservationID     string    `json:"observation_id"`
	ObservationDigest string    `json:"observation_digest"`
	Ordinal           uint32    `json:"ordinal"`
	WriterID          string    `json:"writer_id"`
	ReservedAt        time.Time `json:"reserved_at"`
}

type EvidenceReferenceStatus struct {
	CheckID        string     `json:"check_id"`
	AgreementGroup string     `json:"agreement_group"`
	CheckAttempt   uint32     `json:"check_attempt"`
	Required       uint16     `json:"required"`
	Confirmed      uint16     `json:"confirmed"`
	Checked        StatusHead `json:"checked"`
	Count          uint32     `json:"count"`
	Digest         string     `json:"digest"`
}

type StatusHead struct {
	EventSeq uint64      `json:"event_seq"`
	Point    StatusPoint `json:"point"`
}

type StatusPoint struct {
	Origin      bool   `json:"origin"`
	Slot        uint64 `json:"slot,omitempty"`
	Hash        string `json:"hash,omitempty"`
	BlockNumber uint64 `json:"block_number,omitempty"`
	IsByronEBB  bool   `json:"is_byron_ebb,omitempty"`
}

type PendingRollbackStatus struct {
	State           string      `json:"state"`
	RollbackID      string      `json:"rollback_id"`
	EventSeq        uint64      `json:"event_seq"`
	To              StatusPoint `json:"to"`
	OldPhysical     StatusHead  `json:"old_physical"`
	Depth           uint32      `json:"depth"`
	CheckID         string      `json:"check_id"`
	AgreementGroup  string      `json:"agreement_group"`
	CheckAttempt    uint32      `json:"check_attempt"`
	CheckedEventSeq uint64      `json:"checked_event_seq"`
	Required        uint16      `json:"required"`
	EvidenceCount   uint32      `json:"evidence_count"`
	EvidenceDigest  string      `json:"evidence_digest"`
}

// DatasetStatus reads only the bounded, digest-verified manifest head. It
// intentionally succeeds for unservable/checking/disputed datasets so
// operators can diagnose the safety barrier without consulting raw global
// events or facts.
func (d *DB) DatasetStatus(ctx context.Context) (DatasetStatus, bool, error) {
	record, found, err := d.loadAuthoritativeManifest(ctx)
	if err != nil || !found {
		return DatasetStatus{}, found, err
	}
	status, err := datasetStatusFromManifest(record)
	return status, true, err
}

func datasetStatusFromManifest(record manifestRecord) (DatasetStatus, error) {
	if err := verifyManifestRecord(record); err != nil {
		return DatasetStatus{}, err
	}
	if record.Physical.EventSeq < record.Effective.EventSeq {
		return DatasetStatus{}, errors.New("manifest effective event exceeds physical event")
	}
	status := DatasetStatus{
		Initialized:            true,
		ManifestRevision:       record.Revision,
		TransitionID:           hex.EncodeToString(record.TransitionID[:]),
		TransitionKind:         record.TransitionKind,
		RowDigest:              hex.EncodeToString(record.RowDigest[:]),
		DatasetID:              hex.EncodeToString(record.DatasetID[:]),
		SchemaContractHash:     hex.EncodeToString(record.SchemaContractHash[:]),
		NetworkName:            record.NetworkName,
		NetworkMagic:           record.NetworkMagic,
		Start:                  statusPoint(record.Start),
		GenesisSeeded:          record.GenesisSeeded,
		CompleteHistory:        record.CompleteHistory,
		Physical:               statusHead(record.Physical),
		Effective:              statusHead(record.Effective),
		Servable:               record.Servable,
		TrustStatus:            record.TrustStatus,
		TrustBasis:             record.TrustBasis,
		TrustReason:            record.TrustReason,
		CheckAttempt:           record.CheckAttempt,
		CheckStartedAt:         cloneStatusTime(record.CheckStartedAt),
		CheckCompletedAt:       cloneStatusTime(record.CheckCompletedAt),
		EvidenceState:          record.EvidenceState,
		EvidenceCount:          record.EvidenceCount,
		LastAgreedAt:           cloneStatusTime(record.LastAgreedAt),
		CorroborationRequired:  record.CorroborationRequired,
		CorroborationConfirmed: record.CorroborationConfirmed,
		CheckpointInterval:     record.CheckpointInterval,
		PrimarySuffix:          record.PrimarySuffix,
		LagEvents:              record.Physical.EventSeq - record.Effective.EventSeq,
		VisibilityGeneration:   record.VisibilityGeneration,
		WriterBuild:            record.WriterBuild,
		SourceBuild:            record.SourceBuild,
	}
	if record.EvidenceDigest != nil {
		status.EvidenceDigest = hex.EncodeToString(record.EvidenceDigest[:])
	}
	if record.Physical.Point.BlockNumber >= record.Effective.Point.BlockNumber {
		status.LagBlocks =
			record.Physical.Point.BlockNumber - record.Effective.Point.BlockNumber
	}
	if record.CheckID != nil {
		status.CheckID = hex.EncodeToString(record.CheckID[:])
	}
	if record.AgreementGroup != nil {
		status.AgreementGroup = hex.EncodeToString(record.AgreementGroup[:])
	}
	if record.WriterID != nil {
		status.WriterID = hex.EncodeToString(record.WriterID[:])
	}
	if record.Checked != nil {
		head := statusHead(*record.Checked)
		status.Checked = &head
	}
	if record.LastAgreed != nil {
		head := statusHead(*record.LastAgreed)
		status.LastAgreed = &head
	}
	if reference := record.LastAgreedEvidence; reference != nil {
		status.LastAgreedEvidence = &EvidenceReferenceStatus{
			CheckID:        hex.EncodeToString(reference.CheckID[:]),
			AgreementGroup: hex.EncodeToString(reference.Group[:]),
			CheckAttempt:   reference.Attempt,
			Required:       reference.Required,
			Confirmed:      reference.Confirmed,
			Checked:        statusHead(reference.Checked),
			Count:          reference.Count,
			Digest:         hex.EncodeToString(reference.Digest[:]),
		}
	}
	if pending := record.PendingEvidenceWrite; pending != nil {
		status.PendingEvidence = &PendingEvidenceStatus{
			State:             "reserved",
			CheckID:           hex.EncodeToString(pending.Observation.CheckID[:]),
			AgreementGroup:    hex.EncodeToString(pending.Observation.AgreementGroup[:]),
			CheckAttempt:      pending.Observation.CheckAttempt,
			ObservationID:     hex.EncodeToString(pending.Observation.ID[:]),
			ObservationDigest: hex.EncodeToString(pending.Digest[:]),
			Ordinal:           pending.Observation.EvidenceOrdinal,
			WriterID:          hex.EncodeToString(pending.WriterID[:]),
			ReservedAt:        pending.ReservedAt,
		}
	}
	if pending := record.PendingRollback; pending != nil {
		status.PendingRollback = &PendingRollbackStatus{
			State:           pending.State,
			RollbackID:      hex.EncodeToString(pending.ID[:]),
			EventSeq:        pending.EventSeq,
			To:              statusPoint(pending.To),
			OldPhysical:     statusHead(pending.OldPhysical),
			Depth:           pending.Depth,
			CheckID:         hex.EncodeToString(pending.CheckID[:]),
			AgreementGroup:  hex.EncodeToString(pending.Group[:]),
			CheckAttempt:    pending.CheckAttempt,
			CheckedEventSeq: pending.CheckedEventSeq,
			Required:        pending.Required,
			EvidenceCount:   pending.EvidenceCount,
			EvidenceDigest:  hex.EncodeToString(pending.EvidenceDigest[:]),
		}
	}
	return status, nil
}

func cloneStatusTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	ret := value.UTC()
	return &ret
}

func statusHead(head manifestHead) StatusHead {
	return StatusHead{EventSeq: head.EventSeq, Point: statusPoint(head.Point)}
}

func statusPoint(point publication.Point) StatusPoint {
	status := StatusPoint{Origin: point.Origin}
	if point.Origin {
		return status
	}
	status.Slot = point.Slot
	status.Hash = hex.EncodeToString(point.Hash[:])
	status.BlockNumber = point.BlockNumber
	status.IsByronEBB = point.IsByronEBB
	return status
}
