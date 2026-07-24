package clickhouse

import (
	"bytes"
	"errors"
	"fmt"
	"strings"

	"github.com/clicksync-project/clickout/internal/model"
)

const (
	manifestCheckpoint    = uint64(512)
	manifestMaximumSuffix = uint64(767)
)

func authorityPointBefore(left, right authorityPoint) bool {
	if left.Origin {
		return !right.Origin
	}
	if right.Origin {
		return false
	}
	if left.BlockNumber != right.BlockNumber {
		return left.BlockNumber < right.BlockNumber
	}
	if left.Slot != right.Slot {
		return left.Slot < right.Slot
	}
	return bytes.Compare(left.Hash[:], right.Hash[:]) < 0
}

func validateAuthorityPoint(name string, point authorityPoint) error {
	if point.Origin {
		if point.Hash != (authorityHash{}) ||
			point.Slot != 0 ||
			point.BlockNumber != 0 ||
			point.IsByronEBB {
			return fmt.Errorf("%s Origin carries non-Origin fields", name)
		}
		return nil
	}
	if point.Hash == (authorityHash{}) {
		return fmt.Errorf("%s has a zero hash", name)
	}
	return nil
}

func authorityObservationPoint(row authorityObservation) authorityPoint {
	if row.CheckedPointOrigin {
		if row.CheckedPointSlot != nil ||
			row.CheckedPointHash != nil ||
			row.CheckedBlockNumber != nil ||
			row.CheckedPointIsByronEBB {
			return authorityPoint{}
		}
		return authorityPoint{Origin: true}
	}
	if row.CheckedPointSlot == nil ||
		row.CheckedPointHash == nil ||
		row.CheckedBlockNumber == nil {
		return authorityPoint{}
	}
	return authorityPoint{
		Slot:        *row.CheckedPointSlot,
		Hash:        *row.CheckedPointHash,
		BlockNumber: *row.CheckedBlockNumber,
		IsByronEBB:  row.CheckedPointIsByronEBB,
	}
}

func validateAuthorityObservationProvenance(
	row authorityObservation,
	checked authorityPoint,
) error {
	if row.Kind == "source_change" {
		return errors.New("pending evidence cannot reserve a diagnostic source-change row")
	}
	switch row.Kind {
	case "checkpoint", "disagreement":
		if row.Kind == "disagreement" && row.Result == "agreed" {
			return errors.New("disagreement evidence claims agreement")
		}
		if row.ProofMethod != "chain_sync_singleton" &&
			row.ProofMethod != "boundary_singleton_block_fetch" &&
			(row.Kind != "checkpoint" || row.ProofMethod != "follow_block_fetch") {
			return errors.New("checkpoint/disagreement proof method is invalid")
		}
	case "rollback":
		if row.ProofMethod != "follow_block_fetch" &&
			row.ProofMethod != "paired_chain_sync_singleton" {
			return errors.New("rollback proof method is invalid")
		}
	default:
		return fmt.Errorf("unknown evidence kind %q", row.Kind)
	}
	if row.NetworkMagic != 764824073 ||
		strings.TrimSpace(row.PeerHost) == "" ||
		strings.TrimSpace(row.Operator) == "" ||
		authorityObservationPoint(row) != checked {
		return errors.New("evidence network/operator/exact point provenance is invalid")
	}
	if checked.Origin {
		if row.CheckpointSlot != nil ||
			row.CheckpointHash != nil ||
			row.CheckpointBlockNumber != nil ||
			row.CheckpointIsByronEBB != nil {
			return errors.New("Origin evidence checkpoint is not null-shaped")
		}
	} else if row.CheckpointSlot == nil ||
		row.CheckpointHash == nil ||
		row.CheckpointBlockNumber == nil ||
		row.CheckpointIsByronEBB == nil ||
		*row.CheckpointSlot != checked.Slot ||
		*row.CheckpointHash != checked.Hash ||
		*row.CheckpointBlockNumber != checked.BlockNumber ||
		*row.CheckpointIsByronEBB != checked.IsByronEBB {
		return errors.New("evidence checkpoint differs from checked point")
	}
	if row.Result == "agreed" &&
		(strings.TrimSpace(row.PeerAddress) == "" ||
			row.N2NVersion == 0 ||
			row.TipHash == (authorityHash{}) ||
			!row.PointVerified ||
			row.TipBlockNumber < checked.BlockNumber ||
			row.TipSlot < checked.Slot ||
			row.Kind == "disagreement") {
		return errors.New("agreed evidence lacks session/tip provenance")
	}
	switch row.ProofMethod {
	case "chain_sync_singleton", "paired_chain_sync_singleton":
		if row.Result == "agreed" {
			if !row.PointVerified ||
				row.SelectedBodySource ||
				row.BodyHashVerified ||
				row.ParentVerified {
				return errors.New("singleton agreement proof flags are invalid")
			}
		} else if row.SelectedBodySource ||
			row.BodyHashVerified ||
			row.PointVerified ||
			row.ParentVerified {
			return errors.New("failed singleton proof carries flags")
		}
	case "follow_block_fetch":
		if row.Result != "agreed" ||
			!row.SelectedBodySource ||
			!row.BodyHashVerified ||
			!row.PointVerified ||
			row.ParentVerified {
			return errors.New("follow BlockFetch proof flags are invalid")
		}
	case "boundary_singleton_block_fetch":
		if row.Result != "agreed" ||
			!row.BodyHashVerified ||
			!row.PointVerified ||
			row.ParentVerified {
			return errors.New("boundary BlockFetch proof flags are invalid")
		}
	}
	return nil
}

func currentAuthorityEvidenceMatches(
	row authorityRecord,
	reference authorityEvidenceReference,
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

func verifyAuthorityRecord(row authorityRecord) error {
	if row.ManifestKey != 1 || row.Revision == 0 {
		return errors.New("invalid manifest singleton key/revision")
	}
	if (row.Revision == 1 && row.PreviousRowDigest != nil) ||
		(row.Revision > 1 && row.PreviousRowDigest == nil) {
		return errors.New("manifest predecessor digest nullability differs from revision")
	}
	if row.SchemaContractHash != expectedSchemaContract() {
		return errors.New("manifest schema contract differs from Clickout contract")
	}
	if row.DatasetID == ([16]byte{}) ||
		row.NetworkMagic == 0 ||
		strings.TrimSpace(row.NetworkName) == "" ||
		row.ByronGenesisID == (authorityHash{}) ||
		row.ByronGenesisJSONHash == (authorityHash{}) ||
		row.ShelleyGenesisID == (authorityHash{}) ||
		row.ShelleyGenesisJSONHash == (authorityHash{}) {
		return errors.New("manifest immutable identity is incomplete")
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
		return fmt.Errorf("unknown manifest transition %q", row.TransitionKind)
	}
	if row.TrustMode != model.TrustPeerObserved ||
		row.CheckpointInterval != manifestCheckpoint ||
		row.PrimarySuffix > manifestMaximumSuffix ||
		uint32(row.CorroborationConfirmed) > row.EvidenceCount {
		return errors.New("manifest trust/cadence state is invalid")
	}
	if row.CompleteHistory != (row.Start.Origin && row.GenesisSeeded) {
		return errors.New("manifest completeness differs from genesis state")
	}
	switch row.TrustBasis {
	case "official_genesis", "sampled_peer", "partial_boundary", "primary_only":
	default:
		return fmt.Errorf("unknown trust basis %q", row.TrustBasis)
	}
	if row.TrustBasis == "primary_only" &&
		(row.PrimarySuffix == 0 ||
			row.LastAgreed == nil ||
			(row.LastAgreedEvidence == nil && !row.ServableFloorPermanent)) {
		return errors.New("primary-only state lacks authority anchor")
	}
	switch row.TrustStatus {
	case "agreed":
		if row.Disagreement || !row.Servable || row.Effective != row.Physical {
			return errors.New("agreed manifest does not serve its physical head")
		}
		if row.CheckID != nil &&
			row.CorroborationConfirmed < row.CorroborationRequired {
			return errors.New("agreed manifest has insufficient corroboration")
		}
		if row.TrustBasis == "official_genesis" {
			if row.CheckID != nil || row.CorroborationRequired != 0 ||
				row.CorroborationConfirmed != 0 {
				return errors.New("official genesis carries peer-check state")
			}
		} else if row.TrustBasis != "primary_only" &&
			(row.TrustBasis != "sampled_peer" ||
				row.CheckID == nil ||
				row.CorroborationRequired < 2 ||
				row.CorroborationConfirmed < row.CorroborationRequired ||
				row.Checked == nil ||
				row.LastAgreed == nil ||
				row.LastAgreedEvidence == nil ||
				!currentAuthorityEvidenceMatches(row, *row.LastAgreedEvidence)) {
			return errors.New("agreed peer state lacks exact evidence authority")
		}
	case "unavailable":
		if row.Disagreement {
			return errors.New("unavailable manifest carries disagreement")
		}
		if row.Servable {
			if row.Effective != row.Physical {
				return errors.New("servable unavailable manifest is not physical")
			}
		} else if row.LastAgreed != nil ||
			row.Effective != row.ServableFloor ||
			row.ServableFloor.EventSeq != 0 {
			return errors.New("unservable bootstrap clamp is invalid")
		}
	case "checking", "disputed":
		if row.CheckID == nil {
			return errors.New("checking/disputed manifest lacks check ID")
		}
		clamp := row.ServableFloor
		if row.LastAgreed != nil {
			clamp = *row.LastAgreed
		}
		if authorityPointBefore(row.Physical.Point, clamp.Point) {
			clamp = row.Physical
		}
		if row.Effective != clamp ||
			row.Servable != (row.LastAgreed != nil || row.ServableFloorPermanent) ||
			(row.TrustStatus == "checking" && row.Disagreement) ||
			(row.TrustStatus == "disputed" && !row.Disagreement) {
			return errors.New("checking/disputed safety clamp is invalid")
		}
	default:
		return fmt.Errorf("unknown trust status %q", row.TrustStatus)
	}
	if row.Effective.EventSeq > row.Physical.EventSeq ||
		authorityPointBefore(row.Effective.Point, row.ServableFloor.Point) {
		return errors.New("effective head lies outside physical/floor bounds")
	}
	if row.ServableFloorPermanent &&
		(!row.Start.Origin || !row.GenesisSeeded || !row.ServableFloor.Point.Origin) {
		return errors.New("permanent floor is not verified official genesis")
	}
	if !row.Start.Origin &&
		(row.ServableFloorPermanent ||
			row.ServableFloor.EventSeq != 0 ||
			row.ServableFloor.Point != row.Start) {
		return errors.New("partial-history floor changed")
	}
	if row.Start.Origin && !row.GenesisSeeded && row.Servable {
		return errors.New("Origin dataset is servable before genesis completion")
	}
	if row.Start.Origin && row.GenesisSeeded &&
		(!row.ServableFloorPermanent || !row.ServableFloor.Point.Origin) {
		return errors.New("seeded Origin dataset lacks permanent floor")
	}
	if (row.CheckID == nil) != (row.AgreementGroup == nil) {
		return errors.New("check ID/group nullability differs")
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
			return errors.New("manifest without check identity carries check state")
		}
	} else {
		if *row.CheckID == ([16]byte{}) ||
			*row.AgreementGroup == ([16]byte{}) ||
			row.Checked == nil ||
			row.CheckAttempt == 0 ||
			row.CorroborationRequired < 2 ||
			row.CheckStartedAt == nil ||
			(row.EvidenceState != "open" && row.EvidenceState != "frozen") ||
			row.EvidenceDigest == nil {
			return errors.New("manifest check/evidence state is incomplete")
		}
		if row.Checked.EventSeq > row.Physical.EventSeq {
			return errors.New("checked event exceeds physical event")
		}
		if err := validateAuthorityPoint("checked point", row.Checked.Point); err != nil {
			return err
		}
		if row.EvidenceState == "frozen" && row.PendingEvidenceWrite != nil {
			return errors.New("frozen evidence carries pending write")
		}
		if row.TrustStatus == "checking" {
			if row.CheckCompletedAt != nil {
				return errors.New("checking state is already completed")
			}
		} else if row.CheckCompletedAt == nil || row.EvidenceState != "frozen" {
			return errors.New("completed trust state lacks frozen completion")
		}
		if pending := row.PendingEvidenceWrite; pending != nil {
			observation := pending.Observation
			if row.EvidenceState != "open" ||
				pending.WriterID == ([16]byte{}) ||
				observation.CheckID != *row.CheckID ||
				observation.AgreementGroup != *row.AgreementGroup ||
				observation.CheckAttempt != row.CheckAttempt ||
				observation.EvidenceOrdinal != row.EvidenceCount+1 ||
				observation.CorroborationRequired != row.CorroborationRequired ||
				observation.CheckedEventSeq != row.Checked.EventSeq ||
				authorityObservationPoint(observation) != row.Checked.Point {
				return errors.New("pending evidence reservation differs from exact open check")
			}
			if err := verifyAuthorityObservation(observation, pending.Digest); err != nil {
				return err
			}
			if err := validateAuthorityObservationProvenance(
				observation,
				row.Checked.Point,
			); err != nil {
				return err
			}
		}
	}
	if row.LastAgreed == nil {
		if row.LastAgreedAt != nil || row.LastAgreedEvidence != nil {
			return errors.New("absent last-agreed carries authority fields")
		}
	} else if row.LastAgreedAt == nil {
		return errors.New("last-agreed has no timestamp")
	} else if row.LastAgreedEvidence == nil {
		if row.LastAgreed.EventSeq > row.Physical.EventSeq {
			return errors.New("last-agreed event exceeds physical")
		}
		if err := validateAuthorityPoint("last-agreed point", row.LastAgreed.Point); err != nil {
			return err
		}
		if !row.ServableFloorPermanent ||
			!row.ServableFloor.Point.Origin ||
			*row.LastAgreed != row.ServableFloor {
			return errors.New("peer-derived last-agreed lacks evidence")
		}
	} else {
		reference := row.LastAgreedEvidence
		if row.LastAgreed.EventSeq > row.Physical.EventSeq {
			return errors.New("last-agreed event exceeds physical")
		}
		if err := validateAuthorityPoint("last-agreed point", row.LastAgreed.Point); err != nil {
			return err
		}
		if reference.CheckID == ([16]byte{}) ||
			reference.Group == ([16]byte{}) ||
			reference.Attempt == 0 ||
			reference.Required < 2 ||
			reference.Confirmed < reference.Required ||
			uint32(reference.Confirmed) > reference.Count ||
			reference.Digest == (authorityHash{}) ||
			reference.Checked.Point != row.LastAgreed.Point ||
			reference.Checked.EventSeq > row.LastAgreed.EventSeq {
			return errors.New("last-agreed evidence reference is invalid")
		}
		if row.TrustStatus == "agreed" &&
			row.TrustBasis != "official_genesis" &&
			!currentAuthorityEvidenceMatches(row, *reference) {
			return errors.New("current evidence differs from last-agreed authority")
		}
		if reference.Checked.EventSeq > row.Physical.EventSeq {
			return errors.New("last-agreed evidence checked event exceeds physical")
		}
		if err := validateAuthorityPoint(
			"last-agreed checked point",
			reference.Checked.Point,
		); err != nil {
			return err
		}
	}
	for name, point := range map[string]authorityPoint{
		"start":          row.Start,
		"servable floor": row.ServableFloor.Point,
		"physical":       row.Physical.Point,
		"effective":      row.Effective.Point,
	} {
		if err := validateAuthorityPoint(name, point); err != nil {
			return err
		}
	}
	if row.PendingRollback != nil {
		pending := row.PendingRollback
		if pending.ID == ([16]byte{}) ||
			pending.CheckID == ([16]byte{}) ||
			pending.Group == ([16]byte{}) ||
			pending.WriterID == ([16]byte{}) ||
			strings.TrimSpace(pending.Reason) == "" ||
			pending.CheckAttempt == 0 ||
			pending.EventSeq <= row.Physical.EventSeq ||
			pending.OldPhysical != row.Physical ||
			pending.To != pending.CheckedPoint(row) ||
			row.Checked == nil ||
			pending.CheckedEventSeq != row.Checked.EventSeq ||
			pending.Required < 2 ||
			pending.EvidenceCount < uint32(pending.Required) ||
			pending.EvidenceDigest == (authorityHash{}) ||
			row.EvidenceDigest == nil ||
			pending.EvidenceCount != row.EvidenceCount ||
			pending.EvidenceDigest != *row.EvidenceDigest ||
			row.EvidenceState != "frozen" ||
			row.PendingEvidenceWrite != nil ||
			len(pending.Peers) != len(pending.Operators) ||
			len(pending.Operators) < int(pending.Required) {
			return errors.New("pending rollback authority is incomplete")
		}
		if err := validateAuthorityPoint("pending rollback target", pending.To); err != nil {
			return err
		}
		if err := validateAuthorityPoint(
			"pending rollback old physical",
			pending.OldPhysical.Point,
		); err != nil {
			return err
		}
	}
	expected := row
	if err := finalizeAuthorityRecord(&expected); err != nil {
		return err
	}
	if expected.TransitionID != row.TransitionID ||
		expected.RowDigest != row.RowDigest {
		return errors.New("manifest transition/digest differs from canonical content")
	}
	return nil
}

func (pending authorityPendingRollback) CheckedPoint(row authorityRecord) authorityPoint {
	if row.Checked == nil {
		return authorityPoint{}
	}
	return row.Checked.Point
}
