package clickhouse

import (
	"context"
	"errors"

	"github.com/clicksync-project/clickout/internal/model"
)

func modelAuthorityPoint(value authorityPoint) model.Point {
	return model.Point{
		Origin:      value.Origin,
		Slot:        value.Slot,
		Hash:        model.Hash32(value.Hash),
		BlockNumber: value.BlockNumber,
		IsByronEBB:  value.IsByronEBB,
	}
}

func internalAuthorityPoint(value model.Point) authorityPoint {
	return authorityPoint{
		Origin:      value.Origin,
		Slot:        value.Slot,
		Hash:        authorityHash(value.Hash),
		BlockNumber: value.BlockNumber,
		IsByronEBB:  value.IsByronEBB,
	}
}

func modelAuthorityHead(value authorityHead) model.Head {
	return model.Head{
		EventSeq: value.EventSeq,
		Point:    modelAuthorityPoint(value.Point),
	}
}

func internalAuthorityHead(value model.Head) authorityHead {
	return authorityHead{
		EventSeq: value.EventSeq,
		Point:    internalAuthorityPoint(value.Point),
	}
}

func modelAuthorityIdentity(
	value authoritySnapshotIdentity,
) model.SnapshotIdentity {
	return model.SnapshotIdentity{
		DatasetID:              model.DatasetID(value.DatasetID),
		SchemaContractHash:     model.Hash32(value.SchemaContractHash),
		NetworkMagic:           value.NetworkMagic,
		NetworkName:            value.NetworkName,
		ByronGenesisID:         model.Hash32(value.ByronGenesisID),
		ByronGenesisJSONHash:   model.Hash32(value.ByronGenesisJSONHash),
		ShelleyGenesisID:       model.Hash32(value.ShelleyGenesisID),
		ShelleyGenesisJSONHash: model.Hash32(value.ShelleyGenesisJSONHash),
		Start:                  modelAuthorityPoint(value.Start),
		TrustMode:              value.TrustMode,
		CreatedAt:              value.CreatedAt,
		CompleteHistory:        value.CompleteHistory,
	}
}

func internalAuthorityIdentity(
	value model.SnapshotIdentity,
) authoritySnapshotIdentity {
	return authoritySnapshotIdentity{
		DatasetID:              [16]byte(value.DatasetID),
		SchemaContractHash:     authorityHash(value.SchemaContractHash),
		NetworkMagic:           value.NetworkMagic,
		NetworkName:            value.NetworkName,
		ByronGenesisID:         authorityHash(value.ByronGenesisID),
		ByronGenesisJSONHash:   authorityHash(value.ByronGenesisJSONHash),
		ShelleyGenesisID:       authorityHash(value.ShelleyGenesisID),
		ShelleyGenesisJSONHash: authorityHash(value.ShelleyGenesisJSONHash),
		Start:                  internalAuthorityPoint(value.Start),
		TrustMode:              value.TrustMode,
		CreatedAt:              value.CreatedAt,
		CompleteHistory:        value.CompleteHistory,
	}
}

func modelAuthoritySnapshot(
	lease authoritySnapshotLease,
) (model.Snapshot, error) {
	selector := model.SnapshotSelector{
		SelectedPublicationID: lease.SelectedPublicationID,
	}
	switch lease.Mode {
	case authoritySnapshotAtTip:
		selector.Mode = model.SnapshotAtTip
	case authoritySnapshotAtBlock:
		selector.Mode = model.SnapshotAtBlock
		blockHash := model.Hash32(lease.BlockHash)
		selector.RequestedBlockHash = &blockHash
		selectedPoint := modelAuthorityPoint(lease.SelectedPoint)
		selector.SelectedPoint = &selectedPoint
	default:
		return model.Snapshot{}, invalidAuthorityError(
			errors.New("snapshot lease mode cannot be exposed"),
		)
	}
	snapshot := model.Snapshot{
		Identity:             modelAuthorityIdentity(lease.Identity),
		VisibilityGeneration: lease.VisibilityGeneration,
		AuthorityEffective:   modelAuthorityHead(lease.AuthorityEffective),
		QueryHead:            modelAuthorityHead(lease.QueryHead),
		Cutoff: model.Cutoff{
			AdoptionEventSeq: lease.Cutoff.AdoptionEventSeq,
			PublicationID:    lease.Cutoff.PublicationID,
		},
		Selector: selector,
		Diagnostics: model.SnapshotDiagnostics{
			Physical:    modelAuthorityHead(lease.Diagnostics.Physical),
			TrustStatus: lease.Diagnostics.TrustStatus,
			TrustBasis:  lease.Diagnostics.TrustBasis,
			TrustReason: lease.Diagnostics.TrustReason,
		},
	}
	if !snapshot.Valid() {
		return model.Snapshot{}, invalidAuthorityError(
			errors.New("authority snapshot cannot be represented publicly"),
		)
	}
	return snapshot, nil
}

func internalAuthoritySnapshot(
	snapshot model.Snapshot,
) (authoritySnapshotLease, error) {
	if !snapshot.Valid() {
		return authoritySnapshotLease{}, invalidAuthorityError(
			errors.New("public snapshot lease is invalid"),
		)
	}
	lease := authoritySnapshotLease{
		Identity:             internalAuthorityIdentity(snapshot.Identity),
		VisibilityGeneration: snapshot.VisibilityGeneration,
		AuthorityEffective:   internalAuthorityHead(snapshot.AuthorityEffective),
		QueryHead:            internalAuthorityHead(snapshot.QueryHead),
		Cutoff: authorityCutoff{
			AdoptionEventSeq: snapshot.Cutoff.AdoptionEventSeq,
			PublicationID:    snapshot.Cutoff.PublicationID,
		},
		SelectedPublicationID: snapshot.Selector.SelectedPublicationID,
		Diagnostics: authoritySnapshotDiagnostics{
			Physical:    internalAuthorityHead(snapshot.Diagnostics.Physical),
			TrustStatus: snapshot.Diagnostics.TrustStatus,
			TrustBasis:  snapshot.Diagnostics.TrustBasis,
			TrustReason: snapshot.Diagnostics.TrustReason,
		},
	}
	switch snapshot.Selector.Mode {
	case model.SnapshotAtTip:
		lease.Mode = authoritySnapshotAtTip
	case model.SnapshotAtBlock:
		lease.Mode = authoritySnapshotAtBlock
		lease.BlockHash = authorityHash(*snapshot.Selector.RequestedBlockHash)
		lease.SelectedPoint = internalAuthorityPoint(
			*snapshot.Selector.SelectedPoint,
		)
	default:
		return authoritySnapshotLease{}, invalidAuthorityError(
			errors.New("public snapshot selector mode is invalid"),
		)
	}
	if err := validateAuthoritySnapshotLeaseShape(lease); err != nil {
		return authoritySnapshotLease{}, err
	}
	return lease, nil
}

func refreshModelAuthoritySnapshotWithReaders(
	ctx context.Context,
	snapshot model.Snapshot,
	readers authoritySnapshotFinishReaders,
) (model.Snapshot, error) {
	lease, err := internalAuthoritySnapshot(snapshot)
	if err != nil {
		return model.Snapshot{}, err
	}
	diagnostics, err := refreshAuthoritySnapshotLeaseWithReaders(
		ctx,
		lease,
		readers,
	)
	if err != nil {
		return model.Snapshot{}, err
	}
	lease.Diagnostics = diagnostics
	return modelAuthoritySnapshot(lease)
}
