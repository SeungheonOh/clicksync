package clickhouse

import (
	"context"
	"errors"
	"time"
)

type authoritySnapshotMode uint8

const (
	authoritySnapshotAtTip authoritySnapshotMode = iota + 1
	authoritySnapshotAtBlock
)

type authoritySnapshotRequest struct {
	Mode      authoritySnapshotMode
	BlockHash authorityHash
}

type authoritySnapshotIdentity struct {
	DatasetID              [16]byte
	SchemaContractHash     authorityHash
	NetworkMagic           uint32
	NetworkName            string
	ByronGenesisID         authorityHash
	ByronGenesisJSONHash   authorityHash
	ShelleyGenesisID       authorityHash
	ShelleyGenesisJSONHash authorityHash
	Start                  authorityPoint
	TrustMode              string
	CreatedAt              time.Time
	CompleteHistory        bool
}

type authoritySnapshotDiagnostics struct {
	Physical    authorityHead
	TrustStatus string
	TrustBasis  string
	TrustReason string
}

type authoritySnapshotLease struct {
	Identity              authoritySnapshotIdentity
	VisibilityGeneration  uint64
	AuthorityEffective    authorityHead
	QueryHead             authorityHead
	Cutoff                authorityCutoff
	Mode                  authoritySnapshotMode
	BlockHash             authorityHash
	SelectedPublicationID uint64
	SelectedPoint         authorityPoint
	Diagnostics           authoritySnapshotDiagnostics
}

type authoritySnapshotAcquireReaders struct {
	readHead          authorityHeadAttemptReader
	loadEvidence      func(context.Context, [16]byte) ([]authorityObservationRow, error)
	validateArtifacts func(context.Context, authorityRecord) error
	selectAtTip       func(context.Context, authorityRecord) (authoritySelection, error)
	selectAtBlock     func(context.Context, authorityRecord, authorityHash) (authoritySelection, error)
}

func authoritySnapshotIdentityFromRecord(
	record authorityRecord,
) authoritySnapshotIdentity {
	return authoritySnapshotIdentity{
		DatasetID:              record.DatasetID,
		SchemaContractHash:     record.SchemaContractHash,
		NetworkMagic:           record.NetworkMagic,
		NetworkName:            record.NetworkName,
		ByronGenesisID:         record.ByronGenesisID,
		ByronGenesisJSONHash:   record.ByronGenesisJSONHash,
		ShelleyGenesisID:       record.ShelleyGenesisID,
		ShelleyGenesisJSONHash: record.ShelleyGenesisJSONHash,
		Start:                  record.Start,
		TrustMode:              record.TrustMode,
		CreatedAt:              record.CreatedAt,
		CompleteHistory:        record.CompleteHistory,
	}
}

func sameAuthoritySnapshotIdentity(
	left authoritySnapshotIdentity,
	right authoritySnapshotIdentity,
) bool {
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
		left.CreatedAt.Equal(right.CreatedAt) &&
		left.CompleteHistory == right.CompleteHistory
}

func validateAuthoritySnapshotRecord(
	ctx context.Context,
	record authorityRecord,
	loadEvidence func(context.Context, [16]byte) ([]authorityObservationRow, error),
	validateArtifacts func(context.Context, authorityRecord) error,
) error {
	if loadEvidence == nil || validateArtifacts == nil {
		return errors.New("authority snapshot validation has a nil dependency")
	}
	var currentRows []authorityObservationRow
	if record.CheckID != nil {
		rows, err := loadEvidence(ctx, *record.CheckID)
		if err != nil {
			return err
		}
		currentRows = rows
	}
	var lastRows []authorityObservationRow
	if record.LastAgreedEvidence != nil {
		checkID := record.LastAgreedEvidence.CheckID
		if record.CheckID != nil && checkID == *record.CheckID {
			lastRows = currentRows
		} else {
			rows, err := loadEvidence(ctx, checkID)
			if err != nil {
				return err
			}
			lastRows = rows
		}
	}
	if err := bindAuthorityEvidence(record, currentRows, lastRows); err != nil {
		return err
	}
	return validateArtifacts(ctx, record)
}

func acquireAuthoritySnapshotLeaseWithReaders(
	ctx context.Context,
	request authoritySnapshotRequest,
	readers authoritySnapshotAcquireReaders,
) (authoritySnapshotLease, error) {
	if request.Mode != authoritySnapshotAtTip &&
		request.Mode != authoritySnapshotAtBlock {
		return authoritySnapshotLease{}, errors.New(
			"unknown authority snapshot mode",
		)
	}
	if request.Mode == authoritySnapshotAtTip &&
		request.BlockHash != (authorityHash{}) {
		return authoritySnapshotLease{}, errors.New(
			"AtTip authority snapshot request carries a block hash",
		)
	}
	if request.Mode == authoritySnapshotAtBlock &&
		request.BlockHash == (authorityHash{}) {
		return authoritySnapshotLease{}, ErrNotFound
	}
	if readers.readHead == nil ||
		readers.loadEvidence == nil ||
		readers.validateArtifacts == nil ||
		(request.Mode == authoritySnapshotAtTip &&
			readers.selectAtTip == nil) ||
		(request.Mode == authoritySnapshotAtBlock &&
			readers.selectAtBlock == nil) {
		return authoritySnapshotLease{}, errors.New(
			"authority snapshot acquire has a nil dependency",
		)
	}
	return stabilizeAuthorityHead(
		ctx,
		readers.readHead,
		func(
			resolveCtx context.Context,
			attempt authorityHeadAttempt,
		) (authoritySnapshotLease, error) {
			if !attempt.Found {
				return authoritySnapshotLease{}, newSnapshotUnavailableError(
					"dataset manifest is absent",
					nil,
				)
			}
			record := attempt.Latest
			if err := validateAuthoritySnapshotRecord(
				resolveCtx,
				record,
				readers.loadEvidence,
				readers.validateArtifacts,
			); err != nil {
				return authoritySnapshotLease{}, err
			}
			if err := ensureAuthoritySelectionServable(record); err != nil {
				return authoritySnapshotLease{}, err
			}
			var (
				selection authoritySelection
				err       error
			)
			switch request.Mode {
			case authoritySnapshotAtTip:
				selection, err = readers.selectAtTip(resolveCtx, record)
			case authoritySnapshotAtBlock:
				selection, err = readers.selectAtBlock(
					resolveCtx,
					record,
					request.BlockHash,
				)
			}
			if err != nil {
				return authoritySnapshotLease{}, err
			}
			if selection.AuthorityEffective != record.Effective {
				return authoritySnapshotLease{}, invalidAuthorityError(
					errors.New(
						"authority selector returned a different Effective head",
					),
				)
			}
			lease := authoritySnapshotLease{
				Identity:             authoritySnapshotIdentityFromRecord(record),
				VisibilityGeneration: record.VisibilityGeneration,
				AuthorityEffective:   selection.AuthorityEffective,
				QueryHead:            selection.QueryHead,
				Cutoff:               selection.Cutoff,
				Mode:                 request.Mode,
				BlockHash:            request.BlockHash,
				Diagnostics: authoritySnapshotDiagnostics{
					Physical:    record.Physical,
					TrustStatus: record.TrustStatus,
					TrustBasis:  record.TrustBasis,
					TrustReason: record.TrustReason,
				},
			}
			if request.Mode == authoritySnapshotAtBlock {
				lease.SelectedPublicationID = selection.Cutoff.PublicationID
				lease.SelectedPoint = selection.QueryHead.Point
			}
			if err := validateAuthoritySnapshotLeaseShape(lease); err != nil {
				return authoritySnapshotLease{}, err
			}
			return lease, nil
		},
	)
}

type authoritySnapshotHeadKind uint8

const (
	authoritySnapshotBoundaryHead authoritySnapshotHeadKind = iota + 1
	authoritySnapshotAdoptionHead
	authoritySnapshotRollbackHead
)

type authoritySnapshotHeadArtifacts struct {
	Kind          authoritySnapshotHeadKind
	Adoption      authorityPhysicalAdoptionRow
	AdoptionPoint authorityPoint
	Block         authorityPhysicalBlockRow
	BlockPoint    authorityPoint
	Rollback      authorityPhysicalRollbackRow
	RollbackTo    authorityPoint
	RollbackOld   authorityPoint
	RollbackProof authorityHash
}

func loadAuthoritySnapshotHeadArtifacts(
	ctx context.Context,
	record authorityRecord,
	head authorityHead,
	readers authorityArtifactReaders,
) (authoritySnapshotHeadArtifacts, error) {
	if err := ctx.Err(); err != nil {
		return authoritySnapshotHeadArtifacts{}, err
	}
	if head.EventSeq == 0 {
		if head.Point != record.Start {
			return authoritySnapshotHeadArtifacts{}, invalidAuthorityError(
				errors.New(
					"event-zero snapshot head differs from immutable start",
				),
			)
		}
		return authoritySnapshotHeadArtifacts{
			Kind: authoritySnapshotBoundaryHead,
		}, nil
	}
	adoption, adoptionPoint, adoptionFound, err :=
		readers.loadAdoption(ctx, head.EventSeq)
	if err != nil {
		return authoritySnapshotHeadArtifacts{}, err
	}
	rollback, rollbackTo, rollbackOld, rollbackProof, rollbackFound, err :=
		readers.loadRollback(ctx, head.EventSeq)
	if err != nil {
		return authoritySnapshotHeadArtifacts{}, err
	}
	if adoptionFound == rollbackFound {
		return authoritySnapshotHeadArtifacts{}, invalidAuthorityError(
			errors.New(
				"snapshot head does not have exactly one adoption/rollback header",
			),
		)
	}
	if adoptionFound {
		block, blockPoint, found, err := readers.loadBlock(
			ctx,
			adoption.PublicationID,
		)
		if err != nil {
			return authoritySnapshotHeadArtifacts{}, err
		}
		if !found {
			return authoritySnapshotHeadArtifacts{}, invalidAuthorityError(
				errors.New(
					"snapshot adoption has no exact block publication",
				),
			)
		}
		projected := record
		projected.Physical = head
		if err := readers.validateAdoption(
			projected,
			adoption,
			adoptionPoint,
			block,
			blockPoint,
		); err != nil {
			return authoritySnapshotHeadArtifacts{}, err
		}
		foundInvalidation, err := readers.probeAt(
			ctx,
			authorityArtifactInvalidation,
			head.EventSeq,
		)
		if err != nil {
			return authoritySnapshotHeadArtifacts{}, err
		}
		if foundInvalidation {
			return authoritySnapshotHeadArtifacts{}, invalidAuthorityError(
				errors.New(
					"snapshot adoption has a same-event invalidation",
				),
			)
		}
		return authoritySnapshotHeadArtifacts{
			Kind:          authoritySnapshotAdoptionHead,
			Adoption:      adoption,
			AdoptionPoint: adoptionPoint,
			Block:         block,
			BlockPoint:    blockPoint,
		}, nil
	}

	if rollbackTo != head.Point {
		return authoritySnapshotHeadArtifacts{}, invalidAuthorityError(
			errors.New(
				"snapshot rollback target differs from pinned query head",
			),
		)
	}
	evidence, err := readers.loadEvidence(
		ctx,
		authorityUUID(rollback.CheckID),
	)
	if err != nil {
		return authoritySnapshotHeadArtifacts{}, err
	}
	if rollback.AgreementGroup == nil {
		return authoritySnapshotHeadArtifacts{}, invalidAuthorityError(
			errors.New("snapshot rollback agreement group is absent"),
		)
	}
	binding, err := bindAuthorityRollbackEvidence(
		evidence,
		authorityUUID(rollback.CheckID),
		authorityUUID(*rollback.AgreementGroup),
		rollback.CheckAttempt,
		rollback.CorroborationRequired,
		authorityHead{
			EventSeq: rollback.CheckedEventSeq,
			Point:    rollbackTo,
		},
	)
	if err != nil {
		return authoritySnapshotHeadArtifacts{}, err
	}
	if binding.Commitment.Count != rollback.EvidenceCount ||
		binding.Commitment.Digest != rollbackProof ||
		binding.Outcome.Confirmed < rollback.CorroborationRequired ||
		binding.Outcome.Disagreement {
		return authoritySnapshotHeadArtifacts{}, invalidAuthorityError(
			errors.New(
				"snapshot rollback evidence differs from its frozen header",
			),
		)
	}
	if err := validateAuthorityRollbackObserverMap(
		rollback.ObservedPeers,
		rollback.ObservedOperators,
		binding.Agreed,
	); err != nil {
		return authoritySnapshotHeadArtifacts{}, err
	}
	complete, err := readers.validateInvalidations(
		ctx,
		record,
		rollback,
		rollbackTo,
		rollbackOld,
	)
	if err != nil {
		return authoritySnapshotHeadArtifacts{}, err
	}
	if !complete {
		return authoritySnapshotHeadArtifacts{}, invalidAuthorityError(
			errors.New(
				"snapshot rollback has no exact invalidation set",
			),
		)
	}
	return authoritySnapshotHeadArtifacts{
		Kind:          authoritySnapshotRollbackHead,
		Rollback:      rollback,
		RollbackTo:    rollbackTo,
		RollbackOld:   rollbackOld,
		RollbackProof: rollbackProof,
	}, nil
}

type authoritySnapshotFinishReaders struct {
	readHead          authorityHeadAttemptReader
	loadEvidence      func(context.Context, [16]byte) ([]authorityObservationRow, error)
	validateArtifacts func(context.Context, authorityRecord) error
	headArtifacts     authorityArtifactReaders
	cutoff            authoritySelectionCutoffReaders
	loadActive        func(
		context.Context,
		authorityRecord,
		uint64,
		authorityHash,
	) (authorityActiveBlock, bool, error)
	selectAtBlock func(
		context.Context,
		authorityRecord,
		authorityHash,
	) (authoritySelection, error)
}

func unavailableAuthoritySnapshot(
	record authorityRecord,
	reason string,
) error {
	return newSnapshotUnavailableError(reason, &record)
}

func validateAuthoritySnapshotCutoff(
	ctx context.Context,
	record authorityRecord,
	lease authoritySnapshotLease,
	head authoritySnapshotHeadArtifacts,
	readers authoritySelectionCutoffReaders,
) (authorityCutoffArtifacts, bool, error) {
	cutoff, artifacts, found, err := selectAndBindAuthorityCutoff(
		ctx,
		record,
		lease.QueryHead.EventSeq,
		readers,
	)
	if err != nil {
		return authorityCutoffArtifacts{}, false, err
	}
	if cutoff != lease.Cutoff {
		return authorityCutoffArtifacts{}, false, invalidAuthorityError(
			errors.New("snapshot cutoff no longer maps to its exact logical pair"),
		)
	}
	if cutoff == (authorityCutoff{}) {
		if found || head.Kind == authoritySnapshotAdoptionHead {
			return authorityCutoffArtifacts{}, false, invalidAuthorityError(
				errors.New("snapshot cutoff unexpectedly has no exact adoption"),
			)
		}
		return authorityCutoffArtifacts{}, false, nil
	}
	if !found ||
		artifacts.Adoption.EventSeq != cutoff.AdoptionEventSeq ||
		artifacts.Adoption.PublicationID != cutoff.PublicationID ||
		artifacts.Block.PublicationID != cutoff.PublicationID ||
		artifacts.AdoptionPoint != artifacts.BlockPoint {
		return authorityCutoffArtifacts{}, false, invalidAuthorityError(
			errors.New("snapshot cutoff rebound artifacts are not exact"),
		)
	}
	if head.Kind == authoritySnapshotAdoptionHead {
		if cutoff != (authorityCutoff{
			AdoptionEventSeq: head.Adoption.EventSeq,
			PublicationID:    head.Adoption.PublicationID,
		}) ||
			!sameAuthorityPhysicalAdoptionRow(
				artifacts.Adoption,
				head.Adoption,
			) ||
			!sameAuthorityPhysicalBlockRow(artifacts.Block, head.Block) ||
			artifacts.AdoptionPoint != head.AdoptionPoint ||
			artifacts.BlockPoint != head.BlockPoint {
			return authorityCutoffArtifacts{}, false, invalidAuthorityError(
				errors.New(
					"snapshot adoption differs from its rebound cutoff artifacts",
				),
			)
		}
	}
	return artifacts, true, nil
}

func validateAuthoritySnapshotHeadActivity(
	ctx context.Context,
	record authorityRecord,
	snapshot uint64,
	head authoritySnapshotHeadArtifacts,
	readers authoritySnapshotFinishReaders,
) error {
	switch head.Kind {
	case authoritySnapshotBoundaryHead:
		return nil
	case authoritySnapshotAdoptionHead:
		active, found, err := readers.loadActive(
			ctx,
			record,
			snapshot,
			head.BlockPoint.Hash,
		)
		if err != nil {
			return err
		}
		if !found ||
			active.PublicationID != head.Block.PublicationID ||
			active.Point != head.BlockPoint {
			return unavailableAuthoritySnapshot(
				record,
				"pinned adoption is no longer the exact active hash publication",
			)
		}
		if active.AdoptionEventSeq != head.Adoption.EventSeq ||
			active.Synthetic != head.Block.Synthetic {
			return invalidAuthorityError(
				errors.New(
					"active pinned publication differs from its exact adoption",
				),
			)
		}
		return nil
	case authoritySnapshotRollbackHead:
		target := head.RollbackTo
		if target.Origin ||
			(!record.Start.Origin && target == record.Start) {
			return nil
		}
		active, found, err := readers.loadActive(
			ctx,
			record,
			snapshot,
			target.Hash,
		)
		if err != nil {
			return err
		}
		if !found || active.Point != target {
			return unavailableAuthoritySnapshot(
				record,
				"pinned rollback target is no longer exactly active",
			)
		}
		return nil
	default:
		return unavailableAuthoritySnapshot(
			record,
			"snapshot head kind is invalid",
		)
	}
}

func validateAuthoritySnapshotLeaseShape(
	lease authoritySnapshotLease,
) error {
	if lease.Mode != authoritySnapshotAtTip &&
		lease.Mode != authoritySnapshotAtBlock {
		return invalidAuthorityError(
			errors.New("snapshot lease mode is invalid"),
		)
	}
	if err := validateAuthorityPoint(
		"snapshot lease AuthorityEffective point",
		lease.AuthorityEffective.Point,
	); err != nil {
		return invalidAuthorityError(
			errors.New(
				"snapshot lease AuthorityEffective point is invalid",
			),
		)
	}
	if err := validateAuthorityPoint(
		"snapshot lease QueryHead point",
		lease.QueryHead.Point,
	); err != nil {
		return invalidAuthorityError(
			errors.New("snapshot lease QueryHead point is invalid"),
		)
	}
	if lease.Identity.CompleteHistory &&
		(lease.AuthorityEffective.EventSeq == 0 ||
			lease.QueryHead.EventSeq == 0) {
		return invalidAuthorityError(
			errors.New(
				"complete-history snapshot lease cannot pin event zero",
			),
		)
	}
	if (lease.Cutoff.AdoptionEventSeq == 0) !=
		(lease.Cutoff.PublicationID == 0) {
		return invalidAuthorityError(
			errors.New("snapshot lease cutoff is partially zero"),
		)
	}
	switch lease.Mode {
	case authoritySnapshotAtTip:
		if lease.QueryHead != lease.AuthorityEffective ||
			lease.BlockHash != (authorityHash{}) ||
			lease.SelectedPublicationID != 0 ||
			lease.SelectedPoint != (authorityPoint{}) {
			return invalidAuthorityError(
				errors.New("AtTip snapshot lease shape is invalid"),
			)
		}
	case authoritySnapshotAtBlock:
		if lease.BlockHash == (authorityHash{}) ||
			lease.SelectedPublicationID == 0 ||
			lease.QueryHead.EventSeq == 0 ||
			lease.QueryHead.EventSeq > lease.AuthorityEffective.EventSeq ||
			lease.Cutoff != (authorityCutoff{
				AdoptionEventSeq: lease.QueryHead.EventSeq,
				PublicationID:    lease.SelectedPublicationID,
			}) ||
			lease.SelectedPoint != lease.QueryHead.Point {
			return invalidAuthorityError(
				errors.New("AtBlock snapshot lease shape is invalid"),
			)
		}
	}
	return nil
}

func finishAuthoritySnapshotLeaseAgainstRecord(
	ctx context.Context,
	record authorityRecord,
	lease authoritySnapshotLease,
	readers authoritySnapshotFinishReaders,
) error {
	if err := validateAuthoritySnapshotLeaseShape(lease); err != nil {
		return err
	}
	if !sameAuthoritySnapshotIdentity(
		authoritySnapshotIdentityFromRecord(record),
		lease.Identity,
	) {
		return unavailableAuthoritySnapshot(
			record,
			"snapshot immutable dataset identity changed",
		)
	}
	if record.VisibilityGeneration != lease.VisibilityGeneration {
		return unavailableAuthoritySnapshot(
			record,
			"snapshot visibility generation changed",
		)
	}
	if err := ensureAuthoritySelectionServable(record); err != nil {
		return err
	}
	if lease.AuthorityEffective.EventSeq > record.Effective.EventSeq {
		return unavailableAuthoritySnapshot(
			record,
			"snapshot AuthorityEffective is newer than fresh Effective",
		)
	}
	if lease.AuthorityEffective.EventSeq == record.Effective.EventSeq &&
		lease.AuthorityEffective != record.Effective {
		return unavailableAuthoritySnapshot(
			record,
			"snapshot AuthorityEffective point changed at the same event",
		)
	}
	authorityHeadArtifacts, err := loadAuthoritySnapshotHeadArtifacts(
		ctx,
		record,
		lease.AuthorityEffective,
		readers.headArtifacts,
	)
	if err != nil {
		return err
	}
	queryHeadArtifacts := authorityHeadArtifacts
	if lease.QueryHead != lease.AuthorityEffective {
		queryHeadArtifacts, err = loadAuthoritySnapshotHeadArtifacts(
			ctx,
			record,
			lease.QueryHead,
			readers.headArtifacts,
		)
		if err != nil {
			return err
		}
	}
	_, _, err = validateAuthoritySnapshotCutoff(
		ctx,
		record,
		lease,
		queryHeadArtifacts,
		readers.cutoff,
	)
	if err != nil {
		return err
	}
	if err := validateAuthoritySnapshotHeadActivity(
		ctx,
		record,
		lease.AuthorityEffective.EventSeq,
		authorityHeadArtifacts,
		readers,
	); err != nil {
		return err
	}
	if err := validateAuthoritySnapshotHeadActivity(
		ctx,
		record,
		record.Effective.EventSeq,
		authorityHeadArtifacts,
		readers,
	); err != nil {
		return err
	}
	switch lease.Mode {
	case authoritySnapshotAtBlock:
		head := queryHeadArtifacts
		if lease.BlockHash == (authorityHash{}) ||
			lease.SelectedPublicationID == 0 ||
			head.Kind != authoritySnapshotAdoptionHead ||
			head.Adoption.PublicationID != lease.SelectedPublicationID ||
			head.Adoption.EventSeq != lease.QueryHead.EventSeq ||
			head.Block.PublicationID != lease.SelectedPublicationID ||
			head.BlockPoint.Hash != lease.BlockHash {
			return invalidAuthorityError(
				errors.New(
					"AtBlock lease differs from its exact selected publication",
				),
			)
		}
		if (!lease.SelectedPoint.Origin &&
			head.BlockPoint != lease.SelectedPoint) ||
			(lease.SelectedPoint.Origin && !head.Block.Synthetic) {
			return invalidAuthorityError(
				errors.New(
					"AtBlock lease semantic point differs from its exact block",
				),
			)
		}
		original := record
		original.Effective = lease.AuthorityEffective
		selection, err := readers.selectAtBlock(
			ctx,
			original,
			lease.BlockHash,
		)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				return invalidAuthorityError(
					errors.New(
						"AtBlock publication is absent under original AuthorityEffective",
					),
				)
			}
			return err
		}
		if selection.AuthorityEffective != lease.AuthorityEffective ||
			selection.QueryHead != lease.QueryHead ||
			selection.Cutoff != lease.Cutoff ||
			selection.Cutoff.PublicationID != lease.SelectedPublicationID {
			return invalidAuthorityError(
				errors.New(
					"AtBlock selection differs under original AuthorityEffective",
				),
			)
		}
		selection, err = readers.selectAtBlock(
			ctx,
			record,
			lease.BlockHash,
		)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				return unavailableAuthoritySnapshot(
					record,
					"AtBlock publication is no longer active",
				)
			}
			return err
		}
		if selection.AuthorityEffective != record.Effective {
			return invalidAuthorityError(
				errors.New(
					"AtBlock selector returned a different fresh AuthorityEffective",
				),
			)
		}
		if selection.QueryHead != lease.QueryHead ||
			selection.Cutoff != lease.Cutoff ||
			selection.Cutoff.PublicationID != lease.SelectedPublicationID {
			return unavailableAuthoritySnapshot(
				record,
				"AtBlock selection changed under fresh Effective",
			)
		}
		return nil
	case authoritySnapshotAtTip:
		if lease.QueryHead != lease.AuthorityEffective {
			return unavailableAuthoritySnapshot(
				record,
				"AtTip QueryHead differs from AuthorityEffective",
			)
		}
		return nil
	}
	return unavailableAuthoritySnapshot(
		record,
		"snapshot lease mode is invalid",
	)
}

func finishAuthoritySnapshotLeaseWithReaders(
	ctx context.Context,
	lease authoritySnapshotLease,
	readers authoritySnapshotFinishReaders,
) error {
	_, err := refreshAuthoritySnapshotLeaseWithReaders(ctx, lease, readers)
	return err
}

func refreshAuthoritySnapshotLeaseWithReaders(
	ctx context.Context,
	lease authoritySnapshotLease,
	readers authoritySnapshotFinishReaders,
) (authoritySnapshotDiagnostics, error) {
	if readers.readHead == nil ||
		readers.loadEvidence == nil ||
		readers.validateArtifacts == nil ||
		readers.headArtifacts.loadAdoption == nil ||
		readers.headArtifacts.loadBlock == nil ||
		readers.headArtifacts.loadRollback == nil ||
		readers.headArtifacts.loadEvidence == nil ||
		readers.headArtifacts.validateAdoption == nil ||
		readers.headArtifacts.validateInvalidations == nil ||
		readers.headArtifacts.probeAt == nil ||
		readers.cutoff.load == nil ||
		readers.cutoff.bind == nil ||
		readers.loadActive == nil ||
		readers.selectAtBlock == nil {
		return authoritySnapshotDiagnostics{},
			errors.New("authority snapshot Finish has a nil dependency")
	}
	return stabilizeAuthorityHead(
		ctx,
		readers.readHead,
		func(
			resolveCtx context.Context,
			attempt authorityHeadAttempt,
		) (authoritySnapshotDiagnostics, error) {
			if !attempt.Found {
				return authoritySnapshotDiagnostics{}, newSnapshotUnavailableError(
					"dataset manifest is absent",
					nil,
				)
			}
			record := attempt.Latest
			if err := validateAuthoritySnapshotRecord(
				resolveCtx,
				record,
				readers.loadEvidence,
				readers.validateArtifacts,
			); err != nil {
				return authoritySnapshotDiagnostics{}, err
			}
			if err := finishAuthoritySnapshotLeaseAgainstRecord(
				resolveCtx,
				record,
				lease,
				readers,
			); err != nil {
				return authoritySnapshotDiagnostics{}, err
			}
			return authoritySnapshotDiagnostics{
				Physical:    record.Physical,
				TrustStatus: record.TrustStatus,
				TrustBasis:  record.TrustBasis,
				TrustReason: record.TrustReason,
			}, nil
		},
	)
}

func (store *Store) finishAuthoritySnapshotLease(
	ctx context.Context,
	lease authoritySnapshotLease,
) error {
	_, err := store.refreshAuthoritySnapshotLease(ctx, lease)
	return err
}

func (store *Store) refreshAuthoritySnapshotLease(
	ctx context.Context,
	lease authoritySnapshotLease,
) (authoritySnapshotLease, error) {
	diagnostics, err := refreshAuthoritySnapshotLeaseWithReaders(
		ctx,
		lease,
		store.snapshotAuthorityFinishReaders(),
	)
	if err != nil {
		return authoritySnapshotLease{}, err
	}
	lease.Diagnostics = diagnostics
	return lease, nil
}

func (store *Store) snapshotAuthorityFinishReaders() authoritySnapshotFinishReaders {
	return authoritySnapshotFinishReaders{
		readHead:          store.readAuthorityHeadAttempt,
		loadEvidence:      store.loadAuthorityObservationRows,
		validateArtifacts: store.validateAuthorityRecordArtifacts,
		headArtifacts:     store.authorityArtifactValidationReaders(),
		cutoff:            store.authoritySelectionCutoffReaders(),
		loadActive:        store.loadAuthorityActiveBlockByHash,
		selectAtBlock:     store.selectAuthorityAtBlock,
	}
}

func (store *Store) acquireAuthoritySnapshotLease(
	ctx context.Context,
	request authoritySnapshotRequest,
) (authoritySnapshotLease, error) {
	return acquireAuthoritySnapshotLeaseWithReaders(
		ctx,
		request,
		authoritySnapshotAcquireReaders{
			readHead:          store.readAuthorityHeadAttempt,
			loadEvidence:      store.loadAuthorityObservationRows,
			validateArtifacts: store.validateAuthorityRecordArtifacts,
			selectAtTip:       store.selectAuthorityAtTip,
			selectAtBlock:     store.selectAuthorityAtBlock,
		},
	)
}
