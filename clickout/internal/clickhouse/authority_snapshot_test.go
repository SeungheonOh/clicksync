package clickhouse

import (
	"context"
	"crypto/sha256"
	"errors"
	"testing"
	"time"
)

func authoritySnapshotAdoptionTestState(
	t *testing.T,
) (
	authorityRecord,
	authoritySnapshotLease,
	authoritySnapshotFinishReaders,
	*[]uint64,
	*[]uint64,
) {
	t.Helper()
	cutoff, adoption, point, block := authorityCutoffBindingFixture()
	original := authorityHead{
		EventSeq: adoption.EventSeq,
		Point:    point,
	}
	record := authorityRecord{
		DatasetID:              authorityFill16(0x11),
		SchemaContractHash:     authorityFill32(0x12),
		NetworkMagic:           764824073,
		NetworkName:            "mainnet",
		ByronGenesisID:         authorityFill32(0x13),
		ByronGenesisJSONHash:   authorityFill32(0x14),
		ShelleyGenesisID:       authorityFill32(0x15),
		ShelleyGenesisJSONHash: authorityFill32(0x16),
		Start:                  authorityPoint{Origin: true},
		CompleteHistory:        true,
		TrustMode:              "peer_observed",
		CreatedAt: time.Date(
			2026, time.July, 23, 1, 2, 3, 456000000, time.UTC,
		),
		Servable:             true,
		VisibilityGeneration: 7,
		Effective: authorityHead{
			EventSeq: 20,
			Point: authorityPoint{
				Slot:        1,
				Hash:        authorityFill32(0x01),
				BlockNumber: 1,
			},
		},
	}
	lease := authoritySnapshotLease{
		Identity:             authoritySnapshotIdentityFromRecord(record),
		VisibilityGeneration: record.VisibilityGeneration,
		AuthorityEffective:   original,
		QueryHead:            original,
		Cutoff:               cutoff,
		Mode:                 authoritySnapshotAtTip,
	}
	lifecycleSnapshots := make([]uint64, 0, 2)
	selectorSnapshots := make([]uint64, 0, 2)
	headReaders := defaultAuthorityArtifactTestReaders()
	headReaders.loadAdoption = func(
		_ context.Context,
		eventSeq uint64,
	) (authorityPhysicalAdoptionRow, authorityPoint, bool, error) {
		if eventSeq != adoption.EventSeq {
			return authorityPhysicalAdoptionRow{}, authorityPoint{}, false, nil
		}
		return adoption, point, true, nil
	}
	headReaders.loadBlock = func(
		_ context.Context,
		publicationID uint64,
	) (authorityPhysicalBlockRow, authorityPoint, bool, error) {
		if publicationID != block.PublicationID {
			return authorityPhysicalBlockRow{}, authorityPoint{}, false, nil
		}
		return block, point, true, nil
	}
	headReaders.validateAdoption = validateAuthorityPhysicalAdoptionMapping
	readers := authoritySnapshotFinishReaders{
		loadEvidence: func(
			context.Context,
			[16]byte,
		) ([]authorityObservationRow, error) {
			return nil, nil
		},
		validateArtifacts: func(context.Context, authorityRecord) error {
			return nil
		},
		headArtifacts: headReaders,
		cutoff: authoritySelectionCutoffReaders{
			load: func(
				_ context.Context,
				eventSeq uint64,
			) (authorityCutoff, error) {
				if eventSeq != cutoff.AdoptionEventSeq {
					t.Fatalf("cutoff event = %d", eventSeq)
				}
				return cutoff, nil
			},
			bind: func(
				context.Context,
				authorityRecord,
				authorityCutoff,
			) (authorityCutoffArtifacts, bool, error) {
				return authorityCutoffArtifacts{
					Adoption:      adoption,
					AdoptionPoint: point,
					Block:         block,
					BlockPoint:    point,
				}, true, nil
			},
		},
		loadActive: func(
			_ context.Context,
			_ authorityRecord,
			snapshot uint64,
			hash authorityHash,
		) (authorityActiveBlock, bool, error) {
			lifecycleSnapshots = append(lifecycleSnapshots, snapshot)
			if hash != point.Hash {
				t.Fatalf("active hash = %x", hash)
			}
			return authorityActiveBlock{
				PublicationID:    block.PublicationID,
				AdoptionEventSeq: adoption.EventSeq,
				Point:            point,
				Synthetic:        block.Synthetic,
			}, true, nil
		},
		selectAtBlock: func(
			_ context.Context,
			projected authorityRecord,
			hash authorityHash,
		) (authoritySelection, error) {
			selectorSnapshots = append(
				selectorSnapshots,
				projected.Effective.EventSeq,
			)
			if hash != point.Hash {
				t.Fatalf("AtBlock hash = %x", hash)
			}
			return authoritySelection{
				AuthorityEffective: projected.Effective,
				QueryHead:          original,
				Cutoff:             cutoff,
			}, nil
		},
	}
	return record, lease, readers, &lifecycleSnapshots, &selectorSnapshots
}

func TestAcquireAuthoritySnapshotAtTipRetriesChangedHead(t *testing.T) {
	t.Parallel()
	firstObservation := authorityHeadObservation{
		Present:  true,
		Revision: 1,
	}
	changedObservation := authorityHeadObservation{
		Present:  true,
		Revision: 2,
	}
	firstRecord := authorityRecord{
		DatasetID:            authorityFill16(0x11),
		VisibilityGeneration: 4,
		Servable:             true,
		Effective: authorityHead{
			EventSeq: 10,
			Point:    authorityPoint{Hash: authorityFill32(0x21)},
		},
	}
	changedRecord := firstRecord
	changedRecord.VisibilityGeneration = 5
	changedRecord.Effective = authorityHead{
		EventSeq: 12,
		Point:    authorityPoint{Hash: authorityFill32(0x22)},
	}
	attempts := []authorityHeadAttempt{
		{
			Observation: firstObservation,
			Latest:      firstRecord,
			Found:       true,
		},
		{
			Observation: changedObservation,
			Latest:      changedRecord,
			Found:       true,
		},
		{
			Observation: changedObservation,
			Latest:      changedRecord,
			Found:       true,
		},
		{
			Observation: changedObservation,
			Latest:      changedRecord,
			Found:       true,
		},
	}
	index := 0
	resolves := 0
	lease, err := acquireAuthoritySnapshotLeaseWithReaders(
		context.Background(),
		authoritySnapshotRequest{Mode: authoritySnapshotAtTip},
		authoritySnapshotAcquireReaders{
			readHead: func(context.Context) (authorityHeadAttempt, error) {
				attempt := attempts[index]
				index++
				return attempt, nil
			},
			loadEvidence: func(
				context.Context,
				[16]byte,
			) ([]authorityObservationRow, error) {
				t.Fatal("record without evidence references loaded evidence")
				return nil, nil
			},
			validateArtifacts: func(context.Context, authorityRecord) error {
				resolves++
				return nil
			},
			selectAtTip: func(
				_ context.Context,
				record authorityRecord,
			) (authoritySelection, error) {
				return authoritySelection{
					AuthorityEffective: record.Effective,
					QueryHead:          record.Effective,
				}, nil
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if resolves != 2 ||
		index != 4 ||
		lease.VisibilityGeneration != changedRecord.VisibilityGeneration ||
		lease.QueryHead != changedRecord.Effective {
		t.Fatalf(
			"stable AtTip lease = %+v, resolves=%d reads=%d",
			lease,
			resolves,
			index,
		)
	}
}

func TestAuthoritySnapshotLeaseShapeMatrix(t *testing.T) {
	t.Parallel()
	record, atTip, _, _, _ := authoritySnapshotAdoptionTestState(t)
	atBlock := atTip
	atBlock.Mode = authoritySnapshotAtBlock
	atBlock.BlockHash = atBlock.QueryHead.Point.Hash
	atBlock.SelectedPublicationID = atBlock.Cutoff.PublicationID
	atBlock.SelectedPoint = atBlock.QueryHead.Point
	valid := []authoritySnapshotLease{atTip, atBlock}
	for index, lease := range valid {
		if err := validateAuthoritySnapshotLeaseShape(lease); err != nil {
			t.Fatalf("valid lease %d rejected: %v", index, err)
		}
	}
	completeHistoryEventZero := atTip
	completeHistoryEventZero.AuthorityEffective = authorityHead{
		Point: authorityPoint{Origin: true},
	}
	completeHistoryEventZero.QueryHead =
		completeHistoryEventZero.AuthorityEffective
	completeHistoryEventZero.Cutoff = authorityCutoff{}
	if err := validateAuthoritySnapshotLeaseShape(
		completeHistoryEventZero,
	); !errors.Is(err, ErrInvalidDataset) {
		t.Fatalf("complete-history event-zero shape error = %v", err)
	}
	tests := map[string]func(*authoritySnapshotLease){
		"mode": func(lease *authoritySnapshotLease) {
			lease.Mode = 99
		},
		"partial cutoff": func(lease *authoritySnapshotLease) {
			lease.Cutoff.PublicationID = 0
		},
		"AtTip head": func(lease *authoritySnapshotLease) {
			lease.QueryHead.EventSeq++
		},
		"AtTip hash": func(lease *authoritySnapshotLease) {
			lease.BlockHash = authorityFill32(0x31)
		},
		"AtTip publication": func(lease *authoritySnapshotLease) {
			lease.SelectedPublicationID = 1
		},
		"AtTip point": func(lease *authoritySnapshotLease) {
			lease.SelectedPoint = record.Effective.Point
		},
	}
	for name, mutate := range tests {
		name, mutate := name, mutate
		t.Run(name, func(t *testing.T) {
			lease := atTip
			mutate(&lease)
			if err := validateAuthoritySnapshotLeaseShape(
				lease,
			); !errors.Is(err, ErrInvalidDataset) {
				t.Fatalf("shape error = %v", err)
			}
		})
	}
	atBlockTests := map[string]func(*authoritySnapshotLease){
		"hash": func(lease *authoritySnapshotLease) {
			lease.BlockHash = authorityHash{}
		},
		"publication": func(lease *authoritySnapshotLease) {
			lease.SelectedPublicationID = 0
		},
		"event": func(lease *authoritySnapshotLease) {
			lease.QueryHead.EventSeq = 0
		},
		"event beyond effective": func(lease *authoritySnapshotLease) {
			lease.AuthorityEffective.EventSeq--
		},
		"cutoff": func(lease *authoritySnapshotLease) {
			lease.Cutoff.PublicationID++
		},
		"point": func(lease *authoritySnapshotLease) {
			lease.SelectedPoint = record.Effective.Point
		},
	}
	for name, mutate := range atBlockTests {
		name, mutate := name, mutate
		t.Run("AtBlock "+name, func(t *testing.T) {
			lease := atBlock
			mutate(&lease)
			if err := validateAuthoritySnapshotLeaseShape(
				lease,
			); !errors.Is(err, ErrInvalidDataset) {
				t.Fatalf("shape error = %v", err)
			}
		})
	}
}

func TestFinishAuthoritySnapshotIdentityGenerationAndEffectiveGates(
	t *testing.T,
) {
	t.Parallel()
	record, lease, readers, _, _ :=
		authoritySnapshotAdoptionTestState(t)
	sameInstant := time.FixedZone("same instant", -5*60*60)
	record.CreatedAt = record.CreatedAt.In(sameInstant)
	if err := finishAuthoritySnapshotLeaseAgainstRecord(
		context.Background(),
		record,
		lease,
		readers,
	); err != nil {
		t.Fatalf("CreatedAt.Equal representation rejected: %v", err)
	}

	tests := map[string]func(*authorityRecord){
		"dataset ID": func(value *authorityRecord) {
			value.DatasetID[0]++
		},
		"schema": func(value *authorityRecord) {
			value.SchemaContractHash[0]++
		},
		"network magic": func(value *authorityRecord) {
			value.NetworkMagic++
		},
		"network name": func(value *authorityRecord) {
			value.NetworkName += "-other"
		},
		"Byron genesis ID": func(value *authorityRecord) {
			value.ByronGenesisID[0]++
		},
		"Byron genesis JSON": func(value *authorityRecord) {
			value.ByronGenesisJSONHash[0]++
		},
		"Shelley genesis ID": func(value *authorityRecord) {
			value.ShelleyGenesisID[0]++
		},
		"Shelley genesis JSON": func(value *authorityRecord) {
			value.ShelleyGenesisJSONHash[0]++
		},
		"Start": func(value *authorityRecord) {
			value.Start = authorityPoint{Hash: authorityFill32(0x44)}
		},
		"trust mode": func(value *authorityRecord) {
			value.TrustMode += "-other"
		},
		"CreatedAt instant": func(value *authorityRecord) {
			value.CreatedAt = value.CreatedAt.Add(time.Microsecond)
		},
		"complete history": func(value *authorityRecord) {
			value.CompleteHistory = false
		},
		"generation": func(value *authorityRecord) {
			value.VisibilityGeneration++
		},
		"unservable": func(value *authorityRecord) {
			value.Servable = false
		},
		"effective behind": func(value *authorityRecord) {
			value.Effective.EventSeq = lease.AuthorityEffective.EventSeq - 1
		},
	}
	for name, mutate := range tests {
		name, mutate := name, mutate
		t.Run(name, func(t *testing.T) {
			changed := record
			mutate(&changed)
			err := finishAuthoritySnapshotLeaseAgainstRecord(
				context.Background(),
				changed,
				lease,
				readers,
			)
			if !errors.Is(err, ErrSnapshotUnavailable) ||
				errors.Is(err, ErrInvalidDataset) {
				t.Fatalf("stale gate error = %v", err)
			}
		})
	}

	for name, hash := range map[string]authorityHash{
		"lexically lower":  authorityFill32(0x01),
		"lexically higher": authorityFill32(0xff),
	} {
		name, hash := name, hash
		t.Run("same event "+name, func(t *testing.T) {
			changed := record
			changed.Effective = lease.AuthorityEffective
			changed.Effective.Point.Hash = hash
			if hash == lease.AuthorityEffective.Point.Hash {
				changed.Effective.Point.Hash[0]++
			}
			err := finishAuthoritySnapshotLeaseAgainstRecord(
				context.Background(),
				changed,
				lease,
				readers,
			)
			if !errors.Is(err, ErrSnapshotUnavailable) {
				t.Fatalf("same-event remap error = %v", err)
			}
		})
	}
}

func TestAuthoritySnapshotNilDependenciesAndAtTipHash(t *testing.T) {
	t.Parallel()
	called := false
	if _, err := acquireAuthoritySnapshotLeaseWithReaders(
		context.Background(),
		authoritySnapshotRequest{
			Mode:      authoritySnapshotAtTip,
			BlockHash: authorityFill32(0x41),
		},
		authoritySnapshotAcquireReaders{
			readHead: func(context.Context) (authorityHeadAttempt, error) {
				called = true
				return authorityHeadAttempt{}, nil
			},
		},
	); err == nil || called {
		t.Fatalf("AtTip hash acquire called=%t err=%v", called, err)
	}
	if err := finishAuthoritySnapshotLeaseWithReaders(
		context.Background(),
		authoritySnapshotLease{},
		authoritySnapshotFinishReaders{
			readHead: func(context.Context) (authorityHeadAttempt, error) {
				called = true
				return authorityHeadAttempt{}, nil
			},
		},
	); err == nil || called {
		t.Fatalf("nil Finish called=%t err=%v", called, err)
	}
}

func TestFinishAuthoritySnapshotAtTipAdoptionActivityAndCutoff(t *testing.T) {
	t.Parallel()
	record, lease, readers, activitySnapshots, _ :=
		authoritySnapshotAdoptionTestState(t)
	if err := finishAuthoritySnapshotLeaseAgainstRecord(
		context.Background(),
		record,
		lease,
		readers,
	); err != nil {
		t.Fatal(err)
	}
	if len(*activitySnapshots) != 2 ||
		(*activitySnapshots)[0] != lease.AuthorityEffective.EventSeq ||
		(*activitySnapshots)[1] != record.Effective.EventSeq {
		t.Fatalf("activity ceilings = %v", *activitySnapshots)
	}

	t.Run("multiple active", func(t *testing.T) {
		_, lease, readers, _, _ :=
			authoritySnapshotAdoptionTestState(t)
		readers.loadActive = func(
			context.Context,
			authorityRecord,
			uint64,
			authorityHash,
		) (authorityActiveBlock, bool, error) {
			return authorityActiveBlock{}, false, ErrInvalidDataset
		}
		err := finishAuthoritySnapshotLeaseAgainstRecord(
			context.Background(),
			record,
			lease,
			readers,
		)
		if !errors.Is(err, ErrInvalidDataset) {
			t.Fatalf("multiple-active error = %v", err)
		}
	})

	t.Run("fresh inactive", func(t *testing.T) {
		record, lease, readers, _, _ :=
			authoritySnapshotAdoptionTestState(t)
		calls := 0
		original := readers.loadActive
		readers.loadActive = func(
			ctx context.Context,
			record authorityRecord,
			snapshot uint64,
			hash authorityHash,
		) (authorityActiveBlock, bool, error) {
			calls++
			if calls == 2 {
				return authorityActiveBlock{}, false, nil
			}
			return original(ctx, record, snapshot, hash)
		}
		err := finishAuthoritySnapshotLeaseAgainstRecord(
			context.Background(),
			record,
			lease,
			readers,
		)
		if !errors.Is(err, ErrSnapshotUnavailable) ||
			errors.Is(err, ErrInvalidDataset) {
			t.Fatalf("inactive error = %v", err)
		}
	})

	t.Run("cutoff remap", func(t *testing.T) {
		record, lease, readers, _, _ :=
			authoritySnapshotAdoptionTestState(t)
		readers.cutoff.load = func(
			context.Context,
			uint64,
		) (authorityCutoff, error) {
			return authorityCutoff{
				AdoptionEventSeq: lease.Cutoff.AdoptionEventSeq,
				PublicationID:    lease.Cutoff.PublicationID + 1,
			}, nil
		}
		err := finishAuthoritySnapshotLeaseAgainstRecord(
			context.Background(),
			record,
			lease,
			readers,
		)
		if !errors.Is(err, ErrInvalidDataset) {
			t.Fatalf("cutoff remap error = %v", err)
		}
	})

	t.Run("original artifact absent", func(t *testing.T) {
		record, lease, readers, _, _ :=
			authoritySnapshotAdoptionTestState(t)
		readers.headArtifacts.loadAdoption = func(
			context.Context,
			uint64,
		) (authorityPhysicalAdoptionRow, authorityPoint, bool, error) {
			return authorityPhysicalAdoptionRow{},
				authorityPoint{},
				false,
				nil
		}
		err := finishAuthoritySnapshotLeaseAgainstRecord(
			context.Background(),
			record,
			lease,
			readers,
		)
		if !errors.Is(err, ErrInvalidDataset) {
			t.Fatalf("missing artifact error = %v", err)
		}
	})
}

func TestFinishAuthoritySnapshotAtBlockDualSelectors(t *testing.T) {
	t.Parallel()
	record, lease, readers, _, selectorSnapshots :=
		authoritySnapshotAdoptionTestState(t)
	lease.Mode = authoritySnapshotAtBlock
	lease.BlockHash = lease.QueryHead.Point.Hash
	lease.SelectedPublicationID = lease.Cutoff.PublicationID
	lease.SelectedPoint = lease.QueryHead.Point
	if err := finishAuthoritySnapshotLeaseAgainstRecord(
		context.Background(),
		record,
		lease,
		readers,
	); err != nil {
		t.Fatal(err)
	}
	if len(*selectorSnapshots) != 2 ||
		(*selectorSnapshots)[0] != lease.AuthorityEffective.EventSeq ||
		(*selectorSnapshots)[1] != record.Effective.EventSeq {
		t.Fatalf("selector ceilings = %v", *selectorSnapshots)
	}

	t.Run("original mismatch", func(t *testing.T) {
		record, lease, readers, _, _ :=
			authoritySnapshotAdoptionTestState(t)
		lease.Mode = authoritySnapshotAtBlock
		lease.BlockHash = lease.QueryHead.Point.Hash
		lease.SelectedPublicationID = lease.Cutoff.PublicationID
		lease.SelectedPoint = lease.QueryHead.Point
		readers.selectAtBlock = func(
			context.Context,
			authorityRecord,
			authorityHash,
		) (authoritySelection, error) {
			selection := authoritySelection{
				AuthorityEffective: lease.AuthorityEffective,
				QueryHead:          lease.QueryHead,
				Cutoff:             lease.Cutoff,
			}
			selection.QueryHead.EventSeq++
			return selection, nil
		}
		err := finishAuthoritySnapshotLeaseAgainstRecord(
			context.Background(),
			record,
			lease,
			readers,
		)
		if !errors.Is(err, ErrInvalidDataset) {
			t.Fatalf("original mismatch error = %v", err)
		}
	})

	t.Run("original multiple active", func(t *testing.T) {
		record, lease, readers, _, _ :=
			authoritySnapshotAdoptionTestState(t)
		lease.Mode = authoritySnapshotAtBlock
		lease.BlockHash = lease.QueryHead.Point.Hash
		lease.SelectedPublicationID = lease.Cutoff.PublicationID
		lease.SelectedPoint = lease.QueryHead.Point
		readers.selectAtBlock = func(
			context.Context,
			authorityRecord,
			authorityHash,
		) (authoritySelection, error) {
			return authoritySelection{}, ErrInvalidDataset
		}
		err := finishAuthoritySnapshotLeaseAgainstRecord(
			context.Background(),
			record,
			lease,
			readers,
		)
		if !errors.Is(err, ErrInvalidDataset) {
			t.Fatalf("multiple-active error = %v", err)
		}
	})

	t.Run("fresh re-adoption", func(t *testing.T) {
		record, lease, readers, _, _ :=
			authoritySnapshotAdoptionTestState(t)
		lease.Mode = authoritySnapshotAtBlock
		lease.BlockHash = lease.QueryHead.Point.Hash
		lease.SelectedPublicationID = lease.Cutoff.PublicationID
		lease.SelectedPoint = lease.QueryHead.Point
		calls := 0
		original := readers.selectAtBlock
		readers.selectAtBlock = func(
			ctx context.Context,
			projected authorityRecord,
			hash authorityHash,
		) (authoritySelection, error) {
			calls++
			selection, err := original(ctx, projected, hash)
			if calls == 2 {
				selection.QueryHead.EventSeq++
				selection.Cutoff.AdoptionEventSeq++
			}
			return selection, err
		}
		err := finishAuthoritySnapshotLeaseAgainstRecord(
			context.Background(),
			record,
			lease,
			readers,
		)
		if !errors.Is(err, ErrSnapshotUnavailable) ||
			errors.Is(err, ErrInvalidDataset) {
			t.Fatalf("fresh re-adoption error = %v", err)
		}
	})
}

func TestValidateAuthoritySnapshotZeroCutoffByHeadKind(t *testing.T) {
	t.Parallel()
	lease := authoritySnapshotLease{
		QueryHead: authorityHead{
			EventSeq: 7,
			Point:    authorityPoint{Hash: authorityFill32(0x71)},
		},
	}
	readers := authoritySelectionCutoffReaders{
		load: func(
			context.Context,
			uint64,
		) (authorityCutoff, error) {
			return authorityCutoff{}, nil
		},
		bind: func(
			context.Context,
			authorityRecord,
			authorityCutoff,
		) (authorityCutoffArtifacts, bool, error) {
			return authorityCutoffArtifacts{}, false, nil
		},
	}
	if _, found, err := validateAuthoritySnapshotCutoff(
		context.Background(),
		authorityRecord{},
		lease,
		authoritySnapshotHeadArtifacts{
			Kind: authoritySnapshotRollbackHead,
		},
		readers,
	); err != nil || found {
		t.Fatalf("rollback zero cutoff found=%t err=%v", found, err)
	}
	if _, _, err := validateAuthoritySnapshotCutoff(
		context.Background(),
		authorityRecord{},
		lease,
		authoritySnapshotHeadArtifacts{
			Kind: authoritySnapshotAdoptionHead,
		},
		readers,
	); !errors.Is(err, ErrInvalidDataset) {
		t.Fatalf("adoption zero cutoff error = %v", err)
	}
}

func authoritySnapshotRollbackTestState(
	t *testing.T,
) (
	authorityRecord,
	authoritySnapshotLease,
	authoritySnapshotFinishReaders,
	authorityPoint,
	*[]uint64,
) {
	t.Helper()
	group := authorityFill16(0x42)
	evidence := []authorityObservationRow{
		authorityRollbackArtifactEvidenceRow(
			t, group, 1, "operator-a", "relay-a", "agreed",
		),
		authorityRollbackArtifactEvidenceRow(
			t, group, 2, "operator-b", "relay-b", "agreed",
		),
		authorityRollbackArtifactEvidenceRow(
			t, group, 3, "operator-c", "relay-c", "unavailable",
		),
	}
	commitment, err := canonicalAuthorityEvidenceCommitment(
		evidence,
		group,
		1,
	)
	if err != nil {
		t.Fatal(err)
	}
	header := authorityRollbackArtifactTestRow(commitment.Digest)
	header.EvidenceCount = commitment.Count
	decoded, target, oldTip, proof, found, err :=
		decodeAuthorityPhysicalRollbackRows(
			[]authorityPhysicalRollbackRow{header},
			header.EventSeq,
		)
	if err != nil || !found {
		t.Fatalf("rollback fixture decode found=%t err=%v", found, err)
	}
	record, _, _, _, _ := authoritySnapshotAdoptionTestState(t)
	record.Effective = authorityHead{
		EventSeq: 2,
		Point: authorityPoint{
			Slot:        120,
			Hash:        authorityFill32(0x77),
			BlockNumber: 12,
		},
	}
	pinned := authorityHead{
		EventSeq: decoded.EventSeq,
		Point:    target,
	}
	lease := authoritySnapshotLease{
		Identity:             authoritySnapshotIdentityFromRecord(record),
		VisibilityGeneration: record.VisibilityGeneration,
		AuthorityEffective:   pinned,
		QueryHead:            pinned,
		Mode:                 authoritySnapshotAtTip,
	}
	activitySnapshots := make([]uint64, 0, 2)
	headReaders := defaultAuthorityArtifactTestReaders()
	headReaders.loadRollback = func(
		_ context.Context,
		eventSeq uint64,
	) (
		authorityPhysicalRollbackRow,
		authorityPoint,
		authorityPoint,
		authorityHash,
		bool,
		error,
	) {
		if eventSeq != decoded.EventSeq {
			return authorityPhysicalRollbackRow{}, authorityPoint{},
				authorityPoint{}, authorityHash{}, false, nil
		}
		return decoded, target, oldTip, proof, true, nil
	}
	headReaders.loadEvidence = func(
		_ context.Context,
		checkID [16]byte,
	) ([]authorityObservationRow, error) {
		if checkID != authorityUUID(decoded.CheckID) {
			t.Fatalf("historical check ID = %x", checkID)
		}
		return evidence, nil
	}
	headReaders.validateInvalidations = func(
		context.Context,
		authorityRecord,
		authorityPhysicalRollbackRow,
		authorityPoint,
		authorityPoint,
	) (bool, error) {
		return true, nil
	}
	readers := authoritySnapshotFinishReaders{
		loadEvidence: func(
			context.Context,
			[16]byte,
		) ([]authorityObservationRow, error) {
			return nil, nil
		},
		validateArtifacts: func(context.Context, authorityRecord) error {
			return nil
		},
		headArtifacts: headReaders,
		cutoff: authoritySelectionCutoffReaders{
			load: func(
				context.Context,
				uint64,
			) (authorityCutoff, error) {
				return authorityCutoff{}, nil
			},
			bind: func(
				context.Context,
				authorityRecord,
				authorityCutoff,
			) (authorityCutoffArtifacts, bool, error) {
				return authorityCutoffArtifacts{}, false, nil
			},
		},
		loadActive: func(
			_ context.Context,
			_ authorityRecord,
			snapshot uint64,
			hash authorityHash,
		) (authorityActiveBlock, bool, error) {
			activitySnapshots = append(activitySnapshots, snapshot)
			if hash != target.Hash {
				t.Fatalf("rollback target hash = %x", hash)
			}
			return authorityActiveBlock{
				PublicationID: 88,
				Point:         target,
			}, true, nil
		},
		selectAtBlock: func(
			context.Context,
			authorityRecord,
			authorityHash,
		) (authoritySelection, error) {
			return authoritySelection{}, nil
		},
	}
	return record, lease, readers, target, &activitySnapshots
}

func TestFinishHistoricalRollbackAfterLaterAdoption(t *testing.T) {
	t.Parallel()
	record, lease, readers, target, activitySnapshots :=
		authoritySnapshotRollbackTestState(t)
	if record.LastAgreed != nil ||
		record.LastAgreedEvidence != nil ||
		record.Effective == lease.AuthorityEffective {
		t.Fatal("fixture did not isolate historical rollback from fresh LastAgreed")
	}
	if err := finishAuthoritySnapshotLeaseAgainstRecord(
		context.Background(),
		record,
		lease,
		readers,
	); err != nil {
		t.Fatalf("historical rollback after later adoption: %v", err)
	}
	if len(*activitySnapshots) != 2 ||
		(*activitySnapshots)[0] != lease.AuthorityEffective.EventSeq ||
		(*activitySnapshots)[1] != record.Effective.EventSeq {
		t.Fatalf("rollback activity ceilings = %v", *activitySnapshots)
	}

	t.Run("partial Start boundary", func(t *testing.T) {
		record, lease, readers, _, _ :=
			authoritySnapshotRollbackTestState(t)
		record.Start = target
		record.CompleteHistory = false
		lease.Identity = authoritySnapshotIdentityFromRecord(record)
		readers.loadActive = func(
			context.Context,
			authorityRecord,
			uint64,
			authorityHash,
		) (authorityActiveBlock, bool, error) {
			t.Fatal("partial Start rollback target loaded an active block")
			return authorityActiveBlock{}, false, nil
		}
		if err := finishAuthoritySnapshotLeaseAgainstRecord(
			context.Background(),
			record,
			lease,
			readers,
		); err != nil {
			t.Fatalf("partial Start rollback boundary: %v", err)
		}
	})

	t.Run("evidence mutation", func(t *testing.T) {
		record, lease, readers, _, _ :=
			authoritySnapshotRollbackTestState(t)
		original := readers.headArtifacts.loadEvidence
		readers.headArtifacts.loadEvidence = func(
			ctx context.Context,
			checkID [16]byte,
		) ([]authorityObservationRow, error) {
			rows, err := original(ctx, checkID)
			mutated := append([]authorityObservationRow(nil), rows...)
			mutated[0].Digest[0]++
			return mutated, err
		}
		err := finishAuthoritySnapshotLeaseAgainstRecord(
			context.Background(),
			record,
			lease,
			readers,
		)
		if !errors.Is(err, ErrInvalidDataset) {
			t.Fatalf("evidence mutation error = %v", err)
		}
	})

	t.Run("evidence dependency", func(t *testing.T) {
		record, lease, readers, _, _ :=
			authoritySnapshotRollbackTestState(t)
		infrastructure := errors.New("historical evidence query failed")
		readers.headArtifacts.loadEvidence = func(
			context.Context,
			[16]byte,
		) ([]authorityObservationRow, error) {
			return nil, infrastructure
		}
		err := finishAuthoritySnapshotLeaseAgainstRecord(
			context.Background(),
			record,
			lease,
			readers,
		)
		if !errors.Is(err, infrastructure) ||
			errors.Is(err, ErrInvalidDataset) {
			t.Fatalf("evidence dependency error = %v", err)
		}
	})
}

func TestFinishHistoricalRollbackToOrigin(t *testing.T) {
	t.Parallel()
	group := authorityFill16(0x42)
	evidence := []authorityObservationRow{
		authorityRollbackArtifactEvidenceRow(
			t, group, 1, "operator-a", "relay-a", "agreed",
		),
		authorityRollbackArtifactEvidenceRow(
			t, group, 2, "operator-b", "relay-b", "agreed",
		),
		authorityRollbackArtifactEvidenceRow(
			t, group, 3, "operator-c", "relay-c", "unavailable",
		),
	}
	for index := range evidence {
		observation := &evidence[index].Observation
		observation.CheckedPointOrigin = true
		observation.CheckedPointSlot = nil
		observation.CheckedPointHash = nil
		observation.CheckedBlockNumber = nil
		observation.CheckedPointIsByronEBB = false
		observation.CheckpointSlot = nil
		observation.CheckpointHash = nil
		observation.CheckpointBlockNumber = nil
		observation.CheckpointIsByronEBB = nil
		if err := finalizeAuthorityObservationIdentity(observation); err != nil {
			t.Fatal(err)
		}
		payload, err := canonicalAuthorityObservationPayload(*observation)
		if err != nil {
			t.Fatal(err)
		}
		evidence[index].Digest = authorityHash(
			sha256.Sum256([]byte(payload)),
		)
	}
	commitment, err := canonicalAuthorityEvidenceCommitment(
		evidence,
		group,
		1,
	)
	if err != nil {
		t.Fatal(err)
	}
	header := authorityRollbackArtifactTestRow(commitment.Digest)
	header.ToOrigin = true
	header.ToSlot = nil
	header.ToHash = nil
	header.ToBlockNumber = nil
	header.ToIsByronEBB = false
	header.EvidenceCount = commitment.Count
	decoded, target, oldTip, proof, found, err :=
		decodeAuthorityPhysicalRollbackRows(
			[]authorityPhysicalRollbackRow{header},
			header.EventSeq,
		)
	if err != nil || !found || !target.Origin {
		t.Fatalf(
			"Origin rollback decode target=%+v found=%t err=%v",
			target,
			found,
			err,
		)
	}
	record, _, _, _, _ := authoritySnapshotAdoptionTestState(t)
	record.Start = authorityPoint{Origin: true}
	record.Effective = authorityHead{
		EventSeq: 2,
		Point:    authorityPoint{Hash: authorityFill32(0x78)},
	}
	pinned := authorityHead{
		EventSeq: decoded.EventSeq,
		Point:    target,
	}
	lease := authoritySnapshotLease{
		Identity:             authoritySnapshotIdentityFromRecord(record),
		VisibilityGeneration: record.VisibilityGeneration,
		AuthorityEffective:   pinned,
		QueryHead:            pinned,
		Mode:                 authoritySnapshotAtTip,
	}
	headReaders := defaultAuthorityArtifactTestReaders()
	headReaders.loadRollback = func(
		context.Context,
		uint64,
	) (
		authorityPhysicalRollbackRow,
		authorityPoint,
		authorityPoint,
		authorityHash,
		bool,
		error,
	) {
		return decoded, target, oldTip, proof, true, nil
	}
	headReaders.loadEvidence = func(
		context.Context,
		[16]byte,
	) ([]authorityObservationRow, error) {
		return evidence, nil
	}
	headReaders.validateInvalidations = func(
		context.Context,
		authorityRecord,
		authorityPhysicalRollbackRow,
		authorityPoint,
		authorityPoint,
	) (bool, error) {
		return true, nil
	}
	readers := authoritySnapshotFinishReaders{
		headArtifacts: headReaders,
		cutoff: authoritySelectionCutoffReaders{
			load: func(
				context.Context,
				uint64,
			) (authorityCutoff, error) {
				return authorityCutoff{}, nil
			},
			bind: func(
				context.Context,
				authorityRecord,
				authorityCutoff,
			) (authorityCutoffArtifacts, bool, error) {
				return authorityCutoffArtifacts{}, false, nil
			},
		},
		loadActive: func(
			context.Context,
			authorityRecord,
			uint64,
			authorityHash,
		) (authorityActiveBlock, bool, error) {
			t.Fatal("rollback-to-Origin loaded an active block")
			return authorityActiveBlock{}, false, nil
		},
		selectAtBlock: func(
			context.Context,
			authorityRecord,
			authorityHash,
		) (authoritySelection, error) {
			return authoritySelection{}, nil
		},
	}
	if err := finishAuthoritySnapshotLeaseAgainstRecord(
		context.Background(),
		record,
		lease,
		readers,
	); err != nil {
		t.Fatalf("rollback-to-Origin rejected: %v", err)
	}

	readers.headArtifacts.validateInvalidations = func(
		context.Context,
		authorityRecord,
		authorityPhysicalRollbackRow,
		authorityPoint,
		authorityPoint,
	) (bool, error) {
		return false, nil
	}
	if err := finishAuthoritySnapshotLeaseAgainstRecord(
		context.Background(),
		record,
		lease,
		readers,
	); !errors.Is(err, ErrInvalidDataset) {
		t.Fatalf("incomplete Origin invalidations error = %v", err)
	}
}

func TestFinishAuthoritySnapshotBoundaryActivity(t *testing.T) {
	t.Parallel()
	record, _, readers, _, _ := authoritySnapshotAdoptionTestState(t)
	lease := authoritySnapshotLease{
		Identity:             authoritySnapshotIdentityFromRecord(record),
		VisibilityGeneration: record.VisibilityGeneration,
		AuthorityEffective: authorityHead{
			Point: record.Start,
		},
		QueryHead: authorityHead{
			Point: record.Start,
		},
		Mode: authoritySnapshotAtTip,
	}
	if err := finishAuthoritySnapshotLeaseAgainstRecord(
		context.Background(),
		record,
		lease,
		readers,
	); !errors.Is(err, ErrInvalidDataset) {
		t.Fatalf("complete-history event-zero Finish error = %v", err)
	}

	record.Start = authorityPoint{
		Slot:        9,
		Hash:        authorityFill32(0x41),
		BlockNumber: 3,
	}
	record.CompleteHistory = false
	lease.Identity = authoritySnapshotIdentityFromRecord(record)
	lease.AuthorityEffective.Point = record.Start
	lease.QueryHead.Point = record.Start
	readers.cutoff.load = func(
		context.Context,
		uint64,
	) (authorityCutoff, error) {
		return authorityCutoff{}, nil
	}
	readers.cutoff.bind = func(
		context.Context,
		authorityRecord,
		authorityCutoff,
	) (authorityCutoffArtifacts, bool, error) {
		return authorityCutoffArtifacts{}, false, nil
	}
	readers.loadActive = func(
		context.Context,
		authorityRecord,
		uint64,
		authorityHash,
	) (authorityActiveBlock, bool, error) {
		t.Fatal("event-zero Origin boundary loaded an active block")
		return authorityActiveBlock{}, false, nil
	}
	if err := finishAuthoritySnapshotLeaseAgainstRecord(
		context.Background(),
		record,
		lease,
		readers,
	); err != nil {
		t.Fatalf("partial-history event-zero boundary rejected: %v", err)
	}
	if err := validateAuthoritySnapshotHeadActivity(
		context.Background(),
		record,
		record.Effective.EventSeq,
		authoritySnapshotHeadArtifacts{
			Kind:       authoritySnapshotRollbackHead,
			RollbackTo: authorityPoint{Origin: true},
		},
		readers,
	); err != nil {
		t.Fatalf("rollback-to-Origin activity rejected: %v", err)
	}
}

func TestAcquireAuthoritySnapshotAbsenceUnservableAndSelectorOutput(
	t *testing.T,
) {
	t.Parallel()
	baseReaders := func(
		attempts []authorityHeadAttempt,
		selectTip func(
			context.Context,
			authorityRecord,
		) (authoritySelection, error),
	) authoritySnapshotAcquireReaders {
		index := 0
		return authoritySnapshotAcquireReaders{
			readHead: func(context.Context) (authorityHeadAttempt, error) {
				attempt := attempts[index]
				index++
				return attempt, nil
			},
			loadEvidence: func(
				context.Context,
				[16]byte,
			) ([]authorityObservationRow, error) {
				return nil, nil
			},
			validateArtifacts: func(context.Context, authorityRecord) error {
				return nil
			},
			selectAtTip: selectTip,
		}
	}
	t.Run("absent", func(t *testing.T) {
		observation := authorityHeadObservation{}
		_, err := acquireAuthoritySnapshotLeaseWithReaders(
			context.Background(),
			authoritySnapshotRequest{Mode: authoritySnapshotAtTip},
			baseReaders(
				[]authorityHeadAttempt{
					{Observation: observation},
					{Observation: observation},
				},
				func(
					context.Context,
					authorityRecord,
				) (authoritySelection, error) {
					t.Fatal("absent authority ran selector")
					return authoritySelection{}, nil
				},
			),
		)
		if !errors.Is(err, ErrSnapshotUnavailable) {
			t.Fatalf("absent acquire error = %v", err)
		}
	})

	record, _, _, _, _ := authoritySnapshotAdoptionTestState(t)
	observation := authorityHeadObservation{Present: true, Revision: 1}
	t.Run("unservable", func(t *testing.T) {
		unservable := record
		unservable.Servable = false
		_, err := acquireAuthoritySnapshotLeaseWithReaders(
			context.Background(),
			authoritySnapshotRequest{Mode: authoritySnapshotAtTip},
			baseReaders(
				[]authorityHeadAttempt{
					{
						Observation: observation,
						Latest:      unservable,
						Found:       true,
					},
					{
						Observation: observation,
						Latest:      unservable,
						Found:       true,
					},
				},
				func(
					context.Context,
					authorityRecord,
				) (authoritySelection, error) {
					t.Fatal("unservable authority ran selector")
					return authoritySelection{}, nil
				},
			),
		)
		if !errors.Is(err, ErrSnapshotUnavailable) {
			t.Fatalf("unservable acquire error = %v", err)
		}
	})

	t.Run("selector Effective", func(t *testing.T) {
		_, err := acquireAuthoritySnapshotLeaseWithReaders(
			context.Background(),
			authoritySnapshotRequest{Mode: authoritySnapshotAtTip},
			baseReaders(
				[]authorityHeadAttempt{
					{
						Observation: observation,
						Latest:      record,
						Found:       true,
					},
					{
						Observation: observation,
						Latest:      record,
						Found:       true,
					},
				},
				func(
					context.Context,
					authorityRecord,
				) (authoritySelection, error) {
					return authoritySelection{
						AuthorityEffective: authorityHead{
							EventSeq: record.Effective.EventSeq + 1,
							Point:    record.Effective.Point,
						},
						QueryHead: record.Effective,
					}, nil
				},
			),
		)
		if !errors.Is(err, ErrInvalidDataset) {
			t.Fatalf("selector Effective error = %v", err)
		}
	})
}

func TestValidateAuthoritySnapshotRecordEvidenceAndTaxonomy(t *testing.T) {
	t.Parallel()
	currentID := authorityFill16(0x41)
	lastID := authorityFill16(0x42)
	tests := []struct {
		name  string
		last  [16]byte
		calls int
	}{
		{name: "reuse same ID", last: currentID, calls: 1},
		{name: "load distinct ID", last: lastID, calls: 2},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			record := authorityRecord{
				CheckID: &currentID,
				LastAgreedEvidence: &authorityEvidenceReference{
					CheckID: test.last,
				},
			}
			calls := 0
			err := validateAuthoritySnapshotRecord(
				context.Background(),
				record,
				func(
					context.Context,
					[16]byte,
				) ([]authorityObservationRow, error) {
					calls++
					return nil, nil
				},
				func(context.Context, authorityRecord) error {
					t.Fatal("semantic evidence failure reached artifacts")
					return nil
				},
			)
			if calls != test.calls ||
				!errors.Is(err, ErrInvalidDataset) {
				t.Fatalf("loads=%d error=%v", calls, err)
			}
		})
	}

	failures := []error{
		errors.New("evidence query failed"),
		&ResourceLimitError{
			Phase: "evidence",
			Cause: errors.New("evidence resource limit"),
		},
	}
	for _, failure := range failures {
		failure := failure
		t.Run(failure.Error(), func(t *testing.T) {
			record := authorityRecord{CheckID: &currentID}
			err := validateAuthoritySnapshotRecord(
				context.Background(),
				record,
				func(
					context.Context,
					[16]byte,
				) ([]authorityObservationRow, error) {
					return nil, failure
				},
				func(context.Context, authorityRecord) error {
					return nil
				},
			)
			if !errors.Is(err, failure) ||
				errors.Is(err, ErrInvalidDataset) {
				t.Fatalf("evidence dependency error = %v", err)
			}
		})
	}

	artifactFailure := errors.New("artifact query failed")
	if err := validateAuthoritySnapshotRecord(
		context.Background(),
		authorityRecord{},
		func(
			context.Context,
			[16]byte,
		) ([]authorityObservationRow, error) {
			return nil, nil
		},
		func(context.Context, authorityRecord) error {
			return artifactFailure
		},
	); !errors.Is(err, artifactFailure) ||
		errors.Is(err, ErrInvalidDataset) {
		t.Fatalf("artifact dependency error = %v", err)
	}
}

func TestFinishAuthoritySnapshotStableRetryAndTaxonomy(t *testing.T) {
	t.Parallel()
	record, lease, base, _, _ :=
		authoritySnapshotAdoptionTestState(t)
	stableObservation := authorityHeadObservation{Present: true, Revision: 2}
	t.Run("retry", func(t *testing.T) {
		readers := base
		changedObservation := authorityHeadObservation{
			Present:  true,
			Revision: 1,
		}
		stale := record
		stale.VisibilityGeneration++
		attempts := []authorityHeadAttempt{
			{
				Observation: changedObservation,
				Latest:      stale,
				Found:       true,
			},
			{
				Observation: stableObservation,
				Latest:      record,
				Found:       true,
			},
			{
				Observation: stableObservation,
				Latest:      record,
				Found:       true,
			},
			{
				Observation: stableObservation,
				Latest:      record,
				Found:       true,
			},
		}
		index := 0
		validations := 0
		readers.readHead = func(
			context.Context,
		) (authorityHeadAttempt, error) {
			attempt := attempts[index]
			index++
			return attempt, nil
		}
		readers.validateArtifacts = func(
			context.Context,
			authorityRecord,
		) error {
			validations++
			return nil
		}
		if err := finishAuthoritySnapshotLeaseWithReaders(
			context.Background(),
			lease,
			readers,
		); err != nil {
			t.Fatalf("stable Finish retry: %v", err)
		}
		if index != 4 || validations != 2 {
			t.Fatalf("reads=%d validations=%d", index, validations)
		}
	})

	t.Run("absent", func(t *testing.T) {
		readers := base
		index := 0
		attempts := []authorityHeadAttempt{
			{Observation: stableObservation},
			{Observation: stableObservation},
		}
		readers.readHead = func(
			context.Context,
		) (authorityHeadAttempt, error) {
			attempt := attempts[index]
			index++
			return attempt, nil
		}
		err := finishAuthoritySnapshotLeaseWithReaders(
			context.Background(),
			lease,
			readers,
		)
		if !errors.Is(err, ErrSnapshotUnavailable) {
			t.Fatalf("absent Finish error = %v", err)
		}
	})

	for _, failure := range []error{
		errors.New("Finish artifact query failed"),
		&ResourceLimitError{
			Phase: "Finish",
			Cause: errors.New("Finish resource limit"),
		},
	} {
		failure := failure
		t.Run(failure.Error(), func(t *testing.T) {
			readers := base
			index := 0
			attempts := []authorityHeadAttempt{
				{
					Observation: stableObservation,
					Latest:      record,
					Found:       true,
				},
				{
					Observation: stableObservation,
					Latest:      record,
					Found:       true,
				},
			}
			readers.readHead = func(
				context.Context,
			) (authorityHeadAttempt, error) {
				attempt := attempts[index]
				index++
				return attempt, nil
			}
			readers.validateArtifacts = func(
				context.Context,
				authorityRecord,
			) error {
				return failure
			}
			err := finishAuthoritySnapshotLeaseWithReaders(
				context.Background(),
				lease,
				readers,
			)
			if !errors.Is(err, failure) ||
				errors.Is(err, ErrInvalidDataset) {
				t.Fatalf("Finish dependency error = %v", err)
			}
		})
	}

	t.Run("context", func(t *testing.T) {
		readers := base
		called := false
		readers.readHead = func(
			context.Context,
		) (authorityHeadAttempt, error) {
			called = true
			return authorityHeadAttempt{}, nil
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := finishAuthoritySnapshotLeaseWithReaders(
			ctx,
			lease,
			readers,
		)
		if !errors.Is(err, context.Canceled) ||
			errors.Is(err, ErrInvalidDataset) ||
			called {
			t.Fatalf("context called=%t error=%v", called, err)
		}
	})
}
