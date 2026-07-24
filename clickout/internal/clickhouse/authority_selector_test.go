package clickhouse

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestAuthorityCutoffCandidateSQLIsBoundedAndUnqualified(t *testing.T) {
	t.Parallel()
	for _, fragment := range []string{
		"SELECT event_seq, publication_id",
		"FROM chain_events",
		"PREWHERE event_kind = 'adoption'",
		"AND event_seq <= ?",
		"ORDER BY event_kind, event_seq DESC, publication_id DESC",
		"LIMIT 9",
	} {
		if !strings.Contains(authorityCutoffCandidateSQL, fragment) {
			t.Fatalf("cutoff SQL lacks %q:\n%s", fragment, authorityCutoffCandidateSQL)
		}
	}
	for _, forbidden := range []string{"clicksync.", "max(", "argMax(", "count("} {
		if strings.Contains(authorityCutoffCandidateSQL, forbidden) {
			t.Fatalf("cutoff SQL contains forbidden %q", forbidden)
		}
	}
}

func TestDecodeAuthorityCutoffCandidatesReplayBounds(t *testing.T) {
	t.Parallel()
	row := authorityCutoffCandidateRow{EventSeq: 11, PublicationID: 29}
	got, err := decodeAuthorityCutoffCandidates(nil, 20)
	if err != nil || got != (authorityCutoff{}) {
		t.Fatalf("empty cutoff = %+v, %v", got, err)
	}
	got, err = decodeAuthorityCutoffCandidates(
		[]authorityCutoffCandidateRow{row},
		20,
	)
	if err != nil ||
		got != (authorityCutoff{AdoptionEventSeq: 11, PublicationID: 29}) {
		t.Fatalf("single cutoff = %+v, %v", got, err)
	}

	eightWithLowerSentinel := make([]authorityCutoffCandidateRow, 8, 9)
	for index := range eightWithLowerSentinel {
		eightWithLowerSentinel[index] = row
	}
	eightWithLowerSentinel = append(
		eightWithLowerSentinel,
		authorityCutoffCandidateRow{EventSeq: 10, PublicationID: 31},
	)
	got, err = decodeAuthorityCutoffCandidates(eightWithLowerSentinel, 20)
	if err != nil ||
		got != (authorityCutoff{AdoptionEventSeq: 11, PublicationID: 29}) {
		t.Fatalf("eight replays plus lower sentinel = %+v, %v", got, err)
	}

	nine := make([]authorityCutoffCandidateRow, 9)
	for index := range nine {
		nine[index] = row
	}
	if _, err := decodeAuthorityCutoffCandidates(nine, 20); !errors.Is(err, ErrInvalidDataset) {
		t.Fatalf("nine top-event physical rows error = %v", err)
	}

	conflict := []authorityCutoffCandidateRow{
		row,
		{EventSeq: row.EventSeq, PublicationID: row.PublicationID - 1},
	}
	if _, err := decodeAuthorityCutoffCandidates(conflict, 20); !errors.Is(err, ErrInvalidDataset) {
		t.Fatalf("same-event conflicting publication error = %v", err)
	}

	returnToTop := []authorityCutoffCandidateRow{
		row,
		{EventSeq: row.EventSeq - 2, PublicationID: 40},
		row,
	}
	if _, err := decodeAuthorityCutoffCandidates(returnToTop, 20); !errors.Is(err, ErrInvalidDataset) {
		t.Fatalf("return to top event error = %v", err)
	}

	outOfOrder := []authorityCutoffCandidateRow{
		row,
		{EventSeq: row.EventSeq - 2, PublicationID: 40},
		{EventSeq: row.EventSeq - 1, PublicationID: 39},
	}
	if _, err := decodeAuthorityCutoffCandidates(outOfOrder, 20); !errors.Is(err, ErrInvalidDataset) {
		t.Fatalf("out-of-order lower-event page error = %v", err)
	}
}

func authorityCutoffBindingFixture() (
	authorityCutoff,
	authorityPhysicalAdoptionRow,
	authorityPoint,
	authorityPhysicalBlockRow,
) {
	point := authorityPoint{
		Slot:        101,
		Hash:        authorityFill32(0x71),
		BlockNumber: 12,
	}
	writer := uuid.UUID(authorityFill16(0x41))
	at := time.Date(2026, time.July, 23, 1, 2, 3, 456000000, time.UTC)
	adoption := authorityPhysicalAdoptionRow{
		EventSeq:      11,
		PublicationID: 29,
		Active:        true,
		BlockHash:     string(point.Hash[:]),
		Slot:          point.Slot,
		BlockNumber:   point.BlockNumber,
		WriterID:      writer,
		RecordedAt:    at,
	}
	facts := authorityFill32(0x72)
	block := authorityPhysicalBlockRow{
		PublicationID: adoption.PublicationID,
		BlockHash:     adoption.BlockHash,
		Era:           "Babbage",
		BlockType:     1,
		FactsDigest:   string(facts[:]),
		WriterID:      writer,
		InsertedAt:    at,
	}
	return authorityCutoff{
		AdoptionEventSeq: adoption.EventSeq,
		PublicationID:    adoption.PublicationID,
	}, adoption, point, block
}

func TestBindAuthorityCutoffArtifactsExactMappingAllowsInactiveAtQueryHead(
	t *testing.T,
) {
	t.Parallel()
	cutoff, adoption, point, block := authorityCutoffBindingFixture()
	artifact, found, err := bindAuthorityCutoffArtifacts(
		context.Background(),
		authorityRecord{},
		cutoff,
		authorityCutoffArtifactReaders{
			loadAdoption: func(
				_ context.Context,
				eventSeq uint64,
			) (authorityPhysicalAdoptionRow, authorityPoint, bool, error) {
				if eventSeq != cutoff.AdoptionEventSeq {
					t.Fatalf("adoption event = %d", eventSeq)
				}
				return adoption, point, true, nil
			},
			loadBlock: func(
				_ context.Context,
				publicationID uint64,
			) (authorityPhysicalBlockRow, authorityPoint, bool, error) {
				if publicationID != cutoff.PublicationID {
					t.Fatalf("block publication = %d", publicationID)
				}
				return block, point, true, nil
			},
		},
	)
	if err != nil || !found ||
		artifact.Adoption.EventSeq != cutoff.AdoptionEventSeq ||
		artifact.Block.PublicationID != cutoff.PublicationID {
		t.Fatalf("exact inactive-capable cutoff binding = %+v, %t, %v", artifact, found, err)
	}
}

func TestBindAuthorityCutoffArtifactsRejectsMappingMismatch(t *testing.T) {
	t.Parallel()
	cutoff, adoption, point, block := authorityCutoffBindingFixture()
	adoption.PublicationID++
	_, _, err := bindAuthorityCutoffArtifacts(
		context.Background(),
		authorityRecord{},
		cutoff,
		authorityCutoffArtifactReaders{
			loadAdoption: func(
				context.Context,
				uint64,
			) (authorityPhysicalAdoptionRow, authorityPoint, bool, error) {
				return adoption, point, true, nil
			},
			loadBlock: func(
				context.Context,
				uint64,
			) (authorityPhysicalBlockRow, authorityPoint, bool, error) {
				return block, point, true, nil
			},
		},
	)
	if !errors.Is(err, ErrInvalidDataset) {
		t.Fatalf("cutoff/adoption mapping mismatch error = %v", err)
	}
}

func TestBindAuthorityZeroCutoffReadsNoArtifacts(t *testing.T) {
	t.Parallel()
	called := false
	_, found, err := bindAuthorityCutoffArtifacts(
		context.Background(),
		authorityRecord{},
		authorityCutoff{},
		authorityCutoffArtifactReaders{
			loadAdoption: func(
				context.Context,
				uint64,
			) (authorityPhysicalAdoptionRow, authorityPoint, bool, error) {
				called = true
				return authorityPhysicalAdoptionRow{}, authorityPoint{}, false, nil
			},
			loadBlock: func(
				context.Context,
				uint64,
			) (authorityPhysicalBlockRow, authorityPoint, bool, error) {
				called = true
				return authorityPhysicalBlockRow{}, authorityPoint{}, false, nil
			},
		},
	)
	if err != nil || found || called {
		t.Fatalf("zero cutoff found=%t called=%t err=%v", found, called, err)
	}
}

func TestBindAuthorityCutoffRejectsMaliciousHistoricalSynthetic(t *testing.T) {
	t.Parallel()
	genesisHash := authorityFill32(0x81)
	point := authorityPoint{
		Hash: genesisHash,
	}
	writer := uuid.UUID(authorityFill16(0x42))
	at := time.Date(2026, time.July, 23, 1, 2, 3, 456000000, time.UTC)
	adoption := authorityPhysicalAdoptionRow{
		EventSeq:      1,
		PublicationID: 1,
		Active:        true,
		BlockHash:     string(genesisHash[:]),
		WriterID:      writer,
		RecordedAt:    at,
	}
	facts := authorityFill32(0x82)
	block := authorityPhysicalBlockRow{
		PublicationID: 1,
		BlockHash:     string(genesisHash[:]),
		Era:           "Byron",
		BlockType:     -1,
		Synthetic:     true,
		FactsDigest:   string(facts[:]),
		WriterID:      writer,
		InsertedAt:    at,
	}
	valid := authorityRecord{
		Start:           authorityPoint{Origin: true},
		GenesisSeeded:   true,
		CompleteHistory: true,
		ByronGenesisID:  genesisHash,
	}
	readers := authorityCutoffArtifactReaders{
		loadAdoption: func(
			context.Context,
			uint64,
		) (authorityPhysicalAdoptionRow, authorityPoint, bool, error) {
			return adoption, point, true, nil
		},
		loadBlock: func(
			context.Context,
			uint64,
		) (authorityPhysicalBlockRow, authorityPoint, bool, error) {
			return block, point, true, nil
		},
	}
	cutoff := authorityCutoff{AdoptionEventSeq: 1, PublicationID: 1}
	if _, found, err := bindAuthorityCutoffArtifacts(
		context.Background(),
		valid,
		cutoff,
		readers,
	); err != nil || !found {
		t.Fatalf("official historical synthetic cutoff rejected: found=%t err=%v", found, err)
	}
	malicious := valid
	malicious.ByronGenesisID = authorityFill32(0x83)
	if _, _, err := bindAuthorityCutoffArtifacts(
		context.Background(),
		malicious,
		cutoff,
		readers,
	); err == nil {
		t.Fatal("historical synthetic cutoff with foreign genesis hash was accepted")
	}
}

func TestSelectAuthorityAtTipUsesServableEffectiveAndItsCutoff(t *testing.T) {
	t.Parallel()
	effective := authorityHead{
		EventSeq: 31,
		Point: authorityPoint{
			Slot:        300,
			Hash:        authorityFill32(0x91),
			BlockNumber: 30,
		},
	}
	record := authorityRecord{Servable: true, Effective: effective}
	expectedCutoff := authorityCutoff{AdoptionEventSeq: 29, PublicationID: 41}
	selection, err := selectAuthorityAtTipWithReaders(
		context.Background(),
		record,
		authoritySelectionCutoffReaders{
			load: func(_ context.Context, eventSeq uint64) (authorityCutoff, error) {
				if eventSeq != effective.EventSeq {
					t.Fatalf("cutoff ceiling = %d, want Effective %d", eventSeq, effective.EventSeq)
				}
				return expectedCutoff, nil
			},
			bind: func(
				context.Context,
				authorityRecord,
				authorityCutoff,
			) (authorityCutoffArtifacts, bool, error) {
				return authorityCutoffArtifacts{}, true, nil
			},
		},
	)
	if err != nil ||
		selection.AuthorityEffective != effective ||
		selection.QueryHead != effective ||
		selection.Cutoff != expectedCutoff {
		t.Fatalf("AtTip selection = %+v, %v", selection, err)
	}
}

func TestSelectAuthorityAtTipRejectsUnservableAuthority(t *testing.T) {
	t.Parallel()
	_, err := selectAuthorityAtTipWithReaders(
		context.Background(),
		authorityRecord{
			TrustStatus: "checking",
			TrustReason: "no agreed floor",
		},
		authoritySelectionCutoffReaders{
			load: func(context.Context, uint64) (authorityCutoff, error) {
				t.Fatal("unservable AtTip read a cutoff")
				return authorityCutoff{}, nil
			},
		},
	)
	if !errors.Is(err, ErrSnapshotUnavailable) {
		t.Fatalf("unservable AtTip error = %v", err)
	}
}

func authorityAtBlockTestReaders(
	t *testing.T,
	record authorityRecord,
	hash authorityHash,
	publications []uint64,
	activeAt map[uint64]authorityPublicationLifecycleSummary,
	cutoff authorityCutoff,
) authorityAtBlockReaders {
	t.Helper()
	return authorityAtBlockReaders{
		nextCandidate: func(
			_ context.Context,
			gotHash authorityHash,
			cursorSet bool,
			cursor uint64,
		) (uint64, bool, error) {
			if gotHash != hash {
				t.Fatalf("candidate hash = %x, want %x", gotHash, hash)
			}
			for _, publicationID := range publications {
				if !cursorSet || publicationID > cursor {
					return publicationID, true, nil
				}
			}
			return 0, false, nil
		},
		loadBlock: func(
			_ context.Context,
			publicationID uint64,
		) (authorityPhysicalBlockRow, authorityPoint, bool, error) {
			return authorityPhysicalBlockRow{
					PublicationID: publicationID,
					BlockHash:     string(hash[:]),
				},
				authorityPoint{Hash: hash},
				true,
				nil
		},
		lifecycle: func(
			_ context.Context,
			_ authorityRecord,
			snapshot uint64,
			block authorityPhysicalBlockRow,
			_ authorityPoint,
		) (authorityPublicationLifecycleSummary, error) {
			if snapshot != record.Effective.EventSeq {
				t.Fatalf(
					"lifecycle snapshot = %d, want Effective %d",
					snapshot,
					record.Effective.EventSeq,
				)
			}
			return activeAt[block.PublicationID], nil
		},
		cutoff: authoritySelectionCutoffReaders{
			load: func(_ context.Context, eventSeq uint64) (authorityCutoff, error) {
				if eventSeq != cutoff.AdoptionEventSeq && cutoff != (authorityCutoff{}) {
					t.Fatalf("cutoff query event = %d, want %d", eventSeq, cutoff.AdoptionEventSeq)
				}
				return cutoff, nil
			},
			bind: func(
				_ context.Context,
				_ authorityRecord,
				value authorityCutoff,
			) (authorityCutoffArtifacts, bool, error) {
				if value == (authorityCutoff{}) {
					return authorityCutoffArtifacts{}, false, nil
				}
				point := authorityPoint{Hash: hash}
				return authorityCutoffArtifacts{
					Adoption: authorityPhysicalAdoptionRow{
						EventSeq:      value.AdoptionEventSeq,
						PublicationID: value.PublicationID,
					},
					AdoptionPoint: point,
					Block: authorityPhysicalBlockRow{
						PublicationID: value.PublicationID,
					},
					BlockPoint: point,
				}, true, nil
			},
		},
	}
}

func TestSelectAuthorityAtBlockUsesEffectiveAndAcceptsReAdoption(t *testing.T) {
	t.Parallel()
	hash := authorityFill32(0xa1)
	record := authorityRecord{
		Servable: true,
		Physical: authorityHead{
			EventSeq: 70,
			Point:    authorityPoint{Hash: authorityFill32(0xa2)},
		},
		Effective: authorityHead{
			EventSeq: 50,
			Point:    authorityPoint{Hash: authorityFill32(0xa3)},
		},
	}
	cutoff := authorityCutoff{AdoptionEventSeq: 41, PublicationID: 12}
	readers := authorityAtBlockTestReaders(
		t,
		record,
		hash,
		[]uint64{7, 12},
		map[uint64]authorityPublicationLifecycleSummary{
			7: {
				Adoption: authorityPhysicalMembershipRow{
					EventSeq:      11,
					PublicationID: 7,
				},
				AdoptionFound: true,
				Active:        false,
			},
			12: {
				Adoption: authorityPhysicalMembershipRow{
					EventSeq:      41,
					PublicationID: 12,
				},
				AdoptionFound: true,
				Active:        true,
			},
		},
		cutoff,
	)
	selection, err := selectAuthorityAtBlockWithReaders(
		context.Background(),
		record,
		hash,
		readers,
	)
	if err != nil ||
		selection.AuthorityEffective != record.Effective ||
		selection.QueryHead.EventSeq != 41 ||
		selection.QueryHead.Point.Hash != hash ||
		selection.Cutoff != cutoff {
		t.Fatalf("re-adopted AtBlock selection = %+v, %v", selection, err)
	}
}

func TestSelectAuthorityAtBlockRejectsNoneMultipleAndCutoffMismatch(t *testing.T) {
	t.Parallel()
	hash := authorityFill32(0xb1)
	record := authorityRecord{
		Servable:  true,
		Effective: authorityHead{EventSeq: 50, Point: authorityPoint{Hash: hash}},
	}
	none := authorityAtBlockTestReaders(
		t,
		record,
		hash,
		[]uint64{7},
		map[uint64]authorityPublicationLifecycleSummary{},
		authorityCutoff{},
	)
	if _, err := selectAuthorityAtBlockWithReaders(
		context.Background(),
		record,
		hash,
		none,
	); !errors.Is(err, ErrNotFound) {
		t.Fatalf("no-active AtBlock error = %v", err)
	}

	active := func(event, publication uint64) authorityPublicationLifecycleSummary {
		return authorityPublicationLifecycleSummary{
			Adoption: authorityPhysicalMembershipRow{
				EventSeq:      event,
				PublicationID: publication,
			},
			AdoptionFound: true,
			Active:        true,
		}
	}
	multiple := authorityAtBlockTestReaders(
		t,
		record,
		hash,
		[]uint64{7, 9},
		map[uint64]authorityPublicationLifecycleSummary{
			7: active(31, 7),
			9: active(32, 9),
		},
		authorityCutoff{AdoptionEventSeq: 31, PublicationID: 7},
	)
	if _, err := selectAuthorityAtBlockWithReaders(
		context.Background(),
		record,
		hash,
		multiple,
	); !errors.Is(err, ErrInvalidDataset) {
		t.Fatalf("multi-active AtBlock error = %v", err)
	}

	mismatch := authorityAtBlockTestReaders(
		t,
		record,
		hash,
		[]uint64{7},
		map[uint64]authorityPublicationLifecycleSummary{7: active(31, 7)},
		authorityCutoff{AdoptionEventSeq: 30, PublicationID: 6},
	)
	// The mismatch reader deliberately returns its own pair at the selected
	// event so the selector, rather than the fake, rejects it.
	mismatch.cutoff.load = func(context.Context, uint64) (authorityCutoff, error) {
		return authorityCutoff{AdoptionEventSeq: 30, PublicationID: 6}, nil
	}
	if _, err := selectAuthorityAtBlockWithReaders(
		context.Background(),
		record,
		hash,
		mismatch,
	); !errors.Is(err, ErrInvalidDataset) {
		t.Fatalf("cutoff-mismatch AtBlock error = %v", err)
	}

	zero := authorityAtBlockTestReaders(
		t,
		record,
		hash,
		[]uint64{7},
		map[uint64]authorityPublicationLifecycleSummary{7: active(31, 7)},
		authorityCutoff{},
	)
	if _, err := selectAuthorityAtBlockWithReaders(
		context.Background(),
		record,
		hash,
		zero,
	); !errors.Is(err, ErrInvalidDataset) {
		t.Fatalf("zero-cutoff AtBlock error = %v", err)
	}
}

func TestSelectAuthorityAtBlockRejectsReboundArtifactSubstitution(t *testing.T) {
	t.Parallel()
	hash := authorityFill32(0xba)
	other := authorityPoint{Hash: authorityFill32(0xbb)}
	point := authorityPoint{Hash: hash}
	record := authorityRecord{
		Servable:  true,
		Effective: authorityHead{EventSeq: 50},
	}
	cutoff := authorityCutoff{AdoptionEventSeq: 31, PublicationID: 7}
	active := authorityPublicationLifecycleSummary{
		Adoption: authorityPhysicalMembershipRow{
			EventSeq:      cutoff.AdoptionEventSeq,
			PublicationID: cutoff.PublicationID,
		},
		AdoptionFound: true,
		Active:        true,
	}
	valid := authorityCutoffArtifacts{
		Adoption: authorityPhysicalAdoptionRow{
			EventSeq:      cutoff.AdoptionEventSeq,
			PublicationID: cutoff.PublicationID,
		},
		AdoptionPoint: point,
		Block: authorityPhysicalBlockRow{
			PublicationID: cutoff.PublicationID,
		},
		BlockPoint: point,
	}
	tests := []struct {
		name   string
		mutate func(*authorityCutoffArtifacts)
	}{
		{
			name: "adoption event",
			mutate: func(artifacts *authorityCutoffArtifacts) {
				artifacts.Adoption.EventSeq--
			},
		},
		{
			name: "adoption publication",
			mutate: func(artifacts *authorityCutoffArtifacts) {
				artifacts.Adoption.PublicationID++
			},
		},
		{
			name: "adoption point",
			mutate: func(artifacts *authorityCutoffArtifacts) {
				artifacts.AdoptionPoint = other
			},
		},
		{
			name: "block publication",
			mutate: func(artifacts *authorityCutoffArtifacts) {
				artifacts.Block.PublicationID++
			},
		},
		{
			name: "block point",
			mutate: func(artifacts *authorityCutoffArtifacts) {
				artifacts.BlockPoint = other
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			readers := authorityAtBlockTestReaders(
				t,
				record,
				hash,
				[]uint64{cutoff.PublicationID},
				map[uint64]authorityPublicationLifecycleSummary{
					cutoff.PublicationID: active,
				},
				cutoff,
			)
			substituted := valid
			test.mutate(&substituted)
			readers.cutoff.bind = func(
				context.Context,
				authorityRecord,
				authorityCutoff,
			) (authorityCutoffArtifacts, bool, error) {
				return substituted, true, nil
			}
			if _, err := selectAuthorityAtBlockWithReaders(
				context.Background(),
				record,
				hash,
				readers,
			); !errors.Is(err, ErrInvalidDataset) {
				t.Fatalf("rebound artifact substitution error = %v", err)
			}
		})
	}
}

func TestSelectAuthorityAtBlockZeroHashIsNotFoundWithoutReads(t *testing.T) {
	t.Parallel()
	called := false
	_, err := selectAuthorityAtBlockWithReaders(
		context.Background(),
		authorityRecord{Servable: true},
		authorityHash{},
		authorityAtBlockReaders{
			nextCandidate: func(
				context.Context,
				authorityHash,
				bool,
				uint64,
			) (uint64, bool, error) {
				called = true
				return 0, false, nil
			},
		},
	)
	if !errors.Is(err, ErrNotFound) ||
		errors.Is(err, ErrInvalidDataset) ||
		called {
		t.Fatalf("zero-hash AtBlock called=%t error=%v", called, err)
	}
}

func TestSelectAuthorityAtBlockFutureRawPublicationIsNotFound(t *testing.T) {
	t.Parallel()
	hash := authorityFill32(0xbb)
	record := authorityRecord{
		Servable:  true,
		Effective: authorityHead{EventSeq: 50},
		Physical:  authorityHead{EventSeq: 70},
	}
	readers := authorityAtBlockTestReaders(
		t,
		record,
		hash,
		[]uint64{71},
		// The raw block exists, but its adoption is newer than Effective and
		// therefore absent from the lifecycle read bounded at event 50.
		map[uint64]authorityPublicationLifecycleSummary{},
		authorityCutoff{},
	)
	if _, err := selectAuthorityAtBlockWithReaders(
		context.Background(),
		record,
		hash,
		readers,
	); !errors.Is(err, ErrNotFound) || errors.Is(err, ErrInvalidDataset) {
		t.Fatalf("post-Effective raw AtBlock error = %v", err)
	}
}

func TestAuthoritySelectorsPreserveDependencyErrors(t *testing.T) {
	t.Parallel()
	infrastructure := errors.New("injected query failure")
	record := authorityRecord{
		Servable:  true,
		Effective: authorityHead{EventSeq: 50},
	}
	if _, err := selectAuthorityAtTipWithReaders(
		context.Background(),
		record,
		authoritySelectionCutoffReaders{
			load: func(context.Context, uint64) (authorityCutoff, error) {
				return authorityCutoff{}, infrastructure
			},
		},
	); !errors.Is(err, infrastructure) || errors.Is(err, ErrInvalidDataset) {
		t.Fatalf("AtTip dependency error = %v", err)
	}

	hash := authorityFill32(0xbc)
	if _, err := selectAuthorityAtBlockWithReaders(
		context.Background(),
		record,
		hash,
		authorityAtBlockReaders{
			nextCandidate: func(
				context.Context,
				authorityHash,
				bool,
				uint64,
			) (uint64, bool, error) {
				return 0, false, infrastructure
			},
		},
	); !errors.Is(err, infrastructure) || errors.Is(err, ErrInvalidDataset) {
		t.Fatalf("AtBlock dependency error = %v", err)
	}
}

func TestSelectAuthorityAtBlockMapsOfficialSyntheticToOrigin(t *testing.T) {
	t.Parallel()
	hash := authorityFill32(0xc1)
	writer := uuid.UUID(authorityFill16(0x44))
	at := time.Date(2026, time.July, 23, 3, 4, 5, 6000000, time.UTC)
	facts := authorityFill32(0xc2)
	adoption := authorityPhysicalAdoptionRow{
		EventSeq:      1,
		PublicationID: 1,
		Active:        true,
		BlockHash:     string(hash[:]),
		WriterID:      writer,
		RecordedAt:    at,
	}
	block := authorityPhysicalBlockRow{
		PublicationID: 1,
		BlockHash:     string(hash[:]),
		Era:           "Byron",
		BlockType:     -1,
		Synthetic:     true,
		FactsDigest:   string(facts[:]),
		WriterID:      writer,
		InsertedAt:    at,
	}
	point := authorityPoint{Hash: hash}
	record := authorityRecord{
		Start:           authorityPoint{Origin: true},
		GenesisSeeded:   true,
		CompleteHistory: true,
		ByronGenesisID:  hash,
		Servable:        true,
		Effective: authorityHead{
			EventSeq: 20,
			Point:    authorityPoint{Hash: authorityFill32(0xc3)},
		},
	}
	readers := authorityAtBlockTestReaders(
		t,
		record,
		hash,
		[]uint64{1},
		map[uint64]authorityPublicationLifecycleSummary{
			1: {
				Adoption: authorityPhysicalMembershipRow{
					EventSeq:      1,
					PublicationID: 1,
				},
				AdoptionFound: true,
				Active:        true,
			},
		},
		authorityCutoff{AdoptionEventSeq: 1, PublicationID: 1},
	)
	readers.loadBlock = func(
		context.Context,
		uint64,
	) (authorityPhysicalBlockRow, authorityPoint, bool, error) {
		return block, point, true, nil
	}
	readers.cutoff.bind = func(
		ctx context.Context,
		gotRecord authorityRecord,
		cutoff authorityCutoff,
	) (authorityCutoffArtifacts, bool, error) {
		return bindAuthorityCutoffArtifacts(
			ctx,
			gotRecord,
			cutoff,
			authorityCutoffArtifactReaders{
				loadAdoption: func(
					context.Context,
					uint64,
				) (authorityPhysicalAdoptionRow, authorityPoint, bool, error) {
					return adoption, point, true, nil
				},
				loadBlock: func(
					context.Context,
					uint64,
				) (authorityPhysicalBlockRow, authorityPoint, bool, error) {
					return block, point, true, nil
				},
			},
		)
	}
	selection, err := selectAuthorityAtBlockWithReaders(
		context.Background(),
		record,
		hash,
		readers,
	)
	if err != nil ||
		selection.QueryHead != (authorityHead{
			EventSeq: 1,
			Point:    authorityPoint{Origin: true},
		}) {
		t.Fatalf("synthetic AtBlock selection = %+v, %v", selection, err)
	}
}
