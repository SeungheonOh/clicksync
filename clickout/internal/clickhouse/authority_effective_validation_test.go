package clickhouse

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

func authorityHistoricalAdoptionReaders(
	t *testing.T,
	record authorityRecord,
	adoption authorityPhysicalAdoptionRow,
	adoptionPoint authorityPoint,
	block authorityPhysicalBlockRow,
	blockPoint authorityPoint,
) authorityArtifactReaders {
	t.Helper()
	readers := defaultAuthorityArtifactTestReaders()
	readers.loadAdoption = func(
		_ context.Context,
		eventSeq uint64,
	) (authorityPhysicalAdoptionRow, authorityPoint, bool, error) {
		if eventSeq != record.Effective.EventSeq {
			t.Fatalf("historical adoption event = %d", eventSeq)
		}
		return adoption, adoptionPoint, true, nil
	}
	readers.loadBlock = func(
		_ context.Context,
		publicationID uint64,
	) (authorityPhysicalBlockRow, authorityPoint, bool, error) {
		if publicationID != adoption.PublicationID {
			t.Fatalf("historical adoption publication = %d", publicationID)
		}
		return block, blockPoint, true, nil
	}
	readers.validateAdoption = validateAuthorityPhysicalAdoptionMapping
	readers.validateAdoptionLifecycle = func(
		_ context.Context,
		projected authorityRecord,
		gotAdoption authorityPhysicalAdoptionRow,
		gotBlock authorityPhysicalBlockRow,
		gotPoint authorityPoint,
	) error {
		if projected.Physical != record.Effective ||
			!sameAuthorityPhysicalAdoptionRow(gotAdoption, adoption) ||
			!sameAuthorityPhysicalBlockRow(gotBlock, block) ||
			gotPoint != blockPoint {
			return errors.New("historical adoption was not projected exactly")
		}
		return nil
	}
	return readers
}

func TestValidateAuthorityHistoricalEffectiveAdoption(t *testing.T) {
	record, adoption, point, block := authorityCurrentAdoptionFixture()
	record.Effective = record.Physical
	record.Physical = authorityHead{
		EventSeq: record.Effective.EventSeq + 7,
		Point: authorityPoint{
			Slot:        point.Slot + 7,
			Hash:        authorityFill32(0x77),
			BlockNumber: point.BlockNumber + 7,
		},
	}
	readers := authorityHistoricalAdoptionReaders(
		t,
		record,
		adoption,
		point,
		block,
		point,
	)
	if err := validateAuthorityHistoricalEffectiveArtifacts(
		context.Background(),
		record,
		readers,
	); err != nil {
		t.Fatalf("exact historical adoption rejected: %v", err)
	}

	for name, mutate := range map[string]func(*authorityArtifactReaders){
		"orphan block": func(readers *authorityArtifactReaders) {
			readers.loadBlock = func(
				context.Context,
				uint64,
			) (authorityPhysicalBlockRow, authorityPoint, bool, error) {
				return authorityPhysicalBlockRow{}, authorityPoint{}, false, nil
			}
		},
		"duplicate or reactivated lifecycle": func(readers *authorityArtifactReaders) {
			readers.validateAdoptionLifecycle = func(
				context.Context,
				authorityRecord,
				authorityPhysicalAdoptionRow,
				authorityPhysicalBlockRow,
				authorityPoint,
			) error {
				return errors.New("duplicate/reactivated adoption lifecycle")
			}
		},
		"same-event invalidation": func(readers *authorityArtifactReaders) {
			readers.probeAt = func(
				_ context.Context,
				kind authorityArtifactKind,
				eventSeq uint64,
			) (bool, error) {
				return kind == authorityArtifactInvalidation &&
					eventSeq == record.Effective.EventSeq, nil
			}
		},
		"adoption rollback collision": func(readers *authorityArtifactReaders) {
			readers.loadRollback = func(
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
				return authorityPhysicalRollbackRow{}, authorityPoint{},
					authorityPoint{}, authorityHash{}, true, nil
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			corrupt := readers
			mutate(&corrupt)
			if err := validateAuthorityHistoricalEffectiveArtifacts(
				context.Background(),
				record,
				corrupt,
			); err == nil {
				t.Fatal("corrupt historical adoption was accepted")
			}
		})
	}
}

func TestValidateAuthorityHistoricalEffectiveSyntheticGenesis(t *testing.T) {
	record := validOfficialGenesisAuthority(t)
	record.Effective = authorityHead{
		EventSeq: 1,
		Point:    authorityPoint{Origin: true},
	}
	record.Physical = authorityHead{
		EventSeq: 2,
		Point: authorityPoint{
			Slot:        2,
			Hash:        authorityFill32(0x72),
			BlockNumber: 1,
		},
	}
	writer := uuid.UUID(authorityFill16(0x61))
	at := time.Date(2026, time.July, 23, 12, 0, 0, 123456000, time.UTC)
	genesisHash := string(record.ByronGenesisID[:])
	factsDigest := authorityFill32(0x62)
	adoption := authorityPhysicalAdoptionRow{
		EventSeq:      1,
		PublicationID: 1,
		Active:        true,
		BlockHash:     genesisHash,
		WriterID:      writer,
		RecordedAt:    at,
	}
	blockPoint := authorityPoint{Hash: record.ByronGenesisID}
	block := authorityPhysicalBlockRow{
		PublicationID: 1,
		BlockHash:     genesisHash,
		Era:           "Byron",
		BlockType:     -1,
		Synthetic:     true,
		FactsDigest:   string(factsDigest[:]),
		WriterID:      writer,
		InsertedAt:    at,
	}
	readers := authorityHistoricalAdoptionReaders(
		t,
		record,
		adoption,
		blockPoint,
		block,
		blockPoint,
	)
	if err := validateAuthorityHistoricalEffectiveArtifacts(
		context.Background(),
		record,
		readers,
	); err != nil {
		t.Fatalf("historical synthetic genesis rejected: %v", err)
	}
}

func TestValidateAuthorityHistoricalEffectiveEventZeroBoundaries(t *testing.T) {
	partial := authorityPoint{
		Slot:        100,
		Hash:        authorityFill32(0x51),
		BlockNumber: 10,
	}
	for name, start := range map[string]authorityPoint{
		"Origin":        {Origin: true},
		"partial Start": partial,
	} {
		t.Run(name, func(t *testing.T) {
			record := authorityRecord{
				Start:     start,
				Effective: authorityHead{Point: start},
				Physical: authorityHead{
					EventSeq: 1,
					Point:    authorityPoint{Origin: true},
				},
			}
			if err := validateAuthorityHistoricalEffectiveArtifacts(
				context.Background(),
				record,
				defaultAuthorityArtifactTestReaders(),
			); err != nil {
				t.Fatalf("exact event-zero boundary rejected: %v", err)
			}
			record.Effective.Point = authorityPoint{Origin: !start.Origin}
			if err := validateAuthorityHistoricalEffectiveArtifacts(
				context.Background(),
				record,
				defaultAuthorityArtifactTestReaders(),
			); err == nil {
				t.Fatal("event-zero boundary differing from immutable Start accepted")
			}
		})
	}
}

type authorityHistoricalRollbackFixture struct {
	record   authorityRecord
	header   authorityPhysicalRollbackRow
	to       authorityPoint
	oldTip   authorityPoint
	digest   authorityHash
	evidence []authorityObservationRow
}

func newAuthorityHistoricalRollbackFixture(
	t *testing.T,
) authorityHistoricalRollbackFixture {
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
	commitment, err := canonicalAuthorityEvidenceCommitment(evidence, group, 1)
	if err != nil {
		t.Fatal(err)
	}
	header := authorityRollbackArtifactTestRow(commitment.Digest)
	header.EvidenceCount = commitment.Count
	header.EvidenceDigest = string(commitment.Digest[:])
	decoded, to, oldTip, digest, found, err :=
		decodeAuthorityPhysicalRollbackRows(
			[]authorityPhysicalRollbackRow{header},
			header.EventSeq,
		)
	if err != nil || !found {
		t.Fatalf("decode historical rollback fixture: found=%v err=%v", found, err)
	}
	head := authorityHead{EventSeq: decoded.EventSeq, Point: to}
	reference := authorityEvidenceReference{
		CheckID:   authorityUUID(decoded.CheckID),
		Group:     group,
		Attempt:   decoded.CheckAttempt,
		Required:  decoded.CorroborationRequired,
		Confirmed: 2,
		Checked: authorityHead{
			EventSeq: decoded.CheckedEventSeq,
			Point:    to,
		},
		Count:  commitment.Count,
		Digest: commitment.Digest,
	}
	lastAgreedAt := decoded.RecordedAt
	record := authorityRecord{
		Physical: authorityHead{
			EventSeq: decoded.EventSeq + 8,
			Point: authorityPoint{
				Slot:        to.Slot + 8,
				Hash:        authorityFill32(0x78),
				BlockNumber: to.BlockNumber + 8,
			},
		},
		Effective:          head,
		LastAgreed:         &head,
		LastAgreedAt:       &lastAgreedAt,
		LastAgreedEvidence: &reference,
	}
	return authorityHistoricalRollbackFixture{
		record:   record,
		header:   decoded,
		to:       to,
		oldTip:   oldTip,
		digest:   digest,
		evidence: evidence,
	}
}

func (fixture authorityHistoricalRollbackFixture) readers(
	t *testing.T,
) authorityArtifactReaders {
	t.Helper()
	readers := defaultAuthorityArtifactTestReaders()
	readers.loadRollback = func(
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
		if eventSeq != fixture.record.Effective.EventSeq {
			t.Fatalf("historical rollback event = %d", eventSeq)
		}
		return fixture.header, fixture.to, fixture.oldTip,
			fixture.digest, true, nil
	}
	readers.loadEvidence = func(
		_ context.Context,
		checkID [16]byte,
	) ([]authorityObservationRow, error) {
		if checkID != authorityUUID(fixture.header.CheckID) {
			return nil, errors.New("historical rollback evidence check differs")
		}
		return fixture.evidence, nil
	}
	readers.validateFinalizedRollback = func(
		projected authorityRecord,
		header authorityPhysicalRollbackRow,
		to authorityPoint,
		digest authorityHash,
		evidence []authorityObservationRow,
	) error {
		if projected.Physical != fixture.record.Effective {
			return errors.New("historical rollback was not projected exactly")
		}
		return validateAuthorityFinalizedRollbackHeader(
			projected,
			header,
			to,
			digest,
			evidence,
		)
	}
	readers.validateInvalidations = func(
		_ context.Context,
		projected authorityRecord,
		_ authorityPhysicalRollbackRow,
		_ authorityPoint,
		_ authorityPoint,
	) (bool, error) {
		if projected.Physical != fixture.record.Effective {
			return false, errors.New(
				"historical invalidations were not projected exactly",
			)
		}
		return true, nil
	}
	return readers
}

func TestValidateAuthorityHistoricalEffectiveRollback(t *testing.T) {
	fixture := newAuthorityHistoricalRollbackFixture(t)
	if err := validateAuthorityHistoricalEffectiveArtifacts(
		context.Background(),
		fixture.record,
		fixture.readers(t),
	); err != nil {
		t.Fatalf("exact historical rollback rejected: %v", err)
	}

	for name, mutate := range map[string]func(
		*authorityHistoricalRollbackFixture,
		*authorityArtifactReaders,
	){
		"missing LastAgreed": func(
			fixture *authorityHistoricalRollbackFixture,
			_ *authorityArtifactReaders,
		) {
			fixture.record.LastAgreed = nil
		},
		"wrong LastAgreedAt": func(
			fixture *authorityHistoricalRollbackFixture,
			_ *authorityArtifactReaders,
		) {
			at := fixture.header.RecordedAt.Add(time.Microsecond)
			fixture.record.LastAgreedAt = &at
		},
		"missing evidence": func(
			fixture *authorityHistoricalRollbackFixture,
			readers *authorityArtifactReaders,
		) {
			readers.loadEvidence = func(
				context.Context,
				[16]byte,
			) ([]authorityObservationRow, error) {
				return fixture.evidence[:2], nil
			}
		},
		"observer map mismatch": func(
			fixture *authorityHistoricalRollbackFixture,
			readers *authorityArtifactReaders,
		) {
			header := fixture.header
			header.ObservedPeers = []string{"relay-b", "relay-a"}
			readers.loadRollback = func(
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
				return header, fixture.to, fixture.oldTip,
					fixture.digest, true, nil
			}
		},
		"partial invalidations": func(
			_ *authorityHistoricalRollbackFixture,
			readers *authorityArtifactReaders,
		) {
			readers.validateInvalidations = func(
				context.Context,
				authorityRecord,
				authorityPhysicalRollbackRow,
				authorityPoint,
				authorityPoint,
			) (bool, error) {
				return false, nil
			}
		},
		"invalid descendant walk": func(
			_ *authorityHistoricalRollbackFixture,
			readers *authorityArtifactReaders,
		) {
			readers.validateInvalidations = func(
				context.Context,
				authorityRecord,
				authorityPhysicalRollbackRow,
				authorityPoint,
				authorityPoint,
			) (bool, error) {
				return false, errors.New("old-active descendant chain is corrupt")
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			corrupt := newAuthorityHistoricalRollbackFixture(t)
			readers := corrupt.readers(t)
			mutate(&corrupt, &readers)
			if err := validateAuthorityHistoricalEffectiveArtifacts(
				context.Background(),
				corrupt.record,
				readers,
			); err == nil {
				t.Fatal("corrupt historical rollback was accepted")
			}
		})
	}
}

func TestValidateAuthorityHistoricalRollbackEvidenceOutcomes(t *testing.T) {
	for name, results := range map[string][]string{
		"disagreement":    {"agreed", "agreed", "disagreed"},
		"below threshold": {"agreed", "unavailable", "unavailable"},
	} {
		t.Run(name, func(t *testing.T) {
			fixture := newAuthorityHistoricalRollbackFixture(t)
			group := fixture.record.LastAgreedEvidence.Group
			evidence := make([]authorityObservationRow, 0, len(results))
			for index, result := range results {
				evidence = append(evidence, authorityRollbackArtifactEvidenceRow(
					t,
					group,
					uint32(index+1),
					"operator-"+string(rune('a'+index)),
					"relay-"+string(rune('a'+index)),
					result,
				))
			}
			commitment, err := canonicalAuthorityEvidenceCommitment(
				evidence,
				group,
				fixture.header.CheckAttempt,
			)
			if err != nil {
				t.Fatal(err)
			}
			fixture.evidence = evidence
			fixture.header.EvidenceCount = commitment.Count
			fixture.header.EvidenceDigest = string(commitment.Digest[:])
			fixture.digest = commitment.Digest
			reference := *fixture.record.LastAgreedEvidence
			reference.Count = commitment.Count
			reference.Digest = commitment.Digest
			if name == "below threshold" {
				reference.Confirmed = 1
				fixture.header.ObservedPeers = []string{"relay-a"}
				fixture.header.ObservedOperators = []string{"operator-a"}
			}
			fixture.record.LastAgreedEvidence = &reference
			if err := validateAuthorityHistoricalEffectiveArtifacts(
				context.Background(),
				fixture.record,
				fixture.readers(t),
			); err == nil {
				t.Fatalf("historical rollback %s was accepted", name)
			}
		})
	}
}

func TestValidateAuthorityHistoricalEffectiveHonorsContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	record := authorityRecord{
		Effective: authorityHead{
			EventSeq: 1,
			Point:    authorityPoint{Origin: true},
		},
		Physical: authorityHead{
			EventSeq: 2,
			Point:    authorityPoint{Origin: true},
		},
	}
	err := validateAuthorityHistoricalEffectiveArtifacts(
		ctx,
		record,
		defaultAuthorityArtifactTestReaders(),
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled historical Effective validation = %v", err)
	}
}
