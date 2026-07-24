package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestAuthorityArtifactProbeSQLShape(t *testing.T) {
	t.Parallel()
	for name, test := range map[string]struct {
		query string
		wants []string
	}{
		"adoption at": {
			query: authorityAdoptionAtEventProbeSQL,
			wants: []string{
				"FROM chain_events",
				"event_kind = 'adoption'",
				"event_seq = ?",
				"ORDER BY event_kind, event_seq, publication_id",
			},
		},
		"rollback at": {
			query: authorityRollbackAtEventProbeSQL,
			wants: []string{
				"FROM rollbacks",
				"event_seq = ?",
				"ORDER BY event_seq, rollback_id",
			},
		},
		"invalidation at": {
			query: authorityInvalidationAtEventProbeSQL,
			wants: []string{
				"FROM chain_events",
				"event_kind = 'invalidation'",
				"event_seq = ?",
				"ORDER BY event_kind, event_seq, publication_id",
			},
		},
		"adoption after": {
			query: authorityAdoptionAfterProbeSQL,
			wants: []string{
				"FROM chain_events",
				"event_kind = 'adoption'",
				"event_seq > ?",
				"ORDER BY event_kind, event_seq, publication_id",
			},
		},
		"rollback after": {
			query: authorityRollbackAfterProbeSQL,
			wants: []string{
				"FROM rollbacks",
				"event_seq > ?",
				"ORDER BY event_seq, rollback_id",
			},
		},
		"invalidation after": {
			query: authorityInvalidationAfterProbeSQL,
			wants: []string{
				"FROM chain_events",
				"event_kind = 'invalidation'",
				"event_seq > ?",
				"ORDER BY event_kind, event_seq, publication_id",
			},
		},
		"rollback between": {
			query: authorityRollbackBetweenProbeSQL,
			wants: []string{
				"FROM rollbacks",
				"event_seq > ?",
				"event_seq < ?",
				"ORDER BY event_seq, rollback_id",
			},
		},
		"invalidation between": {
			query: authorityInvalidationBetweenProbeSQL,
			wants: []string{
				"FROM chain_events",
				"event_kind = 'invalidation'",
				"event_seq > ?",
				"event_seq < ?",
				"ORDER BY event_kind, event_seq, publication_id",
			},
		},
	} {
		name, test := name, test
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			for _, want := range test.wants {
				if !strings.Contains(test.query, want) {
					t.Fatalf("query lacks %q:\n%s", want, test.query)
				}
			}
			if !strings.Contains(test.query, "LIMIT 1") {
				t.Fatalf("probe is not LIMIT 1:\n%s", test.query)
			}
			for _, forbidden := range []string{
				"clicksync.",
				"OFFSET",
				"argMax",
				"count(",
			} {
				if strings.Contains(test.query, forbidden) {
					t.Fatalf(
						"query contains forbidden %q:\n%s",
						forbidden,
						test.query,
					)
				}
			}
		})
	}
}

func defaultAuthorityArtifactTestReaders() authorityArtifactReaders {
	return authorityArtifactReaders{
		loadAdoption: func(
			context.Context,
			uint64,
		) (authorityPhysicalAdoptionRow, authorityPoint, bool, error) {
			return authorityPhysicalAdoptionRow{}, authorityPoint{}, false, nil
		},
		loadBlock: func(
			context.Context,
			uint64,
		) (authorityPhysicalBlockRow, authorityPoint, bool, error) {
			return authorityPhysicalBlockRow{}, authorityPoint{}, false, nil
		},
		loadRollback: func(
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
				authorityPoint{}, authorityHash{}, false, nil
		},
		loadEvidence: func(
			context.Context,
			[16]byte,
		) ([]authorityObservationRow, error) {
			return nil, nil
		},
		validateAdoption: func(
			authorityRecord,
			authorityPhysicalAdoptionRow,
			authorityPoint,
			authorityPhysicalBlockRow,
			authorityPoint,
		) error {
			return nil
		},
		validateAdoptionLifecycle: func(
			context.Context,
			authorityRecord,
			authorityPhysicalAdoptionRow,
			authorityPhysicalBlockRow,
			authorityPoint,
		) error {
			return nil
		},
		validateFinalizedRollback: func(
			authorityRecord,
			authorityPhysicalRollbackRow,
			authorityPoint,
			authorityHash,
			[]authorityObservationRow,
		) error {
			return nil
		},
		validateInvalidations: func(
			context.Context,
			authorityRecord,
			authorityPhysicalRollbackRow,
			authorityPoint,
			authorityPoint,
		) (bool, error) {
			return true, nil
		},
		probeAt: func(
			context.Context,
			authorityArtifactKind,
			uint64,
		) (bool, error) {
			return false, nil
		},
	}
}

func TestAuthorityArtifactCompositionSemanticTaxonomy(t *testing.T) {
	t.Parallel()
	record := authorityRecord{
		Start:    authorityPoint{Origin: true},
		Physical: authorityHead{EventSeq: 1},
	}
	if err := validateAuthorityExactHeadArtifacts(
		context.Background(),
		record,
		record.Physical,
		defaultAuthorityArtifactTestReaders(),
	); !errors.Is(err, ErrInvalidDataset) {
		t.Fatalf("exact-head semantic error = %v", err)
	}
	if err := validateAuthorityPendingRollbackArtifacts(
		context.Background(),
		record,
		defaultAuthorityArtifactTestReaders(),
	); !errors.Is(err, ErrInvalidDataset) {
		t.Fatalf("pending semantic error = %v", err)
	}
	if err := validateAuthorityPendingRollbackEvidence(
		authorityPendingRollback{},
		nil,
	); !errors.Is(err, ErrInvalidDataset) {
		t.Fatalf("pending-evidence semantic error = %v", err)
	}
	if err := validateAuthorityArtifactBarriers(
		context.Background(),
		record,
		nil,
	); !errors.Is(err, ErrInvalidDataset) {
		t.Fatalf("barrier semantic error = %v", err)
	}
	if _, err := validateAuthorityRollbackInvalidationSet(
		context.Background(),
		authorityPhysicalRollbackRow{},
		nil,
		nil,
		nil,
	); !errors.Is(err, ErrInvalidDataset) {
		t.Fatalf("invalidation semantic error = %v", err)
	}
}

func TestAuthorityArtifactCompositionPreservesDependencyErrors(t *testing.T) {
	t.Parallel()
	record := authorityRecord{
		Start:    authorityPoint{Origin: true},
		Physical: authorityHead{EventSeq: 1},
	}
	failures := []error{
		errors.New("injected infrastructure failure"),
		&ResourceLimitError{
			Phase: "injected",
			Cause: errors.New("injected resource failure"),
		},
	}
	for _, failure := range failures {
		failure := failure
		t.Run(failure.Error(), func(t *testing.T) {
			readers := defaultAuthorityArtifactTestReaders()
			readers.loadAdoption = func(
				context.Context,
				uint64,
			) (authorityPhysicalAdoptionRow, authorityPoint, bool, error) {
				return authorityPhysicalAdoptionRow{},
					authorityPoint{},
					false,
					failure
			}
			if err := validateAuthorityExactHeadArtifacts(
				context.Background(),
				record,
				record.Physical,
				readers,
			); !errors.Is(err, failure) ||
				errors.Is(err, ErrInvalidDataset) {
				t.Fatalf("exact-head dependency error = %v", err)
			}
			if err := validateAuthorityArtifactBarriers(
				context.Background(),
				record,
				func(
					context.Context,
					authorityArtifactKind,
					uint64,
					*uint64,
				) (uint64, bool, error) {
					return 0, false, failure
				},
			); !errors.Is(err, failure) ||
				errors.Is(err, ErrInvalidDataset) {
				t.Fatalf("barrier dependency error = %v", err)
			}
		})
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := validateAuthorityExactHeadArtifacts(
		ctx,
		record,
		record.Physical,
		defaultAuthorityArtifactTestReaders(),
	); !errors.Is(err, context.Canceled) ||
		errors.Is(err, ErrInvalidDataset) {
		t.Fatalf("exact-head context error = %v", err)
	}
}

func TestValidateAuthorityCurrentPhysicalArtifactsEventZero(t *testing.T) {
	t.Parallel()
	record := authorityRecord{
		Start: authorityPoint{Origin: true},
		Physical: authorityHead{
			Point: authorityPoint{Origin: true},
		},
	}
	readers := defaultAuthorityArtifactTestReaders()
	var probed []authorityArtifactKind
	readers.probeAt = func(
		_ context.Context,
		kind authorityArtifactKind,
		eventSeq uint64,
	) (bool, error) {
		if eventSeq != 0 {
			t.Fatalf("event-zero probe used event %d", eventSeq)
		}
		probed = append(probed, kind)
		return false, nil
	}
	if err := validateAuthorityCurrentPhysicalArtifacts(
		context.Background(),
		record,
		readers,
	); err != nil {
		t.Fatalf("artifact-free event zero rejected: %v", err)
	}
	if fmt.Sprint(probed) != "[adoption rollback invalidation]" {
		t.Fatalf("event-zero probes = %v", probed)
	}
	partialStart := authorityInvalidationTestPoint(0x51)
	partial := record
	partial.Start = partialStart
	partial.Physical.Point = partialStart
	if err := validateAuthorityCurrentPhysicalArtifacts(
		context.Background(),
		partial,
		readers,
	); err != nil {
		t.Fatalf("artifact-free partial Start at event zero rejected: %v", err)
	}
	mismatch := partial
	mismatch.Physical.Point = authorityInvalidationTestPoint(0x52)
	if err := validateAuthorityCurrentPhysicalArtifacts(
		context.Background(),
		mismatch,
		readers,
	); err == nil {
		t.Fatal("event-zero physical point differing from Start was accepted")
	}

	for _, corruptKind := range []authorityArtifactKind{
		authorityArtifactAdoption,
		authorityArtifactRollback,
		authorityArtifactInvalidation,
	} {
		corruptKind := corruptKind
		t.Run(string(corruptKind), func(t *testing.T) {
			t.Parallel()
			corrupt := defaultAuthorityArtifactTestReaders()
			corrupt.probeAt = func(
				_ context.Context,
				kind authorityArtifactKind,
				_ uint64,
			) (bool, error) {
				return kind == corruptKind, nil
			}
			if err := validateAuthorityCurrentPhysicalArtifacts(
				context.Background(),
				record,
				corrupt,
			); err == nil {
				t.Fatal("event-zero artifact was accepted")
			}
		})
	}
}

func TestValidateAuthorityCurrentPhysicalArtifactsRejectsEventZeroAboveCurrent(
	t *testing.T,
) {
	t.Parallel()
	record, _, _, _ := authorityCurrentAdoptionFixture()
	kinds := []authorityArtifactKind{
		authorityArtifactAdoption,
		authorityArtifactRollback,
		authorityArtifactInvalidation,
	}
	for mask := 1; mask < 1<<len(kinds); mask++ {
		mask := mask
		t.Run(fmt.Sprintf("mask_%d", mask), func(t *testing.T) {
			t.Parallel()
			readers := defaultAuthorityArtifactTestReaders()
			readers.probeAt = func(
				_ context.Context,
				kind authorityArtifactKind,
				eventSeq uint64,
			) (bool, error) {
				if eventSeq != 0 {
					t.Fatalf("reserved-event probe used event %d", eventSeq)
				}
				for index, candidate := range kinds {
					if candidate == kind {
						return mask&(1<<index) != 0, nil
					}
				}
				t.Fatalf("unknown artifact kind %q", kind)
				return false, nil
			}
			if err := validateAuthorityCurrentPhysicalArtifacts(
				context.Background(),
				record,
				readers,
			); err == nil {
				t.Fatal("event-zero artifact survived above a positive current event")
			}
		})
	}
}

func authorityCurrentAdoptionFixture() (
	authorityRecord,
	authorityPhysicalAdoptionRow,
	authorityPoint,
	authorityPhysicalBlockRow,
) {
	point := authorityPoint{
		Slot:        100,
		Hash:        authorityFill32(0x51),
		BlockNumber: 10,
	}
	writer := uuid.UUID(authorityFill16(0x43))
	at := authorityInvalidationTestHeader(0).RecordedAt
	factsDigest := authorityFill32(0x61)
	adoption := authorityPhysicalAdoptionRow{
		EventSeq:      5,
		PublicationID: 9,
		Active:        true,
		BlockHash:     string(point.Hash[:]),
		Slot:          point.Slot,
		BlockNumber:   point.BlockNumber,
		WriterID:      writer,
		RecordedAt:    at,
	}
	block := authorityPhysicalBlockRow{
		PublicationID: adoption.PublicationID,
		BlockHash:     adoption.BlockHash,
		Era:           "Babbage",
		BlockType:     1,
		FactsDigest:   string(factsDigest[:]),
		WriterID:      writer,
		InsertedAt:    at,
	}
	return authorityRecord{
		Physical: authorityHead{EventSeq: adoption.EventSeq, Point: point},
	}, adoption, point, block
}

func TestValidateAuthorityCurrentPhysicalArtifactsAdoption(t *testing.T) {
	t.Parallel()
	record, adoption, point, block := authorityCurrentAdoptionFixture()
	readers := defaultAuthorityArtifactTestReaders()
	readers.loadAdoption = func(
		_ context.Context,
		eventSeq uint64,
	) (authorityPhysicalAdoptionRow, authorityPoint, bool, error) {
		if eventSeq != record.Physical.EventSeq {
			t.Fatalf("adoption event = %d", eventSeq)
		}
		return adoption, point, true, nil
	}
	readers.loadBlock = func(
		_ context.Context,
		publicationID uint64,
	) (authorityPhysicalBlockRow, authorityPoint, bool, error) {
		if publicationID != adoption.PublicationID {
			t.Fatalf("block publication = %d", publicationID)
		}
		return block, point, true, nil
	}
	readers.validateAdoption = validateAuthorityPhysicalAdoptionMapping
	lifecycleValidated := false
	readers.validateAdoptionLifecycle = func(
		_ context.Context,
		gotRecord authorityRecord,
		gotAdoption authorityPhysicalAdoptionRow,
		gotBlock authorityPhysicalBlockRow,
		gotBlockPoint authorityPoint,
	) error {
		lifecycleValidated = true
		if gotRecord.Physical != record.Physical ||
			!sameAuthorityPhysicalAdoptionRow(gotAdoption, adoption) ||
			!sameAuthorityPhysicalBlockRow(gotBlock, block) ||
			gotBlockPoint != point {
			return errors.New("wrong current-adoption lifecycle inputs")
		}
		return nil
	}
	if err := validateAuthorityCurrentPhysicalArtifacts(
		context.Background(),
		record,
		readers,
	); err != nil || !lifecycleValidated {
		t.Fatalf(
			"exact current adoption rejected: lifecycle=%v err=%v",
			lifecycleValidated,
			err,
		)
	}

	sameEventInvalidation := readers
	sameEventInvalidation.probeAt = func(
		_ context.Context,
		kind authorityArtifactKind,
		eventSeq uint64,
	) (bool, error) {
		return kind == authorityArtifactInvalidation &&
			eventSeq == record.Physical.EventSeq, nil
	}
	if err := validateAuthorityCurrentPhysicalArtifacts(
		context.Background(),
		record,
		sameEventInvalidation,
	); err == nil {
		t.Fatal("adoption with same-event invalidation was accepted")
	}

	missingBlock := readers
	missingBlock.loadBlock = func(
		context.Context,
		uint64,
	) (authorityPhysicalBlockRow, authorityPoint, bool, error) {
		return authorityPhysicalBlockRow{}, authorityPoint{}, false, nil
	}
	if err := validateAuthorityCurrentPhysicalArtifacts(
		context.Background(),
		record,
		missingBlock,
	); err == nil {
		t.Fatal("adoption without exact block was accepted")
	}

	badLifecycle := readers
	badLifecycle.validateAdoptionLifecycle = func(
		context.Context,
		authorityRecord,
		authorityPhysicalAdoptionRow,
		authorityPhysicalBlockRow,
		authorityPoint,
	) error {
		return errors.New("publication lifecycle is corrupt")
	}
	if err := validateAuthorityCurrentPhysicalArtifacts(
		context.Background(),
		record,
		badLifecycle,
	); err == nil {
		t.Fatal("adoption with a corrupt publication lifecycle was accepted")
	}
}

func TestValidateAuthorityCurrentPhysicalArtifactsXOR(t *testing.T) {
	t.Parallel()
	record, adoption, point, _ := authorityCurrentAdoptionFixture()
	for name, test := range map[string]struct {
		adoptionFound bool
		rollbackFound bool
	}{
		"neither": {},
		"both": {
			adoptionFound: true,
			rollbackFound: true,
		},
	} {
		name, test := name, test
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			readers := defaultAuthorityArtifactTestReaders()
			readers.loadAdoption = func(
				context.Context,
				uint64,
			) (authorityPhysicalAdoptionRow, authorityPoint, bool, error) {
				return adoption, point, test.adoptionFound, nil
			}
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
					authorityPoint{}, authorityHash{}, test.rollbackFound, nil
			}
			if err := validateAuthorityCurrentPhysicalArtifacts(
				context.Background(),
				record,
				readers,
			); err == nil {
				t.Fatal("current event XOR corruption was accepted")
			}
		})
	}
}

func TestValidateAuthorityCurrentPhysicalArtifactsRollback(t *testing.T) {
	t.Parallel()
	header := authorityInvalidationTestHeader(0)
	to := authorityInvalidationTestPoint(0x51)
	oldTip := to
	digest := authorityFill32(0x61)
	header.CheckID = uuid.UUID(authorityFill16(0x41))
	record := authorityRecord{
		Physical: authorityHead{EventSeq: header.EventSeq, Point: to},
	}
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
		if eventSeq != header.EventSeq {
			t.Fatalf("rollback event = %d", eventSeq)
		}
		return header, to, oldTip, digest, true, nil
	}
	validatedHeader := false
	readers.validateFinalizedRollback = func(
		_ authorityRecord,
		got authorityPhysicalRollbackRow,
		gotTo authorityPoint,
		gotDigest authorityHash,
		_ []authorityObservationRow,
	) error {
		validatedHeader = true
		if !sameAuthorityPhysicalRollbackRow(got, header) ||
			gotTo != to ||
			gotDigest != digest {
			return errors.New("wrong finalized header inputs")
		}
		return nil
	}
	if err := validateAuthorityCurrentPhysicalArtifacts(
		context.Background(),
		record,
		readers,
	); err != nil || !validatedHeader {
		t.Fatalf(
			"complete current rollback rejected: validated=%v err=%v",
			validatedHeader,
			err,
		)
	}
	incomplete := readers
	incomplete.validateInvalidations = func(
		context.Context,
		authorityRecord,
		authorityPhysicalRollbackRow,
		authorityPoint,
		authorityPoint,
	) (bool, error) {
		return false, nil
	}
	if err := validateAuthorityCurrentPhysicalArtifacts(
		context.Background(),
		record,
		incomplete,
	); err == nil {
		t.Fatal("finalized rollback without exact invalidations was accepted")
	}
}

func authorityPendingArtifactFixture(
	t *testing.T,
) (
	authorityRecord,
	authorityPendingRollback,
	authorityPhysicalRollbackRow,
	authorityPoint,
	authorityPoint,
	authorityHash,
	[]authorityObservationRow,
) {
	t.Helper()
	digest := authorityFill32(0x61)
	header := authorityRollbackArtifactTestRow(digest)
	header.Depth = 2
	_, to, _, _, found, err :=
		decodeAuthorityPhysicalRollbackRows(
			[]authorityPhysicalRollbackRow{header},
			header.EventSeq,
		)
	if err != nil || !found {
		t.Fatalf("decode pending fixture: found=%v err=%v", found, err)
	}
	group := authorityUUID(*header.AgreementGroup)
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
		header.CheckAttempt,
	)
	if err != nil {
		t.Fatal(err)
	}
	header.EvidenceCount = commitment.Count
	header.EvidenceDigest = string(commitment.Digest[:])
	decoded, to, oldTip, decodedDigest, found, err :=
		decodeAuthorityPhysicalRollbackRows(
			[]authorityPhysicalRollbackRow{header},
			header.EventSeq,
		)
	if err != nil || !found {
		t.Fatalf("decode committed pending fixture: found=%v err=%v", found, err)
	}
	pending := authorityPendingFromRollbackHeader(
		decoded,
		to,
		oldTip,
		decodedDigest,
	)
	checkID := pending.CheckID
	checked := authorityHead{
		EventSeq: pending.CheckedEventSeq,
		Point:    pending.To,
	}
	record := authorityRecord{
		TrustStatus:           "checking",
		CheckID:               &checkID,
		AgreementGroup:        &group,
		CheckAttempt:          pending.CheckAttempt,
		CorroborationRequired: pending.Required,
		EvidenceState:         "frozen",
		EvidenceCount:         pending.EvidenceCount,
		EvidenceDigest:        &pending.EvidenceDigest,
		Checked:               &checked,
		Physical:              pending.OldPhysical,
		PendingRollback:       &pending,
	}
	return record, pending, decoded, to, oldTip, decodedDigest, evidence
}

func authorityPendingArtifactTestReaders(
	t *testing.T,
	checkID [16]byte,
	evidence []authorityObservationRow,
) authorityArtifactReaders {
	t.Helper()
	readers := defaultAuthorityArtifactTestReaders()
	readers.loadEvidence = func(
		_ context.Context,
		gotCheckID [16]byte,
	) ([]authorityObservationRow, error) {
		if gotCheckID != checkID {
			return nil, errors.New(
				"pending evidence reader received the wrong check ID",
			)
		}
		return evidence, nil
	}
	return readers
}

func TestAuthorityPhysicalRollbackFromPendingExact(t *testing.T) {
	t.Parallel()
	_, pending, _, _, _, _, _ := authorityPendingArtifactFixture(t)
	synthetic := authorityPhysicalRollbackFromPending(pending)
	row, to, oldTip, digest, found, err :=
		decodeAuthorityPhysicalRollbackRows(
			[]authorityPhysicalRollbackRow{synthetic},
			pending.EventSeq,
		)
	if err != nil || !found {
		t.Fatalf("synthetic pending header rejected: found=%v err=%v", found, err)
	}
	if err := validateAuthorityPendingRollbackHeader(
		pending,
		row,
		to,
		oldTip,
		digest,
	); err != nil {
		t.Fatalf("synthetic pending header differs: %v", err)
	}
}

func TestValidateAuthorityPendingRollbackArtifactsCrashCuts(t *testing.T) {
	t.Parallel()
	for name, test := range map[string]struct {
		state         string
		headerFound   bool
		complete      bool
		validationErr error
		wantError     bool
		corruptHeader bool
	}{
		"reserved empty": {
			state: "reserved",
		},
		"reserved exact": {
			state:    "reserved",
			complete: true,
		},
		"reserved header": {
			state:       "reserved",
			headerFound: true,
			complete:    true,
			wantError:   true,
		},
		"reserved partial": {
			state:         "reserved",
			validationErr: errors.New("partial invalidations"),
			wantError:     true,
		},
		"written empty": {
			state:     "invalidations_written",
			wantError: true,
		},
		"written exact header absent": {
			state:    "invalidations_written",
			complete: true,
		},
		"written exact header present": {
			state:       "invalidations_written",
			headerFound: true,
			complete:    true,
		},
		"written corrupt header": {
			state:         "invalidations_written",
			headerFound:   true,
			complete:      true,
			corruptHeader: true,
			wantError:     true,
		},
	} {
		name, test := name, test
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			record, pending, header, to, oldTip, digest, evidence :=
				authorityPendingArtifactFixture(t)
			pending.State = test.state
			record.PendingRollback = &pending
			readers := authorityPendingArtifactTestReaders(
				t,
				pending.CheckID,
				evidence,
			)
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
				if test.corruptHeader {
					header.Reason += "!"
				}
				return header, to, oldTip, digest, test.headerFound, nil
			}
			readers.validateInvalidations = func(
				_ context.Context,
				_ authorityRecord,
				got authorityPhysicalRollbackRow,
				gotTo authorityPoint,
				gotOld authorityPoint,
			) (bool, error) {
				if got.RollbackID != uuid.UUID(pending.ID) ||
					got.EventSeq != pending.EventSeq ||
					gotTo != pending.To ||
					gotOld != pending.OldPhysical.Point {
					return false, errors.New(
						"invalidation validator did not receive pending authority",
					)
				}
				return test.complete, test.validationErr
			}
			err := validateAuthorityPendingRollbackArtifacts(
				context.Background(),
				record,
				readers,
			)
			if test.wantError && err == nil {
				t.Fatal("invalid pending crash cut was accepted")
			}
			if !test.wantError && err != nil {
				t.Fatalf("valid pending crash cut rejected: %v", err)
			}
		})
	}
}

func TestValidateAuthorityPendingRollbackArtifactsRejectsBadAuthority(
	t *testing.T,
) {
	t.Parallel()
	for name, mutate := range map[string]func(
		*authorityRecord,
		*authorityPendingRollback,
	){
		"state": func(
			_ *authorityRecord,
			pending *authorityPendingRollback,
		) {
			pending.State = "other"
		},
		"anchor": func(
			record *authorityRecord,
			_ *authorityPendingRollback,
		) {
			record.Physical.EventSeq++
		},
		"operator duplicate": func(
			_ *authorityRecord,
			pending *authorityPendingRollback,
		) {
			pending.Operators[1] = pending.Operators[0]
		},
		"evidence overflow": func(
			_ *authorityRecord,
			pending *authorityPendingRollback,
		) {
			pending.EvidenceCount = 65536
		},
		"time": func(
			_ *authorityRecord,
			pending *authorityPendingRollback,
		) {
			pending.StartedAt = pending.StartedAt.Add(1)
		},
	} {
		name, mutate := name, mutate
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			record, pending, _, _, _, _, evidence :=
				authorityPendingArtifactFixture(t)
			pending.Operators = append([]string(nil), pending.Operators...)
			mutate(&record, &pending)
			record.PendingRollback = &pending
			if err := validateAuthorityPendingRollbackArtifacts(
				context.Background(),
				record,
				authorityPendingArtifactTestReaders(
					t,
					pending.CheckID,
					evidence,
				),
			); err == nil {
				t.Fatal("bad pending rollback authority was accepted")
			}
		})
	}
}

func refreshAuthorityPendingArtifactEvidence(
	t *testing.T,
	record *authorityRecord,
	pending *authorityPendingRollback,
	evidence []authorityObservationRow,
) {
	t.Helper()
	commitment, err := canonicalAuthorityEvidenceCommitment(
		evidence,
		pending.Group,
		pending.CheckAttempt,
	)
	if err != nil {
		t.Fatal(err)
	}
	pending.EvidenceCount = commitment.Count
	pending.EvidenceDigest = commitment.Digest
	record.EvidenceCount = commitment.Count
	digest := commitment.Digest
	record.EvidenceDigest = &digest
}

func TestValidateAuthorityPendingRollbackArtifactsEvidenceAuthority(
	t *testing.T,
) {
	t.Parallel()
	tests := map[string]func(
		*testing.T,
		*authorityRecord,
		*authorityPendingRollback,
		*[]authorityObservationRow,
	){
		"disagreement": func(
			t *testing.T,
			record *authorityRecord,
			pending *authorityPendingRollback,
			evidence *[]authorityObservationRow,
		) {
			row := (*evidence)[2]
			row.Observation.Result = "disagreed"
			row.Observation.PointVerified = false
			(*evidence)[2] = refreshBindingTestRow(t, row)
			refreshAuthorityPendingArtifactEvidence(
				t,
				record,
				pending,
				*evidence,
			)
		},
		"below threshold": func(
			t *testing.T,
			record *authorityRecord,
			pending *authorityPendingRollback,
			evidence *[]authorityObservationRow,
		) {
			row := (*evidence)[1]
			row.Observation.Result = "unavailable"
			row.Observation.PointVerified = false
			(*evidence)[1] = refreshBindingTestRow(t, row)
			refreshAuthorityPendingArtifactEvidence(
				t,
				record,
				pending,
				*evidence,
			)
		},
		"arbitrary operator": func(
			_ *testing.T,
			_ *authorityRecord,
			pending *authorityPendingRollback,
			_ *[]authorityObservationRow,
		) {
			pending.Operators[1] = "operator-z"
		},
		"swapped peer": func(
			_ *testing.T,
			_ *authorityRecord,
			pending *authorityPendingRollback,
			_ *[]authorityObservationRow,
		) {
			pending.Peers[0], pending.Peers[1] =
				pending.Peers[1], pending.Peers[0]
		},
		"omitted agreed observer": func(
			_ *testing.T,
			_ *authorityRecord,
			pending *authorityPendingRollback,
			_ *[]authorityObservationRow,
		) {
			pending.Peers = pending.Peers[:1]
			pending.Operators = pending.Operators[:1]
		},
		"extra observer": func(
			_ *testing.T,
			_ *authorityRecord,
			pending *authorityPendingRollback,
			_ *[]authorityObservationRow,
		) {
			pending.Peers = append(pending.Peers, "relay-c")
			pending.Operators = append(pending.Operators, "operator-c")
		},
		"check ID mismatch": func(
			_ *testing.T,
			_ *authorityRecord,
			pending *authorityPendingRollback,
			_ *[]authorityObservationRow,
		) {
			pending.CheckID[0]++
		},
		"group mismatch": func(
			_ *testing.T,
			_ *authorityRecord,
			pending *authorityPendingRollback,
			_ *[]authorityObservationRow,
		) {
			pending.Group[0]++
		},
		"attempt mismatch": func(
			_ *testing.T,
			_ *authorityRecord,
			pending *authorityPendingRollback,
			_ *[]authorityObservationRow,
		) {
			pending.CheckAttempt++
		},
		"required mismatch": func(
			_ *testing.T,
			_ *authorityRecord,
			pending *authorityPendingRollback,
			_ *[]authorityObservationRow,
		) {
			pending.Required++
		},
		"checked event mismatch": func(
			_ *testing.T,
			_ *authorityRecord,
			pending *authorityPendingRollback,
			_ *[]authorityObservationRow,
		) {
			pending.CheckedEventSeq++
		},
		"completed current check": func(
			_ *testing.T,
			record *authorityRecord,
			_ *authorityPendingRollback,
			_ *[]authorityObservationRow,
		) {
			record.TrustStatus = "agreed"
		},
		"non-frozen current evidence": func(
			_ *testing.T,
			record *authorityRecord,
			_ *authorityPendingRollback,
			_ *[]authorityObservationRow,
		) {
			record.EvidenceState = "open"
		},
		"corrupt evidence row": func(
			_ *testing.T,
			_ *authorityRecord,
			_ *authorityPendingRollback,
			evidence *[]authorityObservationRow,
		) {
			(*evidence)[0].Digest[0]++
		},
		"ninth evidence replay": func(
			_ *testing.T,
			_ *authorityRecord,
			_ *authorityPendingRollback,
			evidence *[]authorityObservationRow,
		) {
			replayed := make([]authorityObservationRow, 0, len(*evidence)+8)
			for range 9 {
				replayed = append(replayed, (*evidence)[0])
			}
			replayed = append(replayed, (*evidence)[1:]...)
			*evidence = replayed
		},
	}
	for name, mutate := range tests {
		name, mutate := name, mutate
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			record, pending, _, _, _, _, evidence :=
				authorityPendingArtifactFixture(t)
			pending.Peers = append([]string(nil), pending.Peers...)
			pending.Operators = append([]string(nil), pending.Operators...)
			evidence = append([]authorityObservationRow(nil), evidence...)
			mutate(t, &record, &pending, &evidence)
			record.PendingRollback = &pending
			readers := authorityPendingArtifactTestReaders(
				t,
				pending.CheckID,
				evidence,
			)
			if err := validateAuthorityPendingRollbackArtifacts(
				context.Background(),
				record,
				readers,
			); err == nil {
				t.Fatal("corrupt pending rollback evidence authority was accepted")
			}
		})
	}
}

func TestValidateAuthorityPendingRollbackArtifactsObserverOrderIndependent(
	t *testing.T,
) {
	t.Parallel()
	record, pending, _, _, _, _, evidence :=
		authorityPendingArtifactFixture(t)
	pending.Peers = []string{"relay-b", " relay-a "}
	pending.Operators = []string{"operator-b", " Operator-A "}
	record.PendingRollback = &pending
	if err := validateAuthorityPendingRollbackArtifacts(
		context.Background(),
		record,
		authorityPendingArtifactTestReaders(
			t,
			pending.CheckID,
			evidence,
		),
	); err != nil {
		t.Fatalf("reordered exact pending observer map rejected: %v", err)
	}
}

func TestValidateAuthorityPendingRollbackArtifactsDepthZeroEmptySet(
	t *testing.T,
) {
	t.Parallel()
	record, pending, _, _, _, _, evidence := authorityPendingArtifactFixture(t)
	pending.State = "invalidations_written"
	pending.Depth = 0
	pending.OldPhysical.Point = pending.To
	record.Physical = pending.OldPhysical
	record.PendingRollback = &pending
	readers := authorityPendingArtifactTestReaders(
		t,
		pending.CheckID,
		evidence,
	)
	readers.validateInvalidations = func(
		_ context.Context,
		_ authorityRecord,
		header authorityPhysicalRollbackRow,
		to authorityPoint,
		oldTip authorityPoint,
	) (bool, error) {
		if header.Depth != 0 || to != oldTip {
			return false, errors.New("depth-zero pending authority was not exact")
		}
		return true, nil
	}
	if err := validateAuthorityPendingRollbackArtifacts(
		context.Background(),
		record,
		readers,
	); err != nil {
		t.Fatalf("depth-zero exact empty invalidation set rejected: %v", err)
	}
}

type authorityBarrierProbeCall struct {
	kind  authorityArtifactKind
	lower uint64
	upper *uint64
}

func TestValidateAuthorityArtifactBarriersNoPendingAllowsFutureAdoption(
	t *testing.T,
) {
	t.Parallel()
	record := authorityRecord{Physical: authorityHead{EventSeq: 5}}
	var calls []authorityBarrierProbeCall
	probe := func(
		_ context.Context,
		kind authorityArtifactKind,
		lower uint64,
		upper *uint64,
	) (uint64, bool, error) {
		if kind == authorityArtifactAdoption {
			return 6, true, nil
		}
		calls = append(calls, authorityBarrierProbeCall{
			kind:  kind,
			lower: lower,
			upper: upper,
		})
		return 0, false, nil
	}
	if err := validateAuthorityArtifactBarriers(
		context.Background(),
		record,
		probe,
	); err != nil {
		t.Fatalf("future adoption-only crash remnant rejected: %v", err)
	}
	if len(calls) != 2 ||
		calls[0].kind != authorityArtifactRollback ||
		calls[1].kind != authorityArtifactInvalidation ||
		calls[0].lower != 5 ||
		calls[1].lower != 5 ||
		calls[0].upper != nil ||
		calls[1].upper != nil {
		t.Fatalf("no-pending barrier calls = %#v", calls)
	}
}

func TestValidateAuthorityArtifactBarriersNoPendingRejectsRollbackArtifacts(
	t *testing.T,
) {
	t.Parallel()
	for _, foundKind := range []authorityArtifactKind{
		authorityArtifactRollback,
		authorityArtifactInvalidation,
	} {
		foundKind := foundKind
		t.Run(string(foundKind), func(t *testing.T) {
			t.Parallel()
			record := authorityRecord{Physical: authorityHead{EventSeq: 5}}
			if err := validateAuthorityArtifactBarriers(
				context.Background(),
				record,
				func(
					_ context.Context,
					kind authorityArtifactKind,
					lower uint64,
					upper *uint64,
				) (uint64, bool, error) {
					if upper != nil || lower != 5 {
						t.Fatalf(
							"wrong no-pending range (%d,%v)",
							lower,
							upper,
						)
					}
					return 6, kind == foundKind, nil
				},
			); err == nil {
				t.Fatal("future unreserved rollback artifact was accepted")
			}
		})
	}
}

func authorityBarrierPendingRecord() authorityRecord {
	old := authorityHead{EventSeq: 5}
	pending := authorityPendingRollback{
		State:       "reserved",
		EventSeq:    10,
		OldPhysical: old,
	}
	return authorityRecord{
		Physical:        old,
		PendingRollback: &pending,
	}
}

func TestValidateAuthorityArtifactBarriersPendingRanges(t *testing.T) {
	t.Parallel()
	record := authorityBarrierPendingRecord()
	var calls []authorityBarrierProbeCall
	err := validateAuthorityArtifactBarriers(
		context.Background(),
		record,
		func(
			_ context.Context,
			kind authorityArtifactKind,
			lower uint64,
			upper *uint64,
		) (uint64, bool, error) {
			var copied *uint64
			if upper != nil {
				value := *upper
				copied = &value
			}
			calls = append(calls, authorityBarrierProbeCall{
				kind:  kind,
				lower: lower,
				upper: copied,
			})
			return 0, false, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(calls) != 5 ||
		calls[0].kind != authorityArtifactAdoption ||
		calls[0].lower != 5 ||
		calls[0].upper != nil {
		t.Fatalf("pending barrier calls = %#v", calls)
	}
	for index, kind := range []authorityArtifactKind{
		authorityArtifactRollback,
		authorityArtifactInvalidation,
	} {
		between := calls[1+index*2]
		after := calls[2+index*2]
		if between.kind != kind ||
			between.lower != 5 ||
			between.upper == nil ||
			*between.upper != 10 ||
			after.kind != kind ||
			after.lower != 10 ||
			after.upper != nil {
			t.Fatalf(
				"%s pending ranges: between=%#v after=%#v",
				kind,
				between,
				after,
			)
		}
	}
}

func TestValidateAuthorityArtifactBarriersPendingRejectsEveryForbiddenRange(
	t *testing.T,
) {
	t.Parallel()
	for name, test := range map[string]struct {
		kind       authorityArtifactKind
		lower      uint64
		upper      uint64
		foundEvent uint64
	}{
		"adoption allocator gap": {
			kind:       authorityArtifactAdoption,
			lower:      5,
			foundEvent: 6,
		},
		"adoption after pending": {
			kind:       authorityArtifactAdoption,
			lower:      5,
			foundEvent: 11,
		},
		"adoption at pending": {
			kind:       authorityArtifactAdoption,
			lower:      5,
			foundEvent: 10,
		},
		"rollback allocator gap": {
			kind:       authorityArtifactRollback,
			lower:      5,
			upper:      10,
			foundEvent: 6,
		},
		"rollback after pending": {
			kind:       authorityArtifactRollback,
			lower:      10,
			foundEvent: 11,
		},
		"invalidation allocator gap": {
			kind:       authorityArtifactInvalidation,
			lower:      5,
			upper:      10,
			foundEvent: 6,
		},
		"invalidation after pending": {
			kind:       authorityArtifactInvalidation,
			lower:      10,
			foundEvent: 11,
		},
	} {
		name, test := name, test
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			record := authorityBarrierPendingRecord()
			if err := validateAuthorityArtifactBarriers(
				context.Background(),
				record,
				func(
					_ context.Context,
					kind authorityArtifactKind,
					lower uint64,
					upper *uint64,
				) (uint64, bool, error) {
					upperValue := uint64(0)
					if upper != nil {
						upperValue = *upper
					}
					if kind == test.kind &&
						lower == test.lower &&
						upperValue == test.upper {
						return test.foundEvent, true, nil
					}
					return 0, false, nil
				},
			); err == nil {
				t.Fatal("forbidden pending artifact range was accepted")
			}
		})
	}
}

func TestAuthorityArtifactProbeUsesDedicatedLimits(t *testing.T) {
	t.Parallel()
	limits := authorityArtifactProbePhaseLimits()
	if limits.MaxResultRows != 1 {
		t.Fatalf("probe max result rows = %d", limits.MaxResultRows)
	}
}
