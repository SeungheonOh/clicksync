package clickhouse

import (
	"crypto/sha256"
	"strings"
	"testing"
	"time"
)

func bindingTestRow(
	t *testing.T,
	checkID [16]byte,
	group [16]byte,
	attempt uint32,
	required uint16,
	checked authorityHead,
	ordinal uint32,
	operator string,
	result string,
) authorityObservationRow {
	t.Helper()
	slot := checked.Point.Slot
	hash := checked.Point.Hash
	number := checked.Point.BlockNumber
	isByron := checked.Point.IsByronEBB
	var (
		checkpointSlot   *uint64
		checkpointHash   *authorityHash
		checkpointNumber *uint64
		checkpointByron  *bool
		checkedSlot      *uint64
		checkedHash      *authorityHash
		checkedNumber    *uint64
	)
	if !checked.Point.Origin {
		checkpointSlot = &slot
		checkpointHash = &hash
		checkpointNumber = &number
		checkpointByron = &isByron
		checkedSlot = &slot
		checkedHash = &hash
		checkedNumber = &number
	}
	observation := authorityObservation{
		Kind:                   "checkpoint",
		PeerHost:               "relay-" + operator,
		PeerAddress:            "192.0.2.1:3001",
		Operator:               operator,
		N2NVersion:             15,
		NetworkMagic:           764824073,
		TipSlot:                slot + 1,
		TipHash:                authorityFill32(byte(ordinal + 0x60)),
		TipBlockNumber:         number + 1,
		CheckpointSlot:         checkpointSlot,
		CheckpointHash:         checkpointHash,
		CheckpointBlockNumber:  checkpointNumber,
		CheckpointIsByronEBB:   checkpointByron,
		CheckID:                checkID,
		AgreementGroup:         group,
		CheckAttempt:           attempt,
		EvidenceOrdinal:        ordinal,
		ProofMethod:            "chain_sync_singleton",
		CorroborationRequired:  required,
		CheckedEventSeq:        checked.EventSeq,
		CheckedPointOrigin:     checked.Point.Origin,
		CheckedPointSlot:       checkedSlot,
		CheckedPointHash:       checkedHash,
		CheckedBlockNumber:     checkedNumber,
		CheckedPointIsByronEBB: isByron,
		PointVerified:          result == "agreed",
		Result:                 result,
		Reason:                 result,
		ObservedAt: time.Date(
			2026, time.July, 23, 12, 0, int(ordinal), 123456000, time.UTC,
		),
	}
	return refreshBindingTestRow(t, authorityObservationRow{
		Observation: observation,
		OperatorKey: strings.ToLower(strings.TrimSpace(operator)),
	})
}

func refreshBindingTestRow(
	t *testing.T,
	row authorityObservationRow,
) authorityObservationRow {
	t.Helper()
	if err := finalizeAuthorityObservationIdentity(&row.Observation); err != nil {
		t.Fatal(err)
	}
	return digestBindingTestRow(t, row)
}

func digestBindingTestRow(
	t *testing.T,
	row authorityObservationRow,
) authorityObservationRow {
	t.Helper()
	payload, err := canonicalAuthorityObservationPayload(row.Observation)
	if err != nil {
		t.Fatal(err)
	}
	row.Digest = authorityHash(sha256.Sum256([]byte(payload)))
	return row
}

func bindingPendingFromRow(
	t *testing.T,
	row authorityObservationRow,
) *authorityPendingEvidence {
	t.Helper()
	payload, err := canonicalAuthorityObservationPayload(row.Observation)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeAuthorityObservation(payload)
	if err != nil {
		t.Fatal(err)
	}
	return &authorityPendingEvidence{
		Observation: decoded,
		Digest:      row.Digest,
		Payload:     payload,
	}
}

func bindingTestFixture(
	t *testing.T,
) (authorityRecord, []authorityObservationRow) {
	t.Helper()
	checkID := authorityFill16(0x41)
	group := authorityFill16(0x42)
	checked := authorityHead{
		EventSeq: 9,
		Point: authorityPoint{
			Slot:        100,
			Hash:        authorityFill32(0x51),
			BlockNumber: 10,
		},
	}
	rows := []authorityObservationRow{
		bindingTestRow(t, checkID, group, 3, 2, checked, 1, "Operator A", "agreed"),
		bindingTestRow(t, checkID, group, 3, 2, checked, 2, "Operator B", "agreed"),
	}
	commitment, err := canonicalAuthorityEvidenceCommitment(rows, group, 3)
	if err != nil {
		t.Fatal(err)
	}
	completed := time.Date(2026, time.July, 23, 13, 0, 0, 0, time.UTC)
	record := authorityRecord{
		CheckID:                &checkID,
		AgreementGroup:         &group,
		CheckAttempt:           3,
		CorroborationRequired:  2,
		CorroborationConfirmed: 2,
		CheckCompletedAt:       &completed,
		EvidenceState:          "frozen",
		EvidenceCount:          commitment.Count,
		EvidenceDigest:         &commitment.Digest,
		Checked:                &checked,
		LastAgreedEvidence: &authorityEvidenceReference{
			CheckID:   checkID,
			Group:     group,
			Attempt:   3,
			Required:  2,
			Confirmed: 2,
			Checked:   checked,
			Count:     commitment.Count,
			Digest:    commitment.Digest,
		},
	}
	return record, rows
}

func TestBindAuthorityEvidenceExactIdentityProvenanceAndOperators(t *testing.T) {
	t.Parallel()
	record, rows := bindingTestFixture(t)
	if err := bindAuthorityEvidence(record, rows, nil); err != nil {
		t.Fatalf("valid evidence failed to bind: %v", err)
	}
	tests := map[string]func(*authorityObservationRow){
		"check ID": func(row *authorityObservationRow) {
			row.Observation.CheckID[0] ^= 1
		},
		"group": func(row *authorityObservationRow) {
			row.Observation.AgreementGroup[0] ^= 1
		},
		"attempt": func(row *authorityObservationRow) {
			row.Observation.CheckAttempt++
		},
		"required": func(row *authorityObservationRow) {
			row.Observation.CorroborationRequired++
		},
		"checked event": func(row *authorityObservationRow) {
			row.Observation.CheckedEventSeq++
		},
		"checked point": func(row *authorityObservationRow) {
			hash := *row.Observation.CheckedPointHash
			hash[0] ^= 1
			row.Observation.CheckedPointHash = &hash
		},
		"proof": func(row *authorityObservationRow) {
			row.Observation.ProofMethod = "unverified"
		},
	}
	for name, mutate := range tests {
		name, mutate := name, mutate
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			corrupt := append([]authorityObservationRow(nil), rows...)
			mutate(&corrupt[0])
			corrupt[0] = refreshBindingTestRow(t, corrupt[0])
			if err := bindAuthorityEvidence(record, corrupt, nil); err == nil {
				t.Fatal("corrupt evidence binding was accepted")
			}
		})
	}

	duplicateOperator := append([]authorityObservationRow(nil), rows...)
	duplicateOperator[1].Observation.Operator = " operator a "
	duplicateOperator[1].OperatorKey = "operator a"
	duplicateOperator[1] = refreshBindingTestRow(t, duplicateOperator[1])
	if err := bindAuthorityEvidence(record, duplicateOperator, nil); err == nil {
		t.Fatal("duplicate normalized operator was accepted")
	}
}

func TestBindAuthorityEvidenceCurrentCommitmentPendingAndOutcomes(t *testing.T) {
	t.Parallel()
	record, rows := bindingTestFixture(t)
	originalReference := *record.LastAgreedEvidence
	record.LastAgreedEvidence = nil

	badCount := record
	badCount.EvidenceCount--
	if err := bindAuthorityEvidence(badCount, rows, nil); err == nil {
		t.Fatal("wrong current evidence count was accepted")
	}
	badDigest := record
	digest := *badDigest.EvidenceDigest
	digest[0] ^= 1
	badDigest.EvidenceDigest = &digest
	if err := bindAuthorityEvidence(badDigest, rows, nil); err == nil {
		t.Fatal("wrong current evidence digest was accepted")
	}
	badOutcome := record
	badOutcome.CorroborationConfirmed--
	if err := bindAuthorityEvidence(badOutcome, rows, nil); err == nil {
		t.Fatal("wrong completed confirmation count was accepted")
	}

	checking := record
	checking.CheckCompletedAt = nil
	checking.CorroborationConfirmed = 0
	checking.Disagreement = false
	if err := bindAuthorityEvidence(checking, rows, nil); err != nil {
		t.Fatalf("checking transient counters were bound: %v", err)
	}
	checking.CorroborationConfirmed = 1
	if err := bindAuthorityEvidence(checking, rows, nil); err == nil {
		t.Fatal("checking state with a completed confirmation count was accepted")
	}
	checking.CorroborationConfirmed = 0
	checking.Disagreement = true
	if err := bindAuthorityEvidence(checking, rows, nil); err == nil {
		t.Fatal("checking state with a completed disagreement was accepted")
	}
	checking.Disagreement = false

	commitment, err := canonicalAuthorityEvidenceCommitment(
		rows,
		*record.AgreementGroup,
		record.CheckAttempt,
	)
	if err != nil {
		t.Fatal(err)
	}
	pending := checking
	pending.EvidenceCount = 1
	prefix := commitment.PrefixDigests[1]
	pending.EvidenceDigest = &prefix
	pending.PendingEvidenceWrite = bindingPendingFromRow(t, rows[1])
	if err := bindAuthorityEvidence(pending, rows[:1], nil); err != nil {
		t.Fatalf("not-yet-physical pending reservation failed to bind: %v", err)
	}
	if err := bindAuthorityEvidence(pending, rows, nil); err != nil {
		t.Fatalf("physical pending reservation failed to bind: %v", err)
	}
	wrongPending := pending
	pendingWrite := *pending.PendingEvidenceWrite
	wrongPending.PendingEvidenceWrite = &pendingWrite
	wrongPending.PendingEvidenceWrite.Payload = "different"
	if err := bindAuthorityEvidence(wrongPending, rows, nil); err == nil {
		t.Fatal("physical row differing from the reservation was accepted")
	}

	wrongPrefix := pending
	prefix = *pending.EvidenceDigest
	prefix[0] ^= 1
	wrongPrefix.EvidenceDigest = &prefix
	if err := bindAuthorityEvidence(wrongPrefix, rows[:1], nil); err == nil {
		t.Fatal("pending reservation over the wrong committed prefix was accepted")
	}

	third := bindingTestRow(
		t,
		*record.CheckID,
		*record.AgreementGroup,
		record.CheckAttempt,
		record.CorroborationRequired,
		*record.Checked,
		3,
		"Operator C",
		"agreed",
	)
	if err := bindAuthorityEvidence(
		pending,
		append(append([]authorityObservationRow(nil), rows...), third),
		nil,
	); err == nil {
		t.Fatal("two physical rows beyond the committed prefix were accepted")
	}

	pending.LastAgreedEvidence = &originalReference
	if err := bindAuthorityEvidence(pending, rows, nil); err == nil {
		t.Fatal("same-check immutable LastAgreed accepted a pending extension")
	}

	open := record
	open.LastAgreedEvidence = &originalReference
	open.CheckCompletedAt = nil
	open.CorroborationConfirmed = 0
	open.Disagreement = false
	open.PendingEvidenceWrite = nil
	open.EvidenceState = "open"
	if err := bindAuthorityEvidence(open, rows, nil); err == nil {
		t.Fatal("same-check immutable LastAgreed accepted open evidence without a pending extension")
	}
}

func TestBindAuthorityEvidenceProspectivePendingCorruption(t *testing.T) {
	t.Parallel()
	record, rows := bindingTestFixture(t)
	record.LastAgreedEvidence = nil
	record.CheckCompletedAt = nil
	record.CorroborationConfirmed = 0
	record.EvidenceCount = 1
	commitment, err := canonicalAuthorityEvidenceCommitment(
		rows,
		*record.AgreementGroup,
		record.CheckAttempt,
	)
	if err != nil {
		t.Fatal(err)
	}
	prefix := commitment.PrefixDigests[1]
	record.EvidenceDigest = &prefix

	tests := map[string]func(authorityObservationRow) authorityObservationRow{
		"duplicate ID and evidence identity": func(_ authorityObservationRow) authorityObservationRow {
			duplicate := rows[0]
			duplicate.Observation.EvidenceOrdinal = 2
			return refreshBindingTestRow(t, duplicate)
		},
		"duplicate evidence identity": func(row authorityObservationRow) authorityObservationRow {
			row.Observation.EvidenceIdentity = rows[0].Observation.EvidenceIdentity
			return digestBindingTestRow(t, row)
		},
		"duplicate operator same outcome": func(row authorityObservationRow) authorityObservationRow {
			row.Observation.Operator = " operator a "
			row.OperatorKey = "operator a"
			return refreshBindingTestRow(t, row)
		},
		"duplicate operator different outcome": func(row authorityObservationRow) authorityObservationRow {
			row.Observation.Operator = " operator a "
			row.Observation.Result = "disagreed"
			row.Observation.PointVerified = false
			row.OperatorKey = "operator a"
			return refreshBindingTestRow(t, row)
		},
		"ordinal": func(row authorityObservationRow) authorityObservationRow {
			row.Observation.EvidenceOrdinal++
			return refreshBindingTestRow(t, row)
		},
		"group": func(row authorityObservationRow) authorityObservationRow {
			row.Observation.AgreementGroup[0] ^= 1
			return refreshBindingTestRow(t, row)
		},
		"attempt": func(row authorityObservationRow) authorityObservationRow {
			row.Observation.CheckAttempt++
			return refreshBindingTestRow(t, row)
		},
		"proof": func(row authorityObservationRow) authorityObservationRow {
			row.Observation.ProofMethod = "unverified"
			return refreshBindingTestRow(t, row)
		},
	}
	for name, corrupt := range tests {
		name, corrupt := name, corrupt
		t.Run(name, func(t *testing.T) {
			candidate := corrupt(rows[1])
			mutated := record
			mutated.PendingEvidenceWrite = bindingPendingFromRow(t, candidate)
			if err := bindAuthorityEvidence(mutated, rows[:1], nil); err == nil {
				t.Fatal("corrupt absent pending reservation was accepted")
			}
		})
	}
}

func TestBindAuthorityEvidencePendingAtEmptyPrefix(t *testing.T) {
	t.Parallel()
	record, rows := bindingTestFixture(t)
	record.LastAgreedEvidence = nil
	record.CheckCompletedAt = nil
	record.CorroborationConfirmed = 0
	record.EvidenceCount = 0
	empty, err := canonicalAuthorityEvidenceCommitment(
		nil,
		*record.AgreementGroup,
		record.CheckAttempt,
	)
	if err != nil {
		t.Fatal(err)
	}
	record.EvidenceDigest = &empty.Digest
	record.PendingEvidenceWrite = bindingPendingFromRow(t, rows[0])
	if err := bindAuthorityEvidence(record, nil, nil); err != nil {
		t.Fatalf("absent first pending row failed to bind: %v", err)
	}
	if err := bindAuthorityEvidence(record, rows[:1], nil); err != nil {
		t.Fatalf("present first pending row failed to bind: %v", err)
	}
}

func TestBindAuthorityEvidenceRejectsOriginWithCheckedPointFields(t *testing.T) {
	t.Parallel()
	checkID := authorityFill16(0x71)
	group := authorityFill16(0x72)
	checked := authorityHead{
		EventSeq: 5,
		Point:    authorityPoint{Origin: true},
	}
	rows := []authorityObservationRow{
		bindingTestRow(t, checkID, group, 1, 2, checked, 1, "Operator A", "agreed"),
		bindingTestRow(t, checkID, group, 1, 2, checked, 2, "Operator B", "agreed"),
	}
	commitment, err := canonicalAuthorityEvidenceCommitment(rows, group, 1)
	if err != nil {
		t.Fatal(err)
	}
	completed := time.Date(2026, time.July, 23, 13, 0, 0, 0, time.UTC)
	record := authorityRecord{
		CheckID:                &checkID,
		AgreementGroup:         &group,
		CheckAttempt:           1,
		CorroborationRequired:  2,
		CorroborationConfirmed: 2,
		CheckCompletedAt:       &completed,
		EvidenceCount:          commitment.Count,
		EvidenceDigest:         &commitment.Digest,
		Checked:                &checked,
	}
	if err := bindAuthorityEvidence(record, rows, nil); err != nil {
		t.Fatalf("valid Origin evidence failed to bind: %v", err)
	}
	slot := uint64(1)
	hash := authorityFill32(0x73)
	number := uint64(1)
	rows[0].Observation.CheckedPointSlot = &slot
	rows[0].Observation.CheckedPointHash = &hash
	rows[0].Observation.CheckedBlockNumber = &number
	rows[0] = refreshBindingTestRow(t, rows[0])
	if err := bindAuthorityEvidence(record, rows, nil); err == nil {
		t.Fatal("Origin evidence carrying checked-point fields was accepted")
	}
}

func TestBindAuthorityEvidenceLastAgreedIsImmutableAndNonDisputed(t *testing.T) {
	t.Parallel()
	record, rows := bindingTestFixture(t)
	record.CheckID = nil
	record.AgreementGroup = nil
	record.Checked = nil
	record.EvidenceDigest = nil
	record.EvidenceCount = 0
	record.CheckCompletedAt = nil
	if err := bindAuthorityEvidence(record, nil, rows); err != nil {
		t.Fatalf("valid historical authority failed to bind: %v", err)
	}

	wrongConfirmed := record
	reference := *record.LastAgreedEvidence
	wrongConfirmed.LastAgreedEvidence = &reference
	wrongConfirmed.LastAgreedEvidence.Confirmed--
	if err := bindAuthorityEvidence(wrongConfirmed, nil, rows); err == nil {
		t.Fatal("wrong immutable confirmed count was accepted")
	}
	wrongDigest := record
	reference = *record.LastAgreedEvidence
	wrongDigest.LastAgreedEvidence = &reference
	wrongDigest.LastAgreedEvidence.Digest[0] ^= 1
	if err := bindAuthorityEvidence(wrongDigest, nil, rows); err == nil {
		t.Fatal("wrong immutable digest was accepted")
	}

	disputed := append([]authorityObservationRow(nil), rows...)
	disputed = append(disputed, bindingTestRow(
		t,
		record.LastAgreedEvidence.CheckID,
		record.LastAgreedEvidence.Group,
		record.LastAgreedEvidence.Attempt,
		record.LastAgreedEvidence.Required,
		record.LastAgreedEvidence.Checked,
		3,
		"Operator C",
		"disagreed",
	))
	commitment, err := canonicalAuthorityEvidenceCommitment(
		disputed,
		record.LastAgreedEvidence.Group,
		record.LastAgreedEvidence.Attempt,
	)
	if err != nil {
		t.Fatal(err)
	}
	disputedRecord := record
	reference = *record.LastAgreedEvidence
	disputedRecord.LastAgreedEvidence = &reference
	disputedRecord.LastAgreedEvidence.Count = commitment.Count
	disputedRecord.LastAgreedEvidence.Digest = commitment.Digest
	if err := bindAuthorityEvidence(disputedRecord, nil, disputed); err == nil {
		t.Fatal("disputed evidence was accepted as last-agreed authority")
	}
}
