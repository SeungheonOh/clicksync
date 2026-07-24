package clickhouse

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"
)

func commitmentTestRow(
	t *testing.T,
	group [16]byte,
	attempt uint32,
	ordinal uint32,
	seed byte,
) authorityObservationRow {
	t.Helper()
	slot := uint64(100)
	number := uint64(10)
	isByron := false
	pointHash := authorityFill32(0x51)
	observation := authorityObservation{
		Kind:                  "checkpoint",
		PeerHost:              "relay",
		PeerAddress:           "192.0.2.1:3001",
		Operator:              "operator",
		N2NVersion:            15,
		NetworkMagic:          764824073,
		TipSlot:               101,
		TipHash:               authorityFill32(seed + 1),
		TipBlockNumber:        11,
		CheckpointSlot:        &slot,
		CheckpointHash:        &pointHash,
		CheckpointBlockNumber: &number,
		CheckpointIsByronEBB:  &isByron,
		CheckID:               authorityFill16(0x41),
		AgreementGroup:        group,
		CheckAttempt:          attempt,
		EvidenceOrdinal:       ordinal,
		ProofMethod:           "chain_sync_singleton",
		CorroborationRequired: 2,
		CheckedEventSeq:       9,
		CheckedPointSlot:      &slot,
		CheckedPointHash:      &pointHash,
		CheckedBlockNumber:    &number,
		PointVerified:         true,
		Result:                "agreed",
		Reason:                string([]byte{seed}),
		ObservedAt: time.Date(
			2026, time.July, 23, 12, 0, int(seed), 123456000, time.UTC,
		),
	}
	if err := finalizeAuthorityObservationIdentity(&observation); err != nil {
		t.Fatal(err)
	}
	payload, err := canonicalAuthorityObservationPayload(observation)
	if err != nil {
		t.Fatal(err)
	}
	return authorityObservationRow{
		Observation: observation,
		Digest:      authorityHash(sha256.Sum256([]byte(payload))),
	}
}

func TestCanonicalAuthorityEvidenceCommitmentEmptyAndKnownVector(t *testing.T) {
	t.Parallel()
	group := authorityFill16(0x42)
	empty, err := canonicalAuthorityEvidenceCommitment(nil, group, 3)
	if err != nil {
		t.Fatal(err)
	}
	expectedEmpty := sha256.Sum256([]byte("clicksync-trust-evidence-set\x00"))
	if empty.Count != 0 ||
		empty.Digest != authorityHash(expectedEmpty) ||
		len(empty.Rows) != 0 ||
		len(empty.PrefixDigests) != 1 ||
		empty.PrefixDigests[0] != empty.Digest {
		t.Fatalf("empty commitment = %#v", empty)
	}

	rows := []authorityObservationRow{
		commitmentTestRow(t, group, 3, 1, 1),
		commitmentTestRow(t, group, 3, 2, 2),
	}
	commitment, err := canonicalAuthorityEvidenceCommitment(rows, group, 3)
	if err != nil {
		t.Fatal(err)
	}
	const expectedHex = "4e8ca5cd2e609ea84fd13f0cac97ab31bd3cd06a3fc808870b4433f271f7bed7"
	if got := hex.EncodeToString(commitment.Digest[:]); got != expectedHex {
		t.Fatalf("commitment vector = %s", got)
	}
	if commitment.Count != 2 ||
		len(commitment.Rows) != 2 ||
		len(commitment.PrefixDigests) != 3 ||
		commitment.PrefixDigests[2] != commitment.Digest {
		t.Fatalf("commitment shape = %#v", commitment)
	}
}

func TestCanonicalAuthorityEvidenceReplayBounds(t *testing.T) {
	t.Parallel()
	group := authorityFill16(0x42)
	row := commitmentTestRow(t, group, 1, 1, 1)
	eight := make([]authorityObservationRow, manifestDuplicateLimit)
	for index := range eight {
		eight[index] = row
	}
	commitment, err := canonicalAuthorityEvidenceCommitment(eight, group, 1)
	if err != nil {
		t.Fatal(err)
	}
	if commitment.Count != 1 || len(commitment.Rows) != 1 {
		t.Fatalf("physical replays were not grouped: %#v", commitment)
	}
	if _, err := canonicalAuthorityEvidenceCommitment(
		append(eight, row),
		group,
		1,
	); err == nil {
		t.Fatal("ninth physical replay was accepted")
	}
}

func TestCanonicalAuthorityEvidenceRejectsCorruptGrouping(t *testing.T) {
	t.Parallel()
	group := authorityFill16(0x42)
	first := commitmentTestRow(t, group, 2, 1, 1)
	second := commitmentTestRow(t, group, 2, 2, 2)
	tests := map[string][]authorityObservationRow{
		"gap": {
			first,
			commitmentTestRow(t, group, 2, 3, 3),
		},
		"conflicting replay": {
			first,
			commitmentTestRow(t, group, 2, 1, 2),
		},
	}
	reusedID := first
	reusedID.Observation.EvidenceOrdinal = 2
	payload, err := canonicalAuthorityObservationPayload(reusedID.Observation)
	if err != nil {
		t.Fatal(err)
	}
	reusedID.Digest = authorityHash(sha256.Sum256([]byte(payload)))
	tests["reused ID"] = []authorityObservationRow{first, reusedID}

	reusedIdentity := second
	reusedIdentity.Observation.EvidenceIdentity = first.Observation.EvidenceIdentity
	tests["reused evidence identity"] = []authorityObservationRow{first, reusedIdentity}

	for name, rows := range tests {
		rows := rows
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if _, err := canonicalAuthorityEvidenceCommitment(rows, group, 2); err == nil {
				t.Fatal("corrupt grouping was accepted")
			}
		})
	}
	if _, err := canonicalAuthorityEvidenceCommitment(
		[]authorityObservationRow{first},
		authorityFill16(0x43),
		2,
	); err == nil {
		t.Fatal("wrong agreement group was accepted")
	}
	if _, err := canonicalAuthorityEvidenceCommitment(
		[]authorityObservationRow{first},
		group,
		3,
	); err == nil {
		t.Fatal("wrong check attempt was accepted")
	}
}
