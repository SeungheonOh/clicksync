package store

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"clicksync/internal/model"
	"clicksync/internal/publication"
	"clicksync/migrations"
)

type clickoutAuthorityGolden struct {
	SchemaContract string `json:"schema_contract_sha256"`
	Rows           []struct {
		Name             string `json:"name"`
		TransitionID     string `json:"transition_id"`
		CanonicalPayload string `json:"canonical_payload_sha256"`
		RowDigest        string `json:"row_digest"`
	} `json:"rows"`
}

func TestClickoutAuthorityGoldenMatchesProducerCanonicalizer(t *testing.T) {
	_, file, _, _ := runtime.Caller(0)
	path := filepath.Join(
		filepath.Dir(file),
		"..",
		"..",
		"clickout",
		"internal",
		"clickhouse",
		"testdata",
		"authority_golden.json",
	)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var golden clickoutAuthorityGolden
	if err := json.Unmarshal(data, &golden); err != nil {
		t.Fatal(err)
	}
	if golden.SchemaContract != hex.EncodeToString(migrations.ContractHash[:]) {
		t.Fatalf("fixture contract=%s producer=%x", golden.SchemaContract, migrations.ContractHash)
	}
	if len(golden.Rows) != 5 {
		t.Fatalf("unexpected authority golden rows: %+v", golden.Rows)
	}
	records := clickoutAuthorityProducerRecords(t)
	if len(records) != len(golden.Rows) {
		t.Fatalf("producer rows=%d golden=%d", len(records), len(golden.Rows))
	}
	for index, record := range records {
		row := golden.Rows[index]
		payload, err := manifestCanonicalPayload(record)
		if err != nil {
			t.Fatal(err)
		}
		if got := hex.EncodeToString(record.TransitionID[:]); got != row.TransitionID {
			t.Errorf("%s producer transition=%s fixture=%s", row.Name, got, row.TransitionID)
		}
		if got := hex.EncodeToString(record.RowDigest[:]); got != row.RowDigest {
			t.Errorf("%s producer row digest=%s fixture=%s", row.Name, got, row.RowDigest)
		}
		if got := sha256.Sum256(payload); hex.EncodeToString(got[:]) != row.CanonicalPayload {
			t.Errorf("%s producer payload digest=%x fixture=%s", row.Name, got, row.CanonicalPayload)
		}
		if err := verifyManifestRecord(record); err != nil {
			t.Errorf("%s producer record invalid: %v", row.Name, err)
		}
	}
}

func clickoutAuthorityProducerRecords(t *testing.T) []manifestRecord {
	t.Helper()
	at := time.Date(2026, 7, 23, 18, 0, 0, 123456000, time.UTC)
	origin := manifestHead{EventSeq: 1, Point: publication.Point{Origin: true}}
	writer := manifestID(0x31)
	official := manifestRecord{
		Revision:               1,
		TransitionKind:         "initialize",
		DatasetID:              manifestID(0x11),
		SchemaContractHash:     migrations.ContractHash,
		NetworkMagic:           mainnetMagic,
		NetworkName:            "mainnet",
		ByronGenesisID:         manifestHash(0x21),
		ByronGenesisJSONHash:   manifestHash(0x22),
		ShelleyGenesisID:       manifestHash(0x23),
		ShelleyGenesisJSONHash: manifestHash(0x24),
		Start:                  publication.Point{Origin: true},
		GenesisSeeded:          true,
		CompleteHistory:        true,
		TrustMode:              "peer_observed_structurally_verified",
		TrustStatus:            "agreed",
		TrustBasis:             "official_genesis",
		CheckpointInterval:     manifestCheckpointBlocks,
		TrustReason:            "official genesis distribution verified exactly",
		EvidenceState:          "none",
		LastAgreed:             &origin,
		LastAgreedAt:           &at,
		ServableFloor:          origin,
		ServableFloorPermanent: true,
		Physical:               origin,
		Effective:              origin,
		Servable:               true,
		VisibilityGeneration:   1,
		WriterID:               &writer,
		WriterBuild:            "authority-test",
		SourceBuild:            "authority-test",
		CreatedAt:              at,
		UpdatedAt:              at,
	}
	if err := finalizeManifestRecord(&official); err != nil {
		t.Fatal(err)
	}
	sampled := validManifestRecord(t)

	pendingEvidence := sampled
	pendingEvidence.TransitionKind = "evidence_write_reserved"
	checkID := manifestID(0x41)
	groupID := manifestID(0x42)
	pendingEvidence.CheckID = &checkID
	pendingEvidence.AgreementGroup = &groupID
	pendingEvidence.CheckAttempt = 1
	pendingEvidence.CorroborationRequired = 2
	pendingEvidence.CorroborationConfirmed = 0
	pendingEvidence.TrustStatus = "checking"
	pendingEvidence.TrustReason = "exact candidate membership check in progress"
	started := at.Add(time.Second)
	pendingEvidence.CheckStartedAt = &started
	pendingEvidence.CheckCompletedAt = nil
	pendingEvidence.EvidenceState = "open"
	pendingEvidence.EvidenceCount = 0
	empty := emptyTrustEvidenceCommitment().Digest
	pendingEvidence.EvidenceDigest = &empty
	checked := pendingEvidence.Physical
	pendingEvidence.Checked = &checked
	slot := checked.Point.Slot
	hash := checked.Point.Hash
	number := checked.Point.BlockNumber
	isByron := checked.Point.IsByronEBB
	observation := model.PeerObservation{
		Kind:                   "rollback",
		PeerHost:               "relay-pending",
		PeerAddress:            "192.0.2.41:3001",
		Operator:               "operator-pending",
		N2NVersion:             15,
		NetworkMagic:           mainnetMagic,
		TipSlot:                slot,
		TipHash:                hash,
		TipBlockNumber:         number,
		CheckpointSlot:         &slot,
		CheckpointHash:         &hash,
		CheckpointBlockNumber:  &number,
		CheckpointIsByronEBB:   &isByron,
		CheckID:                checkID,
		AgreementGroup:         groupID,
		CheckAttempt:           1,
		EvidenceOrdinal:        1,
		ProofMethod:            "paired_chain_sync_singleton",
		CorroborationRequired:  2,
		CheckedEventSeq:        checked.EventSeq,
		CheckedPointSlot:       &slot,
		CheckedPointHash:       &hash,
		CheckedBlockNumber:     &number,
		CheckedPointIsByronEBB: isByron,
		PointVerified:          true,
		Result:                 "agreed",
		ObservedAt:             at.Add(2 * time.Second),
	}
	if err := model.FinalizePeerObservationIdentity(&observation); err != nil {
		t.Fatal(err)
	}
	observationDigest, err := model.PeerObservationDigest(observation)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := canonicalPendingEvidencePayload(observation)
	if err != nil {
		t.Fatal(err)
	}
	pendingWriter := manifestID(0x43)
	pendingEvidence.PendingEvidenceWrite = &manifestPendingEvidenceWrite{
		Observation: observation,
		Digest:      observationDigest,
		Payload:     payload,
		WriterID:    pendingWriter,
		ReservedAt:  at.Add(2 * time.Second),
	}
	if err := finalizeManifestRecord(&pendingEvidence); err != nil {
		t.Fatal(err)
	}

	pendingRollback := sampled
	pendingRollback.TransitionKind = "rollback_reserved"
	pendingRollback.PendingRollback = &manifestPendingRollback{
		State:           "reserved",
		ID:              manifestID(0x51),
		EventSeq:        sampled.Physical.EventSeq + 1,
		To:              sampled.Checked.Point,
		OldPhysical:     sampled.Physical,
		Depth:           0,
		Reason:          "golden rollback reservation",
		Peers:           []string{"relay-a", "relay-b"},
		Operators:       []string{"operator-a", "operator-b"},
		Required:        sampled.CorroborationRequired,
		CheckID:         *sampled.CheckID,
		Group:           *sampled.AgreementGroup,
		CheckAttempt:    sampled.CheckAttempt,
		CheckedEventSeq: sampled.Checked.EventSeq,
		EvidenceCount:   sampled.EvidenceCount,
		EvidenceDigest:  *sampled.EvidenceDigest,
		WriterID:        manifestID(0x52),
		StartedAt:       at.Add(2 * time.Second),
	}
	if err := finalizeManifestRecord(&pendingRollback); err != nil {
		t.Fatal(err)
	}

	finalizedRollback := sampled
	finalizedRollback.TransitionKind = "rollback_finalized"
	finalizedRollback.Physical.EventSeq = 1
	finalizedRollback.Effective = finalizedRollback.Physical
	agreed := finalizedRollback.Physical
	finalizedRollback.LastAgreed = &agreed
	finalizedAt := at.Add(3 * time.Second)
	finalizedRollback.LastAgreedAt = &finalizedAt
	finalizedRollback.VisibilityGeneration++
	finalizedRollback.TrustReason = "golden finalized rollback"
	if err := finalizeManifestRecord(&finalizedRollback); err != nil {
		t.Fatal(err)
	}

	return []manifestRecord{
		official,
		sampled,
		pendingEvidence,
		pendingRollback,
		finalizedRollback,
	}
}
