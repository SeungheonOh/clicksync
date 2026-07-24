package clickhouse

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/clicksync-project/clickout/internal/model"
)

func authorityFill32(value byte) authorityHash {
	var result authorityHash
	for index := range result {
		result[index] = value
	}
	return result
}

func authorityFill16(value byte) [16]byte {
	var result [16]byte
	for index := range result {
		result[index] = value
	}
	return result
}

func validOfficialGenesisAuthority(t *testing.T) authorityRecord {
	t.Helper()
	at := time.Date(2026, 7, 23, 18, 0, 0, 123456000, time.UTC)
	origin := authorityHead{EventSeq: 1, Point: authorityPoint{Origin: true}}
	writer := authorityFill16(0x31)
	record := authorityRecord{
		Revision:               1,
		TransitionKind:         "initialize",
		DatasetID:              authorityFill16(0x11),
		SchemaContractHash:     expectedSchemaContract(),
		NetworkMagic:           764824073,
		NetworkName:            "mainnet",
		ByronGenesisID:         authorityFill32(0x21),
		ByronGenesisJSONHash:   authorityFill32(0x22),
		ShelleyGenesisID:       authorityFill32(0x23),
		ShelleyGenesisJSONHash: authorityFill32(0x24),
		Start:                  authorityPoint{Origin: true},
		GenesisSeeded:          true,
		CompleteHistory:        true,
		TrustMode:              model.TrustPeerObserved,
		TrustStatus:            "agreed",
		TrustBasis:             "official_genesis",
		CheckpointInterval:     manifestCheckpoint,
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
	if err := finalizeAuthorityRecord(&record); err != nil {
		t.Fatal(err)
	}
	if err := verifyAuthorityRecord(record); err != nil {
		t.Fatalf("test authority is invalid: %v", err)
	}
	return record
}

func sampledAuthority(t *testing.T) authorityRecord {
	t.Helper()
	at := time.Date(2026, 7, 23, 18, 0, 0, 123456000, time.UTC)
	boundary := authorityHead{
		Point: authorityPoint{
			Slot:        1000,
			Hash:        authorityFill32(100),
			BlockNumber: 100,
		},
	}
	writer := authorityFill16(0x33)
	checkID := authorityFill16(0x31)
	groupID := authorityFill16(0x32)
	evidenceDigest := authorityFill32(0x34)
	started := at.Add(-time.Second)
	record := authorityRecord{
		Revision:               1,
		TransitionKind:         "initialize",
		DatasetID:              authorityFill16(0x11),
		SchemaContractHash:     expectedSchemaContract(),
		NetworkMagic:           764824073,
		NetworkName:            "mainnet",
		ByronGenesisID:         authorityFill32(0x21),
		ByronGenesisJSONHash:   authorityFill32(0x22),
		ShelleyGenesisID:       authorityFill32(0x23),
		ShelleyGenesisJSONHash: authorityFill32(0x24),
		Start:                  boundary.Point,
		TrustMode:              model.TrustPeerObserved,
		TrustStatus:            "agreed",
		TrustBasis:             "sampled_peer",
		CheckID:                &checkID,
		AgreementGroup:         &groupID,
		CheckAttempt:           1,
		CorroborationRequired:  2,
		CorroborationConfirmed: 2,
		CheckpointInterval:     manifestCheckpoint,
		TrustReason:            "bootstrap boundary agreed",
		CheckStartedAt:         &started,
		CheckCompletedAt:       &at,
		EvidenceState:          "frozen",
		EvidenceCount:          2,
		EvidenceDigest:         &evidenceDigest,
		Checked:                &boundary,
		LastAgreed:             &boundary,
		LastAgreedAt:           &at,
		LastAgreedEvidence: &authorityEvidenceReference{
			CheckID:   checkID,
			Group:     groupID,
			Attempt:   1,
			Required:  2,
			Confirmed: 2,
			Checked:   boundary,
			Count:     2,
			Digest:    evidenceDigest,
		},
		ServableFloor:        boundary,
		Physical:             boundary,
		Effective:            boundary,
		Servable:             true,
		VisibilityGeneration: 1,
		WriterID:             &writer,
		WriterBuild:          "test",
		SourceBuild:          "test-source",
		CreatedAt:            at,
		UpdatedAt:            at,
	}
	if err := finalizeAuthorityRecord(&record); err != nil {
		t.Fatal(err)
	}
	if err := verifyAuthorityRecord(record); err != nil {
		t.Fatalf("sampled authority fixture is invalid: %v", err)
	}
	return record
}

func refreshPendingAuthorityObservation(t *testing.T, record *authorityRecord) {
	t.Helper()
	observation := record.PendingEvidenceWrite.Observation
	if err := finalizeAuthorityObservationIdentity(&observation); err != nil {
		t.Fatal(err)
	}
	observation.ObservedAt = normalizeAuthorityTime(observation.ObservedAt)
	encoded, err := json.Marshal(observation)
	if err != nil {
		t.Fatal(err)
	}
	digest := authorityHash(sha256.Sum256(encoded))
	payload, err := canonicalAuthorityObservationPayload(observation)
	if err != nil {
		t.Fatal(err)
	}
	record.PendingEvidenceWrite.Observation = observation
	record.PendingEvidenceWrite.Digest = digest
	record.PendingEvidenceWrite.Payload = payload
}

func authorityGoldenRecords(t *testing.T) []authorityRecord {
	t.Helper()
	official := validOfficialGenesisAuthority(t)
	sampled := sampledAuthority(t)
	at := sampled.UpdatedAt

	pendingEvidence := sampled
	pendingEvidence.TransitionKind = "evidence_write_reserved"
	checkID := authorityFill16(0x41)
	groupID := authorityFill16(0x42)
	pendingEvidence.CheckID = &checkID
	pendingEvidence.AgreementGroup = &groupID
	pendingEvidence.CheckAttempt = 1
	pendingEvidence.CorroborationConfirmed = 0
	pendingEvidence.TrustStatus = "checking"
	pendingEvidence.TrustReason = "exact candidate membership check in progress"
	started := at.Add(time.Second)
	pendingEvidence.CheckStartedAt = &started
	pendingEvidence.CheckCompletedAt = nil
	pendingEvidence.EvidenceState = "open"
	pendingEvidence.EvidenceCount = 0
	empty := authorityHash(sha256.Sum256([]byte("clicksync-trust-evidence-set\x00")))
	pendingEvidence.EvidenceDigest = &empty
	checked := pendingEvidence.Physical
	pendingEvidence.Checked = &checked
	slot := checked.Point.Slot
	hash := checked.Point.Hash
	number := checked.Point.BlockNumber
	isByron := checked.Point.IsByronEBB
	pendingEvidence.PendingEvidenceWrite = &authorityPendingEvidence{
		Observation: authorityObservation{
			Kind:                   "rollback",
			PeerHost:               "relay-pending",
			PeerAddress:            "192.0.2.41:3001",
			Operator:               "operator-pending",
			N2NVersion:             15,
			NetworkMagic:           764824073,
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
		},
		WriterID:   authorityFill16(0x43),
		ReservedAt: at.Add(2 * time.Second),
	}
	refreshPendingAuthorityObservation(t, &pendingEvidence)
	if err := finalizeAuthorityRecord(&pendingEvidence); err != nil {
		t.Fatal(err)
	}
	if err := verifyAuthorityRecord(pendingEvidence); err != nil {
		t.Fatalf("pending evidence fixture is invalid: %v", err)
	}

	pendingRollback := sampled
	pendingRollback.TransitionKind = "rollback_reserved"
	pendingRollback.PendingRollback = &authorityPendingRollback{
		State:           "reserved",
		ID:              authorityFill16(0x51),
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
		WriterID:        authorityFill16(0x52),
		StartedAt:       at.Add(2 * time.Second),
	}
	if err := finalizeAuthorityRecord(&pendingRollback); err != nil {
		t.Fatal(err)
	}
	if err := verifyAuthorityRecord(pendingRollback); err != nil {
		t.Fatalf("pending rollback fixture is invalid: %v", err)
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
	if err := finalizeAuthorityRecord(&finalizedRollback); err != nil {
		t.Fatal(err)
	}
	if err := verifyAuthorityRecord(finalizedRollback); err != nil {
		t.Fatalf("finalized rollback fixture is invalid: %v", err)
	}
	return []authorityRecord{
		official,
		sampled,
		pendingEvidence,
		pendingRollback,
		finalizedRollback,
	}
}

func TestAuthorityCanonicalDigestAndBoundedHead(t *testing.T) {
	records := authorityGoldenRecords(t)
	record := records[0]
	data, err := os.ReadFile("testdata/authority_golden.json")
	if err != nil {
		t.Fatal(err)
	}
	var golden struct {
		SchemaContract string `json:"schema_contract_sha256"`
		Rows           []struct {
			Name             string `json:"name"`
			TransitionID     string `json:"transition_id"`
			CanonicalPayload string `json:"canonical_payload_sha256"`
			RowDigest        string `json:"row_digest"`
		} `json:"rows"`
	}
	if err := json.Unmarshal(data, &golden); err != nil {
		t.Fatal(err)
	}
	if len(golden.Rows) != len(records) {
		t.Fatalf("unexpected authority golden: %+v", golden)
	}
	if golden.SchemaContract != expectedSchemaContract().String() {
		t.Fatalf(
			"golden schema contract=%s embedded=%s",
			golden.SchemaContract,
			expectedSchemaContract().String(),
		)
	}
	for index, candidate := range records {
		expected := golden.Rows[index]
		if got := hex.EncodeToString(candidate.TransitionID[:]); got != expected.TransitionID {
			t.Errorf("%s canonical transition ID = %s", expected.Name, got)
		}
		if got := candidate.RowDigest.String(); got != expected.RowDigest {
			t.Errorf("%s canonical row digest = %s", expected.Name, got)
		}
		payload, err := canonicalAuthorityPayload(candidate)
		if err != nil {
			t.Fatal(err)
		}
		if got := sha256.Sum256(payload); hex.EncodeToString(got[:]) != expected.CanonicalPayload {
			t.Errorf("%s canonical payload digest = %x", expected.Name, got)
		}
	}
	if latest, err := validateAuthorityRevisionRecords(
		[]authorityRecord{record},
		1,
		"latest",
	); err != nil || latest.RowDigest != record.RowDigest {
		t.Fatalf("revision-one bounded head = %+v err=%v", latest, err)
	}

	tampered := record
	tampered.TrustReason = "tampered"
	if err := verifyAuthorityRecord(tampered); err == nil {
		t.Fatal("tampered canonical manifest was accepted")
	}
	nine := make([]authorityRecord, manifestRawReadLimit)
	for index := range nine {
		nine[index] = record
	}
	if _, err := validateAuthorityRevisionRecords(nine, 1, "latest"); err == nil {
		t.Fatal("nine physical head rows were accepted")
	}
}

func TestAuthorityGoldenRepresentativeMutationsFailClosed(t *testing.T) {
	records := authorityGoldenRecords(t)
	for index, record := range records {
		transitionMutation := record
		transitionMutation.TransitionID[0] ^= 1
		if err := verifyAuthorityRecord(transitionMutation); err == nil {
			t.Fatalf("row %d one-bit transition ID mutation was accepted", index)
		}

		digestMutation := record
		digestMutation.RowDigest[0] ^= 1
		if err := verifyAuthorityRecord(digestMutation); err == nil {
			t.Fatalf("row %d one-bit row digest mutation was accepted", index)
		}
	}

	nestedPointerMutation := records[2]
	pendingPointerMutation := *nestedPointerMutation.PendingEvidenceWrite
	nestedPointerMutation.PendingEvidenceWrite = &pendingPointerMutation
	checkpointHashMutation := *pendingPointerMutation.Observation.CheckpointHash
	checkpointHashMutation[0] ^= 1
	nestedPointerMutation.PendingEvidenceWrite.Observation.CheckpointHash =
		&checkpointHashMutation
	if err := verifyAuthorityRecord(nestedPointerMutation); err == nil {
		t.Fatal("pending evidence one-bit nested checkpoint hash mutation was accepted")
	}

	wrongMagic := records[2]
	pendingEvidence := *wrongMagic.PendingEvidenceWrite
	wrongMagic.PendingEvidenceWrite = &pendingEvidence
	wrongMagic.PendingEvidenceWrite.Observation.NetworkMagic--
	refreshPendingAuthorityObservation(t, &wrongMagic)
	if err := finalizeAuthorityRecord(&wrongMagic); err != nil {
		t.Fatal(err)
	}
	if err := verifyAuthorityRecord(wrongMagic); err == nil {
		t.Fatal("pending evidence with non-mainnet magic was accepted")
	}

	underThreshold := records[1]
	lastAgreedEvidence := *underThreshold.LastAgreedEvidence
	underThreshold.LastAgreedEvidence = &lastAgreedEvidence
	underThreshold.TrustBasis = "primary_only"
	underThreshold.PrimarySuffix = 1
	underThreshold.CorroborationConfirmed = 1
	underThreshold.LastAgreedEvidence.Confirmed = 1
	if err := finalizeAuthorityRecord(&underThreshold); err != nil {
		t.Fatal(err)
	}
	if err := verifyAuthorityRecord(underThreshold); err == nil {
		t.Fatal("agreed primary-only state below its current threshold was accepted")
	}

	badRollback := records[3]
	pendingRollback := *badRollback.PendingRollback
	pendingRollback.Operators = append([]string(nil), pendingRollback.Operators...)
	badRollback.PendingRollback = &pendingRollback
	badRollback.PendingRollback.Operators =
		badRollback.PendingRollback.Operators[:1]
	if err := finalizeAuthorityRecord(&badRollback); err != nil {
		t.Fatal(err)
	}
	if err := verifyAuthorityRecord(badRollback); err == nil {
		t.Fatal("pending rollback with mismatched observer arrays was accepted")
	}
}

func TestAuthorityContractHashTracksRootDescriptor(t *testing.T) {
	if schemaContractDescriptor == "" {
		t.Fatal("embedded Clickout contract descriptor is empty")
	}
	if got := sha256.Sum256([]byte(schemaContractDescriptor)); got != expectedSchemaContract() {
		t.Fatalf("Clickout contract hash %x differs from embedded descriptor %x", expectedSchemaContract(), got)
	}
}

func TestAuthorityHashEncodingCannotDriftToClickoutHexJSON(t *testing.T) {
	value := authorityFill32(1)
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) < 3 || encoded[0] != '[' ||
		encoded[len(encoded)-1] != ']' ||
		strings.Contains(string(encoded), `"`) {
		t.Fatalf("authority hash is not producer-compatible integer-array JSON: %s", encoded)
	}
	sources, err := filepath.Glob("authority*.go")
	if err != nil {
		t.Fatal(err)
	}
	checked := 0
	for _, sourcePath := range sources {
		if strings.HasSuffix(sourcePath, "_test.go") {
			continue
		}
		source, err := os.ReadFile(sourcePath)
		if err != nil {
			t.Fatal(err)
		}
		checked++
		if strings.Contains(string(source), "model.Hash32") {
			t.Fatalf(
				"authority canonical graph in %s reuses Clickout model.Hash32",
				sourcePath,
			)
		}
	}
	if checked == 0 {
		t.Fatal("no non-test authority source files were checked")
	}
}

func TestAuthorityHeadAcceptsProducerManifest(t *testing.T) {
	if os.Getenv("CLICKOUT_AUTHORITY_CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set CLICKOUT_AUTHORITY_CLICKHOUSE_INTEGRATION=1")
	}
	store, err := Open(Config{
		Addresses:    []string{"127.0.0.1:19100"},
		Database:     "clicksync",
		Username:     "default",
		Password:     "integration-only",
		QueryTimeout: 30 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	record, found, err := store.loadAuthorityHead(context.Background())
	if err != nil || !found {
		t.Fatalf("producer authority found=%t record=%+v err=%v", found, record, err)
	}
	if record.SchemaContractHash != expectedSchemaContract() {
		t.Fatalf("producer contract = %x", record.SchemaContractHash)
	}
}
