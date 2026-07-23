package store

import (
	"testing"
	"time"

	"clicksync/internal/model"
	"clicksync/internal/publication"
	"clicksync/migrations"
)

func TestValidateBoundedManifestRows(t *testing.T) {
	first := validManifestRecord(t)
	second, err := appendManifestTransition(
		first,
		"physical_adoption",
		first.UpdatedAt.Add(time.Second),
		func(next *manifestRecord) error {
			return applyPhysicalManifestUpdate(
				next,
				publication.ManifestUpdate{
					EventSeq:        17,
					Tip:             manifestTestPoint(101),
					Kind:            publication.ManifestAdoption,
					RemoteAdoptions: 1,
					WriterID:        manifestID(0x44),
					WriterBuild:     "test-next",
				},
				1,
				false,
			)
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	if got, found, err := validateBoundedManifestRows([]manifestRecord{second, first}); err != nil ||
		!found || got.RowDigest != second.RowDigest {
		t.Fatalf("valid chain result = revision %d found=%t err=%v", got.Revision, found, err)
	}

	eightHeads := make([]manifestRecord, 0, 9)
	for range manifestDuplicateLimit {
		eightHeads = append(eightHeads, second)
	}
	eightHeads = append(eightHeads, first)
	if _, found, err := validateBoundedManifestRows(eightHeads); err != nil || !found {
		t.Fatalf("eight lost-response duplicates were rejected: found=%t err=%v", found, err)
	}

	nineHeads := append([]manifestRecord(nil), eightHeads[:8]...)
	nineHeads = append(nineHeads, second)
	if _, _, err := validateBoundedManifestRows(nineHeads); err == nil {
		t.Fatal("nine latest physical rows were accepted")
	}

	conflict := second
	conflict.TrustReason = "conflicting latest physical row"
	if err := finalizeManifestRecord(&conflict); err != nil {
		t.Fatal(err)
	}
	if _, _, err := validateBoundedManifestRows([]manifestRecord{second, conflict, first}); err == nil {
		t.Fatal("conflicting latest rows were accepted")
	}

	if _, _, err := validateBoundedManifestRows([]manifestRecord{second}); err == nil {
		t.Fatal("missing predecessor was accepted")
	}

	wrongPredecessor := second
	wrong := manifestHash(0x99)
	wrongPredecessor.PreviousRowDigest = &wrong
	if err := finalizeManifestRecord(&wrongPredecessor); err != nil {
		t.Fatal(err)
	}
	if _, _, err := validateBoundedManifestRows(
		[]manifestRecord{wrongPredecessor, first},
	); err == nil {
		t.Fatal("wrong predecessor digest was accepted")
	}

	changedIdentity := second
	changedIdentity.NetworkName = "other-network"
	if err := finalizeManifestRecord(&changedIdentity); err != nil {
		t.Fatal(err)
	}
	if _, _, err := validateBoundedManifestRows(
		[]manifestRecord{changedIdentity, first},
	); err == nil {
		t.Fatal("immutable identity change was accepted")
	}

	tampered := second
	tampered.PrimarySuffix++
	if _, _, err := validateBoundedManifestRows([]manifestRecord{tampered, first}); err == nil {
		t.Fatal("client-side row-digest mismatch was accepted")
	}
}

func TestValidateBoundedManifestRowsFreshAndInitialHistory(t *testing.T) {
	if got, found, err := validateBoundedManifestRows(nil); err != nil || found ||
		got != (manifestRecord{}) {
		t.Fatalf("empty manifest result = %+v found=%t err=%v", got, found, err)
	}
	first := validManifestRecord(t)
	if _, found, err := validateBoundedManifestRows([]manifestRecord{first}); err != nil || !found {
		t.Fatalf("revision one was rejected: found=%t err=%v", found, err)
	}
	impossible := first
	impossible.Revision = 2
	previous := manifestHash(0x55)
	impossible.PreviousRowDigest = &previous
	if err := finalizeManifestRecord(&impossible); err != nil {
		t.Fatal(err)
	}
	if _, _, err := validateBoundedManifestRows([]manifestRecord{first, impossible}); err == nil {
		t.Fatal("revision one with lower history was accepted")
	}
}

func TestApplyPhysicalManifestUpdateCountsRemoteBlocksNotEventSpace(t *testing.T) {
	record := validManifestRecord(t)
	record.VisibilityGeneration = 7
	update := publication.ManifestUpdate{
		EventSeq:        50, // reserved event gaps are intentionally irrelevant
		Tip:             manifestTestPoint(101),
		Kind:            publication.ManifestAdoption,
		RemoteAdoptions: 1,
		WriterID:        manifestID(0x71),
		WriterBuild:     "adoption",
	}
	if err := applyPhysicalManifestUpdate(&record, update, 1, false); err != nil {
		t.Fatal(err)
	}
	if record.PrimarySuffix != 1 {
		t.Fatalf("primary suffix = %d, want one accepted remote block", record.PrimarySuffix)
	}
	if record.VisibilityGeneration != 7 {
		t.Fatalf("ordinary adoption rotated visibility generation to %d", record.VisibilityGeneration)
	}
	if record.Physical.EventSeq != 50 || record.Effective != record.Physical {
		t.Fatalf("ordinary physical/effective heads = %+v / %+v", record.Physical, record.Effective)
	}

	genesis := validManifestRecord(t)
	genesis.Start = publication.Point{Origin: true}
	genesis.Physical = manifestHead{Point: publication.Point{Origin: true}}
	genesis.Effective = genesis.Physical
	genesis.ServableFloor = genesis.Physical
	genesis.GenesisSeeded = false
	genesis.CompleteHistory = false
	genesis.ServableFloorPermanent = false
	genesis.Servable = false
	genesis.TrustStatus = "unavailable"
	genesis.TrustBasis = "official_genesis"
	genesis.LastAgreed = nil
	genesis.LastAgreedAt = nil
	if err := applyPhysicalManifestUpdate(
		&genesis,
		publication.ManifestUpdate{
			EventSeq: 22,
			Tip:      publication.Point{Origin: true},
			Kind:     publication.ManifestGenesis,
		},
		0,
		false,
	); err != nil {
		t.Fatal(err)
	}
	if genesis.PrimarySuffix != 0 || genesis.Servable || genesis.Effective.EventSeq != 0 {
		t.Fatalf("synthetic genesis changed suffix/bootstrap visibility: %+v", genesis)
	}

	beforeRollbackSuffix := record.PrimarySuffix
	if err := applyPhysicalManifestUpdate(
		&record,
		publication.ManifestUpdate{
			EventSeq: 51,
			Tip:      manifestTestPoint(100),
			Kind:     publication.ManifestRollback,
		},
		0,
		true,
	); err != nil {
		t.Fatal(err)
	}
	if record.PrimarySuffix != beforeRollbackSuffix {
		t.Fatalf("rollback changed suffix from %d to %d", beforeRollbackSuffix, record.PrimarySuffix)
	}
	if record.VisibilityGeneration != 8 {
		t.Fatalf("rollback generation = %d, want 8", record.VisibilityGeneration)
	}

	depthZero := record
	if err := applyPhysicalManifestUpdate(
		&depthZero,
		publication.ManifestUpdate{
			EventSeq: 52,
			Tip:      depthZero.Physical.Point,
			Kind:     publication.ManifestRollback,
		},
		0,
		true,
	); err != nil {
		t.Fatal(err)
	}
	if depthZero.VisibilityGeneration != 9 {
		t.Fatalf("depth-zero rollback generation = %d, want 9", depthZero.VisibilityGeneration)
	}

	atLimit := validManifestRecord(t)
	atLimit.PrimarySuffix = manifestMaximumSuffix
	if err := applyPhysicalManifestUpdate(
		&atLimit,
		publication.ManifestUpdate{
			EventSeq: 2,
			Tip:      manifestTestPoint(101),
			Kind:     publication.ManifestAdoption,
		},
		1,
		false,
	); err == nil {
		t.Fatal("remote adoption beyond the 767-block suffix was accepted")
	}
}

func TestManifestTrustStateInvariants(t *testing.T) {
	record := validManifestRecord(t)
	record.TrustStatus = "checking"
	if err := finalizeManifestRecord(&record); err != nil {
		t.Fatal(err)
	}
	if err := verifyManifestRecord(record); err == nil {
		t.Fatal("checking state without check/group/evidence identity was accepted")
	}

	record = validManifestRecord(t)
	record.TrustStatus = "unavailable"
	record.Servable = false
	if err := finalizeManifestRecord(&record); err != nil {
		t.Fatal(err)
	}
	if err := verifyManifestRecord(record); err == nil {
		t.Fatal("unavailable state discarded an established servable floor")
	}

	record = validManifestRecord(t)
	record.Effective = record.ServableFloor
	record.TrustStatus = "disputed"
	record.Disagreement = true
	checkID := manifestID(0x61)
	groupID := manifestID(0x62)
	record.CheckID = &checkID
	record.AgreementGroup = &groupID
	record.CheckAttempt = 1
	record.CorroborationRequired = 2
	record.CorroborationConfirmed = 1
	checked := record.Physical
	record.Checked = &checked
	started := record.UpdatedAt.Add(time.Second)
	completed := started.Add(time.Second)
	record.CheckStartedAt = &started
	record.CheckCompletedAt = &completed
	if err := finalizeManifestRecord(&record); err != nil {
		t.Fatal(err)
	}
	if err := verifyManifestRecord(record); err != nil {
		t.Fatalf("clamped disputed state was rejected: %v", err)
	}
}

func TestReconcileCannotNormalizeAwayTrustOrRollbackBarrier(t *testing.T) {
	latest := validManifestRecord(t)
	update := publication.ManifestUpdate{
		EventSeq: 9,
		Tip:      manifestTestPoint(101),
		Kind:     publication.ManifestReconcile,
	}
	latest.TrustStatus = "checking"
	if err := validateManifestReconcileAdvance(latest, update); err == nil {
		t.Fatal("reconcile advanced past checking barrier")
	}
	latest.TrustStatus = "disputed"
	if err := validateManifestReconcileAdvance(latest, update); err == nil {
		t.Fatal("reconcile advanced past disputed barrier")
	}
	latest.TrustStatus = "agreed"
	latest.PendingRollback = &manifestPendingRollback{
		State:       "reserved",
		ID:          manifestID(0x81),
		EventSeq:    update.EventSeq,
		To:          update.Tip,
		OldPhysical: latest.Physical,
		StartedAt:   latest.UpdatedAt,
	}
	if err := validateManifestReconcileAdvance(latest, update); err == nil {
		t.Fatal("generic reconcile finalized a pending rollback")
	}
	update.EventSeq++
	if err := validateManifestReconcileAdvance(latest, update); err == nil {
		t.Fatal("reconcile accepted an event beyond pending rollback reservation")
	}
}

func validManifestRecord(t *testing.T) manifestRecord {
	t.Helper()
	at := time.Date(2026, 7, 23, 18, 0, 0, 123456000, time.UTC)
	boundary := manifestHead{Point: manifestTestPoint(100)}
	writer := manifestID(0x33)
	checkID := manifestID(0x31)
	groupID := manifestID(0x32)
	started := at.Add(-time.Second)
	record := manifestRecord{
		ManifestKey:            manifestKey,
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
		Start:                  boundary.Point,
		TrustMode:              "peer_observed_structurally_verified",
		TrustStatus:            "agreed",
		TrustBasis:             "sampled_peer",
		CheckID:                &checkID,
		AgreementGroup:         &groupID,
		CheckAttempt:           1,
		CorroborationRequired:  2,
		CorroborationConfirmed: 2,
		CheckpointInterval:     manifestCheckpointBlocks,
		TrustReason:            "bootstrap boundary agreed",
		CheckStartedAt:         &started,
		CheckCompletedAt:       &at,
		Checked:                &boundary,
		LastAgreed:             &boundary,
		LastAgreedAt:           &at,
		ServableFloor:          boundary,
		ServableFloorPermanent: false,
		Physical:               boundary,
		Effective:              boundary,
		Servable:               true,
		VisibilityGeneration:   1,
		WriterID:               &writer,
		WriterBuild:            "test",
		SourceBuild:            "test-source",
		CreatedAt:              at,
		UpdatedAt:              at,
	}
	if err := finalizeManifestRecord(&record); err != nil {
		t.Fatal(err)
	}
	if err := verifyManifestRecord(record); err != nil {
		t.Fatalf("test manifest is invalid: %v", err)
	}
	return record
}

func manifestTestPoint(number uint64) publication.Point {
	return publication.Point{
		Slot:        number * 10,
		Hash:        manifestHash(byte(number)),
		BlockNumber: number,
	}
}

func manifestHash(value byte) model.Hash32 {
	var hash model.Hash32
	for index := range hash {
		hash[index] = value
	}
	return hash
}

func manifestID(value byte) [16]byte {
	var id [16]byte
	for index := range id {
		id[index] = value
	}
	return id
}
