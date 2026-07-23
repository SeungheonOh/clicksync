package store

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/crypto/blake2b"

	"clicksync/internal/config"
	"clicksync/internal/model"
	"clicksync/internal/publication"
	"clicksync/internal/writerlock"
)

func TestNativePublicationBinaryRoundTrip(t *testing.T) {
	if os.Getenv("CLICKSYNC_CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set CLICKSYNC_CLICKHOUSE_INTEGRATION=1 for isolated ClickHouse")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	db, err := Open(config.Config{
		ClickHouseHost:     "127.0.0.1",
		ClickHousePort:     19100,
		ClickHouseUser:     "default",
		ClickHousePassword: "integration-only",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.conn.Exec(ctx, `DROP DATABASE IF EXISTS clicksync`); err != nil {
		t.Fatal(err)
	}
	if err := db.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if identity, found, err := db.LoadManifestIdentityIfExists(ctx); err != nil {
		t.Fatal(err)
	} else if found {
		t.Fatalf("fresh migrated database returned manifest identity %+v", identity)
	}
	now := time.Date(2026, 7, 23, 12, 0, 0, 123456000, time.UTC)
	writerID := id16(0x41)
	lock, err := writerlock.Acquire(
		filepath.Join(t.TempDir(), "writer.lock"),
		"single-host-flock",
	)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Release()
	byronID, byronJSON, shelleyID, shelleyJSON := MainnetGenesisIdentity()
	seed := ManifestSeed{
		NetworkMagic:           764824073,
		NetworkName:            "mainnet",
		ByronGenesisID:         byronID,
		ByronGenesisJSONHash:   byronJSON,
		ShelleyGenesisID:       shelleyID,
		ShelleyGenesisJSONHash: shelleyJSON,
		Start: publication.Point{
			Slot:        99,
			Hash:        hash32Fill(0x13),
			BlockNumber: 9,
		},
		WriterID:    writerID,
		WriterBuild: "integration",
		SourceBuild: "fixture",
		CreatedAt:   now,
	}
	identity, err := db.LoadOrCreateManifest(ctx, lock, seed)
	if err != nil {
		t.Fatal(err)
	}
	if identity.DatasetID == ([16]byte{}) || identity.Start.BlockNumber != 9 {
		t.Fatalf("generated manifest identity = %+v", identity)
	}
	if loaded, found, err := db.LoadManifestIdentityIfExists(ctx); err != nil {
		t.Fatal(err)
	} else if !found || loaded != identity {
		t.Fatalf("loaded manifest identity = %+v found=%t", loaded, found)
	}
	if status, found, err := db.LatestWriterAudit(ctx); err != nil {
		t.Fatal(err)
	} else if found {
		t.Fatalf("fresh dataset returned writer audit %+v", status)
	}
	allocator, err := db.NewAllocator(ctx)
	if err != nil {
		t.Fatal(err)
	}
	coordinator, err := publication.New(db, allocator, lock, publication.Config{
		WriterID:    writerID,
		WriterBuild: "integration",
		Now: func() time.Time {
			return now
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	policy := hash28Fill(0x55)
	paymentKey := hash28Fill(0x56)
	firstTx := hash32Fill(0x21)
	firstBlock := model.Block{
		Hash:                   hash32Fill(0x20),
		ParentHash:             hash32Pointer(hash32Fill(0x13)),
		Slot:                   100,
		Number:                 10,
		Era:                    "conway",
		Type:                   7,
		BodyHashVerified:       true,
		TransactionIDsVerified: true,
		ObservedAt:             now,
		Transactions: []model.Transaction{{
			Hash:         firstTx,
			Order:        0,
			Era:          "conway",
			Phase2Valid:  true,
			FlowKind:     "regular",
			DeclaredFee:  uint64Pointer(2),
			EffectiveFee: uint64Pointer(2),
			MintApplied:  true,
			Outputs: []model.Output{{
				TransactionHash:       firstTx,
				TransactionOrder:      0,
				Index:                 0,
				BodyOrdinal:           0,
				Kind:                  "regular",
				Address:               append([]byte{0x71}, bytesOf28(policy)...),
				PaymentCredentialKind: "script",
				PaymentCredentialHash: &policy,
				Lovelace:              42,
				DatumKind:             "none",
			}, {
				TransactionHash:       firstTx,
				TransactionOrder:      0,
				Index:                 1,
				BodyOrdinal:           1,
				Kind:                  "regular",
				Address:               append([]byte{0x61}, bytesOf28(paymentKey)...),
				PaymentCredentialKind: "key",
				PaymentCredentialHash: &paymentKey,
				Lovelace:              100,
				DatumKind:             "none",
			}},
		}},
	}
	datumCBOR := []byte{0xd8, 0x79, 0x9f, 0x01, 0xff}
	datumHash := model.Hash32(blake2b.Sum256(datumCBOR))
	secondTx := hash32Fill(0x31)
	thirdTx := hash32Fill(0x32)
	fourthTx := hash32Fill(0x33)
	unknownTx := hash32Fill(0x77)
	redeemerData := []byte{0x80}
	redeemerHash := model.Hash32(blake2b.Sum256(redeemerData))
	metadataCBOR := []byte{0xa1, 0x01, 0x41, 0xff}
	metadataHash := model.Hash32(blake2b.Sum256(metadataCBOR))
	block := model.Block{
		Hash:                   hash32Fill(0x30),
		ParentHash:             hash32Pointer(firstBlock.Hash),
		Slot:                   101,
		Number:                 11,
		Era:                    "conway",
		Type:                   7,
		BodyHashVerified:       true,
		TransactionIDsVerified: true,
		ObservedAt:             now.Add(time.Second),
		Datums: []model.DatumBody{{
			Hash: datumHash,
			CBOR: datumCBOR,
		}},
		Transactions: []model.Transaction{
			{
				Hash:         secondTx,
				Order:        0,
				Era:          "conway",
				Phase2Valid:  true,
				FlowKind:     "regular",
				DeclaredFee:  uint64Pointer(3),
				EffectiveFee: uint64Pointer(3),
				MintApplied:  true,
				Mint: []model.AssetDelta{{
					PolicyID: policy,
					Name:     []byte{0x00, 0xff},
					Quantity: 9,
				}},
				Inputs: []model.Input{
					inputFixture(secondTx, 0, firstTx, 0, 0),
					inputFixture(secondTx, 0, unknownTx, 9, 1),
				},
				Outputs: []model.Output{{
					TransactionHash:       secondTx,
					TransactionOrder:      0,
					Index:                 0,
					BodyOrdinal:           0,
					Kind:                  "regular",
					Address:               append([]byte{0x61}, bytesOf28(paymentKey)...),
					PaymentCredentialKind: "key",
					PaymentCredentialHash: &paymentKey,
					Lovelace:              21,
					Assets: []model.Asset{{
						PolicyID: policy,
						Name:     []byte{0x00, 0xff},
						Quantity: 9,
					}},
					DatumKind: "inline",
					DatumHash: &datumHash,
				}},
				DatumObservations: []model.DatumObservation{{
					Hash:             datumHash,
					TransactionHash:  secondTx,
					TransactionOrder: 0,
					SourceKind:       "inline_output",
					SourceOrdinal:    0,
					OutputIndex:      uint32Pointer(0),
				}},
				Withdrawals: []model.Withdrawal{{
					TransactionHash:  secondTx,
					TransactionOrder: 0,
					BodyOrdinal:      0,
					RewardAccount:    []byte{0xe1, 0x00, 0xff},
					Lovelace:         5,
					Applied:          true,
					CredentialKind:   "script",
					CredentialHash:   policy,
				}},
				Redeemers: []model.Redeemer{{
					TransactionHash:   secondTx,
					TransactionOrder:  0,
					RawPurposeTag:     0,
					Purpose:           "spend",
					Index:             0,
					DataCBOR:          redeemerData,
					DataHash:          redeemerHash,
					ExUnitsMemory:     10,
					ExUnitsSteps:      20,
					Applied:           true,
					TargetTxHash:      &firstTx,
					TargetOutputIndex: uint32Pointer(0),
				}},
				Metadata: &model.Metadata{
					TransactionHash:  secondTx,
					TransactionOrder: 0,
					Labels:           []uint64{1, 7},
					CBOR:             metadataCBOR,
					ContentHash:      metadataHash,
				},
			},
			{
				Hash:         thirdTx,
				Order:        1,
				Era:          "conway",
				Phase2Valid:  true,
				FlowKind:     "regular",
				DeclaredFee:  uint64Pointer(4),
				EffectiveFee: uint64Pointer(4),
				MintApplied:  true,
				Inputs: []model.Input{
					inputFixture(thirdTx, 1, secondTx, 0, 0),
				},
			},
			{
				Hash:         fourthTx,
				Order:        2,
				Era:          "conway",
				Phase2Valid:  false,
				FlowKind:     "collateral",
				DeclaredFee:  uint64Pointer(99),
				EffectiveFee: nil,
				MintApplied:  false,
				Inputs: []model.Input{{
					TransactionHash:  fourthTx,
					TransactionOrder: 2,
					SourceHash:       firstTx,
					SourceIndex:      1,
					BodyOrdinal:      0,
					Role:             "collateral",
					Consumed:         true,
				}},
				Outputs: []model.Output{{
					TransactionHash:       fourthTx,
					TransactionOrder:      2,
					Index:                 0,
					BodyOrdinal:           0,
					Kind:                  "collateral_return",
					Address:               append([]byte{0x61}, bytesOf28(paymentKey)...),
					PaymentCredentialKind: "key",
					PaymentCredentialHash: &paymentKey,
					Lovelace:              40,
					DatumKind:             "none",
				}},
			},
		},
	}
	peerObservation := model.PeerObservation{
		ID:               id16(0x71),
		Kind:             "checkpoint",
		PeerHost:         "relay-a",
		PeerAddress:      "192.0.2.1:3001",
		Operator:         "operator-a",
		N2NVersion:       15,
		NetworkMagic:     764824073,
		TipSlot:          block.Slot,
		TipHash:          block.Hash,
		TipBlockNumber:   block.Number,
		BodyHashVerified: true,
		PointVerified:    true,
		ParentVerified:   true,
		Result:           "agreed",
		ObservedAt:       now,
	}
	result, err := coordinator.PublishBatch(ctx, publication.Batch{
		Items: []publication.BatchItem{
			{Block: firstBlock, Source: sourceFixture()},
			{
				Block:            block,
				Source:           sourceFixture(),
				PeerObservations: []model.PeerObservation{peerObservation},
			},
		},
		FirstStagedAt: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.PublicationIDs) != 2 ||
		result.FirstEventSeq != 1 ||
		result.LastEventSeq != 2 {
		t.Fatalf("physical batch result = %+v", result)
	}
	expectedSinglePart := map[string]bool{
		"blocks":               false,
		"transactions":         false,
		"inputs":               false,
		"outputs":              false,
		"datum_bodies":         false,
		"datum_observations":   false,
		"withdrawals":          false,
		"redeemers":            false,
		"transaction_metadata": false,
		"peer_observations":    false,
		"chain_events":         false,
	}
	partTables := make([]string, 0, len(expectedSinglePart))
	for table := range expectedSinglePart {
		partTables = append(partTables, table)
	}
	partRows, err := db.conn.Query(ctx, `
SELECT table, countIf(active)
FROM system.parts
WHERE database = 'clicksync'
  AND table IN ?
GROUP BY table`, partTables)
	if err != nil {
		t.Fatal(err)
	}
	for partRows.Next() {
		var table string
		var count uint64
		if err := partRows.Scan(&table, &count); err != nil {
			partRows.Close()
			t.Fatal(err)
		}
		if count != 1 {
			partRows.Close()
			t.Fatalf("physical batch table %s active parts = %d, want 1", table, count)
		}
		expectedSinglePart[table] = true
	}
	if err := partRows.Err(); err != nil {
		partRows.Close()
		t.Fatal(err)
	}
	partRows.Close()
	for table, seen := range expectedSinglePart {
		if !seen {
			t.Fatalf("populated physical batch table %s has no part", table)
		}
	}
	restartSeed := seed
	restartSeed.Start.BlockNumber = 0 // restart reuses the stored derived height.
	restartSeed.WriterID = id16(0x42)
	restarted, err := db.LoadOrCreateManifest(ctx, lock, restartSeed)
	if err != nil {
		t.Fatal(err)
	}
	if restarted.DatasetID != identity.DatasetID ||
		restarted.Start.BlockNumber != identity.Start.BlockNumber {
		t.Fatalf("restart identity changed: before=%+v after=%+v", identity, restarted)
	}
	var resolved []bool
	rows, err := db.conn.Query(ctx, `
SELECT source_is_resolved
FROM clicksync.inputs
WHERE block_number = 11
ORDER BY tx_order, body_ordinal`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var value bool
		if err := rows.Scan(&value); err != nil {
			t.Fatal(err)
		}
		resolved = append(resolved, value)
	}
	rows.Close()
	if len(resolved) != 4 ||
		!resolved[0] ||
		resolved[1] ||
		!resolved[2] ||
		!resolved[3] {
		t.Fatalf("source resolution = %v, want [true false true true]", resolved)
	}
	var address, datum, redeemer, metadata []byte
	if err := db.conn.QueryRow(ctx, `
SELECT
    any(o.address),
    any(d.datum_cbor),
    any(r.data_cbor),
    any(m.metadata_cbor)
FROM clicksync.outputs AS o
CROSS JOIN clicksync.datum_bodies AS d
CROSS JOIN clicksync.redeemers AS r
CROSS JOIN clicksync.transaction_metadata AS m
WHERE o.block_number = 11`).Scan(&address, &datum, &redeemer, &metadata); err != nil {
		t.Fatal(err)
	}
	if string(address) != string(append([]byte{0x61}, bytesOf28(paymentKey)...)) ||
		string(datum) != string(datumCBOR) ||
		string(redeemer) != string([]byte{0x80}) ||
		string(metadata) != string([]byte{0xa1, 0x01, 0x41, 0xff}) {
		t.Fatalf("binary round-trip mismatch: %x %x %x %x", address, datum, redeemer, metadata)
	}
	var resolvedScriptHash []byte
	if err := db.conn.QueryRow(ctx, `
SELECT assumeNotNull(resolved_script_hash)
FROM clicksync.redeemers
WHERE block_number = 11 AND purpose = 'spend'`).Scan(&resolvedScriptHash); err != nil {
		t.Fatal(err)
	}
	if string(resolvedScriptHash) != string(policy[:]) {
		t.Fatalf("resolved spend script hash = %x, want %x", resolvedScriptHash, policy)
	}
	var effectiveCollateralFee *uint64
	if err := db.conn.QueryRow(ctx, `
SELECT effective_fee_lovelace
FROM clicksync.transactions
WHERE tx_hash = ?`, string(fourthTx[:])).Scan(&effectiveCollateralFee); err != nil {
		t.Fatal(err)
	}
	if effectiveCollateralFee == nil || *effectiveCollateralFee != 60 {
		t.Fatalf("derived effective collateral fee = %v, want 60", effectiveCollateralFee)
	}

	firstPoint := publication.Point{
		Slot:        firstBlock.Slot,
		Hash:        firstBlock.Hash,
		BlockNumber: firstBlock.Number,
	}
	descendants, err := db.ActiveDescendants(ctx, 2, firstPoint, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(descendants) != 1 ||
		descendants[0].PublicationID != result.PublicationIDs[1] ||
		descendants[0].Point != result.LastCommitted {
		t.Fatalf("rollback descendants = %+v", descendants)
	}
	rollback := publication.RollbackCommit{
		RollbackID:            id16(0x81),
		EventSeq:              3,
		To:                    firstPoint,
		OldTip:                result.LastCommitted,
		Depth:                 1,
		Reason:                "integration rollback",
		ObservedPeers:         []string{"relay-a", "relay-b"},
		ObservedOperators:     []string{"operator-a", "operator-b"},
		CorroborationRequired: 2,
		WriterID:              writerID,
		RecordedAt:            now.Add(2 * time.Second),
	}
	if err := db.InsertInvalidations(ctx, rollback, descendants); err != nil {
		t.Fatal(err)
	}
	snapshot, err := db.CommittedSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot != 2 {
		t.Fatalf("headerless invalidation advanced committed snapshot to %d", snapshot)
	}
	activeBeforeHeader, err := db.activeCandidatePublications(
		ctx,
		snapshot,
		[]uint64{result.PublicationIDs[1]},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(activeBeforeHeader) != 1 ||
		activeBeforeHeader[0] != result.PublicationIDs[1] {
		t.Fatalf("headerless invalidation changed membership: %v", activeBeforeHeader)
	}
	if err := db.InsertRollbackHeader(ctx, rollback); err != nil {
		t.Fatal(err)
	}
	snapshot, err = db.CommittedSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot != 3 {
		t.Fatalf("rollback snapshot = %d, want 3", snapshot)
	}
	committed, err := db.RollbackCommitted(ctx, rollback)
	if err != nil {
		t.Fatal(err)
	}
	if !committed {
		t.Fatal("exact rollback readback did not find committed header")
	}
	activeAfterHeader, err := db.activeCandidatePublications(
		ctx,
		snapshot,
		[]uint64{result.PublicationIDs[1]},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(activeAfterHeader) != 0 {
		t.Fatalf("committed rollback left descendant active: %v", activeAfterHeader)
	}
	tip, err := db.CommittedTip(ctx, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if tip != firstPoint {
		t.Fatalf("committed rollback tip = %+v, want %+v", tip, firstPoint)
	}
	if _, err := db.ActiveDescendants(ctx, snapshot, result.LastCommitted, 10); err == nil {
		t.Fatal("inactive descendant was accepted as a rollback target")
	}
	if err := db.ReconcileManifest(ctx, writerID, "integration", now.Add(3*time.Second)); err != nil {
		t.Fatal(err)
	}
	candidates, err := db.IntersectionCandidates(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(candidates) < 2 ||
		candidates[0].Point.Slot != firstPoint.Slot ||
		string(candidates[0].Point.Hash) != string(firstPoint.Hash[:]) ||
		candidates[0].BlockNumber != firstPoint.BlockNumber ||
		candidates[0].IsByronEBB ||
		candidates[len(candidates)-1].Point.Slot != seed.Start.Slot ||
		string(candidates[len(candidates)-1].Point.Hash) != string(seed.Start.Hash[:]) ||
		candidates[len(candidates)-1].BlockNumber != seed.Start.BlockNumber ||
		candidates[len(candidates)-1].IsByronEBB {
		t.Fatalf("typed intersection candidates = %+v", candidates)
	}
	var manifestEvent uint64
	var manifestSlot, manifestNumber *uint64
	var manifestHash []byte
	var manifestEBB bool
	if err := db.conn.QueryRow(ctx, `
SELECT
    committed_event_seq,
    committed_tip_slot,
    committed_tip_hash,
    committed_tip_block_number,
    committed_tip_is_byron_ebb
FROM clicksync.dataset_manifest
ORDER BY revision DESC
LIMIT 1`).Scan(
		&manifestEvent,
		&manifestSlot,
		&manifestHash,
		&manifestNumber,
		&manifestEBB,
	); err != nil {
		t.Fatal(err)
	}
	if manifestEvent != 3 ||
		manifestSlot == nil || *manifestSlot != firstPoint.Slot ||
		manifestNumber == nil || *manifestNumber != firstPoint.BlockNumber ||
		string(manifestHash) != string(firstPoint.Hash[:]) ||
		manifestEBB {
		t.Fatalf(
			"reconciled manifest = event %d point %v:%x:%v ebb=%t",
			manifestEvent,
			manifestSlot,
			manifestHash,
			manifestNumber,
			manifestEBB,
		)
	}

	audit, err := NewWriterAudit(
		identity.DatasetID,
		id16(0x91),
		"integration",
		now.Add(4*time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.BeginWriterAudit(ctx, lock, audit); err != nil {
		t.Fatal(err)
	}
	if err := db.HeartbeatWriterAudit(ctx, lock, audit, now.Add(5*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := db.ReleaseWriterAudit(
		ctx,
		lock,
		audit,
		now.Add(6*time.Second),
		"integration complete",
	); err != nil {
		t.Fatal(err)
	}
	auditStatus, found, err := db.LatestWriterAudit(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !found ||
		auditStatus.Revision != 3 ||
		auditStatus.OwnerID != audit.OwnerID ||
		auditStatus.State != "released" ||
		auditStatus.ReleaseReason != "integration complete" ||
		auditStatus.ReleasedAt == nil ||
		!auditStatus.ReleasedAt.Equal(now.Add(6*time.Second)) {
		t.Fatalf("latest writer audit = %+v found=%t", auditStatus, found)
	}
	if err := lock.Release(); err != nil {
		t.Fatal(err)
	}
	if err := db.HeartbeatWriterAudit(ctx, lock, audit, now.Add(7*time.Second)); err == nil {
		t.Fatal("heartbeat without the real flock succeeded")
	}
}

func TestNativeIntersectionCandidatesDenseGeometricAndByronBoundary(t *testing.T) {
	if os.Getenv("CLICKSYNC_CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set CLICKSYNC_CLICKHOUSE_INTEGRATION=1 for isolated ClickHouse")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	db, err := Open(config.Config{
		ClickHouseHost:     "127.0.0.1",
		ClickHousePort:     19100,
		ClickHouseUser:     "default",
		ClickHousePassword: "integration-only",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.conn.Exec(ctx, `DROP DATABASE IF EXISTS clicksync`); err != nil {
		t.Fatal(err)
	}
	if err := db.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	lock, err := writerlock.Acquire(
		filepath.Join(t.TempDir(), "writer.lock"),
		"single-host-flock",
	)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Release()
	now := time.Date(2026, 7, 23, 13, 0, 0, 654321000, time.UTC)
	writerID := id16(0xa1)
	byronID, byronJSON, shelleyID, shelleyJSON := MainnetGenesisIdentity()
	boundary := publication.Point{
		Slot:        100,
		Hash:        hash32Fill(0xb1),
		BlockNumber: 9,
		IsByronEBB:  true,
	}
	seed := ManifestSeed{
		NetworkMagic:           764824073,
		NetworkName:            "mainnet",
		ByronGenesisID:         byronID,
		ByronGenesisJSONHash:   byronJSON,
		ShelleyGenesisID:       shelleyID,
		ShelleyGenesisJSONHash: shelleyJSON,
		Start:                  boundary,
		WriterID:               writerID,
		WriterBuild:            "candidate-integration",
		SourceBuild:            "fixture",
		CreatedAt:              now,
	}
	identity, err := db.LoadOrCreateManifest(ctx, lock, seed)
	if err != nil {
		t.Fatal(err)
	}
	noOpRollback := publication.RollbackCommit{
		RollbackID:            id16(0xa2),
		EventSeq:              1,
		To:                    boundary,
		OldTip:                boundary,
		Depth:                 0,
		Reason:                "boundary corroboration",
		ObservedPeers:         []string{"relay-a", "relay-b"},
		ObservedOperators:     []string{"operator-a", "operator-b"},
		CorroborationRequired: 2,
		WriterID:              writerID,
		RecordedAt:            now,
	}
	if err := db.InsertRollbackHeader(ctx, noOpRollback); err != nil {
		t.Fatal(err)
	}
	if committed, err := db.RollbackCommitted(ctx, noOpRollback); err != nil {
		t.Fatal(err)
	} else if !committed {
		t.Fatal("no-op rollback exact readback was not committed")
	}
	if snapshot, err := db.CommittedSnapshot(ctx); err != nil {
		t.Fatal(err)
	} else if snapshot != 1 {
		t.Fatalf("no-op rollback snapshot = %d, want 1", snapshot)
	}
	restartSeed := seed
	restartSeed.Start.BlockNumber = 0
	restarted, err := db.LoadOrCreateManifest(ctx, lock, restartSeed)
	if err != nil {
		t.Fatal(err)
	}
	if restarted.DatasetID != identity.DatasetID ||
		restarted.Start != boundary {
		t.Fatalf("no-op restart identity changed: before=%+v after=%+v", identity, restarted)
	}

	attempts := make([]publication.Attempt, 33)
	for index := range attempts {
		step := uint64(index + 1)
		attempts[index] = publication.Attempt{
			PublicationID: step,
			Block: model.Block{
				Hash:                   hash32Fill(byte(0xc0 + index)),
				Slot:                   99 + step,
				Number:                 9 + step,
				BodyHashVerified:       true,
				TransactionIDsVerified: true,
				ObservedAt:             now.Add(time.Duration(step) * time.Microsecond),
			},
			Source:     sourceFixture(),
			WriterID:   writerID,
			InsertedAt: now.Add(time.Duration(step) * time.Microsecond),
		}
	}
	if err := db.InsertBlockBatch(ctx, attempts); err != nil {
		t.Fatal(err)
	}
	if err := db.InsertAdoptionBatch(ctx, attempts, 2); err != nil {
		t.Fatal(err)
	}
	restarted, err = db.LoadOrCreateManifest(ctx, lock, restartSeed)
	if err != nil {
		t.Fatal(err)
	}
	if restarted.DatasetID != identity.DatasetID ||
		restarted.Start != boundary {
		t.Fatalf("candidate restart identity changed: before=%+v after=%+v", identity, restarted)
	}
	candidates, err := db.IntersectionCandidates(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(candidates) != 34 {
		t.Fatalf("candidate count = %d, want 34: %+v", len(candidates), candidates)
	}
	first := candidates[0]
	successor := candidates[len(candidates)-2]
	terminal := candidates[len(candidates)-1]
	if first.Point.Slot != 132 ||
		first.BlockNumber != 42 ||
		first.IsByronEBB ||
		successor.Point.Slot != boundary.Slot ||
		successor.BlockNumber != boundary.BlockNumber+1 ||
		successor.IsByronEBB ||
		terminal.Point.Slot != boundary.Slot ||
		string(terminal.Point.Hash) != string(boundary.Hash[:]) ||
		terminal.BlockNumber != boundary.BlockNumber ||
		!terminal.IsByronEBB {
		t.Fatalf(
			"typed dense/geometric candidates first=%+v successor=%+v terminal=%+v",
			first,
			successor,
			terminal,
		)
	}
}

func sourceFixture() publication.Source {
	return publication.Source{
		PeerHost:     "relay-a",
		PeerAddress:  "192.0.2.1:3001",
		Operator:     "operator-a",
		N2NVersion:   15,
		NetworkMagic: 764824073,
	}
}

func inputFixture(
	tx model.Hash32,
	order uint32,
	source model.Hash32,
	index uint32,
	ordinal uint32,
) model.Input {
	return model.Input{
		TransactionHash:  tx,
		TransactionOrder: order,
		SourceHash:       source,
		SourceIndex:      index,
		BodyOrdinal:      ordinal,
		Role:             "regular",
		Consumed:         true,
	}
}

func hash32Fill(value byte) model.Hash32 {
	var ret model.Hash32
	for index := range ret {
		ret[index] = value
	}
	return ret
}

func hash28Fill(value byte) model.Hash28 {
	var ret model.Hash28
	for index := range ret {
		ret[index] = value
	}
	return ret
}

func id16(value byte) [16]byte {
	var ret [16]byte
	for index := range ret {
		ret[index] = value
	}
	ret[6] = (ret[6] & 0x0f) | 0x40
	ret[8] = (ret[8] & 0x3f) | 0x80
	return ret
}

func hash32Pointer(value model.Hash32) *model.Hash32 { return &value }
func uint32Pointer(value uint32) *uint32             { return &value }
func uint64Pointer(value uint64) *uint64             { return &value }
