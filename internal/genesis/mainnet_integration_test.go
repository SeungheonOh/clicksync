package genesis_test

import (
	"context"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/blinklabs-io/gouroboros/ledger"

	"clicksync/internal/config"
	"clicksync/internal/genesis"
	"clicksync/internal/model"
	"clicksync/internal/publication"
	"clicksync/internal/store"
	"clicksync/internal/writerlock"
)

func TestOfficialMainnetOriginSeedAndKnownEpoch0EBBPublicationContinuity(t *testing.T) {
	if os.Getenv("CLICKSYNC_ORIGIN_INTEGRATION") != "1" {
		t.Skip("set CLICKSYNC_ORIGIN_INTEGRATION=1 for official genesis integration")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	bundle, err := genesis.Mainnet()
	if err != nil {
		t.Fatal(err)
	}
	if len(bundle.Block.Transactions) != int(genesis.AVVMEntries) {
		t.Fatalf("genesis transactions = %d", len(bundle.Block.Transactions))
	}

	admin, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{"127.0.0.1:19100"},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
			Password: "integration-only",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	if err := admin.Exec(ctx, `DROP DATABASE IF EXISTS clicksync`); err != nil {
		t.Fatal(err)
	}
	db, err := store.Open(config.Config{
		ClickHouseHost:     "127.0.0.1",
		ClickHousePort:     19100,
		ClickHouseUser:     "default",
		ClickHousePassword: "integration-only",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
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
	now := time.Date(2026, 7, 23, 14, 0, 0, 123456000, time.UTC)
	writerID := originID(0xd1)
	byronID, byronJSON, shelleyID, shelleyJSON := store.MainnetGenesisIdentity()
	seed := store.ManifestSeed{
		NetworkMagic:           764824073,
		NetworkName:            "mainnet",
		ByronGenesisID:         byronID,
		ByronGenesisJSONHash:   byronJSON,
		ShelleyGenesisID:       shelleyID,
		ShelleyGenesisJSONHash: shelleyJSON,
		Start:                  publication.Point{Origin: true},
		WriterID:               writerID,
		WriterBuild:            "origin-integration",
		SourceBuild:            "official-mainnet-genesis",
		CreatedAt:              now,
		OriginGenesis:          &bundle.Proof,
	}
	identity, err := db.LoadOrCreateManifest(ctx, lock, seed)
	if err != nil {
		t.Fatal(err)
	}
	allocator, err := db.NewAllocator(ctx)
	if err != nil {
		t.Fatal(err)
	}
	coordinator, err := publication.New(db, allocator, lock, publication.Config{
		WriterID:    writerID,
		WriterBuild: "origin-integration",
		Now:         func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := coordinator.PublishBatch(ctx, publication.Batch{
		Items: []publication.BatchItem{{
			Block:  bundle.Block,
			Source: bundle.Source,
		}},
		FirstStagedAt: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	digest, err := publication.FactsDigest(bundle.Block, bundle.Source)
	if err != nil {
		t.Fatal(err)
	}
	expected := []store.GenesisPublication{{
		PublicationID:    result.PublicationIDs[0],
		FactsDigest:      digest,
		TransactionCount: genesis.AVVMEntries,
		OutputCount:      genesis.AVVMEntries,
		InitialSupply:    genesis.InitialSupply,
	}}
	if err := db.MarkGenesisSeeded(
		ctx,
		&failingLock{base: lock, failAt: 1},
		expected,
		writerID,
		"origin-integration",
		now,
	); err == nil {
		t.Fatal("genesis marker ignored flock loss before tip correction")
	}
	if err := db.MarkGenesisSeeded(
		ctx,
		&failingLock{base: lock, failAt: 2},
		expected,
		writerID,
		"origin-integration",
		now,
	); err == nil {
		t.Fatal("genesis marker ignored flock loss before completion insert")
	}
	beforeRecovery, err := db.LoadManifestIdentity(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if beforeRecovery.GenesisSeeded || beforeRecovery.CompleteHistory {
		t.Fatalf("lost-lock marker became complete: %+v", beforeRecovery)
	}
	if err := genesis.EnsurePublished(
		ctx,
		db,
		coordinator,
		lock,
		bundle,
		writerID,
		"origin-integration",
		now,
	); err != nil {
		t.Fatal(err)
	}
	seeded, err := db.LoadManifestIdentity(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if seeded.DatasetID != identity.DatasetID ||
		!seeded.GenesisSeeded ||
		!seeded.CompleteHistory {
		t.Fatalf("seeded identity = %+v", seeded)
	}
	snapshot, err := db.CommittedSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot != 1 {
		t.Fatalf("genesis recovery republished adoption: snapshot=%d", snapshot)
	}
	var transactions, outputs, supply uint64
	if err := admin.QueryRow(ctx, `
SELECT
    (SELECT count() FROM clicksync.transactions WHERE flow_kind = 'genesis'),
    (SELECT count() FROM clicksync.outputs WHERE output_kind = 'genesis'),
    (SELECT sum(lovelace) FROM clicksync.outputs WHERE output_kind = 'genesis')
`).Scan(&transactions, &outputs, &supply); err != nil {
		t.Fatal(err)
	}
	if transactions != uint64(genesis.AVVMEntries) ||
		outputs != uint64(genesis.AVVMEntries) ||
		supply != genesis.InitialSupply {
		t.Fatalf("genesis rows tx=%d outputs=%d supply=%d", transactions, outputs, supply)
	}

	restartSeed := seed
	restartSeed.WriterID = originID(0xd2)
	restartSeed.OriginGenesis = nil
	restarted, err := db.LoadOrCreateManifest(ctx, lock, restartSeed)
	if err != nil {
		t.Fatal(err)
	}
	if restarted.DatasetID != identity.DatasetID ||
		restarted.Start != (publication.Point{Origin: true}) {
		t.Fatalf("Origin restart identity changed: %+v", restarted)
	}
	restartAllocator, err := db.NewAllocator(ctx)
	if err != nil {
		t.Fatal(err)
	}
	restartCoordinator, err := publication.New(
		db,
		restartAllocator,
		lock,
		publication.Config{
			WriterID:    restartSeed.WriterID,
			WriterBuild: "origin-restart",
			Now:         func() time.Time { return now },
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := genesis.EnsurePublished(
		ctx,
		db,
		restartCoordinator,
		lock,
		bundle,
		restartSeed.WriterID,
		"origin-restart",
		now,
	); err != nil {
		t.Fatal(err)
	}
	if snapshot, err = db.CommittedSnapshot(ctx); err != nil || snapshot != 1 {
		t.Fatalf("restart genesis snapshot=%d err=%v", snapshot, err)
	}

	ebbHash := originHash("89d9b5a5b8ddc8d7e5a6795e9774d97faf1efea59b2caf7eaf9f8c5b32059df4")
	ebb := model.Block{
		Hash:                   ebbHash,
		Slot:                   0,
		Number:                 0,
		Era:                    "Byron",
		Type:                   int16(ledger.BlockTypeByronEbb),
		BodyHashVerified:       true,
		TransactionIDsVerified: true,
		ObservedAt:             now,
	}
	if _, err := restartCoordinator.PublishBatch(ctx, publication.Batch{
		Items: []publication.BatchItem{{
			Block: ebb,
			Source: publication.Source{
				PeerHost:     "relay-a",
				PeerAddress:  "192.0.2.1:3001",
				Operator:     "operator-a",
				N2NVersion:   15,
				NetworkMagic: 764824073,
			},
		}},
		FirstStagedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	snapshot, err = db.CommittedSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	tip, err := db.CommittedTip(ctx, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot != 2 ||
		tip.Hash != ebbHash ||
		tip.Slot != 0 ||
		tip.BlockNumber != 0 ||
		!tip.IsByronEBB {
		t.Fatalf("first Byron continuation snapshot=%d tip=%+v", snapshot, tip)
	}
}

type failingLock struct {
	base   store.LockAssertion
	calls  int
	failAt int
}

func (lock *failingLock) AssertHeld() error {
	lock.calls++
	if lock.calls == lock.failAt {
		return errors.New("injected flock loss")
	}
	return lock.base.AssertHeld()
}

func (lock *failingLock) Path() string { return lock.base.Path() }

func originID(value byte) [16]byte {
	var ret [16]byte
	for index := range ret {
		ret[index] = value
	}
	ret[6] = (ret[6] & 0x0f) | 0x40
	ret[8] = (ret[8] & 0x3f) | 0x80
	return ret
}

func originHash(value string) model.Hash32 {
	decoded, err := hex.DecodeString(value)
	if err != nil {
		panic(err)
	}
	var ret model.Hash32
	copy(ret[:], decoded)
	return ret
}
