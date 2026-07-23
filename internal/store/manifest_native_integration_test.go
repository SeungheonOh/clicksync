package store

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"clicksync/internal/config"
	"clicksync/internal/publication"
	"clicksync/internal/writerlock"
)

func TestNativeManifestDDLConstraintsAndDuplicateIntegrity(t *testing.T) {
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
	var createStatement string
	if err := db.conn.QueryRow(
		ctx,
		`SHOW CREATE TABLE clicksync.dataset_manifest`,
	).Scan(&createStatement); err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{
		"ENGINE = MergeTree",
		"ORDER BY (manifest_key, revision)",
		"index_granularity = 64",
	} {
		if !strings.Contains(createStatement, required) {
			t.Fatalf("native manifest DDL lacks %q:\n%s", required, createStatement)
		}
	}
	if strings.Contains(createStatement, "ReplacingMergeTree") {
		t.Fatalf("native manifest unexpectedly uses replacement semantics:\n%s", createStatement)
	}

	lock, err := writerlock.Acquire(
		filepath.Join(t.TempDir(), "writer.lock"),
		"single-host-flock",
	)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Release()
	byronID, byronJSON, shelleyID, shelleyJSON := MainnetGenesisIdentity()
	now := time.Date(2026, 7, 23, 19, 0, 0, 123456000, time.UTC)
	if _, err := db.LoadOrCreateManifest(ctx, lock, ManifestSeed{
		NetworkMagic:           mainnetMagic,
		NetworkName:            "mainnet",
		ByronGenesisID:         byronID,
		ByronGenesisJSONHash:   byronJSON,
		ShelleyGenesisID:       shelleyID,
		ShelleyGenesisJSONHash: shelleyJSON,
		Start: publication.Point{
			Slot:        10,
			Hash:        hash32Fill(0x91),
			BlockNumber: 1,
		},
		WriterID:    id16(0x92),
		WriterBuild: "manifest-native",
		SourceBuild: "manifest-native",
		CreatedAt:   now,
	}); err != nil {
		t.Fatal(err)
	}
	latest, found, err := db.loadLatestManifestRecord(ctx)
	if err != nil || !found {
		t.Fatalf("load manifest before invalid append found=%t err=%v", found, err)
	}
	var rowsBefore uint64
	if err := db.conn.QueryRow(
		ctx,
		`SELECT count() FROM clicksync.dataset_manifest`,
	).Scan(&rowsBefore); err != nil {
		t.Fatal(err)
	}
	invalid := latest
	invalid.Revision++
	invalid.PreviousRowDigest = &latest.RowDigest
	invalid.TransitionKind = "trust_agreed"
	invalid.CorroborationConfirmed = 1
	invalid.EvidenceCount = 0
	if err := db.appendManifestRecord(ctx, invalid); err == nil {
		t.Fatal("semantic invalid manifest append was accepted")
	}
	var rowsAfter uint64
	if err := db.conn.QueryRow(
		ctx,
		`SELECT count() FROM clicksync.dataset_manifest`,
	).Scan(&rowsAfter); err != nil {
		t.Fatal(err)
	}
	if rowsAfter != rowsBefore {
		t.Fatalf("invalid append prepared/inserted rows: before=%d after=%d", rowsBefore, rowsAfter)
	}

	// Nullable point constraints must reject a tuple with an event but no
	// origin discriminator, rather than letting SQL NULL make CHECK unknown.
	const malformedChecked = `
INSERT INTO clicksync.dataset_manifest
SELECT * REPLACE
(
    revision + 1 AS revision,
    toNullable(toUInt64(1)) AS checked_event_seq
)
FROM
(
    SELECT *
    FROM clicksync.dataset_manifest
    PREWHERE manifest_key = 1
    ORDER BY revision DESC
    LIMIT 1
)`
	if err := db.conn.Exec(ctx, malformedChecked); err == nil {
		t.Fatal("native manifest accepted a partial checked event-point")
	}
	const malformedAgreed = `
INSERT INTO clicksync.dataset_manifest
SELECT * REPLACE
(
    revision + 1 AS revision,
    toNullable(toUInt64(1)) AS last_agreed_event_seq
)
FROM
(
    SELECT *
    FROM clicksync.dataset_manifest
    PREWHERE manifest_key = 1
    ORDER BY revision DESC
    LIMIT 1
)`
	if err := db.conn.Exec(ctx, malformedAgreed); err == nil {
		t.Fatal("native manifest accepted a partial last-agreed event-point")
	}

	latest, found, err = db.loadLatestManifestRecord(ctx)
	if err != nil || !found {
		t.Fatalf("read native manifest head: found=%t err=%v", found, err)
	}
	const duplicateLatest = `
INSERT INTO clicksync.dataset_manifest
SELECT *
FROM
(
    SELECT *
    FROM clicksync.dataset_manifest
    PREWHERE manifest_key = 1
    ORDER BY revision DESC
    LIMIT 1
)`
	if err := db.conn.Exec(ctx, duplicateLatest); err != nil {
		t.Fatal(err)
	}
	var physicalRows uint64
	if err := db.conn.QueryRow(
		ctx,
		`SELECT count() FROM clicksync.dataset_manifest WHERE manifest_key = 1 AND revision = ?`,
		latest.Revision,
	).Scan(&physicalRows); err != nil {
		t.Fatal(err)
	}
	if physicalRows != 2 {
		t.Fatalf("lost-response duplicate physical rows = %d, want 2", physicalRows)
	}
	if got, found, err := db.loadLatestManifestRecord(ctx); err != nil ||
		!found || got.RowDigest != latest.RowDigest {
		t.Fatalf("identical native duplicate was not tolerated: found=%t err=%v", found, err)
	}

	conflict := latest
	conflict.TrustReason = "native conflicting retry"
	if err := db.appendManifestRecord(ctx, conflict); err == nil {
		t.Fatal("same-revision native manifest conflict was accepted")
	}
	if _, _, err := db.loadLatestManifestRecord(ctx); err == nil {
		t.Fatal("native conflicting latest physical rows did not fail closed")
	}
}
