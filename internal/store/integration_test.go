//go:build clickhouse_integration

package store

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"

	"cardano-clicksync/internal/config"
)

func TestClickHouseFreshInitializationAndWriteOnlyPublication(t *testing.T) {
	if os.Getenv("CLICKSYNC_CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set CLICKSYNC_CLICKHOUSE_INTEGRATION=1 for the destructive dedicated-database integration test")
	}
	cfg, err := config.DatabaseFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	db, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := db.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	if err := db.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := db.Migrate(ctx); err != nil {
		t.Fatalf("idempotent second migration: %v", err)
	}
	if _, err := db.Initialize(ctx, DatasetConfig{
		NetworkMagic: 42,
		NetworkName:  "integration",
		Start:        Point{Origin: true},
		SourceBuild:  "integration-test",
	}); err != nil {
		t.Fatal(err)
	}

	marker := fmt.Sprintf("cardano-clicksync-integration-%d", time.Now().UnixNano())
	publishCtx := clickhouse.Context(
		ctx,
		clickhouse.WithSettings(clickhouse.Settings{"log_comment": marker}),
	)
	candidate := richCandidate(0x71)
	if _, err := db.Publish(
		publishCtx,
		&fakeLock{},
		[]Candidate{candidate},
	); err != nil {
		t.Fatal(err)
	}
	state, err := db.State(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if state.Snapshot != 1 || state.Tip.Hash != candidate.Block.Hash ||
		len(state.Canonical) != 1 {
		t.Fatalf("post-adoption state = %#v", state)
	}
	inspected, found, err := db.Inspect(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if !found || inspected.Snapshot != state.Snapshot ||
		inspected.Tip != state.Tip {
		t.Fatalf("read-only Inspect = %#v, found %v", inspected, found)
	}
	if _, err := db.Rollback(ctx, &fakeLock{}, RollbackRequest{
		To: Point{Origin: true},
		Relays: []Relay{
			{Host: "a:3001", Address: "192.0.2.1:3001", Operator: "a", N2NVersion: 13},
			{Host: "b:3001", Address: "192.0.2.2:3001", Operator: "b", N2NVersion: 13},
		},
		Reason:       "integration rollback",
		MaximumDepth: 10,
	}); err != nil {
		t.Fatal(err)
	}
	state, err = db.State(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if state.Snapshot != 2 || !state.Tip.Origin ||
		len(state.Canonical) != 0 {
		t.Fatalf("post-rollback state = %#v", state)
	}
	readopted, err := db.Publish(
		ctx,
		&fakeLock{},
		[]Candidate{candidate},
	)
	if err != nil {
		t.Fatal(err)
	}
	if readopted.FirstPublicationID != 2 || readopted.FirstEventSeq != 3 {
		t.Fatalf("re-adoption reused identifiers: %#v", readopted)
	}
	if err := db.conn.Exec(ctx, "SYSTEM FLUSH LOGS"); err != nil {
		t.Fatal(err)
	}
	var selects, inserts uint64
	if err := db.conn.QueryRow(ctx, `
SELECT
    countIf(query_kind = 'Select'),
    countIf(query_kind = 'Insert')
FROM system.query_log
WHERE type = 'QueryFinish'
  AND log_comment = ?
`, marker).Scan(&selects, &inserts); err != nil {
		t.Fatal(err)
	}
	if selects != 0 {
		t.Fatalf("ordinary roll-forward issued %d SELECT queries", selects)
	}
	if inserts != 10 {
		t.Fatalf(
			"ordinary populated one-block roll-forward issued %d inserts, want nine fact tables plus adoption",
			inserts,
		)
	}
}

func BenchmarkClickHousePublisherRepresentativeBatch(b *testing.B) {
	if os.Getenv("CLICKSYNC_CLICKHOUSE_INTEGRATION") != "1" {
		b.Skip("set CLICKSYNC_CLICKHOUSE_INTEGRATION=1 against a disposable ClickHouse")
	}
	cfg, err := config.DatabaseFromEnv()
	if err != nil {
		b.Fatal(err)
	}
	db, err := Open(cfg)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	if err := db.Ping(ctx); err != nil {
		b.Fatal(err)
	}
	if err := db.Migrate(ctx); err != nil {
		b.Fatal(err)
	}
	if _, err := db.Initialize(ctx, DatasetConfig{
		NetworkMagic: 42,
		NetworkName:  "integration",
		Start:        Point{Origin: true},
		SourceBuild:  "integration-test",
	}); err != nil {
		b.Fatal(err)
	}

	const blocksPerBatch = 1024
	candidates := make([]Candidate, blocksPerBatch)
	var (
		rawBytes int64
		factRows uint64
	)
	for index := range candidates {
		candidates[index] = richCandidate(byte(index%250 + 1))
		rawBytes += int64(candidates[index].RawLength)
		counts := countFacts(candidates[index].Block)
		factRows += 1 +
			uint64(len(candidates[index].Block.Datums)) +
			counts.transactions +
			counts.inputs +
			counts.outputs +
			counts.datumObservations +
			counts.withdrawals +
			counts.redeemers +
			counts.metadata
	}
	lock := &fakeLock{}
	b.ReportAllocs()
	b.SetBytes(rawBytes)
	b.ResetTimer()
	for range b.N {
		if _, err := db.Publish(ctx, lock, candidates); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(blocksPerBatch, "blocks/op")
	b.ReportMetric(float64(factRows), "fact_rows/op")
}
