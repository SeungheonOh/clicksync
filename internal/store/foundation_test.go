package store

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"

	"cardano-clicksync/internal/config"
	"cardano-clicksync/internal/model"
	"cardano-clicksync/migrations"
)

func TestClickHouseOptionsAreSynchronousNativeAndPooled(t *testing.T) {
	t.Parallel()
	cfg := config.Database{
		Host:     "db.internal",
		Port:     9440,
		User:     "writer",
		Password: "secret",
		Name:     "clicksync",
		OpenConn: 23,
	}
	options := clickHouseOptions(cfg, cfg.OpenConn)
	if options.Protocol != clickhouse.Native {
		t.Fatalf("protocol = %v, want native", options.Protocol)
	}
	if options.Compression == nil ||
		options.Compression.Method != clickhouse.CompressionLZ4 {
		t.Fatalf("compression = %#v, want LZ4", options.Compression)
	}
	if got := options.Settings["async_insert"]; got != 0 {
		t.Fatalf("async_insert = %#v, want 0", got)
	}
	if got := options.Settings["wait_for_async_insert"]; got != 1 {
		t.Fatalf("wait_for_async_insert = %#v, want 1", got)
	}
	if options.MaxOpenConns != 23 || options.MaxIdleConns != 23 {
		t.Fatalf(
			"pool = %d/%d, want 23/23",
			options.MaxOpenConns,
			options.MaxIdleConns,
		)
	}
	if options.Auth.Database != "default" {
		t.Fatalf("initial database = %q, want default", options.Auth.Database)
	}
}

func TestMigrateExecutesOnlyEmbeddedIdempotentStatements(t *testing.T) {
	t.Parallel()
	connection := newFakeConnection()
	db := newDB(connection)
	if err := db.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	statements, err := migrations.SplitSQL(migrations.Initial)
	if err != nil {
		t.Fatal(err)
	}
	if len(connection.execs) != len(statements) {
		t.Fatalf("Exec count = %d, want %d", len(connection.execs), len(statements))
	}
	for _, statement := range connection.execs {
		if !strings.Contains(strings.ToUpper(statement), "IF NOT EXISTS") {
			t.Fatalf("non-idempotent migration statement: %.100q", statement)
		}
	}
}

func TestInitializeFreshExactRestartAndLostResponse(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name         string
		lostResponse bool
		start        Point
	}{
		{name: "acknowledged_origin", start: Point{Origin: true}},
		{
			name: "acknowledged_partial_start",
			start: Point{
				Slot:        42,
				Hash:        testHash(0x42),
				BlockNumber: 7,
				IsByronEBB:  true,
			},
		},
		{name: "response_lost", lostResponse: true, start: Point{Origin: true}},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			connection := newFakeConnection()
			if test.lostResponse {
				connection.sendErrors["dataset"] = errors.New("connection reset")
				connection.persistOnError["dataset"] = true
			}
			db := newDB(connection)
			now := time.Date(2026, 7, 24, 12, 34, 56, 123456000, time.UTC)
			db.now = func() time.Time { return now }
			cfg := DatasetConfig{
				NetworkMagic: 42,
				NetworkName:  "testnet",
				Start:        test.start,
				SourceBuild:  "first-build",
			}
			first, err := db.Initialize(context.Background(), cfg)
			if err != nil {
				t.Fatal(err)
			}
			if first.SchemaHash != model.Hash32(migrations.SchemaHash) {
				t.Fatalf("schema hash = %x, want %x", first.SchemaHash, migrations.SchemaHash)
			}
			if first.CreatedAt != now || first.DatasetID.String() == "" {
				t.Fatalf("unexpected initialized identity: %#v", first)
			}

			restarted := newDB(connection)
			cfg.SourceBuild = "newer-executable"
			second, err := restarted.Initialize(context.Background(), cfg)
			if err != nil {
				t.Fatal(err)
			}
			if !sameDatasetIdentity(first, second) {
				t.Fatalf("restart identity changed:\nfirst %#v\nsecond %#v", first, second)
			}
		})
	}
}

func TestInitializeRejectsConflictingDatasetIdentity(t *testing.T) {
	t.Parallel()
	connection := newFakeConnection()
	db := newDB(connection)
	cfg := DatasetConfig{
		NetworkMagic: 42,
		NetworkName:  "testnet",
		Start:        Point{Origin: true},
		SourceBuild:  "build",
	}
	if _, err := db.Initialize(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}

	restarted := newDB(connection)
	conflicting := cfg
	conflicting.NetworkMagic = 43
	if _, err := restarted.Initialize(context.Background(), conflicting); err == nil {
		t.Fatal("conflicting network magic was accepted")
	}

	connection.mu.Lock()
	duplicate := append([]any(nil), connection.datasetRows[0]...)
	duplicate[3] = "other-network"
	connection.datasetRows = append(connection.datasetRows, duplicate)
	connection.mu.Unlock()
	restarted = newDB(connection)
	if _, err := restarted.Initialize(context.Background(), cfg); err == nil ||
		!strings.Contains(err.Error(), "conflicting immutable identities") {
		t.Fatalf("physical conflict error = %v", err)
	}
}

func TestHighWaterQueriesCoverRawOrphanTables(t *testing.T) {
	t.Parallel()
	for _, table := range []string{
		"blocks",
		"transactions",
		"inputs",
		"outputs",
		"datum_bodies",
		"datum_observations",
		"withdrawals",
		"redeemers",
		"transaction_metadata",
		"chain_events",
	} {
		if !strings.Contains(publicationHighWaterSQL, "clicksync."+table) {
			t.Errorf("publication high-water omits %s", table)
		}
	}
	if !strings.Contains(eventHighWaterSQL, "clicksync.chain_events") ||
		!strings.Contains(eventHighWaterSQL, "clicksync.rollbacks") {
		t.Fatal("event high-water omits a raw event table")
	}

	connection := newFakeConnection()
	connection.publicationHighWater = 91
	connection.eventHighWater = 37
	allocator, err := newDB(connection).loadAllocator(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	firstPublication, err := allocator.reservePublications(3)
	if err != nil || firstPublication != 92 {
		t.Fatalf("publication reservation = %d, %v; want 92", firstPublication, err)
	}
	firstEvent, err := allocator.reserveEvents(2)
	if err != nil || firstEvent != 38 {
		t.Fatalf("event reservation = %d, %v; want 38", firstEvent, err)
	}
}

func TestAllocatorExhaustionNeverWraps(t *testing.T) {
	t.Parallel()
	if _, err := newAllocator(math.MaxUint64, 0); err == nil {
		t.Fatal("maximum publication high-water was accepted")
	}
	allocator, err := newAllocator(math.MaxUint64-1, math.MaxUint64-1)
	if err != nil {
		t.Fatal(err)
	}
	if first, err := allocator.reservePublications(1); err != nil ||
		first != math.MaxUint64 {
		t.Fatalf("last publication = %d, %v", first, err)
	}
	if _, err := allocator.reservePublications(1); err == nil {
		t.Fatal("publication allocator wrapped")
	}
	if first, err := allocator.reserveEvents(1); err != nil ||
		first != math.MaxUint64 {
		t.Fatalf("last event = %d, %v", first, err)
	}
	if _, err := allocator.reserveEvents(1); err == nil {
		t.Fatal("event allocator wrapped")
	}
}
