package config

import (
	"strings"
	"testing"
)

func TestParseRelaysRequiresStrictIndependentSet(t *testing.T) {
	valid, err := ParseRelays("A.example:3001|one,b.example:3002|two")
	if err != nil {
		t.Fatalf("parse valid relays: %v", err)
	}
	if got, want := len(valid), 2; got != want {
		t.Fatalf("relay count = %d, want %d", got, want)
	}
	for _, test := range []string{
		"a.example:3001|one",
		"a.example:3001|one,A.EXAMPLE:3001|two",
		"a.example:3001|one,b.example:3001|ONE",
		"a.example:0|one,b.example:3001|two",
		"a.example:3001,b.example:3001|two",
	} {
		if _, err := ParseRelays(test); err == nil {
			t.Errorf("ParseRelays(%q) unexpectedly succeeded", test)
		}
	}
}

func TestParsePoint(t *testing.T) {
	origin, err := ParsePoint("origin")
	if err != nil || !origin.Origin {
		t.Fatalf("parse origin = %#v, %v", origin, err)
	}
	hash := strings.Repeat("ab", 32)
	point, err := ParsePoint("42:" + hash + ":7:ebb")
	if err != nil {
		t.Fatalf("parse point: %v", err)
	}
	if point.Origin || point.Slot != 42 || point.BlockNumber != 7 ||
		!point.IsByronEBB || point.Hash[0] != 0xab {
		t.Fatalf("unexpected point: %#v", point)
	}
	for _, value := range []string{
		"",
		"42:" + hash,
		"no:" + hash + ":7",
		"42:abcd:7",
		"42:" + strings.Repeat("00", 32) + ":7",
		"42:" + hash + ":no",
		"42:" + hash + ":7:other",
	} {
		if _, err := ParsePoint(value); err == nil {
			t.Errorf("ParsePoint(%q) unexpectedly succeeded", value)
		}
	}
}

func TestSyncConfigRejectsThresholdLikeOrUnboundedValues(t *testing.T) {
	t.Setenv("CLICKHOUSE_PASSWORD", "secret")
	t.Setenv("CARDANO_RELAYS", "a.example:3001|one,b.example:3001|two")
	t.Setenv("CLICKSYNC_BLOCKFETCH_RANGE_BLOCKS", "4096")
	cfg, err := SyncFromEnv()
	if err != nil {
		t.Fatalf("default sync config: %v", err)
	}
	if got, want := len(cfg.Relays), 2; got != want {
		t.Fatalf("relay count = %d, want %d", got, want)
	}
	if cfg.BlockFetchRangeBlocks != 4096 {
		t.Fatalf(
			"BlockFetch range = %d, want 4096",
			cfg.BlockFetchRangeBlocks,
		)
	}
	cfg.BlockFetchRangeBlocks = 8193
	if err := cfg.Validate(); err == nil {
		t.Fatal("oversized BlockFetch range unexpectedly validated")
	}
	cfg.BlockFetchRangeBlocks = 4096
	cfg.BlockFetchQueueSize = 513
	if err := cfg.Validate(); err == nil {
		t.Fatal("oversized BlockFetch queue unexpectedly validated")
	}
}

func TestDatabaseConfigIgnoresRelaySettings(t *testing.T) {
	t.Setenv("CLICKHOUSE_PASSWORD", "secret")
	t.Setenv("CARDANO_RELAYS", "invalid")
	if _, err := DatabaseFromEnv(); err != nil {
		t.Fatalf("database-only config used relay settings: %v", err)
	}
}

func TestRollbackDepthConfigIsIndependentAndBounded(t *testing.T) {
	t.Setenv("CARDANO_RELAYS", "invalid")
	t.Setenv("CLICKSYNC_ROLLBACK_DEPTH", "4096")
	depth, err := RollbackDepthFromEnv()
	if err != nil {
		t.Fatalf("rollback-depth config: %v", err)
	}
	if depth != 4096 {
		t.Fatalf("rollback depth = %d, want 4096", depth)
	}
	for _, value := range []string{"0", "100001", "invalid"} {
		t.Setenv("CLICKSYNC_ROLLBACK_DEPTH", value)
		if _, err := RollbackDepthFromEnv(); err == nil {
			t.Fatalf("rollback depth %q unexpectedly succeeded", value)
		}
	}
}
