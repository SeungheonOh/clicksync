package config

import (
	"strings"
	"testing"
)

func TestValidateBoundsAndIndependentPeers(t *testing.T) {
	cfg := Config{
		ClickHouseHost:     "clickhouse",
		ClickHousePort:     9000,
		ClickHouseUser:     "clicksync",
		ClickHousePassword: "secret",
		ClickHouseDatabase: "clicksync",
		NetworkName:        "mainnet",
		NetworkMagic:       MainnetMagic,
		Peers: []Peer{
			{Host: "backbone.cardano.iog.io:3001", Operator: "iog"},
			{
				Host:     "backbone.mainnet.cardanofoundation.org:3001",
				Operator: "cardano-foundation",
			},
		},
		Corroboration:   2,
		Start:           "intersection",
		StartPoint:      "193253841:" + strings.Repeat("ab", 32),
		QueueCapacity:   4,
		HeaderBatchSize: 32,
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	cfg.Peers[1] = cfg.Peers[0]
	if err := cfg.Validate(); err == nil {
		t.Fatal("accepted duplicate corroboration peers")
	}
}

func TestPeersFromEnvRequiresExplicitIndependentOperatorLabels(t *testing.T) {
	t.Setenv(
		"CARDANO_PEERS",
		"relay.example:3001|operator-a,[2001:db8::1]:3002|operator-b",
	)
	cfg, err := PeersFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Peers) != 2 ||
		cfg.Peers[0].Host != "relay.example:3001" ||
		cfg.Peers[1].Host != "[2001:db8::1]:3002" {
		t.Fatalf("peers = %#v", cfg.Peers)
	}

	for name, value := range map[string]string{
		"missing label":       "one.example:3001,two.example:3001|two",
		"duplicate operator":  "one.example:3001|same,two.example:3001|same",
		"duplicate endpoint":  "one.example:3001|one,ONE.EXAMPLE:3001|two",
		"malformed endpoint":  "one.example|one,two.example:3001|two",
		"unsafe label":        "one.example:3001|one space,two.example:3001|two",
		"duplicate separator": "one.example:3001|one|extra,two.example:3001|two",
	} {
		t.Run(name, func(t *testing.T) {
			t.Setenv("CARDANO_PEERS", value)
			if _, err := PeersFromEnv(); err == nil {
				t.Fatalf("accepted %q", value)
			}
		})
	}
}

func TestPeersFromEnvIsMainnetOnly(t *testing.T) {
	t.Setenv("CARDANO_NETWORK_NAME", "preprod")
	if _, err := PeersFromEnv(); err == nil {
		t.Fatal("accepted non-mainnet network name")
	}
	t.Setenv("CARDANO_NETWORK_NAME", "mainnet")
	t.Setenv("CARDANO_NETWORK_MAGIC", "1")
	if _, err := PeersFromEnv(); err == nil {
		t.Fatal("accepted non-mainnet network magic")
	}
}

func TestParseStartPoint(t *testing.T) {
	hash := strings.Repeat("ab", 32)
	point, err := ParseStartPoint("193253841:" + hash)
	if err != nil {
		t.Fatal(err)
	}
	if point.Slot != 193253841 {
		t.Fatalf("point = %#v", point)
	}
	for _, value := range []string{
		"",
		"origin",
		"1:2",
		"13715435:193253841:" + hash,
		"x:" + hash,
		"1:00",
		"1:" + strings.Repeat("00", 32),
		"1:2:" + hash + ":extra",
	} {
		if _, err := ParseStartPoint(value); err == nil {
			t.Fatalf("accepted malformed start point %q", value)
		}
	}
}

func TestBlankIntersectionNeverFallsBackToOrigin(t *testing.T) {
	t.Setenv("CLICKHOUSE_PASSWORD", "secret")
	t.Setenv("CLICKSYNC_START", "intersection")
	t.Setenv("CLICKSYNC_START_POINT", "")
	if _, err := FromEnv(); err == nil ||
		!strings.Contains(err.Error(), "CLICKSYNC_START_POINT is required") {
		t.Fatalf("blank intersection error = %v", err)
	}
}
