package main

import (
	"strings"
	"testing"
)

func TestParsePoint(t *testing.T) {
	point, err := parsePoint("42:" + strings.Repeat("ab", 32))
	if err != nil {
		t.Fatal(err)
	}
	if point.Slot != 42 || len(point.Hash) != 32 {
		t.Fatalf("unexpected point: %#v", point)
	}
}

func TestParsePointRejectsMalformed(t *testing.T) {
	for _, value := range []string{"", "1", "x:" + strings.Repeat("00", 32), "1:00"} {
		if _, err := parsePoint(value); err == nil {
			t.Fatalf("accepted malformed point %q", value)
		}
	}
}

func TestParseFlagsUsesBoundedDefaults(t *testing.T) {
	cfg, err := parseFlags(nil)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.corroborate != 2 || cfg.ackWindow != 1 || len(cfg.peers) != 3 {
		t.Fatalf("unexpected defaults: %#v", cfg)
	}
}

func TestParseFlagsRejectsDuplicateCorroborationPeers(t *testing.T) {
	_, err := parseFlags([]string{
		"--peer", "relay.example:3001",
		"--peer", "RELAY.EXAMPLE:3001",
		"--corroborate", "2",
	})
	if err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("unexpected error: %v", err)
	}
}
