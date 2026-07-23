package main

import (
	"strings"
	"testing"
)

func TestDocumentedPeersCommandNeedsNoDatabaseOrNetwork(t *testing.T) {
	t.Setenv("CLICKHOUSE_PASSWORD", "")
	t.Setenv("CARDANO_PEERS", "")
	if err := run([]string{"peers"}); err != nil {
		t.Fatal(err)
	}
}

func TestUsageListsOnlySupportedCommands(t *testing.T) {
	err := run(nil)
	if err == nil {
		t.Fatal("missing usage error")
	}
	const expected = "usage: clicksync migrate|sync|status|peers|writer"
	if err.Error() != expected {
		t.Fatalf("usage = %q, want %q", err, expected)
	}
	for _, removed := range []string{"storage", "lease"} {
		if strings.Contains(err.Error(), removed) {
			t.Fatalf("usage retained removed command %q", removed)
		}
	}
}

func TestRemovedAndUnknownCommandsFailBeforeConfiguration(t *testing.T) {
	t.Setenv("CLICKHOUSE_PASSWORD", "")
	t.Setenv("CARDANO_PEERS", "malformed")
	for _, command := range []string{"storage", "lease", "unknown"} {
		err := run([]string{command})
		if err == nil || err.Error() != "unknown ingestion command "+`"`+command+`"` {
			t.Fatalf("%s error = %v", command, err)
		}
	}
}
