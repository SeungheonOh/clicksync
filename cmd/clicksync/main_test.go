package main

import "testing"

func TestDocumentedPeersCommandNeedsNoDatabaseOrNetwork(t *testing.T) {
	t.Setenv("CLICKHOUSE_PASSWORD", "")
	t.Setenv("CARDANO_PEERS", "")
	if err := run([]string{"peers"}); err != nil {
		t.Fatal(err)
	}
}
