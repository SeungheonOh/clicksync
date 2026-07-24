package main

import (
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"

	"cardano-clicksync/internal/config"
	"cardano-clicksync/internal/model"
	"cardano-clicksync/internal/store"

	"github.com/google/uuid"
)

func TestUsageListsOnlyImplementedCommands(t *testing.T) {
	err := run(nil, os.Stdout, discardCommandLogger())
	if err == nil || err.Error() != usage {
		t.Fatalf("usage error = %v, want %q", err, usage)
	}
	for _, removed := range []string{
		"peers",
		"writer",
		"lease",
		"corroboration",
	} {
		if strings.Contains(err.Error(), removed) {
			t.Fatalf("usage retained removed command %q", removed)
		}
	}
}

func TestUnknownCommandFailsBeforeConfiguration(t *testing.T) {
	t.Setenv("CLICKHOUSE_PASSWORD", "")
	t.Setenv("CARDANO_RELAYS", "malformed")
	err := run([]string{"unknown"}, os.Stdout, discardCommandLogger())
	if err == nil || !strings.Contains(err.Error(), `unknown command "unknown"`) {
		t.Fatalf("unknown command error = %v", err)
	}
}

func TestStatusConfigurationIgnoresRelaySettings(t *testing.T) {
	t.Setenv("CLICKHOUSE_PASSWORD", "secret")
	t.Setenv("CARDANO_RELAYS", "malformed")
	t.Setenv("CLICKSYNC_ROLLBACK_DEPTH", "123")
	if _, err := config.DatabaseFromEnv(); err != nil {
		t.Fatalf("status database config used relay settings: %v", err)
	}
	if depth, err := config.RollbackDepthFromEnv(); err != nil || depth != 123 {
		t.Fatalf("status rollback depth = %d, %v", depth, err)
	}
}

func TestStatusOutputUsesBinarySafeHexPoints(t *testing.T) {
	hash := model.Hash32{0x01, 0x02, 0xff}
	schema := model.Hash32{0xaa}
	state := store.State{
		Dataset: store.DatasetIdentity{
			DatasetID:    uuid.MustParse("00000000-0000-0000-0000-000000000001"),
			SchemaHash:   schema,
			NetworkMagic: 42,
			NetworkName:  "test",
			Start: store.Point{
				Slot:        1,
				Hash:        hash,
				BlockNumber: 2,
			},
			SourceBuild: "test-build",
		},
		Snapshot: 9,
		Tip: store.Point{
			Slot:        3,
			Hash:        hash,
			BlockNumber: 4,
		},
		Canonical: []store.CanonicalBlock{{}},
	}
	got := statusFromState(state)
	if !got.Initialized ||
		got.SchemaHash[:2] != "aa" ||
		got.Tip.Hash !=
			"0102ff0000000000000000000000000000000000000000000000000000000000" ||
		got.CanonicalDepth != 1 {
		t.Fatalf("unexpected status output: %#v", got)
	}
}

func discardCommandLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
