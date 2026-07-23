package cli

import (
	"errors"
	"strings"
	"testing"

	"github.com/clicksync-project/clickout/internal/limits"
	"github.com/clicksync-project/clickout/internal/repository"
)

func TestRequiredCommandsParse(t *testing.T) {
	t.Parallel()
	hash := strings.Repeat("01", 32)
	policy := strings.Repeat("02", 28)
	tests := [][]string{
		{"utxo", hash + "#2"},
		{"utxo", hash + "#2", "--at", hash},
		{"tx", hash},
		{"address", "addr1...", "--state", "current"},
		{"address", "addr1...", "--state", "history", "--limit", "12", "--cursor", "c"},
		{"datum", hash},
		{"redeemers", hash},
		{"metadata", hash},
		{"withdrawals", hash},
		{"trace", "--direction", "forward", "--utxo", hash + "#2"},
		{"trace", "--direction", "reverse", "--tx", hash, "--asset", policy + "."},
		{"trace", "--direction", "forward", "--address", "addr1...", "--max-depth", "32", "--max-nodes", "100000"},
	}
	for _, args := range tests {
		args := args
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			t.Parallel()
			if _, err := Parse(args); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestTraceDefaultsAndExclusiveSeed(t *testing.T) {
	t.Parallel()
	hash := strings.Repeat("01", 32)
	invocation, err := Parse([]string{
		"trace", "--direction", "forward", "--utxo", hash + "#0",
	})
	if err != nil {
		t.Fatal(err)
	}
	if invocation.Trace.Direction != repository.Forward ||
		invocation.Trace.Limits.MaxDepth != limits.DefaultTraceDepth ||
		invocation.Trace.Limits.MaxNodes != limits.DefaultTraceNodes {
		t.Fatalf("unexpected invocation: %#v", invocation)
	}
	if _, err := Parse([]string{
		"trace", "--direction", "forward", "--utxo", hash + "#0", "--tx", hash,
	}); !errors.Is(err, ErrUsage) {
		t.Fatalf("expected usage error, got %v", err)
	}
}

func TestHardBoundsAndRequiredAddressState(t *testing.T) {
	t.Parallel()
	hash := strings.Repeat("01", 32)
	for _, args := range [][]string{
		{"trace", "--direction", "forward", "--utxo", hash + "#0", "--max-depth", "33"},
		{"trace", "--direction", "forward", "--utxo", hash + "#0", "--max-nodes", "100001"},
		{"address", "addr1...", "--state", "current", "--limit", "10001"},
		{"address", "addr1..."},
	} {
		if _, err := Parse(args); !errors.Is(err, ErrUsage) {
			t.Fatalf("%v: expected usage error, got %v", args, err)
		}
	}
}
