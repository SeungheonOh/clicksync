package migrations

import (
	"crypto/sha256"
	"strings"
	"testing"
)

func TestDatasetManifestIsAppendOnlyAndPKPruned(t *testing.T) {
	start := strings.Index(Initial, "CREATE TABLE IF NOT EXISTS clicksync.dataset_manifest")
	end := strings.Index(Initial[start:], "CREATE TABLE IF NOT EXISTS clicksync.blocks")
	if start < 0 || end < 0 {
		t.Fatal("dataset_manifest DDL section is missing")
	}
	manifest := Initial[start : start+end]
	for _, required := range []string{
		"schema_contract_hash FixedString(32)",
		"transition_id UUID",
		"previous_row_digest Nullable(FixedString(32))",
		"row_digest FixedString(32)",
		"physical_event_seq UInt64",
		"effective_event_seq UInt64",
		"visibility_generation UInt64",
		"pending_rollback_state Enum8",
		"ENGINE = MergeTree",
		"ORDER BY (manifest_key, revision)",
		"SETTINGS index_granularity = 64",
	} {
		if !strings.Contains(manifest, required) {
			t.Fatalf("dataset_manifest DDL lacks %q", required)
		}
	}
	for _, forbidden := range []string{
		"ReplacingMergeTree",
		"committed_event_seq",
		"committed_tip_",
	} {
		if strings.Contains(manifest, forbidden) {
			t.Fatalf("dataset_manifest DDL retains forbidden %q", forbidden)
		}
	}
}

func TestNullableManifestPointChecksAreNullTotal(t *testing.T) {
	start := strings.Index(Initial, "CREATE TABLE IF NOT EXISTS clicksync.dataset_manifest")
	end := strings.Index(Initial[start:], "CREATE TABLE IF NOT EXISTS clicksync.blocks")
	manifest := Initial[start : start+end]
	for _, nullableBool := range []string{
		"checked_point_origin",
		"last_agreed_point_origin",
		"pending_rollback_to_origin",
		"pending_rollback_old_physical_origin",
	} {
		if !strings.Contains(manifest, "isNotNull("+nullableBool+")") ||
			!strings.Contains(manifest, "assumeNotNull("+nullableBool+")") {
			t.Fatalf("nullable point field %s lacks null-total checks", nullableBool)
		}
	}
}

func TestContractHashIsCanonicalDescriptorContentIdentity(t *testing.T) {
	if ContractDescriptor == "" {
		t.Fatal("canonical schema contract descriptor is empty")
	}
	if got := sha256.Sum256([]byte(ContractDescriptor)); got != ContractHash {
		t.Fatalf("contract hash = %x, want %x", ContractHash, got)
	}
}
