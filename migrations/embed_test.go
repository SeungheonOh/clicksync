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
		"pending_rollback_check_id Nullable(UUID)",
		"pending_rollback_check_attempt Nullable(UInt32)",
		"pending_rollback_checked_event_seq Nullable(UInt64)",
		"pending_rollback_evidence_count Nullable(UInt32)",
		"pending_rollback_evidence_digest Nullable(FixedString(32))",
		"evidence_state Enum8",
		"evidence_count UInt32",
		"pending_evidence_payload String",
		"last_agreed_check_id Nullable(UUID)",
		"last_agreed_evidence_digest Nullable(FixedString(32))",
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

func TestTrustEvidenceAndRollbackCommitmentsAreInFreshSchema(t *testing.T) {
	for _, required := range []string{
		"evidence_ordinal UInt32",
		"proof_method Enum8",
		"peer_observations_by_logical_id",
		"ORDER BY (check_id, evidence_ordinal, observation_id)",
		"peer_observations_proof_flags",
		"evidence_count UInt32",
		"evidence_digest FixedString(32)",
		"pending_rollback_evidence_count",
		"pending_rollback_evidence_digest",
	} {
		if !strings.Contains(Initial, required) {
			t.Fatalf("fresh schema lacks trust/rollback contract field %q", required)
		}
	}
	for _, required := range []string{
		"evidence_count",
		"evidence_digest",
		"unhex(repeat('22', 32))",
	} {
		if !strings.Contains(ContractFixture, required) {
			t.Fatalf("contract fixture lacks rollback commitment %q", required)
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
