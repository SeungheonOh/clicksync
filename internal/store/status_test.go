package store

import (
	"encoding/hex"
	"encoding/json"
	"testing"
	"time"
)

func TestDatasetStatusCoversTrustAndPendingStates(t *testing.T) {
	at := time.Date(2026, 7, 23, 21, 0, 0, 123456000, time.UTC)
	for _, test := range []struct {
		name   string
		mutate func(*manifestRecord)
	}{
		{name: "agreed"},
		{
			name: "unavailable",
			mutate: func(record *manifestRecord) {
				record.TrustStatus = "unavailable"
				record.TrustReason = "peers temporarily unavailable"
				record.CorroborationConfirmed = 1
			},
		},
		{
			name: "checking",
			mutate: func(record *manifestRecord) {
				record.TrustStatus = "checking"
				record.TrustReason = "exact point check in progress"
				record.CheckCompletedAt = nil
				record.CorroborationConfirmed = 0
			},
		},
		{
			name: "disputed",
			mutate: func(record *manifestRecord) {
				record.TrustStatus = "disputed"
				record.TrustReason = "mixed exact membership"
				record.Disagreement = true
				record.CorroborationConfirmed = 1
			},
		},
		{
			name: "unservable",
			mutate: func(record *manifestRecord) {
				record.TrustStatus = "unavailable"
				record.TrustReason = "bootstrap threshold unavailable"
				record.LastAgreed = nil
				record.LastAgreedAt = nil
				record.LastAgreedEvidence = nil
				record.Servable = false
				record.CorroborationConfirmed = 0
			},
		},
		{
			name: "pending",
			mutate: func(record *manifestRecord) {
				record.TrustStatus = "checking"
				record.CheckCompletedAt = nil
				record.CorroborationConfirmed = 0
				record.PendingRollback = &manifestPendingRollback{
					State:           "reserved",
					ID:              manifestID(0x81),
					EventSeq:        record.Physical.EventSeq + 1,
					To:              record.Physical.Point,
					OldPhysical:     record.Physical,
					Depth:           0,
					Reason:          "depth-zero pending status",
					Peers:           []string{"relay-a", "relay-b"},
					Operators:       []string{"operator-a", "operator-b"},
					Required:        2,
					CheckID:         *record.CheckID,
					Group:           *record.AgreementGroup,
					CheckAttempt:    record.CheckAttempt,
					CheckedEventSeq: record.Checked.EventSeq,
					EvidenceCount:   record.EvidenceCount,
					EvidenceDigest:  *record.EvidenceDigest,
					WriterID:        manifestID(0x82),
					StartedAt:       at,
				}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			record := validManifestRecord(t)
			if test.mutate != nil {
				test.mutate(&record)
			}
			if err := finalizeManifestRecord(&record); err != nil {
				t.Fatal(err)
			}
			status, err := datasetStatusFromManifest(record)
			if err != nil {
				t.Fatal(err)
			}
			if !status.Initialized ||
				status.DatasetID == "" ||
				status.SchemaContractHash == "" ||
				status.TrustStatus != record.TrustStatus ||
				status.Servable != record.Servable ||
				status.Physical.EventSeq != record.Physical.EventSeq ||
				status.Effective.EventSeq != record.Effective.EventSeq ||
				status.CorroborationRequired != record.CorroborationRequired ||
				status.CorroborationConfirmed != record.CorroborationConfirmed ||
				status.VisibilityGeneration != record.VisibilityGeneration {
				t.Fatalf("status = %+v", status)
			}
			if status.EvidenceState != record.EvidenceState ||
				status.EvidenceCount != record.EvidenceCount ||
				status.EvidenceDigest != hex.EncodeToString(record.EvidenceDigest[:]) {
				t.Fatalf("status evidence = state=%q count=%d digest=%q", status.EvidenceState, status.EvidenceCount, status.EvidenceDigest)
			}
			if (record.LastAgreedEvidence == nil) != (status.LastAgreedEvidence == nil) {
				t.Fatalf("last agreed evidence presence: record=%+v status=%+v", record.LastAgreedEvidence, status.LastAgreedEvidence)
			}
			if reference := record.LastAgreedEvidence; reference != nil {
				if status.LastAgreedEvidence.CheckID != hex.EncodeToString(reference.CheckID[:]) ||
					status.LastAgreedEvidence.AgreementGroup != hex.EncodeToString(reference.Group[:]) ||
					status.LastAgreedEvidence.CheckAttempt != reference.Attempt ||
					status.LastAgreedEvidence.Required != reference.Required ||
					status.LastAgreedEvidence.Confirmed != reference.Confirmed ||
					status.LastAgreedEvidence.Count != reference.Count ||
					status.LastAgreedEvidence.Digest != hex.EncodeToString(reference.Digest[:]) {
					t.Fatalf("last agreed evidence status = %+v, record = %+v", status.LastAgreedEvidence, reference)
				}
			}
			if (record.PendingRollback == nil) != (status.PendingRollback == nil) {
				t.Fatalf("pending status = %+v", status.PendingRollback)
			}
			if pending := record.PendingRollback; pending != nil {
				if status.PendingRollback.EvidenceCount != pending.EvidenceCount ||
					status.PendingRollback.EvidenceDigest != hex.EncodeToString(pending.EvidenceDigest[:]) {
					t.Fatalf("pending rollback evidence status = %+v, record = %+v", status.PendingRollback, pending)
				}
			}
			encoded, err := json.Marshal(status)
			if err != nil {
				t.Fatal(err)
			}
			var payload map[string]any
			if err := json.Unmarshal(encoded, &payload); err != nil {
				t.Fatal(err)
			}
			for _, required := range []string{
				"manifest_revision",
				"transition_id",
				"transition_kind",
				"row_digest",
				"physical",
				"effective",
				"trust_status",
				"check_started_at",
				"evidence_state",
				"evidence_count",
				"evidence_digest",
				"checkpoint_interval",
				"writer_build",
				"source_build",
			} {
				if _, ok := payload[required]; !ok {
					t.Fatalf("JSON status lacks %q: %s", required, encoded)
				}
			}
			for field, length := range map[string]int{
				"dataset_id":           32,
				"schema_contract_hash": 64,
				"transition_id":        32,
				"row_digest":           64,
				"check_id":             32,
				"agreement_group":      32,
				"writer_id":            32,
			} {
				value, ok := payload[field].(string)
				if !ok || len(value) != length {
					t.Fatalf("JSON %s=%v, want lowercase hex length %d", field, payload[field], length)
				}
			}
			for _, field := range []string{
				"check_started_at",
				"check_completed_at",
				"last_agreed_at",
			} {
				if value, ok := payload[field].(string); ok {
					parsed, err := time.Parse(time.RFC3339Nano, value)
					if err != nil || parsed.Location() != time.UTC {
						t.Fatalf("JSON %s=%q is not RFC3339 UTC: %v", field, value, err)
					}
				}
			}
			_, pendingPresent := payload["pending_rollback"]
			if pendingPresent != (record.PendingRollback != nil) {
				t.Fatalf("JSON pending presence=%t record=%+v", pendingPresent, record.PendingRollback)
			}
			if record.LastAgreedEvidence != nil {
				lastEvidence, ok := payload["last_agreed_evidence"].(map[string]any)
				if !ok {
					t.Fatalf("JSON last_agreed_evidence=%v", payload["last_agreed_evidence"])
				}
				for field, length := range map[string]int{
					"check_id":        32,
					"agreement_group": 32,
					"digest":          64,
				} {
					value, ok := lastEvidence[field].(string)
					if !ok || len(value) != length {
						t.Fatalf("JSON last_agreed_evidence.%s=%v, want hex length %d", field, lastEvidence[field], length)
					}
				}
			}
			if pendingPresent {
				pending := payload["pending_rollback"].(map[string]any)
				if digest, ok := pending["evidence_digest"].(string); !ok || len(digest) != 64 {
					t.Fatalf("JSON pending_rollback.evidence_digest=%v", pending["evidence_digest"])
				}
			}
			if test.name == "unservable" && payload["servable"] != false {
				t.Fatalf("unservable JSON = %s", encoded)
			}
		})
	}
}
