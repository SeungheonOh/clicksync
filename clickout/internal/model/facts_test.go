package model

import (
	"encoding/json"
	"slices"
	"testing"
)

func TestFlowHyperedgeDerivesOneCompactTransactionContext(t *testing.T) {
	t.Parallel()
	fee := uint64(7)
	source := Output{Ref: UTxORef{TxHash: Hash32{1}, Index: 2}}
	consumed := Spend{
		Source:         source.Ref,
		IsConsumed:     true,
		SourceResolved: true,
		SourceOutput:   &source,
	}
	reference := Spend{
		Source:     UTxORef{TxHash: Hash32{2}, Index: 3},
		Role:       InputReference,
		IsConsumed: false,
	}
	produced := Output{Ref: UTxORef{TxHash: Hash32{3}, Index: 4}}
	applied := Withdrawal{Applied: true}
	unapplied := Withdrawal{Applied: false}
	transaction := Transaction{
		Hash:         Hash32{9},
		Era:          "Conway",
		Phase2Valid:  true,
		FlowKind:     "regular",
		EffectiveFee: &fee,
		MintApplied:  true,
		Mint: []SignedAssetQuantity{
			{PolicyID: PolicyID{1}, Quantity: 4},
			{PolicyID: PolicyID{2}, Quantity: -3},
			{PolicyID: PolicyID{3}, Quantity: 0},
		},
		Inputs:      []Spend{consumed, reference},
		Outputs:     []Output{produced},
		Withdrawals: []Withdrawal{applied, unapplied},
		Redeemers:   []Redeemer{{Applied: false}},
		Metadata:    &TransactionMetadata{Labels: []uint64{1}},
		Datums: []TransactionDatum{{
			Hash:         Hash32{4},
			BodyCBOR:     Bytes{0x01},
			BodyVerified: true,
			Observations: []TransactionDatumObservation{{
				SourceKind:    "witness",
				SourceOrdinal: 0,
			}},
		}},
	}

	edge := NewFlowHyperedge(transaction)
	if edge.Transaction.Hash != transaction.Hash ||
		len(edge.Transaction.AppliedWithdrawals) != 1 ||
		!edge.Transaction.AppliedWithdrawals[0].Applied ||
		len(edge.Transaction.Redeemers) != 1 ||
		len(edge.Transaction.Datums) != 1 {
		t.Fatalf("compact transaction context = %#v", edge.Transaction)
	}
	if len(edge.Inputs) != 2 || len(edge.Outputs) != 1 {
		t.Fatalf("edge inputs/outputs = %d/%d", len(edge.Inputs), len(edge.Outputs))
	}
	if got := edge.ConsumedInputs(); len(got) != 1 || got[0].Source != source.Ref {
		t.Fatalf("consumed inputs = %#v", got)
	}
	if got := edge.ConsumedInputValues(); len(got) != 1 || got[0].Ref != source.Ref {
		t.Fatalf("consumed values = %#v", got)
	}
	if got := transaction.ConsumedInputRefs(); len(got) != 1 || got[0] != source.Ref {
		t.Fatalf("consumed refs = %#v", got)
	}
	if got := transaction.ProducedOutputs(); len(got) != 1 || got[0].Ref != produced.Ref {
		t.Fatalf("produced outputs = %#v", got)
	}
	if got := transaction.AppliedWithdrawals(); len(got) != 1 || !got[0].Applied {
		t.Fatalf("applied withdrawals = %#v", got)
	}
	if edge.FeeSink == nil || edge.FeeSink.Lovelace != fee {
		t.Fatalf("fee sink = %#v", edge.FeeSink)
	}
	if len(edge.MintDeltas) != 2 ||
		!edge.MintDeltas[0].IsSource || edge.MintDeltas[0].IsSink ||
		edge.MintDeltas[1].IsSource || !edge.MintDeltas[1].IsSink {
		t.Fatalf("mint deltas = %#v", edge.MintDeltas)
	}
}

func TestFlowHyperedgeJSONHasNoRemovedDuplicateFields(t *testing.T) {
	t.Parallel()
	fee := uint64(7)
	edge := NewFlowHyperedge(Transaction{
		Hash:         Hash32{1},
		EffectiveFee: &fee,
		MintApplied:  true,
		Mint: []SignedAssetQuantity{{
			PolicyID: PolicyID{2},
			Quantity: 3,
		}},
		Inputs:  []Spend{},
		Outputs: []Output{},
	})
	encoded, err := json.Marshal(edge)
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &document); err != nil {
		t.Fatal(err)
	}
	for _, removed := range []string{
		"consumed_inputs",
		"consumed_input_values",
		"produced_outputs",
		"applied_withdrawals",
	} {
		if _, exists := document[removed]; exists {
			t.Errorf("removed top-level field %q present: %s", removed, encoded)
		}
	}
	if _, exists := document["inputs"]; !exists {
		t.Errorf("edge inputs absent: %s", encoded)
	}
	if _, exists := document["outputs"]; !exists {
		t.Errorf("edge outputs absent: %s", encoded)
	}

	var transaction map[string]json.RawMessage
	if err := json.Unmarshal(document["transaction"], &transaction); err != nil {
		t.Fatal(err)
	}
	for _, duplicate := range []string{"inputs", "outputs", "mint", "withdrawals"} {
		if _, exists := transaction[duplicate]; exists {
			t.Errorf("compact transaction repeats %q: %s", duplicate, encoded)
		}
	}
	if _, exists := transaction["applied_withdrawals"]; !exists {
		t.Errorf("compact transaction omits applied_withdrawals: %s", encoded)
	}
	for _, contextField := range []string{
		"applied_withdrawals",
		"redeemers",
		"metadata",
	} {
		raw, exists := transaction[contextField]
		if !exists {
			continue
		}
		if string(raw) == "null" {
			continue
		}
		var entries []map[string]json.RawMessage
		if contextField == "metadata" {
			var entry map[string]json.RawMessage
			if err := json.Unmarshal(raw, &entry); err != nil {
				t.Fatal(err)
			}
			entries = []map[string]json.RawMessage{entry}
		} else if err := json.Unmarshal(raw, &entries); err != nil {
			t.Fatal(err)
		}
		for _, entry := range entries {
			if _, exists := entry["tx_hash"]; exists {
				t.Errorf("%s repeats tx_hash: %s", contextField, encoded)
			}
		}
	}
	var feeSink map[string]json.RawMessage
	if err := json.Unmarshal(document["fee_sink"], &feeSink); err != nil {
		t.Fatal(err)
	}
	if _, exists := feeSink["tx_hash"]; exists {
		t.Errorf("fee sink repeats tx_hash: %s", encoded)
	}
	var mint []map[string]json.RawMessage
	if err := json.Unmarshal(document["mint_deltas"], &mint); err != nil {
		t.Fatal(err)
	}
	if len(mint) != 1 {
		t.Fatalf("mint deltas = %s", document["mint_deltas"])
	}
	if _, exists := mint[0]["tx_hash"]; exists {
		t.Errorf("mint delta repeats tx_hash: %s", encoded)
	}
}

func TestTransactionDatumContextValidation(t *testing.T) {
	t.Parallel()
	outputIndex := uint32(3)
	for _, valid := range []TransactionDatum{
		{
			Hash:         Hash32{1},
			BodyCBOR:     Bytes{1},
			BodyVerified: true,
			Observations: []TransactionDatumObservation{{
				SourceKind: "witness",
			}},
		},
		{
			Hash:         Hash32{2},
			BodyCBOR:     Bytes{1},
			BodyVerified: true,
			Observations: []TransactionDatumObservation{{
				SourceKind:  "inline_output",
				OutputIndex: &outputIndex,
			}},
		},
	} {
		if !valid.Valid() {
			t.Errorf("valid datum rejected: %#v", valid)
		}
	}
	for _, invalid := range []TransactionDatum{
		{},
		{BodyCBOR: Bytes{1}, BodyVerified: true},
		{
			BodyCBOR:     Bytes{1},
			BodyVerified: true,
			Observations: []TransactionDatumObservation{{
				SourceKind: "unknown",
			}},
		},
		{
			BodyCBOR:     Bytes{1},
			BodyVerified: true,
			Observations: []TransactionDatumObservation{
				{SourceKind: "witness", SourceOrdinal: 1},
				{SourceKind: "witness", SourceOrdinal: 0},
			},
		},
		{
			BodyVerified: true,
			Observations: []TransactionDatumObservation{{
				SourceKind: "witness",
			}},
		},
		{
			BodyCBOR: Bytes{1},
			Observations: []TransactionDatumObservation{{
				SourceKind: "witness",
			}},
		},
	} {
		if invalid.Valid() {
			t.Errorf("invalid datum accepted: %#v", invalid)
		}
	}

	valid := Transaction{
		Datums: []TransactionDatum{
			{
				Hash:         Hash32{1},
				BodyCBOR:     Bytes{1},
				BodyVerified: true,
				Observations: []TransactionDatumObservation{{
					SourceKind: "witness",
				}},
			},
			{
				Hash:         Hash32{2},
				BodyCBOR:     Bytes{2},
				BodyVerified: true,
				Observations: []TransactionDatumObservation{{
					SourceKind: "witness",
				}},
			},
		},
	}
	if !valid.DatumContextValid() {
		t.Fatal("sorted grouped datum context rejected")
	}
	valid.Datums[0], valid.Datums[1] = valid.Datums[1], valid.Datums[0]
	if valid.DatumContextValid() {
		t.Fatal("unordered datum groups accepted")
	}
}

func TestOwnedTransactionChildJSONKeySets(t *testing.T) {
	t.Parallel()
	outputIndex := uint32(1)
	tests := []struct {
		name  string
		value any
		keys  []string
	}{
		{
			name: "withdrawal",
			value: Withdrawal{
				RewardAccount:  Bytes{1},
				RewardText:     "stake_test1",
				Lovelace:       2,
				BodyOrdinal:    3,
				Applied:        true,
				CredentialKind: "key",
				CredentialHash: Bytes{4},
			},
			keys: []string{
				"body_ordinal",
				"credential_hash",
				"credential_kind",
				"is_applied",
				"lovelace",
				"reward_account",
				"reward_text",
			},
		},
		{
			name:  "redeemer",
			value: Redeemer{},
			keys: []string{
				"data_cbor",
				"execution_memory",
				"execution_steps",
				"index",
				"is_applied",
				"purpose",
				"purpose_tag",
				"resolved_target",
			},
		},
		{
			name:  "metadata",
			value: TransactionMetadata{},
			keys: []string{
				"byte_length",
				"content_hash",
				"labels",
				"map_cbor",
			},
		},
		{
			name: "datum",
			value: TransactionDatum{
				Hash:         Hash32{1},
				BodyCBOR:     Bytes{2},
				BodyVerified: true,
				Observations: []TransactionDatumObservation{{
					SourceKind:    "inline_output",
					SourceOrdinal: 3,
					OutputIndex:   &outputIndex,
				}},
			},
			keys: []string{
				"body_cbor",
				"body_verified",
				"hash",
				"observations",
			},
		},
		{
			name: "datum observation",
			value: TransactionDatumObservation{
				SourceKind:    "inline_output",
				SourceOrdinal: 3,
				OutputIndex:   &outputIndex,
			},
			keys: []string{
				"output_index",
				"source_kind",
				"source_ordinal",
			},
		},
	}
	for _, test := range tests {
		encoded, err := json.Marshal(test.value)
		if err != nil {
			t.Fatalf("%s: %v", test.name, err)
		}
		var object map[string]json.RawMessage
		if err := json.Unmarshal(encoded, &object); err != nil {
			t.Fatalf("%s: %v", test.name, err)
		}
		keys := make([]string, 0, len(object))
		for key := range object {
			keys = append(keys, key)
		}
		slices.Sort(keys)
		if !slices.Equal(keys, test.keys) {
			t.Errorf("%s keys = %v, want %v", test.name, keys, test.keys)
		}
	}
}
