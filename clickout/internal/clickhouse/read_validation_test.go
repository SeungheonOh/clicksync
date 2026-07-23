package clickhouse

import (
	"testing"

	"github.com/clicksync-project/clickout/internal/model"
)

func TestOutputReadValidationFailsClosed(t *testing.T) {
	t.Parallel()
	base := outputValues{
		txHash:      make([]byte, 32),
		blockHash:   make([]byte, 32),
		kind:        "regular",
		paymentKind: "none",
		datumKind:   "none",
	}
	tests := []struct {
		name   string
		mutate func(*outputValues)
	}{
		{
			name: "missing payment hash",
			mutate: func(value *outputValues) {
				value.paymentKind = "script"
			},
		},
		{
			name: "unexpected payment hash",
			mutate: func(value *outputValues) {
				hash := string(make([]byte, 28))
				value.paymentHash = &hash
			},
		},
		{
			name: "missing inline datum hash",
			mutate: func(value *outputValues) {
				value.datumKind = "inline"
			},
		},
		{
			name: "reference script pair mismatch",
			mutate: func(value *outputValues) {
				hash := string(make([]byte, 28))
				value.referenceScriptHash = &hash
			},
		},
		{
			name: "zero asset",
			mutate: func(value *outputValues) {
				value.policies = []string{string(make([]byte, 28))}
				value.names = []string{"asset"}
				value.quantities = []uint64{0}
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			value := base
			test.mutate(&value)
			if _, err := makeOutput(value); err == nil {
				t.Fatal("malformed output was accepted")
			}
		})
	}
}

func TestRewardAccountReadValidation(t *testing.T) {
	t.Parallel()
	hash := make([]byte, 28)
	account := append([]byte{0xe1}, hash...)
	if err := validateRewardAccount(account, "key", hash); err != nil {
		t.Fatal(err)
	}
	if err := validateRewardAccount(account, "script", hash); err == nil {
		t.Fatal("key reward account was accepted as script")
	}
	testnet := append([]byte(nil), account...)
	testnet[0] = 0xe0
	if err := validateRewardAccount(testnet, "key", hash); err == nil {
		t.Fatal("testnet reward account was accepted in the mainnet dataset")
	}
}

func TestCompactRedeemerIdentityReadValidation(t *testing.T) {
	t.Parallel()
	ordinal := uint32(0)
	valid := model.Redeemer{
		PurposeTag: 2,
		Purpose:    "certificate",
		Target: model.ResolvedTarget{
			Status:            "resolved",
			BodyOrdinal:       &ordinal,
			ProcedureIdentity: append([]byte{2, 18}, make([]byte, 32)...),
		},
	}
	if err := validateRedeemerTarget(valid); err != nil {
		t.Fatal(err)
	}
	valid.Target.ProcedureIdentity[0] = 5
	if err := validateRedeemerTarget(valid); err == nil {
		t.Fatal("purpose-mismatched compact target identity was accepted")
	}
}
