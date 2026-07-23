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
		address:     []byte{0x82, 0x01},
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

func TestOutputAddressPaymentCredentialValidation(t *testing.T) {
	t.Parallel()
	hash := make([]byte, 28)
	for index := range hash {
		hash[index] = 0x44
	}
	asString := string(hash)
	baseStake := make([]byte, 28)
	tests := []struct {
		name    string
		address []byte
		kind    string
		hash    *string
		valid   bool
	}{
		{"base key", append(append([]byte{0x01}, hash...), baseStake...), "key", &asString, true},
		{"base script", append(append([]byte{0x11}, hash...), baseStake...), "script", &asString, true},
		{"enterprise key", append([]byte{0x61}, hash...), "key", &asString, true},
		{"enterprise script", append([]byte{0x71}, hash...), "script", &asString, true},
		{"pointer key", append(append([]byte{0x41}, hash...), 0, 0, 0), "key", &asString, true},
		{"Byron none", []byte{0x82, 0x01}, "none", nil, true},
		{"wrong network", append([]byte{0x60}, hash...), "key", &asString, false},
		{"reward output", append([]byte{0xe1}, hash...), "key", &asString, false},
		{"pointer truncated", append(append([]byte{0x41}, hash...), 0, 0), "key", &asString, false},
		{"pointer trailing", append(append([]byte{0x41}, hash...), 0, 0, 0, 0), "key", &asString, false},
		{"pointer noncanonical", append(append([]byte{0x41}, hash...), 0x80, 0, 0, 0), "key", &asString, false},
		{"credential kind mismatch", append([]byte{0x61}, hash...), "script", &asString, false},
		{"empty credential", append([]byte{0x61}, hash...), "none", nil, false},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			value := outputValues{
				txHash:      make([]byte, 32),
				blockHash:   make([]byte, 32),
				kind:        "regular",
				address:     test.address,
				paymentKind: test.kind,
				paymentHash: test.hash,
				datumKind:   "none",
			}
			_, err := makeOutput(value)
			if test.valid && err != nil {
				t.Fatal(err)
			}
			if !test.valid && err == nil {
				t.Fatal("corrupt address/credential row was accepted")
			}
		})
	}
}

func TestPointerAddressCanonicalBounds(t *testing.T) {
	t.Parallel()
	valid := [][]byte{
		{0x81, 0x00, 0, 0},
		{0x81, 0x00, 0x83, 0xff, 0x7f, 1},
		{0x81, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f, 0, 0},
	}
	for _, suffix := range valid {
		if err := validatePointerAddressSuffix(suffix); err != nil {
			t.Fatalf("valid pointer suffix %x rejected: %v", suffix, err)
		}
	}
	invalid := [][]byte{
		{0x80, 0, 0, 0},
		{0x82, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0, 0, 0},
		{0, 0x84, 0x80, 0, 0},
	}
	for _, suffix := range invalid {
		if err := validatePointerAddressSuffix(suffix); err == nil {
			t.Fatalf("invalid pointer suffix %x accepted", suffix)
		}
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
