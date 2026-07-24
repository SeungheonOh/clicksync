package clickhouse

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"hash/crc32"
	"testing"

	"github.com/clicksync-project/clickout/internal/model"
)

func testByronAddressWithAttributes(attributes []byte) []byte {
	payload := append([]byte{0x83, 0x58, 0x1c}, make([]byte, 28)...)
	payload = append(payload, attributes...)
	payload = append(payload, 0x00)
	address := []byte{0x82, 0xd8, 0x18, 0x58, byte(len(payload))}
	address = append(address, payload...)
	address = append(address, 0x1a, 0, 0, 0, 0)
	binary.BigEndian.PutUint32(
		address[len(address)-4:],
		crc32.ChecksumIEEE(payload),
	)
	return address
}

func testByronAddress() []byte {
	return testByronAddressWithAttributes([]byte{0xa0})
}

func TestOutputReadValidationFailsClosed(t *testing.T) {
	t.Parallel()
	base := outputValues{
		txHash:      make([]byte, 32),
		blockHash:   make([]byte, 32),
		kind:        "regular",
		address:     testByronAddress(),
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
		{
			name: "oversized asset name",
			mutate: func(value *outputValues) {
				value.policies = []string{string(make([]byte, 28))}
				value.names = []string{string(make([]byte, 33))}
				value.quantities = []uint64{1}
			},
		},
		{
			name: "unsupported reference language",
			mutate: func(value *outputValues) {
				hash := string(make([]byte, 28))
				language := "plutus_v5"
				value.referenceScriptHash = &hash
				value.referenceLanguage = &language
			},
		},
		{
			name: "oversized address",
			mutate: func(value *outputValues) {
				value.address = make([]byte, 257)
				value.address[0] = 0x82
			},
		},
		{
			name: "fake Byron address",
			mutate: func(value *outputValues) {
				value.address = []byte{0x82, 0x01}
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
		{"base trailing", append(append(append([]byte{0x01}, hash...), baseStake...), 0xaa), "key", &asString, true},
		{"enterprise key", append([]byte{0x61}, hash...), "key", &asString, true},
		{"enterprise script", append([]byte{0x71}, hash...), "script", &asString, true},
		{"enterprise trailing", append(append([]byte{0x61}, hash...), 0xaa), "key", &asString, true},
		{"pointer key", append(append([]byte{0x41}, hash...), 0, 0, 0), "key", &asString, true},
		{"pointer trailing", append(append([]byte{0x41}, hash...), 0, 0, 0, 0xaa), "key", &asString, true},
		{"pointer noncanonical", append(append([]byte{0x41}, hash...), 0x80, 0, 0, 0), "key", &asString, true},
		{"Byron none", testByronAddress(), "none", nil, true},
		{"wrong network", append([]byte{0x60}, hash...), "key", &asString, false},
		{"reward output", append([]byte{0xe1}, hash...), "key", &asString, false},
		{"pointer truncated", append(append([]byte{0x41}, hash...), 0, 0), "key", &asString, false},
		{"base truncated", append(append([]byte{0x01}, hash...), baseStake[:27]...), "key", &asString, false},
		{"enterprise truncated", append([]byte{0x61}, hash[:27]...), "key", &asString, false},
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

func TestHistoricalTrailingAddressReadValidationIsEraAware(t *testing.T) {
	t.Parallel()
	raw, err := hex.DecodeString(
		"015bad085057ac10ecc643450a2031ae566ff63b395153cea2d023ba67" +
			"0e3a8d3f188fd573eca848a2380eb6d57cf698be9eb750d14816f5e1" +
			"13d5f4a3fe0478b2241e0168e3cba5001a22c15a11",
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) != 78 {
		t.Fatalf("fixture address has %d bytes, want 78", len(raw))
	}
	credential, err := hex.DecodeString(
		"5bad085057ac10ecc643450a2031ae566ff63b395153cea2d023ba67",
	)
	if err != nil {
		t.Fatal(err)
	}
	credentialString := string(credential)
	output, err := makeOutput(outputValues{
		txHash:      make([]byte, 32),
		blockHash:   make([]byte, 32),
		kind:        "regular",
		address:     raw,
		paymentKind: "key",
		paymentHash: &credentialString,
		datumKind:   "none",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(output.Address, raw) ||
		!bytes.Equal(output.PaymentCredentialHash, credential) {
		t.Fatalf(
			"output address/credential = %x / %x",
			output.Address,
			output.PaymentCredentialHash,
		)
	}
	if err := validateOutputEraCapabilities(output, "Alonzo", false); err != nil {
		t.Fatal(err)
	}
	if err := validateOutputEraCapabilities(output, "Babbage", false); !errors.Is(err, ErrConflictingRow) {
		t.Fatalf("Babbage validation error = %v", err)
	}
}

func TestPointerAddressCanonicalBounds(t *testing.T) {
	t.Parallel()
	valid := [][]byte{
		{0x81, 0x00, 0, 0},
		{0x81, 0x00, 0x83, 0xff, 0x7f, 1},
		{0x8f, 0xff, 0xff, 0xff, 0x7f, 0, 0},
	}
	for _, suffix := range valid {
		if err := validatePointerAddressSuffix(suffix, false); err != nil {
			t.Fatalf("valid pointer suffix %x rejected: %v", suffix, err)
		}
	}
	invalid := [][]byte{
		{0x80, 0, 0, 0},
		{0x82, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0, 0, 0},
		{0, 0x84, 0x80, 0, 0},
	}
	for _, suffix := range invalid {
		if err := validatePointerAddressSuffix(suffix, false); err == nil {
			t.Fatalf("invalid pointer suffix %x accepted", suffix)
		}
	}
}

func TestByronAddressKnownAttributesFailClosed(t *testing.T) {
	t.Parallel()
	for name, attributes := range map[string][]byte{
		"derivation payload is not bytes": {0xa1, 0x01, 0x00},
		"network payload is not bytes":    {0xa1, 0x02, 0x00},
		"network bytes are not uint32":    {0xa1, 0x02, 0x41, 0xff},
		"duplicate attribute":             {0xa2, 0x01, 0x40, 0x01, 0x40},
		"unknown attribute":               {0xa1, 0x03, 0x40},
		"empty derivation payload":        {0xa1, 0x01, 0x40},
	} {
		attributes := attributes
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if err := validateByronAddress(
				testByronAddressWithAttributes(attributes),
			); err == nil {
				t.Fatal("malformed Byron address was accepted")
			}
		})
	}
	for name, encoded := range map[string]string{
		"nonminimal address type": "82d818582583581c5d5e698eba3dd9452add99a1af9461beb0ba61b8bece26e7399878dda102410218001a7b9bf95c",
		"unsorted attributes":     "82d818582783581c5d5e698eba3dd9452add99a1af9461beb0ba61b8bece26e7399878dda20241020141aa001a15989de6",
	} {
		encoded := encoded
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			address, err := hex.DecodeString(encoded)
			if err != nil {
				t.Fatal(err)
			}
			if err := validateByronAddress(address); err == nil {
				t.Fatal("noncanonical Byron address was accepted")
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
