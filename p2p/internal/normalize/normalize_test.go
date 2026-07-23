package normalize

import (
	"encoding/hex"
	"encoding/json"
	"math"
	"math/big"
	"strings"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
)

type inlineWithoutAPIDatumHash struct {
	lcommon.TransactionOutput
}

func (inlineWithoutAPIDatumHash) DatumHash() *lcommon.Blake2b256 {
	return nil
}

func TestValidTransactionFactsAreUTxOOnlyAndDeterministic(t *testing.T) {
	datum := testDatum(t, []byte{0x01})
	inline := testInlineOutput(t, datum, testAssets(t), 123)
	input := ledger.NewShelleyTransactionInput(strings.Repeat("11", 32), 2)
	reference := ledger.NewShelleyTransactionInput(strings.Repeat("22", 32), 3)
	mint := testMint(t)
	tx := testConwayTransaction(t, true, txOptions{
		inputs:    []ledger.ShelleyTransactionInput{input},
		outputs:   []ledger.BabbageTransactionOutput{inline},
		reference: []ledger.ShelleyTransactionInput{reference},
		fee:       17,
		mint:      mint,
		witnessDatums: []lcommon.Datum{
			datum,
			datum,
		},
	})
	datums := datumCollector{}
	got, err := transactionFacts(tx, 4, datums)
	if err != nil {
		t.Fatal(err)
	}
	if !got.Valid || got.FlowKind != "regular" || len(got.Inputs) != 1 ||
		len(got.Outputs) != 1 || len(got.Mint) != 2 {
		t.Fatalf("unexpected valid facts: %#v", got)
	}
	if got.Inputs[0].SourceTxHash != input.Id().String() {
		t.Fatalf("wrong effective input: %#v", got.Inputs[0])
	}
	if got.Inputs[0].SourceTxHash == reference.Id().String() {
		t.Fatal("reference input was treated as consumed")
	}
	if got.FeeLovelace == nil || *got.FeeLovelace != "17" || !got.FeeKnown {
		t.Fatalf("wrong fee: %#v", got)
	}
	if got.Outputs[0].DatumKind != "inline" ||
		got.Outputs[0].DatumHash == nil ||
		*got.Outputs[0].DatumHash != datum.Hash().String() {
		t.Fatalf("wrong inline datum: %#v", got.Outputs[0])
	}
	if len(datums) != 1 {
		t.Fatalf("datum was not deduplicated: %#v", datums)
	}
	stored := datums[datum.Hash().String()]
	if stored.DatumCBORHex != "01" || len(stored.Sources) != 2 {
		t.Fatalf("wrong exact datum facts: %#v", stored)
	}
	if got.Outputs[0].Assets[0].AssetNameHex != "" ||
		len(got.Outputs[0].Assets[1].AssetNameHex) != 64 {
		t.Fatalf("empty/32-byte asset names not preserved: %#v", got.Outputs[0].Assets)
	}
}

func TestInvalidTransactionUsesOnlyCollateralAndCollateralReturn(t *testing.T) {
	regular := ledger.NewShelleyTransactionInput(strings.Repeat("11", 32), 1)
	collateral := ledger.NewShelleyTransactionInput(strings.Repeat("22", 32), 2)
	reference := ledger.NewShelleyTransactionInput(strings.Repeat("33", 32), 3)
	regularOutput := testOutput(t, nil, 100)
	collateralReturn := testOutput(t, nil, 40)
	tx := testConwayTransaction(t, false, txOptions{
		inputs:           []ledger.ShelleyTransactionInput{regular},
		outputs:          []ledger.BabbageTransactionOutput{regularOutput, regularOutput},
		collateral:       []ledger.ShelleyTransactionInput{collateral},
		collateralReturn: &collateralReturn,
		reference:        []ledger.ShelleyTransactionInput{reference},
		fee:              99,
		totalCollateral:  60,
		mint:             testMint(t),
	})
	got, err := transactionFacts(tx, 0, datumCollector{})
	if err != nil {
		t.Fatal(err)
	}
	if got.Valid || got.FlowKind != "collateral" || len(got.Inputs) != 1 ||
		got.Inputs[0].SourceTxHash != collateral.Id().String() {
		t.Fatalf("wrong effective collateral input: %#v", got)
	}
	if len(got.Outputs) != 1 || got.Outputs[0].OutputIndex != 2 ||
		got.Outputs[0].Kind != "collateral" {
		t.Fatalf("wrong collateral return index: %#v", got.Outputs)
	}
	if len(got.Mint) != 0 {
		t.Fatalf("invalid transaction retained mint: %#v", got.Mint)
	}
	if got.FeeLovelace == nil || *got.FeeLovelace != "60" || !got.FeeKnown {
		t.Fatalf("wrong collateral fee: %#v", got)
	}
	for _, input := range got.Inputs {
		if input.SourceTxHash == regular.Id().String() ||
			input.SourceTxHash == reference.Id().String() {
			t.Fatal("regular/reference input leaked into invalid effective flow")
		}
	}
}

func TestInlineDatumHashComesFromExactCBORWhenAPIReturnsNil(t *testing.T) {
	datum := testDatum(t, []byte{0x18, 0x2a})
	output := testInlineOutput(t, datum, nil, 1)
	wrapped := inlineWithoutAPIDatumHash{TransactionOutput: &output}
	datums := datumCollector{}
	got, err := outputFacts(strings.Repeat("ab", 32), 0, 0, "regular", wrapped, datums)
	if err != nil {
		t.Fatal(err)
	}
	if got.DatumHash == nil || *got.DatumHash != datum.Hash().String() {
		t.Fatalf("exact-CBOR datum hash not retained: %#v", got)
	}
	if datums[datum.Hash().String()].DatumCBORHex != "182a" {
		t.Fatalf("exact datum CBOR not retained: %#v", datums)
	}
}

func TestDatumHashGoldenVector(t *testing.T) {
	const expected = "ee155ace9c40292074cb6aff8c9ccdd273c81648ff1149ef36bcea6ebb8a3e25"
	datum := testDatum(t, []byte{0x01})
	got, err := verifiedDatum(&datum)
	if err != nil {
		t.Fatal(err)
	}
	if got.String() != expected {
		t.Fatalf("datum hash mismatch: got %s, want %s", got, expected)
	}
}

func TestHashOnlyDatumDoesNotInventBody(t *testing.T) {
	hash := lcommon.NewBlake2b256(bytesOf(0x55, 32))
	output := testHashOnlyOutput(t, hash, 1)
	datums := datumCollector{}
	got, err := outputFacts(strings.Repeat("ab", 32), 0, 0, "regular", &output, datums)
	if err != nil {
		t.Fatal(err)
	}
	if got.DatumKind != "hash" || got.DatumHash == nil || *got.DatumHash != hash.String() {
		t.Fatalf("wrong hash-only output: %#v", got)
	}
	if len(datums) != 0 {
		t.Fatalf("invented datum body for hash-only output: %#v", datums)
	}
}

func TestInvalidWithoutTotalCollateralHasUnknownFee(t *testing.T) {
	collateral := ledger.NewShelleyTransactionInput(strings.Repeat("22", 32), 2)
	tx := testConwayTransaction(t, false, txOptions{
		collateral: []ledger.ShelleyTransactionInput{collateral},
		fee:        99,
	})
	got, err := transactionFacts(tx, 0, datumCollector{})
	if err != nil {
		t.Fatal(err)
	}
	if got.FeeKnown || got.FeeLovelace != nil {
		t.Fatalf("invented invalid transaction fee: %#v", got)
	}
}

func TestAssetAndNumericBounds(t *testing.T) {
	var policy lcommon.Blake2b224
	tooLong := bytesOf(0xaa, 33)
	assets := lcommon.NewMultiAsset(map[lcommon.Blake2b224]map[cbor.ByteString]*big.Int{
		policy: {cbor.NewByteString(tooLong): big.NewInt(1)},
	})
	if _, err := outputAssets(&assets); err == nil || !strings.Contains(err.Error(), "asset name") {
		t.Fatalf("accepted oversized asset name: %v", err)
	}
	overflow := new(big.Int).Lsh(big.NewInt(1), 64)
	assets = lcommon.NewMultiAsset(map[lcommon.Blake2b224]map[cbor.ByteString]*big.Int{
		policy: {cbor.NewByteString(nil): overflow},
	})
	if _, err := outputAssets(&assets); err == nil || !strings.Contains(err.Error(), "UInt64") {
		t.Fatalf("accepted UInt64 overflow: %v", err)
	}
	mint := lcommon.NewMultiAsset(map[lcommon.Blake2b224]map[cbor.ByteString]*big.Int{
		policy: {cbor.NewByteString(nil): new(big.Int).Lsh(big.NewInt(1), 63)},
	})
	if _, err := mintFacts(&mint); err == nil || !strings.Contains(err.Error(), "Int64") {
		t.Fatalf("accepted Int64 overflow: %v", err)
	}
}

func TestTransactionIDMismatchFailsClosed(t *testing.T) {
	body := []byte{0xa0}
	expectedBytes, err := hex.DecodeString(
		"d36a2619a672494604e11bb447cbcf5231e9f2ba25c2169177edc941bd50ad6c",
	)
	if err != nil {
		t.Fatal(err)
	}
	expected := lcommon.NewBlake2b256(expectedBytes)
	if err := verifyTransactionID(expected, body); err != nil {
		t.Fatalf("literal transaction ID/body vector failed: %v", err)
	}
	wrong := lcommon.NewBlake2b256(bytesOf(0x42, 32))
	if err := verifyTransactionID(wrong, body); err == nil ||
		!strings.Contains(err.Error(), "transaction ID mismatch") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func testHashOnlyOutput(
	t *testing.T,
	hash lcommon.Blake2b256,
	lovelace uint64,
) ledger.BabbageTransactionOutput {
	t.Helper()
	raw, err := cbor.Encode(map[uint64]any{
		0: testAddress(t),
		1: ledger.MaryTransactionOutputValue{Amount: lovelace},
		2: []any{
			uint64(babbage.DatumOptionTypeHash),
			hash,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	output, err := ledger.NewBabbageTransactionOutputFromCbor(raw)
	if err != nil {
		t.Fatal(err)
	}
	return *output
}

func TestDijkstraNestedTransactionFailsClosed(t *testing.T) {
	tx := &ledger.DijkstraTransaction{
		Body: ledger.DijkstraTransactionBody{
			TxSubTransactions: cbor.NewSetType(
				[]dijkstra.DijkstraSubTransaction{{}},
				true,
			),
		},
	}
	if err := rejectUnsupportedTransaction(tx); err == nil ||
		!strings.Contains(err.Error(), "nested transaction semantics") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNormalizedJSONHasNoForbiddenBroadPayload(t *testing.T) {
	datum := testDatum(t, []byte{0x01})
	block := Block{
		Transactions: []Transaction{{TxHash: strings.Repeat("00", 32)}},
		Datums: []Datum{{
			DatumHash:    datum.Hash().String(),
			DatumCBORHex: "01",
		}},
	}
	data, err := json.Marshal(block)
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	for _, forbidden := range []string{
		`"tx_cbor"`,
		`"block_cbor"`,
		`"script_cbor"`,
		`"redeemer"`,
		`"metadata"`,
		`"governance"`,
	} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("forbidden payload %s in %s", forbidden, text)
		}
	}
	if !strings.Contains(text, `"datum_cbor_hex":"01"`) {
		t.Fatalf("exact datum CBOR missing: %s", text)
	}
}

type txOptions struct {
	inputs           []ledger.ShelleyTransactionInput
	outputs          []ledger.BabbageTransactionOutput
	collateral       []ledger.ShelleyTransactionInput
	collateralReturn *ledger.BabbageTransactionOutput
	reference        []ledger.ShelleyTransactionInput
	fee              uint64
	totalCollateral  uint64
	mint             *lcommon.MultiAsset[lcommon.MultiAssetTypeMint]
	witnessDatums    []lcommon.Datum
}

func testConwayTransaction(t *testing.T, valid bool, options txOptions) *ledger.ConwayTransaction {
	t.Helper()
	bodyTemplate := ledger.ConwayTransactionBody{
		TxInputs:           conway.NewConwayTransactionInputSet(options.inputs),
		TxOutputs:          options.outputs,
		TxFee:              options.fee,
		TxMint:             options.mint,
		TxCollateral:       cbor.NewSetType(options.collateral, true),
		TxCollateralReturn: options.collateralReturn,
		TxTotalCollateral:  options.totalCollateral,
		TxReferenceInputs:  cbor.NewSetType(options.reference, true),
	}
	bodyCBOR, err := cbor.Encode(&bodyTemplate)
	if err != nil {
		t.Fatal(err)
	}
	body, err := ledger.NewConwayTransactionBodyFromCbor(bodyCBOR)
	if err != nil {
		t.Fatal(err)
	}
	return &ledger.ConwayTransaction{
		Body:      *body,
		TxIsValid: valid,
		WitnessSet: ledger.ConwayTransactionWitnessSet{
			WsPlutusData: cbor.NewSetType(options.witnessDatums, true),
		},
	}
}

func testAddress(t *testing.T) lcommon.Address {
	t.Helper()
	raw := append([]byte{0x61}, bytesOf(0x11, 28)...)
	address, err := lcommon.NewAddressFromBytes(raw)
	if err != nil {
		t.Fatal(err)
	}
	return address
}

func testOutput(
	t *testing.T,
	assets *lcommon.MultiAsset[lcommon.MultiAssetTypeOutput],
	lovelace uint64,
) ledger.BabbageTransactionOutput {
	t.Helper()
	return ledger.BabbageTransactionOutput{
		OutputAddress: testAddress(t),
		OutputAmount: ledger.MaryTransactionOutputValue{
			Amount: lovelace,
			Assets: assets,
		},
	}
}

func testInlineOutput(
	t *testing.T,
	datum lcommon.Datum,
	assets *lcommon.MultiAsset[lcommon.MultiAssetTypeOutput],
	lovelace uint64,
) ledger.BabbageTransactionOutput {
	t.Helper()
	raw, err := cbor.Encode(map[uint64]any{
		0: testAddress(t),
		1: ledger.MaryTransactionOutputValue{
			Amount: lovelace,
			Assets: assets,
		},
		2: []any{
			uint64(babbage.DatumOptionTypeData),
			cbor.Tag{Number: 24, Content: datum.Cbor()},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	output, err := ledger.NewBabbageTransactionOutputFromCbor(raw)
	if err != nil {
		t.Fatal(err)
	}
	return *output
}

func testDatum(t *testing.T, raw []byte) lcommon.Datum {
	t.Helper()
	var datum lcommon.Datum
	if err := datum.UnmarshalCBOR(raw); err != nil {
		t.Fatal(err)
	}
	return datum
}

func testAssets(t *testing.T) *lcommon.MultiAsset[lcommon.MultiAssetTypeOutput] {
	t.Helper()
	var policy lcommon.Blake2b224
	copy(policy[:], bytesOf(0x10, len(policy)))
	assets := lcommon.NewMultiAsset(map[lcommon.Blake2b224]map[cbor.ByteString]*big.Int{
		policy: {
			cbor.NewByteString(nil):               new(big.Int).SetUint64(math.MaxUint64),
			cbor.NewByteString(bytesOf(0x22, 32)): big.NewInt(7),
			cbor.NewByteString([]byte{0x01}):      big.NewInt(0),
		},
	})
	return &assets
}

func testMint(t *testing.T) *lcommon.MultiAsset[lcommon.MultiAssetTypeMint] {
	t.Helper()
	var policy lcommon.Blake2b224
	copy(policy[:], bytesOf(0x20, len(policy)))
	mint := lcommon.NewMultiAsset(map[lcommon.Blake2b224]map[cbor.ByteString]*big.Int{
		policy: {
			cbor.NewByteString(nil):          big.NewInt(math.MinInt64),
			cbor.NewByteString([]byte{0x01}): big.NewInt(math.MaxInt64),
			cbor.NewByteString([]byte{0x02}): big.NewInt(0),
		},
	})
	return &mint
}

func bytesOf(value byte, count int) []byte {
	ret := make([]byte, count)
	for idx := range ret {
		ret[idx] = value
	}
	return ret
}
