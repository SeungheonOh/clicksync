package normalize

import (
	"bytes"
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

func TestNativeTransactionBundleRetainsAllInputRoles(t *testing.T) {
	regular := ledger.NewShelleyTransactionInput(strings.Repeat("11", 32), 1)
	collateral := ledger.NewShelleyTransactionInput(strings.Repeat("22", 32), 2)
	reference := ledger.NewShelleyTransactionInput(strings.Repeat("33", 32), 3)
	datum := testDatum(t, []byte{0x01})
	output := testInlineOutput(t, datum, testAssets(), 123)
	output.TxOutScriptRef = &lcommon.ScriptRef{
		Type:   lcommon.ScriptRefTypePlutusV2,
		Script: lcommon.PlutusV2Script{0x01, 0x02},
	}
	tx := testConwayTransaction(t, true, txOptions{
		inputs:        []ledger.ShelleyTransactionInput{regular},
		outputs:       []ledger.BabbageTransactionOutput{output},
		collateral:    []ledger.ShelleyTransactionInput{collateral},
		reference:     []ledger.ShelleyTransactionInput{reference},
		fee:           17,
		mint:          testMint(),
		witnessDatums: []lcommon.Datum{datum},
	})
	datums := bundleDatumCollector{}
	got, err := transactionBundle(tx, 4, "Conway", datums)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Inputs) != 3 ||
		got.Inputs[0].Role != "regular" || !got.Inputs[0].Consumed ||
		got.Inputs[1].Role != "collateral" || got.Inputs[1].Consumed ||
		got.Inputs[2].Role != "reference" || got.Inputs[2].Consumed {
		t.Fatalf("input roles = %#v", got.Inputs)
	}
	if got.DeclaredFee == nil || *got.DeclaredFee != 17 ||
		got.EffectiveFee == nil || *got.EffectiveFee != 17 {
		t.Fatalf("fees = declared %v effective %v", got.DeclaredFee, got.EffectiveFee)
	}
	if len(got.Outputs) != 1 || got.Outputs[0].DatumKind != "inline" ||
		got.Outputs[0].ReferenceScriptHash == nil ||
		got.Outputs[0].ReferenceScriptLanguage == nil ||
		*got.Outputs[0].ReferenceScriptLanguage != "plutus_v2" {
		t.Fatalf("output = %#v", got.Outputs)
	}
	if len(got.DatumObservations) != 2 {
		t.Fatalf("datum observations = %#v", got.DatumObservations)
	}
	for hash, body := range datums {
		if len(body.Observations) != 2 {
			t.Fatalf("body observations = %#v", body.Observations)
		}
		for _, observation := range body.Observations {
			if observation.Hash != hash {
				t.Fatalf("body observation hash = %x, body = %x", observation.Hash, hash)
			}
		}
	}
	if len(got.Mint) != 2 || !got.MintApplied {
		t.Fatalf("mint = %#v, applied=%v", got.Mint, got.MintApplied)
	}
}

func TestInvalidBundleRetainsContextButOnlyCollateralFlow(t *testing.T) {
	regular := ledger.NewShelleyTransactionInput(strings.Repeat("11", 32), 1)
	collateral := ledger.NewShelleyTransactionInput(strings.Repeat("22", 32), 2)
	reference := ledger.NewShelleyTransactionInput(strings.Repeat("33", 32), 3)
	returnOutput := testOutput(nil, 40)
	tx := testConwayTransaction(t, false, txOptions{
		inputs:           []ledger.ShelleyTransactionInput{regular},
		outputs:          []ledger.BabbageTransactionOutput{testOutput(nil, 100)},
		collateral:       []ledger.ShelleyTransactionInput{collateral},
		reference:        []ledger.ShelleyTransactionInput{reference},
		collateralReturn: &returnOutput,
		totalCollateral:  60,
		fee:              99,
		mint:             testMint(),
	})
	got, err := transactionBundle(tx, 0, "Conway", bundleDatumCollector{})
	if err != nil {
		t.Fatal(err)
	}
	if got.Phase2Valid || got.FlowKind != "collateral" || got.MintApplied {
		t.Fatalf("invalid context = %#v", got)
	}
	if len(got.Inputs) != 3 || got.Inputs[0].Consumed || !got.Inputs[1].Consumed || got.Inputs[2].Consumed {
		t.Fatalf("invalid input effects = %#v", got.Inputs)
	}
	if len(got.Outputs) != 1 || got.Outputs[0].Kind != "collateral_return" || got.Outputs[0].Index != 1 {
		t.Fatalf("invalid outputs = %#v", got.Outputs)
	}
	if got.DeclaredFee == nil || *got.DeclaredFee != 99 ||
		got.EffectiveFee == nil || *got.EffectiveFee != 60 {
		t.Fatalf("fees = %v/%v", got.DeclaredFee, got.EffectiveFee)
	}
	if len(got.Mint) != 2 {
		t.Fatalf("unapplied mint context missing: %#v", got.Mint)
	}
}

func TestWithdrawalRedeemerAndMetadataAreExactAndResolved(t *testing.T) {
	input := ledger.NewShelleyTransactionInput(strings.Repeat("11", 32), 7)
	reward := testRewardAccount(t, true, 0x44)
	datum := testDatum(t, []byte{0x18, 0x2a})
	redeemers := conway.ConwayRedeemers{Redeemers: map[lcommon.RedeemerKey]lcommon.RedeemerValue{
		{Tag: lcommon.RedeemerTagReward, Index: 0}: {
			Data: datum, ExUnits: lcommon.ExUnits{Memory: 2, Steps: 3},
		},
		{Tag: lcommon.RedeemerTagSpend, Index: 0}: {
			Data: datum, ExUnits: lcommon.ExUnits{Memory: 4, Steps: 5},
		},
		{Tag: lcommon.RedeemerTagMint, Index: 0}: {
			Data: datum, ExUnits: lcommon.ExUnits{Memory: 6, Steps: 7},
		},
	}}
	tx := testConwayTransaction(t, true, txOptions{
		inputs:      []ledger.ShelleyTransactionInput{input},
		outputs:     []ledger.BabbageTransactionOutput{testOutput(nil, 1)},
		fee:         1,
		mint:        testMint(),
		withdrawals: map[*lcommon.Address]uint64{reward: 123},
		redeemers:   redeemers,
	})
	const metadataHex = "\xa2\x18\x2a\x61x\x01\x81\x02"
	metadata, err := lcommon.DecodeAuxiliaryDataToMetadata([]byte(metadataHex))
	if err != nil {
		t.Fatal(err)
	}
	tx.TxMetadata = metadata
	got, err := transactionBundle(tx, 0, "Conway", bundleDatumCollector{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Withdrawals) != 1 || !got.Withdrawals[0].Applied ||
		got.Withdrawals[0].CredentialKind != "script" ||
		!bytes.Equal(got.Withdrawals[0].RewardAccount, mustAddressBytes(t, reward)) {
		t.Fatalf("withdrawals = %#v", got.Withdrawals)
	}
	if len(got.Redeemers) != 3 ||
		got.Redeemers[0].Purpose != "spend" ||
		got.Redeemers[1].Purpose != "mint" ||
		got.Redeemers[2].Purpose != "reward" ||
		got.Redeemers[0].TargetTxHash == nil ||
		got.Redeemers[1].TargetPolicyID == nil ||
		len(got.Redeemers[2].TargetRewardAccount) == 0 {
		t.Fatalf("redeemers = %#v", got.Redeemers)
	}
	if got.Metadata == nil ||
		!bytes.Equal(got.Metadata.CBOR, []byte(metadataHex)) ||
		len(got.Metadata.Labels) != 2 ||
		got.Metadata.Labels[0] != 1 ||
		got.Metadata.Labels[1] != 42 {
		t.Fatalf("metadata = %#v", got.Metadata)
	}
}

func TestDijkstraNestedTransactionFailsBeforeNormalization(t *testing.T) {
	tx := &ledger.DijkstraTransaction{
		Body: ledger.DijkstraTransactionBody{
			TxSubTransactions: cbor.NewSetType([]dijkstra.DijkstraSubTransaction{{}}, true),
		},
	}
	if err := rejectUnsupportedTransaction(tx); err == nil ||
		!strings.Contains(err.Error(), "nested transaction semantics") {
		t.Fatalf("error = %v", err)
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
	withdrawals      map[*lcommon.Address]uint64
	redeemers        conway.ConwayRedeemers
}

func testConwayTransaction(t *testing.T, valid bool, options txOptions) *ledger.ConwayTransaction {
	t.Helper()
	template := ledger.ConwayTransactionBody{
		TxInputs:           conway.NewConwayTransactionInputSet(options.inputs),
		TxOutputs:          options.outputs,
		TxFee:              options.fee,
		TxMint:             options.mint,
		TxCollateral:       cbor.NewSetType(options.collateral, true),
		TxCollateralReturn: options.collateralReturn,
		TxTotalCollateral:  options.totalCollateral,
		TxReferenceInputs:  cbor.NewSetType(options.reference, true),
		TxWithdrawals:      options.withdrawals,
	}
	raw, err := cbor.Encode(&template)
	if err != nil {
		t.Fatal(err)
	}
	body, err := ledger.NewConwayTransactionBodyFromCbor(raw)
	if err != nil {
		t.Fatal(err)
	}
	return &ledger.ConwayTransaction{
		Body:      *body,
		TxIsValid: valid,
		WitnessSet: ledger.ConwayTransactionWitnessSet{
			WsPlutusData: cbor.NewSetType(options.witnessDatums, true),
			WsRedeemers:  options.redeemers,
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

func testRewardAccount(t *testing.T, script bool, fill byte) *lcommon.Address {
	t.Helper()
	header := byte(0xe1)
	if script {
		header = 0xf1
	}
	address, err := lcommon.NewAddressFromBytes(append([]byte{header}, bytesOf(fill, 28)...))
	if err != nil {
		t.Fatal(err)
	}
	return &address
}

func mustAddressBytes(t *testing.T, address *lcommon.Address) []byte {
	t.Helper()
	raw, err := address.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func testOutput(
	assets *lcommon.MultiAsset[lcommon.MultiAssetTypeOutput],
	lovelace uint64,
) ledger.BabbageTransactionOutput {
	return ledger.BabbageTransactionOutput{
		OutputAddress: testAddressNoFail(),
		OutputAmount:  ledger.MaryTransactionOutputValue{Amount: lovelace, Assets: assets},
	}
}

func testAddressNoFail() lcommon.Address {
	address, err := lcommon.NewAddressFromBytes(append([]byte{0x61}, bytesOf(0x11, 28)...))
	if err != nil {
		panic(err)
	}
	return address
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
		1: ledger.MaryTransactionOutputValue{Amount: lovelace, Assets: assets},
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

func testAssets() *lcommon.MultiAsset[lcommon.MultiAssetTypeOutput] {
	var policy lcommon.Blake2b224
	copy(policy[:], bytesOf(0x10, len(policy)))
	assets := lcommon.NewMultiAsset(map[lcommon.Blake2b224]map[cbor.ByteString]*big.Int{
		policy: {
			cbor.NewByteString(nil):               new(big.Int).SetUint64(math.MaxUint64),
			cbor.NewByteString(bytesOf(0x22, 32)): big.NewInt(7),
		},
	})
	return &assets
}

func testMint() *lcommon.MultiAsset[lcommon.MultiAssetTypeMint] {
	var policy lcommon.Blake2b224
	copy(policy[:], bytesOf(0x20, len(policy)))
	mint := lcommon.NewMultiAsset(map[lcommon.Blake2b224]map[cbor.ByteString]*big.Int{
		policy: {
			cbor.NewByteString(nil):          big.NewInt(math.MinInt64),
			cbor.NewByteString([]byte{0x01}): big.NewInt(math.MaxInt64),
		},
	})
	return &mint
}

func bytesOf(value byte, count int) []byte {
	return bytes.Repeat([]byte{value}, count)
}
