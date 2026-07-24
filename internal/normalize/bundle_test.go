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
)

func TestTransactionBundleProjectsEffectivePhase2Flow(t *testing.T) {
	regular := ledger.NewShelleyTransactionInput(strings.Repeat("11", 32), 1)
	collateral := ledger.NewShelleyTransactionInput(strings.Repeat("22", 32), 2)
	reference := ledger.NewShelleyTransactionInput(strings.Repeat("33", 32), 3)
	datum := testDatum(t, []byte{0x01})
	output := testInlineOutput(t, datum, testAssets(), 123)
	output.TxOutScriptRef = &lcommon.ScriptRef{
		Type:   lcommon.ScriptRefTypePlutusV2,
		Script: lcommon.PlutusV2Script{0x01, 0x02},
	}

	t.Run("valid", func(t *testing.T) {
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
		if !got.Phase2Valid || got.FlowKind != "regular" || !got.MintApplied {
			t.Fatalf("valid flow context = %#v", got)
		}
		if len(got.Inputs) != 3 ||
			!got.Inputs[0].Consumed ||
			got.Inputs[1].Consumed ||
			got.Inputs[2].Consumed {
			t.Fatalf("valid input projection = %#v", got.Inputs)
		}
		if len(got.Outputs) != 1 ||
			got.Outputs[0].Kind != "regular" ||
			got.Outputs[0].DatumKind != "inline" ||
			got.Outputs[0].ReferenceScriptHash == nil ||
			got.Outputs[0].ReferenceScriptLanguage == nil ||
			*got.Outputs[0].ReferenceScriptLanguage != "plutus_v2" {
			t.Fatalf("valid output projection = %#v", got.Outputs)
		}
		if len(got.Mint) != 2 ||
			got.DeclaredFee == nil || *got.DeclaredFee != 17 ||
			got.EffectiveFee == nil || *got.EffectiveFee != 17 {
			t.Fatalf("valid value projection = %#v", got)
		}
		if len(got.DatumObservations) != 2 || len(datums) != 1 {
			t.Fatalf("datum projection = %#v / %#v", got.DatumObservations, datums)
		}
	})

	t.Run("phase-2 invalid", func(t *testing.T) {
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
			t.Fatalf("invalid flow context = %#v", got)
		}
		if len(got.Inputs) != 3 ||
			got.Inputs[0].Consumed ||
			!got.Inputs[1].Consumed ||
			got.Inputs[2].Consumed {
			t.Fatalf("invalid input projection = %#v", got.Inputs)
		}
		if len(got.Outputs) != 1 ||
			got.Outputs[0].Kind != "collateral_return" ||
			got.Outputs[0].Index != 1 {
			t.Fatalf("invalid output projection = %#v", got.Outputs)
		}
		if got.DeclaredFee == nil || *got.DeclaredFee != 99 ||
			got.EffectiveFee == nil || *got.EffectiveFee != 60 ||
			len(got.Mint) != 2 {
			t.Fatalf("invalid value context = %#v", got)
		}
	})
}

func TestDuplicateAndOverlappingInputReferencesRemainObservations(t *testing.T) {
	shared := ledger.NewShelleyTransactionInput(strings.Repeat("11", 32), 7)
	tx := testConwayTransaction(t, true, txOptions{
		inputs:    []ledger.ShelleyTransactionInput{shared, shared},
		reference: []ledger.ShelleyTransactionInput{shared},
		fee:       1,
	})
	got, err := transactionBundle(tx, 0, "Conway", bundleDatumCollector{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Inputs) != 3 {
		t.Fatalf("input observations = %#v", got.Inputs)
	}
	for ordinal := range 2 {
		if got.Inputs[ordinal].Role != "regular" ||
			got.Inputs[ordinal].BodyOrdinal != uint32(ordinal) ||
			!got.Inputs[ordinal].Consumed {
			t.Fatalf("regular observation %d = %#v", ordinal, got.Inputs[ordinal])
		}
	}
	if got.Inputs[2].Role != "reference" || got.Inputs[2].Consumed {
		t.Fatalf("reference observation = %#v", got.Inputs[2])
	}
}

func TestOptionalAddressEnrichmentNeverRejectsRawObservation(t *testing.T) {
	output := ledger.BabbageTransactionOutput{
		OutputAddress: lcommon.Address{},
		OutputAmount:  ledger.MaryTransactionOutputValue{Amount: 1},
	}
	got, _, err := outputBundle(
		[32]byte{1},
		0,
		0,
		0,
		"regular",
		"Conway",
		output,
		bundleDatumCollector{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.Address, []byte{0}) {
		t.Fatalf("raw address = %x, want 00", got.Address)
	}
	if got.PaymentCredentialKind != nil || got.PaymentCredentialHash != nil {
		t.Fatalf(
			"unavailable payment enrichment = %v/%v",
			got.PaymentCredentialKind,
			got.PaymentCredentialHash,
		)
	}

	reward := &lcommon.Address{}
	tx := &ledger.ConwayTransaction{
		Body: ledger.ConwayTransactionBody{
			TxWithdrawals: map[*lcommon.Address]uint64{reward: 2},
		},
		TxIsValid: true,
	}
	withdrawals, err := withdrawalBundle(tx, [32]byte{2}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(withdrawals) != 1 ||
		!bytes.Equal(withdrawals[0].RewardAccount, []byte{0}) ||
		withdrawals[0].CredentialKind != nil ||
		withdrawals[0].CredentialHash != nil {
		t.Fatalf("raw withdrawal observation = %#v", withdrawals)
	}
}

func TestHashOnlyDatumObservationIsProjectedWithoutBody(t *testing.T) {
	var wantHash lcommon.Blake2b256
	copy(wantHash[:], bytes.Repeat([]byte{0x77}, len(wantHash)))
	raw, err := cbor.Encode(map[uint64]any{
		0: testAddress(t),
		1: ledger.MaryTransactionOutputValue{Amount: 1},
		2: []any{
			uint64(babbage.DatumOptionTypeHash),
			wantHash.Bytes(),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	output, err := ledger.NewBabbageTransactionOutputFromCbor(raw)
	if err != nil {
		t.Fatal(err)
	}
	got, observations, err := outputBundle(
		[32]byte{1},
		0,
		0,
		0,
		"regular",
		"Babbage",
		output,
		bundleDatumCollector{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if got.DatumKind != "hash" ||
		got.DatumHash == nil ||
		!bytes.Equal(got.DatumHash[:], wantHash.Bytes()) ||
		len(observations) != 0 {
		t.Fatalf("hash-only datum projection = %#v / %#v", got, observations)
	}
}

func TestAllRedeemerPurposesAndContextCBOR(t *testing.T) {
	input := ledger.NewShelleyTransactionInput(strings.Repeat("11", 32), 7)
	reward := testRewardAccount(t, true, 0x44)
	datum := testDatum(t, []byte{0x18, 0x2a})
	scriptCredential := lcommon.Credential{
		CredType:   lcommon.CredentialTypeScriptHash,
		Credential: lcommon.NewBlake2b224(bytes.Repeat([]byte{0x41}, 28)),
	}
	certificate := &lcommon.StakeRegistrationCertificate{
		CertType:        uint(lcommon.CertificateTypeStakeRegistration),
		StakeCredential: scriptCredential,
	}
	voter := &lcommon.Voter{
		Type: lcommon.VoterTypeDRepScriptHash,
	}
	copy(voter.Hash[:], bytes.Repeat([]byte{0x51}, len(voter.Hash)))
	action := &lcommon.InfoGovAction{Type: uint(lcommon.GovActionTypeInfo)}
	proposal := conway.ConwayProposalProcedure{
		PPDeposit:       1,
		PPRewardAccount: *testRewardAccount(t, false, 0x42),
		PPGovAction: conway.ConwayGovAction{
			Type:   uint(lcommon.GovActionTypeInfo),
			Action: action,
		},
	}
	redeemers := make(map[lcommon.RedeemerKey]lcommon.RedeemerValue)
	for tag := lcommon.RedeemerTagSpend; tag <= lcommon.RedeemerTagProposing; tag++ {
		redeemers[lcommon.RedeemerKey{Tag: tag, Index: 0}] = lcommon.RedeemerValue{
			Data:    datum,
			ExUnits: lcommon.ExUnits{Memory: int64(tag) + 1, Steps: int64(tag) + 2},
		}
	}
	tx := testConwayTransaction(t, true, txOptions{
		inputs:      []ledger.ShelleyTransactionInput{input},
		outputs:     []ledger.BabbageTransactionOutput{testOutput(nil, 1)},
		fee:         1,
		mint:        testMint(),
		withdrawals: map[*lcommon.Address]uint64{reward: 123},
		certificates: []lcommon.CertificateWrapper{{
			Type:        uint(lcommon.CertificateTypeStakeRegistration),
			Certificate: certificate,
		}},
		voting:    lcommon.VotingProcedures{voter: {}},
		proposals: []conway.ConwayProposalProcedure{proposal},
		redeemers: conway.ConwayRedeemers{Redeemers: redeemers},
	})
	const metadataCBOR = "\xa1\x01\x42\xff\x00"
	metadata, err := lcommon.DecodeAuxiliaryDataToMetadata([]byte(metadataCBOR))
	if err != nil {
		t.Fatal(err)
	}
	tx.TxMetadata = metadata

	got, err := transactionBundle(tx, 0, "Conway", bundleDatumCollector{})
	if err != nil {
		t.Fatal(err)
	}
	wantPurposes := []string{
		"spend",
		"mint",
		"certificate",
		"reward",
		"vote",
		"proposal",
	}
	if len(got.Redeemers) != len(wantPurposes) {
		t.Fatalf("redeemers = %#v", got.Redeemers)
	}
	for index, purpose := range wantPurposes {
		row := got.Redeemers[index]
		if row.Purpose != purpose || !bytes.Equal(row.DataCBOR, datum.Cbor()) {
			t.Fatalf("redeemer %d = %#v, want purpose %q", index, row, purpose)
		}
	}
	if got.Redeemers[0].TargetTxHash == nil ||
		got.Redeemers[1].TargetPolicyID == nil ||
		len(got.Redeemers[2].TargetIdentity) != 34 ||
		len(got.Redeemers[3].TargetRewardAccount) == 0 ||
		len(got.Redeemers[4].TargetIdentity) == 0 ||
		len(got.Redeemers[5].TargetIdentity) != 34 {
		t.Fatalf("redeemer targets = %#v", got.Redeemers)
	}
	if got.Metadata == nil ||
		!bytes.Equal(got.Metadata.CBOR, []byte(metadataCBOR)) ||
		len(got.Metadata.Labels) != 1 ||
		got.Metadata.Labels[0] != 1 {
		t.Fatalf("metadata = %#v", got.Metadata)
	}
}

func TestMalformedCBORAndNumericOverflowFailBeforeFacts(t *testing.T) {
	if _, err := Decode(ledger.BlockTypeConway, []byte{0xff}); err == nil {
		t.Fatal("malformed block CBOR unexpectedly decoded")
	}
	if _, err := uint64Value(big.NewInt(-1), "negative"); err == nil {
		t.Fatal("negative UInt64 unexpectedly normalized")
	}
	overflow := new(big.Int).Lsh(big.NewInt(1), 64)
	if _, err := uint64Value(overflow, "overflow"); err == nil {
		t.Fatal("overflowing UInt64 unexpectedly normalized")
	}
	var policy lcommon.Blake2b224
	assets := lcommon.NewMultiAsset(
		map[lcommon.Blake2b224]map[cbor.ByteString]*big.Int{
			policy: {cbor.NewByteString([]byte("x")): overflow},
		},
	)
	if _, err := assetBundle(&assets); err == nil {
		t.Fatal("overflowing output asset quantity unexpectedly normalized")
	}
}

func TestRawAssetObservationsAreNotLedgerValidated(t *testing.T) {
	var policy lcommon.Blake2b224
	longName := bytes.Repeat([]byte{0xab}, 64)
	outputAssets := lcommon.NewMultiAsset(
		map[lcommon.Blake2b224]map[cbor.ByteString]*big.Int{
			policy: {cbor.NewByteString(longName): big.NewInt(0)},
		},
	)
	assets, err := assetBundle(&outputAssets)
	if err != nil {
		t.Fatal(err)
	}
	if len(assets) != 1 ||
		assets[0].Quantity != 0 ||
		!bytes.Equal(assets[0].Name, longName) {
		t.Fatalf("raw output assets = %#v", assets)
	}

	mintAssets := lcommon.NewMultiAsset(
		map[lcommon.Blake2b224]map[cbor.ByteString]*big.Int{
			policy: {cbor.NewByteString(longName): big.NewInt(0)},
		},
	)
	mint, err := mintBundle(&mintAssets)
	if err != nil {
		t.Fatal(err)
	}
	if len(mint) != 1 ||
		mint[0].Quantity != 0 ||
		!bytes.Equal(mint[0].Name, longName) {
		t.Fatalf("raw mint observations = %#v", mint)
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
	certificates     []lcommon.CertificateWrapper
	voting           lcommon.VotingProcedures
	proposals        []conway.ConwayProposalProcedure
	redeemers        conway.ConwayRedeemers
}

func testConwayTransaction(
	t *testing.T,
	valid bool,
	options txOptions,
) *ledger.ConwayTransaction {
	t.Helper()
	template := ledger.ConwayTransactionBody{
		TxInputs:             conway.NewConwayTransactionInputSet(options.inputs),
		TxOutputs:            options.outputs,
		TxFee:                options.fee,
		TxMint:               options.mint,
		TxCollateral:         cbor.NewSetType(options.collateral, true),
		TxCollateralReturn:   options.collateralReturn,
		TxTotalCollateral:    options.totalCollateral,
		TxReferenceInputs:    cbor.NewSetType(options.reference, true),
		TxWithdrawals:        options.withdrawals,
		TxCertificates:       options.certificates,
		TxVotingProcedures:   options.voting,
		TxProposalProcedures: options.proposals,
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
	raw := append([]byte{0x61}, bytes.Repeat([]byte{0x11}, 28)...)
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
	address, err := lcommon.NewAddressFromBytes(
		append([]byte{header}, bytes.Repeat([]byte{fill}, 28)...),
	)
	if err != nil {
		t.Fatal(err)
	}
	return &address
}

func testOutput(
	assets *lcommon.MultiAsset[lcommon.MultiAssetTypeOutput],
	lovelace uint64,
) ledger.BabbageTransactionOutput {
	address, err := lcommon.NewAddressFromBytes(
		append([]byte{0x61}, bytes.Repeat([]byte{0x11}, 28)...),
	)
	if err != nil {
		panic(err)
	}
	return ledger.BabbageTransactionOutput{
		OutputAddress: address,
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
	copy(policy[:], bytes.Repeat([]byte{0x10}, len(policy)))
	assets := lcommon.NewMultiAsset(
		map[lcommon.Blake2b224]map[cbor.ByteString]*big.Int{
			policy: {
				cbor.NewByteString(nil):                            new(big.Int).SetUint64(math.MaxUint64),
				cbor.NewByteString(bytes.Repeat([]byte{0x22}, 32)): big.NewInt(7),
			},
		},
	)
	return &assets
}

func testMint() *lcommon.MultiAsset[lcommon.MultiAssetTypeMint] {
	var policy lcommon.Blake2b224
	copy(policy[:], bytes.Repeat([]byte{0x20}, len(policy)))
	mint := lcommon.NewMultiAsset(
		map[lcommon.Blake2b224]map[cbor.ByteString]*big.Int{
			policy: {
				cbor.NewByteString(nil):          big.NewInt(math.MinInt64),
				cbor.NewByteString([]byte{0x01}): big.NewInt(math.MaxInt64),
			},
		},
	)
	return &mint
}
