package normalize

import (
	"bytes"
	"encoding/hex"
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

func TestInvalidZeroTotalCollateralRemainsUnknownDuringNormalization(t *testing.T) {
	collateral := ledger.NewShelleyTransactionInput(strings.Repeat("22", 32), 2)
	tx := testConwayTransaction(t, false, txOptions{
		collateral: []ledger.ShelleyTransactionInput{collateral},
		fee:        99,
	})
	got, err := transactionBundle(tx, 0, "Conway", bundleDatumCollector{})
	if err != nil {
		t.Fatal(err)
	}
	if got.EffectiveFee != nil {
		t.Fatalf("zero/absent total collateral normalized as known fee %d", *got.EffectiveFee)
	}
}

func TestTransactionBundleRejectsRegularReferenceOverlap(t *testing.T) {
	shared := ledger.NewShelleyTransactionInput(strings.Repeat("11", 32), 7)
	tx := testConwayTransaction(t, true, txOptions{
		inputs:    []ledger.ShelleyTransactionInput{shared},
		reference: []ledger.ShelleyTransactionInput{shared},
		fee:       1,
	})
	if _, err := transactionBundle(tx, 0, "Conway", bundleDatumCollector{}); err == nil ||
		!strings.Contains(
			err.Error(),
			"regular input reference 1111111111111111111111111111111111111111111111111111111111111111#7 also appears as reference input",
		) {
		t.Fatalf("error = %v", err)
	}
}

func TestTransactionBundleAllowsRegularCollateralOverlap(t *testing.T) {
	shared := ledger.NewShelleyTransactionInput(strings.Repeat("11", 32), 7)
	tx := testConwayTransaction(t, true, txOptions{
		inputs:     []ledger.ShelleyTransactionInput{shared},
		collateral: []ledger.ShelleyTransactionInput{shared},
		fee:        1,
	})
	got, err := transactionBundle(tx, 0, "Conway", bundleDatumCollector{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Inputs) != 2 ||
		got.Inputs[0].Role != "regular" || !got.Inputs[0].Consumed ||
		got.Inputs[0].BodyOrdinal != 0 ||
		got.Inputs[1].Role != "collateral" || got.Inputs[1].Consumed ||
		got.Inputs[1].BodyOrdinal != 0 {
		t.Fatalf("input roles = %#v", got.Inputs)
	}
}

func TestTransactionBundleAllowsCollateralReferenceOverlap(t *testing.T) {
	shared := ledger.NewShelleyTransactionInput(strings.Repeat("11", 32), 7)
	tx := testConwayTransaction(t, true, txOptions{
		collateral: []ledger.ShelleyTransactionInput{shared},
		reference:  []ledger.ShelleyTransactionInput{shared},
		fee:        1,
	})
	got, err := transactionBundle(tx, 0, "Conway", bundleDatumCollector{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Inputs) != 2 ||
		got.Inputs[0].Role != "collateral" || got.Inputs[0].Consumed ||
		got.Inputs[0].BodyOrdinal != 0 ||
		got.Inputs[1].Role != "reference" || got.Inputs[1].Consumed ||
		got.Inputs[1].BodyOrdinal != 0 {
		t.Fatalf("input roles = %#v", got.Inputs)
	}
}

func TestSpendRedeemerUsesLedgerOrderedInputSetAndPreservesBodyOrdinal(t *testing.T) {
	wireFirst := ledger.NewShelleyTransactionInput(strings.Repeat("22", 32), 9)
	ledgerFirst := ledger.NewShelleyTransactionInput(strings.Repeat("11", 32), 7)
	datum := testDatum(t, []byte{0x01})
	tx := testConwayTransaction(t, true, txOptions{
		inputs:  []ledger.ShelleyTransactionInput{wireFirst, ledgerFirst},
		outputs: []ledger.BabbageTransactionOutput{testOutput(nil, 1)},
		fee:     1,
		redeemers: conway.ConwayRedeemers{Redeemers: map[lcommon.RedeemerKey]lcommon.RedeemerValue{
			{Tag: lcommon.RedeemerTagSpend, Index: 0}: {
				Data: datum, ExUnits: lcommon.ExUnits{Memory: 1, Steps: 1},
			},
		}},
	})
	got, err := transactionBundle(tx, 0, "Conway", bundleDatumCollector{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Inputs) != 2 ||
		got.Inputs[0].BodyOrdinal != 0 ||
		got.Inputs[0].SourceHash[0] != 0x22 ||
		got.Inputs[1].BodyOrdinal != 1 ||
		got.Inputs[1].SourceHash[0] != 0x11 {
		t.Fatalf("wire input order/body ordinals changed: %#v", got.Inputs)
	}
	if len(got.Redeemers) != 1 ||
		got.Redeemers[0].TargetTxHash == nil ||
		got.Redeemers[0].TargetTxHash[0] != 0x11 ||
		got.Redeemers[0].TargetOutputIndex == nil ||
		*got.Redeemers[0].TargetOutputIndex != 7 {
		t.Fatalf("spend redeemer did not target ledger-ordered input: %#v", got.Redeemers)
	}
}

func TestSpendRedeemerRejectsDuplicateInputReference(t *testing.T) {
	input := ledger.NewShelleyTransactionInput(strings.Repeat("11", 32), 7)
	datum := testDatum(t, []byte{0x01})
	tx := testConwayTransaction(t, true, txOptions{
		inputs:  []ledger.ShelleyTransactionInput{input, input},
		outputs: []ledger.BabbageTransactionOutput{testOutput(nil, 1)},
		fee:     1,
		redeemers: conway.ConwayRedeemers{Redeemers: map[lcommon.RedeemerKey]lcommon.RedeemerValue{
			{Tag: lcommon.RedeemerTagSpend, Index: 0}: {
				Data: datum, ExUnits: lcommon.ExUnits{Memory: 1, Steps: 1},
			},
		}},
	})
	if _, err := transactionBundle(tx, 0, "Conway", bundleDatumCollector{}); err == nil ||
		!strings.Contains(err.Error(), "duplicate regular input reference") {
		t.Fatalf("error = %v", err)
	}
}

func TestMainnetLegacyRedeemerArrayCanonicalizesDuplicatePointer(t *testing.T) {
	// Exact witness-set key 5 payload from canonical mainnet transaction
	// 836a0975388c78b46eb4521689b117afde516bde9ef1a7735506e5ae1a296ace
	// in Conway block 10781466. Its final two entries repeat mint(0).
	raw, err := hex.DecodeString(
		"83840002d879808219b3381a00fdf28e" +
			"840100d87980821a000d8a0d1a10c32faf" +
			"840100d87980821a000d8a0d1a10c32faf",
	)
	if err != nil {
		t.Fatal(err)
	}
	var redeemers conway.ConwayRedeemers
	if err := redeemers.UnmarshalCBOR(raw); err != nil {
		t.Fatal(err)
	}
	decodedEntries := 0
	for range redeemers.Iter() {
		decodedEntries++
	}
	if decodedEntries != 3 {
		t.Fatalf("decoded witness entries = %d, want 3", decodedEntries)
	}
	tx := testConwayTransaction(t, true, txOptions{
		inputs: []ledger.ShelleyTransactionInput{
			ledger.NewShelleyTransactionInput(strings.Repeat("11", 32), 0),
			ledger.NewShelleyTransactionInput(strings.Repeat("22", 32), 0),
			ledger.NewShelleyTransactionInput(strings.Repeat("33", 32), 0),
		},
		outputs:   []ledger.BabbageTransactionOutput{testOutput(nil, 1)},
		fee:       1,
		mint:      testMint(),
		redeemers: redeemers,
	})
	got, err := transactionBundle(tx, 0, "Conway", bundleDatumCollector{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Redeemers) != 2 {
		t.Fatalf("canonical redeemers = %#v", got.Redeemers)
	}
	spend := got.Redeemers[0]
	mint := got.Redeemers[1]
	if spend.Purpose != "spend" || spend.Index != 2 ||
		spend.ExUnitsMemory != 45880 || spend.ExUnitsSteps != 16642702 ||
		mint.Purpose != "mint" || mint.Index != 0 ||
		mint.ExUnitsMemory != 887309 || mint.ExUnitsSteps != 281227183 {
		t.Fatalf("canonical redeemers = %#v", got.Redeemers)
	}
	wantData := []byte{0xd8, 0x79, 0x80}
	wantHash, err := hex.DecodeString(
		"923918e403bf43c34b4ef6b48eb2ee04babed17320d8d1b9ff9ad086e86f44ec",
	)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(spend.DataCBOR, wantData) ||
		!bytes.Equal(mint.DataCBOR, wantData) ||
		!bytes.Equal(spend.DataHash[:], wantHash) ||
		!bytes.Equal(mint.DataHash[:], wantHash) {
		t.Fatalf("canonical redeemer data/hash = %#v", got.Redeemers)
	}
}

func TestLegacyRedeemerArrayDuplicatePointerIsLastWins(t *testing.T) {
	// Map.fromList semantics must follow original encoded order. These two
	// spend(0) entries intentionally carry different data and execution units.
	raw, err := hex.DecodeString("828400000082010284000001820304")
	if err != nil {
		t.Fatal(err)
	}
	var redeemers conway.ConwayRedeemers
	if err := redeemers.UnmarshalCBOR(raw); err != nil {
		t.Fatal(err)
	}
	tx := testConwayTransaction(t, true, txOptions{
		inputs: []ledger.ShelleyTransactionInput{
			ledger.NewShelleyTransactionInput(strings.Repeat("11", 32), 0),
		},
		outputs:   []ledger.BabbageTransactionOutput{testOutput(nil, 1)},
		fee:       1,
		redeemers: redeemers,
	})
	got, err := transactionBundle(tx, 0, "Conway", bundleDatumCollector{})
	if err != nil {
		t.Fatal(err)
	}
	wantHash, err := hex.DecodeString(
		"ee155ace9c40292074cb6aff8c9ccdd273c81648ff1149ef36bcea6ebb8a3e25",
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Redeemers) != 1 ||
		!bytes.Equal(got.Redeemers[0].DataCBOR, []byte{0x01}) ||
		!bytes.Equal(got.Redeemers[0].DataHash[:], wantHash) ||
		got.Redeemers[0].ExUnitsMemory != 3 ||
		got.Redeemers[0].ExUnitsSteps != 4 {
		t.Fatalf("last-wins redeemer = %#v", got.Redeemers)
	}
}

func TestVoterRedeemerOrderMatchesLedgerAndCDDLOracle(t *testing.T) {
	if lcommon.VoterTypeConstitutionalCommitteeHotKeyHash != 0 ||
		lcommon.VoterTypeConstitutionalCommitteeHotScriptHash != 1 ||
		lcommon.VoterTypeDRepKeyHash != 2 ||
		lcommon.VoterTypeDRepScriptHash != 3 ||
		lcommon.VoterTypeStakingPoolKeyHash != 4 {
		t.Fatal("gouroboros voter tags differ from the Conway CDDL 0..4 oracle")
	}
	voter := func(voterType uint8, fill byte) *lcommon.Voter {
		ret := &lcommon.Voter{Type: voterType}
		for index := range ret.Hash {
			ret.Hash[index] = fill
		}
		return ret
	}
	committeeScriptHigh := voter(lcommon.VoterTypeConstitutionalCommitteeHotScriptHash, 0x22)
	committeeScriptLow := voter(lcommon.VoterTypeConstitutionalCommitteeHotScriptHash, 0x11)
	committeeKey := voter(lcommon.VoterTypeConstitutionalCommitteeHotKeyHash, 0x01)
	drepScript := voter(lcommon.VoterTypeDRepScriptHash, 0x01)
	drepKey := voter(lcommon.VoterTypeDRepKeyHash, 0x01)
	pool := voter(lcommon.VoterTypeStakingPoolKeyHash, 0x01)
	votes := lcommon.VotingProcedures{
		pool:                {},
		drepKey:             {},
		committeeKey:        {},
		committeeScriptHigh: {},
		drepScript:          {},
		committeeScriptLow:  {},
	}
	got, err := sortedVoters(votes)
	if err != nil {
		t.Fatal(err)
	}
	want := []*lcommon.Voter{
		committeeScriptLow,
		committeeScriptHigh,
		committeeKey,
		drepScript,
		drepKey,
		pool,
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("voter %d = type %d hash %x, want type %d hash %x",
				index, got[index].Type, got[index].Hash, want[index].Type, want[index].Hash)
		}
	}
	unknown := voter(5, 0)
	if _, err := sortedVoters(lcommon.VotingProcedures{unknown: {}}); err == nil ||
		!strings.Contains(err.Error(), "unknown voter type") {
		t.Fatalf("unknown voter error = %v", err)
	}
}

func TestCompactCertificateAndProposalTargetIdentityIsDeterministic(t *testing.T) {
	credential := func(fill byte) lcommon.Credential {
		return lcommon.Credential{
			CredType:   lcommon.CredentialTypeScriptHash,
			Credential: lcommon.NewBlake2b224(bytesOf(fill, 28)),
		}
	}
	certificate := func(fill byte) *lcommon.StakeRegistrationCertificate {
		return &lcommon.StakeRegistrationCertificate{
			CertType:        uint(lcommon.CertificateTypeStakeRegistration),
			StakeCredential: credential(fill),
		}
	}
	first, err := compactLedgerTargetIdentity(
		uint8(lcommon.RedeemerTagCert),
		uint(lcommon.CertificateTypeStakeRegistration),
		uint8(lcommon.CertificateTypeUpdateDrep),
		certificate(0x11),
	)
	if err != nil {
		t.Fatal(err)
	}
	same, err := compactLedgerTargetIdentity(
		uint8(lcommon.RedeemerTagCert),
		uint(lcommon.CertificateTypeStakeRegistration),
		uint8(lcommon.CertificateTypeUpdateDrep),
		certificate(0x11),
	)
	if err != nil {
		t.Fatal(err)
	}
	different, err := compactLedgerTargetIdentity(
		uint8(lcommon.RedeemerTagCert),
		uint(lcommon.CertificateTypeStakeRegistration),
		uint8(lcommon.CertificateTypeUpdateDrep),
		certificate(0x12),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 34 ||
		first[0] != uint8(lcommon.RedeemerTagCert) ||
		first[1] != uint8(lcommon.CertificateTypeStakeRegistration) {
		t.Fatalf("certificate identity header = %x", first)
	}
	if !bytes.Equal(first, same) || bytes.Equal(first, different) {
		t.Fatalf("certificate identities same=%x different=%x", same, different)
	}
	cached := certificate(0x11)
	cached.SetCbor([]byte{0x81, 0x00})
	canonical, err := cbor.Encode(cached)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(canonical, cached.Cbor()) {
		t.Fatal("canonical certificate encoding reused cached original CBOR")
	}
	cachedIdentity, err := compactLedgerTargetIdentity(
		uint8(lcommon.RedeemerTagCert),
		uint(lcommon.CertificateTypeStakeRegistration),
		uint8(lcommon.CertificateTypeUpdateDrep),
		cached,
	)
	if err != nil {
		t.Fatal(err)
	}
	canonicalDigest := lcommon.Blake2b256Hash(canonical)
	if !bytes.Equal(cachedIdentity[2:], canonicalDigest.Bytes()) {
		t.Fatalf(
			"identity digest = %x, canonical digest = %x",
			cachedIdentity[2:],
			canonicalDigest.Bytes(),
		)
	}

	proposal := func(deposit uint64) conway.ConwayProposalProcedure {
		action := &lcommon.InfoGovAction{Type: uint(lcommon.GovActionTypeInfo)}
		return conway.ConwayProposalProcedure{
			PPDeposit:       deposit,
			PPRewardAccount: *testRewardAccount(t, false, 0x21),
			PPGovAction: conway.ConwayGovAction{
				Type:   uint(lcommon.GovActionTypeInfo),
				Action: action,
			},
		}
	}
	proposalFirst, err := compactLedgerTargetIdentity(
		uint8(lcommon.RedeemerTagProposing),
		uint(lcommon.GovActionTypeInfo),
		uint8(lcommon.GovActionTypeInfo),
		proposal(1),
	)
	if err != nil {
		t.Fatal(err)
	}
	proposalSame, err := compactLedgerTargetIdentity(
		uint8(lcommon.RedeemerTagProposing),
		uint(lcommon.GovActionTypeInfo),
		uint8(lcommon.GovActionTypeInfo),
		proposal(1),
	)
	if err != nil {
		t.Fatal(err)
	}
	proposalDifferent, err := compactLedgerTargetIdentity(
		uint8(lcommon.RedeemerTagProposing),
		uint(lcommon.GovActionTypeInfo),
		uint8(lcommon.GovActionTypeInfo),
		proposal(2),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(proposalFirst) != 34 ||
		proposalFirst[0] != uint8(lcommon.RedeemerTagProposing) ||
		proposalFirst[1] != uint8(lcommon.GovActionTypeInfo) {
		t.Fatalf("proposal identity header = %x", proposalFirst)
	}
	if !bytes.Equal(proposalFirst, proposalSame) ||
		bytes.Equal(proposalFirst, proposalDifferent) {
		t.Fatalf(
			"proposal identities same=%x different=%x",
			proposalSame,
			proposalDifferent,
		)
	}
}

func TestCompactTargetIdentityFailsClosedWithoutBoundedCanonicalCBOR(t *testing.T) {
	if _, err := compactLedgerTargetIdentity(2, 0, 18, make(chan int)); err == nil ||
		!strings.Contains(err.Error(), "encode canonical") {
		t.Fatalf("unavailable CBOR error = %v", err)
	}
	oversized := []any{uint64(0), bytesOf(0x55, maxContextCBORSize)}
	if _, err := compactLedgerTargetIdentity(2, 0, 18, oversized); err == nil ||
		!strings.Contains(err.Error(), "outside") {
		t.Fatalf("oversized CBOR error = %v", err)
	}
	if _, err := compactLedgerTargetIdentity(2, 19, 18, []any{uint64(19)}); err == nil ||
		!strings.Contains(err.Error(), "outside") {
		t.Fatalf("unknown constructor error = %v", err)
	}
}

func TestCertificateAndProposalRedeemersPersistCompactTargetIdentity(t *testing.T) {
	scriptCredential := lcommon.Credential{
		CredType:   lcommon.CredentialTypeScriptHash,
		Credential: lcommon.NewBlake2b224(bytesOf(0x41, 28)),
	}
	certificate := &lcommon.StakeRegistrationCertificate{
		CertType:        uint(lcommon.CertificateTypeStakeRegistration),
		StakeCredential: scriptCredential,
	}
	action := &lcommon.InfoGovAction{Type: uint(lcommon.GovActionTypeInfo)}
	proposal := conway.ConwayProposalProcedure{
		PPDeposit:       1,
		PPRewardAccount: *testRewardAccount(t, false, 0x42),
		PPGovAction: conway.ConwayGovAction{
			Type:   uint(lcommon.GovActionTypeInfo),
			Action: action,
		},
	}
	datum := testDatum(t, []byte{0x01})
	tx := testConwayTransaction(t, true, txOptions{
		outputs: []ledger.BabbageTransactionOutput{testOutput(nil, 1)},
		fee:     1,
		certificates: []lcommon.CertificateWrapper{{
			Type:        uint(lcommon.CertificateTypeStakeRegistration),
			Certificate: certificate,
		}},
		proposals: []conway.ConwayProposalProcedure{proposal},
		redeemers: conway.ConwayRedeemers{Redeemers: map[lcommon.RedeemerKey]lcommon.RedeemerValue{
			{Tag: lcommon.RedeemerTagCert, Index: 0}: {
				Data: datum, ExUnits: lcommon.ExUnits{Memory: 1, Steps: 1},
			},
			{Tag: lcommon.RedeemerTagProposing, Index: 0}: {
				Data: datum, ExUnits: lcommon.ExUnits{Memory: 1, Steps: 1},
			},
		}},
	})
	got, err := transactionBundle(tx, 0, "Conway", bundleDatumCollector{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Redeemers) != 2 {
		t.Fatalf("redeemers = %#v", got.Redeemers)
	}
	certificateRedeemer := got.Redeemers[0]
	proposalRedeemer := got.Redeemers[1]
	if certificateRedeemer.Purpose != "certificate" ||
		len(certificateRedeemer.TargetIdentity) != 34 ||
		certificateRedeemer.TargetIdentity[1] !=
			uint8(lcommon.CertificateTypeStakeRegistration) ||
		certificateRedeemer.ResolvedScriptHash == nil {
		t.Fatalf("certificate redeemer = %#v", certificateRedeemer)
	}
	if proposalRedeemer.Purpose != "proposal" ||
		len(proposalRedeemer.TargetIdentity) != 34 ||
		proposalRedeemer.TargetIdentity[1] != uint8(lcommon.GovActionTypeInfo) ||
		proposalRedeemer.ResolvedScriptHash != nil {
		t.Fatalf("proposal redeemer = %#v", proposalRedeemer)
	}
}

func TestPaymentCredentialExtractionIsLedgerCompatible(t *testing.T) {
	address := func(header byte, payment byte, suffix []byte) []byte {
		ret := append([]byte{header}, bytesOf(payment, 28)...)
		return append(ret, suffix...)
	}
	baseStake := bytesOf(0x77, 28)
	tests := []struct {
		name string
		raw  []byte
		kind string
		hash byte
	}{
		{"base key", address(0x01, 0x11, baseStake), "key", 0x11},
		{"base script", address(0x11, 0x22, baseStake), "script", 0x22},
		{"pointer key", address(0x41, 0x33, []byte{0, 0, 0}), "key", 0x33},
		{"pointer script", address(0x51, 0x44, []byte{0, 0, 0}), "script", 0x44},
		{"enterprise key", address(0x61, 0x55, nil), "key", 0x55},
		{"enterprise script", address(0x71, 0x66, nil), "script", 0x66},
		{"base trailing", address(0x01, 0x77, append(baseStake, 0xaa)), "key", 0x77},
		{"pointer trailing", address(0x41, 0x88, []byte{0, 0, 0, 0xaa}), "key", 0x88},
		{"enterprise trailing", address(0x61, 0x99, []byte{0xaa}), "key", 0x99},
	}
	byron, err := lcommon.NewByronAddressFromParts(
		lcommon.ByronAddressTypePubkey,
		bytesOf(0x88, 28),
		lcommon.ByronAddressAttributes{},
	)
	if err != nil {
		t.Fatal(err)
	}
	byronRaw, err := byron.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	tests = append(tests, struct {
		name string
		raw  []byte
		kind string
		hash byte
	}{"Byron unsupported", byronRaw, "none", 0})
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			kind, hash, err := paymentCredentialFromAddress(test.raw, "Alonzo")
			if err != nil {
				t.Fatal(err)
			}
			if kind != test.kind {
				t.Fatalf("kind = %q, want %q", kind, test.kind)
			}
			if test.kind == "none" {
				if hash != nil {
					t.Fatalf("unsupported address returned hash %x", *hash)
				}
			} else if hash == nil || hash[0] != test.hash {
				t.Fatalf("hash = %v, want fill %x", hash, test.hash)
			}
		})
	}
	malformed := [][]byte{
		address(0x60, 0x11, nil),
		address(0x70, 0x11, nil),
		address(0x41, 0x11, []byte{0, 0}),
		{0x91},
		address(0xe1, 0x77, nil),
		address(0xf1, 0x77, nil),
	}
	for _, raw := range malformed {
		if _, _, err := paymentCredentialFromAddress(raw, "Alonzo"); err == nil {
			t.Fatalf("malformed address accepted: %x", raw)
		}
	}
	for _, raw := range [][]byte{
		address(0x01, 0x11, append(baseStake, 0xaa)),
		address(0x41, 0x11, []byte{0, 0, 0, 0xaa}),
		address(0x61, 0x11, []byte{0xaa}),
	} {
		if _, _, err := paymentCredentialFromAddress(raw, "Babbage"); err == nil {
			t.Fatalf("Babbage trailing address accepted: %x", raw)
		}
	}
}

func TestPaymentCredentialFromHistoricalTrailingBaseAddress(t *testing.T) {
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
	want, err := hex.DecodeString(
		"5bad085057ac10ecc643450a2031ae566ff63b395153cea2d023ba67",
	)
	if err != nil {
		t.Fatal(err)
	}
	kind, hash, err := paymentCredentialFromAddress(raw, "Alonzo")
	if err != nil {
		t.Fatal(err)
	}
	if kind != "key" || hash == nil || !bytes.Equal(hash[:], want) {
		t.Fatalf("credential = %q %x, want key %x", kind, hash, want)
	}
	if _, _, err := paymentCredentialFromAddress(raw, "Babbage"); err == nil {
		t.Fatal("historical trailing address was accepted as Babbage")
	}
	decoded, err := lcommon.NewAddressFromBytes(raw)
	if err != nil {
		t.Fatal(err)
	}
	roundTrip, err := decoded.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(roundTrip, raw) {
		t.Fatalf("raw address was not preserved: got %x, want %x", roundTrip, raw)
	}
}

func TestWithdrawalRejectsNonMainnetRewardAccount(t *testing.T) {
	testnet, err := lcommon.NewAddressFromBytes(
		append([]byte{0xe0}, bytesOf(0x11, 28)...),
	)
	if err != nil {
		t.Fatal(err)
	}
	tx := testConwayTransaction(t, true, txOptions{
		outputs:     []ledger.BabbageTransactionOutput{testOutput(nil, 1)},
		fee:         1,
		withdrawals: map[*lcommon.Address]uint64{&testnet: 1},
	})
	if _, err := transactionBundle(tx, 0, "Conway", bundleDatumCollector{}); err == nil ||
		!strings.Contains(err.Error(), "not pinned mainnet") {
		t.Fatalf("error = %v", err)
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

func TestRewardRedeemerUsesLedgerAccountAddressOrder(t *testing.T) {
	keyReward := testRewardAccount(t, false, 0x11)
	scriptReward := testRewardAccount(t, true, 0x22)
	if bytes.Compare(
		mustAddressBytes(t, keyReward),
		mustAddressBytes(t, scriptReward),
	) >= 0 {
		t.Fatal("oracle requires raw key reward bytes to sort before script bytes")
	}
	datum := testDatum(t, []byte{0x01})
	tx := testConwayTransaction(t, true, txOptions{
		outputs: []ledger.BabbageTransactionOutput{testOutput(nil, 1)},
		fee:     1,
		withdrawals: map[*lcommon.Address]uint64{
			keyReward:    1,
			scriptReward: 2,
		},
		redeemers: conway.ConwayRedeemers{Redeemers: map[lcommon.RedeemerKey]lcommon.RedeemerValue{
			{Tag: lcommon.RedeemerTagReward, Index: 0}: {
				Data: datum, ExUnits: lcommon.ExUnits{Memory: 1, Steps: 1},
			},
		}},
	})
	got, err := transactionBundle(tx, 0, "Conway", bundleDatumCollector{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Withdrawals) != 2 ||
		got.Withdrawals[0].CredentialKind != "script" ||
		got.Withdrawals[0].BodyOrdinal != 0 ||
		got.Withdrawals[1].CredentialKind != "key" ||
		got.Withdrawals[1].BodyOrdinal != 1 {
		t.Fatalf("ledger withdrawal ordering = %#v", got.Withdrawals)
	}
	if len(got.Redeemers) != 1 ||
		!bytes.Equal(got.Redeemers[0].TargetRewardAccount, mustAddressBytes(t, scriptReward)) ||
		got.Redeemers[0].ResolvedScriptHash == nil ||
		*got.Redeemers[0].ResolvedScriptHash != got.Withdrawals[0].CredentialHash {
		t.Fatalf("reward redeemer target = %#v", got.Redeemers)
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

func TestTransactionIDVerificationRejectsMismatch(t *testing.T) {
	bodyCBOR := []byte{0xa0}
	var wrong lcommon.Blake2b256
	if err := verifyTransactionID(wrong, bodyCBOR); err == nil ||
		!strings.Contains(err.Error(), "transaction ID mismatch") {
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
	certificates     []lcommon.CertificateWrapper
	proposals        []conway.ConwayProposalProcedure
	redeemers        conway.ConwayRedeemers
}

func testConwayTransaction(t *testing.T, valid bool, options txOptions) *ledger.ConwayTransaction {
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
