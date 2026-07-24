package normalize

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"math/big"
	"slices"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"

	"cardano-clicksync/internal/model"
)

type bundleDatumCollector map[model.Hash32][]*model.DatumBody

// Bundle projects one decoded block into the native-writer fact model. It does
// not perform a second semantic or ledger validation pass. No full block,
// transaction, witness, auxiliary-data, or script CBOR leaves this function.
func Bundle(block lcommon.Block) (model.Block, error) {
	if block == nil {
		return model.Block{}, errors.New("nil block")
	}
	blockType := block.Type()
	if blockType < math.MinInt16 || blockType > math.MaxInt16 {
		return model.Block{}, fmt.Errorf("block type %d exceeds Int16", blockType)
	}
	blockHash, err := hash32(block.Hash().Bytes(), "block hash")
	if err != nil {
		return model.Block{}, err
	}
	var parentHash *model.Hash32
	if rawParent := block.PrevHash().Bytes(); len(rawParent) > 0 {
		value, err := hash32(rawParent, "parent hash")
		if err != nil {
			return model.Block{}, err
		}
		parentHash = &value
	}
	era := block.Era().Name
	transactions := block.Transactions()
	if len(transactions) > math.MaxUint32 {
		return model.Block{}, errors.New("transaction count exceeds UInt32")
	}
	ret := model.Block{
		Hash:         blockHash,
		ParentHash:   parentHash,
		Slot:         block.SlotNumber(),
		Number:       block.BlockNumber(),
		Era:          era,
		Type:         int16(blockType),
		Transactions: make([]model.Transaction, 0, len(transactions)),
	}
	datums := bundleDatumCollector{}
	for order, tx := range transactions {
		if tx == nil {
			return model.Block{}, fmt.Errorf("transaction %d is nil", order)
		}
		if order > math.MaxUint32 {
			return model.Block{}, errors.New("transaction order exceeds UInt32")
		}
		if err := ensureSupportedTransaction(tx); err != nil {
			return model.Block{}, fmt.Errorf("transaction %d: %w", order, err)
		}
		facts, err := transactionBundle(tx, uint32(order), era, datums)
		if err != nil {
			return model.Block{}, fmt.Errorf("transaction %d: %w", order, err)
		}
		ret.Transactions = append(ret.Transactions, facts)
	}
	hashes := make([]model.Hash32, 0, len(datums))
	for hash := range datums {
		hashes = append(hashes, hash)
	}
	slices.SortFunc(hashes, func(a, b model.Hash32) int {
		return bytes.Compare(a[:], b[:])
	})
	for _, hash := range hashes {
		for _, body := range datums[hash] {
			slices.SortFunc(body.Observations, compareDatumObservation)
			ret.Datums = append(ret.Datums, *body)
		}
	}
	return ret, nil
}

func transactionBundle(
	tx lcommon.Transaction,
	order uint32,
	era string,
	datums bundleDatumCollector,
) (model.Transaction, error) {
	txHash, err := hash32(tx.Hash().Bytes(), "transaction hash")
	if err != nil {
		return model.Transaction{}, err
	}
	valid := tx.IsValid()
	regularInputs := tx.Inputs()
	collateralInputs := tx.Collateral()
	referenceInputs := tx.ReferenceInputs()
	outputs := tx.Outputs()
	witnesses := tx.Witnesses()
	var witnessDatums []lcommon.Datum
	if witnesses != nil {
		witnessDatums = witnesses.PlutusData()
		if len(witnessDatums) > math.MaxUint32 {
			return model.Transaction{}, errors.New("witness datum count exceeds UInt32")
		}
	}
	outputCapacity := len(outputs)
	if !valid {
		outputCapacity = 1
	}
	ret := model.Transaction{
		Hash:              txHash,
		Order:             order,
		Era:               era,
		Phase2Valid:       valid,
		FlowKind:          "regular",
		MintApplied:       valid,
		Inputs:            make([]model.Input, 0, len(regularInputs)+len(collateralInputs)+len(referenceInputs)),
		Outputs:           make([]model.Output, 0, outputCapacity),
		DatumObservations: make([]model.DatumObservation, 0, len(outputs)+len(witnessDatums)),
	}
	if !valid {
		ret.FlowKind = "collateral"
	}
	if tx.Type() != ledger.TxTypeByron {
		declared, err := uint64Value(tx.Fee(), "declared fee")
		if err != nil {
			return model.Transaction{}, err
		}
		ret.DeclaredFee = &declared
		if valid {
			effective := declared
			ret.EffectiveFee = &effective
		} else if tx.TotalCollateral() != nil && tx.TotalCollateral().Sign() > 0 {
			effective, err := uint64Value(tx.TotalCollateral(), "total collateral")
			if err != nil {
				return model.Transaction{}, err
			}
			ret.EffectiveFee = &effective
		}
	}
	if err := appendInputs(&ret, regularInputs, "regular", valid); err != nil {
		return model.Transaction{}, err
	}
	if err := appendInputs(&ret, collateralInputs, "collateral", !valid); err != nil {
		return model.Transaction{}, err
	}
	if err := appendInputs(&ret, referenceInputs, "reference", false); err != nil {
		return model.Transaction{}, err
	}
	if valid {
		if len(outputs) > math.MaxUint32 {
			return model.Transaction{}, errors.New("output count exceeds UInt32")
		}
		for index, output := range outputs {
			facts, observations, err := outputBundle(
				txHash,
				order,
				uint32(index),
				uint32(index),
				"regular",
				era,
				output,
				datums,
			)
			if err != nil {
				return model.Transaction{}, fmt.Errorf("output %d: %w", index, err)
			}
			ret.Outputs = append(ret.Outputs, facts)
			ret.DatumObservations = append(ret.DatumObservations, observations...)
		}
	} else if output := tx.CollateralReturn(); output != nil {
		if len(outputs) > math.MaxUint32 {
			return model.Transaction{}, errors.New("collateral return index exceeds UInt32")
		}
		facts, observations, err := outputBundle(
			txHash,
			order,
			uint32(len(outputs)),
			0,
			"collateral_return",
			era,
			output,
			datums,
		)
		if err != nil {
			return model.Transaction{}, fmt.Errorf("collateral return: %w", err)
		}
		ret.Outputs = append(ret.Outputs, facts)
		ret.DatumObservations = append(ret.DatumObservations, observations...)
	}
	ret.Mint, err = mintBundle(tx.AssetMint())
	if err != nil {
		return model.Transaction{}, err
	}
	ret.Withdrawals, err = withdrawalBundle(tx, txHash, order)
	if err != nil {
		return model.Transaction{}, err
	}
	ret.Redeemers, err = redeemerBundle(
		tx,
		witnesses,
		txHash,
		order,
		regularInputs,
		ret.Withdrawals,
	)
	if err != nil {
		return model.Transaction{}, err
	}
	ret.Metadata, err = metadataBundle(tx, txHash, order)
	if err != nil {
		return model.Transaction{}, err
	}
	if witnesses != nil {
		for ordinal, datum := range witnessDatums {
			observation := model.DatumObservation{
				TransactionHash:  txHash,
				TransactionOrder: order,
				SourceKind:       "witness",
				SourceOrdinal:    uint32(ordinal),
			}
			hash, err := datums.add(datum, observation)
			if err != nil {
				return model.Transaction{}, fmt.Errorf("witness datum %d: %w", ordinal, err)
			}
			observation.Hash = hash
			ret.DatumObservations = append(ret.DatumObservations, observation)
		}
	}
	slices.SortFunc(ret.DatumObservations, compareDatumObservation)
	return ret, nil
}

func appendInputs(
	tx *model.Transaction,
	inputs []lcommon.TransactionInput,
	role string,
	consumed bool,
) error {
	if len(inputs) > math.MaxUint32 {
		return fmt.Errorf("%s input count exceeds UInt32", role)
	}
	for ordinal, input := range inputs {
		if input == nil {
			return fmt.Errorf("%s input %d is nil", role, ordinal)
		}
		sourceHash, err := hash32(input.Id().Bytes(), "source transaction hash")
		if err != nil {
			return err
		}
		tx.Inputs = append(tx.Inputs, model.Input{
			TransactionHash:  tx.Hash,
			TransactionOrder: tx.Order,
			SourceHash:       sourceHash,
			SourceIndex:      input.Index(),
			BodyOrdinal:      uint32(ordinal),
			Role:             role,
			Consumed:         consumed,
		})
	}
	return nil
}

func paymentCredentialFromAddress(
	decoded *lcommon.Address,
) (*string, *model.Hash28) {
	if decoded == nil {
		return nil, nil
	}
	addressType := decoded.Type()
	switch addressType {
	case lcommon.AddressTypeByron:
		kind := "none"
		return &kind, nil
	case lcommon.AddressTypeNoneKey,
		lcommon.AddressTypeNoneScript:
		return nil, nil
	case lcommon.AddressTypeKeyKey,
		lcommon.AddressTypeKeyScript,
		lcommon.AddressTypeScriptKey,
		lcommon.AddressTypeScriptScript:
	case lcommon.AddressTypeKeyNone,
		lcommon.AddressTypeScriptNone:
	case lcommon.AddressTypeKeyPointer,
		lcommon.AddressTypeScriptPointer:
	default:
		return nil, nil
	}
	if decoded.PayloadPayload() == nil {
		return nil, nil
	}
	paymentHash := decoded.PaymentKeyHash()
	hash, err := hash28(
		paymentHash.Bytes(),
		"payment credential",
	)
	if err != nil {
		return nil, nil
	}
	kind := "key"
	switch addressType {
	case lcommon.AddressTypeScriptKey,
		lcommon.AddressTypeScriptScript,
		lcommon.AddressTypeScriptPointer,
		lcommon.AddressTypeScriptNone:
		kind = "script"
	}
	return &kind, &hash
}

func outputBundle(
	txHash model.Hash32,
	txOrder uint32,
	index uint32,
	ordinal uint32,
	kind string,
	_ string,
	output lcommon.TransactionOutput,
	datums bundleDatumCollector,
) (model.Output, []model.DatumObservation, error) {
	if output == nil {
		return model.Output{}, nil, errors.New("nil output")
	}
	decodedAddress := output.Address()
	address, err := decodedAddress.Bytes()
	if err != nil {
		return model.Output{}, nil, fmt.Errorf("address bytes: %w", err)
	}
	lovelace, err := uint64Value(output.Amount(), "output lovelace")
	if err != nil {
		return model.Output{}, nil, err
	}
	assets, err := assetBundle(output.Assets())
	if err != nil {
		return model.Output{}, nil, err
	}
	paymentKind, paymentHash := paymentCredentialFromAddress(&decodedAddress)
	ret := model.Output{
		TransactionHash:       txHash,
		TransactionOrder:      txOrder,
		Index:                 index,
		BodyOrdinal:           ordinal,
		Kind:                  kind,
		Address:               address,
		PaymentCredentialKind: paymentKind,
		PaymentCredentialHash: paymentHash,
		Lovelace:              lovelace,
		Assets:                assets,
		DatumKind:             "none",
	}
	var observations []model.DatumObservation
	if script := output.ScriptRef(); script != nil {
		if hash, err := hash28(script.Hash().Bytes(), "reference script hash"); err == nil {
			ret.ReferenceScriptHash = &hash
		}
		if language, err := scriptLanguage(script); err == nil {
			ret.ReferenceScriptLanguage = &language
		}
	}
	datum := output.Datum()
	decodedHash := output.DatumHash()
	switch {
	case datum != nil:
		hash, err := bundleDatumHash(datum)
		if err != nil {
			return model.Output{}, nil, err
		}
		ret.DatumKind = "inline"
		ret.DatumHash = &hash
		outputIndex := index
		observation := model.DatumObservation{
			Hash:             hash,
			TransactionHash:  txHash,
			TransactionOrder: txOrder,
			SourceKind:       "inline_output",
			SourceOrdinal:    ordinal,
			OutputIndex:      &outputIndex,
		}
		if _, err := datums.add(*datum, observation); err != nil {
			return model.Output{}, nil, err
		}
		observations = append(observations, observation)
	case decodedHash != nil:
		hash, err := hash32(decodedHash.Bytes(), "datum hash")
		if err != nil {
			return model.Output{}, nil, err
		}
		ret.DatumKind = "hash"
		ret.DatumHash = &hash
	}
	return ret, observations, nil
}

func assetBundle(assets *lcommon.MultiAsset[lcommon.MultiAssetTypeOutput]) ([]model.Asset, error) {
	if assets == nil {
		return nil, nil
	}
	var ret []model.Asset
	for _, policy := range assets.Policies() {
		policyID, err := hash28(policy.Bytes(), "asset policy")
		if err != nil {
			return nil, err
		}
		for _, name := range assets.Assets(policy) {
			quantity := assets.Asset(policy, name)
			value, err := uint64Value(quantity, "output asset quantity")
			if err != nil {
				return nil, err
			}
			ret = append(ret, model.Asset{PolicyID: policyID, Name: bytes.Clone(name), Quantity: value})
		}
	}
	slices.SortFunc(ret, func(a, b model.Asset) int {
		if value := bytes.Compare(a.PolicyID[:], b.PolicyID[:]); value != 0 {
			return value
		}
		return bytes.Compare(a.Name, b.Name)
	})
	return ret, nil
}

func mintBundle(mint *lcommon.MultiAsset[lcommon.MultiAssetTypeMint]) ([]model.AssetDelta, error) {
	if mint == nil {
		return nil, nil
	}
	var ret []model.AssetDelta
	for _, policy := range mint.Policies() {
		policyID, err := hash28(policy.Bytes(), "mint policy")
		if err != nil {
			return nil, err
		}
		for _, name := range mint.Assets(policy) {
			quantity := mint.Asset(policy, name)
			if quantity == nil || !quantity.IsInt64() {
				return nil, fmt.Errorf("mint quantity does not fit Int64")
			}
			ret = append(ret, model.AssetDelta{
				PolicyID: policyID,
				Name:     bytes.Clone(name),
				Quantity: quantity.Int64(),
			})
		}
	}
	slices.SortFunc(ret, func(a, b model.AssetDelta) int {
		if value := bytes.Compare(a.PolicyID[:], b.PolicyID[:]); value != 0 {
			return value
		}
		return bytes.Compare(a.Name, b.Name)
	})
	return ret, nil
}

func withdrawalBundle(
	tx lcommon.Transaction,
	txHash model.Hash32,
	order uint32,
) ([]model.Withdrawal, error) {
	type item struct {
		address        []byte
		value          *big.Int
		network        uint
		credentialKind *string
		credentialHash *model.Hash28
	}
	withdrawals := tx.Withdrawals()
	if len(withdrawals) > math.MaxUint32 {
		return nil, errors.New("withdrawal count exceeds UInt32")
	}
	items := make([]item, 0, len(withdrawals))
	for address, value := range withdrawals {
		if address == nil {
			return nil, errors.New("nil withdrawal reward account")
		}
		raw, err := address.Bytes()
		if err != nil {
			return nil, fmt.Errorf("withdrawal reward account: %w", err)
		}
		var kind *string
		var credentialHash *model.Hash28
		if credential, ok := address.StakeCredential(); ok {
			value, err := hash28(
				credential.Credential.Bytes(),
				"withdrawal credential",
			)
			if err == nil {
				switch credential.CredType {
				case lcommon.CredentialTypeAddrKeyHash:
					label := "key"
					kind = &label
					credentialHash = &value
				case lcommon.CredentialTypeScriptHash:
					label := "script"
					kind = &label
					credentialHash = &value
				}
			}
		}
		items = append(items, item{
			address:        raw,
			value:          value,
			network:        address.NetworkId(),
			credentialKind: kind,
			credentialHash: credentialHash,
		})
	}
	// Reward redeemer pointers use Map AccountAddress order, not reward-account
	// wire bytes. AccountAddress derives Ord by Network then Credential, whose
	// constructor order is ScriptHashObj before KeyHashObj, then hash bytes.
	// https://cardano-ledger.cardano.intersectmbo.org/cardano-ledger-core/Cardano-Ledger-Core.html
	// https://cardano-ledger.cardano.intersectmbo.org/cardano-ledger-alonzo/src/Cardano.Ledger.Alonzo.TxBody.html
	slices.SortFunc(items, func(a, b item) int {
		switch {
		case a.network < b.network:
			return -1
		case a.network > b.network:
			return 1
		}
		rank := func(kind string) int {
			if kind == "script" {
				return 0
			}
			return 1
		}
		if a.credentialKind == nil || a.credentialHash == nil ||
			b.credentialKind == nil || b.credentialHash == nil {
			return bytes.Compare(a.address, b.address)
		}
		if rankA, rankB := rank(*a.credentialKind), rank(*b.credentialKind); rankA != rankB {
			return rankA - rankB
		}
		return bytes.Compare(a.credentialHash[:], b.credentialHash[:])
	})
	ret := make([]model.Withdrawal, 0, len(items))
	for ordinal, value := range items {
		amount, err := uint64Value(value.value, "withdrawal lovelace")
		if err != nil {
			return nil, err
		}
		ret = append(ret, model.Withdrawal{
			TransactionHash:  txHash,
			TransactionOrder: order,
			BodyOrdinal:      uint32(ordinal),
			RewardAccount:    value.address,
			Lovelace:         amount,
			Applied:          tx.IsValid(),
			CredentialKind:   value.credentialKind,
			CredentialHash:   value.credentialHash,
		})
	}
	return ret, nil
}

func redeemerBundle(
	tx lcommon.Transaction,
	witnesses lcommon.TransactionWitnessSet,
	txHash model.Hash32,
	order uint32,
	regularInputs []lcommon.TransactionInput,
	withdrawals []model.Withdrawal,
) ([]model.Redeemer, error) {
	if witnesses == nil {
		return nil, nil
	}
	redeemers := witnesses.Redeemers()
	if redeemers == nil {
		return nil, nil
	}
	items, err := canonicalRedeemerItems(redeemers)
	if err != nil {
		return nil, err
	}
	if len(items) > math.MaxUint32 {
		return nil, errors.New("redeemer count exceeds UInt32")
	}
	ret := make([]model.Redeemer, 0, len(items))
	for _, item := range items {
		body := item.value.Data.Cbor()
		if len(body) == 0 {
			return nil, errors.New("redeemer data CBOR is missing")
		}
		if item.value.ExUnits.Memory < 0 || item.value.ExUnits.Steps < 0 {
			return nil, errors.New("negative redeemer execution units")
		}
		dataHash, err := hash32(lcommon.Blake2b256Hash(body).Bytes(), "redeemer data hash")
		if err != nil {
			return nil, err
		}
		row := model.Redeemer{
			TransactionHash:  txHash,
			TransactionOrder: order,
			RawPurposeTag:    uint8(item.key.Tag),
			Index:            item.key.Index,
			DataCBOR:         bytes.Clone(body),
			DataHash:         dataHash,
			ExUnitsMemory:    uint64(item.value.ExUnits.Memory),
			ExUnitsSteps:     uint64(item.value.ExUnits.Steps),
			Applied:          tx.IsValid(),
		}
		if err := resolveRedeemer(
			tx,
			item.key,
			&row,
			regularInputs,
			withdrawals,
		); err != nil {
			return nil, err
		}
		ret = append(ret, row)
	}
	return ret, nil
}

type redeemerItem struct {
	key   lcommon.RedeemerKey
	value lcommon.RedeemerValue
}

type legacyRedeemer struct {
	cbor.StructAsArray
	Tag     lcommon.RedeemerTag
	Index   uint32
	Data    lcommon.Datum
	ExUnits lcommon.ExUnits
}

func canonicalRedeemerItems(
	redeemers lcommon.TransactionWitnessRedeemers,
) ([]redeemerItem, error) {
	values := make(map[lcommon.RedeemerKey]lcommon.RedeemerValue)
	rawSource, hasRawSource := redeemers.(interface{ Cbor() []byte })
	raw := []byte(nil)
	if hasRawSource {
		raw = rawSource.Cbor()
	}
	if len(raw) > 0 && raw[0]&0xe0 == 0x80 {
		var encoded []legacyRedeemer
		consumed, err := cbor.Decode(raw, &encoded)
		if err != nil {
			return nil, fmt.Errorf("decode legacy redeemer array: %w", err)
		}
		if consumed != len(raw) {
			return nil, errors.New("legacy redeemer array has trailing CBOR")
		}
		// cardano-ledger interprets this historical array encoding through
		// Map.fromList. Preserve its last-wins semantics in original encoded
		// order; Iter sorts first and cannot safely resolve equal keys.
		for _, encodedRedeemer := range encoded {
			key := lcommon.RedeemerKey{
				Tag:   encodedRedeemer.Tag,
				Index: encodedRedeemer.Index,
			}
			values[key] = lcommon.RedeemerValue{
				Data:    encodedRedeemer.Data,
				ExUnits: encodedRedeemer.ExUnits,
			}
		}
	} else {
		if len(raw) > 0 && raw[0]&0xe0 != 0xa0 {
			return nil, fmt.Errorf(
				"redeemers have unsupported top-level CBOR type 0x%02x",
				raw[0],
			)
		}
		for key, value := range redeemers.Iter() {
			values[key] = value
		}
	}
	items := make([]redeemerItem, 0, len(values))
	for key, value := range values {
		items = append(items, redeemerItem{key: key, value: value})
	}
	slices.SortFunc(items, func(a, b redeemerItem) int {
		return lcommon.CompareRedeemerKeys(a.key, b.key)
	})
	return items, nil
}

func resolveRedeemer(
	tx lcommon.Transaction,
	key lcommon.RedeemerKey,
	row *model.Redeemer,
	regularInputs []lcommon.TransactionInput,
	withdrawals []model.Withdrawal,
) error {
	switch key.Tag {
	case lcommon.RedeemerTagSpend:
		row.Purpose = "spend"
		inputs, err := canonicalSpendInputs(regularInputs)
		if err != nil {
			return err
		}
		if int(key.Index) >= len(inputs) {
			return fmt.Errorf("spend redeemer index %d is unresolved", key.Index)
		}
		target := inputs[key.Index]
		row.TargetTxHash = &target.hash
		row.TargetOutputIndex = &target.index
	case lcommon.RedeemerTagMint:
		row.Purpose = "mint"
		if tx.AssetMint() == nil {
			return fmt.Errorf("mint redeemer index %d has no mint policies", key.Index)
		}
		policies := tx.AssetMint().Policies()
		slices.SortFunc(policies, func(a, b lcommon.Blake2b224) int {
			return bytes.Compare(a.Bytes(), b.Bytes())
		})
		if int(key.Index) >= len(policies) {
			return fmt.Errorf("mint redeemer index %d is unresolved", key.Index)
		}
		policy, err := hash28(policies[key.Index].Bytes(), "mint redeemer target")
		if err != nil {
			return err
		}
		row.TargetPolicyID = &policy
		row.ResolvedScriptHash = &policy
	case lcommon.RedeemerTagCert:
		row.Purpose = "certificate"
		if int(key.Index) >= len(tx.Certificates()) {
			return fmt.Errorf("certificate redeemer index %d is unresolved", key.Index)
		}
		certificate := tx.Certificates()[key.Index]
		certificateConstructor, err := canonicalLedgerConstructor(
			certificate,
			uint8(lcommon.CertificateTypeUpdateDrep),
		)
		if err != nil {
			return fmt.Errorf("certificate constructor: %w", err)
		}
		identity, err := compactLedgerTargetIdentity(
			uint8(lcommon.RedeemerTagCert),
			certificateConstructor,
			uint8(lcommon.CertificateTypeUpdateDrep),
			certificate,
		)
		if err != nil {
			return fmt.Errorf("certificate redeemer target identity: %w", err)
		}
		index := key.Index
		row.TargetBodyOrdinal = &index
		row.TargetIdentity = identity
		if hash, ok := certificateScriptHash(certificate); ok {
			row.ResolvedScriptHash = &hash
		}
	case lcommon.RedeemerTagReward:
		row.Purpose = "reward"
		if int(key.Index) >= len(withdrawals) {
			return fmt.Errorf("reward redeemer index %d is unresolved", key.Index)
		}
		target := withdrawals[key.Index]
		row.TargetRewardAccount = bytes.Clone(target.RewardAccount)
		if target.CredentialKind != nil &&
			*target.CredentialKind == "script" &&
			target.CredentialHash != nil {
			hash := *target.CredentialHash
			row.ResolvedScriptHash = &hash
		}
	case lcommon.RedeemerTagVoting:
		row.Purpose = "vote"
		voters, err := sortedVoters(tx.VotingProcedures())
		if err != nil {
			return err
		}
		if int(key.Index) >= len(voters) {
			return fmt.Errorf("vote redeemer index %d is unresolved", key.Index)
		}
		index := key.Index
		row.TargetBodyOrdinal = &index
		row.TargetIdentity = append([]byte{voters[key.Index].Type}, voters[key.Index].Hash[:]...)
		switch voters[key.Index].Type {
		case lcommon.VoterTypeConstitutionalCommitteeHotScriptHash, lcommon.VoterTypeDRepScriptHash:
			hash, _ := hash28(voters[key.Index].Hash[:], "voter script hash")
			row.ResolvedScriptHash = &hash
		}
	case lcommon.RedeemerTagProposing:
		row.Purpose = "proposal"
		if int(key.Index) >= len(tx.ProposalProcedures()) {
			return fmt.Errorf("proposal redeemer index %d is unresolved", key.Index)
		}
		proposal := tx.ProposalProcedures()[key.Index]
		actionConstructor, err := canonicalLedgerConstructor(
			proposal.GovAction(),
			uint8(lcommon.GovActionTypeInfo),
		)
		if err != nil {
			return fmt.Errorf("proposal governance action constructor: %w", err)
		}
		identity, err := compactLedgerTargetIdentity(
			uint8(lcommon.RedeemerTagProposing),
			actionConstructor,
			uint8(lcommon.GovActionTypeInfo),
			proposal,
		)
		if err != nil {
			return fmt.Errorf("proposal redeemer target identity: %w", err)
		}
		index := key.Index
		row.TargetBodyOrdinal = &index
		row.TargetIdentity = identity
		if action := proposal.GovAction(); action != nil {
			if withPolicy, ok := action.(lcommon.GovActionWithPolicy); ok {
				if raw := withPolicy.GetPolicyHash(); len(raw) > 0 {
					hash, err := hash28(raw, "proposal policy script hash")
					if err != nil {
						return err
					}
					row.ResolvedScriptHash = &hash
				}
			}
		}
	default:
		return fmt.Errorf("unsupported redeemer purpose tag %d", key.Tag)
	}
	return nil
}

func compactLedgerTargetIdentity(
	purpose uint8,
	constructor uint,
	maxConstructor uint8,
	value any,
) ([]byte, error) {
	canonical, err := cbor.Encode(value)
	if err != nil {
		return nil, fmt.Errorf("encode canonical ledger CBOR: %w", err)
	}
	if len(canonical) == 0 {
		return nil, errors.New("canonical ledger CBOR is missing")
	}
	if constructor > uint(maxConstructor) {
		return nil, fmt.Errorf(
			"canonical constructor %d outside 0..%d",
			constructor,
			maxConstructor,
		)
	}
	digest := lcommon.Blake2b256Hash(canonical)
	// Compact identity: [redeemer-purpose tag, target constructor, digest].
	ret := make([]byte, 2+len(digest))
	ret[0] = purpose
	ret[1] = uint8(constructor)
	copy(ret[2:], digest.Bytes())
	return ret, nil
}

func canonicalLedgerConstructor(value any, maximum uint8) (uint, error) {
	canonical, err := cbor.Encode(value)
	if err != nil {
		return 0, fmt.Errorf("encode canonical ledger CBOR: %w", err)
	}
	if len(canonical) == 0 {
		return 0, errors.New("canonical ledger CBOR is missing")
	}
	constructor, err := cbor.DecodeIdFromList(canonical)
	if err != nil {
		return 0, fmt.Errorf("decode canonical constructor: %w", err)
	}
	if constructor < 0 || constructor > int(maximum) {
		return 0, fmt.Errorf(
			"canonical constructor %d outside 0..%d",
			constructor,
			maximum,
		)
	}
	return uint(constructor), nil
}

type canonicalSpendInput struct {
	hash  model.Hash32
	index uint32
}

func canonicalSpendInputs(
	inputs []lcommon.TransactionInput,
) ([]canonicalSpendInput, error) {
	ret := make([]canonicalSpendInput, 0, len(inputs))
	for bodyOrdinal, input := range inputs {
		if input == nil {
			return nil, fmt.Errorf("regular input %d is nil", bodyOrdinal)
		}
		hash, err := hash32(input.Id().Bytes(), "spend redeemer input transaction hash")
		if err != nil {
			return nil, err
		}
		ret = append(ret, canonicalSpendInput{hash: hash, index: input.Index()})
	}
	slices.SortFunc(ret, func(left, right canonicalSpendInput) int {
		if compared := bytes.Compare(left.hash[:], right.hash[:]); compared != 0 {
			return compared
		}
		switch {
		case left.index < right.index:
			return -1
		case left.index > right.index:
			return 1
		default:
			return 0
		}
	})
	// Inputs are encoded as a ledger set. Preserve duplicate wire observations
	// in input facts, but collapse equal references only for transaction-local
	// redeemer pointer interpretation.
	if len(ret) > 1 {
		unique := ret[:1]
		for _, item := range ret[1:] {
			if item != unique[len(unique)-1] {
				unique = append(unique, item)
			}
		}
		ret = unique
	}
	return ret, nil
}

func sortedVoters(votes lcommon.VotingProcedures) ([]*lcommon.Voter, error) {
	var voters []*lcommon.Voter
	for voter := range votes {
		if voter == nil {
			return nil, errors.New("nil voter")
		}
		voters = append(voters, voter)
	}
	// The redeemer index is Map.elemAt over the ledger Voter Ord instance.
	// Voter constructor order is Committee, DRep, StakePool. Inside the first
	// two constructors Credential declares ScriptHashObj before KeyHashObj.
	// This is intentionally not the CDDL numeric tag order.
	// Official generated APIs:
	// https://cardano-api.cardano.intersectmbo.org/cardano-api/Cardano-Api-Ledger.html
	// https://cardano-ledger.cardano.intersectmbo.org/cardano-ledger-api/Cardano-Ledger-Api-Governance.html
	ledgerOrdinal := func(voter *lcommon.Voter) (int, error) {
		switch voter.Type {
		case lcommon.VoterTypeConstitutionalCommitteeHotScriptHash:
			return 0, nil
		case lcommon.VoterTypeConstitutionalCommitteeHotKeyHash:
			return 1, nil
		case lcommon.VoterTypeDRepScriptHash:
			return 2, nil
		case lcommon.VoterTypeDRepKeyHash:
			return 3, nil
		case lcommon.VoterTypeStakingPoolKeyHash:
			return 4, nil
		default:
			return 0, fmt.Errorf("unknown voter type %d", voter.Type)
		}
	}
	for _, voter := range voters {
		if _, err := ledgerOrdinal(voter); err != nil {
			return nil, err
		}
	}
	var sortErr error
	slices.SortFunc(voters, func(a, b *lcommon.Voter) int {
		aTag, aErr := ledgerOrdinal(a)
		bTag, bErr := ledgerOrdinal(b)
		if aErr != nil {
			sortErr = aErr
		}
		if bErr != nil {
			sortErr = bErr
		}
		if aTag != bTag {
			return aTag - bTag
		}
		return bytes.Compare(a.Hash[:], b.Hash[:])
	})
	return voters, sortErr
}

func metadataBundle(
	tx lcommon.Transaction,
	txHash model.Hash32,
	order uint32,
) (*model.Metadata, error) {
	metadata := tx.Metadata()
	if metadata == nil {
		return nil, nil
	}
	body := metadata.Cbor()
	if len(body) == 0 {
		return nil, errors.New("metadata map CBOR is missing")
	}
	var pairs []lcommon.MetaPair
	switch value := metadata.(type) {
	case lcommon.MetaMap:
		pairs = value.Pairs
	case *lcommon.MetaMap:
		pairs = value.Pairs
	default:
		return nil, fmt.Errorf("top-level transaction metadata has type %T, want map", metadata)
	}
	labels := make([]uint64, 0, len(pairs))
	for _, pair := range pairs {
		var number *big.Int
		switch value := pair.Key.(type) {
		case lcommon.MetaInt:
			number = value.Value
		case *lcommon.MetaInt:
			number = value.Value
		default:
			return nil, fmt.Errorf("metadata label has type %T, want integer", pair.Key)
		}
		if number == nil || number.Sign() < 0 || number.BitLen() > 64 {
			return nil, errors.New("metadata label does not fit UInt64")
		}
		labels = append(labels, number.Uint64())
	}
	slices.Sort(labels)
	contentHash, _ := hash32(lcommon.Blake2b256Hash(body).Bytes(), "metadata content hash")
	return &model.Metadata{
		TransactionHash:  txHash,
		TransactionOrder: order,
		Labels:           labels,
		CBOR:             bytes.Clone(body),
		ContentHash:      contentHash,
	}, nil
}

func scriptLanguage(script lcommon.Script) (string, error) {
	switch script.(type) {
	case lcommon.NativeScript, *lcommon.NativeScript:
		return "native", nil
	case lcommon.PlutusV1Script, *lcommon.PlutusV1Script:
		return "plutus_v1", nil
	case lcommon.PlutusV2Script, *lcommon.PlutusV2Script:
		return "plutus_v2", nil
	case lcommon.PlutusV3Script, *lcommon.PlutusV3Script:
		return "plutus_v3", nil
	case lcommon.PlutusV4Script, *lcommon.PlutusV4Script:
		return "plutus_v4", nil
	default:
		return "", fmt.Errorf("unknown reference script type %T", script)
	}
}

func certificateScriptHash(certificate lcommon.Certificate) (model.Hash28, bool) {
	var credential *lcommon.Credential
	switch value := certificate.(type) {
	case *lcommon.StakeRegistrationCertificate:
		credential = &value.StakeCredential
	case *lcommon.StakeDeregistrationCertificate:
		credential = &value.StakeCredential
	case *lcommon.RegistrationCertificate:
		credential = &value.StakeCredential
	case *lcommon.DeregistrationCertificate:
		credential = &value.StakeCredential
	case *lcommon.VoteDelegationCertificate:
		credential = &value.StakeCredential
	case *lcommon.VoteRegistrationDelegationCertificate:
		credential = &value.StakeCredential
	case *lcommon.StakeVoteDelegationCertificate:
		credential = &value.StakeCredential
	case *lcommon.StakeRegistrationDelegationCertificate:
		credential = &value.StakeCredential
	case *lcommon.StakeVoteRegistrationDelegationCertificate:
		credential = &value.StakeCredential
	case *lcommon.RegistrationDrepCertificate:
		credential = &value.DrepCredential
	case *lcommon.DeregistrationDrepCertificate:
		credential = &value.DrepCredential
	case *lcommon.UpdateDrepCertificate:
		credential = &value.DrepCredential
	case *lcommon.AuthCommitteeHotCertificate:
		credential = &value.ColdCredential
	case *lcommon.ResignCommitteeColdCertificate:
		credential = &value.ColdCredential
	case *lcommon.StakeDelegationCertificate:
		credential = value.StakeCredential
	}
	if credential == nil || credential.CredType != lcommon.CredentialTypeScriptHash {
		return model.Hash28{}, false
	}
	hash, err := hash28(credential.Credential.Bytes(), "certificate script hash")
	return hash, err == nil
}

func bundleDatumHash(datum *lcommon.Datum) (model.Hash32, error) {
	if datum == nil {
		return model.Hash32{}, errors.New("nil datum")
	}
	body := datum.Cbor()
	if len(body) == 0 {
		return model.Hash32{}, errors.New("datum CBOR is missing")
	}
	computed := lcommon.Blake2b256Hash(body)
	return hash32(computed.Bytes(), "datum hash")
}

func (collector bundleDatumCollector) add(
	datum lcommon.Datum,
	observation model.DatumObservation,
) (model.Hash32, error) {
	hash, err := bundleDatumHash(&datum)
	if err != nil {
		return model.Hash32{}, err
	}
	observation.Hash = hash
	body := datum.Cbor()
	for _, existing := range collector[hash] {
		if bytes.Equal(existing.CBOR, body) {
			existing.Observations = append(existing.Observations, observation)
			return hash, nil
		}
	}
	collector[hash] = append(collector[hash], &model.DatumBody{
		Hash:         hash,
		CBOR:         bytes.Clone(body),
		Observations: []model.DatumObservation{observation},
	})
	return hash, nil
}

func compareDatumObservation(a, b model.DatumObservation) int {
	if value := bytes.Compare(a.Hash[:], b.Hash[:]); value != 0 {
		return value
	}
	if value := bytes.Compare(a.TransactionHash[:], b.TransactionHash[:]); value != 0 {
		return value
	}
	if a.SourceKind != b.SourceKind {
		if a.SourceKind < b.SourceKind {
			return -1
		}
		return 1
	}
	if a.SourceOrdinal < b.SourceOrdinal {
		return -1
	}
	if a.SourceOrdinal > b.SourceOrdinal {
		return 1
	}
	return 0
}

func hash32(value []byte, field string) (model.Hash32, error) {
	if len(value) != 32 {
		return model.Hash32{}, fmt.Errorf("%s has %d bytes, want 32", field, len(value))
	}
	return model.Hash32(value), nil
}

func hash28(value []byte, field string) (model.Hash28, error) {
	if len(value) != 28 {
		return model.Hash28{}, fmt.Errorf("%s has %d bytes, want 28", field, len(value))
	}
	return model.Hash28(value), nil
}

func uint64Value(value *big.Int, field string) (uint64, error) {
	if value == nil {
		return 0, fmt.Errorf("%s is missing", field)
	}
	if value.Sign() < 0 || value.BitLen() > 64 {
		return 0, fmt.Errorf("%s does not fit UInt64", field)
	}
	return value.Uint64(), nil
}
