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

	"clicksync/internal/model"
)

const maxContextCBORSize = 2 * 1024 * 1024

type bundleDatumCollector map[model.Hash32]*model.DatumBody

// Bundle projects a verified decoded block into the sole native-writer fact
// model. No full block, transaction, witness, auxiliary-data, or script CBOR
// leaves this function.
func Bundle(block lcommon.Block) (model.Block, error) {
	if block == nil {
		return model.Block{}, errors.New("nil block")
	}
	if block.Type() < math.MinInt16 || block.Type() > math.MaxInt16 {
		return model.Block{}, fmt.Errorf("block type %d exceeds Int16", block.Type())
	}
	blockHash, err := hash32(block.Hash().Bytes(), "block hash")
	if err != nil {
		return model.Block{}, err
	}
	parentHash, err := hash32(block.PrevHash().Bytes(), "parent hash")
	if err != nil {
		return model.Block{}, err
	}
	ret := model.Block{
		Hash:                   blockHash,
		ParentHash:             &parentHash,
		Slot:                   block.SlotNumber(),
		Number:                 block.BlockNumber(),
		Era:                    block.Era().Name,
		Type:                   int16(block.Type()),
		BodyHashVerified:       true,
		TransactionIDsVerified: true,
		Transactions:           make([]model.Transaction, 0, len(block.Transactions())),
	}
	datums := bundleDatumCollector{}
	for order, tx := range block.Transactions() {
		if tx == nil {
			return model.Block{}, fmt.Errorf("transaction %d is nil", order)
		}
		if order > math.MaxUint32 {
			return model.Block{}, errors.New("transaction order exceeds UInt32")
		}
		if err := rejectUnsupportedTransaction(tx); err != nil {
			return model.Block{}, fmt.Errorf("transaction %d: %w", order, err)
		}
		facts, err := transactionBundle(tx, uint32(order), block.Era().Name, datums)
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
		body := datums[hash]
		slices.SortFunc(body.Observations, compareDatumObservation)
		ret.Datums = append(ret.Datums, *body)
	}
	return ret, nil
}

func transactionBundle(
	tx lcommon.Transaction,
	order uint32,
	era string,
	datums bundleDatumCollector,
) (model.Transaction, error) {
	bodyCBOR, err := transactionBodyCBOR(tx)
	if err != nil {
		return model.Transaction{}, err
	}
	if len(bodyCBOR) == 0 {
		return model.Transaction{}, errors.New("missing exact transaction-body CBOR")
	}
	if err := verifyTransactionID(tx.Hash(), bodyCBOR); err != nil {
		return model.Transaction{}, err
	}
	txHash, err := hash32(tx.Hash().Bytes(), "transaction hash")
	if err != nil {
		return model.Transaction{}, err
	}
	valid := tx.IsValid()
	ret := model.Transaction{
		Hash:        txHash,
		Order:       order,
		Era:         era,
		Phase2Valid: valid,
		FlowKind:    "regular",
		MintApplied: valid,
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
	if err := appendInputs(&ret, tx.Inputs(), "regular", valid); err != nil {
		return model.Transaction{}, err
	}
	if err := appendInputs(&ret, tx.Collateral(), "collateral", !valid); err != nil {
		return model.Transaction{}, err
	}
	if err := appendInputs(&ret, tx.ReferenceInputs(), "reference", false); err != nil {
		return model.Transaction{}, err
	}
	if err := rejectSpendingReferenceOverlap(ret.Inputs); err != nil {
		return model.Transaction{}, err
	}
	if valid {
		for index, output := range tx.Outputs() {
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
		if len(tx.Outputs()) > math.MaxUint32 {
			return model.Transaction{}, errors.New("collateral return index exceeds UInt32")
		}
		facts, observations, err := outputBundle(
			txHash,
			order,
			uint32(len(tx.Outputs())),
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
	ret.Redeemers, err = redeemerBundle(tx, txHash, order)
	if err != nil {
		return model.Transaction{}, err
	}
	ret.Metadata, err = metadataBundle(tx, txHash, order)
	if err != nil {
		return model.Transaction{}, err
	}
	if witnesses := tx.Witnesses(); witnesses != nil {
		for ordinal, datum := range witnesses.PlutusData() {
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
	type inputRef struct {
		hash  model.Hash32
		index uint32
	}
	seen := make(map[inputRef]struct{}, len(inputs))
	for ordinal, input := range inputs {
		if input == nil {
			return fmt.Errorf("%s input %d is nil", role, ordinal)
		}
		sourceHash, err := hash32(input.Id().Bytes(), "source transaction hash")
		if err != nil {
			return err
		}
		ref := inputRef{hash: sourceHash, index: input.Index()}
		if _, duplicate := seen[ref]; duplicate {
			return fmt.Errorf(
				"duplicate %s input reference %x#%d",
				role,
				sourceHash,
				input.Index(),
			)
		}
		seen[ref] = struct{}{}
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

func rejectSpendingReferenceOverlap(inputs []model.Input) error {
	// Conway's UTXO rule rejects BabbageNonDisjointRefInputs:
	// https://cardano-ledger.cardano.intersectmbo.org/cardano-ledger-conway/src/Cardano.Ledger.Conway.Rules.Utxo.html
	// Do not extend this to collateral overlaps, which Alonzo UTxO permits:
	// https://cardano-ledger.cardano.intersectmbo.org/cardano-ledger-alonzo/Cardano-Ledger-Alonzo-UTxO.html
	type inputRef struct {
		hash  model.Hash32
		index uint32
	}
	spending := make(map[inputRef]struct{})
	for _, input := range inputs {
		if input.Role == "regular" {
			spending[inputRef{hash: input.SourceHash, index: input.SourceIndex}] = struct{}{}
		}
	}
	for _, input := range inputs {
		ref := inputRef{hash: input.SourceHash, index: input.SourceIndex}
		if input.Role == "reference" {
			if _, overlaps := spending[ref]; overlaps {
				return fmt.Errorf(
					"regular input reference %x#%d also appears as reference input",
					input.SourceHash,
					input.SourceIndex,
				)
			}
		}
	}
	return nil
}

func paymentCredentialFromAddress(
	address []byte,
	era string,
) (string, *model.Hash28, error) {
	if len(address) == 0 {
		return "", nil, errors.New("empty address")
	}
	decoded, err := lcommon.NewAddressFromBytes(address)
	if err != nil {
		return "", nil, fmt.Errorf("decode address: %w", err)
	}
	addressType := decoded.Type()
	if addressType != lcommon.AddressTypeByron &&
		decoded.NetworkId() != lcommon.AddressNetworkMainnet {
		return "", nil, fmt.Errorf(
			"Shelley-family output address network ID %d is not pinned mainnet",
			decoded.NetworkId(),
		)
	}
	switch addressType {
	case lcommon.AddressTypeByron:
		return "none", nil, nil
	case lcommon.AddressTypeNoneKey,
		lcommon.AddressTypeNoneScript:
		return "", nil, fmt.Errorf(
			"reward-account address type %d is not a transaction output address",
			addressType,
		)
	case lcommon.AddressTypeKeyKey,
		lcommon.AddressTypeKeyScript,
		lcommon.AddressTypeScriptKey,
		lcommon.AddressTypeScriptScript:
		if eraUsesStrictAddresses(era) &&
			len(address) != 1+2*lcommon.AddressHashSize {
			return "", nil, fmt.Errorf(
				"base address type %d has length %d, want %d",
				addressType,
				len(address),
				1+2*lcommon.AddressHashSize,
			)
		}
	case lcommon.AddressTypeKeyNone,
		lcommon.AddressTypeScriptNone:
		if eraUsesStrictAddresses(era) &&
			len(address) != 1+lcommon.AddressHashSize {
			return "", nil, fmt.Errorf(
				"enterprise address type %d has length %d, want %d",
				addressType,
				len(address),
				1+lcommon.AddressHashSize,
			)
		}
	case lcommon.AddressTypeKeyPointer,
		lcommon.AddressTypeScriptPointer:
		if err := validatePointerAddressSuffix(
			address[1+lcommon.AddressHashSize:],
			!eraUsesStrictAddresses(era),
		); err != nil {
			return "", nil, err
		}
	default:
		return "", nil, fmt.Errorf("unknown address type %d", addressType)
	}
	paymentHash := decoded.PaymentKeyHash()
	hash, err := hash28(
		paymentHash.Bytes(),
		"payment credential",
	)
	if err != nil {
		return "", nil, err
	}
	kind := "key"
	switch addressType {
	case lcommon.AddressTypeScriptKey,
		lcommon.AddressTypeScriptScript,
		lcommon.AddressTypeScriptPointer,
		lcommon.AddressTypeScriptNone:
		kind = "script"
	}
	return kind, &hash, nil
}

func eraUsesStrictAddresses(era string) bool {
	switch era {
	case "Shelley", "Allegra", "Mary", "Alonzo":
		return false
	default:
		return true
	}
}

func validatePointerAddressSuffix(suffix []byte, allowTrailing bool) error {
	offset := 0
	limits := [...]uint64{math.MaxUint32, math.MaxUint16, math.MaxUint16}
	for component, limit := range limits {
		start := offset
		var accumulated uint64
		for {
			if offset >= len(suffix) {
				return fmt.Errorf("pointer address component %d is truncated", component)
			}
			value := suffix[offset]
			offset++
			digit := uint64(value & 0x7f)
			if !allowTrailing &&
				offset == start+1 &&
				value&0x80 != 0 &&
				digit == 0 {
				return fmt.Errorf(
					"pointer address component %d has a non-canonical leading zero",
					component,
				)
			}
			if accumulated > (limit-digit)>>7 {
				return fmt.Errorf(
					"pointer address component %d exceeds ledger bound %d",
					component,
					limit,
				)
			}
			accumulated = accumulated<<7 | digit
			if value&0x80 == 0 {
				break
			}
		}
	}
	if !allowTrailing && offset != len(suffix) {
		return fmt.Errorf("pointer address has %d trailing bytes", len(suffix)-offset)
	}
	return nil
}

func outputBundle(
	txHash model.Hash32,
	txOrder uint32,
	index uint32,
	ordinal uint32,
	kind string,
	era string,
	output lcommon.TransactionOutput,
	datums bundleDatumCollector,
) (model.Output, []model.DatumObservation, error) {
	if output == nil {
		return model.Output{}, nil, errors.New("nil output")
	}
	address, err := output.Address().Bytes()
	if err != nil {
		return model.Output{}, nil, fmt.Errorf("address bytes: %w", err)
	}
	if len(address) == 0 || len(address) > maxAddressSize {
		return model.Output{}, nil, fmt.Errorf("address length %d outside 1..%d", len(address), maxAddressSize)
	}
	lovelace, err := uint64Value(output.Amount(), "output lovelace")
	if err != nil {
		return model.Output{}, nil, err
	}
	assets, err := assetBundle(output.Assets())
	if err != nil {
		return model.Output{}, nil, err
	}
	paymentKind, paymentHash, err := paymentCredentialFromAddress(address, era)
	if err != nil {
		return model.Output{}, nil, fmt.Errorf("payment credential: %w", err)
	}
	ret := model.Output{
		TransactionHash:       txHash,
		TransactionOrder:      txOrder,
		Index:                 index,
		BodyOrdinal:           ordinal,
		Kind:                  kind,
		Address:               bytes.Clone(address),
		PaymentCredentialKind: paymentKind,
		PaymentCredentialHash: paymentHash,
		Lovelace:              lovelace,
		Assets:                assets,
		DatumKind:             "none",
	}
	var observations []model.DatumObservation
	if script := output.ScriptRef(); script != nil {
		hash, err := hash28(script.Hash().Bytes(), "reference script hash")
		if err != nil {
			return model.Output{}, nil, err
		}
		language, err := scriptLanguage(script)
		if err != nil {
			return model.Output{}, nil, err
		}
		ret.ReferenceScriptHash = &hash
		ret.ReferenceScriptLanguage = &language
	}
	datum := output.Datum()
	decodedHash := output.DatumHash()
	switch {
	case datum != nil:
		hash, err := verifiedBundleDatum(datum)
		if err != nil {
			return model.Output{}, nil, err
		}
		if decodedHash != nil && !bytes.Equal(decodedHash.Bytes(), hash[:]) {
			return model.Output{}, nil, errors.New("inline datum hash mismatch")
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
			if len(name) > maxAssetNameSize {
				return nil, fmt.Errorf("asset name is %d bytes", len(name))
			}
			quantity := assets.Asset(policy, name)
			value, err := uint64Value(quantity, "output asset quantity")
			if err != nil {
				return nil, err
			}
			if value == 0 {
				continue
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
			if len(name) > maxAssetNameSize {
				return nil, fmt.Errorf("mint asset name is %d bytes", len(name))
			}
			quantity := mint.Asset(policy, name)
			if quantity == nil || !quantity.IsInt64() {
				return nil, fmt.Errorf("mint quantity does not fit Int64")
			}
			if quantity.Sign() == 0 {
				continue
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
		credentialKind string
		credentialHash model.Hash28
	}
	var items []item
	for address, value := range tx.Withdrawals() {
		if address == nil {
			return nil, errors.New("nil withdrawal reward account")
		}
		raw, err := address.Bytes()
		if err != nil {
			return nil, fmt.Errorf("withdrawal reward account: %w", err)
		}
		if address.Type() != lcommon.AddressTypeNoneKey &&
			address.Type() != lcommon.AddressTypeNoneScript {
			return nil, fmt.Errorf(
				"withdrawal address type %d is not a reward account",
				address.Type(),
			)
		}
		if address.NetworkId() != lcommon.AddressNetworkMainnet {
			return nil, fmt.Errorf(
				"withdrawal reward account network ID %d is not pinned mainnet",
				address.NetworkId(),
			)
		}
		credential, ok := address.StakeCredential()
		if !ok {
			return nil, errors.New("withdrawal reward account has no stake credential")
		}
		credentialHash, err := hash28(
			credential.Credential.Bytes(),
			"withdrawal credential",
		)
		if err != nil {
			return nil, err
		}
		kind := "key"
		switch credential.CredType {
		case lcommon.CredentialTypeAddrKeyHash:
		case lcommon.CredentialTypeScriptHash:
			kind = "script"
		default:
			return nil, fmt.Errorf("unknown withdrawal credential type %d", credential.CredType)
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
		if rankA, rankB := rank(a.credentialKind), rank(b.credentialKind); rankA != rankB {
			return rankA - rankB
		}
		return bytes.Compare(a.credentialHash[:], b.credentialHash[:])
	})
	for index := 1; index < len(items); index++ {
		if items[index].network == items[index-1].network &&
			items[index].credentialKind == items[index-1].credentialKind &&
			items[index].credentialHash == items[index-1].credentialHash {
			return nil, fmt.Errorf(
				"duplicate withdrawal reward account %x",
				items[index].address,
			)
		}
	}
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
			RewardAccount:    bytes.Clone(value.address),
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
	txHash model.Hash32,
	order uint32,
) ([]model.Redeemer, error) {
	witnesses := tx.Witnesses()
	if witnesses == nil || witnesses.Redeemers() == nil {
		return nil, nil
	}
	type item struct {
		key   lcommon.RedeemerKey
		value lcommon.RedeemerValue
	}
	var items []item
	for key, value := range witnesses.Redeemers().Iter() {
		items = append(items, item{key: key, value: value})
	}
	slices.SortFunc(items, func(a, b item) int {
		return lcommon.CompareRedeemerKeys(a.key, b.key)
	})
	ret := make([]model.Redeemer, 0, len(items))
	for _, item := range items {
		body := item.value.Data.Cbor()
		if len(body) == 0 || len(body) > maxContextCBORSize {
			return nil, fmt.Errorf("redeemer data length %d outside 1..%d", len(body), maxContextCBORSize)
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
		if err := resolveRedeemer(tx, item.key, &row); err != nil {
			return nil, err
		}
		ret = append(ret, row)
	}
	return ret, nil
}

func resolveRedeemer(tx lcommon.Transaction, key lcommon.RedeemerKey, row *model.Redeemer) error {
	switch key.Tag {
	case lcommon.RedeemerTagSpend:
		row.Purpose = "spend"
		inputs, err := canonicalSpendInputs(tx.Inputs())
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
		if certificateConstructor != certificate.Type() {
			return fmt.Errorf(
				"certificate constructor %d disagrees with decoded type %d",
				certificateConstructor,
				certificate.Type(),
			)
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
		withdrawals, err := withdrawalBundle(tx, row.TransactionHash, row.TransactionOrder)
		if err != nil {
			return err
		}
		if int(key.Index) >= len(withdrawals) {
			return fmt.Errorf("reward redeemer index %d is unresolved", key.Index)
		}
		target := withdrawals[key.Index]
		row.TargetRewardAccount = bytes.Clone(target.RewardAccount)
		if target.CredentialKind == "script" {
			hash := target.CredentialHash
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
	if len(canonical) == 0 || len(canonical) > maxContextCBORSize {
		return nil, fmt.Errorf(
			"canonical ledger CBOR length %d outside 1..%d",
			len(canonical),
			maxContextCBORSize,
		)
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
	if len(canonical) == 0 || len(canonical) > maxContextCBORSize {
		return 0, fmt.Errorf(
			"canonical ledger CBOR length %d outside 1..%d",
			len(canonical),
			maxContextCBORSize,
		)
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
	for index := 1; index < len(ret); index++ {
		if ret[index] == ret[index-1] {
			return nil, fmt.Errorf(
				"duplicate regular input reference %x#%d",
				ret[index].hash,
				ret[index].index,
			)
		}
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
	if len(body) == 0 || len(body) > maxContextCBORSize {
		return nil, fmt.Errorf("metadata map length %d outside 1..%d", len(body), maxContextCBORSize)
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
	for index := 1; index < len(labels); index++ {
		if labels[index] == labels[index-1] {
			return nil, fmt.Errorf("duplicate metadata label %d", labels[index])
		}
	}
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

func verifiedBundleDatum(datum *lcommon.Datum) (model.Hash32, error) {
	if datum == nil {
		return model.Hash32{}, errors.New("nil datum")
	}
	body := datum.Cbor()
	if len(body) == 0 || len(body) > maxContextCBORSize {
		return model.Hash32{}, fmt.Errorf("datum length %d outside 1..%d", len(body), maxContextCBORSize)
	}
	computed := lcommon.Blake2b256Hash(body)
	if computed != datum.Hash() {
		return model.Hash32{}, errors.New("datum hash mismatch")
	}
	return hash32(computed.Bytes(), "datum hash")
}

func (collector bundleDatumCollector) add(
	datum lcommon.Datum,
	observation model.DatumObservation,
) (model.Hash32, error) {
	hash, err := verifiedBundleDatum(&datum)
	if err != nil {
		return model.Hash32{}, err
	}
	observation.Hash = hash
	if existing := collector[hash]; existing != nil {
		if !bytes.Equal(existing.CBOR, datum.Cbor()) {
			return model.Hash32{}, fmt.Errorf("conflicting bodies for datum %x", hash)
		}
		existing.Observations = append(existing.Observations, observation)
		return hash, nil
	}
	collector[hash] = &model.DatumBody{
		Hash:         hash,
		CBOR:         bytes.Clone(datum.Cbor()),
		Observations: []model.DatumObservation{observation},
	}
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
