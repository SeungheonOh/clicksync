// Package normalize projects decoded Cardano blocks into the narrow set of
// ledger-effective facts required for UTxO-flow analysis.
package normalize

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"math/big"
	"slices"

	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

const (
	maxAddressSize   = 256
	maxAssetNameSize = 32
	maxDatumSize     = 2 * 1024 * 1024
)

type datumCollector map[string]*Datum

func BlockFacts(block lcommon.Block) (Block, error) {
	if block == nil {
		return Block{}, errors.New("nil block")
	}
	txs := block.Transactions()
	if len(txs) > math.MaxUint32 {
		return Block{}, fmt.Errorf("transaction count %d exceeds uint32", len(txs))
	}
	ret := Block{
		Era:              block.Era().Name,
		BlockType:        block.Type(),
		BlockHash:        block.Hash().String(),
		ParentHash:       block.PrevHash().String(),
		Slot:             block.SlotNumber(),
		BlockNumber:      block.BlockNumber(),
		TransactionCount: len(txs),
		Transactions:     make([]Transaction, 0, len(txs)),
		Datums:           []Datum{},
	}
	datums := datumCollector{}
	for txIdx, tx := range txs {
		if tx == nil {
			return Block{}, fmt.Errorf("transaction %d is nil", txIdx)
		}
		if err := rejectUnsupportedTransaction(tx); err != nil {
			return Block{}, fmt.Errorf("transaction %d: %w", txIdx, err)
		}
		normalized, err := transactionFacts(tx, uint32(txIdx), datums)
		if err != nil {
			return Block{}, fmt.Errorf("transaction %d: %w", txIdx, err)
		}
		ret.Transactions = append(ret.Transactions, normalized)
	}
	datumHashes := make([]string, 0, len(datums))
	for hash := range datums {
		datumHashes = append(datumHashes, hash)
	}
	slices.Sort(datumHashes)
	for _, hash := range datumHashes {
		datum := datums[hash]
		slices.SortFunc(datum.Sources, func(a, b DatumObservation) int {
			if cmp := a.TxHash + ":" + a.Kind; cmp < b.TxHash+":"+b.Kind {
				return -1
			} else if cmp > b.TxHash+":"+b.Kind {
				return 1
			}
			return 0
		})
		ret.Datums = append(ret.Datums, *datum)
	}
	return ret, nil
}

func rejectUnsupportedTransaction(tx lcommon.Transaction) error {
	switch tx.Type() {
	case ledger.TxTypeByron,
		ledger.TxTypeShelley,
		ledger.TxTypeAllegra,
		ledger.TxTypeMary,
		ledger.TxTypeAlonzo,
		ledger.TxTypeBabbage,
		ledger.TxTypeConway:
		return nil
	case ledger.TxTypeDijkstra:
		dijkstraTx, ok := tx.(*ledger.DijkstraTransaction)
		if !ok {
			return fmt.Errorf("Dijkstra transaction has unexpected concrete type %T", tx)
		}
		if count := len(dijkstraTx.Body.TxSubTransactions.Items()); count > 0 {
			return fmt.Errorf(
				"unsupported Dijkstra nested transaction semantics (%d sub-transactions)",
				count,
			)
		}
		return nil
	default:
		return fmt.Errorf("unsupported transaction type %d", tx.Type())
	}
}

func transactionFacts(
	tx lcommon.Transaction,
	order uint32,
	datums datumCollector,
) (Transaction, error) {
	bodyCBOR, err := transactionBodyCBOR(tx)
	if err != nil {
		return Transaction{}, err
	}
	if len(bodyCBOR) == 0 {
		return Transaction{}, errors.New("missing transaction-body CBOR for ID verification")
	}
	txID := tx.Hash()
	if err := verifyTransactionID(txID, bodyCBOR); err != nil {
		return Transaction{}, err
	}
	txHash := txID.String()
	valid := tx.IsValid()
	flowKind := "regular"
	effectiveInputs := tx.Inputs()
	effectiveOutputs := tx.Outputs()
	outputStartIndex := uint32(0)
	if !valid {
		flowKind = "collateral"
		effectiveInputs = tx.Collateral()
		effectiveOutputs = nil
		if collateralReturn := tx.CollateralReturn(); collateralReturn != nil {
			if len(tx.Outputs()) > math.MaxUint32 {
				return Transaction{}, errors.New("collateral-return index exceeds uint32")
			}
			effectiveOutputs = []lcommon.TransactionOutput{collateralReturn}
			outputStartIndex = uint32(len(tx.Outputs()))
		}
	}
	if len(effectiveInputs) > math.MaxUint32 || len(effectiveOutputs) > math.MaxUint32 {
		return Transaction{}, errors.New("effective input/output count exceeds uint32")
	}

	ret := Transaction{
		TxHash:      txHash,
		TxOrder:     order,
		Valid:       valid,
		FlowKind:    flowKind,
		FeeLovelace: nil,
		FeeKnown:    false,
		Inputs:      make([]Input, 0, len(effectiveInputs)),
		Outputs:     make([]Output, 0, len(effectiveOutputs)),
		Mint:        []AssetDelta{},
	}
	for idx, input := range effectiveInputs {
		if input == nil {
			return Transaction{}, fmt.Errorf("effective input %d is nil", idx)
		}
		ret.Inputs = append(ret.Inputs, Input{
			SourceTxHash: input.Id().String(),
			SourceIndex:  input.Index(),
			Ordinal:      uint32(idx),
			Kind:         flowKind,
		})
	}
	for idx, output := range effectiveOutputs {
		outputIndex := outputStartIndex + uint32(idx)
		normalized, err := outputFacts(txHash, outputIndex, uint32(idx), flowKind, output, datums)
		if err != nil {
			return Transaction{}, fmt.Errorf("effective output %d: %w", idx, err)
		}
		ret.Outputs = append(ret.Outputs, normalized)
	}

	if valid {
		if tx.Type() != ledger.TxTypeByron {
			fee, err := nonnegativeUint64(tx.Fee(), "fee")
			if err != nil {
				return Transaction{}, err
			}
			value := fee.String()
			ret.FeeLovelace = &value
			ret.FeeKnown = true
		}
		mint, err := mintFacts(tx.AssetMint())
		if err != nil {
			return Transaction{}, err
		}
		ret.Mint = mint
	} else if totalCollateral := tx.TotalCollateral(); totalCollateral != nil &&
		totalCollateral.Sign() > 0 {
		fee, err := nonnegativeUint64(totalCollateral, "total collateral")
		if err != nil {
			return Transaction{}, err
		}
		value := fee.String()
		ret.FeeLovelace = &value
		ret.FeeKnown = true
	}

	if witnesses := tx.Witnesses(); witnesses != nil {
		for idx := range witnesses.PlutusData() {
			datum := witnesses.PlutusData()[idx]
			if err := datums.add(&datum, DatumObservation{
				Kind:   "witness",
				TxHash: txHash,
			}); err != nil {
				return Transaction{}, fmt.Errorf("witness datum %d: %w", idx, err)
			}
		}
	}
	return ret, nil
}

func verifyTransactionID(decoded lcommon.Blake2b256, bodyCBOR []byte) error {
	computed := lcommon.Blake2b256Hash(bodyCBOR)
	if computed != decoded {
		return fmt.Errorf(
			"transaction ID mismatch: decoded %s, computed %s",
			decoded,
			computed,
		)
	}
	return nil
}

func transactionBodyCBOR(tx lcommon.Transaction) ([]byte, error) {
	switch value := tx.(type) {
	case *ledger.ByronTransaction:
		return value.Body.Cbor(), nil
	case *ledger.ShelleyTransaction:
		return value.Body.Cbor(), nil
	case *ledger.AllegraTransaction:
		return value.Body.Cbor(), nil
	case *ledger.MaryTransaction:
		return value.Body.Cbor(), nil
	case *ledger.AlonzoTransaction:
		return value.Body.Cbor(), nil
	case *ledger.BabbageTransaction:
		return value.Body.Cbor(), nil
	case *ledger.ConwayTransaction:
		return value.Body.Cbor(), nil
	case *ledger.DijkstraTransaction:
		return value.Body.Cbor(), nil
	default:
		return nil, fmt.Errorf("unsupported concrete transaction type %T", tx)
	}
}

func outputFacts(
	txHash string,
	outputIndex uint32,
	ordinal uint32,
	kind string,
	output lcommon.TransactionOutput,
	datums datumCollector,
) (Output, error) {
	if output == nil {
		return Output{}, errors.New("nil output")
	}
	address, err := output.Address().Bytes()
	if err != nil {
		return Output{}, fmt.Errorf("address bytes: %w", err)
	}
	if len(address) == 0 || len(address) > maxAddressSize {
		return Output{}, fmt.Errorf(
			"address length %d is outside 1..%d bytes",
			len(address),
			maxAddressSize,
		)
	}
	lovelace, err := nonnegativeUint64(output.Amount(), "output lovelace")
	if err != nil {
		return Output{}, err
	}
	assets, err := outputAssets(output.Assets())
	if err != nil {
		return Output{}, err
	}
	ret := Output{
		TxHash:      txHash,
		OutputIndex: outputIndex,
		Ordinal:     ordinal,
		Kind:        kind,
		AddressHex:  hex.EncodeToString(address),
		Lovelace:    lovelace.String(),
		Assets:      assets,
		DatumKind:   "none",
	}
	datum := output.Datum()
	datumHash := output.DatumHash()
	switch {
	case datum != nil:
		computedHash, err := verifiedDatum(datum)
		if err != nil {
			return Output{}, err
		}
		if datumHash != nil && *datumHash != computedHash {
			return Output{}, errors.New("inline datum hash mismatch")
		}
		value := computedHash.String()
		ret.DatumKind = "inline"
		ret.DatumHash = &value
		if err := datums.add(datum, DatumObservation{
			Kind:   "inline",
			TxHash: txHash,
		}); err != nil {
			return Output{}, err
		}
	case datumHash != nil:
		value := datumHash.String()
		ret.DatumKind = "hash"
		ret.DatumHash = &value
	}
	return ret, nil
}

func outputAssets(
	assets *lcommon.MultiAsset[lcommon.MultiAssetTypeOutput],
) ([]Asset, error) {
	if assets == nil {
		return []Asset{}, nil
	}
	type keyed struct {
		policy lcommon.Blake2b224
		name   []byte
		amount *big.Int
	}
	var values []keyed
	for _, policy := range assets.Policies() {
		for _, name := range assets.Assets(policy) {
			if len(name) > maxAssetNameSize {
				return nil, fmt.Errorf("asset name is %d bytes, limit is %d", len(name), maxAssetNameSize)
			}
			amount := assets.Asset(policy, name)
			if amount == nil {
				return nil, errors.New("nil output asset quantity")
			}
			if amount.Sign() < 0 || amount.BitLen() > 64 {
				return nil, fmt.Errorf("output asset quantity %s does not fit UInt64", amount)
			}
			if amount.Sign() == 0 {
				continue
			}
			values = append(values, keyed{policy: policy, name: name, amount: amount})
		}
	}
	slices.SortFunc(values, func(a, b keyed) int {
		if cmp := bytes.Compare(a.policy.Bytes(), b.policy.Bytes()); cmp != 0 {
			return cmp
		}
		return bytes.Compare(a.name, b.name)
	})
	ret := make([]Asset, 0, len(values))
	for _, value := range values {
		ret = append(ret, Asset{
			PolicyID:     value.policy.String(),
			AssetNameHex: hex.EncodeToString(value.name),
			Quantity:     value.amount.String(),
		})
	}
	return ret, nil
}

func mintFacts(
	mint *lcommon.MultiAsset[lcommon.MultiAssetTypeMint],
) ([]AssetDelta, error) {
	if mint == nil {
		return []AssetDelta{}, nil
	}
	type keyed struct {
		policy lcommon.Blake2b224
		name   []byte
		amount *big.Int
	}
	var values []keyed
	for _, policy := range mint.Policies() {
		for _, name := range mint.Assets(policy) {
			if len(name) > maxAssetNameSize {
				return nil, fmt.Errorf("mint asset name is %d bytes, limit is %d", len(name), maxAssetNameSize)
			}
			amount := mint.Asset(policy, name)
			if amount == nil {
				return nil, errors.New("nil mint quantity")
			}
			if amount.Sign() == 0 {
				continue
			}
			if !amount.IsInt64() {
				return nil, fmt.Errorf("mint quantity %s does not fit Int64", amount)
			}
			values = append(values, keyed{policy: policy, name: name, amount: amount})
		}
	}
	slices.SortFunc(values, func(a, b keyed) int {
		if cmp := bytes.Compare(a.policy.Bytes(), b.policy.Bytes()); cmp != 0 {
			return cmp
		}
		return bytes.Compare(a.name, b.name)
	})
	ret := make([]AssetDelta, 0, len(values))
	for _, value := range values {
		ret = append(ret, AssetDelta{
			PolicyID:     value.policy.String(),
			AssetNameHex: hex.EncodeToString(value.name),
			Quantity:     value.amount.String(),
		})
	}
	return ret, nil
}

func nonnegativeUint64(value *big.Int, field string) (*big.Int, error) {
	if value == nil {
		return nil, fmt.Errorf("%s is missing", field)
	}
	if value.Sign() < 0 || value.BitLen() > 64 {
		return nil, fmt.Errorf("%s %s does not fit UInt64", field, value)
	}
	return new(big.Int).Set(value), nil
}

func verifiedDatum(datum *lcommon.Datum) (lcommon.DatumHash, error) {
	if datum == nil {
		return lcommon.DatumHash{}, errors.New("nil datum")
	}
	body := datum.Cbor()
	if len(body) == 0 {
		return lcommon.DatumHash{}, errors.New("datum has no exact CBOR")
	}
	if len(body) > maxDatumSize {
		return lcommon.DatumHash{}, fmt.Errorf("datum is %d bytes, limit is %d", len(body), maxDatumSize)
	}
	computed := lcommon.Blake2b256Hash(body)
	if datum.Hash() != computed {
		return lcommon.DatumHash{}, errors.New("datum decoder hash mismatch")
	}
	return computed, nil
}

func (d datumCollector) add(datum *lcommon.Datum, source DatumObservation) error {
	hash, err := verifiedDatum(datum)
	if err != nil {
		return err
	}
	key := hash.String()
	bodyHex := hex.EncodeToString(datum.Cbor())
	if existing, ok := d[key]; ok {
		if existing.DatumCBORHex != bodyHex {
			return fmt.Errorf("conflicting bodies for datum hash %s", key)
		}
		for _, observed := range existing.Sources {
			if observed == source {
				return nil
			}
		}
		existing.Sources = append(existing.Sources, source)
		return nil
	}
	d[key] = &Datum{
		DatumHash:    key,
		DatumCBORHex: bodyHex,
		Sources:      []DatumObservation{source},
	}
	return nil
}
