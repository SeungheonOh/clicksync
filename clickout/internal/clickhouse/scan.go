package clickhouse

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math"

	"golang.org/x/crypto/blake2b"

	"github.com/clicksync-project/clickout/internal/model"
)

type scanner interface {
	Scan(...any) error
}

func hashArgument(value model.Hash32) string {
	return string(value[:])
}

func calculateContentHash(data []byte) model.Hash32 {
	return model.Hash32(blake2b.Sum256(data))
}

type outputValues struct {
	txHash              []byte
	outputIndex         uint32
	bodyOrdinal         uint32
	blockHash           []byte
	blockNumber         uint64
	kind                string
	address             []byte
	paymentKind         string
	paymentHash         *string
	lovelace            uint64
	policies            []string
	names               []string
	quantities          []uint64
	datumKind           string
	datumHash           *string
	referenceScriptHash *string
	referenceLanguage   *string
}

func scanOutput(row scanner) (model.Output, error) {
	var values outputValues
	if err := row.Scan(
		&values.txHash,
		&values.outputIndex,
		&values.bodyOrdinal,
		&values.blockHash,
		&values.blockNumber,
		&values.kind,
		&values.address,
		&values.paymentKind,
		&values.paymentHash,
		&values.lovelace,
		&values.policies,
		&values.names,
		&values.quantities,
		&values.datumKind,
		&values.datumHash,
		&values.referenceScriptHash,
		&values.referenceLanguage,
	); err != nil {
		return model.Output{}, err
	}
	output, err := makeOutput(values)
	if err != nil {
		return model.Output{}, transactionCorruption(
			"invalid persisted output: %v",
			err,
		)
	}
	return output, nil
}

func scanAddressOutput(row scanner) (model.Output, uint64, error) {
	var values outputValues
	var publicationID uint64
	if err := row.Scan(
		&values.txHash,
		&values.outputIndex,
		&values.bodyOrdinal,
		&values.blockHash,
		&values.blockNumber,
		&values.kind,
		&values.address,
		&values.paymentKind,
		&values.paymentHash,
		&values.lovelace,
		&values.policies,
		&values.names,
		&values.quantities,
		&values.datumKind,
		&values.datumHash,
		&values.referenceScriptHash,
		&values.referenceLanguage,
		&publicationID,
	); err != nil {
		return model.Output{}, 0, err
	}
	output, err := makeOutput(values)
	if err != nil {
		return model.Output{}, 0, transactionCorruption(
			"invalid persisted output: %v",
			err,
		)
	}
	return output, publicationID, nil
}

func makeOutput(values outputValues) (model.Output, error) {
	hash, err := model.Hash32FromBytes(values.txHash)
	if err != nil {
		return model.Output{}, err
	}
	block, err := model.Hash32FromBytes(values.blockHash)
	if err != nil {
		return model.Output{}, err
	}
	if len(values.policies) != len(values.names) || len(values.policies) != len(values.quantities) {
		return model.Output{}, errors.New("output asset arrays have unequal lengths")
	}
	assets := make([]model.OutputAsset, len(values.policies))
	for index := range values.policies {
		if len(values.names[index]) > 32 {
			return model.Output{}, errors.New(
				"output asset name exceeds 32 bytes",
			)
		}
		policy, err := model.PolicyIDFromBytes([]byte(values.policies[index]))
		if err != nil {
			return model.Output{}, err
		}
		assets[index] = model.OutputAsset{
			PolicyID: policy,
			Name:     model.Bytes(bytes.Clone([]byte(values.names[index]))),
			Quantity: values.quantities[index],
		}
		if assets[index].Quantity == 0 {
			return model.Output{}, errors.New("output contains a zero asset quantity")
		}
		if index > 0 {
			previous := assets[index-1]
			if compared := bytes.Compare(previous.PolicyID[:], assets[index].PolicyID[:]); compared > 0 ||
				(compared == 0 && bytes.Compare(previous.Name, assets[index].Name) >= 0) {
				return model.Output{}, errors.New("output assets are not strictly sorted and unique")
			}
		}
	}
	switch model.OutputKind(values.kind) {
	case model.OutputRegular, model.OutputCollateralReturn, model.OutputGenesis:
	default:
		return model.Output{}, fmt.Errorf("unsupported output kind %q", values.kind)
	}
	output := model.Output{
		Ref: model.UTxORef{
			TxHash: hash,
			Index:  values.outputIndex,
		},
		ProducingTx:           hash,
		BodyOrdinal:           values.bodyOrdinal,
		BlockHash:             block,
		BlockHeight:           values.blockNumber,
		Kind:                  model.OutputKind(values.kind),
		Address:               model.Bytes(bytes.Clone(values.address)),
		PaymentCredentialKind: values.paymentKind,
		Lovelace:              values.lovelace,
		Assets:                assets,
		DatumKind:             values.datumKind,
	}
	derivedKind, derivedHash, err := paymentCredentialFromRawAddress(values.address)
	if err != nil {
		return model.Output{}, err
	}
	switch values.paymentKind {
	case "none":
		if values.paymentHash != nil {
			return model.Output{}, errors.New("payment credential kind none has a hash")
		}
	case "key", "script":
		if values.paymentHash == nil {
			return model.Output{}, errors.New("payment credential hash is missing")
		}
		if len(*values.paymentHash) != 28 {
			return model.Output{}, fmt.Errorf(
				"payment credential hash has %d bytes",
				len(*values.paymentHash),
			)
		}
		output.PaymentCredentialHash = model.Bytes(bytes.Clone([]byte(*values.paymentHash)))
	default:
		return model.Output{}, fmt.Errorf(
			"unsupported payment credential kind %q",
			values.paymentKind,
		)
	}
	if values.paymentKind != derivedKind ||
		!bytes.Equal(output.PaymentCredentialHash, derivedHash) {
		return model.Output{}, errors.New(
			"stored payment credential disagrees with raw output address",
		)
	}
	switch values.datumKind {
	case "none":
		if values.datumHash != nil {
			return model.Output{}, errors.New("datum kind none has a hash")
		}
	case "hash", "inline":
		if values.datumHash == nil {
			return model.Output{}, errors.New("datum hash is missing")
		}
		value, err := model.Hash32FromBytes([]byte(*values.datumHash))
		if err != nil {
			return model.Output{}, err
		}
		output.DatumHash = &value
	default:
		return model.Output{}, fmt.Errorf("unsupported datum kind %q", values.datumKind)
	}
	if (values.referenceScriptHash == nil) != (values.referenceLanguage == nil) {
		return model.Output{}, errors.New("reference script hash/language presence disagrees")
	}
	if values.referenceScriptHash != nil {
		value, err := model.PolicyIDFromBytes([]byte(*values.referenceScriptHash))
		if err != nil {
			return model.Output{}, err
		}
		output.ReferenceScriptHash = &value
	}
	if values.referenceLanguage != nil {
		switch *values.referenceLanguage {
		case "native", "plutus_v1", "plutus_v2", "plutus_v3", "plutus_v4":
			output.ReferenceScriptLanguage = *values.referenceLanguage
		default:
			return model.Output{}, fmt.Errorf(
				"unsupported reference script language %q",
				*values.referenceLanguage,
			)
		}
	}
	return output, nil
}

// paymentCredentialFromRawAddress performs the minimum Shelley-family
// decoding needed to prove the denormalized payment credential. Byron
// addresses are opaque here and intentionally have no payment credential.
func paymentCredentialFromRawAddress(address []byte) (string, []byte, error) {
	if len(address) == 0 {
		return "", nil, errors.New("empty output address")
	}
	if len(address) > 256 {
		return "", nil, fmt.Errorf(
			"output address has %d bytes, maximum 256",
			len(address),
		)
	}
	addressType := address[0] >> 4
	if addressType == 8 {
		if err := validateByronAddress(address); err != nil {
			return "", nil, err
		}
		return "none", nil, nil
	}
	if address[0]&0x0f != 1 {
		return "", nil, fmt.Errorf(
			"Shelley-family output address has non-mainnet network id %d",
			address[0]&0x0f,
		)
	}
	var (
		kind string
		want int
	)
	switch addressType {
	case 0, 2:
		kind, want = "key", 57
	case 1, 3:
		kind, want = "script", 57
	case 4:
		kind = "key"
	case 5:
		kind = "script"
	case 6:
		kind, want = "key", 29
	case 7:
		kind, want = "script", 29
	case 14, 15:
		return "", nil, fmt.Errorf(
			"reward address type %d is not a transaction output address",
			addressType,
		)
	default:
		return "", nil, fmt.Errorf("unsupported output address type %d", addressType)
	}
	if addressType == 4 || addressType == 5 {
		if len(address) <= 29 {
			return "", nil, errors.New("pointer output address is truncated")
		}
		if err := validatePointerAddressSuffix(address[29:]); err != nil {
			return "", nil, err
		}
	} else if len(address) != want {
		return "", nil, fmt.Errorf(
			"output address type %d has length %d, want %d",
			addressType,
			len(address),
			want,
		)
	}
	return kind, bytes.Clone(address[1:29]), nil
}

func validatePointerAddressSuffix(suffix []byte) error {
	offset := 0
	componentLimits := [...]uint64{math.MaxUint32, math.MaxUint16, math.MaxUint16}
	for component, limit := range componentLimits {
		start := offset
		var accumulated uint64
		for {
			if offset >= len(suffix) {
				return fmt.Errorf("pointer address component %d is truncated", component)
			}
			value := suffix[offset]
			offset++
			digit := uint64(value & 0x7f)
			if offset == start+1 && value&0x80 != 0 && digit == 0 {
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
	if offset != len(suffix) {
		return fmt.Errorf("pointer address has %d trailing bytes", len(suffix)-offset)
	}
	return nil
}

type boundedCBOR struct {
	data   []byte
	offset int
}

func (decoder *boundedCBOR) head() (byte, uint64, error) {
	if decoder.offset >= len(decoder.data) {
		return 0, 0, errors.New("truncated CBOR")
	}
	initial := decoder.data[decoder.offset]
	decoder.offset++
	major := initial >> 5
	additional := initial & 0x1f
	switch {
	case additional < 24:
		return major, uint64(additional), nil
	case additional == 24:
		if decoder.offset+1 > len(decoder.data) {
			return 0, 0, errors.New("truncated CBOR uint8")
		}
		value := uint64(decoder.data[decoder.offset])
		decoder.offset++
		if value < 24 {
			return 0, 0, errors.New("nonminimal CBOR uint8")
		}
		return major, value, nil
	case additional == 25:
		if decoder.offset+2 > len(decoder.data) {
			return 0, 0, errors.New("truncated CBOR uint16")
		}
		value := uint64(binary.BigEndian.Uint16(
			decoder.data[decoder.offset : decoder.offset+2],
		))
		decoder.offset += 2
		if value <= math.MaxUint8 {
			return 0, 0, errors.New("nonminimal CBOR uint16")
		}
		return major, value, nil
	case additional == 26:
		if decoder.offset+4 > len(decoder.data) {
			return 0, 0, errors.New("truncated CBOR uint32")
		}
		value := uint64(binary.BigEndian.Uint32(
			decoder.data[decoder.offset : decoder.offset+4],
		))
		decoder.offset += 4
		if value <= math.MaxUint16 {
			return 0, 0, errors.New("nonminimal CBOR uint32")
		}
		return major, value, nil
	case additional == 27:
		if decoder.offset+8 > len(decoder.data) {
			return 0, 0, errors.New("truncated CBOR uint64")
		}
		value := binary.BigEndian.Uint64(
			decoder.data[decoder.offset : decoder.offset+8],
		)
		decoder.offset += 8
		if value <= math.MaxUint32 {
			return 0, 0, errors.New("nonminimal CBOR uint64")
		}
		return major, value, nil
	default:
		return 0, 0, errors.New(
			"indefinite or reserved CBOR form is unsupported",
		)
	}
}

func (decoder *boundedCBOR) bytes() ([]byte, error) {
	major, length, err := decoder.head()
	if err != nil {
		return nil, err
	}
	if major != 2 || length > uint64(len(decoder.data)-decoder.offset) {
		return nil, errors.New("invalid CBOR byte string")
	}
	start := decoder.offset
	decoder.offset += int(length)
	return decoder.data[start:decoder.offset], nil
}

func (decoder *boundedCBOR) skip(depth uint8) error {
	if depth > 16 {
		return errors.New("CBOR nesting exceeds 16")
	}
	major, value, err := decoder.head()
	if err != nil {
		return err
	}
	switch major {
	case 0, 1, 7:
		return nil
	case 2, 3:
		if value > uint64(len(decoder.data)-decoder.offset) {
			return errors.New("truncated CBOR string")
		}
		decoder.offset += int(value)
		return nil
	case 4:
		for index := uint64(0); index < value; index++ {
			if err := decoder.skip(depth + 1); err != nil {
				return err
			}
		}
		return nil
	case 5:
		if value > uint64(len(decoder.data)) {
			return errors.New("CBOR map is too large")
		}
		for index := uint64(0); index < value; index++ {
			if err := decoder.skip(depth + 1); err != nil {
				return err
			}
			if err := decoder.skip(depth + 1); err != nil {
				return err
			}
		}
		return nil
	case 6:
		return decoder.skip(depth + 1)
	default:
		return errors.New("unsupported CBOR major type")
	}
}

func validateByronAddress(address []byte) error {
	outer := boundedCBOR{data: address}
	major, length, err := outer.head()
	if err != nil || major != 4 || length != 2 {
		return errors.New("Byron address outer CBOR is not a pair")
	}
	major, tag, err := outer.head()
	if err != nil || major != 6 || tag != 24 {
		return errors.New("Byron address lacks CBOR tag 24")
	}
	payload, err := outer.bytes()
	if err != nil {
		return fmt.Errorf("Byron address payload: %w", err)
	}
	major, checksum, err := outer.head()
	if err != nil || major != 0 || checksum > math.MaxUint32 {
		return errors.New("Byron address CRC is malformed")
	}
	if outer.offset != len(address) {
		return errors.New("Byron address has trailing bytes")
	}
	if uint32(checksum) != crc32.ChecksumIEEE(payload) {
		return errors.New("Byron address CRC mismatch")
	}
	inner := boundedCBOR{data: payload}
	major, length, err = inner.head()
	if err != nil || major != 4 || length != 3 {
		return errors.New("Byron address payload is not a triple")
	}
	root, err := inner.bytes()
	if err != nil || len(root) != 28 {
		return errors.New("Byron address root is malformed")
	}
	major, attributes, err := inner.head()
	if err != nil || major != 5 || attributes > 16 {
		return errors.New("Byron address attributes are malformed")
	}
	attributeKeys := make(map[uint64]struct{}, attributes)
	var previousKey uint64
	for index := uint64(0); index < attributes; index++ {
		major, key, err := inner.head()
		if err != nil || major != 0 {
			return errors.New("Byron address attribute key is malformed")
		}
		if key != 1 && key != 2 {
			return errors.New("Byron address has an unknown attribute")
		}
		if index > 0 && previousKey >= key {
			return errors.New(
				"Byron address attributes are not canonically ordered",
			)
		}
		previousKey = key
		if _, duplicate := attributeKeys[key]; duplicate {
			return errors.New("Byron address has a duplicate attribute")
		}
		attributeKeys[key] = struct{}{}
		switch key {
		case 1:
			payload, err := inner.bytes()
			if err != nil || len(payload) == 0 {
				return errors.New(
					"Byron derivation-path attribute is malformed",
				)
			}
		case 2:
			encodedNetwork, err := inner.bytes()
			if err != nil {
				return errors.New(
					"Byron network attribute is not encoded CBOR bytes",
				)
			}
			network := boundedCBOR{data: encodedNetwork}
			major, value, err := network.head()
			if err != nil ||
				major != 0 ||
				value > math.MaxUint32 ||
				network.offset != len(encodedNetwork) {
				return errors.New(
					"Byron network attribute is not an encoded UInt32",
				)
			}
		}
	}
	major, addressType, err := inner.head()
	if err != nil || major != 0 || addressType > 2 {
		return errors.New("Byron address type is malformed")
	}
	if inner.offset != len(payload) {
		return errors.New("Byron address payload has trailing bytes")
	}
	return nil
}

func scanSpend(row scanner) (model.Spend, error) {
	var (
		sourceHash     []byte
		sourceIndex    uint32
		consumingHash  []byte
		blockHash      []byte
		blockHeight    uint64
		role           string
		ordinal        uint32
		consumed       bool
		sourceResolved bool
	)
	if err := row.Scan(
		&sourceHash,
		&sourceIndex,
		&consumingHash,
		&blockHash,
		&blockHeight,
		&role,
		&ordinal,
		&consumed,
		&sourceResolved,
	); err != nil {
		return model.Spend{}, err
	}
	source, err := model.Hash32FromBytes(sourceHash)
	if err != nil {
		return model.Spend{}, err
	}
	consuming, err := model.Hash32FromBytes(consumingHash)
	if err != nil {
		return model.Spend{}, err
	}
	switch model.InputRole(role) {
	case model.InputRegular, model.InputCollateral, model.InputReference:
	default:
		return model.Spend{}, fmt.Errorf("unsupported input role %q", role)
	}
	if model.InputRole(role) == model.InputReference && consumed {
		return model.Spend{}, errors.New("reference input is marked consumed")
	}
	block, err := model.Hash32FromBytes(blockHash)
	if err != nil {
		return model.Spend{}, err
	}
	return model.Spend{
		Source: model.UTxORef{
			TxHash: source,
			Index:  sourceIndex,
		},
		ConsumingTx:          consuming,
		ConsumingBlockHash:   block,
		ConsumingBlockHeight: blockHeight,
		Role:                 model.InputRole(role),
		BodyOrdinal:          ordinal,
		IsConsumed:           consumed,
		SourceResolved:       sourceResolved,
	}, nil
}

type spendOrdering uint8

const (
	spendByTransaction spendOrdering = iota
	spendByBlockThenTransaction
)

func validateSpendRows(spends []model.Spend, ordering spendOrdering) error {
	ordinals := make(map[string]struct{}, len(spends))
	for index, spend := range spends {
		identity := fmt.Sprintf(
			"%s/%s/%d",
			spend.ConsumingTx.String(),
			spend.Role,
			spend.BodyOrdinal,
		)
		if _, duplicate := ordinals[identity]; duplicate {
			return fmt.Errorf("%w: duplicate input role/body ordinal", ErrConflictingRow)
		}
		ordinals[identity] = struct{}{}
		if index > 0 && compareSpends(spends[index-1], spend, ordering) >= 0 {
			return fmt.Errorf("%w: input rows are not in deterministic role order", ErrConflictingRow)
		}
	}
	return nil
}

func validateCompleteSpendRows(spends []model.Spend) error {
	if err := validateSpendRows(spends, spendByTransaction); err != nil {
		return err
	}
	next := make(map[string]uint32)
	for _, spend := range spends {
		key := spend.ConsumingTx.String() + "/" + string(spend.Role)
		if spend.BodyOrdinal != next[key] {
			return fmt.Errorf(
				"%w: non-consecutive %s input body ordinal",
				ErrConflictingRow,
				spend.Role,
			)
		}
		next[key]++
	}
	return nil
}

func compareSpends(left, right model.Spend, ordering spendOrdering) int {
	if ordering == spendByBlockThenTransaction {
		switch {
		case left.ConsumingBlockHeight < right.ConsumingBlockHeight:
			return -1
		case left.ConsumingBlockHeight > right.ConsumingBlockHeight:
			return 1
		}
	}
	if compared := bytes.Compare(left.ConsumingTx[:], right.ConsumingTx[:]); compared != 0 {
		return compared
	}
	leftRole := inputRoleRank(left.Role)
	rightRole := inputRoleRank(right.Role)
	switch {
	case leftRole < rightRole:
		return -1
	case leftRole > rightRole:
		return 1
	case left.BodyOrdinal < right.BodyOrdinal:
		return -1
	case left.BodyOrdinal > right.BodyOrdinal:
		return 1
	}
	if compared := bytes.Compare(left.Source.TxHash[:], right.Source.TxHash[:]); compared != 0 {
		return compared
	}
	switch {
	case left.Source.Index < right.Source.Index:
		return -1
	case left.Source.Index > right.Source.Index:
		return 1
	default:
		return 0
	}
}

func inputRoleRank(role model.InputRole) uint8 {
	switch role {
	case model.InputRegular:
		return 0
	case model.InputCollateral:
		return 1
	case model.InputReference:
		return 2
	default:
		return 3
	}
}

func validateOutputRows(outputs []model.Output) error {
	ordinals := make(map[string]struct{}, len(outputs))
	refs := make(map[string]struct{}, len(outputs))
	for index, output := range outputs {
		ref := output.Ref.String()
		if _, duplicate := refs[ref]; duplicate {
			return fmt.Errorf("%w: duplicate output reference", ErrConflictingRow)
		}
		refs[ref] = struct{}{}
		ordinal := fmt.Sprintf("%s/%d", output.ProducingTx, output.BodyOrdinal)
		if _, duplicate := ordinals[ordinal]; duplicate {
			return fmt.Errorf("%w: duplicate output body ordinal", ErrConflictingRow)
		}
		ordinals[ordinal] = struct{}{}
		if index == 0 {
			continue
		}
		previous := outputs[index-1]
		if compared := bytes.Compare(previous.ProducingTx[:], output.ProducingTx[:]); compared > 0 ||
			(compared == 0 &&
				(previous.BodyOrdinal > output.BodyOrdinal ||
					(previous.BodyOrdinal == output.BodyOrdinal &&
						previous.Ref.Index >= output.Ref.Index))) {
			return fmt.Errorf("%w: output rows are not in deterministic ordinal order", ErrConflictingRow)
		}
	}
	return nil
}

func validateCompleteOutputRows(outputs []model.Output) error {
	if err := validateOutputRows(outputs); err != nil {
		return err
	}
	next := make(map[string]uint32)
	for _, output := range outputs {
		key := output.ProducingTx.String()
		if output.BodyOrdinal != next[key] {
			return fmt.Errorf(
				"%w: non-consecutive output body ordinal",
				ErrConflictingRow,
			)
		}
		next[key]++
	}
	return nil
}

func decodeSignedAssets(policies, names []string, quantities []int64) ([]model.SignedAssetQuantity, error) {
	if len(policies) != len(names) || len(policies) != len(quantities) {
		return nil, errors.New("mint arrays have unequal lengths")
	}
	result := make([]model.SignedAssetQuantity, len(policies))
	for index := range policies {
		if len(names[index]) > 32 {
			return nil, errors.New("mint asset name exceeds 32 bytes")
		}
		policy, err := model.PolicyIDFromBytes([]byte(policies[index]))
		if err != nil {
			return nil, fmt.Errorf("mint policy %d: %w", index, err)
		}
		result[index] = model.SignedAssetQuantity{
			PolicyID: policy,
			Name:     model.Bytes(bytes.Clone([]byte(names[index]))),
			Quantity: quantities[index],
		}
		if result[index].Quantity == 0 {
			return nil, errors.New("mint contains a zero quantity")
		}
		if index > 0 {
			previous := result[index-1]
			if compared := bytes.Compare(previous.PolicyID[:], result[index].PolicyID[:]); compared > 0 ||
				(compared == 0 && bytes.Compare(previous.Name, result[index].Name) >= 0) {
				return nil, errors.New("mint assets are not strictly sorted and unique")
			}
		}
	}
	return result, nil
}
