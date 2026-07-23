package clickhouse

import (
	"bytes"
	"errors"
	"fmt"
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
	return makeOutput(values)
}

func scanAddressOutput(row scanner) (model.Output, uint64, error) {
	var values outputValues
	var publicationID uint64
	if err := row.Scan(
		&values.txHash,
		&values.outputIndex,
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
	return output, publicationID, err
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
		output.ReferenceScriptLanguage = *values.referenceLanguage
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
	addressType := address[0] >> 4
	if addressType == 8 {
		if len(address) < 2 {
			return "", nil, errors.New("truncated Byron output address")
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
	componentLimits := [...]uint64{math.MaxUint64, math.MaxUint16, math.MaxUint16}
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

func decodeSignedAssets(policies, names []string, quantities []int64) ([]model.SignedAssetQuantity, error) {
	if len(policies) != len(names) || len(policies) != len(quantities) {
		return nil, errors.New("mint arrays have unequal lengths")
	}
	result := make([]model.SignedAssetQuantity, len(policies))
	for index := range policies {
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
