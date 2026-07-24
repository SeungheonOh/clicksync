package model

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

var (
	ErrInvalidDatasetID = errors.New("expected a 16-byte lowercase hexadecimal dataset id")
	ErrInvalidHash32    = errors.New("expected a 32-byte lowercase hexadecimal hash")
	ErrInvalidPolicyID  = errors.New("expected a 28-byte lowercase hexadecimal policy id")
	ErrInvalidUTxORef   = errors.New("expected TX_HASH#INDEX")
	ErrInvalidAsset     = errors.New("expected ada or POLICY_HEX.ASSET_NAME_HEX")
)

type DatasetID [16]byte

func ParseDatasetID(value string) (DatasetID, error) {
	var result DatasetID
	if len(value) != hex.EncodedLen(len(result)) || value != strings.ToLower(value) {
		return result, ErrInvalidDatasetID
	}
	decoded, err := hex.DecodeString(value)
	if err != nil {
		return result, ErrInvalidDatasetID
	}
	copy(result[:], decoded)
	return result, nil
}

func DatasetIDFromBytes(value []byte) (DatasetID, error) {
	var result DatasetID
	if len(value) != len(result) {
		return result, fmt.Errorf(
			"%w: got %d bytes",
			ErrInvalidDatasetID,
			len(value),
		)
	}
	copy(result[:], value)
	return result, nil
}

func (value DatasetID) String() string {
	return hex.EncodeToString(value[:])
}

func (value DatasetID) MarshalJSON() ([]byte, error) {
	return json.Marshal(value.String())
}

func (value *DatasetID) UnmarshalJSON(data []byte) error {
	var text string
	if err := json.Unmarshal(data, &text); err != nil {
		return err
	}
	parsed, err := ParseDatasetID(text)
	if err != nil {
		return err
	}
	*value = parsed
	return nil
}

// Hash32 remains binary throughout query execution. Hex is only its stable
// command-line and JSON rendering.
type Hash32 [32]byte

func ParseHash32(value string) (Hash32, error) {
	var result Hash32
	if len(value) != hex.EncodedLen(len(result)) || value != strings.ToLower(value) {
		return result, ErrInvalidHash32
	}
	decoded, err := hex.DecodeString(value)
	if err != nil {
		return result, ErrInvalidHash32
	}
	copy(result[:], decoded)
	return result, nil
}

func Hash32FromBytes(value []byte) (Hash32, error) {
	var result Hash32
	if len(value) != len(result) {
		return result, fmt.Errorf("%w: got %d bytes", ErrInvalidHash32, len(value))
	}
	copy(result[:], value)
	return result, nil
}

func (value Hash32) String() string {
	return hex.EncodeToString(value[:])
}

func (value Hash32) MarshalJSON() ([]byte, error) {
	return json.Marshal(value.String())
}

func (value *Hash32) UnmarshalJSON(data []byte) error {
	var text string
	if err := json.Unmarshal(data, &text); err != nil {
		return err
	}
	parsed, err := ParseHash32(text)
	if err != nil {
		return err
	}
	*value = parsed
	return nil
}

type PolicyID [28]byte

func ParsePolicyID(value string) (PolicyID, error) {
	var result PolicyID
	if len(value) != hex.EncodedLen(len(result)) || value != strings.ToLower(value) {
		return result, ErrInvalidPolicyID
	}
	decoded, err := hex.DecodeString(value)
	if err != nil {
		return result, ErrInvalidPolicyID
	}
	copy(result[:], decoded)
	return result, nil
}

func PolicyIDFromBytes(value []byte) (PolicyID, error) {
	var result PolicyID
	if len(value) != len(result) {
		return result, fmt.Errorf("%w: got %d bytes", ErrInvalidPolicyID, len(value))
	}
	copy(result[:], value)
	return result, nil
}

func (value PolicyID) String() string {
	return hex.EncodeToString(value[:])
}

func (value PolicyID) MarshalJSON() ([]byte, error) {
	return json.Marshal(value.String())
}

func (value *PolicyID) UnmarshalJSON(data []byte) error {
	var text string
	if err := json.Unmarshal(data, &text); err != nil {
		return err
	}
	parsed, err := ParsePolicyID(text)
	if err != nil {
		return err
	}
	*value = parsed
	return nil
}

// Bytes is intentionally a byte slice. encoding/json renders it as base64,
// preserving arbitrary address, asset-name and CBOR bytes without assuming
// UTF-8 or hex-backed database columns.
type Bytes []byte

type UTxORef struct {
	TxHash Hash32 `json:"tx_hash"`
	Index  uint32 `json:"index"`
}

func ParseUTxORef(value string) (UTxORef, error) {
	var result UTxORef
	hashText, indexText, ok := strings.Cut(value, "#")
	if !ok || hashText == "" || indexText == "" || strings.Contains(indexText, "#") {
		return result, ErrInvalidUTxORef
	}
	hash, err := ParseHash32(hashText)
	if err != nil {
		return result, fmt.Errorf("%w: %v", ErrInvalidUTxORef, err)
	}
	var index uint64
	for _, digit := range indexText {
		if digit < '0' || digit > '9' {
			return result, ErrInvalidUTxORef
		}
		index = index*10 + uint64(digit-'0')
		if index > uint64(^uint32(0)) {
			return result, ErrInvalidUTxORef
		}
	}
	result.TxHash = hash
	result.Index = uint32(index)
	return result, nil
}

func (value UTxORef) String() string {
	return fmt.Sprintf("%s#%d", value.TxHash, value.Index)
}

type AssetSelector struct {
	ADA       bool     `json:"ada"`
	PolicyID  PolicyID `json:"policy_id,omitempty"`
	AssetName Bytes    `json:"asset_name,omitempty"`
}

func ParseAssetSelector(value string) (AssetSelector, error) {
	if value == "ada" {
		return AssetSelector{ADA: true}, nil
	}
	policyText, nameText, ok := strings.Cut(value, ".")
	if !ok || strings.Contains(nameText, ".") || len(nameText) > 64 || len(nameText)%2 != 0 {
		return AssetSelector{}, ErrInvalidAsset
	}
	policy, err := ParsePolicyID(policyText)
	if err != nil {
		return AssetSelector{}, ErrInvalidAsset
	}
	name, err := hex.DecodeString(nameText)
	if err != nil {
		return AssetSelector{}, ErrInvalidAsset
	}
	return AssetSelector{PolicyID: policy, AssetName: Bytes(name)}, nil
}

func (value AssetSelector) String() string {
	if value.ADA {
		return "ada"
	}
	return value.PolicyID.String() + "." + hex.EncodeToString(value.AssetName)
}
