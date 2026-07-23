package cursor

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
)

const version = 1

var ErrInvalid = errors.New("invalid or corrupted cursor")

type Value struct {
	Version       uint8  `json:"v"`
	Scope         string `json:"scope"`
	SnapshotEvent uint64 `json:"snapshot_event"`
	LastKey       string `json:"last_key"`
}

type wireValue struct {
	Value
	Checksum string `json:"checksum"`
}

func Encode(value Value) (string, error) {
	if value.Scope == "" || value.LastKey == "" {
		return "", fmt.Errorf("%w: incomplete value", ErrInvalid)
	}
	value.Version = version
	payload, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(payload)
	wire, err := json.Marshal(wireValue{
		Value:    value,
		Checksum: base64.RawURLEncoding.EncodeToString(sum[:]),
	})
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(wire), nil
}

func Decode(encoded string, expectedScope string, snapshotEvent uint64) (Value, error) {
	result, err := DecodePinned(encoded, expectedScope)
	if err != nil || result.SnapshotEvent != snapshotEvent {
		return Value{}, ErrInvalid
	}
	return result, nil
}

func DecodePinned(encoded string, expectedScope string) (Value, error) {
	var result wireValue
	wire, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil || json.Unmarshal(wire, &result) != nil {
		return Value{}, ErrInvalid
	}
	if result.Version != version || result.Scope != expectedScope || result.LastKey == "" {
		return Value{}, ErrInvalid
	}
	payload, err := json.Marshal(result.Value)
	if err != nil {
		return Value{}, ErrInvalid
	}
	sum := sha256.Sum256(payload)
	expected, err := base64.RawURLEncoding.DecodeString(result.Checksum)
	if err != nil || len(expected) != len(sum) ||
		subtle.ConstantTimeCompare(expected, sum[:]) != 1 {
		return Value{}, ErrInvalid
	}
	return result.Value, nil
}
