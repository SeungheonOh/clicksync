package cursor

import (
	"bytes"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/clicksync-project/clickout/internal/model"
)

var ErrInvalid = errors.New("invalid or corrupted cursor")

type Value struct {
	Scope    string         `json:"scope"`
	Snapshot model.Snapshot `json:"snapshot"`
	LastKey  string         `json:"last_key"`
}

type wireValue struct {
	Scope    string         `json:"scope"`
	Snapshot model.Snapshot `json:"snapshot"`
	LastKey  string         `json:"last_key"`
	Checksum string         `json:"checksum"`
}

func Encode(value Value) (string, error) {
	if value.Scope == "" || value.LastKey == "" || !value.Snapshot.Valid() {
		return "", fmt.Errorf("%w: incomplete value", ErrInvalid)
	}
	payload, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(payload)
	wire, err := json.Marshal(wireValue{
		Scope:    value.Scope,
		Snapshot: value.Snapshot,
		LastKey:  value.LastKey,
		Checksum: base64.RawURLEncoding.EncodeToString(sum[:]),
	})
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(wire), nil
}

func Decode(encoded string, expectedScope string) (Value, error) {
	wire, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil ||
		base64.RawURLEncoding.EncodeToString(wire) != encoded {
		return Value{}, ErrInvalid
	}
	var decoded wireValue
	decoder := json.NewDecoder(bytes.NewReader(wire))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&decoded) != nil {
		return Value{}, ErrInvalid
	}
	if err := requireJSONEOF(decoder); err != nil {
		return Value{}, ErrInvalid
	}
	canonical, err := json.Marshal(decoded)
	if err != nil || !bytes.Equal(canonical, wire) {
		return Value{}, ErrInvalid
	}
	value := Value{
		Scope:    decoded.Scope,
		Snapshot: decoded.Snapshot,
		LastKey:  decoded.LastKey,
	}
	if value.Scope != expectedScope ||
		value.LastKey == "" ||
		!value.Snapshot.Valid() {
		return Value{}, ErrInvalid
	}
	payload, err := json.Marshal(value)
	if err != nil {
		return Value{}, ErrInvalid
	}
	sum := sha256.Sum256(payload)
	checksum, err := base64.RawURLEncoding.DecodeString(decoded.Checksum)
	if err != nil ||
		base64.RawURLEncoding.EncodeToString(checksum) != decoded.Checksum ||
		len(checksum) != len(sum) ||
		subtle.ConstantTimeCompare(checksum, sum[:]) != 1 {
		return Value{}, ErrInvalid
	}
	return value, nil
}

func requireJSONEOF(decoder *json.Decoder) error {
	var extra any
	err := decoder.Decode(&extra)
	if errors.Is(err, io.EOF) {
		return nil
	}
	return ErrInvalid
}
