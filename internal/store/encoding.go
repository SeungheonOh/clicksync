package store

import (
	"bytes"
	"errors"
	"fmt"

	"cardano-clicksync/internal/model"
)

func validPoint(point Point) bool {
	if point.Origin {
		return point.Slot == 0 &&
			point.Hash == (model.Hash32{}) &&
			point.BlockNumber == 0 &&
			!point.IsByronEBB
	}
	return point.Hash != (model.Hash32{})
}

func bytes32(hash model.Hash32) []byte {
	return bytes.Clone(hash[:])
}

func bytes28(hash model.Hash28) []byte {
	return bytes.Clone(hash[:])
}

func nullableHash32(hash *model.Hash32) any {
	if hash == nil {
		return nil
	}
	return bytes32(*hash)
}

func nullableHash28(hash *model.Hash28) any {
	if hash == nil {
		return nil
	}
	return bytes28(*hash)
}

func nullableString(value *string) any {
	if value == nil {
		return nil
	}
	return *value
}

func hash32(value []byte) (model.Hash32, error) {
	if len(value) != len(model.Hash32{}) {
		return model.Hash32{}, fmt.Errorf("hash length %d, want 32", len(value))
	}
	var result model.Hash32
	copy(result[:], value)
	return result, nil
}

func hash28(value []byte) (model.Hash28, error) {
	if len(value) != len(model.Hash28{}) {
		return model.Hash28{}, fmt.Errorf("hash length %d, want 28", len(value))
	}
	var result model.Hash28
	copy(result[:], value)
	return result, nil
}

func scanPoint(
	origin bool,
	slot *uint64,
	hashBytes []byte,
	blockNumber *uint64,
	isByronEBB bool,
) (Point, error) {
	if origin {
		if slot != nil || len(hashBytes) != 0 || blockNumber != nil || isByronEBB {
			return Point{}, errors.New("Origin point has invalid nullable fields")
		}
		return Point{Origin: true}, nil
	}
	if slot == nil || blockNumber == nil {
		return Point{}, errors.New("non-Origin point is incomplete")
	}
	hash, err := hash32(hashBytes)
	if err != nil {
		return Point{}, err
	}
	point := Point{
		Slot:        *slot,
		Hash:        hash,
		BlockNumber: *blockNumber,
		IsByronEBB:  isByronEBB,
	}
	if !validPoint(point) {
		return Point{}, errors.New("non-Origin point has invalid shape")
	}
	return point, nil
}
