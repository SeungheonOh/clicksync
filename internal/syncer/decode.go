package syncer

import (
	"context"
	"errors"
	"fmt"

	"cardano-clicksync/internal/model"
	"cardano-clicksync/internal/normalize"
)

// DecodedBlock carries publication provenance around the provenance-neutral
// normalized fact bundle. Raw CBOR is deliberately absent.
type DecodedBlock struct {
	Block       model.Block
	ChainPoint  model.Point
	ContentHash model.Hash32
	RawLength   uint64
	Relays      []model.RelayIdentity
}

func DecodeAgreedBlock(
	ctx context.Context,
	event model.AgreedEvent,
) (DecodedBlock, error) {
	if err := context.Cause(ctx); err != nil {
		return DecodedBlock{}, err
	}
	if event.Kind != model.EventForward {
		return DecodedBlock{}, errors.New("cannot decode a non-forward event")
	}
	if len(event.RawCBOR) == 0 {
		return DecodedBlock{}, errors.New("agreed forward event has no retained raw CBOR")
	}
	facts, err := normalize.Decode(event.BlockType, event.RawCBOR)
	if err != nil {
		return DecodedBlock{}, fmt.Errorf("decode and normalize agreed raw block: %w", err)
	}
	facts.ObservedAt = event.ObservedAt.UTC()
	return DecodedBlock{
		Block:       facts,
		ChainPoint:  clonePoint(event.Point),
		ContentHash: event.ContentHash,
		RawLength:   event.RawLength,
		Relays:      cloneRelays(event.Relays),
	}, nil
}

func FactRows(block model.Block) uint64 {
	rows := uint64(1 + len(block.Datums))
	for _, transaction := range block.Transactions {
		rows++
		rows += uint64(len(transaction.Inputs))
		rows += uint64(len(transaction.Outputs))
		rows += uint64(len(transaction.DatumObservations))
		rows += uint64(len(transaction.Withdrawals))
		rows += uint64(len(transaction.Redeemers))
		if transaction.Metadata != nil {
			rows++
		}
	}
	return rows
}

func clonePoint(value model.Point) model.Point {
	return value
}

func cloneRelays(values []model.RelayIdentity) []model.RelayIdentity {
	return append([]model.RelayIdentity(nil), values...)
}
