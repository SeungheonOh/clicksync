package app

import (
	"context"
	"errors"

	"github.com/clicksync-project/clickout/internal/address"
	"github.com/clicksync-project/clickout/internal/cli"
	chstore "github.com/clicksync-project/clickout/internal/clickhouse"
	"github.com/clicksync-project/clickout/internal/cursor"
	"github.com/clicksync-project/clickout/internal/metrics"
	"github.com/clicksync-project/clickout/internal/model"
	"github.com/clicksync-project/clickout/internal/repository"
)

type Engine struct {
	reader repository.Reader
}

var errSnapshotPinChanged = errors.New(
	"repository changed immutable snapshot pin",
)

func New(reader repository.Reader) *Engine {
	return &Engine{reader: reader}
}

func (engine *Engine) snapshotForRead(
	ctx context.Context,
	at model.AtPoint,
) (model.Snapshot, error) {
	snapshot, err := engine.reader.Snapshot(ctx, at)
	if err != nil {
		return model.Snapshot{}, err
	}
	return engine.validateSnapshotBeforeRead(ctx, snapshot)
}

func (engine *Engine) validateSnapshotBeforeRead(
	ctx context.Context,
	snapshot model.Snapshot,
) (model.Snapshot, error) {
	refreshed, err := engine.reader.ValidateSnapshotBeforeRead(ctx, snapshot)
	if err != nil {
		return model.Snapshot{}, err
	}
	if !refreshed.SamePin(snapshot) {
		return model.Snapshot{}, errSnapshotPinChanged
	}
	return refreshed, nil
}

func finalizeResponse[T any](
	ctx context.Context,
	engine *Engine,
	snapshot model.Snapshot,
	data T,
	collector *metrics.Collector,
	decorate func(*model.Response[T]) error,
) (any, error) {
	refreshed, err := engine.reader.FinishSnapshot(ctx, snapshot)
	if err != nil {
		return nil, err
	}
	if !refreshed.SamePin(snapshot) {
		return nil, errSnapshotPinChanged
	}
	response := model.NewResponse(refreshed, data)
	if decorate != nil {
		if err := decorate(&response); err != nil {
			return nil, err
		}
	}
	response.QueryMetrics = collector.Snapshot()
	return response, nil
}

func (engine *Engine) Execute(ctx context.Context, invocation cli.Invocation) (any, error) {
	collector := &metrics.Collector{}
	ctx = metrics.WithCollector(ctx, collector)

	switch invocation.Command {
	case "utxo":
		snapshot, err := engine.snapshotForRead(ctx, invocation.At)
		if err != nil {
			return nil, err
		}
		state, boundaries, err := engine.reader.UTxO(ctx, snapshot, *invocation.UTxO)
		if err != nil {
			if errors.Is(err, chstore.ErrNotFound) && len(boundaries) > 0 {
				return finalizeResponse(
					ctx,
					engine,
					snapshot,
					(*model.OutputState)(nil),
					collector,
					func(response *model.Response[*model.OutputState]) error {
						response.UnresolvedPartialHistory = boundaries
						return nil
					},
				)
			}
			return nil, err
		}
		return finalizeResponse(
			ctx,
			engine,
			snapshot,
			state,
			collector,
			func(response *model.Response[model.OutputState]) error {
				response.UnresolvedPartialHistory = boundaries
				return nil
			},
		)
	case "tx":
		snapshot, err := engine.snapshotForRead(ctx, invocation.At)
		if err != nil {
			return nil, err
		}
		transaction, boundaries, err := engine.reader.Transaction(ctx, snapshot, *invocation.Tx)
		if err != nil {
			return nil, err
		}
		return finalizeResponse(
			ctx,
			engine,
			snapshot,
			transaction,
			collector,
			func(response *model.Response[model.Transaction]) error {
				response.UnresolvedPartialHistory = boundaries
				return nil
			},
		)
	case "address":
		return engine.address(ctx, collector, invocation)
	case "datum":
		return engine.datum(ctx, collector, invocation)
	case "redeemers":
		return engine.redeemers(ctx, collector, invocation)
	case "metadata":
		return engine.metadata(ctx, collector, invocation)
	case "withdrawals":
		return engine.withdrawals(ctx, collector, invocation)
	case "trace":
		return engine.trace(ctx, collector, invocation)
	default:
		return nil, cli.ErrUsage
	}
}

func (engine *Engine) address(
	ctx context.Context,
	collector *metrics.Collector,
	invocation cli.Invocation,
) (any, error) {
	raw, err := address.Decode(invocation.Address)
	if err != nil {
		return nil, err
	}
	lastKey := ""
	var snapshot model.Snapshot
	if invocation.Cursor != "" {
		value, err := cursor.Decode(
			invocation.Cursor,
			addressScope(raw, invocation.State),
		)
		if err != nil {
			return nil, err
		}
		snapshot, err = engine.validateSnapshotBeforeRead(
			ctx,
			value.Snapshot,
		)
		if err != nil {
			return nil, err
		}
		lastKey = value.LastKey
	} else {
		snapshot, err = engine.snapshotForRead(ctx, invocation.At)
		if err != nil {
			return nil, err
		}
	}
	page, boundaries, err := engine.reader.Address(ctx, snapshot, repository.AddressQuery{
		Address: raw,
		State:   invocation.State,
		Limit:   invocation.Limit,
		LastKey: lastKey,
	})
	if err != nil {
		return nil, err
	}
	return finalizeResponse(
		ctx,
		engine,
		snapshot,
		page,
		collector,
		func(response *model.Response[model.AddressPage]) error {
			response.UnresolvedPartialHistory = boundaries
			if response.Data.Cursor == "" {
				return nil
			}
			value, err := cursor.Decode(
				response.Data.Cursor,
				addressScope(raw, invocation.State),
			)
			if err != nil || !value.Snapshot.SamePin(snapshot) {
				return cursor.ErrInvalid
			}
			value.Snapshot = response.Snapshot
			response.Data.Cursor, err = cursor.Encode(value)
			if err != nil {
				return err
			}
			response.Truncation.Truncated = true
			response.Truncation.Reason = "address_page_limit"
			response.Truncation.LosslessResume = true
			return nil
		},
	)
}

func addressScope(raw []byte, state string) string {
	return chstore.AddressScope(raw, state)
}

func (engine *Engine) datum(
	ctx context.Context,
	collector *metrics.Collector,
	invocation cli.Invocation,
) (any, error) {
	snapshot, err := engine.snapshotForRead(ctx, invocation.At)
	if err != nil {
		return nil, err
	}
	datum, err := engine.reader.Datum(ctx, snapshot, *invocation.Hash)
	if err != nil {
		return nil, err
	}
	return finalizeResponse(ctx, engine, snapshot, datum, collector, nil)
}

func (engine *Engine) redeemers(
	ctx context.Context,
	collector *metrics.Collector,
	invocation cli.Invocation,
) (any, error) {
	snapshot, err := engine.snapshotForRead(ctx, invocation.At)
	if err != nil {
		return nil, err
	}
	redeemers, boundaries, err := engine.reader.Redeemers(ctx, snapshot, *invocation.Tx)
	if err != nil {
		return nil, err
	}
	return finalizeResponse(
		ctx,
		engine,
		snapshot,
		redeemers,
		collector,
		func(response *model.Response[[]model.Redeemer]) error {
			response.UnresolvedPartialHistory = boundaries
			return nil
		},
	)
}

func (engine *Engine) metadata(
	ctx context.Context,
	collector *metrics.Collector,
	invocation cli.Invocation,
) (any, error) {
	snapshot, err := engine.snapshotForRead(ctx, invocation.At)
	if err != nil {
		return nil, err
	}
	metadata, err := engine.reader.Metadata(ctx, snapshot, *invocation.Tx)
	if err != nil {
		return nil, err
	}
	return finalizeResponse(ctx, engine, snapshot, metadata, collector, nil)
}

func (engine *Engine) withdrawals(
	ctx context.Context,
	collector *metrics.Collector,
	invocation cli.Invocation,
) (any, error) {
	snapshot, err := engine.snapshotForRead(ctx, invocation.At)
	if err != nil {
		return nil, err
	}
	withdrawals, err := engine.reader.Withdrawals(ctx, snapshot, *invocation.Tx)
	if err != nil {
		return nil, err
	}
	return finalizeResponse(ctx, engine, snapshot, withdrawals, collector, nil)
}
