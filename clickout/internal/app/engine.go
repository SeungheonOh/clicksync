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

func New(reader repository.Reader) *Engine {
	return &Engine{reader: reader}
}

func (engine *Engine) Execute(ctx context.Context, invocation cli.Invocation) (any, error) {
	collector := &metrics.Collector{}
	ctx = metrics.WithCollector(ctx, collector)

	switch invocation.Command {
	case "utxo":
		snapshot, err := engine.reader.Snapshot(ctx, invocation.At)
		if err != nil {
			return nil, err
		}
		state, boundaries, err := engine.reader.UTxO(ctx, snapshot, *invocation.UTxO)
		if err != nil {
			if errors.Is(err, chstore.ErrNotFound) && len(boundaries) > 0 {
				response := model.NewResponse[*model.OutputState](snapshot, nil)
				if len(boundaries) > 0 {
					response.UnresolvedPartialHistory = boundaries
				}
				response.QueryMetrics = collector.Snapshot()
				return response, nil
			}
			return nil, err
		}
		response := model.NewResponse(snapshot, state)
		if len(boundaries) > 0 {
			response.UnresolvedPartialHistory = boundaries
		}
		response.QueryMetrics = collector.Snapshot()
		return response, nil
	case "tx":
		snapshot, err := engine.reader.Snapshot(ctx, invocation.At)
		if err != nil {
			return nil, err
		}
		transaction, boundaries, err := engine.reader.Transaction(ctx, snapshot, *invocation.Tx)
		if err != nil {
			return nil, err
		}
		response := model.NewResponse(snapshot, transaction)
		if len(boundaries) > 0 {
			response.UnresolvedPartialHistory = boundaries
		}
		response.QueryMetrics = collector.Snapshot()
		return response, nil
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
	at := invocation.At
	lastKey := ""
	if invocation.Cursor != "" {
		value, err := cursor.DecodePinned(
			invocation.Cursor,
			addressScope(raw, invocation.State),
		)
		if err != nil {
			return nil, err
		}
		at = model.AtPoint{Event: &value.SnapshotEvent}
		lastKey = value.LastKey
	}
	snapshot, err := engine.reader.Snapshot(ctx, at)
	if err != nil {
		return nil, err
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
	response := model.NewResponse(snapshot, page)
	if len(boundaries) > 0 {
		response.UnresolvedPartialHistory = boundaries
	}
	if page.Cursor != "" {
		response.Truncation.Truncated = true
		response.Truncation.Reason = "address_page_limit"
		response.Truncation.LosslessResume = true
	}
	response.QueryMetrics = collector.Snapshot()
	return response, nil
}

func addressScope(raw []byte, state string) string {
	return chstore.AddressScope(raw, state)
}

func (engine *Engine) datum(
	ctx context.Context,
	collector *metrics.Collector,
	invocation cli.Invocation,
) (any, error) {
	snapshot, err := engine.reader.Snapshot(ctx, invocation.At)
	if err != nil {
		return nil, err
	}
	datum, err := engine.reader.Datum(ctx, snapshot, *invocation.Hash)
	if err != nil {
		return nil, err
	}
	response := model.NewResponse(snapshot, datum)
	response.QueryMetrics = collector.Snapshot()
	return response, nil
}

func (engine *Engine) redeemers(
	ctx context.Context,
	collector *metrics.Collector,
	invocation cli.Invocation,
) (any, error) {
	snapshot, err := engine.reader.Snapshot(ctx, invocation.At)
	if err != nil {
		return nil, err
	}
	redeemers, boundaries, err := engine.reader.Redeemers(ctx, snapshot, *invocation.Tx)
	if err != nil {
		return nil, err
	}
	response := model.NewResponse(snapshot, redeemers)
	if len(boundaries) > 0 {
		response.UnresolvedPartialHistory = boundaries
	}
	response.QueryMetrics = collector.Snapshot()
	return response, nil
}

func (engine *Engine) metadata(
	ctx context.Context,
	collector *metrics.Collector,
	invocation cli.Invocation,
) (any, error) {
	snapshot, err := engine.reader.Snapshot(ctx, invocation.At)
	if err != nil {
		return nil, err
	}
	metadata, err := engine.reader.Metadata(ctx, snapshot, *invocation.Tx)
	if err != nil {
		return nil, err
	}
	response := model.NewResponse(snapshot, metadata)
	response.QueryMetrics = collector.Snapshot()
	return response, nil
}

func (engine *Engine) withdrawals(
	ctx context.Context,
	collector *metrics.Collector,
	invocation cli.Invocation,
) (any, error) {
	snapshot, err := engine.reader.Snapshot(ctx, invocation.At)
	if err != nil {
		return nil, err
	}
	withdrawals, err := engine.reader.Withdrawals(ctx, snapshot, *invocation.Tx)
	if err != nil {
		return nil, err
	}
	response := model.NewResponse(snapshot, withdrawals)
	response.QueryMetrics = collector.Snapshot()
	return response, nil
}
