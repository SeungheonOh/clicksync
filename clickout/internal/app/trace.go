package app

import (
	"context"
	"errors"
	"sort"

	"github.com/clicksync-project/clickout/internal/address"
	"github.com/clicksync-project/clickout/internal/cli"
	"github.com/clicksync-project/clickout/internal/limits"
	"github.com/clicksync-project/clickout/internal/metrics"
	"github.com/clicksync-project/clickout/internal/model"
	"github.com/clicksync-project/clickout/internal/repository"
)

func (engine *Engine) trace(
	ctx context.Context,
	collector *metrics.Collector,
	invocation cli.Invocation,
) (any, error) {
	snapshot, err := engine.reader.Snapshot(ctx, invocation.At)
	if err != nil {
		return nil, err
	}
	seed := invocation.Trace.Seed
	if invocation.Trace.Address != "" {
		raw, err := address.Decode(invocation.Trace.Address)
		if err != nil {
			return nil, err
		}
		seed.Address = raw
	}
	seedResult, boundaries, err := engine.reader.TraceSeeds(
		ctx,
		snapshot,
		repository.TraceQuery{
			Direction: invocation.Trace.Direction,
			Seed:      seed,
			Asset:     invocation.Trace.Asset,
		},
		limits.DefaultAddressPage,
	)
	if err != nil {
		return nil, err
	}
	trace := model.Trace{
		Direction:  string(invocation.Trace.Direction),
		Asset:      invocation.Trace.Asset,
		Hyperedges: make([]model.FlowHyperedge, 0),
	}
	response := model.NewResponse(snapshot, trace)
	response.UnresolvedPartialHistory = append(
		response.UnresolvedPartialHistory,
		boundaries...,
	)
	if seedResult.Truncated {
		response.Truncation.Truncated = true
		response.Truncation.Reason = "address_seed_limit"
	}

	frontier := uniqueSorted(seedResult.UTxOs)
	visited := make(map[string]struct{}, len(frontier))
	for _, ref := range frontier {
		if uint32(len(visited)) >= invocation.Trace.Limits.MaxNodes {
			response.Truncation.Truncated = true
			response.Truncation.Reason = "max_nodes"
			response.Truncation.ContinuationFrontier = append(
				response.Truncation.ContinuationFrontier,
				ref,
			)
			continue
		}
		visited[ref.String()] = struct{}{}
	}
	if len(response.Truncation.ContinuationFrontier) > 0 {
		frontier = frontier[:len(visited)]
	}
	seenEdges := make(map[string]struct{})

	for depth := uint32(0); depth < invocation.Trace.Limits.MaxDepth && len(frontier) > 0; depth++ {
		layerCtx, cancel := context.WithTimeout(ctx, invocation.Trace.Limits.LayerTimeout)
		next := make([]model.UTxORef, 0)
		layerTimedOut := false
		for offset := 0; offset < len(frontier); offset += int(invocation.Trace.Limits.FrontierBatch) {
			end := offset + int(invocation.Trace.Limits.FrontierBatch)
			if end > len(frontier) {
				end = len(frontier)
			}
			var (
				edges      []model.FlowHyperedge
				unresolved []model.PartialHistoryBoundary
				err        error
			)
			if invocation.Trace.Direction == repository.Forward {
				edges, unresolved, err = engine.reader.ExpandForward(
					layerCtx,
					snapshot,
					frontier[offset:end],
					invocation.Trace.Asset,
				)
			} else {
				edges, unresolved, err = engine.reader.ExpandReverse(
					layerCtx,
					snapshot,
					frontier[offset:end],
					invocation.Trace.Asset,
				)
			}
			response.UnresolvedPartialHistory = append(
				response.UnresolvedPartialHistory,
				unresolved...,
			)
			if err != nil {
				if ctx.Err() != nil {
					cancel()
					return nil, ctx.Err()
				}
				if errors.Is(err, context.DeadlineExceeded) || errors.Is(layerCtx.Err(), context.DeadlineExceeded) {
					layerTimedOut = true
					response.Truncation.Truncated = true
					response.Truncation.Reason = "layer_timeout"
					response.Truncation.ContinuationFrontier = append(
						response.Truncation.ContinuationFrontier,
						frontier[offset:]...,
					)
					break
				}
				cancel()
				return nil, err
			}
			for _, edge := range edges {
				if _, exists := seenEdges[edge.Transaction.String()]; !exists {
					seenEdges[edge.Transaction.String()] = struct{}{}
					response.Data.Hyperedges = append(response.Data.Hyperedges, edge)
				}
				if invocation.Trace.Direction == repository.Forward {
					for _, output := range edge.ProducedOutputs {
						if assetMatches(output, invocation.Trace.Asset) {
							next = append(next, output.Ref)
						}
					}
				} else {
					for _, output := range edge.ConsumedInputValues {
						if assetMatches(output, invocation.Trace.Asset) {
							next = append(next, output.Ref)
						}
					}
				}
			}
		}
		cancel()
		response.Data.Depth = depth + 1
		if layerTimedOut {
			break
		}
		next = uniqueSorted(next)
		unvisited := next[:0]
		for _, ref := range next {
			if _, exists := visited[ref.String()]; exists {
				continue
			}
			if uint32(len(visited)) >= invocation.Trace.Limits.MaxNodes {
				response.Truncation.Truncated = true
				response.Truncation.Reason = "max_nodes"
				response.Truncation.ContinuationFrontier = append(
					response.Truncation.ContinuationFrontier,
					ref,
				)
				continue
			}
			visited[ref.String()] = struct{}{}
			unvisited = append(unvisited, ref)
		}
		frontier = unvisited
		if response.Truncation.Reason == "max_nodes" {
			response.Truncation.ContinuationFrontier = append(
				response.Truncation.ContinuationFrontier,
				frontier...,
			)
			break
		}
	}
	if len(frontier) > 0 && response.Data.Depth >= invocation.Trace.Limits.MaxDepth &&
		!response.Truncation.Truncated {
		response.Truncation.Truncated = true
		response.Truncation.Reason = "max_depth"
		response.Truncation.ContinuationFrontier = append(
			response.Truncation.ContinuationFrontier,
			frontier...,
		)
	}
	response.Truncation.ContinuationFrontier = uniqueSorted(
		response.Truncation.ContinuationFrontier,
	)
	response.Truncation.LosslessResume = false
	response.Data.Visited = uint32(len(visited))
	response.QueryMetrics = collector.Snapshot()
	return response, nil
}

func uniqueSorted(values []model.UTxORef) []model.UTxORef {
	seen := make(map[string]model.UTxORef, len(values))
	for _, value := range values {
		seen[value.String()] = value
	}
	keys := make([]string, 0, len(seen))
	for key := range seen {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]model.UTxORef, 0, len(keys))
	for _, key := range keys {
		result = append(result, seen[key])
	}
	return result
}

func assetMatches(output model.Output, selector model.AssetSelector) bool {
	if selector.ADA {
		return output.Lovelace > 0
	}
	for _, asset := range output.Assets {
		if asset.PolicyID == selector.PolicyID &&
			string(asset.Name) == string(selector.AssetName) &&
			asset.Quantity > 0 {
			return true
		}
	}
	return false
}
