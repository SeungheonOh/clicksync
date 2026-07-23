package app

import (
	"context"
	"errors"
	"sort"

	"github.com/clicksync-project/clickout/internal/address"
	"github.com/clicksync-project/clickout/internal/cli"
	"github.com/clicksync-project/clickout/internal/cursor"
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
	seed := invocation.Trace.Seed
	at := invocation.At
	seedLastKey := ""
	if invocation.Trace.Address != "" {
		raw, err := address.Decode(invocation.Trace.Address)
		if err != nil {
			return nil, err
		}
		seed.Address = raw
		if invocation.Trace.SeedCursor != "" {
			value, err := cursor.DecodePinned(
				invocation.Trace.SeedCursor,
				addressScope(raw, "history"),
			)
			if err != nil {
				return nil, err
			}
			at = model.AtPoint{Event: &value.SnapshotEvent}
			seedLastKey = value.LastKey
		}
	}
	snapshot, err := engine.reader.Snapshot(ctx, at)
	if err != nil {
		return nil, err
	}
	seedLimit := limits.DefaultAddressPage
	if len(seed.Address) > 0 && invocation.Trace.Limits.MaxNodes < seedLimit {
		seedLimit = invocation.Trace.Limits.MaxNodes
	}
	seedResult, boundaries, err := engine.reader.TraceSeeds(
		ctx,
		snapshot,
		repository.TraceQuery{
			Direction:   invocation.Trace.Direction,
			Seed:        seed,
			SeedLastKey: seedLastKey,
			Asset:       invocation.Trace.Asset,
		},
		seedLimit,
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
		response.Truncation.ContinuationCursor = seedResult.ContinuationCursor
		response.Truncation.LosslessResume = false
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
		layerTruncated := false
		for offset := 0; offset < len(frontier); {
			remainingEdges := invocation.Trace.Limits.MaxEdges - uint32(len(seenEdges))
			if remainingEdges == 0 {
				response.Truncation.Truncated = true
				response.Truncation.Reason = "max_edges"
				response.Truncation.ContinuationFrontier = append(
					response.Truncation.ContinuationFrontier,
					frontier[offset:]...,
				)
				layerTruncated = true
				break
			}
			batchSize := invocation.Trace.Limits.FrontierBatch
			if batchSize > remainingEdges {
				batchSize = remainingEdges
			}
			remainingNodes := invocation.Trace.Limits.MaxNodes - uint32(len(visited))
			if remainingNodes > 0 && batchSize > remainingNodes {
				batchSize = remainingNodes
			}
			if batchSize == 0 {
				batchSize = 1
			}
			batchStart := offset
			end := offset + int(batchSize)
			if end > len(frontier) {
				end = len(frontier)
			}
			var (
				expansion  repository.ExpansionResult
				unresolved []model.PartialHistoryBoundary
				err        error
			)
			budget := repository.ExpansionBudget{
				MaxEdges:            remainingEdges,
				MaxNodes:            max(remainingNodes, 1),
				ExcludeTransactions: sortedEdgeHashes(seenEdges),
			}
			if invocation.Trace.Direction == repository.Forward {
				expansion, unresolved, err = engine.reader.ExpandForward(
					layerCtx,
					snapshot,
					frontier[offset:end],
					invocation.Trace.Asset,
					budget,
				)
			} else {
				expansion, unresolved, err = engine.reader.ExpandReverse(
					layerCtx,
					snapshot,
					frontier[offset:end],
					invocation.Trace.Asset,
					budget,
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
			if len(expansion.Hyperedges) > int(remainingEdges) {
				cancel()
				return nil, errors.New("repository exceeded trace expansion edge budget")
			}
			for index, edge := range expansion.Hyperedges {
				key := edge.Transaction.String()
				if _, exists := seenEdges[key]; exists ||
					(index > 0 &&
						expansion.Hyperedges[index-1].Transaction.String() >= key) {
					cancel()
					return nil, errors.New("repository returned duplicate or unordered hyperedges")
				}
				seenEdges[key] = struct{}{}
				response.Data.Hyperedges = append(response.Data.Hyperedges, edge)
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
			offset = end
			if expansion.Truncated {
				response.Truncation.Truncated = true
				response.Truncation.Reason = "max_edges"
				response.Truncation.ContinuationFrontier = append(
					response.Truncation.ContinuationFrontier,
					frontier[batchStart:]...,
				)
				layerTruncated = true
				break
			}
		}
		cancel()
		response.Data.Depth = depth + 1
		if layerTimedOut || layerTruncated {
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

func sortedEdgeHashes(seen map[string]struct{}) []model.Hash32 {
	keys := make([]string, 0, len(seen))
	for key := range seen {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]model.Hash32, 0, len(keys))
	for _, key := range keys {
		hash, err := model.ParseHash32(key)
		if err != nil {
			panic("invalid internal transaction hash: " + key)
		}
		result = append(result, hash)
	}
	return result
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
