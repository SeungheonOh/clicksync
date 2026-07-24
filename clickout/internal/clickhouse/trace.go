package clickhouse

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/clicksync-project/clickout/internal/cursor"
	"github.com/clicksync-project/clickout/internal/limits"
	"github.com/clicksync-project/clickout/internal/model"
	"github.com/clicksync-project/clickout/internal/repository"
)

const maxTraceSeedWindows = 16

func traceHydrationPhaseLimits() phaseLimits {
	return hydrationPhaseLimits(uint64(limits.HardMaxTraceNodes) + 1)
}

func validateHyperedgeResources(edge model.FlowHyperedge, maxNodes uint32) error {
	nodes := make(map[string]struct{}, len(edge.Inputs)+len(edge.Outputs))
	for _, input := range edge.Inputs {
		nodes[input.Source.String()] = struct{}{}
	}
	for _, output := range edge.Outputs {
		nodes[output.Ref.String()] = struct{}{}
	}
	if len(nodes) > int(maxNodes) {
		return &ResourceLimitError{
			Phase: "trace_hyperedge",
			Cause: fmt.Errorf(
				"transaction %s has %d input/output nodes, limit %d",
				edge.Transaction.Hash,
				len(nodes),
				maxNodes,
			),
		}
	}
	return nil
}

func rewrapTraceAddressContinuation(
	encoded string,
	address []byte,
	direction repository.TraceDirection,
	asset model.AssetSelector,
	snapshot model.Snapshot,
) (string, string, error) {
	value, err := cursor.Decode(
		encoded,
		addressScope(address, "history"),
	)
	if err != nil || !value.Snapshot.SamePin(snapshot) {
		return "", "", cursor.ErrInvalid
	}
	continuation, err := cursor.Encode(cursor.Value{
		Scope:    TraceAddressScope(address, direction, asset),
		Snapshot: snapshot,
		LastKey:  value.LastKey,
	})
	if err != nil {
		return "", "", err
	}
	return value.LastKey, continuation, nil
}

func (store *Store) TraceSeeds(
	ctx context.Context,
	snapshot model.Snapshot,
	query repository.TraceQuery,
	limit uint32,
) (repository.TraceSeedResult, []model.PartialHistoryBoundary, error) {
	if query.Seed.UTxO != nil {
		output, err := store.outputByRef(ctx, snapshot, *query.Seed.UTxO)
		if err != nil {
			if errors.Is(err, ErrNotFound) &&
				!snapshot.Identity.CompleteHistory {
				return repository.TraceSeedResult{}, []model.PartialHistoryBoundary{{
					UTxO:   *query.Seed.UTxO,
					Reason: "trace seed is outside this partial-history dataset",
				}}, nil
			}
			return repository.TraceSeedResult{}, nil, err
		}
		if !outputHasAsset(output, query.Asset) {
			return repository.TraceSeedResult{UTxOs: make([]model.UTxORef, 0)}, nil, nil
		}
		return repository.TraceSeedResult{UTxOs: []model.UTxORef{*query.Seed.UTxO}}, nil, nil
	}
	if query.Seed.Tx != nil {
		transactions, boundaries, err := store.transactionsByTx(
			ctx,
			snapshot,
			[]model.Hash32{*query.Seed.Tx},
		)
		if err != nil {
			return repository.TraceSeedResult{}, nil, err
		}
		if len(transactions) != 1 ||
			transactions[0].Hash != *query.Seed.Tx {
			return repository.TraceSeedResult{}, nil, ErrConflictingRow
		}
		seeds := make([]model.UTxORef, 0, len(transactions[0].Outputs))
		for _, output := range transactions[0].Outputs {
			if outputHasAsset(output, query.Asset) {
				seeds = append(seeds, output.Ref)
			}
		}
		seeds = uniqueRefs(seeds)
		result := repository.TraceSeedResult{UTxOs: seeds}
		if len(seeds) > int(limit) {
			result.UTxOs = append(
				[]model.UTxORef(nil),
				seeds[:limit]...,
			)
			result.TruncationReason = model.TruncationMaxNodes
			result.ContinuationFrontier = append(
				[]model.UTxORef(nil),
				seeds[limit:]...,
			)
		}
		if !result.Valid() {
			return repository.TraceSeedResult{}, nil, ErrConflictingRow
		}
		return result, boundaries, nil
	}
	if len(query.Seed.Address) > 0 {
		seeds := make([]model.UTxORef, 0, limit)
		boundaries := make([]model.PartialHistoryBoundary, 0)
		lastKey := query.SeedLastKey
		truncated := false
		continuation := ""
		for window := 0; window < maxTraceSeedWindows; window++ {
			remaining := limit - uint32(len(seeds))
			if remaining == 0 {
				break
			}
			page, pageBoundaries, err := store.Address(ctx, snapshot, repository.AddressQuery{
				Address: query.Seed.Address,
				State:   "history",
				Limit:   remaining,
				LastKey: lastKey,
			})
			if err != nil {
				return repository.TraceSeedResult{}, nil, err
			}
			boundaries = append(boundaries, pageBoundaries...)
			for _, item := range page.Items {
				if !outputHasAsset(item.Output, query.Asset) {
					continue
				}
				seeds = append(seeds, item.Output.Ref)
			}
			if page.Cursor == "" {
				break
			}
			nextKey, rewrapped, err := rewrapTraceAddressContinuation(
				page.Cursor,
				query.Seed.Address,
				query.Direction,
				query.Asset,
				snapshot,
			)
			if err != nil {
				return repository.TraceSeedResult{}, nil, err
			}
			continuation = rewrapped
			if len(seeds) == int(limit) {
				truncated = true
				break
			}
			lastKey = nextKey
			if window == maxTraceSeedWindows-1 {
				truncated = true
			}
		}
		result := repository.TraceSeedResult{
			UTxOs: uniqueRefs(seeds),
		}
		if truncated {
			result.TruncationReason = model.TruncationAddressSeedLimit
			result.ContinuationCursor = continuation
		}
		if !result.Valid() {
			return repository.TraceSeedResult{}, nil, ErrConflictingRow
		}
		return result, boundaries, nil
	}
	return repository.TraceSeedResult{}, nil, errors.New("missing trace seed")
}

func (store *Store) ExpandForward(
	ctx context.Context,
	snapshot model.Snapshot,
	sources []model.UTxORef,
	_ model.AssetSelector,
	budget repository.ExpansionBudget,
) (repository.ExpansionResult, []model.PartialHistoryBoundary, error) {
	if len(sources) == 0 {
		return repository.ExpansionResult{Hyperedges: []model.FlowHyperedge{}}, nil, nil
	}
	if err := validateExpansionBudget(budget); err != nil {
		return repository.ExpansionResult{}, nil, err
	}
	predicate, values := tuplePredicate("i.source_tx_hash", "i.source_output_index", sources)
	exclusion, exclusionValues := traceExclusion("i.tx_hash", budget.ExcludeTransactions)
	sql := targetedFactSQL(`
        SELECT *
        FROM inputs AS i
        WHERE i.is_consumed
          AND `+predicate+`
          AND i.publication_id <= publication_watermark
`, `
SELECT DISTINCT
    i.tx_hash,
    toUInt8(
        ifNull(b.present, toUInt8(0)) = 1
        AND ifNull(b.block_number, toUInt64(0)) = i.block_number
        AND i.tx_order < ifNull(b.transaction_count, toUInt32(0))
    )
FROM fact_candidates AS i
INNER JOIN active_candidate_publications AS ap
    ON i.publication_id = ap.publication_id
LEFT JOIN candidate_blocks AS b
    ON i.publication_id = b.publication_id
WHERE 1`+exclusion+`
ORDER BY i.tx_hash
LIMIT ?`)
	arguments := append(values, exclusionValues...)
	arguments = append(arguments, uint64(budget.MaxEdges)+1)
	queryCtx, finish := store.instrumentPhase(
		ctx,
		"trace_forward_candidates",
		candidatePhaseLimits(uint64(budget.MaxEdges)+1),
	)
	rows, err := store.conn.Query(
		queryCtx,
		sql,
		activeArguments(snapshot, arguments...)...,
	)
	if err != nil {
		finish()
		return repository.ExpansionResult{}, nil, mapQueryError("trace_forward_candidates", err)
	}
	hashes, err := scanCandidateHashes(rows)
	rows.Close()
	finish()
	if err != nil {
		return repository.ExpansionResult{}, nil, mapQueryError("trace_forward_candidates", err)
	}
	return store.hydrateExpansion(ctx, snapshot, hashes, budget)
}

func (store *Store) ExpandReverse(
	ctx context.Context,
	snapshot model.Snapshot,
	targets []model.UTxORef,
	_ model.AssetSelector,
	budget repository.ExpansionBudget,
) (repository.ExpansionResult, []model.PartialHistoryBoundary, error) {
	if len(targets) == 0 {
		return repository.ExpansionResult{Hyperedges: []model.FlowHyperedge{}}, nil, nil
	}
	if err := validateExpansionBudget(budget); err != nil {
		return repository.ExpansionResult{}, nil, err
	}
	predicate, values := tuplePredicate("o.tx_hash", "o.output_index", targets)
	exclusion, exclusionValues := traceExclusion("o.tx_hash", budget.ExcludeTransactions)
	sql := targetedFactSQL(`
        SELECT *
        FROM outputs AS o
        WHERE `+predicate+`
          AND o.publication_id <= publication_watermark
`, `
SELECT DISTINCT
    o.tx_hash,
    toUInt8(
        ifNull(b.present, toUInt8(0)) = 1
        AND ifNull(b.block_number, toUInt64(0)) = o.block_number
        AND o.tx_order < ifNull(b.transaction_count, toUInt32(0))
    )
FROM fact_candidates AS o
INNER JOIN active_candidate_publications AS ap
    ON o.publication_id = ap.publication_id
LEFT JOIN candidate_blocks AS b
    ON o.publication_id = b.publication_id
WHERE 1`+exclusion+`
ORDER BY o.tx_hash
LIMIT ?`)
	arguments := append(values, exclusionValues...)
	arguments = append(arguments, uint64(budget.MaxEdges)+1)
	queryCtx, finish := store.instrumentPhase(
		ctx,
		"trace_reverse_candidates",
		candidatePhaseLimits(uint64(budget.MaxEdges)+1),
	)
	rows, err := store.conn.Query(
		queryCtx,
		sql,
		activeArguments(snapshot, arguments...)...,
	)
	if err != nil {
		finish()
		return repository.ExpansionResult{}, nil, mapQueryError("trace_reverse_candidates", err)
	}
	hashes, err := scanCandidateHashes(rows)
	rows.Close()
	finish()
	if err != nil {
		return repository.ExpansionResult{}, nil, mapQueryError("trace_reverse_candidates", err)
	}
	return store.hydrateExpansion(ctx, snapshot, hashes, budget)
}

func validateExpansionBudget(budget repository.ExpansionBudget) error {
	if budget.MaxEdges == 0 || budget.MaxEdges > limits.HardMaxTraceEdges {
		return limits.ErrEdgesOutOfRange
	}
	if budget.MaxNodes == 0 || budget.MaxNodes > limits.HardMaxTraceNodes {
		return limits.ErrNodesOutOfRange
	}
	previous := ""
	for _, hash := range budget.ExcludeTransactions {
		current := hash.String()
		if previous != "" && previous >= current {
			return errors.New("excluded transaction hashes must be strictly sorted and unique")
		}
		previous = current
	}
	return nil
}

func traceExclusion(column string, hashes []model.Hash32) (string, []any) {
	if len(hashes) == 0 {
		return "", nil
	}
	predicate, values := hashPredicate(column, hashes)
	return " AND NOT (" + predicate + ")", values
}

func (store *Store) hydrateExpansion(
	ctx context.Context,
	snapshot model.Snapshot,
	candidates []model.Hash32,
	budget repository.ExpansionBudget,
) (repository.ExpansionResult, []model.PartialHistoryBoundary, error) {
	candidates, candidateTruncated := acceptTraceCandidates(candidates, budget.MaxEdges)
	nodesByTransaction, err := store.traceCandidateNodes(ctx, snapshot, candidates)
	if err != nil {
		return repository.ExpansionResult{}, nil, err
	}
	candidates, nodeTruncated, err := fitTraceCandidateNodes(
		candidates,
		nodesByTransaction,
		budget.MaxNodes,
	)
	if err != nil {
		return repository.ExpansionResult{}, nil, err
	}
	edges, boundaries, err := store.hyperedgesByTx(ctx, snapshot, candidates)
	if err != nil {
		return repository.ExpansionResult{}, nil, err
	}
	verified, postHydrationTruncated, err := fitTraceEdgeNodes(edges, budget.MaxNodes)
	if err != nil {
		return repository.ExpansionResult{}, nil, err
	}
	if postHydrationTruncated || len(verified) != len(edges) {
		return repository.ExpansionResult{}, nil, ErrConflictingRow
	}
	result := repository.ExpansionResult{Hyperedges: edges}
	switch {
	case nodeTruncated:
		result.TruncationReason = model.TruncationMaxNodes
	case candidateTruncated:
		result.TruncationReason = model.TruncationMaxEdges
	}
	if !result.Valid() {
		return repository.ExpansionResult{}, nil, ErrConflictingRow
	}
	return result, boundaries, nil
}

func (store *Store) traceCandidateNodes(
	ctx context.Context,
	snapshot model.Snapshot,
	candidates []model.Hash32,
) (map[string]map[string]struct{}, error) {
	result := make(map[string]map[string]struct{}, len(candidates))
	if len(candidates) == 0 {
		return result, nil
	}
	for _, hash := range candidates {
		result[hash.String()] = make(map[string]struct{})
	}
	if err := store.traceInputCandidateNodes(ctx, snapshot, candidates, result); err != nil {
		return nil, err
	}
	if err := store.traceOutputCandidateNodes(ctx, snapshot, candidates, result); err != nil {
		return nil, err
	}
	for _, hash := range candidates {
		if len(result[hash.String()]) == 0 {
			return nil, ErrConflictingRow
		}
	}
	return result, nil
}

func (store *Store) traceInputCandidateNodes(
	ctx context.Context,
	snapshot model.Snapshot,
	candidates []model.Hash32,
	result map[string]map[string]struct{},
) error {
	predicate, values := hashPredicate("i.tx_hash", candidates)
	sql := targetedFactSQL(`
        SELECT *
        FROM inputs AS i
        WHERE `+predicate+`
          AND i.publication_id <= publication_watermark
`, `
SELECT
    tx_hash,
    groupArray(source_tx_hash),
    groupArray(source_output_index),
    min(block_present)
FROM
(
    SELECT DISTINCT
        i.tx_hash,
        i.source_tx_hash,
        i.source_output_index,
        toUInt8(
            ifNull(b.present, toUInt8(0)) = 1
            AND ifNull(b.block_number, toUInt64(0)) = i.block_number
            AND i.tx_order < ifNull(b.transaction_count, toUInt32(0))
        ) AS block_present
    FROM fact_candidates AS i
    INNER JOIN active_candidate_publications AS ap
        ON i.publication_id = ap.publication_id
    LEFT JOIN candidate_blocks AS b
        ON i.publication_id = b.publication_id
    ORDER BY i.tx_hash, i.source_tx_hash, i.source_output_index
)
GROUP BY tx_hash
ORDER BY tx_hash`)
	return store.scanTraceCandidateNodes(
		ctx,
		"trace_input_node_identities",
		sql,
		activeArguments(snapshot, values...),
		uint64(len(candidates)),
		result,
	)
}

func (store *Store) traceOutputCandidateNodes(
	ctx context.Context,
	snapshot model.Snapshot,
	candidates []model.Hash32,
	result map[string]map[string]struct{},
) error {
	predicate, values := hashPredicate("o.tx_hash", candidates)
	sql := targetedFactSQL(`
        SELECT *
        FROM outputs AS o
        WHERE `+predicate+`
          AND o.publication_id <= publication_watermark
`, `
SELECT
    tx_hash,
    groupArray(tx_hash),
    groupArray(output_index),
    min(block_present)
FROM
(
    SELECT DISTINCT
        o.tx_hash,
        o.output_index,
        toUInt8(
            ifNull(b.present, toUInt8(0)) = 1
            AND ifNull(b.block_number, toUInt64(0)) = o.block_number
            AND o.tx_order < ifNull(b.transaction_count, toUInt32(0))
        ) AS block_present
    FROM fact_candidates AS o
    INNER JOIN active_candidate_publications AS ap
        ON o.publication_id = ap.publication_id
    LEFT JOIN candidate_blocks AS b
        ON o.publication_id = b.publication_id
    ORDER BY o.tx_hash, o.output_index
)
GROUP BY tx_hash
ORDER BY tx_hash`)
	return store.scanTraceCandidateNodes(
		ctx,
		"trace_output_node_identities",
		sql,
		activeArguments(snapshot, values...),
		uint64(len(candidates)),
		result,
	)
}

func (store *Store) scanTraceCandidateNodes(
	ctx context.Context,
	phase string,
	sql string,
	arguments []any,
	maxRows uint64,
	result map[string]map[string]struct{},
) error {
	queryCtx, finish := store.instrumentPhase(
		ctx,
		phase,
		hydrationPhaseLimits(maxRows),
	)
	defer finish()
	rows, err := store.conn.Query(queryCtx, sql, arguments...)
	if err != nil {
		return mapQueryError(phase, err)
	}
	defer rows.Close()
	seenTransactions := make(map[string]struct{}, maxRows)
	for rows.Next() {
		var txHash []byte
		var nodeHashes []string
		var nodeIndexes []uint32
		if err := (&blockPresenceScanner{row: rows}).Scan(
			&txHash,
			&nodeHashes,
			&nodeIndexes,
		); err != nil {
			return mapQueryError(phase, err)
		}
		tx, err := model.Hash32FromBytes(txHash)
		if err != nil {
			return err
		}
		key := tx.String()
		nodes, exists := result[key]
		if !exists {
			return ErrConflictingRow
		}
		if _, duplicate := seenTransactions[key]; duplicate {
			return ErrConflictingRow
		}
		seenTransactions[key] = struct{}{}
		if len(nodeHashes) != len(nodeIndexes) {
			return errors.New("trace node identity arrays have unequal lengths")
		}
		var previous model.UTxORef
		for index := range nodeHashes {
			hash, err := model.Hash32FromBytes([]byte(nodeHashes[index]))
			if err != nil {
				return err
			}
			ref := model.UTxORef{TxHash: hash, Index: nodeIndexes[index]}
			if index > 0 && compareUTxORefs(previous, ref) >= 0 {
				return ErrConflictingRow
			}
			previous = ref
			nodes[ref.String()] = struct{}{}
		}
	}
	if err := rows.Err(); err != nil {
		return mapQueryError(phase, err)
	}
	return nil
}

func compareUTxORefs(left, right model.UTxORef) int {
	if compared := bytes.Compare(left.TxHash[:], right.TxHash[:]); compared != 0 {
		return compared
	}
	switch {
	case left.Index < right.Index:
		return -1
	case left.Index > right.Index:
		return 1
	default:
		return 0
	}
}

func fitTraceCandidateNodes(
	candidates []model.Hash32,
	nodesByTransaction map[string]map[string]struct{},
	maxNodes uint32,
) ([]model.Hash32, bool, error) {
	seen := make(map[string]struct{}, maxNodes)
	for index, candidate := range candidates {
		nodes, exists := nodesByTransaction[candidate.String()]
		if !exists || len(nodes) == 0 {
			return nil, false, ErrConflictingRow
		}
		added := 0
		for node := range nodes {
			if _, exists := seen[node]; !exists {
				added++
			}
		}
		if len(seen)+added > int(maxNodes) {
			if index == 0 {
				return nil, false, &ResourceLimitError{
					Phase: "trace_hyperedge",
					Cause: fmt.Errorf(
						"transaction %s has %d unique input/output nodes, remaining limit %d",
						candidate,
						len(nodes),
						maxNodes,
					),
				}
			}
			return candidates[:index], true, nil
		}
		for node := range nodes {
			seen[node] = struct{}{}
		}
	}
	return candidates, false, nil
}

func fitTraceEdgeNodes(
	edges []model.FlowHyperedge,
	maxNodes uint32,
) ([]model.FlowHyperedge, bool, error) {
	nodes := make(map[string]map[string]struct{}, len(edges))
	candidates := make([]model.Hash32, len(edges))
	for index, edge := range edges {
		candidates[index] = edge.Transaction.Hash
		edgeNodes := make(map[string]struct{}, len(edge.Inputs)+len(edge.Outputs))
		for _, input := range edge.Inputs {
			edgeNodes[input.Source.String()] = struct{}{}
		}
		for _, output := range edge.Outputs {
			edgeNodes[output.Ref.String()] = struct{}{}
		}
		nodes[edge.Transaction.Hash.String()] = edgeNodes
	}
	accepted, truncated, err := fitTraceCandidateNodes(candidates, nodes, maxNodes)
	if err != nil {
		return nil, false, err
	}
	return edges[:len(accepted)], truncated, nil
}

func acceptTraceCandidates(
	candidates []model.Hash32,
	limit uint32,
) ([]model.Hash32, bool) {
	truncated := len(candidates) > int(limit)
	if truncated {
		return candidates[:limit], true
	}
	return candidates, false
}

func (store *Store) hyperedgesByTx(
	ctx context.Context,
	snapshot model.Snapshot,
	hashes []model.Hash32,
) ([]model.FlowHyperedge, []model.PartialHistoryBoundary, error) {
	if len(hashes) == 0 {
		return []model.FlowHyperedge{}, nil, nil
	}
	transactions, boundaries, err := store.transactionsByTx(
		ctx,
		snapshot,
		hashes,
	)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, nil, ErrConflictingRow
		}
		return nil, nil, err
	}
	edges := make([]model.FlowHyperedge, len(transactions))
	for index, transaction := range transactions {
		edges[index] = model.NewFlowHyperedge(transaction)
	}
	return edges, boundaries, nil
}

func (store *Store) outputsByRefs(
	ctx context.Context,
	snapshot model.Snapshot,
	refs []model.UTxORef,
	contextBodies *contextBodyPool,
) ([]model.Output, []model.PartialHistoryBoundary, error) {
	if len(refs) == 0 {
		return []model.Output{}, nil, nil
	}
	predicate, values := tuplePredicate("o.tx_hash", "o.output_index", refs)
	sql := targetedFactSQL(`
        SELECT *
        FROM outputs AS o
        WHERE `+predicate+`
          AND o.publication_id <= publication_watermark
`, `
SELECT`+outputColumns+`,
    toUInt8(
        ifNull(b.present, toUInt8(0)) = 1
        AND ifNull(b.block_number, toUInt64(0)) = o.block_number
        AND o.tx_order < ifNull(b.transaction_count, toUInt32(0))
    ),
    ifNull(b.era, ''),
    ifNull(b.synthetic, false)
FROM fact_candidates AS o
INNER JOIN active_candidate_publications AS ap
    ON o.publication_id = ap.publication_id
LEFT JOIN candidate_blocks AS b ON o.publication_id = b.publication_id
ORDER BY o.tx_hash, o.body_ordinal, o.output_index, o.publication_id`)
	queryCtx, finish := store.instrumentPhase(
		ctx,
		"trace_output_values",
		hydrationPhaseLimits(uint64(len(refs))+1),
	)
	defer finish()
	rows, err := store.conn.Query(queryCtx, sql, activeArguments(snapshot, values...)...)
	if err != nil {
		return nil, nil, mapQueryError("trace_output_values", err)
	}
	defer rows.Close()
	outputs := make([]model.Output, 0, len(refs))
	found := make(map[string]struct{}, len(refs))
	for rows.Next() {
		owner := &outputOwnerScanner{row: rows}
		output, err := scanOutput(owner)
		if err != nil {
			return nil, nil, mapQueryError("trace_output_values", err)
		}
		if err := validateOutputEraCapabilities(
			output,
			owner.era,
			owner.synthetic,
		); err != nil {
			return nil, nil, err
		}
		if _, duplicate := found[output.Ref.String()]; duplicate {
			return nil, nil, ErrConflictingRow
		}
		found[output.Ref.String()] = struct{}{}
		outputs = append(outputs, output)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, mapQueryError("trace_output_values", err)
	}
	if err := validateOutputRows(outputs); err != nil {
		return nil, nil, err
	}
	if err := store.hydrateInlineDatumsWithBodies(
		ctx,
		outputs,
		contextBodies,
	); err != nil {
		return nil, nil, err
	}
	boundaries := make([]model.PartialHistoryBoundary, 0)
	for _, ref := range refs {
		if _, exists := found[ref.String()]; exists {
			continue
		}
		if snapshot.Identity.CompleteHistory {
			return nil, nil, ErrNotFound
		}
		boundaries = append(boundaries, model.PartialHistoryBoundary{
			UTxO:   ref,
			Reason: "source output is outside this partial-history dataset",
		})
	}
	return outputs, boundaries, nil
}

func uniqueRefs(refs []model.UTxORef) []model.UTxORef {
	seen := make(map[string]model.UTxORef, len(refs))
	for _, ref := range refs {
		seen[ref.String()] = ref
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

func hashPredicate(column string, hashes []model.Hash32) (string, []any) {
	placeholders := make([]string, len(hashes))
	values := make([]any, len(hashes))
	for index := range hashes {
		placeholders[index] = "?"
		values[index] = hashArgument(hashes[index])
	}
	return column + " IN (" + strings.Join(placeholders, ",") + ")", values
}

func scanHashes(rows interface {
	Next() bool
	Scan(...any) error
	Err() error
}) ([]model.Hash32, error) {
	result := make([]model.Hash32, 0)
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		hash, err := model.Hash32FromBytes(raw)
		if err != nil {
			return nil, err
		}
		result = append(result, hash)
	}
	return result, rows.Err()
}

func scanCandidateHashes(rows interface {
	Next() bool
	Scan(...any) error
	Err() error
}) ([]model.Hash32, error) {
	result := make([]model.Hash32, 0)
	for rows.Next() {
		var raw []byte
		if err := (&blockPresenceScanner{row: rows}).Scan(&raw); err != nil {
			return nil, err
		}
		hash, err := model.Hash32FromBytes(raw)
		if err != nil {
			return nil, err
		}
		result = append(result, hash)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for index := 1; index < len(result); index++ {
		if bytes.Compare(result[index-1][:], result[index][:]) >= 0 {
			return nil, errors.New(
				"trace transaction candidates are not strictly sorted and unique",
			)
		}
	}
	return result, nil
}

func outputHasAsset(output model.Output, selector model.AssetSelector) bool {
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
