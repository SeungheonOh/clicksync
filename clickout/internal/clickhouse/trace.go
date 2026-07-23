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
	nodes := make(map[string]struct{}, len(edge.Inputs)+len(edge.ProducedOutputs))
	for _, input := range edge.Inputs {
		nodes[input.Source.String()] = struct{}{}
	}
	for _, output := range edge.ProducedOutputs {
		nodes[output.Ref.String()] = struct{}{}
	}
	if len(nodes) > int(maxNodes) {
		return &ResourceLimitError{
			Phase: "trace_hyperedge",
			Cause: fmt.Errorf(
				"transaction %s has %d input/output nodes, limit %d",
				edge.Transaction,
				len(nodes),
				maxNodes,
			),
		}
	}
	return nil
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
			if errors.Is(err, ErrNotFound) && !snapshot.CompleteHistory {
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
		outputs, err := store.outputsByTx(ctx, snapshot, *query.Seed.Tx)
		if err != nil {
			return repository.TraceSeedResult{}, nil, err
		}
		seeds := make([]model.UTxORef, 0, len(outputs))
		for _, output := range outputs {
			if outputHasAsset(output, query.Asset) {
				seeds = append(seeds, output.Ref)
			}
		}
		truncated := len(seeds) > int(limit)
		if truncated {
			seeds = seeds[:limit]
		}
		return repository.TraceSeedResult{UTxOs: seeds, Truncated: truncated}, nil, nil
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
			continuation = page.Cursor
			if len(seeds) == int(limit) {
				truncated = true
				break
			}
			value, err := cursor.DecodePinned(
				page.Cursor,
				addressScope(query.Seed.Address, "history"),
			)
			if err != nil || value.SnapshotEvent != snapshot.Event {
				return repository.TraceSeedResult{}, nil, cursor.ErrInvalid
			}
			lastKey = value.LastKey
			if window == maxTraceSeedWindows-1 {
				truncated = true
			}
		}
		return repository.TraceSeedResult{
			UTxOs:              seeds,
			Truncated:          truncated,
			ContinuationCursor: continuation,
		}, boundaries, nil
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
SELECT DISTINCT i.tx_hash
FROM fact_candidates AS i
INNER JOIN active_candidate_publications AS ap
    ON i.publication_id = ap.publication_id
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
SELECT DISTINCT o.tx_hash
FROM fact_candidates AS o
INNER JOIN active_candidate_publications AS ap
    ON o.publication_id = ap.publication_id
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
	candidates, truncated := acceptTraceCandidates(candidates, budget.MaxEdges)
	edges, boundaries, err := store.hyperedgesByTx(ctx, snapshot, candidates)
	if err != nil {
		return repository.ExpansionResult{}, nil, err
	}
	for _, edge := range edges {
		if err := validateHyperedgeResources(edge, budget.MaxNodes); err != nil {
			return repository.ExpansionResult{}, nil, err
		}
	}
	return repository.ExpansionResult{
		Hyperedges: edges,
		Truncated:  truncated,
	}, boundaries, nil
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
	predicate, values := hashPredicate("t.tx_hash", hashes)
	headerSQL := targetedFactSQL(`
        SELECT *
        FROM transactions AS t
        WHERE `+predicate+`
          AND t.publication_id <= publication_watermark
`, `
SELECT
    t.tx_hash,
    t.effective_fee_lovelace,
    t.mint_is_applied,
    t.mint_policy_ids,
    t.mint_asset_names,
    t.mint_quantities
FROM fact_candidates AS t
INNER JOIN active_candidate_publications AS ap
    ON t.publication_id = ap.publication_id
ORDER BY t.tx_hash`)
	queryCtx, finish := store.instrumentPhase(
		ctx,
		"trace_transactions",
		traceHydrationPhaseLimits(),
	)
	rows, err := store.conn.Query(queryCtx, headerSQL, activeArguments(snapshot, values...)...)
	if err != nil {
		finish()
		return nil, nil, mapQueryError("trace_transactions", err)
	}
	edges := make(map[string]*model.FlowHyperedge, len(hashes))
	for rows.Next() {
		var txHash []byte
		var fee *uint64
		var mintApplied bool
		var policies, names []string
		var quantities []int64
		if err := rows.Scan(&txHash, &fee, &mintApplied, &policies, &names, &quantities); err != nil {
			rows.Close()
			finish()
			return nil, nil, mapQueryError("trace_transactions", err)
		}
		tx, err := model.Hash32FromBytes(txHash)
		if err != nil {
			rows.Close()
			finish()
			return nil, nil, err
		}
		if _, exists := edges[tx.String()]; exists {
			rows.Close()
			finish()
			return nil, nil, ErrConflictingRow
		}
		mint, err := decodeSignedAssets(policies, names, quantities)
		if err != nil {
			rows.Close()
			finish()
			return nil, nil, err
		}
		edge := &model.FlowHyperedge{
			Transaction:         tx,
			Inputs:              make([]model.Spend, 0),
			ConsumedInputs:      make([]model.UTxORef, 0),
			ConsumedInputValues: make([]model.Output, 0),
			ProducedOutputs:     make([]model.Output, 0),
			AppliedWithdrawals:  make([]model.Withdrawal, 0),
			MintDeltas:          make([]model.MintDelta, 0, len(mint)),
		}
		if fee != nil && *fee > 0 {
			edge.FeeSink = &model.FeeSink{TxHash: tx, Lovelace: *fee}
		}
		if mintApplied {
			for _, quantity := range mint {
				edge.MintDeltas = append(edge.MintDeltas, model.MintDelta{
					TxHash:   tx,
					Asset:    quantity,
					IsSource: quantity.Quantity > 0,
					IsSink:   quantity.Quantity < 0,
				})
			}
		}
		edges[tx.String()] = edge
	}
	err = rows.Err()
	rows.Close()
	finish()
	if err != nil {
		return nil, nil, mapQueryError("trace_transactions", err)
	}
	if len(edges) != len(hashes) {
		return nil, nil, ErrConflictingRow
	}

	hashPredicateSQL, hashValues := hashPredicate("i.tx_hash", hashes)
	inputSQL := targetedFactSQL(`
        SELECT *
        FROM inputs AS i
        WHERE `+hashPredicateSQL+`
          AND i.publication_id <= publication_watermark
`, `
SELECT
    i.source_tx_hash,
    i.source_output_index,
    i.tx_hash,
    b.block_hash,
    i.block_number,
    i.role,
    i.body_ordinal,
    i.is_consumed,
    i.source_is_resolved
FROM fact_candidates AS i
INNER JOIN active_candidate_publications AS ap
    ON i.publication_id = ap.publication_id
INNER JOIN candidate_blocks AS b ON i.publication_id = b.publication_id
ORDER BY
    i.tx_hash,
    multiIf(i.role = 'regular', 0, i.role = 'collateral', 1, i.role = 'reference', 2, 3),
    i.body_ordinal,
    i.source_tx_hash,
    i.source_output_index,
    i.publication_id`)
	queryCtx, finish = store.instrumentPhase(
		ctx,
		"trace_consumed_inputs",
		traceHydrationPhaseLimits(),
	)
	rows, err = store.conn.Query(queryCtx, inputSQL, activeArguments(snapshot, hashValues...)...)
	if err != nil {
		finish()
		return nil, nil, mapQueryError("trace_consumed_inputs", err)
	}
	boundaries := make([]model.PartialHistoryBoundary, 0)
	resolvedRefs := make([]model.UTxORef, 0)
	type inputLocation struct {
		edge  *model.FlowHyperedge
		index int
	}
	locations := make(map[string][]inputLocation)
	scannedInputs := make([]model.Spend, 0)
	for rows.Next() {
		input, err := scanSpend(rows)
		if err != nil {
			rows.Close()
			finish()
			return nil, nil, mapQueryError("trace_consumed_inputs", err)
		}
		edge := edges[input.ConsumingTx.String()]
		if edge == nil {
			rows.Close()
			finish()
			return nil, nil, errors.New("input has no active transaction")
		}
		scannedInputs = append(scannedInputs, input)
		edge.Inputs = append(edge.Inputs, input)
		location := inputLocation{edge: edge, index: len(edge.Inputs) - 1}
		locations[input.Source.String()] = append(locations[input.Source.String()], location)
		if input.IsConsumed {
			edge.ConsumedInputs = append(edge.ConsumedInputs, input.Source)
		}
		resolvedRefs = append(resolvedRefs, input.Source)
	}
	err = rows.Err()
	rows.Close()
	finish()
	if err != nil {
		return nil, nil, mapQueryError("trace_consumed_inputs", err)
	}
	if err := validateCompleteSpendRows(scannedInputs); err != nil {
		return nil, nil, err
	}

	resolvedRefs = uniqueRefs(resolvedRefs)
	inputValues, valueBoundaries, err := store.outputsByRefs(ctx, snapshot, resolvedRefs)
	if err != nil {
		return nil, nil, err
	}
	boundaries = append(boundaries, valueBoundaries...)
	for _, output := range inputValues {
		inputLocations := locations[output.Ref.String()]
		if len(inputLocations) == 0 {
			return nil, nil, errors.New("resolved source output has no input")
		}
		for _, location := range inputLocations {
			source := output
			location.edge.Inputs[location.index].SourceResolved = true
			location.edge.Inputs[location.index].SourceOutput = &source
			if location.edge.Inputs[location.index].IsConsumed {
				location.edge.ConsumedInputValues = append(
					location.edge.ConsumedInputValues,
					output,
				)
			}
		}
	}

	outputPredicateSQL, outputValues := hashPredicate("o.tx_hash", hashes)
	outputSQL := targetedFactSQL(`
        SELECT *
        FROM outputs AS o
        WHERE `+outputPredicateSQL+`
          AND o.publication_id <= publication_watermark
`, `
SELECT`+outputColumns+`
FROM fact_candidates AS o
INNER JOIN active_candidate_publications AS ap
    ON o.publication_id = ap.publication_id
INNER JOIN candidate_blocks AS b ON o.publication_id = b.publication_id
ORDER BY o.tx_hash, o.body_ordinal, o.output_index, o.publication_id`)
	queryCtx, finish = store.instrumentPhase(
		ctx,
		"trace_produced_outputs",
		traceHydrationPhaseLimits(),
	)
	rows, err = store.conn.Query(queryCtx, outputSQL, activeArguments(snapshot, outputValues...)...)
	if err != nil {
		finish()
		return nil, nil, mapQueryError("trace_produced_outputs", err)
	}
	scannedProduced := make([]model.Output, 0)
	for rows.Next() {
		output, err := scanOutput(rows)
		if err != nil {
			rows.Close()
			finish()
			return nil, nil, mapQueryError("trace_produced_outputs", err)
		}
		edge := edges[output.ProducingTx.String()]
		if edge == nil {
			rows.Close()
			finish()
			return nil, nil, errors.New("produced output has no active transaction")
		}
		scannedProduced = append(scannedProduced, output)
		edge.ProducedOutputs = append(edge.ProducedOutputs, output)
	}
	err = rows.Err()
	rows.Close()
	finish()
	if err != nil {
		return nil, nil, mapQueryError("trace_produced_outputs", err)
	}
	if err := validateCompleteOutputRows(scannedProduced); err != nil {
		return nil, nil, err
	}
	produced := make([]model.Output, 0)
	for _, edge := range edges {
		produced = append(produced, edge.ProducedOutputs...)
	}
	if err := store.hydrateInlineDatums(ctx, produced); err != nil {
		return nil, nil, err
	}
	for _, edge := range edges {
		edge.ProducedOutputs = edge.ProducedOutputs[:0]
	}
	for _, output := range produced {
		edge := edges[output.ProducingTx.String()]
		edge.ProducedOutputs = append(edge.ProducedOutputs, output)
	}

	withdrawalPredicateSQL, withdrawalValues := hashPredicate("w.tx_hash", hashes)
	withdrawalSQL := targetedFactSQL(`
        SELECT *
        FROM withdrawals AS w
        WHERE w.is_applied
          AND `+withdrawalPredicateSQL+`
          AND w.publication_id <= publication_watermark
`, `
SELECT
    w.tx_hash,
    w.reward_account,
    w.lovelace,
    w.body_ordinal,
    w.credential_kind,
    w.credential_hash
FROM fact_candidates AS w
INNER JOIN active_candidate_publications AS ap
    ON w.publication_id = ap.publication_id
ORDER BY w.tx_hash, w.body_ordinal`)
	queryCtx, finish = store.instrumentPhase(
		ctx,
		"trace_applied_withdrawals",
		traceHydrationPhaseLimits(),
	)
	rows, err = store.conn.Query(
		queryCtx,
		withdrawalSQL,
		activeArguments(snapshot, withdrawalValues...)...,
	)
	if err != nil {
		finish()
		return nil, nil, mapQueryError("trace_applied_withdrawals", err)
	}
	for rows.Next() {
		var txHash, reward, credential []byte
		var credentialKind string
		var amount uint64
		var ordinal uint32
		if err := rows.Scan(
			&txHash,
			&reward,
			&amount,
			&ordinal,
			&credentialKind,
			&credential,
		); err != nil {
			rows.Close()
			finish()
			return nil, nil, mapQueryError("trace_applied_withdrawals", err)
		}
		tx, err := model.Hash32FromBytes(txHash)
		if err != nil {
			rows.Close()
			finish()
			return nil, nil, err
		}
		if err := validateRewardAccount(reward, credentialKind, credential); err != nil {
			rows.Close()
			finish()
			return nil, nil, err
		}
		edge := edges[tx.String()]
		if edge == nil {
			rows.Close()
			finish()
			return nil, nil, errors.New("withdrawal has no active transaction")
		}
		edge.AppliedWithdrawals = append(edge.AppliedWithdrawals, model.Withdrawal{
			TxHash:         tx,
			RewardAccount:  model.Bytes(bytes.Clone(reward)),
			Lovelace:       amount,
			BodyOrdinal:    ordinal,
			Applied:        true,
			CredentialKind: credentialKind,
			CredentialHash: model.Bytes(bytes.Clone(credential)),
		})
	}
	err = rows.Err()
	rows.Close()
	finish()
	if err != nil {
		return nil, nil, mapQueryError("trace_applied_withdrawals", err)
	}

	result := make([]model.FlowHyperedge, 0, len(edges))
	keys := make([]string, 0, len(edges))
	for key := range edges {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if err := validateHyperedgeResources(*edges[key], limits.HardMaxTraceNodes); err != nil {
			return nil, nil, err
		}
		result = append(result, *edges[key])
	}
	return result, boundaries, nil
}

func (store *Store) outputsByRefs(
	ctx context.Context,
	snapshot model.Snapshot,
	refs []model.UTxORef,
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
SELECT`+outputColumns+`
FROM fact_candidates AS o
INNER JOIN active_candidate_publications AS ap
    ON o.publication_id = ap.publication_id
INNER JOIN candidate_blocks AS b ON o.publication_id = b.publication_id
ORDER BY o.tx_hash, o.body_ordinal, o.output_index, o.publication_id`)
	queryCtx, finish := store.instrumentPhase(
		ctx,
		"trace_output_values",
		traceHydrationPhaseLimits(),
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
		output, err := scanOutput(rows)
		if err != nil {
			return nil, nil, mapQueryError("trace_output_values", err)
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
	if err := store.hydrateInlineDatums(ctx, outputs); err != nil {
		return nil, nil, err
	}
	boundaries := make([]model.PartialHistoryBoundary, 0)
	for _, ref := range refs {
		if _, exists := found[ref.String()]; exists {
			continue
		}
		if snapshot.CompleteHistory {
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
	result, err := scanHashes(rows)
	if err != nil {
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
