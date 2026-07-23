package clickhouse

import (
	"bytes"
	"context"
	"errors"
	"sort"
	"strings"

	"github.com/clicksync-project/clickout/internal/model"
	"github.com/clicksync-project/clickout/internal/repository"
)

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
		page, boundaries, err := store.Address(ctx, snapshot, repository.AddressQuery{
			Address: query.Seed.Address,
			State:   "history",
			Limit:   limit,
		})
		if err != nil {
			return repository.TraceSeedResult{}, nil, err
		}
		seeds := make([]model.UTxORef, 0, len(page.Items))
		for _, item := range page.Items {
			if outputHasAsset(item.Output, query.Asset) {
				seeds = append(seeds, item.Output.Ref)
			}
		}
		return repository.TraceSeedResult{
			UTxOs:     seeds,
			Truncated: page.Cursor != "",
		}, boundaries, nil
	}
	return repository.TraceSeedResult{}, nil, errors.New("missing trace seed")
}

func (store *Store) ExpandForward(
	ctx context.Context,
	snapshot model.Snapshot,
	sources []model.UTxORef,
	asset model.AssetSelector,
) ([]model.FlowHyperedge, []model.PartialHistoryBoundary, error) {
	if len(sources) == 0 {
		return []model.FlowHyperedge{}, nil, nil
	}
	predicate, values := tuplePredicate("i.source_tx_hash", "i.source_output_index", sources)
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
ORDER BY i.tx_hash`)
	queryCtx, finish := store.instrument(ctx, "trace_forward_spends")
	rows, err := store.conn.Query(queryCtx, sql, activeArguments(snapshot, values...)...)
	if err != nil {
		finish()
		return nil, nil, err
	}
	hashes, err := scanHashes(rows)
	rows.Close()
	finish()
	if err != nil {
		return nil, nil, err
	}
	return store.hyperedgesByTx(ctx, snapshot, hashes)
}

func (store *Store) ExpandReverse(
	ctx context.Context,
	snapshot model.Snapshot,
	targets []model.UTxORef,
	asset model.AssetSelector,
) ([]model.FlowHyperedge, []model.PartialHistoryBoundary, error) {
	if len(targets) == 0 {
		return []model.FlowHyperedge{}, nil, nil
	}
	values, boundaries, err := store.outputsByRefs(ctx, snapshot, targets)
	if err != nil {
		return nil, nil, err
	}
	hashes := make([]model.Hash32, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, output := range values {
		key := output.ProducingTx.String()
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		hashes = append(hashes, output.ProducingTx)
	}
	edges, edgeBoundaries, err := store.hyperedgesByTx(ctx, snapshot, hashes)
	boundaries = append(boundaries, edgeBoundaries...)
	return edges, boundaries, err
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
	queryCtx, finish := store.instrument(ctx, "trace_transactions")
	rows, err := store.conn.Query(queryCtx, headerSQL, activeArguments(snapshot, values...)...)
	if err != nil {
		finish()
		return nil, nil, err
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
			return nil, nil, err
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
		return nil, nil, err
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
ORDER BY i.tx_hash, i.body_ordinal`)
	queryCtx, finish = store.instrument(ctx, "trace_consumed_inputs")
	rows, err = store.conn.Query(queryCtx, inputSQL, activeArguments(snapshot, hashValues...)...)
	if err != nil {
		finish()
		return nil, nil, err
	}
	boundaries := make([]model.PartialHistoryBoundary, 0)
	resolvedRefs := make([]model.UTxORef, 0)
	type inputLocation struct {
		edge  *model.FlowHyperedge
		index int
	}
	locations := make(map[string][]inputLocation)
	for rows.Next() {
		input, err := scanSpend(rows)
		if err != nil {
			rows.Close()
			finish()
			return nil, nil, err
		}
		edge := edges[input.ConsumingTx.String()]
		if edge == nil {
			rows.Close()
			finish()
			return nil, nil, errors.New("input has no active transaction")
		}
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
ORDER BY o.tx_hash, o.body_ordinal`)
	queryCtx, finish = store.instrument(ctx, "trace_produced_outputs")
	rows, err = store.conn.Query(queryCtx, outputSQL, activeArguments(snapshot, outputValues...)...)
	if err != nil {
		finish()
		return nil, nil, err
	}
	for rows.Next() {
		output, err := scanOutput(rows)
		if err != nil {
			rows.Close()
			finish()
			return nil, nil, err
		}
		edge := edges[output.ProducingTx.String()]
		if edge == nil {
			rows.Close()
			finish()
			return nil, nil, errors.New("produced output has no active transaction")
		}
		edge.ProducedOutputs = append(edge.ProducedOutputs, output)
	}
	err = rows.Err()
	rows.Close()
	finish()
	if err != nil {
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
	queryCtx, finish = store.instrument(ctx, "trace_applied_withdrawals")
	rows, err = store.conn.Query(
		queryCtx,
		withdrawalSQL,
		activeArguments(snapshot, withdrawalValues...)...,
	)
	if err != nil {
		finish()
		return nil, nil, err
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
			return nil, nil, err
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
		return nil, nil, err
	}

	result := make([]model.FlowHyperedge, 0, len(edges))
	keys := make([]string, 0, len(edges))
	for key := range edges {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
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
ORDER BY o.tx_hash, o.output_index`)
	queryCtx, finish := store.instrument(ctx, "trace_output_values")
	defer finish()
	rows, err := store.conn.Query(queryCtx, sql, activeArguments(snapshot, values...)...)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	outputs := make([]model.Output, 0, len(refs))
	found := make(map[string]struct{}, len(refs))
	for rows.Next() {
		output, err := scanOutput(rows)
		if err != nil {
			return nil, nil, err
		}
		if _, duplicate := found[output.Ref.String()]; duplicate {
			return nil, nil, ErrConflictingRow
		}
		found[output.Ref.String()] = struct{}{}
		outputs = append(outputs, output)
	}
	if err := rows.Err(); err != nil {
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
