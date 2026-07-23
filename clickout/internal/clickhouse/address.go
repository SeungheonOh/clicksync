package clickhouse

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/clicksync-project/clickout/internal/cursor"
	"github.com/clicksync-project/clickout/internal/limits"
	"github.com/clicksync-project/clickout/internal/model"
	"github.com/clicksync-project/clickout/internal/repository"
)

type addressKey struct {
	BlockNumber   uint64 `json:"block_number"`
	TxHash        string `json:"tx_hash"`
	OutputIndex   uint32 `json:"output_index"`
	PublicationID uint64 `json:"publication_id"`
}

func addressScope(address []byte, state string) string {
	sum := sha256.Sum256(address)
	return "address/" + state + "/" + hex.EncodeToString(sum[:])
}

func AddressScope(address []byte, state string) string {
	return addressScope(address, state)
}

func encodeAddressKey(key addressKey) (string, error) {
	data, err := json.Marshal(key)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(data), nil
}

func decodeAddressKey(value string) (addressKey, error) {
	data, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return addressKey{}, cursor.ErrInvalid
	}
	var key addressKey
	if err := json.Unmarshal(data, &key); err != nil || key.TxHash == "" {
		return addressKey{}, cursor.ErrInvalid
	}
	if _, err := model.ParseHash32(key.TxHash); err != nil {
		return addressKey{}, cursor.ErrInvalid
	}
	return key, nil
}

func (store *Store) Address(
	ctx context.Context,
	snapshot model.Snapshot,
	query repository.AddressQuery,
) (model.AddressPage, []model.PartialHistoryBoundary, error) {
	if query.State != "current" && query.State != "history" {
		return model.AddressPage{}, nil, errors.New("address state must be current or history")
	}
	if err := limits.ValidatePage(query.Limit); err != nil {
		return model.AddressPage{}, nil, err
	}
	var (
		key       addressKey
		hasCursor bool
	)
	if query.LastKey != "" {
		var err error
		key, err = decodeAddressKey(query.LastKey)
		if err != nil {
			return model.AddressPage{}, nil, err
		}
		hasCursor = true
	}
	sql, arguments, err := addressSQL(snapshot, query, key, hasCursor)
	if err != nil {
		return model.AddressPage{}, nil, err
	}
	queryCtx, finish := store.instrument(ctx, "address_"+query.State)
	defer finish()
	rows, err := store.conn.Query(queryCtx, sql, arguments...)
	if err != nil {
		return model.AddressPage{}, nil, err
	}
	defer rows.Close()
	type item struct {
		state model.OutputState
		key   addressKey
	}
	items := make([]item, 0, query.Limit+1)
	for rows.Next() {
		output, publicationID, err := scanAddressOutput(rows)
		if err != nil {
			return model.AddressPage{}, nil, err
		}
		items = append(items, item{
			state: model.OutputState{
				Output:    output,
				IsCurrent: query.State == "current",
			},
			key: addressKey{
				BlockNumber:   output.BlockHeight,
				TxHash:        output.Ref.TxHash.String(),
				OutputIndex:   output.Ref.Index,
				PublicationID: publicationID,
			},
		})
	}
	if err := rows.Err(); err != nil {
		return model.AddressPage{}, nil, err
	}
	outputs := make([]model.Output, len(items))
	for index := range items {
		outputs[index] = items[index].state.Output
	}
	if err := store.hydrateInlineDatums(ctx, outputs); err != nil {
		return model.AddressPage{}, nil, err
	}
	for index := range items {
		items[index].state.Output = outputs[index]
	}

	truncated := len(items) > int(query.Limit)
	if truncated {
		items = items[:query.Limit]
	}
	if query.State == "history" {
		refs := make([]model.UTxORef, len(items))
		for index := range items {
			refs[index] = items[index].state.Output.Ref
		}
		spenders, err := store.spendersByRefs(ctx, snapshot, refs)
		if err != nil {
			return model.AddressPage{}, nil, err
		}
		for index := range items {
			consumers := spenders[items[index].state.Output.Ref.String()]
			if len(consumers) > 1 {
				return model.AddressPage{}, nil, ErrConflictingRow
			}
			items[index].state.IsCurrent = len(consumers) == 0
			if len(consumers) == 1 {
				items[index].state.SpentBy = &consumers[0]
			}
		}
	}
	page := model.AddressPage{
		Address: model.Bytes(bytes.Clone(query.Address)),
		State:   query.State,
		Items:   make([]model.OutputState, len(items)),
	}
	for index := range items {
		page.Items[index] = items[index].state
	}
	if truncated && len(items) > 0 {
		lastKey, err := encodeAddressKey(items[len(items)-1].key)
		if err != nil {
			return model.AddressPage{}, nil, err
		}
		page.Cursor, err = cursor.Encode(cursor.Value{
			Scope:         addressScope(query.Address, query.State),
			SnapshotEvent: snapshot.Event,
			LastKey:       lastKey,
		})
		if err != nil {
			return model.AddressPage{}, nil, err
		}
	}
	return page, nil, nil
}

func addressSQL(
	snapshot model.Snapshot,
	query repository.AddressQuery,
	key addressKey,
	hasCursor bool,
) (string, []any, error) {
	cursorFilter := ""
	candidateArguments := []any{string(query.Address), string(query.Address)}
	if hasCursor {
		hash, err := model.ParseHash32(key.TxHash)
		if err != nil {
			return "", nil, cursor.ErrInvalid
		}
		cursorFilter = `
  AND (o.block_number, o.tx_hash, o.output_index, o.publication_id) > (?, ?, ?, ?)`
		candidateArguments = append(
			candidateArguments,
			key.BlockNumber,
			hashArgument(hash),
			key.OutputIndex,
			key.PublicationID,
		)
	}
	arguments := activeArguments(snapshot, candidateArguments...)
	arguments = append(arguments, uint64(query.Limit)+1)
	candidate := `
        SELECT *
        FROM outputs
        WHERE address_hash = sipHash64(?)
          AND address = ?
          AND publication_id <= publication_watermark` + cursorFilter
	if query.State == "history" {
		sql := targetedFactSQL(candidate, `
SELECT`+outputColumns+`,
    o.publication_id
FROM fact_candidates AS o
INNER JOIN active_candidate_publications AS ap
    ON o.publication_id = ap.publication_id
INNER JOIN blocks AS b ON o.publication_id = b.publication_id
ORDER BY o.block_number, o.tx_hash, o.output_index, o.publication_id
LIMIT ?`)
		return sql, arguments, nil
	}
	// Current-address lookup still starts with the address projection. Only
	// inputs whose source ref occurs in that candidate set are considered, and
	// membership is evaluated for the union of those two bounded fact sets.
	sql := `
WITH
    toUInt64(?) AS snapshot_event,
    toUInt64(?) AS publication_watermark,
    output_candidates AS
    (
` + candidate + `
    ),
    input_candidates AS
    (
        SELECT i.*
        FROM inputs AS i
        INNER JOIN
        (
            SELECT DISTINCT tx_hash, output_index
            FROM output_candidates
        ) AS refs
            ON i.source_tx_hash = refs.tx_hash
           AND i.source_output_index = refs.output_index
        WHERE i.is_consumed
          AND i.publication_id <= publication_watermark
    ),
    candidate_publications AS
    (
        SELECT publication_id FROM output_candidates
        UNION DISTINCT
        SELECT publication_id FROM input_candidates
    ),
    candidate_blocks AS
    (
        SELECT publication_id, block_hash
        FROM blocks
        WHERE publication_id IN
            (SELECT publication_id FROM candidate_publications)
          AND publication_id <= publication_watermark
    ),
    candidate_invalidations AS
    (
        SELECT
            ce.publication_id,
            ce.event_seq,
            ce.active,
            assumeNotNull(ce.rollback_id) AS rollback_id
        FROM chain_events AS ce
        WHERE ce.publication_id IN
            (SELECT publication_id FROM candidate_publications)
          AND ce.event_seq <= snapshot_event
          AND ce.event_kind = 'invalidation'
    ),
    candidate_rollback_headers AS
    (
        SELECT rb.rollback_id, rb.event_seq
        FROM rollbacks AS rb
        WHERE (rb.rollback_id, rb.event_seq) IN
        (
            SELECT rollback_id, event_seq
            FROM candidate_invalidations
        )
    ),
    committed_candidate_membership AS
    (
        SELECT ce.publication_id, ce.event_seq, ce.active
        FROM chain_events AS ce
        WHERE ce.publication_id IN
            (SELECT publication_id FROM candidate_publications)
          AND ce.event_seq <= snapshot_event
          AND ce.event_kind = 'adoption'

        UNION ALL

        SELECT ci.publication_id, ci.event_seq, ci.active
        FROM candidate_rollback_headers AS rb
        INNER JOIN candidate_invalidations AS ci
            ON rb.rollback_id = ci.rollback_id
           AND rb.event_seq = ci.event_seq
    ),
    active_candidate_publications AS
    (
        SELECT publication_id
        FROM committed_candidate_membership
        GROUP BY publication_id
        HAVING argMax(active, event_seq) = 1
    ),
    active_outputs AS
    (
        SELECT o.*
        FROM output_candidates AS o
        INNER JOIN active_candidate_publications AS ap
            ON o.publication_id = ap.publication_id
    ),
    active_spends AS
    (
        SELECT i.*
        FROM input_candidates AS i
        INNER JOIN active_candidate_publications AS ap
            ON i.publication_id = ap.publication_id
    )
SELECT` + outputColumns + `,
    o.publication_id
FROM active_outputs AS o
LEFT ANTI JOIN active_spends AS i
    ON i.source_tx_hash = o.tx_hash
   AND i.source_output_index = o.output_index
INNER JOIN candidate_blocks AS b ON o.publication_id = b.publication_id
ORDER BY o.block_number, o.tx_hash, o.output_index, o.publication_id
LIMIT ?`
	return sql, arguments, nil
}

func (store *Store) spendersByRefs(
	ctx context.Context,
	snapshot model.Snapshot,
	refs []model.UTxORef,
) (map[string][]model.Hash32, error) {
	result := make(map[string][]model.Hash32, len(refs))
	if len(refs) == 0 {
		return result, nil
	}
	predicate, values := tuplePredicate("i.source_tx_hash", "i.source_output_index", refs)
	sql := targetedFactSQL(`
        SELECT *
        FROM inputs AS i
        WHERE i.is_consumed
          AND `+predicate+`
          AND i.publication_id <= publication_watermark
`, `
SELECT DISTINCT i.source_tx_hash, i.source_output_index, i.tx_hash
FROM fact_candidates AS i
INNER JOIN active_candidate_publications AS ap
    ON i.publication_id = ap.publication_id`)
	queryCtx, finish := store.instrument(ctx, "address_spends")
	defer finish()
	rows, err := store.conn.Query(queryCtx, sql, activeArguments(snapshot, values...)...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var sourceHash, txHash []byte
		var sourceIndex uint32
		if err := rows.Scan(&sourceHash, &sourceIndex, &txHash); err != nil {
			return nil, err
		}
		source, err := model.Hash32FromBytes(sourceHash)
		if err != nil {
			return nil, err
		}
		consumer, err := model.Hash32FromBytes(txHash)
		if err != nil {
			return nil, err
		}
		ref := model.UTxORef{TxHash: source, Index: sourceIndex}
		result[ref.String()] = append(result[ref.String()], consumer)
	}
	return result, rows.Err()
}

func tuplePredicate(hashColumn, indexColumn string, refs []model.UTxORef) (string, []any) {
	predicate := "("
	values := make([]any, 0, len(refs)*2)
	for index, ref := range refs {
		if index > 0 {
			predicate += " OR "
		}
		predicate += fmt.Sprintf("(%s = ? AND %s = ?)", hashColumn, indexColumn)
		values = append(values, hashArgument(ref.TxHash), ref.Index)
	}
	return predicate + ")", values
}
