package clickhouse

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/clicksync-project/clickout/internal/cursor"
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
	if query.Limit == 0 {
		return model.AddressPage{}, nil, errors.New("address limit cannot be zero")
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
		Address: model.Bytes(query.Address),
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
	currentFilter := ""
	if query.State == "current" {
		currentFilter = `
  AND (o.tx_hash, o.output_index) NOT IN
      (
          SELECT i.source_tx_hash, i.source_output_index
          FROM inputs AS i
          INNER JOIN active_publications AS spent_ap
              ON i.publication_id = spent_ap.publication_id
          WHERE i.is_consumed
      )`
	}
	cursorFilter := ""
	arguments := activeArguments(snapshot, string(query.Address))
	if hasCursor {
		hash, err := model.ParseHash32(key.TxHash)
		if err != nil {
			return "", nil, cursor.ErrInvalid
		}
		cursorFilter = `
  AND (o.block_number, o.tx_hash, o.output_index, o.publication_id) > (?, ?, ?, ?)`
		arguments = append(
			arguments,
			key.BlockNumber,
			hashArgument(hash),
			key.OutputIndex,
			key.PublicationID,
		)
	}
	arguments = append(arguments, uint64(query.Limit)+1)
	sql := activePublicationsCTE + `
SELECT` + outputColumns + `,
    o.publication_id
FROM outputs AS o
INNER JOIN active_publications AS ap ON o.publication_id = ap.publication_id
INNER JOIN blocks AS b ON o.publication_id = b.publication_id
WHERE o.address = ?` + currentFilter + cursorFilter + `
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
	sql := activePublicationsCTE + `
SELECT DISTINCT i.source_tx_hash, i.source_output_index, i.tx_hash
FROM inputs AS i
INNER JOIN active_publications AS ap ON i.publication_id = ap.publication_id
WHERE i.is_consumed
  AND ` + predicate
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
