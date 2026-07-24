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

// addressKey is the complete physical ordering key used by the address
// candidate scan. AddressHash alone is not an identity: the raw address is
// deliberately part of both the database predicate and the resume key.
type addressKey struct {
	AddressHash   uint64      `json:"address_hash"`
	Address       model.Bytes `json:"address"`
	BlockNumber   uint64      `json:"block_number"`
	TxHash        string      `json:"tx_hash"`
	OutputIndex   uint32      `json:"output_index"`
	PublicationID uint64      `json:"publication_id"`
}

type addressCandidate struct {
	key addressKey
	ref model.UTxORef
}

func addressScope(address []byte, state string) string {
	sum := sha256.Sum256(address)
	return "address/" + state + "/" + hex.EncodeToString(sum[:])
}

func AddressScope(address []byte, state string) string {
	return addressScope(address, state)
}

func TraceAddressScope(
	address []byte,
	direction repository.TraceDirection,
	asset model.AssetSelector,
) string {
	sum := sha256.Sum256(address)
	return "trace/" + string(direction) + "/address/" +
		hex.EncodeToString(sum[:]) + "/" + asset.String()
}

func encodeAddressKey(key addressKey) (string, error) {
	data, err := json.Marshal(key)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(data), nil
}

func decodeAddressKey(value string, address []byte) (addressKey, error) {
	data, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return addressKey{}, cursor.ErrInvalid
	}
	var key addressKey
	if err := json.Unmarshal(data, &key); err != nil ||
		key.TxHash == "" ||
		!bytes.Equal(key.Address, address) {
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
		key, err = decodeAddressKey(query.LastKey, query.Address)
		if err != nil {
			return model.AddressPage{}, nil, err
		}
		expected, err := store.addressHash(ctx, query.Address)
		if err != nil {
			return model.AddressPage{}, nil, err
		}
		if key.AddressHash != expected {
			return model.AddressPage{}, nil, cursor.ErrInvalid
		}
		hasCursor = true
	}

	candidates, hasMore, err := store.addressCandidates(
		ctx,
		snapshot,
		query.Address,
		query.Limit,
		key,
		hasCursor,
	)
	if err != nil {
		return model.AddressPage{}, nil, err
	}
	active, err := store.activeAddressPublications(ctx, snapshot, candidates)
	if err != nil {
		return model.AddressPage{}, nil, err
	}
	activeCandidates := make([]addressCandidate, 0, len(candidates))
	for _, candidate := range candidates {
		if _, ok := active[candidate.key.PublicationID]; ok {
			activeCandidates = append(activeCandidates, candidate)
		}
	}
	spenders, err := store.spendersByCandidates(ctx, snapshot, activeCandidates)
	if err != nil {
		return model.AddressPage{}, nil, err
	}

	selected := make([]addressCandidate, 0, len(activeCandidates))
	for _, candidate := range activeCandidates {
		consumers := spenders[candidate.ref.String()]
		if len(consumers) > 1 {
			return model.AddressPage{}, nil, ErrConflictingRow
		}
		if query.State == "current" && len(consumers) != 0 {
			continue
		}
		selected = append(selected, candidate)
	}
	outputs, err := store.hydrateAddressCandidates(ctx, snapshot, selected)
	if err != nil {
		return model.AddressPage{}, nil, err
	}
	items := make([]model.OutputState, 0, len(selected))
	for _, candidate := range selected {
		output, ok := outputs[addressCandidateIdentity(candidate)]
		if !ok {
			return model.AddressPage{}, nil, ErrConflictingRow
		}
		consumers := spenders[candidate.ref.String()]
		state := model.OutputState{
			Output:    output,
			IsCurrent: len(consumers) == 0,
		}
		if len(consumers) == 1 {
			consumer := consumers[0]
			state.SpentBy = &consumer
		}
		items = append(items, state)
	}

	hydrated := make([]model.Output, len(items))
	for index := range items {
		hydrated[index] = items[index].Output
	}
	if err := store.hydrateInlineDatums(ctx, hydrated); err != nil {
		return model.AddressPage{}, nil, err
	}
	for index := range items {
		items[index].Output = hydrated[index]
	}

	page := model.AddressPage{
		Address: model.Bytes(bytes.Clone(query.Address)),
		State:   query.State,
		Items:   items,
	}
	// The sentinel is not processed. The resume key is the last physical
	// candidate that was processed, even when every processed candidate was
	// inactive/spent and the returned page is empty.
	if hasMore && len(candidates) > 0 {
		lastKey, err := encodeAddressKey(candidates[len(candidates)-1].key)
		if err != nil {
			return model.AddressPage{}, nil, err
		}
		page.Cursor, err = cursor.Encode(cursor.Value{
			Scope:    addressScope(query.Address, query.State),
			Snapshot: snapshot,
			LastKey:  lastKey,
		})
		if err != nil {
			return model.AddressPage{}, nil, err
		}
	}
	return page, nil, nil
}

const addressHashSQL = `SELECT sipHash64(?)`

func (store *Store) addressHash(ctx context.Context, address []byte) (uint64, error) {
	queryCtx, finish := store.instrumentPhase(
		ctx,
		"address_cursor_hash",
		resultPhaseLimits(1),
	)
	defer finish()
	var value uint64
	if err := store.conn.QueryRow(queryCtx, addressHashSQL, string(address)).Scan(&value); err != nil {
		return 0, mapQueryError("address_cursor_hash", err)
	}
	return value, nil
}

func (store *Store) addressCandidates(
	ctx context.Context,
	snapshot model.Snapshot,
	address []byte,
	window uint32,
	key addressKey,
	hasCursor bool,
) ([]addressCandidate, bool, error) {
	sql, arguments, err := addressCandidateSQL(snapshot, address, window, key, hasCursor)
	if err != nil {
		return nil, false, err
	}
	queryCtx, finish := store.instrumentPhase(
		ctx,
		"address_candidates",
		candidatePhaseLimits(uint64(window)+1),
	)
	defer finish()
	rows, err := store.conn.Query(queryCtx, sql, arguments...)
	if err != nil {
		return nil, false, mapQueryError("address_candidates", err)
	}
	defer rows.Close()
	result := make([]addressCandidate, 0, window+1)
	for rows.Next() {
		var (
			addressHash   uint64
			rawAddress    []byte
			blockNumber   uint64
			rawHash       []byte
			outputIndex   uint32
			publicationID uint64
		)
		if err := rows.Scan(
			&addressHash,
			&rawAddress,
			&blockNumber,
			&rawHash,
			&outputIndex,
			&publicationID,
		); err != nil {
			return nil, false, err
		}
		if !bytes.Equal(rawAddress, address) {
			return nil, false, ErrConflictingRow
		}
		hash, err := model.Hash32FromBytes(rawHash)
		if err != nil {
			return nil, false, err
		}
		candidate := addressCandidate{
			key: addressKey{
				AddressHash:   addressHash,
				Address:       model.Bytes(bytes.Clone(rawAddress)),
				BlockNumber:   blockNumber,
				TxHash:        hash.String(),
				OutputIndex:   outputIndex,
				PublicationID: publicationID,
			},
			ref: model.UTxORef{TxHash: hash, Index: outputIndex},
		}
		if len(result) > 0 && compareAddressKeys(result[len(result)-1].key, candidate.key) >= 0 {
			return nil, false, ErrConflictingRow
		}
		if hasCursor && compareAddressKeys(key, candidate.key) >= 0 {
			return nil, false, ErrConflictingRow
		}
		result = append(result, candidate)
	}
	if err := rows.Err(); err != nil {
		return nil, false, mapQueryError("address_candidates", err)
	}
	hasMore := len(result) > int(window)
	if hasMore {
		result = result[:window]
	}
	return result, hasMore, nil
}

func addressCandidateSQL(
	snapshot model.Snapshot,
	address []byte,
	window uint32,
	key addressKey,
	hasCursor bool,
) (string, []any, error) {
	cursorFilter := ""
	arguments := []any{
		snapshot.Cutoff.PublicationID,
		string(address),
		string(address),
	}
	if hasCursor {
		hash, err := model.ParseHash32(key.TxHash)
		if err != nil {
			return "", nil, cursor.ErrInvalid
		}
		cursorFilter = `
  AND (address_hash, address, block_number, tx_hash, output_index, publication_id)
      > (?, ?, ?, ?, ?, ?)`
		arguments = append(
			arguments,
			key.AddressHash,
			string(key.Address),
			key.BlockNumber,
			hashArgument(hash),
			key.OutputIndex,
			key.PublicationID,
		)
	}
	arguments = append(arguments, uint64(window)+1)
	return `
WITH toUInt64(?) AS publication_watermark
SELECT
    address_hash,
    address,
    block_number,
    tx_hash,
    output_index,
    publication_id
FROM outputs
WHERE address_hash = sipHash64(?)
  AND address = ?
  AND publication_id <= publication_watermark` + cursorFilter + `
ORDER BY address_hash, address, block_number, tx_hash, output_index, publication_id
LIMIT ?`, arguments, nil
}

func compareAddressKeys(left, right addressKey) int {
	switch {
	case left.AddressHash < right.AddressHash:
		return -1
	case left.AddressHash > right.AddressHash:
		return 1
	}
	if compared := bytes.Compare(left.Address, right.Address); compared != 0 {
		return compared
	}
	switch {
	case left.BlockNumber < right.BlockNumber:
		return -1
	case left.BlockNumber > right.BlockNumber:
		return 1
	}
	if left.TxHash < right.TxHash {
		return -1
	}
	if left.TxHash > right.TxHash {
		return 1
	}
	switch {
	case left.OutputIndex < right.OutputIndex:
		return -1
	case left.OutputIndex > right.OutputIndex:
		return 1
	case left.PublicationID < right.PublicationID:
		return -1
	case left.PublicationID > right.PublicationID:
		return 1
	default:
		return 0
	}
}

func (store *Store) activeAddressPublications(
	ctx context.Context,
	snapshot model.Snapshot,
	candidates []addressCandidate,
) (map[uint64]struct{}, error) {
	result := make(map[uint64]struct{}, len(candidates))
	publications := uniqueCandidatePublications(candidates)
	if len(publications) == 0 {
		return result, nil
	}
	candidateSQL, values := publicationRowsSQL(publications)
	sql := targetedFactSQL(`
        SELECT publication_id
        FROM
        (
`+candidateSQL+`
        )
        WHERE publication_id <= publication_watermark
`, `
SELECT DISTINCT ap.publication_id
FROM active_candidate_publications AS ap
ORDER BY ap.publication_id`)
	queryCtx, finish := store.instrumentPhase(
		ctx,
		"address_membership",
		hydrationPhaseLimits(uint64(len(publications))),
	)
	defer finish()
	rows, err := store.conn.Query(queryCtx, sql, activeArguments(snapshot, values...)...)
	if err != nil {
		return nil, mapQueryError("address_membership", err)
	}
	defer rows.Close()
	for rows.Next() {
		var publicationID uint64
		if err := rows.Scan(&publicationID); err != nil {
			return nil, err
		}
		if _, duplicate := result[publicationID]; duplicate {
			return nil, ErrConflictingRow
		}
		result[publicationID] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, mapQueryError("address_membership", err)
	}
	return result, nil
}

func (store *Store) hydrateAddressCandidates(
	ctx context.Context,
	snapshot model.Snapshot,
	candidates []addressCandidate,
) (map[string]model.Output, error) {
	result := make(map[string]model.Output, len(candidates))
	if len(candidates) == 0 {
		return result, nil
	}
	predicate, values := addressCandidatePredicate("o", candidates)
	sql := `
WITH toUInt64(?) AS publication_watermark
SELECT` + outputColumns + `,
    o.publication_id
FROM outputs AS o
INNER JOIN blocks AS b ON o.publication_id = b.publication_id
WHERE ` + predicate + `
  AND o.publication_id <= publication_watermark
ORDER BY
    o.address_hash,
    o.address,
    o.block_number,
    o.tx_hash,
    o.output_index,
    o.publication_id`
	arguments := append([]any{snapshot.Cutoff.PublicationID}, values...)
	queryCtx, finish := store.instrumentPhase(
		ctx,
		"address_outputs",
		hydrationPhaseLimits(uint64(len(candidates))),
	)
	defer finish()
	rows, err := store.conn.Query(queryCtx, sql, arguments...)
	if err != nil {
		return nil, mapQueryError("address_outputs", err)
	}
	defer rows.Close()
	expected := make(map[string]addressCandidate, len(candidates))
	for _, candidate := range candidates {
		expected[addressCandidateIdentity(candidate)] = candidate
	}
	for rows.Next() {
		output, publicationID, err := scanAddressOutput(rows)
		if err != nil {
			return nil, err
		}
		candidate := addressCandidate{
			key: addressKey{
				BlockNumber:   output.BlockHeight,
				TxHash:        output.Ref.TxHash.String(),
				OutputIndex:   output.Ref.Index,
				PublicationID: publicationID,
			},
			ref: output.Ref,
		}
		identity := addressCandidateIdentity(candidate)
		wanted, ok := expected[identity]
		if !ok ||
			wanted.key.BlockNumber != output.BlockHeight ||
			!bytes.Equal(wanted.key.Address, output.Address) {
			return nil, ErrConflictingRow
		}
		if _, duplicate := result[identity]; duplicate {
			return nil, ErrConflictingRow
		}
		result[identity] = output
	}
	if err := rows.Err(); err != nil {
		return nil, mapQueryError("address_outputs", err)
	}
	if len(result) != len(candidates) {
		return nil, ErrConflictingRow
	}
	return result, nil
}

func (store *Store) spendersByCandidates(
	ctx context.Context,
	snapshot model.Snapshot,
	candidates []addressCandidate,
) (map[string][]model.Hash32, error) {
	refs := make([]model.UTxORef, 0, len(candidates))
	for _, candidate := range candidates {
		refs = append(refs, candidate.ref)
	}
	return store.spendersByRefs(ctx, snapshot, uniqueRefs(refs))
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
SELECT
    i.source_tx_hash,
    i.source_output_index,
    i.tx_hash,
    i.publication_id
FROM fact_candidates AS i
INNER JOIN active_candidate_publications AS ap
    ON i.publication_id = ap.publication_id
ORDER BY
    i.source_tx_hash,
    i.source_output_index,
    i.tx_hash,
    i.publication_id`)
	queryCtx, finish := store.instrumentPhase(
		ctx,
		"address_spends",
		hydrationPhaseLimits(uint64(len(refs))*2),
	)
	defer finish()
	rows, err := store.conn.Query(queryCtx, sql, activeArguments(snapshot, values...)...)
	if err != nil {
		return nil, mapQueryError("address_spends", err)
	}
	defer rows.Close()
	type consumption struct {
		tx          model.Hash32
		publication uint64
	}
	seen := make(map[string]consumption)
	for rows.Next() {
		var sourceHash, txHash []byte
		var sourceIndex uint32
		var publicationID uint64
		if err := rows.Scan(&sourceHash, &sourceIndex, &txHash, &publicationID); err != nil {
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
		identity := fmt.Sprintf("%s/%s/%d", ref.String(), consumer.String(), publicationID)
		if _, duplicate := seen[identity]; duplicate {
			return nil, ErrConflictingRow
		}
		seen[identity] = consumption{tx: consumer, publication: publicationID}
		result[ref.String()] = append(result[ref.String()], consumer)
	}
	if err := rows.Err(); err != nil {
		return nil, mapQueryError("address_spends", err)
	}
	return result, nil
}

func uniqueCandidatePublications(candidates []addressCandidate) []uint64 {
	seen := make(map[uint64]struct{}, len(candidates))
	result := make([]uint64, 0, len(candidates))
	for _, candidate := range candidates {
		if _, ok := seen[candidate.key.PublicationID]; ok {
			continue
		}
		seen[candidate.key.PublicationID] = struct{}{}
		result = append(result, candidate.key.PublicationID)
	}
	return result
}

func addressCandidateIdentity(candidate addressCandidate) string {
	return fmt.Sprintf(
		"%d/%s/%d",
		candidate.key.PublicationID,
		candidate.ref.TxHash.String(),
		candidate.ref.Index,
	)
}

func addressCandidatePredicate(alias string, candidates []addressCandidate) (string, []any) {
	predicate := "("
	values := make([]any, 0, len(candidates)*3)
	for index, candidate := range candidates {
		if index > 0 {
			predicate += " OR "
		}
		predicate += fmt.Sprintf(
			"(%s.publication_id = ? AND %s.tx_hash = ? AND %s.output_index = ?)",
			alias,
			alias,
			alias,
		)
		values = append(
			values,
			candidate.key.PublicationID,
			hashArgument(candidate.ref.TxHash),
			candidate.ref.Index,
		)
	}
	return predicate + ")", values
}

func publicationRowsSQL(publications []uint64) (string, []any) {
	rows := ""
	values := make([]any, 0, len(publications))
	for index, publicationID := range publications {
		if index > 0 {
			rows += "\nUNION ALL\n"
		}
		rows += "SELECT toUInt64(?) AS publication_id"
		values = append(values, publicationID)
	}
	return rows, values
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
