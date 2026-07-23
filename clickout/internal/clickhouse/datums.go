package clickhouse

import (
	"bytes"
	"context"
	"errors"

	"github.com/clicksync-project/clickout/internal/model"
)

// hydrateInlineDatums resolves every inline datum in one content-addressed
// query. Datum bodies are immutable and verified against their key, while
// observation visibility remains governed by the enclosing output query's
// captured snapshot.
func (store *Store) hydrateInlineDatums(ctx context.Context, outputs []model.Output) error {
	hashes := make([]model.Hash32, 0)
	seen := make(map[string]struct{})
	for _, output := range outputs {
		if output.DatumKind != "inline" || output.DatumHash == nil {
			continue
		}
		key := output.DatumHash.String()
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		hashes = append(hashes, *output.DatumHash)
	}
	if len(hashes) == 0 {
		return nil
	}
	predicate, values := hashPredicate("datum_hash", hashes)
	sql := `
SELECT
    datum_hash,
    argMin(datum_cbor, (first_publication_id, first_seen_at)),
    argMin(byte_length, (first_publication_id, first_seen_at)),
    argMin(content_hash, (first_publication_id, first_seen_at)),
    uniqExact((datum_cbor, byte_length, content_hash))
FROM datum_bodies
WHERE ` + predicate + `
GROUP BY datum_hash
ORDER BY datum_hash`
	queryCtx, finish := store.instrument(ctx, "inline_datums_batch")
	defer finish()
	rows, err := store.conn.Query(queryCtx, sql, values...)
	if err != nil {
		return err
	}
	defer rows.Close()
	bodies := make(map[string]model.Bytes, len(hashes))
	for rows.Next() {
		var rawHash, body, contentHash []byte
		var length uint32
		var variants uint64
		if err := rows.Scan(&rawHash, &body, &length, &contentHash, &variants); err != nil {
			return err
		}
		hash, err := model.Hash32FromBytes(rawHash)
		if err != nil {
			return err
		}
		content, err := model.Hash32FromBytes(contentHash)
		if err != nil {
			return err
		}
		if variants != 1 {
			return ErrConflictingRow
		}
		verified, err := verifyInlineDatumBody(hash, body, length, content)
		if err != nil {
			return err
		}
		bodies[hash.String()] = verified
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for index := range outputs {
		if outputs[index].DatumKind != "inline" || outputs[index].DatumHash == nil {
			continue
		}
		body, exists := bodies[outputs[index].DatumHash.String()]
		if !exists {
			return errors.New("inline datum body is missing")
		}
		outputs[index].InlineDatumCBOR = model.Bytes(bytes.Clone(body))
	}
	return nil
}

func verifyInlineDatumBody(
	hash model.Hash32,
	body []byte,
	length uint32,
	content model.Hash32,
) (model.Bytes, error) {
	if uint32(len(body)) != length || content != hash || content != calculateContentHash(body) {
		return nil, errors.New("inline datum body failed content verification")
	}
	return model.Bytes(bytes.Clone(body)), nil
}
