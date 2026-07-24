package clickhouse

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/clicksync-project/clickout/internal/model"
)

func activeArguments(snapshot model.Snapshot, extra ...any) []any {
	result := make([]any, 0, 2+len(extra))
	result = append(
		result,
		snapshot.QueryHead.EventSeq,
		snapshot.Cutoff.PublicationID,
	)
	return append(result, extra...)
}

func (store *Store) UTxO(
	ctx context.Context,
	snapshot model.Snapshot,
	ref model.UTxORef,
) (model.OutputState, []model.PartialHistoryBoundary, error) {
	output, err := store.outputByRef(ctx, snapshot, ref)
	if err != nil {
		if errors.Is(err, ErrNotFound) &&
			!snapshot.Identity.CompleteHistory {
			return model.OutputState{}, []model.PartialHistoryBoundary{{
				UTxO:   ref,
				Reason: "source output is outside this partial-history dataset",
			}}, err
		}
		return model.OutputState{}, nil, err
	}
	uses, usesTruncated, err := store.usesByRef(ctx, snapshot, ref)
	if err != nil {
		return model.OutputState{}, nil, err
	}
	consumers, err := store.spendersByRef(ctx, snapshot, ref)
	if err != nil {
		return model.OutputState{}, nil, err
	}
	if len(consumers) > 1 {
		return model.OutputState{}, nil, ErrConflictingRow
	}
	state := model.OutputState{
		Output:        output,
		Uses:          uses,
		UsesTruncated: usesTruncated,
		IsCurrent:     len(consumers) == 0,
	}
	if len(consumers) == 1 {
		state.SpentBy = &consumers[0]
		edges, boundaries, err := store.hyperedgesByTx(ctx, snapshot, consumers)
		if err != nil {
			return model.OutputState{}, nil, err
		}
		if len(edges) != 1 || edges[0].Transaction.Hash != consumers[0] {
			return model.OutputState{}, nil, ErrConflictingRow
		}
		edge := edges[0]
		state.Consumption = &edge
		if err := validateOutputStateContextOccurrences(state); err != nil {
			return model.OutputState{}, nil, err
		}
		return state, boundaries, nil
	}
	if err := validateOutputStateContextOccurrences(state); err != nil {
		return model.OutputState{}, nil, err
	}
	return state, nil, nil
}

func (store *Store) outputByRef(
	ctx context.Context,
	snapshot model.Snapshot,
	ref model.UTxORef,
) (model.Output, error) {
	queryCtx, finish := store.instrument(ctx, "utxo_output")
	defer finish()
	rows, err := store.conn.Query(
		queryCtx,
		outputByRefSQL,
		activeArguments(snapshot, hashArgument(ref.TxHash), ref.Index)...,
	)
	if err != nil {
		return model.Output{}, err
	}
	defer rows.Close()
	var results []model.Output
	for rows.Next() {
		owner := &outputOwnerScanner{row: rows}
		output, err := scanOutput(owner)
		if err != nil {
			return model.Output{}, err
		}
		if err := validateOutputEraCapabilities(
			output,
			owner.era,
			owner.synthetic,
		); err != nil {
			return model.Output{}, err
		}
		results = append(results, output)
	}
	if err := rows.Err(); err != nil {
		return model.Output{}, err
	}
	switch len(results) {
	case 0:
		return model.Output{}, ErrNotFound
	case 1:
		if err := store.hydrateInlineDatums(ctx, results); err != nil {
			return model.Output{}, err
		}
		return results[0], nil
	default:
		return model.Output{}, ErrConflictingRow
	}
}

func (store *Store) usesByRef(
	ctx context.Context,
	snapshot model.Snapshot,
	ref model.UTxORef,
) ([]model.OutputUse, bool, error) {
	queryCtx, finish := store.instrument(ctx, "utxo_uses")
	defer finish()
	rows, err := store.conn.Query(
		queryCtx,
		usesByRefSQL,
		activeArguments(snapshot, hashArgument(ref.TxHash), ref.Index)...,
	)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()
	spends := make([]model.Spend, 0)
	for rows.Next() {
		use, err := scanSpend(&blockPresenceScanner{row: rows})
		if err != nil {
			return nil, false, err
		}
		spends = append(spends, use)
	}
	if err := rows.Err(); err != nil {
		return nil, false, err
	}
	if err := validateSpendRows(spends, spendByBlockThenTransaction); err != nil {
		return nil, false, err
	}
	truncated := len(spends) > 10_000
	if truncated {
		spends = spends[:10_000]
	}
	result := make([]model.OutputUse, len(spends))
	for index, spend := range spends {
		result[index] = model.NewOutputUse(spend)
	}
	return result, truncated, nil
}

func (store *Store) spendersByRef(
	ctx context.Context,
	snapshot model.Snapshot,
	ref model.UTxORef,
) ([]model.Hash32, error) {
	queryCtx, finish := store.instrument(ctx, "utxo_spend")
	defer finish()
	rows, err := store.conn.Query(
		queryCtx,
		spendByRefSQL,
		activeArguments(snapshot, hashArgument(ref.TxHash), ref.Index)...,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]model.Hash32, 0, 1)
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
	return result, rows.Err()
}

func (store *Store) Transaction(
	ctx context.Context,
	snapshot model.Snapshot,
	hash model.Hash32,
) (model.Transaction, []model.PartialHistoryBoundary, error) {
	transactions, boundaries, err := store.transactionsByTx(
		ctx,
		snapshot,
		[]model.Hash32{hash},
	)
	if err != nil {
		return model.Transaction{}, nil, err
	}
	if len(transactions) != 1 || transactions[0].Hash != hash {
		return model.Transaction{}, nil, ErrConflictingRow
	}
	return transactions[0], boundaries, nil
}

func (store *Store) Datum(
	ctx context.Context,
	snapshot model.Snapshot,
	hash model.Hash32,
) (model.Datum, error) {
	result := model.Datum{
		Hash:               hash,
		ActiveObservations: make([]model.DatumObservation, 0),
	}
	queryCtx, finish := store.instrument(ctx, "datum_body")
	rows, err := store.conn.Query(queryCtx, datumBodySQL, hashArgument(hash))
	if err != nil {
		finish()
		return model.Datum{}, err
	}
	bodyRows := 0
	for rows.Next() {
		var body []byte
		var length uint32
		var contentHash []byte
		var variants uint64
		if err := rows.Scan(&body, &length, &contentHash, &variants); err != nil {
			rows.Close()
			finish()
			return model.Datum{}, err
		}
		if len(body) == 0 ||
			len(body) > maximumTransactionContextBytes ||
			uint32(len(body)) != length {
			rows.Close()
			finish()
			return model.Datum{}, persistedRowCorruption(
				"datum body",
				errors.New(
					"datum length does not match stored byte_length",
				),
			)
		}
		if variants != 1 {
			rows.Close()
			finish()
			return model.Datum{}, ErrConflictingRow
		}
		content, err := model.Hash32FromBytes(contentHash)
		if err != nil || content != hash || content != calculateContentHash(body) {
			rows.Close()
			finish()
			return model.Datum{}, persistedRowCorruption(
				"datum body",
				errors.New("datum content hash mismatch"),
			)
		}
		result.BodyCBOR = model.Bytes(bytes.Clone(body))
		result.BodyVerified = true
		bodyRows++
	}
	err = rows.Err()
	rows.Close()
	finish()
	if err != nil {
		return model.Datum{}, err
	}
	if bodyRows > 1 {
		return model.Datum{}, ErrConflictingRow
	}

	queryCtx, finish = store.instrument(ctx, "datum_active_observations")
	defer finish()
	rows, err = store.conn.Query(
		queryCtx,
		datumObservationsSQL,
		activeArguments(snapshot, hashArgument(hash))...,
	)
	if err != nil {
		return model.Datum{}, err
	}
	defer rows.Close()
	for rows.Next() {
		var datumHash, txHash, blockHash []byte
		var sourceKind string
		owner := &outputOwnerScanner{row: rows}
		if err := owner.Scan(
			&datumHash,
			&txHash,
			&blockHash,
			&sourceKind,
		); err != nil {
			return model.Datum{}, err
		}
		level, ok := transactionEraLevel(owner.era)
		if !ok || level < 3 {
			return model.Datum{}, transactionCorruption(
				"datum observation belongs to unsupported era %q",
				owner.era,
			)
		}
		if sourceKind != "witness" &&
			(sourceKind != "inline_output" || level < 4) {
			return model.Datum{}, transactionCorruption(
				"datum observation source %q is invalid in %s",
				sourceKind,
				owner.era,
			)
		}
		datum, err := model.Hash32FromBytes(datumHash)
		if err != nil {
			return model.Datum{}, persistedRowCorruption(
				"datum observation",
				err,
			)
		}
		tx, err := model.Hash32FromBytes(txHash)
		if err != nil {
			return model.Datum{}, persistedRowCorruption(
				"datum observation",
				err,
			)
		}
		block, err := model.Hash32FromBytes(blockHash)
		if err != nil {
			return model.Datum{}, persistedRowCorruption(
				"datum observation",
				err,
			)
		}
		result.ActiveObservations = append(result.ActiveObservations, model.DatumObservation{
			DatumHash:  datum,
			TxHash:     tx,
			BlockHash:  block,
			SourceKind: sourceKind,
			Active:     true,
		})
	}
	if err := rows.Err(); err != nil {
		return model.Datum{}, err
	}
	if !result.BodyVerified && len(result.ActiveObservations) == 0 {
		return model.Datum{}, ErrNotFound
	}
	return result, nil
}

func (store *Store) Withdrawals(
	ctx context.Context,
	snapshot model.Snapshot,
	hash model.Hash32,
) ([]model.Withdrawal, error) {
	queryCtx, finish := store.instrument(ctx, "withdrawals")
	defer finish()
	transactions, _, err := store.transactionsByTx(
		queryCtx,
		snapshot,
		[]model.Hash32{hash},
	)
	if err != nil {
		return nil, err
	}
	if len(transactions) != 1 || transactions[0].Hash != hash {
		return nil, ErrConflictingRow
	}
	return transactions[0].Withdrawals, nil
}

func scanWithdrawalRow(row scanner) (model.Hash32, model.Withdrawal, error) {
	var txHash, reward, credential []byte
	var credentialKind string
	var amount uint64
	var ordinal uint32
	var applied bool
	if err := row.Scan(
		&txHash,
		&reward,
		&amount,
		&ordinal,
		&applied,
		&credentialKind,
		&credential,
	); err != nil {
		return model.Hash32{}, model.Withdrawal{}, err
	}
	owner, err := model.Hash32FromBytes(txHash)
	if err != nil {
		return model.Hash32{}, model.Withdrawal{}, err
	}
	if err := validateRewardAccount(reward, credentialKind, credential); err != nil {
		return model.Hash32{}, model.Withdrawal{}, err
	}
	return owner, model.Withdrawal{
		RewardAccount:  model.Bytes(bytes.Clone(reward)),
		Lovelace:       amount,
		BodyOrdinal:    ordinal,
		Applied:        applied,
		CredentialKind: credentialKind,
		CredentialHash: model.Bytes(bytes.Clone(credential)),
	}, nil
}

func (store *Store) Metadata(
	ctx context.Context,
	snapshot model.Snapshot,
	hash model.Hash32,
) (model.TransactionMetadata, error) {
	queryCtx, finish := store.instrument(ctx, "metadata")
	defer finish()
	transactions, _, err := store.transactionsByTx(
		queryCtx,
		snapshot,
		[]model.Hash32{hash},
	)
	if err != nil {
		return model.TransactionMetadata{}, err
	}
	if len(transactions) != 1 || transactions[0].Hash != hash {
		return model.TransactionMetadata{}, ErrConflictingRow
	}
	if transactions[0].Metadata == nil {
		return model.TransactionMetadata{}, ErrNotFound
	}
	return *transactions[0].Metadata, nil
}

func scanMetadataRow(
	row scanner,
) (model.Hash32, model.TransactionMetadata, error) {
	var txHash, cbor, contentHash []byte
	var labels []uint64
	var length uint32
	if err := row.Scan(&txHash, &labels, &cbor, &length, &contentHash); err != nil {
		return model.Hash32{}, model.TransactionMetadata{}, err
	}
	owner, err := model.Hash32FromBytes(txHash)
	if err != nil {
		return model.Hash32{}, model.TransactionMetadata{}, err
	}
	content, err := model.Hash32FromBytes(contentHash)
	if err != nil {
		return model.Hash32{}, model.TransactionMetadata{}, err
	}
	if len(cbor) == 0 ||
		len(cbor) > maximumTransactionContextBytes ||
		uint32(len(cbor)) != length {
		return model.Hash32{}, model.TransactionMetadata{}, errors.New(
			"metadata length mismatch",
		)
	}
	if content != calculateContentHash(cbor) {
		return model.Hash32{}, model.TransactionMetadata{}, errors.New(
			"metadata content hash mismatch",
		)
	}
	for index := 1; index < len(labels); index++ {
		if labels[index-1] >= labels[index] {
			return model.Hash32{}, model.TransactionMetadata{}, errors.New(
				"metadata labels are not strictly sorted and unique",
			)
		}
	}
	return owner, model.TransactionMetadata{
		Labels:      append([]uint64(nil), labels...),
		MapCBOR:     model.Bytes(bytes.Clone(cbor)),
		ByteLength:  uint64(length),
		ContentHash: content,
	}, nil
}

func validateRewardAccount(account []byte, kind string, credential []byte) error {
	if len(account) != 29 {
		return fmt.Errorf("reward account has %d bytes, want 29", len(account))
	}
	if account[0]&0x0f != 1 {
		return fmt.Errorf("reward account has non-mainnet network id %d", account[0]&0x0f)
	}
	var expectedHeader byte
	switch kind {
	case "key":
		expectedHeader = 0xe0
	case "script":
		expectedHeader = 0xf0
	default:
		return fmt.Errorf("unsupported reward credential kind %q", kind)
	}
	if account[0]&0xf0 != expectedHeader {
		return errors.New("reward credential kind disagrees with account header")
	}
	if len(credential) != 28 {
		return fmt.Errorf("credential hash has %d bytes", len(credential))
	}
	if !bytes.Equal(account[1:], credential) {
		return errors.New("reward credential hash disagrees with account")
	}
	return nil
}

func closeRows(rows driver.Rows) {
	_ = rows.Close()
}
