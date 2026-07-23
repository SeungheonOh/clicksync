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
	result = append(result, snapshot.Event, snapshot.PublicationWatermark)
	return append(result, extra...)
}

func (store *Store) UTxO(
	ctx context.Context,
	snapshot model.Snapshot,
	ref model.UTxORef,
) (model.OutputState, []model.PartialHistoryBoundary, error) {
	output, err := store.outputByRef(ctx, snapshot, ref)
	if err != nil {
		if errors.Is(err, ErrNotFound) && !snapshot.CompleteHistory {
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
	for index := range uses {
		uses[index].SourceResolved = true
		source := output
		uses[index].SourceOutput = &source
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
		if len(edges) != 1 || edges[0].Transaction != consumers[0] {
			return model.OutputState{}, nil, ErrConflictingRow
		}
		edge := edges[0]
		state.Consumption = &edge
		return state, boundaries, nil
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
		output, err := scanOutput(rows)
		if err != nil {
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
) ([]model.Spend, bool, error) {
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
	result := make([]model.Spend, 0)
	for rows.Next() {
		use, err := scanSpend(rows)
		if err != nil {
			return nil, false, err
		}
		result = append(result, use)
	}
	if err := rows.Err(); err != nil {
		return nil, false, err
	}
	truncated := len(result) > 10_000
	if truncated {
		result = result[:10_000]
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

func (store *Store) Transaction(
	ctx context.Context,
	snapshot model.Snapshot,
	hash model.Hash32,
) (model.Transaction, []model.PartialHistoryBoundary, error) {
	transaction, err := store.transactionHeader(ctx, snapshot, hash)
	if err != nil {
		return model.Transaction{}, nil, err
	}
	inputs, _, err := store.inputsByTx(ctx, snapshot, hash)
	if err != nil {
		return model.Transaction{}, nil, err
	}
	refs := make([]model.UTxORef, 0, len(inputs))
	for _, input := range inputs {
		refs = append(refs, input.Source)
	}
	resolvedOutputs, boundaries, err := store.outputsByRefs(ctx, snapshot, refs)
	if err != nil {
		return model.Transaction{}, nil, err
	}
	resolved := make(map[string]struct{}, len(resolvedOutputs))
	resolvedValues := make(map[string]model.Output, len(resolvedOutputs))
	for _, output := range resolvedOutputs {
		resolved[output.Ref.String()] = struct{}{}
		resolvedValues[output.Ref.String()] = output
	}
	for index := range inputs {
		key := inputs[index].Source.String()
		_, inputs[index].SourceResolved = resolved[key]
		if output, ok := resolvedValues[key]; ok {
			value := output
			inputs[index].SourceOutput = &value
		}
	}
	outputs, err := store.outputsByTx(ctx, snapshot, hash)
	if err != nil {
		return model.Transaction{}, nil, err
	}
	transaction.Inputs = inputs
	transaction.Outputs = outputs
	if snapshot.CompleteHistory {
		boundaries = nil
	}
	return transaction, boundaries, nil
}

func (store *Store) transactionHeader(
	ctx context.Context,
	snapshot model.Snapshot,
	hash model.Hash32,
) (model.Transaction, error) {
	queryCtx, finish := store.instrument(ctx, "transaction")
	defer finish()
	rows, err := store.conn.Query(
		queryCtx,
		transactionHeaderSQL,
		activeArguments(snapshot, hashArgument(hash))...,
	)
	if err != nil {
		return model.Transaction{}, err
	}
	defer rows.Close()
	var results []model.Transaction
	for rows.Next() {
		var (
			txHash       []byte
			blockHash    []byte
			blockNumber  uint64
			txOrder      uint32
			parentHash   *string
			subIndex     *uint32
			era          string
			phase2Valid  bool
			flowKind     string
			declaredFee  *uint64
			effectiveFee *uint64
			mintApplied  bool
			policies     []string
			names        []string
			quantities   []int64
		)
		if err := rows.Scan(
			&txHash,
			&blockHash,
			&blockNumber,
			&txOrder,
			&parentHash,
			&subIndex,
			&era,
			&phase2Valid,
			&flowKind,
			&declaredFee,
			&effectiveFee,
			&mintApplied,
			&policies,
			&names,
			&quantities,
		); err != nil {
			return model.Transaction{}, err
		}
		tx, err := model.Hash32FromBytes(txHash)
		if err != nil {
			return model.Transaction{}, err
		}
		block, err := model.Hash32FromBytes(blockHash)
		if err != nil {
			return model.Transaction{}, err
		}
		mint, err := decodeSignedAssets(policies, names, quantities)
		if err != nil {
			return model.Transaction{}, err
		}
		transaction := model.Transaction{
			Hash:                tx,
			BlockHash:           block,
			BlockHeight:         blockNumber,
			Order:               txOrder,
			SubtransactionIndex: subIndex,
			Era:                 era,
			Phase2Valid:         phase2Valid,
			FlowKind:            flowKind,
			DeclaredFee:         declaredFee,
			EffectiveFee:        effectiveFee,
			MintApplied:         mintApplied,
			Mint:                mint,
			Inputs:              make([]model.Spend, 0),
			Outputs:             make([]model.Output, 0),
		}
		if parentHash != nil {
			parent, err := model.Hash32FromBytes([]byte(*parentHash))
			if err != nil {
				return model.Transaction{}, err
			}
			transaction.ParentHash = &parent
		}
		results = append(results, transaction)
	}
	if err := rows.Err(); err != nil {
		return model.Transaction{}, err
	}
	switch len(results) {
	case 0:
		return model.Transaction{}, ErrNotFound
	case 1:
		return results[0], nil
	default:
		return model.Transaction{}, ErrConflictingRow
	}
}

func (store *Store) inputsByTx(
	ctx context.Context,
	snapshot model.Snapshot,
	hash model.Hash32,
) ([]model.Spend, []model.PartialHistoryBoundary, error) {
	queryCtx, finish := store.instrument(ctx, "transaction_inputs")
	defer finish()
	rows, err := store.conn.Query(queryCtx, inputsByTxSQL, activeArguments(snapshot, hashArgument(hash))...)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	inputs := make([]model.Spend, 0)
	boundaries := make([]model.PartialHistoryBoundary, 0)
	for rows.Next() {
		input, err := scanSpend(rows)
		if err != nil {
			return nil, nil, err
		}
		inputs = append(inputs, input)
	}
	return inputs, boundaries, rows.Err()
}

func (store *Store) outputsByTx(
	ctx context.Context,
	snapshot model.Snapshot,
	hash model.Hash32,
) ([]model.Output, error) {
	queryCtx, finish := store.instrument(ctx, "transaction_outputs")
	defer finish()
	rows, err := store.conn.Query(queryCtx, outputsByTxSQL, activeArguments(snapshot, hashArgument(hash))...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	outputs := make([]model.Output, 0)
	for rows.Next() {
		output, err := scanOutput(rows)
		if err != nil {
			return nil, err
		}
		outputs = append(outputs, output)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := store.hydrateInlineDatums(ctx, outputs); err != nil {
		return nil, err
	}
	return outputs, nil
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
		if uint32(len(body)) != length {
			rows.Close()
			finish()
			return model.Datum{}, errors.New("datum length does not match stored byte_length")
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
			return model.Datum{}, errors.New("datum content hash mismatch")
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
		if err := rows.Scan(&datumHash, &txHash, &blockHash, &sourceKind); err != nil {
			return model.Datum{}, err
		}
		datum, err := model.Hash32FromBytes(datumHash)
		if err != nil {
			return model.Datum{}, err
		}
		tx, err := model.Hash32FromBytes(txHash)
		if err != nil {
			return model.Datum{}, err
		}
		block, err := model.Hash32FromBytes(blockHash)
		if err != nil {
			return model.Datum{}, err
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
	rows, err := store.conn.Query(
		queryCtx,
		withdrawalsByTxSQL,
		activeArguments(snapshot, hashArgument(hash))...,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]model.Withdrawal, 0)
	for rows.Next() {
		var txHash, reward, credential []byte
		var credentialKind string
		var amount uint64
		var ordinal uint32
		var applied bool
		if err := rows.Scan(
			&txHash,
			&reward,
			&amount,
			&ordinal,
			&applied,
			&credentialKind,
			&credential,
		); err != nil {
			return nil, err
		}
		tx, err := model.Hash32FromBytes(txHash)
		if err != nil {
			return nil, err
		}
		if err := validateRewardAccount(reward, credentialKind, credential); err != nil {
			return nil, err
		}
		result = append(result, model.Withdrawal{
			TxHash:         tx,
			RewardAccount:  model.Bytes(bytes.Clone(reward)),
			Lovelace:       amount,
			BodyOrdinal:    ordinal,
			Applied:        applied,
			CredentialKind: credentialKind,
			CredentialHash: model.Bytes(bytes.Clone(credential)),
		})
	}
	return result, rows.Err()
}

func (store *Store) Metadata(
	ctx context.Context,
	snapshot model.Snapshot,
	hash model.Hash32,
) (model.TransactionMetadata, error) {
	queryCtx, finish := store.instrument(ctx, "metadata")
	defer finish()
	rows, err := store.conn.Query(
		queryCtx,
		metadataByTxSQL,
		activeArguments(snapshot, hashArgument(hash))...,
	)
	if err != nil {
		return model.TransactionMetadata{}, err
	}
	defer rows.Close()
	var results []model.TransactionMetadata
	for rows.Next() {
		var txHash, cbor, contentHash []byte
		var labels []uint64
		var length uint32
		if err := rows.Scan(&txHash, &labels, &cbor, &length, &contentHash); err != nil {
			return model.TransactionMetadata{}, err
		}
		tx, err := model.Hash32FromBytes(txHash)
		if err != nil {
			return model.TransactionMetadata{}, err
		}
		content, err := model.Hash32FromBytes(contentHash)
		if err != nil {
			return model.TransactionMetadata{}, err
		}
		if uint32(len(cbor)) != length {
			return model.TransactionMetadata{}, errors.New("metadata length mismatch")
		}
		if content != calculateContentHash(cbor) {
			return model.TransactionMetadata{}, errors.New("metadata content hash mismatch")
		}
		for index := 1; index < len(labels); index++ {
			if labels[index-1] >= labels[index] {
				return model.TransactionMetadata{}, errors.New(
					"metadata labels are not strictly sorted and unique",
				)
			}
		}
		results = append(results, model.TransactionMetadata{
			TxHash:      tx,
			Labels:      append([]uint64(nil), labels...),
			MapCBOR:     model.Bytes(bytes.Clone(cbor)),
			ByteLength:  uint64(length),
			ContentHash: content,
		})
	}
	if err := rows.Err(); err != nil {
		return model.TransactionMetadata{}, err
	}
	switch len(results) {
	case 0:
		return model.TransactionMetadata{}, ErrNotFound
	case 1:
		return results[0], nil
	default:
		return model.TransactionMetadata{}, ErrConflictingRow
	}
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
