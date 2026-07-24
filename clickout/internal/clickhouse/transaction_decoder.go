package clickhouse

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/clicksync-project/clickout/internal/limits"
	"github.com/clicksync-project/clickout/internal/model"
)

type transactionDeclaredCounts struct {
	RegularInputs     uint32
	CollateralInputs  uint32
	ReferenceInputs   uint32
	Outputs           uint32
	Withdrawals       uint32
	Redeemers         uint32
	MetadataPresent   bool
	DatumObservations uint32
}

type decodedTransaction struct {
	Transaction   model.Transaction
	PublicationID uint64
	Synthetic     bool
	Declared      transactionDeclaredCounts
}

type factOwner struct {
	PublicationID uint64
	BlockHash     model.Hash32
	BlockNumber   uint64
	Order         uint32
}

type ownedFactScanner struct {
	row   scanner
	owner factOwner
}

type blockPresenceScanner struct {
	row scanner
}

type outputOwnerScanner struct {
	row       scanner
	era       string
	synthetic bool
}

func (owned *outputOwnerScanner) Scan(dest ...any) error {
	var ownerValid uint8
	if err := owned.row.Scan(
		append(dest, &ownerValid, &owned.era, &owned.synthetic)...,
	); err != nil {
		return err
	}
	if ownerValid != 1 {
		return transactionCorruption(
			"active output publication has no matching block owner",
		)
	}
	return nil
}

func (linked *blockPresenceScanner) Scan(dest ...any) error {
	var blockPresent uint8
	if err := linked.row.Scan(append(dest, &blockPresent)...); err != nil {
		return err
	}
	if blockPresent != 1 {
		return transactionCorruption(
			"active fact publication has no block owner",
		)
	}
	return nil
}

func (owned *ownedFactScanner) Scan(dest ...any) error {
	var blockHash []byte
	var blockPresent uint8
	dest = append(
		dest,
		&owned.owner.PublicationID,
		&blockPresent,
		&blockHash,
		&owned.owner.BlockNumber,
		&owned.owner.Order,
	)
	if err := owned.row.Scan(dest...); err != nil {
		return err
	}
	if blockPresent != 1 {
		return transactionCorruption(
			"active fact publication has no block owner",
		)
	}
	block, err := model.Hash32FromBytes(blockHash)
	if err != nil {
		return err
	}
	owned.owner.BlockHash = block
	return nil
}

const (
	transactionDecoderMaxChildRows = uint64(250_000)
	maximumTransactionContextBytes = 2 * 1024 * 1024
)

func (store *Store) transactionsByTx(
	ctx context.Context,
	snapshot model.Snapshot,
	hashes []model.Hash32,
) ([]model.Transaction, []model.PartialHistoryBoundary, error) {
	if err := validateRequestedTransactionHashes(hashes); err != nil {
		return nil, nil, err
	}
	records, err := store.decodeTransactionHeaders(ctx, snapshot, hashes)
	if err != nil {
		return nil, nil, err
	}
	if err := validateDeclaredBatchBounds(records); err != nil {
		return nil, nil, err
	}
	if err := store.decodeTransactionInputs(ctx, snapshot, hashes, records); err != nil {
		return nil, nil, err
	}
	if err := store.decodeTransactionOutputs(ctx, snapshot, hashes, records); err != nil {
		return nil, nil, err
	}
	if err := store.decodeTransactionWithdrawals(ctx, snapshot, hashes, records); err != nil {
		return nil, nil, err
	}
	contextBodies := newContextBodyPool()
	if err := store.decodeTransactionRedeemers(
		ctx,
		snapshot,
		hashes,
		records,
		contextBodies,
	); err != nil {
		return nil, nil, err
	}
	if err := store.decodeTransactionMetadata(
		ctx,
		snapshot,
		hashes,
		records,
		contextBodies,
	); err != nil {
		return nil, nil, err
	}
	if err := store.decodeTransactionDatums(
		ctx,
		snapshot,
		hashes,
		records,
		contextBodies,
	); err != nil {
		return nil, nil, err
	}
	boundaries, err := store.resolveDecodedTransactionSources(
		ctx,
		snapshot,
		records,
		contextBodies,
	)
	if err != nil {
		return nil, nil, err
	}
	result := make([]model.Transaction, 0, len(hashes))
	for _, hash := range hashes {
		record := records[hash.String()]
		if record == nil {
			return nil, nil, ErrConflictingRow
		}
		if err := validateDecodedTransaction(record); err != nil {
			return nil, nil, err
		}
		result = append(result, record.Transaction)
	}
	if err := validateTransactionContextOccurrences(result); err != nil {
		return nil, nil, err
	}
	return result, boundaries, nil
}

func validateRequestedTransactionHashes(hashes []model.Hash32) error {
	if len(hashes) == 0 || len(hashes) > int(limits.HardMaxTraceEdges) {
		return fmt.Errorf("%w: invalid transaction decoder batch size", ErrResourceLimit)
	}
	for index, hash := range hashes {
		if hash == (model.Hash32{}) {
			return errors.New("transaction decoder hash is zero")
		}
		if index > 0 && bytes.Compare(hashes[index-1][:], hash[:]) >= 0 {
			return errors.New(
				"transaction decoder hashes must be strictly sorted and unique",
			)
		}
	}
	return nil
}

func validateFactOwner(
	record *decodedTransaction,
	owner factOwner,
	hash model.Hash32,
) error {
	transaction := record.Transaction
	if hash != transaction.Hash ||
		owner.PublicationID != record.PublicationID ||
		owner.BlockHash != transaction.BlockHash ||
		owner.BlockNumber != transaction.BlockHeight ||
		owner.Order != transaction.Order {
		return fmt.Errorf("%w: transaction child ownership mismatch", ErrConflictingRow)
	}
	return nil
}

func transactionRecord(
	records map[string]*decodedTransaction,
	hash model.Hash32,
) (*decodedTransaction, error) {
	record := records[hash.String()]
	if record == nil {
		return nil, fmt.Errorf("%w: child row has no active transaction", ErrConflictingRow)
	}
	return record, nil
}

func (store *Store) decodeTransactionHeaders(
	ctx context.Context,
	snapshot model.Snapshot,
	hashes []model.Hash32,
) (map[string]*decodedTransaction, error) {
	predicate, values := hashPredicate("t.tx_hash", hashes)
	sql := targetedFactSQL(`
        SELECT *
        FROM transactions AS t
        WHERE `+predicate+`
          AND t.publication_id <= publication_watermark
`, `
SELECT
    t.tx_hash,
    ifNull(b.block_hash, toFixedString('', 32)),
    t.block_number,
    t.tx_order,
    t.parent_tx_hash,
    t.subtransaction_index,
    t.era,
    t.phase2_valid,
    t.flow_kind,
    t.declared_fee_lovelace,
    t.effective_fee_lovelace,
    t.mint_is_applied,
    t.mint_policy_ids,
    t.mint_asset_names,
    t.mint_quantities,
    t.regular_input_count,
    t.collateral_input_count,
    t.reference_input_count,
    t.produced_output_count,
    t.withdrawal_count,
    t.redeemer_count,
    t.metadata_present,
    t.datum_observation_count,
    t.publication_id,
    toUInt8(
        ifNull(b.present, toUInt8(0)) = 1
        AND ifNull(b.block_number, toUInt64(0)) = t.block_number
    ),
    ifNull(b.block_number, toUInt64(0)),
    ifNull(b.era, ''),
    ifNull(b.transaction_count, toUInt32(0)),
    ifNull(b.synthetic, false)
FROM fact_candidates AS t
INNER JOIN active_candidate_publications AS ap
    ON t.publication_id = ap.publication_id
LEFT JOIN candidate_blocks AS b
    ON t.publication_id = b.publication_id
ORDER BY t.tx_hash, t.publication_id, t.tx_order`)
	queryCtx, finish := store.instrumentPhase(
		ctx,
		"transaction_headers_batch",
		hydrationPhaseLimits(uint64(len(hashes))+1),
	)
	rows, err := store.conn.Query(
		queryCtx,
		sql,
		activeArguments(snapshot, values...)...,
	)
	if err != nil {
		finish()
		return nil, mapQueryError("transaction_headers_batch", err)
	}
	defer rows.Close()
	defer finish()
	records := make(map[string]*decodedTransaction, len(hashes))
	for rows.Next() {
		var (
			txHash           []byte
			blockHash        []byte
			blockNumber      uint64
			txOrder          uint32
			parentHash       *string
			subIndex         *uint32
			era              string
			phase2Valid      bool
			flowKind         string
			declaredFee      *uint64
			effectiveFee     *uint64
			mintApplied      bool
			policies         []string
			names            []string
			quantities       []int64
			declared         transactionDeclaredCounts
			publicationID    uint64
			blockPresent     uint8
			ownerBlockNumber uint64
			ownerEra         string
			transactionCount uint32
			synthetic        bool
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
			&declared.RegularInputs,
			&declared.CollateralInputs,
			&declared.ReferenceInputs,
			&declared.Outputs,
			&declared.Withdrawals,
			&declared.Redeemers,
			&declared.MetadataPresent,
			&declared.DatumObservations,
			&publicationID,
			&blockPresent,
			&ownerBlockNumber,
			&ownerEra,
			&transactionCount,
			&synthetic,
		); err != nil {
			return nil, mapQueryError("transaction_headers_batch", err)
		}
		if blockPresent != 1 {
			return nil, transactionCorruption(
				"active transaction publication has no block owner",
			)
		}
		hash, err := model.Hash32FromBytes(txHash)
		if err != nil {
			return nil, persistedRowCorruption("transaction header", err)
		}
		block, err := model.Hash32FromBytes(blockHash)
		if err != nil {
			return nil, persistedRowCorruption("transaction header", err)
		}
		if blockNumber != ownerBlockNumber ||
			era != ownerEra ||
			txOrder >= transactionCount ||
			block == (model.Hash32{}) {
			return nil, fmt.Errorf("%w: transaction block ownership mismatch", ErrConflictingRow)
		}
		mint, err := decodeSignedAssets(policies, names, quantities)
		if err != nil {
			return nil, persistedRowCorruption("transaction header", err)
		}
		transaction := model.Transaction{
			Hash:                hash,
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
		}
		if parentHash != nil {
			parent, err := model.Hash32FromBytes([]byte(*parentHash))
			if err != nil {
				return nil, persistedRowCorruption(
					"transaction header",
					err,
				)
			}
			transaction.ParentHash = &parent
		}
		key := hash.String()
		if _, duplicate := records[key]; duplicate {
			return nil, ErrConflictingRow
		}
		record := &decodedTransaction{
			Transaction:   transaction,
			PublicationID: publicationID,
			Synthetic:     synthetic,
			Declared:      declared,
		}
		if err := validateDeclaredTransactionBounds(record); err != nil {
			return nil, err
		}
		record.Transaction.Inputs = make([]model.Spend, 0)
		record.Transaction.Outputs = make([]model.Output, 0)
		record.Transaction.Withdrawals = make([]model.Withdrawal, 0)
		record.Transaction.Redeemers = make([]model.Redeemer, 0)
		record.Transaction.Datums = make([]model.TransactionDatum, 0)
		records[key] = record
	}
	if err := rows.Err(); err != nil {
		return nil, mapQueryError("transaction_headers_batch", err)
	}
	if len(records) != len(hashes) {
		if len(hashes) == 1 && len(records) == 0 {
			return nil, ErrNotFound
		}
		return nil, ErrConflictingRow
	}
	return records, nil
}

func validateDeclaredTransactionBounds(record *decodedTransaction) error {
	declared := record.Declared
	counts := []uint32{
		declared.RegularInputs,
		declared.CollateralInputs,
		declared.ReferenceInputs,
		declared.Outputs,
		declared.Withdrawals,
		declared.Redeemers,
		declared.DatumObservations,
	}
	var total uint64
	for _, count := range counts {
		if uint64(count) > transactionDecoderMaxChildRows {
			return &ResourceLimitError{
				Phase: "transaction_header_counts",
				Cause: fmt.Errorf(
					"transaction %s declares %d context rows, limit %d",
					record.Transaction.Hash,
					count,
					transactionDecoderMaxChildRows,
				),
			}
		}
		total += uint64(count)
	}
	if total > transactionDecoderMaxChildRows {
		return &ResourceLimitError{
			Phase: "transaction_header_counts",
			Cause: fmt.Errorf(
				"transaction %s declares %d total context rows, limit %d",
				record.Transaction.Hash,
				total,
				transactionDecoderMaxChildRows,
			),
		}
	}
	return nil
}

type transactionBatchRows struct {
	Inputs       uint64
	Outputs      uint64
	Withdrawals  uint64
	Redeemers    uint64
	Metadata     uint64
	Observations uint64
}

func declaredBatchRows(
	records map[string]*decodedTransaction,
) (transactionBatchRows, error) {
	var rows transactionBatchRows
	for _, record := range records {
		declared := record.Declared
		values := []struct {
			target *uint64
			add    uint64
		}{
			{
				&rows.Inputs,
				uint64(declared.RegularInputs) +
					uint64(declared.CollateralInputs) +
					uint64(declared.ReferenceInputs),
			},
			{&rows.Outputs, uint64(declared.Outputs)},
			{&rows.Withdrawals, uint64(declared.Withdrawals)},
			{&rows.Redeemers, uint64(declared.Redeemers)},
			{&rows.Observations, uint64(declared.DatumObservations)},
		}
		if declared.MetadataPresent {
			values = append(values, struct {
				target *uint64
				add    uint64
			}{&rows.Metadata, 1})
		}
		for _, value := range values {
			if value.add > transactionDecoderMaxChildRows-*value.target {
				return transactionBatchRows{}, &ResourceLimitError{
					Phase: "transaction_batch_counts",
					Cause: fmt.Errorf(
						"declared child rows exceed %d",
						transactionDecoderMaxChildRows,
					),
				}
			}
			*value.target += value.add
		}
	}
	return rows, nil
}

func validateDeclaredBatchBounds(
	records map[string]*decodedTransaction,
) error {
	_, err := declaredBatchRows(records)
	return err
}

func declaredPhaseLimits(rows uint64) phaseLimits {
	return hydrationPhaseLimits(rows + 1)
}

func (store *Store) decodeTransactionInputs(
	ctx context.Context,
	snapshot model.Snapshot,
	hashes []model.Hash32,
	records map[string]*decodedTransaction,
) error {
	declared, err := declaredBatchRows(records)
	if err != nil {
		return err
	}
	predicate, values := hashPredicate("i.tx_hash", hashes)
	sql := targetedFactSQL(`
        SELECT *
        FROM inputs AS i
        WHERE `+predicate+`
          AND i.publication_id <= publication_watermark
`, `
SELECT
    i.source_tx_hash,
    i.source_output_index,
    i.tx_hash,
    ifNull(b.block_hash, toFixedString('', 32)),
    i.block_number,
    i.role,
    i.body_ordinal,
    i.is_consumed,
    i.source_is_resolved,
    i.publication_id,
    toUInt8(
        ifNull(b.present, toUInt8(0)) = 1
        AND ifNull(b.block_number, toUInt64(0)) = i.block_number
    ),
    ifNull(b.block_hash, toFixedString('', 32)),
    i.block_number,
    i.tx_order
FROM fact_candidates AS i
INNER JOIN active_candidate_publications AS ap
    ON i.publication_id = ap.publication_id
LEFT JOIN candidate_blocks AS b
    ON i.publication_id = b.publication_id
ORDER BY
    i.tx_hash,
    multiIf(i.role = 'regular', 0, i.role = 'collateral', 1, i.role = 'reference', 2, 3),
    i.body_ordinal,
    i.source_tx_hash,
    i.source_output_index,
    i.publication_id`)
	queryCtx, finish := store.instrumentPhase(
		ctx,
		"transaction_inputs_batch",
		declaredPhaseLimits(declared.Inputs),
	)
	rows, err := store.conn.Query(
		queryCtx,
		sql,
		activeArguments(snapshot, values...)...,
	)
	if err != nil {
		finish()
		return mapQueryError("transaction_inputs_batch", err)
	}
	defer rows.Close()
	defer finish()
	for rows.Next() {
		owned := &ownedFactScanner{row: rows}
		input, err := scanSpend(owned)
		if err != nil {
			return persistedRowCorruption("input", err)
		}
		record, err := transactionRecord(records, input.ConsumingTx)
		if err != nil {
			return err
		}
		if err := validateFactOwner(record, owned.owner, input.ConsumingTx); err != nil {
			return err
		}
		if input.ConsumingBlockHash != owned.owner.BlockHash ||
			input.ConsumingBlockHeight != owned.owner.BlockNumber {
			return fmt.Errorf("%w: input block ownership mismatch", ErrConflictingRow)
		}
		record.Transaction.Inputs = append(record.Transaction.Inputs, input)
	}
	if err := rows.Err(); err != nil {
		return mapQueryError("transaction_inputs_batch", err)
	}
	return nil
}

func (store *Store) decodeTransactionOutputs(
	ctx context.Context,
	snapshot model.Snapshot,
	hashes []model.Hash32,
	records map[string]*decodedTransaction,
) error {
	declared, err := declaredBatchRows(records)
	if err != nil {
		return err
	}
	predicate, values := hashPredicate("o.tx_hash", hashes)
	sql := targetedFactSQL(`
        SELECT *
        FROM outputs AS o
        WHERE `+predicate+`
          AND o.publication_id <= publication_watermark
`, `
SELECT`+outputColumns+`,
    o.publication_id,
    toUInt8(
        ifNull(b.present, toUInt8(0)) = 1
        AND ifNull(b.block_number, toUInt64(0)) = o.block_number
    ),
    ifNull(b.block_hash, toFixedString('', 32)),
    o.block_number,
    o.tx_order
FROM fact_candidates AS o
INNER JOIN active_candidate_publications AS ap
    ON o.publication_id = ap.publication_id
LEFT JOIN candidate_blocks AS b
    ON o.publication_id = b.publication_id
ORDER BY
    o.tx_hash,
    o.body_ordinal,
    o.output_index,
    o.publication_id`)
	queryCtx, finish := store.instrumentPhase(
		ctx,
		"transaction_outputs_batch",
		declaredPhaseLimits(declared.Outputs),
	)
	rows, err := store.conn.Query(
		queryCtx,
		sql,
		activeArguments(snapshot, values...)...,
	)
	if err != nil {
		finish()
		return mapQueryError("transaction_outputs_batch", err)
	}
	defer rows.Close()
	defer finish()
	for rows.Next() {
		owned := &ownedFactScanner{row: rows}
		output, err := scanOutput(owned)
		if err != nil {
			return persistedRowCorruption("output", err)
		}
		record, err := transactionRecord(records, output.ProducingTx)
		if err != nil {
			return err
		}
		if err := validateFactOwner(record, owned.owner, output.ProducingTx); err != nil {
			return err
		}
		if output.BlockHash != owned.owner.BlockHash ||
			output.BlockHeight != owned.owner.BlockNumber {
			return fmt.Errorf("%w: output block ownership mismatch", ErrConflictingRow)
		}
		record.Transaction.Outputs = append(record.Transaction.Outputs, output)
	}
	if err := rows.Err(); err != nil {
		return mapQueryError("transaction_outputs_batch", err)
	}
	return nil
}

func (store *Store) decodeTransactionWithdrawals(
	ctx context.Context,
	snapshot model.Snapshot,
	hashes []model.Hash32,
	records map[string]*decodedTransaction,
) error {
	declared, err := declaredBatchRows(records)
	if err != nil {
		return err
	}
	predicate, values := hashPredicate("w.tx_hash", hashes)
	sql := targetedFactSQL(`
        SELECT *
        FROM withdrawals AS w
        WHERE `+predicate+`
          AND w.publication_id <= publication_watermark
`, `
SELECT
    w.tx_hash,
    w.reward_account,
    w.lovelace,
    w.body_ordinal,
    w.is_applied,
    w.credential_kind,
    w.credential_hash,
    w.publication_id,
    toUInt8(
        ifNull(b.present, toUInt8(0)) = 1
        AND ifNull(b.block_number, toUInt64(0)) = w.block_number
    ),
    ifNull(b.block_hash, toFixedString('', 32)),
    w.block_number,
    w.tx_order
FROM fact_candidates AS w
INNER JOIN active_candidate_publications AS ap
    ON w.publication_id = ap.publication_id
LEFT JOIN candidate_blocks AS b
    ON w.publication_id = b.publication_id
ORDER BY w.tx_hash, w.body_ordinal, w.publication_id`)
	queryCtx, finish := store.instrumentPhase(
		ctx,
		"transaction_withdrawals_batch",
		declaredPhaseLimits(declared.Withdrawals),
	)
	rows, err := store.conn.Query(
		queryCtx,
		sql,
		activeArguments(snapshot, values...)...,
	)
	if err != nil {
		finish()
		return mapQueryError("transaction_withdrawals_batch", err)
	}
	defer rows.Close()
	defer finish()
	for rows.Next() {
		owned := &ownedFactScanner{row: rows}
		hash, withdrawal, err := scanWithdrawalRow(owned)
		if err != nil {
			return persistedRowCorruption("withdrawal", err)
		}
		record, err := transactionRecord(records, hash)
		if err != nil {
			return err
		}
		if err := validateFactOwner(record, owned.owner, hash); err != nil {
			return err
		}
		record.Transaction.Withdrawals = append(
			record.Transaction.Withdrawals,
			withdrawal,
		)
	}
	if err := rows.Err(); err != nil {
		return mapQueryError("transaction_withdrawals_batch", err)
	}
	return nil
}

func (store *Store) decodeTransactionRedeemers(
	ctx context.Context,
	snapshot model.Snapshot,
	hashes []model.Hash32,
	records map[string]*decodedTransaction,
	contextBodies *contextBodyPool,
) error {
	declared, err := declaredBatchRows(records)
	if err != nil {
		return err
	}
	predicate, values := hashPredicate("r.tx_hash", hashes)
	sql := targetedFactSQL(`
        SELECT *
        FROM redeemers AS r
        WHERE `+predicate+`
          AND r.publication_id <= publication_watermark
`, `
SELECT
    r.tx_hash,
    r.raw_purpose_tag,
    r.purpose,
    r.redeemer_index,
    r.data_cbor,
    r.data_byte_length,
    r.data_hash,
    r.ex_units_memory,
    r.ex_units_steps,
    r.is_applied,
    r.resolution_status,
    r.target_tx_hash,
    r.target_output_index,
    r.target_policy_id,
    r.target_reward_account,
    r.target_body_ordinal,
    r.target_identity,
    r.resolved_script_hash,
    r.publication_id,
    toUInt8(
        ifNull(b.present, toUInt8(0)) = 1
        AND ifNull(b.block_number, toUInt64(0)) = r.block_number
    ),
    ifNull(b.block_hash, toFixedString('', 32)),
    r.block_number,
    r.tx_order
FROM fact_candidates AS r
INNER JOIN active_candidate_publications AS ap
    ON r.publication_id = ap.publication_id
LEFT JOIN candidate_blocks AS b
    ON r.publication_id = b.publication_id
ORDER BY
    r.tx_hash,
    multiIf(
        r.purpose = 'spend', 0,
        r.purpose = 'mint', 1,
        r.purpose = 'reward', 2,
        r.purpose = 'certificate', 3,
        r.purpose = 'vote', 4,
        r.purpose = 'proposal', 5,
        6
    ),
    r.redeemer_index,
    r.publication_id`)
	queryCtx, finish := store.instrumentPhase(
		ctx,
		"transaction_redeemers_batch",
		declaredPhaseLimits(declared.Redeemers),
	)
	rows, err := store.conn.Query(
		queryCtx,
		sql,
		activeArguments(snapshot, values...)...,
	)
	if err != nil {
		finish()
		return mapQueryError("transaction_redeemers_batch", err)
	}
	defer rows.Close()
	defer finish()
	for rows.Next() {
		owned := &ownedFactScanner{row: rows}
		hash, redeemer, err := scanRedeemerRow(owned)
		if err != nil {
			return persistedRowCorruption("redeemer", err)
		}
		record, err := transactionRecord(records, hash)
		if err != nil {
			return err
		}
		if err := validateFactOwner(record, owned.owner, hash); err != nil {
			return err
		}
		retained, err := contextBodies.retain(
			"transaction_context_bodies",
			calculateContentHash(redeemer.DataCBOR),
			redeemer.DataCBOR,
		)
		if err != nil {
			return err
		}
		redeemer.DataCBOR = retained
		record.Transaction.Redeemers = append(
			record.Transaction.Redeemers,
			redeemer,
		)
	}
	if err := rows.Err(); err != nil {
		return mapQueryError("transaction_redeemers_batch", err)
	}
	return nil
}

func (store *Store) decodeTransactionMetadata(
	ctx context.Context,
	snapshot model.Snapshot,
	hashes []model.Hash32,
	records map[string]*decodedTransaction,
	contextBodies *contextBodyPool,
) error {
	declared, err := declaredBatchRows(records)
	if err != nil {
		return err
	}
	predicate, values := hashPredicate("m.tx_hash", hashes)
	sql := targetedFactSQL(`
        SELECT *
        FROM transaction_metadata AS m
        WHERE `+predicate+`
          AND m.publication_id <= publication_watermark
`, `
SELECT
    m.tx_hash,
    m.labels,
    m.metadata_cbor,
    m.byte_length,
    m.content_hash,
    m.publication_id,
    toUInt8(
        ifNull(b.present, toUInt8(0)) = 1
        AND ifNull(b.block_number, toUInt64(0)) = m.block_number
    ),
    ifNull(b.block_hash, toFixedString('', 32)),
    m.block_number,
    m.tx_order
FROM fact_candidates AS m
INNER JOIN active_candidate_publications AS ap
    ON m.publication_id = ap.publication_id
LEFT JOIN candidate_blocks AS b
    ON m.publication_id = b.publication_id
ORDER BY m.tx_hash, m.publication_id`)
	queryCtx, finish := store.instrumentPhase(
		ctx,
		"transaction_metadata_batch",
		declaredPhaseLimits(declared.Metadata),
	)
	rows, err := store.conn.Query(
		queryCtx,
		sql,
		activeArguments(snapshot, values...)...,
	)
	if err != nil {
		finish()
		return mapQueryError("transaction_metadata_batch", err)
	}
	defer rows.Close()
	defer finish()
	for rows.Next() {
		owned := &ownedFactScanner{row: rows}
		hash, metadata, err := scanMetadataRow(owned)
		if err != nil {
			return persistedRowCorruption("metadata", err)
		}
		record, err := transactionRecord(records, hash)
		if err != nil {
			return err
		}
		if err := validateFactOwner(record, owned.owner, hash); err != nil {
			return err
		}
		if record.Transaction.Metadata != nil {
			return ErrConflictingRow
		}
		retained, err := contextBodies.retain(
			"transaction_context_bodies",
			metadata.ContentHash,
			metadata.MapCBOR,
		)
		if err != nil {
			return err
		}
		metadata.MapCBOR = retained
		value := metadata
		record.Transaction.Metadata = &value
	}
	if err := rows.Err(); err != nil {
		return mapQueryError("transaction_metadata_batch", err)
	}
	return nil
}

func (store *Store) decodeTransactionDatums(
	ctx context.Context,
	snapshot model.Snapshot,
	hashes []model.Hash32,
	records map[string]*decodedTransaction,
	contextBodies *contextBodyPool,
) error {
	declared, err := declaredBatchRows(records)
	if err != nil {
		return err
	}
	predicate, values := hashPredicate("d.tx_hash", hashes)
	sql := targetedFactSQL(`
        SELECT *
        FROM datum_observations AS d
        WHERE `+predicate+`
          AND d.publication_id <= publication_watermark
`, `
SELECT
    d.datum_hash,
    d.tx_hash,
    d.source_kind,
    d.source_ordinal,
    d.output_index,
    d.publication_id,
    toUInt8(
        ifNull(b.present, toUInt8(0)) = 1
        AND ifNull(b.block_number, toUInt64(0)) = d.block_number
    ),
    ifNull(b.block_hash, toFixedString('', 32)),
    d.block_number,
    d.tx_order
FROM fact_candidates AS d
INNER JOIN active_candidate_publications AS ap
    ON d.publication_id = ap.publication_id
LEFT JOIN candidate_blocks AS b
    ON d.publication_id = b.publication_id
ORDER BY
    d.tx_hash,
    d.datum_hash,
    d.source_kind,
    d.source_ordinal,
    d.output_index,
    d.publication_id`)
	queryCtx, finish := store.instrumentPhase(
		ctx,
		"transaction_datum_observations_batch",
		declaredPhaseLimits(declared.Observations),
	)
	rows, err := store.conn.Query(
		queryCtx,
		sql,
		activeArguments(snapshot, values...)...,
	)
	if err != nil {
		finish()
		return mapQueryError("transaction_datum_observations_batch", err)
	}
	for rows.Next() {
		var (
			rawDatum      []byte
			rawOwner      []byte
			sourceKind    string
			sourceOrdinal uint32
			outputIndex   *uint32
			publicationID uint64
			blockPresent  uint8
			rawBlock      []byte
			blockNumber   uint64
			order         uint32
		)
		if err := rows.Scan(
			&rawDatum,
			&rawOwner,
			&sourceKind,
			&sourceOrdinal,
			&outputIndex,
			&publicationID,
			&blockPresent,
			&rawBlock,
			&blockNumber,
			&order,
		); err != nil {
			rows.Close()
			finish()
			return mapQueryError("transaction_datum_observations_batch", err)
		}
		if blockPresent != 1 {
			rows.Close()
			finish()
			return transactionCorruption(
				"active datum observation publication has no block owner",
			)
		}
		datumHash, err := model.Hash32FromBytes(rawDatum)
		if err != nil {
			rows.Close()
			finish()
			return persistedRowCorruption("datum observation", err)
		}
		ownerHash, err := model.Hash32FromBytes(rawOwner)
		if err != nil {
			rows.Close()
			finish()
			return persistedRowCorruption("datum observation", err)
		}
		blockHash, err := model.Hash32FromBytes(rawBlock)
		if err != nil {
			rows.Close()
			finish()
			return persistedRowCorruption("datum observation", err)
		}
		record, err := transactionRecord(records, ownerHash)
		if err != nil {
			rows.Close()
			finish()
			return err
		}
		if err := validateFactOwner(record, factOwner{
			PublicationID: publicationID,
			BlockHash:     blockHash,
			BlockNumber:   blockNumber,
			Order:         order,
		}, ownerHash); err != nil {
			rows.Close()
			finish()
			return err
		}
		observation := model.TransactionDatumObservation{
			SourceKind:    sourceKind,
			SourceOrdinal: sourceOrdinal,
			OutputIndex:   outputIndex,
		}
		datums := record.Transaction.Datums
		if len(datums) == 0 || datums[len(datums)-1].Hash != datumHash {
			record.Transaction.Datums = append(
				record.Transaction.Datums,
				model.TransactionDatum{
					Hash:         datumHash,
					Observations: make([]model.TransactionDatumObservation, 0, 1),
				},
			)
			datums = record.Transaction.Datums
		}
		last := &record.Transaction.Datums[len(record.Transaction.Datums)-1]
		last.Observations = append(last.Observations, observation)
	}
	rowsErr := rows.Err()
	rows.Close()
	finish()
	if rowsErr != nil {
		return mapQueryError("transaction_datum_observations_batch", rowsErr)
	}

	hashSet := make(map[string]model.Hash32)
	for _, record := range records {
		for _, datum := range record.Transaction.Datums {
			hashSet[datum.Hash.String()] = datum.Hash
		}
		for _, output := range record.Transaction.Outputs {
			if output.DatumKind == "inline" && output.DatumHash != nil {
				hashSet[output.DatumHash.String()] = *output.DatumHash
			}
		}
	}
	bodyHashes := make([]model.Hash32, 0, len(hashSet))
	for _, hash := range hashSet {
		bodyHashes = append(bodyHashes, hash)
	}
	sort.Slice(bodyHashes, func(left, right int) bool {
		return bytes.Compare(bodyHashes[left][:], bodyHashes[right][:]) < 0
	})
	bodies, err := store.loadDatumBodies(
		ctx,
		bodyHashes,
		"transaction_datum_bodies_batch",
		contextBodies,
	)
	if err != nil {
		return err
	}
	for _, record := range records {
		for index := range record.Transaction.Datums {
			datum := &record.Transaction.Datums[index]
			body, exists := bodies[datum.Hash.String()]
			if !exists {
				return transactionCorruption(
					"transaction datum body is missing",
				)
			}
			datum.BodyCBOR = body
			datum.BodyVerified = true
		}
		for index := range record.Transaction.Outputs {
			output := &record.Transaction.Outputs[index]
			if output.DatumKind != "inline" || output.DatumHash == nil {
				continue
			}
			body, exists := bodies[output.DatumHash.String()]
			if !exists {
				return transactionCorruption(
					"inline datum body is missing",
				)
			}
			output.InlineDatumCBOR = body
		}
	}
	return nil
}

func (store *Store) resolveDecodedTransactionSources(
	ctx context.Context,
	snapshot model.Snapshot,
	records map[string]*decodedTransaction,
	contextBodies *contextBodyPool,
) ([]model.PartialHistoryBoundary, error) {
	refs := make([]model.UTxORef, 0)
	for _, record := range records {
		for _, input := range record.Transaction.Inputs {
			refs = append(refs, input.Source)
		}
		for _, redeemer := range record.Transaction.Redeemers {
			if redeemer.Purpose == "spend" &&
				redeemer.Target.SourceUTxO != nil {
				refs = append(refs, *redeemer.Target.SourceUTxO)
			}
		}
	}
	refs = uniqueRefs(refs)
	if len(refs) > int(transactionDecoderMaxChildRows) {
		return nil, &ResourceLimitError{
			Phase: "transaction_source_outputs",
			Cause: fmt.Errorf(
				"source lookup has %d refs, limit %d",
				len(refs),
				transactionDecoderMaxChildRows,
			),
		}
	}
	outputs, boundaries, err := store.outputsByRefs(
		ctx,
		snapshot,
		refs,
		contextBodies,
	)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, transactionCorruption(
				"complete-history transaction source output is missing",
			)
		}
		return nil, err
	}
	byRef := make(map[string]*model.Output, len(outputs))
	for index := range outputs {
		output := &outputs[index]
		key := output.Ref.String()
		if _, duplicate := byRef[key]; duplicate {
			return nil, ErrConflictingRow
		}
		byRef[key] = output
	}
	for _, record := range records {
		for index := range record.Transaction.Inputs {
			input := &record.Transaction.Inputs[index]
			output := byRef[input.Source.String()]
			found := output != nil
			if input.SourceResolved != found {
				return nil, fmt.Errorf(
					"%w: persisted source resolution disagrees with active output",
					ErrConflictingRow,
				)
			}
			if found {
				input.SourceOutput = output
			}
		}
		for index := range record.Transaction.Redeemers {
			redeemer := &record.Transaction.Redeemers[index]
			if redeemer.Purpose != "spend" ||
				redeemer.Target.SourceUTxO == nil {
				continue
			}
			redeemer.Target.SourceOutput =
				byRef[redeemer.Target.SourceUTxO.String()]
		}
	}
	return boundaries, nil
}

func transactionCorruption(format string, arguments ...any) error {
	return fmt.Errorf(
		"%w: %s",
		ErrConflictingRow,
		fmt.Sprintf(format, arguments...),
	)
}

func persistedRowCorruption(kind string, err error) error {
	if errors.Is(err, ErrConflictingRow) ||
		errors.Is(err, ErrResourceLimit) {
		return err
	}
	return transactionCorruption("invalid persisted %s row: %v", kind, err)
}

func validateDecodedTransaction(record *decodedTransaction) error {
	transaction := &record.Transaction
	if transaction.Hash == (model.Hash32{}) ||
		transaction.BlockHash == (model.Hash32{}) {
		return transactionCorruption("zero transaction or block hash")
	}
	if transaction.ParentHash != nil ||
		transaction.SubtransactionIndex != nil {
		return transactionCorruption(
			"nested transaction linkage is unsupported",
		)
	}
	if err := validateDecodedInputs(transaction, record.Declared); err != nil {
		return err
	}
	if err := validateDecodedOutputs(transaction, record.Declared); err != nil {
		return err
	}
	if err := validateDecodedWithdrawals(transaction, record.Declared); err != nil {
		return err
	}
	if err := validateDecodedRedeemers(transaction, record.Declared); err != nil {
		return err
	}
	if err := validateDecodedMetadata(transaction, record.Declared); err != nil {
		return err
	}
	if err := validateDecodedDatums(transaction, record.Declared); err != nil {
		return err
	}
	if err := validateDecodedMint(transaction); err != nil {
		return err
	}
	if !validTransactionEra(transaction.Era) {
		return transactionCorruption(
			"unsupported transaction era %q",
			transaction.Era,
		)
	}
	if err := validateTransactionEraCapabilities(
		transaction,
		record.Synthetic,
	); err != nil {
		return err
	}
	switch transaction.FlowKind {
	case "regular":
		feesValid := transaction.DeclaredFee != nil &&
			transaction.EffectiveFee != nil &&
			*transaction.DeclaredFee == *transaction.EffectiveFee
		if transaction.Era == "Byron" {
			feesValid = transaction.DeclaredFee == nil &&
				transaction.EffectiveFee == nil
		}
		if record.Synthetic ||
			!transaction.Phase2Valid ||
			!feesValid ||
			!transaction.MintApplied {
			return transactionCorruption("invalid regular transaction header semantics")
		}
	case "collateral":
		if record.Synthetic ||
			transaction.Phase2Valid ||
			transaction.MintApplied ||
			!collateralEra(transaction.Era) ||
			transaction.DeclaredFee == nil ||
			(transaction.EffectiveFee != nil &&
				*transaction.EffectiveFee == 0) {
			return transactionCorruption("invalid collateral transaction header semantics")
		}
		if err := validateDecodedCollateralFee(transaction); err != nil {
			return err
		}
	case "genesis":
		if !record.Synthetic ||
			!transaction.Phase2Valid ||
			transaction.Era != "Byron" ||
			transaction.BlockHeight != 0 ||
			transaction.ParentHash != nil ||
			transaction.SubtransactionIndex != nil ||
			transaction.DeclaredFee != nil ||
			transaction.EffectiveFee != nil ||
			transaction.MintApplied ||
			len(transaction.Mint) != 0 ||
			len(transaction.Inputs) != 0 ||
			len(transaction.Outputs) != 1 ||
			len(transaction.Withdrawals) != 0 ||
			len(transaction.Redeemers) != 0 ||
			transaction.Metadata != nil ||
			len(transaction.Datums) != 0 {
			return transactionCorruption("invalid canonical genesis transaction")
		}
		output := transaction.Outputs[0]
		if output.Kind != model.OutputGenesis ||
			output.Ref.Index != 0 ||
			output.BodyOrdinal != 0 ||
			output.Lovelace == 0 ||
			len(output.Assets) != 0 ||
			output.DatumKind != "none" ||
			output.DatumHash != nil ||
			len(output.InlineDatumCBOR) != 0 ||
			output.ReferenceScriptHash != nil ||
			output.ReferenceScriptLanguage != "" ||
			output.PaymentCredentialKind != "none" ||
			len(output.PaymentCredentialHash) != 0 ||
			calculateContentHash(output.Address) != transaction.Hash {
			return transactionCorruption("invalid canonical genesis output")
		}
		kind, credential, err := paymentCredentialFromRawAddress(output.Address)
		if err != nil || kind != "none" || len(credential) != 0 {
			return transactionCorruption(
				"canonical genesis output is not a valid Byron address",
			)
		}
	default:
		return transactionCorruption(
			"unsupported transaction flow kind %q",
			transaction.FlowKind,
		)
	}
	return nil
}

func validTransactionEra(era string) bool {
	_, ok := transactionEraLevel(era)
	return ok
}

func transactionEraLevel(era string) (int, bool) {
	switch era {
	case "Byron":
		return 0, true
	case "Shelley":
		return 1, true
	case "Allegra":
		return 1, true
	case "Mary":
		return 2, true
	case "Alonzo":
		return 3, true
	case "Babbage":
		return 4, true
	case "Conway":
		return 5, true
	case "Dijkstra":
		return 6, true
	default:
		return 0, false
	}
}

func collateralEra(era string) bool {
	switch era {
	case "Alonzo", "Babbage", "Conway", "Dijkstra":
		return true
	default:
		return false
	}
}

func validateTransactionEraCapabilities(
	transaction *model.Transaction,
	synthetic bool,
) error {
	level, ok := transactionEraLevel(transaction.Era)
	if !ok {
		return transactionCorruption(
			"unsupported transaction era %q",
			transaction.Era,
		)
	}
	if level == 0 &&
		(transaction.Metadata != nil ||
			len(transaction.Withdrawals) != 0) {
		return transactionCorruption(
			"Byron transaction contains post-Byron context",
		)
	}
	if level < 2 && len(transaction.Mint) != 0 {
		return transactionCorruption(
			"%s transaction contains multi-asset mint",
			transaction.Era,
		)
	}
	if level < 3 {
		if !transaction.Phase2Valid ||
			len(transaction.Redeemers) != 0 ||
			len(transaction.Datums) != 0 {
			return transactionCorruption(
				"%s transaction contains Plutus context",
				transaction.Era,
			)
		}
	}
	for _, input := range transaction.Inputs {
		if level < 3 && input.Role == model.InputCollateral {
			return transactionCorruption(
				"%s transaction contains collateral inputs",
				transaction.Era,
			)
		}
		if level < 4 && input.Role == model.InputReference {
			return transactionCorruption(
				"%s transaction contains reference inputs",
				transaction.Era,
			)
		}
	}
	for _, output := range transaction.Outputs {
		if err := validateOutputEraCapabilities(
			output,
			transaction.Era,
			synthetic,
		); err != nil {
			return err
		}
	}
	for _, redeemer := range transaction.Redeemers {
		if level < 5 &&
			(redeemer.Purpose == "vote" ||
				redeemer.Purpose == "proposal") {
			return transactionCorruption(
				"%s transaction contains Conway redeemer purpose %q",
				transaction.Era,
				redeemer.Purpose,
			)
		}
	}
	return nil
}

func validateOutputEraCapabilities(
	output model.Output,
	era string,
	synthetic bool,
) error {
	level, ok := transactionEraLevel(era)
	if !ok {
		return transactionCorruption("unsupported output owner era %q", era)
	}
	if synthetic {
		if era != "Byron" ||
			output.Kind != model.OutputGenesis ||
			output.BlockHeight != 0 ||
			output.Ref.Index != 0 ||
			output.BodyOrdinal != 0 ||
			output.Lovelace == 0 ||
			len(output.Assets) != 0 ||
			output.DatumKind != "none" ||
			output.DatumHash != nil ||
			len(output.InlineDatumCBOR) != 0 ||
			output.ReferenceScriptHash != nil ||
			output.ReferenceScriptLanguage != "" ||
			output.PaymentCredentialKind != "none" ||
			len(output.PaymentCredentialHash) != 0 ||
			calculateContentHash(output.Address) != output.ProducingTx {
			return transactionCorruption(
				"synthetic genesis output has invalid canonical shape",
			)
		}
	} else if level < 4 {
		if output.Kind != model.OutputRegular {
			return transactionCorruption(
				"%s output has unsupported kind %q",
				era,
				output.Kind,
			)
		}
	} else if output.Kind != model.OutputRegular &&
		output.Kind != model.OutputCollateralReturn {
		return transactionCorruption(
			"%s output has unsupported kind %q",
			era,
			output.Kind,
		)
	}
	switch output.Kind {
	case model.OutputRegular:
		if output.Ref.Index != output.BodyOrdinal {
			return transactionCorruption(
				"regular output index/body ordinal mismatch",
			)
		}
	case model.OutputCollateralReturn:
		if output.BodyOrdinal != 0 {
			return transactionCorruption(
				"collateral return body ordinal is not zero",
			)
		}
	case model.OutputGenesis:
		if output.Ref.Index != 0 || output.BodyOrdinal != 0 {
			return transactionCorruption(
				"genesis output index/body ordinal mismatch",
			)
		}
	}
	if level == 0 {
		kind, credential, err := paymentCredentialFromRawAddress(output.Address)
		if err != nil ||
			kind != "none" ||
			len(credential) != 0 ||
			output.PaymentCredentialKind != "none" ||
			len(output.PaymentCredentialHash) != 0 {
			return transactionCorruption(
				"Byron output does not contain a canonical Byron address",
			)
		}
	}
	if level >= 4 {
		if err := validateShelleyOutputAddressShape(output.Address, false); err != nil {
			return transactionCorruption(
				"%s output contains a non-canonical address: %v",
				era,
				err,
			)
		}
	}
	if level < 2 && len(output.Assets) != 0 {
		return transactionCorruption(
			"%s output contains multi-assets",
			era,
		)
	}
	if level < 3 && output.DatumKind != "none" {
		return transactionCorruption(
			"%s output contains a datum",
			era,
		)
	}
	if level < 4 {
		if output.DatumKind == "inline" ||
			output.ReferenceScriptHash != nil ||
			output.ReferenceScriptLanguage != "" ||
			output.Kind == model.OutputCollateralReturn {
			return transactionCorruption(
				"%s output contains Babbage features",
				era,
			)
		}
		return nil
	}
	if output.ReferenceScriptHash == nil {
		return nil
	}
	maximumPlutus := 2
	if level >= 5 {
		maximumPlutus = 3
	}
	if level >= 6 {
		maximumPlutus = 4
	}
	switch output.ReferenceScriptLanguage {
	case "native":
		return nil
	case "plutus_v1":
		return nil
	case "plutus_v2":
		return nil
	case "plutus_v3":
		if maximumPlutus >= 3 {
			return nil
		}
	case "plutus_v4":
		if maximumPlutus >= 4 {
			return nil
		}
	}
	return transactionCorruption(
		"reference script language %q is unsupported in %s",
		output.ReferenceScriptLanguage,
		era,
	)
}

func validateDecodedInputs(
	transaction *model.Transaction,
	declared transactionDeclaredCounts,
) error {
	if err := validateCompleteSpendRows(transaction.Inputs); err != nil {
		return err
	}
	counts := map[model.InputRole]uint32{
		model.InputRegular:    0,
		model.InputCollateral: 0,
		model.InputReference:  0,
	}
	roleRefs := make(map[string]struct{}, len(transaction.Inputs))
	refRoles := make(map[string]uint8, len(transaction.Inputs))
	for _, input := range transaction.Inputs {
		if input.ConsumingTx != transaction.Hash ||
			input.ConsumingBlockHash != transaction.BlockHash ||
			input.ConsumingBlockHeight != transaction.BlockHeight {
			return transactionCorruption("input transaction ownership mismatch")
		}
		if input.SourceResolved != (input.SourceOutput != nil) {
			return transactionCorruption("input source resolution state mismatch")
		}
		if input.SourceOutput != nil &&
			input.SourceOutput.Ref != input.Source {
			return transactionCorruption("input source output reference mismatch")
		}
		roleKey := string(input.Role) + "/" + input.Source.String()
		if _, duplicate := roleRefs[roleKey]; duplicate {
			return transactionCorruption("duplicate input role/reference")
		}
		roleRefs[roleKey] = struct{}{}
		switch input.Role {
		case model.InputRegular:
			counts[input.Role]++
			if input.IsConsumed != transaction.Phase2Valid {
				return transactionCorruption(
					"regular input consumption disagrees with phase-2 validity",
				)
			}
			refRoles[input.Source.String()] |= 1
		case model.InputCollateral:
			counts[input.Role]++
			if input.IsConsumed == transaction.Phase2Valid {
				return transactionCorruption(
					"collateral input consumption disagrees with phase-2 validity",
				)
			}
			refRoles[input.Source.String()] |= 2
		case model.InputReference:
			counts[input.Role]++
			if input.IsConsumed {
				return transactionCorruption("reference input is consumed")
			}
			refRoles[input.Source.String()] |= 4
		default:
			return transactionCorruption("unknown input role %q", input.Role)
		}
	}
	for _, roles := range refRoles {
		if roles&1 != 0 && roles&4 != 0 {
			return transactionCorruption(
				"regular and reference inputs share a UTxO",
			)
		}
	}
	if counts[model.InputRegular] != declared.RegularInputs ||
		counts[model.InputCollateral] != declared.CollateralInputs ||
		counts[model.InputReference] != declared.ReferenceInputs {
		return transactionCorruption("declared input counts mismatch")
	}
	return nil
}

func validateDecodedOutputs(
	transaction *model.Transaction,
	declared transactionDeclaredCounts,
) error {
	if uint32(len(transaction.Outputs)) != declared.Outputs {
		return transactionCorruption("declared output count mismatch")
	}
	if err := validateCompleteOutputRows(transaction.Outputs); err != nil {
		return err
	}
	for position, output := range transaction.Outputs {
		if output.ProducingTx != transaction.Hash ||
			output.Ref.TxHash != transaction.Hash ||
			output.BlockHash != transaction.BlockHash ||
			output.BlockHeight != transaction.BlockHeight {
			return transactionCorruption("output transaction ownership mismatch")
		}
		switch transaction.FlowKind {
		case "regular":
			if output.Kind != model.OutputRegular ||
				output.Ref.Index != uint32(position) ||
				output.BodyOrdinal != uint32(position) {
				return transactionCorruption(
					"regular output index/kind/ordinal mismatch",
				)
			}
		case "collateral":
			if len(transaction.Outputs) > 1 ||
				output.Kind != model.OutputCollateralReturn ||
				output.BodyOrdinal != 0 {
				return transactionCorruption(
					"collateral return shape mismatch",
				)
			}
		case "genesis":
			if output.Kind != model.OutputGenesis ||
				output.Ref.Index != uint32(position) ||
				output.BodyOrdinal != uint32(position) {
				return transactionCorruption(
					"genesis output index/kind/ordinal mismatch",
				)
			}
		}
	}
	return nil
}

func validateDecodedWithdrawals(
	transaction *model.Transaction,
	declared transactionDeclaredCounts,
) error {
	if uint32(len(transaction.Withdrawals)) != declared.Withdrawals {
		return transactionCorruption("declared withdrawal count mismatch")
	}
	for index, withdrawal := range transaction.Withdrawals {
		if withdrawal.BodyOrdinal != uint32(index) {
			return transactionCorruption(
				"withdrawal body ordinals are not consecutive",
			)
		}
		if withdrawal.Applied != transaction.Phase2Valid {
			return transactionCorruption(
				"withdrawal application disagrees with phase-2 validity",
			)
		}
		if err := validateRewardAccount(
			withdrawal.RewardAccount,
			withdrawal.CredentialKind,
			withdrawal.CredentialHash,
		); err != nil {
			return transactionCorruption("invalid withdrawal: %v", err)
		}
		if index > 0 && compareWithdrawalAccounts(
			transaction.Withdrawals[index-1],
			withdrawal,
		) >= 0 {
			return transactionCorruption(
				"withdrawals are not in canonical AccountAddress order",
			)
		}
	}
	return nil
}

func compareWithdrawalAccounts(left, right model.Withdrawal) int {
	leftNetwork := left.RewardAccount[0] & 0x0f
	rightNetwork := right.RewardAccount[0] & 0x0f
	switch {
	case leftNetwork < rightNetwork:
		return -1
	case leftNetwork > rightNetwork:
		return 1
	}
	credentialRank := func(kind string) int {
		if kind == "script" {
			return 0
		}
		return 1
	}
	if compared := credentialRank(left.CredentialKind) -
		credentialRank(right.CredentialKind); compared != 0 {
		return compared
	}
	return bytes.Compare(left.CredentialHash, right.CredentialHash)
}

func validateDecodedRedeemers(
	transaction *model.Transaction,
	declared transactionDeclaredCounts,
) error {
	if uint32(len(transaction.Redeemers)) != declared.Redeemers {
		return transactionCorruption("declared redeemer count mismatch")
	}
	regularInputs := make(map[string]*model.Spend)
	orderedRegularInputs := make([]model.UTxORef, 0)
	for index := range transaction.Inputs {
		input := &transaction.Inputs[index]
		if input.Role == model.InputRegular {
			regularInputs[input.Source.String()] = input
			orderedRegularInputs = append(orderedRegularInputs, input.Source)
		}
	}
	sort.Slice(orderedRegularInputs, func(left, right int) bool {
		if compared := bytes.Compare(
			orderedRegularInputs[left].TxHash[:],
			orderedRegularInputs[right].TxHash[:],
		); compared != 0 {
			return compared < 0
		}
		return orderedRegularInputs[left].Index <
			orderedRegularInputs[right].Index
	})
	mintPolicies := make([]model.PolicyID, 0)
	for _, asset := range transaction.Mint {
		if len(mintPolicies) == 0 ||
			mintPolicies[len(mintPolicies)-1] != asset.PolicyID {
			mintPolicies = append(mintPolicies, asset.PolicyID)
		}
	}
	previousRank := -1
	var previousIndex uint32
	for position := range transaction.Redeemers {
		redeemer := &transaction.Redeemers[position]
		if len(redeemer.DataCBOR) == 0 ||
			len(redeemer.DataCBOR) > maximumTransactionContextBytes {
			return transactionCorruption("redeemer CBOR length is invalid")
		}
		if redeemer.Applied != transaction.Phase2Valid {
			return transactionCorruption(
				"redeemer application disagrees with phase-2 validity",
			)
		}
		if err := validateRedeemerTarget(*redeemer); err != nil {
			return transactionCorruption("invalid redeemer target: %v", err)
		}
		rank, ok := redeemerPurposeRank(redeemer.Purpose)
		if !ok {
			return transactionCorruption(
				"unsupported redeemer purpose %q",
				redeemer.Purpose,
			)
		}
		if position > 0 &&
			(previousRank > rank ||
				(previousRank == rank && previousIndex >= redeemer.Index)) {
			return transactionCorruption(
				"redeemers are not strictly sorted and unique",
			)
		}
		previousRank = rank
		previousIndex = redeemer.Index
		switch redeemer.Purpose {
		case "spend":
			if int(redeemer.Index) >= len(orderedRegularInputs) ||
				redeemer.Target.SourceUTxO == nil ||
				*redeemer.Target.SourceUTxO !=
					orderedRegularInputs[redeemer.Index] {
				return transactionCorruption(
					"spend redeemer target disagrees with ledger pointer",
				)
			}
		case "mint":
			if int(redeemer.Index) >= len(mintPolicies) ||
				redeemer.Target.PolicyID == nil ||
				*redeemer.Target.PolicyID != mintPolicies[redeemer.Index] {
				return transactionCorruption(
					"mint redeemer target disagrees with ledger pointer",
				)
			}
		case "reward":
			if int(redeemer.Index) >= len(transaction.Withdrawals) ||
				!bytes.Equal(
					redeemer.Target.RewardAccount,
					transaction.Withdrawals[redeemer.Index].RewardAccount,
				) {
				return transactionCorruption(
					"reward redeemer target disagrees with ledger pointer",
				)
			}
		case "certificate", "vote", "proposal":
			if redeemer.Target.BodyOrdinal == nil ||
				*redeemer.Target.BodyOrdinal != redeemer.Index {
				return transactionCorruption(
					"%s redeemer target disagrees with ledger pointer",
					redeemer.Purpose,
				)
			}
		}
		if redeemer.Purpose != "spend" {
			if redeemer.Target.SourceOutput != nil {
				return transactionCorruption(
					"non-spend redeemer has a source output",
				)
			}
			continue
		}
		ref := redeemer.Target.SourceUTxO
		if ref == nil {
			return transactionCorruption("spend redeemer has no target")
		}
		input := regularInputs[ref.String()]
		if input == nil {
			return transactionCorruption(
				"spend redeemer target is not a regular input",
			)
		}
		if input.SourceOutput != redeemer.Target.SourceOutput {
			return transactionCorruption(
				"spend redeemer does not share canonical source output",
			)
		}
		if redeemer.Target.SourceOutput == nil {
			if len(redeemer.Target.ScriptHash) != 0 {
				return transactionCorruption(
					"unresolved spend redeemer retains a script hash",
				)
			}
			continue
		}
		if err := validateSpendScriptContext(
			*redeemer,
			*redeemer.Target.SourceOutput,
		); err != nil {
			return transactionCorruption(
				"invalid spend script context: %v",
				err,
			)
		}
	}
	return nil
}

func redeemerPurposeRank(purpose string) (int, bool) {
	switch purpose {
	case "spend":
		return 0, true
	case "mint":
		return 1, true
	case "reward":
		return 2, true
	case "certificate":
		return 3, true
	case "vote":
		return 4, true
	case "proposal":
		return 5, true
	default:
		return 0, false
	}
}

func validateDecodedMetadata(
	transaction *model.Transaction,
	declared transactionDeclaredCounts,
) error {
	if (transaction.Metadata != nil) != declared.MetadataPresent {
		return transactionCorruption("declared metadata presence mismatch")
	}
	if transaction.Metadata == nil {
		return nil
	}
	metadata := transaction.Metadata
	if len(metadata.MapCBOR) == 0 ||
		len(metadata.MapCBOR) > maximumTransactionContextBytes ||
		uint64(len(metadata.MapCBOR)) != metadata.ByteLength ||
		calculateContentHash(metadata.MapCBOR) != metadata.ContentHash {
		return transactionCorruption("metadata body verification mismatch")
	}
	for index := 1; index < len(metadata.Labels); index++ {
		if metadata.Labels[index-1] >= metadata.Labels[index] {
			return transactionCorruption(
				"metadata labels are not sorted and unique",
			)
		}
	}
	return nil
}

func validateDecodedDatums(
	transaction *model.Transaction,
	declared transactionDeclaredCounts,
) error {
	if !transaction.DatumContextValid() {
		return transactionCorruption("invalid grouped transaction datum context")
	}
	outputs := make(map[uint32]*model.Output, len(transaction.Outputs))
	inlineObservations := make(map[uint32]uint32)
	witnessOrdinals := make(map[uint32]struct{})
	var observationCount uint64
	for index := range transaction.Outputs {
		output := &transaction.Outputs[index]
		outputs[output.Ref.Index] = output
	}
	for _, datum := range transaction.Datums {
		if len(datum.BodyCBOR) > maximumTransactionContextBytes ||
			calculateContentHash(datum.BodyCBOR) != datum.Hash {
			return transactionCorruption("transaction datum content hash mismatch")
		}
		observationCount += uint64(len(datum.Observations))
		for _, observation := range datum.Observations {
			if observation.SourceKind == "witness" {
				if _, duplicate := witnessOrdinals[observation.SourceOrdinal]; duplicate {
					return transactionCorruption(
						"duplicate witness datum observation ordinal",
					)
				}
				witnessOrdinals[observation.SourceOrdinal] = struct{}{}
				continue
			}
			if observation.OutputIndex == nil {
				return transactionCorruption(
					"inline datum observation lacks output index",
				)
			}
			output := outputs[*observation.OutputIndex]
			if output == nil ||
				output.BodyOrdinal != observation.SourceOrdinal ||
				output.DatumKind != "inline" ||
				output.DatumHash == nil ||
				*output.DatumHash != datum.Hash ||
				!bytes.Equal(output.InlineDatumCBOR, datum.BodyCBOR) {
				return transactionCorruption(
					"inline datum observation disagrees with output",
				)
			}
			inlineObservations[*observation.OutputIndex]++
		}
	}
	for ordinal := uint32(0); ordinal < uint32(len(witnessOrdinals)); ordinal++ {
		if _, exists := witnessOrdinals[ordinal]; !exists {
			return transactionCorruption(
				"witness datum observation ordinals are not consecutive",
			)
		}
	}
	if observationCount != uint64(declared.DatumObservations) {
		return transactionCorruption("declared datum observation count mismatch")
	}
	for _, output := range transaction.Outputs {
		if output.DatumKind == "inline" &&
			inlineObservations[output.Ref.Index] != 1 {
			return transactionCorruption(
				"inline output lacks exactly one datum observation",
			)
		}
	}
	return nil
}

func validateDecodedMint(transaction *model.Transaction) error {
	for index, asset := range transaction.Mint {
		if asset.Quantity == 0 || len(asset.Name) > 32 {
			return transactionCorruption("invalid mint asset")
		}
		if index > 0 {
			previous := transaction.Mint[index-1]
			if compared := bytes.Compare(
				previous.PolicyID[:],
				asset.PolicyID[:],
			); compared > 0 ||
				(compared == 0 &&
					bytes.Compare(previous.Name, asset.Name) >= 0) {
				return transactionCorruption(
					"mint assets are not sorted and unique",
				)
			}
		}
	}
	return nil
}

func validateDecodedCollateralFee(transaction *model.Transaction) error {
	var (
		total       uint64
		count       int
		allResolved = true
	)
	for _, input := range transaction.Inputs {
		if input.Role != model.InputCollateral {
			continue
		}
		count++
		if input.SourceOutput == nil {
			allResolved = false
			continue
		}
		if ^uint64(0)-total < input.SourceOutput.Lovelace {
			return transactionCorruption("collateral input sum overflows")
		}
		total += input.SourceOutput.Lovelace
	}
	if count == 0 || !allResolved {
		return nil
	}
	var returned uint64
	if len(transaction.Outputs) == 1 {
		returned = transaction.Outputs[0].Lovelace
	}
	if returned >= total {
		return transactionCorruption(
			"collateral return is not below resolved inputs",
		)
	}
	derived := total - returned
	if transaction.EffectiveFee == nil ||
		*transaction.EffectiveFee != derived {
		return transactionCorruption(
			"effective collateral fee disagrees with resolved inputs",
		)
	}
	return nil
}
