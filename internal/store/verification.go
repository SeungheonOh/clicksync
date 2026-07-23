package store

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/ext"
	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/google/uuid"

	"clicksync/internal/publication"
)

type contentContract struct {
	table   string
	columns []string
	rows    [][]any
	filter  string
}

func (d *DB) verifyContentContractFiltered(
	ctx context.Context,
	contract contentContract,
	filter string,
	args ...any,
) error {
	tuple := "tuple(" + strings.Join(contract.columns, ", ") + ")"
	query := "SELECT SHA256(toString(" + tuple + ")) FROM clicksync." +
		contract.table + " WHERE " + filter
	actualRows, err := d.conn.Query(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("query persisted hashes: %w", err)
	}
	var actual [][]byte
	for actualRows.Next() {
		var digest []byte
		if err := actualRows.Scan(&digest); err != nil {
			actualRows.Close()
			return fmt.Errorf("scan persisted hash: %w", err)
		}
		actual = append(actual, append([]byte(nil), digest...))
	}
	if err := actualRows.Err(); err != nil {
		actualRows.Close()
		return fmt.Errorf("iterate persisted hashes: %w", err)
	}
	if err := actualRows.Close(); err != nil {
		return fmt.Errorf("close persisted hash rows: %w", err)
	}
	columnTypes, err := d.columnTypes(ctx, contract.table, contract.columns)
	if err != nil {
		return err
	}
	expected := make([][]byte, 0, len(contract.rows))
	if len(contract.rows) == 0 {
		if len(actual) != 0 {
			return fmt.Errorf("persisted rows = %d, want zero", len(actual))
		}
		return nil
	}
	externalColumns := make([]func(*ext.Table) error, 0, len(contract.columns))
	for index, columnName := range contract.columns {
		externalColumns = append(
			externalColumns,
			ext.Column(columnName, column.Type(columnTypes[index])),
		)
	}
	expectedTable, err := ext.NewTable("expected_content", externalColumns...)
	if err != nil {
		return fmt.Errorf("build typed expected table: %w", err)
	}
	for rowIndex, row := range contract.rows {
		if len(row) != len(columnTypes) {
			return fmt.Errorf(
				"expected row %d has %d values for %d columns",
				rowIndex,
				len(row),
				len(columnTypes),
			)
		}
		if err := expectedTable.Append(row...); err != nil {
			return fmt.Errorf("append typed expected row %d: %w", rowIndex, err)
		}
	}
	expectedContext := clickhouse.Context(
		ctx,
		clickhouse.WithExternalTable(expectedTable),
	)
	expectedRows, err := d.conn.Query(
		expectedContext,
		"SELECT SHA256(toString("+tuple+")) FROM expected_content",
	)
	if err != nil {
		return fmt.Errorf("bulk hash %d expected rows: %w", len(contract.rows), err)
	}
	for expectedRows.Next() {
		var digest []byte
		if err := expectedRows.Scan(&digest); err != nil {
			expectedRows.Close()
			return fmt.Errorf("scan expected hash: %w", err)
		}
		expected = append(expected, append([]byte(nil), digest...))
	}
	if err := expectedRows.Err(); err != nil {
		expectedRows.Close()
		return fmt.Errorf("iterate expected hashes: %w", err)
	}
	if err := expectedRows.Close(); err != nil {
		return fmt.Errorf("close expected hash rows: %w", err)
	}
	sortDigests(actual)
	sortDigests(expected)
	if len(actual) != len(expected) {
		return fmt.Errorf("hash row count = %d, want %d", len(actual), len(expected))
	}
	for index := range actual {
		if !bytes.Equal(actual[index], expected[index]) {
			return fmt.Errorf("persisted row hash multiset differs at index %d", index)
		}
	}
	return nil
}

func (d *DB) verifyPersistedBatchContent(
	ctx context.Context,
	attempts []publication.Attempt,
) error {
	var (
		order    []string
		combined = make(map[string]contentContract)
		ids      = make([]uint64, 0, len(attempts))
	)
	for _, attempt := range attempts {
		ids = append(ids, attempt.PublicationID)
		contracts, err := publicationContentContracts(attempt)
		if err != nil {
			return err
		}
		for _, contract := range contracts {
			existing, ok := combined[contract.table]
			if !ok {
				order = append(order, contract.table)
				existing = contentContract{
					table:   contract.table,
					columns: append([]string(nil), contract.columns...),
					filter:  contract.filter,
				}
			} else if !equalColumnNames(existing.columns, contract.columns) {
				return fmt.Errorf("batch %s content contract columns differ", contract.table)
			}
			existing.rows = append(existing.rows, contract.rows...)
			combined[contract.table] = existing
		}
	}
	for _, table := range order {
		contract := combined[table]
		filterColumn := "publication_id"
		if table == "datum_bodies" {
			filterColumn = "first_publication_id"
		}
		if err := d.verifyContentContractFiltered(
			ctx,
			contract,
			filterColumn+" IN ?",
			ids,
		); err != nil {
			return fmt.Errorf("%s batch content digest: %w", table, err)
		}
	}
	return nil
}

func equalColumnNames(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func (d *DB) columnTypes(
	ctx context.Context,
	table string,
	columns []string,
) ([]string, error) {
	rows, err := d.conn.Query(
		ctx,
		`SELECT name, type FROM system.columns WHERE database = 'clicksync' AND table = ?`,
		table,
	)
	if err != nil {
		return nil, fmt.Errorf("query column types: %w", err)
	}
	defer rows.Close()
	byName := make(map[string]string, len(columns))
	for rows.Next() {
		var name, columnType string
		if err := rows.Scan(&name, &columnType); err != nil {
			return nil, fmt.Errorf("scan column type: %w", err)
		}
		byName[name] = columnType
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate column types: %w", err)
	}
	ret := make([]string, len(columns))
	for index, column := range columns {
		columnType, ok := byName[column]
		if !ok {
			return nil, fmt.Errorf("column %s.%s is absent", table, column)
		}
		ret[index] = columnType
	}
	return ret, nil
}

func sortDigests(values [][]byte) {
	sort.Slice(values, func(i, j int) bool {
		return bytes.Compare(values[i], values[j]) < 0
	})
}

func publicationContentContracts(attempt publication.Attempt) ([]contentContract, error) {
	contracts := []contentContract{
		{
			table: "blocks",
			columns: []string{
				"publication_id", "block_hash", "parent_hash", "slot", "block_number",
				"era", "block_type", "transaction_count", "input_count", "output_count",
				"datum_observation_count", "withdrawal_count", "redeemer_count",
				"metadata_count", "synthetic", "source_peer", "source_address",
				"source_operator", "n2n_version", "network_magic", "body_hash_verified",
				"transaction_hashes_verified", "facts_digest", "writer_id", "observed_at",
				"inserted_at",
			},
			rows: [][]any{blockExpectedRow(attempt)},
		},
		{
			table: "transactions",
			columns: []string{
				"publication_id", "block_number", "tx_hash", "tx_order", "parent_tx_hash",
				"subtransaction_index", "era", "phase2_valid", "flow_kind",
				"declared_fee_lovelace", "effective_fee_lovelace", "mint_is_applied",
				"mint_policy_ids", "mint_asset_names", "mint_quantities",
				"regular_input_count", "collateral_input_count", "reference_input_count",
				"produced_output_count", "withdrawal_count", "redeemer_count",
				"metadata_present", "datum_observation_count",
			},
		},
		{
			table: "inputs",
			columns: []string{
				"publication_id", "block_number", "tx_hash", "tx_order", "source_tx_hash",
				"source_output_index", "body_ordinal", "role", "is_consumed",
				"source_is_resolved",
			},
		},
		{
			table: "outputs",
			columns: []string{
				"publication_id", "block_number", "tx_hash", "tx_order", "output_index",
				"body_ordinal", "output_kind", "address", "lovelace", "asset_policy_ids",
				"asset_names", "asset_quantities", "datum_kind", "datum_hash",
				"reference_script_hash", "reference_script_language",
			},
		},
		{
			table: "datum_bodies",
			columns: []string{
				"datum_hash", "datum_cbor", "byte_length", "content_hash",
				"first_publication_id", "first_seen_at",
			},
			filter: "first_publication_id = ?",
		},
		{
			table: "datum_observations",
			columns: []string{
				"publication_id", "block_number", "datum_hash", "tx_hash", "tx_order",
				"source_kind", "source_ordinal", "output_index",
			},
		},
		{
			table: "withdrawals",
			columns: []string{
				"publication_id", "block_number", "tx_hash", "tx_order", "body_ordinal",
				"reward_account", "lovelace", "is_applied", "credential_kind",
				"credential_hash",
			},
		},
		{
			table: "redeemers",
			columns: []string{
				"publication_id", "block_number", "tx_hash", "tx_order",
				"raw_purpose_tag", "purpose", "redeemer_index", "data_cbor",
				"data_byte_length", "data_hash", "ex_units_memory", "ex_units_steps",
				"is_applied", "resolution_status", "target_tx_hash",
				"target_output_index", "target_policy_id", "target_reward_account",
				"target_body_ordinal", "target_identity", "resolved_script_hash",
			},
		},
		{
			table: "transaction_metadata",
			columns: []string{
				"publication_id", "block_number", "tx_hash", "tx_order", "labels",
				"metadata_cbor", "byte_length", "content_hash",
			},
		},
	}
	for index := range contracts {
		if contracts[index].filter == "" {
			contracts[index].filter = "publication_id = ?"
		}
	}
	for _, transaction := range attempt.Block.Transactions {
		policies := make([][]byte, 0, len(transaction.Mint))
		names := make([][]byte, 0, len(transaction.Mint))
		quantities := make([]int64, 0, len(transaction.Mint))
		for _, asset := range transaction.Mint {
			policies = append(policies, bytesOf28(asset.PolicyID))
			names = append(names, append([]byte(nil), asset.Name...))
			quantities = append(quantities, asset.Quantity)
		}
		var regular, collateral, reference uint32
		for _, input := range transaction.Inputs {
			switch input.Role {
			case "regular":
				regular++
			case "collateral":
				collateral++
			case "reference":
				reference++
			default:
				return nil, fmt.Errorf("unknown input role %q", input.Role)
			}
		}
		contracts[1].rows = append(contracts[1].rows, []any{
			attempt.PublicationID, attempt.Block.Number, bytesOf32(transaction.Hash),
			transaction.Order, nullableHash32(transaction.ParentHash),
			transaction.SubtransactionIndex, transaction.Era, transaction.Phase2Valid,
			transaction.FlowKind, transaction.DeclaredFee, transaction.EffectiveFee,
			transaction.MintApplied, policies, names, quantities, regular, collateral,
			reference, uint32(len(transaction.Outputs)), uint32(len(transaction.Withdrawals)),
			uint32(len(transaction.Redeemers)), transaction.Metadata != nil,
			uint32(len(transaction.DatumObservations)),
		})
		for _, input := range transaction.Inputs {
			contracts[2].rows = append(contracts[2].rows, []any{
				attempt.PublicationID, attempt.Block.Number, bytesOf32(input.TransactionHash),
				input.TransactionOrder, bytesOf32(input.SourceHash), input.SourceIndex,
				input.BodyOrdinal, input.Role, input.Consumed, input.SourceResolved,
			})
		}
		for _, output := range transaction.Outputs {
			outputPolicies := make([][]byte, 0, len(output.Assets))
			outputNames := make([][]byte, 0, len(output.Assets))
			outputQuantities := make([]uint64, 0, len(output.Assets))
			for _, asset := range output.Assets {
				outputPolicies = append(outputPolicies, bytesOf28(asset.PolicyID))
				outputNames = append(outputNames, append([]byte(nil), asset.Name...))
				outputQuantities = append(outputQuantities, asset.Quantity)
			}
			contracts[3].rows = append(contracts[3].rows, []any{
				attempt.PublicationID, attempt.Block.Number, bytesOf32(output.TransactionHash),
				output.TransactionOrder, output.Index, output.BodyOrdinal, output.Kind,
				append([]byte(nil), output.Address...), output.Lovelace, outputPolicies,
				outputNames, outputQuantities, output.DatumKind,
				nullableHash32(output.DatumHash), nullableHash28(output.ReferenceScriptHash),
				output.ReferenceScriptLanguage,
			})
		}
		for _, observation := range transaction.DatumObservations {
			contracts[5].rows = append(contracts[5].rows, []any{
				attempt.PublicationID, attempt.Block.Number, bytesOf32(observation.Hash),
				bytesOf32(observation.TransactionHash), observation.TransactionOrder,
				observation.SourceKind, observation.SourceOrdinal, observation.OutputIndex,
			})
		}
		for _, withdrawal := range transaction.Withdrawals {
			contracts[6].rows = append(contracts[6].rows, []any{
				attempt.PublicationID, attempt.Block.Number,
				bytesOf32(withdrawal.TransactionHash), withdrawal.TransactionOrder,
				withdrawal.BodyOrdinal, append([]byte(nil), withdrawal.RewardAccount...),
				withdrawal.Lovelace, withdrawal.Applied, withdrawal.CredentialKind,
				bytesOf28(withdrawal.CredentialHash),
			})
		}
		for _, redeemer := range transaction.Redeemers {
			var targetReward, targetIdentity any
			if redeemer.TargetRewardAccount != nil {
				targetReward = append([]byte(nil), redeemer.TargetRewardAccount...)
			}
			if redeemer.TargetIdentity != nil {
				targetIdentity = append([]byte(nil), redeemer.TargetIdentity...)
			}
			contracts[7].rows = append(contracts[7].rows, []any{
				attempt.PublicationID, attempt.Block.Number,
				bytesOf32(redeemer.TransactionHash), redeemer.TransactionOrder,
				redeemer.RawPurposeTag, redeemer.Purpose, redeemer.Index,
				append([]byte(nil), redeemer.DataCBOR...), uint32(len(redeemer.DataCBOR)),
				bytesOf32(redeemer.DataHash), redeemer.ExUnitsMemory, redeemer.ExUnitsSteps,
				redeemer.Applied, "resolved", nullableHash32(redeemer.TargetTxHash),
				redeemer.TargetOutputIndex, nullableHash28(redeemer.TargetPolicyID),
				targetReward, redeemer.TargetBodyOrdinal, targetIdentity,
				nullableHash28(redeemer.ResolvedScriptHash),
			})
		}
		if transaction.Metadata != nil {
			metadata := transaction.Metadata
			contracts[8].rows = append(contracts[8].rows, []any{
				attempt.PublicationID, attempt.Block.Number,
				bytesOf32(metadata.TransactionHash), metadata.TransactionOrder,
				append([]uint64(nil), metadata.Labels...), append([]byte(nil), metadata.CBOR...),
				uint32(len(metadata.CBOR)), bytesOf32(metadata.ContentHash),
			})
		}
	}
	for _, body := range attempt.NewDatumBodies {
		contracts[4].rows = append(contracts[4].rows, []any{
			bytesOf32(body.Hash), append([]byte(nil), body.CBOR...), uint32(len(body.CBOR)),
			bytesOf32(body.Hash), attempt.PublicationID, attempt.InsertedAt,
		})
	}
	return contracts, nil
}

func blockExpectedRow(attempt publication.Attempt) []any {
	return []any{
		attempt.PublicationID,
		bytesOf32(attempt.Block.Hash),
		nullableHash32(attempt.Block.ParentHash),
		attempt.Block.Slot,
		attempt.Block.Number,
		attempt.Block.Era,
		attempt.Block.Type,
		uint32(attempt.Counts.Transactions),
		uint32(attempt.Counts.Inputs),
		uint32(attempt.Counts.Outputs),
		uint32(attempt.Counts.DatumObservations),
		uint32(attempt.Counts.Withdrawals),
		uint32(attempt.Counts.Redeemers),
		uint32(attempt.Counts.Metadata),
		attempt.Block.Synthetic,
		attempt.Source.PeerHost,
		attempt.Source.PeerAddress,
		attempt.Source.Operator,
		attempt.Source.N2NVersion,
		attempt.Source.NetworkMagic,
		attempt.Block.BodyHashVerified,
		attempt.Block.TransactionIDsVerified,
		bytesOf32(attempt.FactsDigest),
		uuid.UUID(attempt.WriterID),
		attempt.Block.ObservedAt.UTC(),
		attempt.InsertedAt,
	}
}
