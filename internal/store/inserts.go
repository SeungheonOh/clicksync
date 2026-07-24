package store

import (
	"bytes"
	"context"
	"fmt"

	"github.com/blinklabs-io/gouroboros/ledger"

	"cardano-clicksync/internal/model"
)

const insertBlocksSQL = `INSERT INTO clicksync.blocks
(
    publication_id, block_hash, parent_hash, slot, block_number, era, block_type,
    transaction_count, input_count, output_count, datum_observation_count,
    withdrawal_count, redeemer_count, metadata_count, synthetic, content_hash,
    relay_hosts, relay_addresses, relay_operators, n2n_versions, network_magic,
    observed_at, inserted_at
)`

const insertTransactionsSQL = `INSERT INTO clicksync.transactions
(
    publication_id, block_number, tx_hash, tx_order, parent_tx_hash,
    subtransaction_index, era, phase2_valid, flow_kind, declared_fee_lovelace,
    effective_fee_lovelace, mint_is_applied, mint_policy_ids, mint_asset_names,
    mint_quantities, regular_input_count, collateral_input_count,
    reference_input_count, produced_output_count, withdrawal_count,
    redeemer_count, metadata_present, datum_observation_count
)`

const insertInputsSQL = `INSERT INTO clicksync.inputs
(
    publication_id, block_number, tx_hash, tx_order, source_tx_hash,
    source_output_index, body_ordinal, role, is_consumed
)`

const insertOutputsSQL = `INSERT INTO clicksync.outputs
(
    publication_id, block_number, tx_hash, tx_order, output_index, body_ordinal,
    output_kind, address, payment_credential_kind, payment_credential_hash,
    lovelace, asset_policy_ids, asset_names, asset_quantities, datum_kind,
    datum_hash, reference_script_hash, reference_script_language
)`

const insertDatumBodiesSQL = `INSERT INTO clicksync.datum_bodies
(
    publication_id, block_number, datum_hash, datum_cbor, byte_length,
    observed_at
)`

const insertDatumObservationsSQL = `INSERT INTO clicksync.datum_observations
(
    publication_id, block_number, datum_hash, tx_hash, tx_order, source_kind,
    source_ordinal, output_index
)`

const insertWithdrawalsSQL = `INSERT INTO clicksync.withdrawals
(
    publication_id, block_number, tx_hash, tx_order, body_ordinal,
    reward_account, lovelace, is_applied, credential_kind, credential_hash
)`

const insertRedeemersSQL = `INSERT INTO clicksync.redeemers
(
    publication_id, block_number, tx_hash, tx_order, raw_purpose_tag, purpose,
    redeemer_index, data_cbor, data_byte_length, data_hash, ex_units_memory,
    ex_units_steps, is_applied, target_tx_hash, target_output_index,
    target_policy_id, target_reward_account, target_body_ordinal,
    target_identity, resolved_script_hash
)`

const insertMetadataSQL = `INSERT INTO clicksync.transaction_metadata
(
    publication_id, block_number, tx_hash, tx_order, labels, metadata_cbor,
    byte_length, content_hash
)`

type factCounts struct {
	transactions      uint64
	inputs            uint64
	outputs           uint64
	datumObservations uint64
	withdrawals       uint64
	redeemers         uint64
	metadata          uint64
}

func countFacts(block model.Block) factCounts {
	counts := factCounts{transactions: uint64(len(block.Transactions))}
	for _, transaction := range block.Transactions {
		counts.inputs += uint64(len(transaction.Inputs))
		counts.outputs += uint64(len(transaction.Outputs))
		counts.datumObservations += uint64(len(transaction.DatumObservations))
		counts.withdrawals += uint64(len(transaction.Withdrawals))
		counts.redeemers += uint64(len(transaction.Redeemers))
		if transaction.Metadata != nil {
			counts.metadata++
		}
	}
	return counts
}

func pointForBlock(block model.Block) Point {
	return Point{
		Slot:        block.Slot,
		Hash:        block.Hash,
		BlockNumber: block.Number,
		IsByronEBB: block.Era == "Byron" &&
			block.Type == int16(ledger.BlockTypeByronEbb),
	}
}

func (d *DB) insertBlocks(
	ctx context.Context,
	networkMagic uint32,
	publications []publication,
) error {
	batch, err := d.conn.PrepareBatch(ctx, insertBlocksSQL)
	if err != nil {
		return err
	}
	for _, item := range publications {
		hosts := make([]string, len(item.Relays))
		addresses := make([]string, len(item.Relays))
		operators := make([]string, len(item.Relays))
		versions := make([]uint16, len(item.Relays))
		for index, relay := range item.Relays {
			hosts[index] = relay.Host
			addresses[index] = relay.Address
			operators[index] = relay.Operator
			versions[index] = relay.N2NVersion
		}
		if err := batch.Append(
			item.PublicationID,
			bytes32(item.Block.Hash),
			nullableHash32(item.Block.ParentHash),
			item.Block.Slot,
			item.Block.Number,
			item.Block.Era,
			item.Block.Type,
			uint32(item.Counts.transactions),
			uint32(item.Counts.inputs),
			uint32(item.Counts.outputs),
			uint32(item.Counts.datumObservations),
			uint32(item.Counts.withdrawals),
			uint32(item.Counts.redeemers),
			uint32(item.Counts.metadata),
			item.Block.Synthetic,
			bytes32(item.ContentHash),
			hosts,
			addresses,
			operators,
			versions,
			networkMagic,
			item.Block.ObservedAt.UTC(),
			item.InsertedAt,
		); err != nil {
			_ = batch.Abort()
			return err
		}
	}
	return batch.Send()
}

func (d *DB) insertTransactions(
	ctx context.Context,
	publications []publication,
) error {
	if !hasTransactions(publications) {
		return nil
	}
	batch, err := d.conn.PrepareBatch(ctx, insertTransactionsSQL)
	if err != nil {
		return err
	}
	for _, item := range publications {
		for _, transaction := range item.Block.Transactions {
			policies := make([][]byte, len(transaction.Mint))
			names := make([][]byte, len(transaction.Mint))
			quantities := make([]int64, len(transaction.Mint))
			for index, asset := range transaction.Mint {
				policies[index] = bytes28(asset.PolicyID)
				names[index] = bytes.Clone(asset.Name)
				quantities[index] = asset.Quantity
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
					_ = batch.Abort()
					return fmt.Errorf("unknown input role %q", input.Role)
				}
			}
			if err := batch.Append(
				item.PublicationID,
				item.Block.Number,
				bytes32(transaction.Hash),
				transaction.Order,
				nullableHash32(transaction.ParentHash),
				transaction.SubtransactionIndex,
				transaction.Era,
				transaction.Phase2Valid,
				transaction.FlowKind,
				transaction.DeclaredFee,
				transaction.EffectiveFee,
				transaction.MintApplied,
				policies,
				names,
				quantities,
				regular,
				collateral,
				reference,
				uint32(len(transaction.Outputs)),
				uint32(len(transaction.Withdrawals)),
				uint32(len(transaction.Redeemers)),
				transaction.Metadata != nil,
				uint32(len(transaction.DatumObservations)),
			); err != nil {
				_ = batch.Abort()
				return err
			}
		}
	}
	return batch.Send()
}

func (d *DB) insertInputs(
	ctx context.Context,
	publications []publication,
) error {
	if totalInputs(publications) == 0 {
		return nil
	}
	batch, err := d.conn.PrepareBatch(ctx, insertInputsSQL)
	if err != nil {
		return err
	}
	for _, item := range publications {
		for _, transaction := range item.Block.Transactions {
			for _, input := range transaction.Inputs {
				if err := batch.Append(
					item.PublicationID,
					item.Block.Number,
					bytes32(input.TransactionHash),
					input.TransactionOrder,
					bytes32(input.SourceHash),
					input.SourceIndex,
					input.BodyOrdinal,
					input.Role,
					input.Consumed,
				); err != nil {
					_ = batch.Abort()
					return err
				}
			}
		}
	}
	return batch.Send()
}

func (d *DB) insertOutputs(
	ctx context.Context,
	publications []publication,
) error {
	if totalOutputs(publications) == 0 {
		return nil
	}
	batch, err := d.conn.PrepareBatch(ctx, insertOutputsSQL)
	if err != nil {
		return err
	}
	for _, item := range publications {
		for _, transaction := range item.Block.Transactions {
			for _, output := range transaction.Outputs {
				policies := make([][]byte, len(output.Assets))
				names := make([][]byte, len(output.Assets))
				quantities := make([]uint64, len(output.Assets))
				for index, asset := range output.Assets {
					policies[index] = bytes28(asset.PolicyID)
					names[index] = bytes.Clone(asset.Name)
					quantities[index] = asset.Quantity
				}
				if err := batch.Append(
					item.PublicationID,
					item.Block.Number,
					bytes32(output.TransactionHash),
					output.TransactionOrder,
					output.Index,
					output.BodyOrdinal,
					output.Kind,
					bytes.Clone(output.Address),
					nullableString(output.PaymentCredentialKind),
					nullableHash28(output.PaymentCredentialHash),
					output.Lovelace,
					policies,
					names,
					quantities,
					output.DatumKind,
					nullableHash32(output.DatumHash),
					nullableHash28(output.ReferenceScriptHash),
					nullableString(output.ReferenceScriptLanguage),
				); err != nil {
					_ = batch.Abort()
					return err
				}
			}
		}
	}
	return batch.Send()
}

func (d *DB) insertDatumBodies(
	ctx context.Context,
	publications []publication,
) error {
	total := 0
	for _, item := range publications {
		total += len(item.Block.Datums)
	}
	if total == 0 {
		return nil
	}
	batch, err := d.conn.PrepareBatch(ctx, insertDatumBodiesSQL)
	if err != nil {
		return err
	}
	for _, item := range publications {
		for _, datum := range item.Block.Datums {
			if err := batch.Append(
				item.PublicationID,
				item.Block.Number,
				bytes32(datum.Hash),
				bytes.Clone(datum.CBOR),
				uint32(len(datum.CBOR)),
				item.Block.ObservedAt.UTC(),
			); err != nil {
				_ = batch.Abort()
				return err
			}
		}
	}
	return batch.Send()
}

func (d *DB) insertDatumObservations(
	ctx context.Context,
	publications []publication,
) error {
	if totalDatumObservations(publications) == 0 {
		return nil
	}
	batch, err := d.conn.PrepareBatch(ctx, insertDatumObservationsSQL)
	if err != nil {
		return err
	}
	for _, item := range publications {
		for _, transaction := range item.Block.Transactions {
			for _, observation := range transaction.DatumObservations {
				if err := batch.Append(
					item.PublicationID,
					item.Block.Number,
					bytes32(observation.Hash),
					bytes32(observation.TransactionHash),
					observation.TransactionOrder,
					observation.SourceKind,
					observation.SourceOrdinal,
					observation.OutputIndex,
				); err != nil {
					_ = batch.Abort()
					return err
				}
			}
		}
	}
	return batch.Send()
}

func (d *DB) insertWithdrawals(
	ctx context.Context,
	publications []publication,
) error {
	if totalWithdrawals(publications) == 0 {
		return nil
	}
	batch, err := d.conn.PrepareBatch(ctx, insertWithdrawalsSQL)
	if err != nil {
		return err
	}
	for _, item := range publications {
		for _, transaction := range item.Block.Transactions {
			for _, withdrawal := range transaction.Withdrawals {
				if err := batch.Append(
					item.PublicationID,
					item.Block.Number,
					bytes32(withdrawal.TransactionHash),
					withdrawal.TransactionOrder,
					withdrawal.BodyOrdinal,
					bytes.Clone(withdrawal.RewardAccount),
					withdrawal.Lovelace,
					withdrawal.Applied,
					nullableString(withdrawal.CredentialKind),
					nullableHash28(withdrawal.CredentialHash),
				); err != nil {
					_ = batch.Abort()
					return err
				}
			}
		}
	}
	return batch.Send()
}

func (d *DB) insertRedeemers(
	ctx context.Context,
	publications []publication,
) error {
	if totalRedeemers(publications) == 0 {
		return nil
	}
	batch, err := d.conn.PrepareBatch(ctx, insertRedeemersSQL)
	if err != nil {
		return err
	}
	for _, item := range publications {
		for _, transaction := range item.Block.Transactions {
			for _, redeemer := range transaction.Redeemers {
				if err := batch.Append(
					item.PublicationID,
					item.Block.Number,
					bytes32(redeemer.TransactionHash),
					redeemer.TransactionOrder,
					redeemer.RawPurposeTag,
					redeemer.Purpose,
					redeemer.Index,
					bytes.Clone(redeemer.DataCBOR),
					uint32(len(redeemer.DataCBOR)),
					bytes32(redeemer.DataHash),
					redeemer.ExUnitsMemory,
					redeemer.ExUnitsSteps,
					redeemer.Applied,
					nullableHash32(redeemer.TargetTxHash),
					redeemer.TargetOutputIndex,
					nullableHash28(redeemer.TargetPolicyID),
					nullableBytes(redeemer.TargetRewardAccount),
					redeemer.TargetBodyOrdinal,
					nullableBytes(redeemer.TargetIdentity),
					nullableHash28(redeemer.ResolvedScriptHash),
				); err != nil {
					_ = batch.Abort()
					return err
				}
			}
		}
	}
	return batch.Send()
}

func (d *DB) insertMetadata(
	ctx context.Context,
	publications []publication,
) error {
	if totalMetadata(publications) == 0 {
		return nil
	}
	batch, err := d.conn.PrepareBatch(ctx, insertMetadataSQL)
	if err != nil {
		return err
	}
	for _, item := range publications {
		for _, transaction := range item.Block.Transactions {
			metadata := transaction.Metadata
			if metadata == nil {
				continue
			}
			if err := batch.Append(
				item.PublicationID,
				item.Block.Number,
				bytes32(metadata.TransactionHash),
				metadata.TransactionOrder,
				append([]uint64(nil), metadata.Labels...),
				bytes.Clone(metadata.CBOR),
				uint32(len(metadata.CBOR)),
				bytes32(metadata.ContentHash),
			); err != nil {
				_ = batch.Abort()
				return err
			}
		}
	}
	return batch.Send()
}

func nullableBytes(value []byte) any {
	if value == nil {
		return nil
	}
	return bytes.Clone(value)
}

func hasTransactions(publications []publication) bool {
	for _, item := range publications {
		if len(item.Block.Transactions) != 0 {
			return true
		}
	}
	return false
}

func totalInputs(publications []publication) uint64 {
	var total uint64
	for _, item := range publications {
		total += item.Counts.inputs
	}
	return total
}

func totalOutputs(publications []publication) uint64 {
	var total uint64
	for _, item := range publications {
		total += item.Counts.outputs
	}
	return total
}

func totalDatumObservations(publications []publication) uint64 {
	var total uint64
	for _, item := range publications {
		total += item.Counts.datumObservations
	}
	return total
}

func totalWithdrawals(publications []publication) uint64 {
	var total uint64
	for _, item := range publications {
		total += item.Counts.withdrawals
	}
	return total
}

func totalRedeemers(publications []publication) uint64 {
	var total uint64
	for _, item := range publications {
		total += item.Counts.redeemers
	}
	return total
}

func totalMetadata(publications []publication) uint64 {
	var total uint64
	for _, item := range publications {
		total += item.Counts.metadata
	}
	return total
}
