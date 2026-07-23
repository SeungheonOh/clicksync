package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/google/uuid"

	"clicksync/internal/model"
	"clicksync/internal/publication"
)

func (d *DB) CommittedSnapshot(ctx context.Context) (uint64, error) {
	const query = `
SELECT greatest(
    (SELECT max(event_seq) FROM clicksync.chain_events WHERE event_kind = 'adoption'),
    (SELECT max(event_seq) FROM clicksync.rollbacks)
)`
	var eventSeq uint64
	if err := d.conn.QueryRow(ctx, query).Scan(&eventSeq); err != nil {
		return 0, fmt.Errorf("read committed event: %w", err)
	}
	return eventSeq, nil
}

func (d *DB) ResolveActiveOutputs(
	ctx context.Context,
	snapshot uint64,
	refs []publication.OutputRef,
) (map[publication.OutputRef]struct{}, error) {
	ret := make(map[publication.OutputRef]struct{})
	if len(refs) == 0 {
		return ret, nil
	}
	hashes := make([]string, 0, len(refs))
	indexes := make([]uint32, 0, len(refs))
	for _, ref := range refs {
		hashes = append(hashes, string(ref.Hash[:]))
		indexes = append(indexes, ref.Index)
	}
	query := `
SELECT o.tx_hash, o.output_index, groupUniqArray(o.publication_id)
FROM clicksync.outputs AS o
WHERE (o.tx_hash, o.output_index) IN
(
    SELECT tupleElement(ref, 1), tupleElement(ref, 2)
    FROM
    (
        SELECT arrayJoin(arrayZip(?, ?)) AS ref
    )
)
GROUP BY o.tx_hash, o.output_index`
	rows, err := d.conn.Query(ctx, query, hashes, indexes)
	if err != nil {
		return nil, fmt.Errorf("query active source outputs: %w", err)
	}
	defer rows.Close()
	publicationRefs := make(map[uint64][]publication.OutputRef)
	var candidateIDs []uint64
	for rows.Next() {
		var hash []byte
		var index uint32
		var publicationIDs []uint64
		if err := rows.Scan(&hash, &index, &publicationIDs); err != nil {
			return nil, fmt.Errorf("scan active source output: %w", err)
		}
		converted, err := hash32(hash)
		if err != nil {
			return nil, err
		}
		ref := publication.OutputRef{Hash: converted, Index: index}
		for _, publicationID := range publicationIDs {
			if _, first := publicationRefs[publicationID]; !first {
				candidateIDs = append(candidateIDs, publicationID)
			}
			publicationRefs[publicationID] = append(publicationRefs[publicationID], ref)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate active source outputs: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close active source outputs: %w", err)
	}
	if len(candidateIDs) == 0 {
		return ret, nil
	}
	activeIDs, err := d.activeCandidatePublications(ctx, snapshot, candidateIDs)
	if err != nil {
		return nil, err
	}
	for _, publicationID := range activeIDs {
		for _, ref := range publicationRefs[publicationID] {
			ret[ref] = struct{}{}
		}
	}
	spent, err := d.activeConsumedOutputRefs(ctx, snapshot, refs)
	if err != nil {
		return nil, err
	}
	for ref := range spent {
		delete(ret, ref)
	}
	return ret, nil
}

func (d *DB) activeConsumedOutputRefs(
	ctx context.Context,
	snapshot uint64,
	refs []publication.OutputRef,
) (map[publication.OutputRef]struct{}, error) {
	ret := make(map[publication.OutputRef]struct{})
	if len(refs) == 0 {
		return ret, nil
	}
	hashes := make([]string, 0, len(refs))
	indexes := make([]uint32, 0, len(refs))
	for _, ref := range refs {
		hashes = append(hashes, string(ref.Hash[:]))
		indexes = append(indexes, ref.Index)
	}
	const query = `
SELECT source_tx_hash, source_output_index, groupUniqArray(publication_id)
FROM clicksync.inputs
WHERE is_consumed
  AND (source_tx_hash, source_output_index) IN
(
    SELECT tupleElement(ref, 1), tupleElement(ref, 2)
    FROM
    (
        SELECT arrayJoin(arrayZip(?, ?)) AS ref
    )
)
GROUP BY source_tx_hash, source_output_index`
	rows, err := d.conn.Query(ctx, query, hashes, indexes)
	if err != nil {
		return nil, fmt.Errorf("query candidate consumed outputs: %w", err)
	}
	defer rows.Close()
	byPublication := make(map[uint64][]publication.OutputRef)
	var candidateIDs []uint64
	for rows.Next() {
		var hashBytes []byte
		var index uint32
		var publicationIDs []uint64
		if err := rows.Scan(&hashBytes, &index, &publicationIDs); err != nil {
			return nil, fmt.Errorf("scan candidate consumed output: %w", err)
		}
		hash, err := hash32(hashBytes)
		if err != nil {
			return nil, err
		}
		ref := publication.OutputRef{Hash: hash, Index: index}
		for _, publicationID := range publicationIDs {
			if _, first := byPublication[publicationID]; !first {
				candidateIDs = append(candidateIDs, publicationID)
			}
			byPublication[publicationID] = append(byPublication[publicationID], ref)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate candidate consumed outputs: %w", err)
	}
	activeIDs, err := d.activeCandidatePublications(ctx, snapshot, candidateIDs)
	if err != nil {
		return nil, err
	}
	for _, publicationID := range activeIDs {
		for _, ref := range byPublication[publicationID] {
			ret[ref] = struct{}{}
		}
	}
	return ret, nil
}

func (d *DB) activeCandidatePublications(
	ctx context.Context,
	snapshot uint64,
	candidateIDs []uint64,
) ([]uint64, error) {
	if len(candidateIDs) == 0 {
		return nil, nil
	}
	const membershipQuery = `
SELECT publication_id
FROM
(
    SELECT publication_id, event_seq, active
    FROM clicksync.chain_events
    WHERE publication_id IN ?
      AND event_seq <= ?
      AND event_kind = 'adoption'
    UNION ALL
    SELECT ce.publication_id, ce.event_seq, ce.active
    FROM clicksync.chain_events AS ce
    INNER JOIN clicksync.rollbacks AS rb
      ON ce.rollback_id = rb.rollback_id
     AND ce.event_seq = rb.event_seq
    WHERE ce.publication_id IN ?
      AND ce.event_seq <= ?
      AND ce.event_kind = 'invalidation'
)
GROUP BY publication_id
HAVING argMax(active, event_seq) = true`
	activeRows, err := d.conn.Query(
		ctx,
		membershipQuery,
		candidateIDs,
		snapshot,
		candidateIDs,
		snapshot,
	)
	if err != nil {
		return nil, fmt.Errorf("query candidate publication membership: %w", err)
	}
	defer activeRows.Close()
	var ret []uint64
	for activeRows.Next() {
		var publicationID uint64
		if err := activeRows.Scan(&publicationID); err != nil {
			return nil, fmt.Errorf("scan candidate publication membership: %w", err)
		}
		ret = append(ret, publicationID)
	}
	if err := activeRows.Err(); err != nil {
		return nil, fmt.Errorf("iterate candidate publication membership: %w", err)
	}
	return ret, nil
}

func (d *DB) ExistingDatumBodies(
	ctx context.Context,
	hashes []model.Hash32,
) (map[model.Hash32][]byte, error) {
	ret := make(map[model.Hash32][]byte, len(hashes))
	if len(hashes) == 0 {
		return ret, nil
	}
	values := make([]string, 0, len(hashes))
	for _, hash := range hashes {
		values = append(values, string(hash[:]))
	}
	const query = `
SELECT
    datum_hash,
    argMin(datum_cbor, tuple(first_seen_at, first_publication_id)),
    uniqExact(datum_cbor)
FROM clicksync.datum_bodies
WHERE datum_hash IN ?
GROUP BY datum_hash`
	rows, err := d.conn.Query(ctx, query, values)
	if err != nil {
		return nil, fmt.Errorf("query datum bodies: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var hashBytes, cbor []byte
		var variants uint64
		if err := rows.Scan(&hashBytes, &cbor, &variants); err != nil {
			return nil, fmt.Errorf("scan datum body: %w", err)
		}
		if variants != 1 {
			return nil, fmt.Errorf("datum hash %x has %d physical body variants", hashBytes, variants)
		}
		hash, err := hash32(hashBytes)
		if err != nil {
			return nil, err
		}
		ret[hash] = append([]byte(nil), cbor...)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate datum bodies: %w", err)
	}
	return ret, nil
}

func (d *DB) InsertBlockBatch(
	ctx context.Context,
	attempts []publication.Attempt,
) error {
	if len(attempts) == 0 {
		return nil
	}
	const query = `INSERT INTO clicksync.blocks
(
    publication_id, block_hash, parent_hash, slot, block_number, era, block_type,
    transaction_count, input_count, output_count, datum_observation_count,
    withdrawal_count, redeemer_count, metadata_count, synthetic, source_peer,
    source_address, source_operator, n2n_version, network_magic,
    body_hash_verified, transaction_hashes_verified, facts_digest, writer_id,
    observed_at, inserted_at
)`
	batch, err := d.conn.PrepareBatch(ctx, query)
	if err != nil {
		return err
	}
	for _, attempt := range attempts {
		var parent any
		if attempt.Block.ParentHash != nil {
			parent = bytesOf32(*attempt.Block.ParentHash)
		}
		if err := batch.Append(
			attempt.PublicationID,
			bytesOf32(attempt.Block.Hash),
			parent,
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
		); err != nil {
			_ = batch.Abort()
			return err
		}
	}
	return batch.Send()
}

func (d *DB) InsertTransactionBatch(
	ctx context.Context,
	attempts []publication.Attempt,
) error {
	total := 0
	for _, attempt := range attempts {
		total += len(attempt.Block.Transactions)
	}
	if total == 0 {
		return nil
	}
	const query = `INSERT INTO clicksync.transactions
(
    publication_id, block_number, tx_hash, tx_order, parent_tx_hash,
    subtransaction_index, era, phase2_valid, flow_kind, declared_fee_lovelace,
    effective_fee_lovelace, mint_is_applied, mint_policy_ids, mint_asset_names,
    mint_quantities, regular_input_count, collateral_input_count,
    reference_input_count, produced_output_count, withdrawal_count,
    redeemer_count, metadata_present, datum_observation_count
)`
	batch, err := d.conn.PrepareBatch(ctx, query)
	if err != nil {
		return err
	}
	for _, attempt := range attempts {
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
					_ = batch.Abort()
					return fmt.Errorf("unknown input role %q", input.Role)
				}
			}
			if err := batch.Append(
				attempt.PublicationID,
				attempt.Block.Number,
				bytesOf32(transaction.Hash),
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

func (d *DB) InsertInputBatch(
	ctx context.Context,
	attempts []publication.Attempt,
) error {
	var total uint64
	for _, attempt := range attempts {
		total += attempt.Counts.Inputs
	}
	if total == 0 {
		return nil
	}
	const query = `INSERT INTO clicksync.inputs
(
    publication_id, block_number, tx_hash, tx_order, source_tx_hash,
    source_output_index, body_ordinal, role, is_consumed, source_is_resolved
)`
	batch, err := d.conn.PrepareBatch(ctx, query)
	if err != nil {
		return err
	}
	for _, attempt := range attempts {
		for _, transaction := range attempt.Block.Transactions {
			for _, input := range transaction.Inputs {
				if err := batch.Append(
					attempt.PublicationID,
					attempt.Block.Number,
					bytesOf32(input.TransactionHash),
					input.TransactionOrder,
					bytesOf32(input.SourceHash),
					input.SourceIndex,
					input.BodyOrdinal,
					input.Role,
					input.Consumed,
					input.SourceResolved,
				); err != nil {
					_ = batch.Abort()
					return err
				}
			}
		}
	}
	return batch.Send()
}

func (d *DB) InsertOutputBatch(
	ctx context.Context,
	attempts []publication.Attempt,
) error {
	var total uint64
	for _, attempt := range attempts {
		total += attempt.Counts.Outputs
	}
	if total == 0 {
		return nil
	}
	const query = `INSERT INTO clicksync.outputs
(
    publication_id, block_number, tx_hash, tx_order, output_index, body_ordinal,
    output_kind, address, lovelace, asset_policy_ids, asset_names,
    asset_quantities, datum_kind, datum_hash, reference_script_hash,
    reference_script_language
)`
	batch, err := d.conn.PrepareBatch(ctx, query)
	if err != nil {
		return err
	}
	for _, attempt := range attempts {
		for _, transaction := range attempt.Block.Transactions {
			for _, output := range transaction.Outputs {
				policies := make([][]byte, 0, len(output.Assets))
				names := make([][]byte, 0, len(output.Assets))
				quantities := make([]uint64, 0, len(output.Assets))
				for _, asset := range output.Assets {
					policies = append(policies, bytesOf28(asset.PolicyID))
					names = append(names, append([]byte(nil), asset.Name...))
					quantities = append(quantities, asset.Quantity)
				}
				if err := batch.Append(
					attempt.PublicationID,
					attempt.Block.Number,
					bytesOf32(output.TransactionHash),
					output.TransactionOrder,
					output.Index,
					output.BodyOrdinal,
					output.Kind,
					append([]byte(nil), output.Address...),
					output.Lovelace,
					policies,
					names,
					quantities,
					output.DatumKind,
					nullableHash32(output.DatumHash),
					nullableHash28(output.ReferenceScriptHash),
					output.ReferenceScriptLanguage,
				); err != nil {
					_ = batch.Abort()
					return err
				}
			}
		}
	}
	return batch.Send()
}

func (d *DB) InsertDatumBodyBatch(
	ctx context.Context,
	attempts []publication.Attempt,
) error {
	total := 0
	for _, attempt := range attempts {
		total += len(attempt.NewDatumBodies)
	}
	if total == 0 {
		return nil
	}
	const query = `INSERT INTO clicksync.datum_bodies
(
    datum_hash, datum_cbor, byte_length, content_hash, first_publication_id,
    first_seen_at
)`
	batch, err := d.conn.PrepareBatch(ctx, query)
	if err != nil {
		return err
	}
	for _, attempt := range attempts {
		for _, body := range attempt.NewDatumBodies {
			if len(body.CBOR) > math.MaxUint32 {
				_ = batch.Abort()
				return errors.New("datum body exceeds UInt32 length")
			}
			if err := batch.Append(
				bytesOf32(body.Hash),
				append([]byte(nil), body.CBOR...),
				uint32(len(body.CBOR)),
				bytesOf32(body.Hash),
				attempt.PublicationID,
				attempt.InsertedAt,
			); err != nil {
				_ = batch.Abort()
				return err
			}
		}
	}
	return batch.Send()
}

func (d *DB) InsertDatumObservationBatch(
	ctx context.Context,
	attempts []publication.Attempt,
) error {
	var total uint64
	for _, attempt := range attempts {
		total += attempt.Counts.DatumObservations
	}
	if total == 0 {
		return nil
	}
	const query = `INSERT INTO clicksync.datum_observations
(
    publication_id, block_number, datum_hash, tx_hash, tx_order, source_kind,
    source_ordinal, output_index
)`
	batch, err := d.conn.PrepareBatch(ctx, query)
	if err != nil {
		return err
	}
	for _, attempt := range attempts {
		for _, transaction := range attempt.Block.Transactions {
			for _, observation := range transaction.DatumObservations {
				if err := batch.Append(
					attempt.PublicationID,
					attempt.Block.Number,
					bytesOf32(observation.Hash),
					bytesOf32(observation.TransactionHash),
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

func (d *DB) InsertWithdrawalBatch(
	ctx context.Context,
	attempts []publication.Attempt,
) error {
	var total uint64
	for _, attempt := range attempts {
		total += attempt.Counts.Withdrawals
	}
	if total == 0 {
		return nil
	}
	const query = `INSERT INTO clicksync.withdrawals
(
    publication_id, block_number, tx_hash, tx_order, body_ordinal,
    reward_account, lovelace, is_applied, credential_kind, credential_hash
)`
	batch, err := d.conn.PrepareBatch(ctx, query)
	if err != nil {
		return err
	}
	for _, attempt := range attempts {
		for _, transaction := range attempt.Block.Transactions {
			for _, withdrawal := range transaction.Withdrawals {
				if err := batch.Append(
					attempt.PublicationID,
					attempt.Block.Number,
					bytesOf32(withdrawal.TransactionHash),
					withdrawal.TransactionOrder,
					withdrawal.BodyOrdinal,
					append([]byte(nil), withdrawal.RewardAccount...),
					withdrawal.Lovelace,
					withdrawal.Applied,
					withdrawal.CredentialKind,
					bytesOf28(withdrawal.CredentialHash),
				); err != nil {
					_ = batch.Abort()
					return err
				}
			}
		}
	}
	return batch.Send()
}

func (d *DB) InsertRedeemerBatch(
	ctx context.Context,
	attempts []publication.Attempt,
) error {
	var total uint64
	for _, attempt := range attempts {
		total += attempt.Counts.Redeemers
	}
	if total == 0 {
		return nil
	}
	const query = `INSERT INTO clicksync.redeemers
(
    publication_id, block_number, tx_hash, tx_order, raw_purpose_tag, purpose,
    redeemer_index, data_cbor, data_byte_length, data_hash, ex_units_memory,
    ex_units_steps, is_applied, resolution_status, target_tx_hash,
    target_output_index, target_policy_id, target_reward_account,
    target_body_ordinal, target_identity, resolved_script_hash
)`
	batch, err := d.conn.PrepareBatch(ctx, query)
	if err != nil {
		return err
	}
	for _, attempt := range attempts {
		for _, transaction := range attempt.Block.Transactions {
			for _, redeemer := range transaction.Redeemers {
				if len(redeemer.DataCBOR) > math.MaxUint32 {
					_ = batch.Abort()
					return errors.New("redeemer data exceeds UInt32 length")
				}
				var targetReward any
				if redeemer.TargetRewardAccount != nil {
					targetReward = append([]byte(nil), redeemer.TargetRewardAccount...)
				}
				var targetIdentity any
				if redeemer.TargetIdentity != nil {
					targetIdentity = append([]byte(nil), redeemer.TargetIdentity...)
				}
				if err := batch.Append(
					attempt.PublicationID,
					attempt.Block.Number,
					bytesOf32(redeemer.TransactionHash),
					redeemer.TransactionOrder,
					redeemer.RawPurposeTag,
					redeemer.Purpose,
					redeemer.Index,
					append([]byte(nil), redeemer.DataCBOR...),
					uint32(len(redeemer.DataCBOR)),
					bytesOf32(redeemer.DataHash),
					redeemer.ExUnitsMemory,
					redeemer.ExUnitsSteps,
					redeemer.Applied,
					"resolved",
					nullableHash32(redeemer.TargetTxHash),
					redeemer.TargetOutputIndex,
					nullableHash28(redeemer.TargetPolicyID),
					targetReward,
					redeemer.TargetBodyOrdinal,
					targetIdentity,
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

func (d *DB) InsertMetadataBatch(
	ctx context.Context,
	attempts []publication.Attempt,
) error {
	var total uint64
	for _, attempt := range attempts {
		total += attempt.Counts.Metadata
	}
	if total == 0 {
		return nil
	}
	const query = `INSERT INTO clicksync.transaction_metadata
(
    publication_id, block_number, tx_hash, tx_order, labels, metadata_cbor,
    byte_length, content_hash
)`
	batch, err := d.conn.PrepareBatch(ctx, query)
	if err != nil {
		return err
	}
	for _, attempt := range attempts {
		for _, transaction := range attempt.Block.Transactions {
			if transaction.Metadata == nil {
				continue
			}
			metadata := transaction.Metadata
			if len(metadata.CBOR) > math.MaxUint32 {
				_ = batch.Abort()
				return errors.New("metadata exceeds UInt32 length")
			}
			if err := batch.Append(
				attempt.PublicationID,
				attempt.Block.Number,
				bytesOf32(metadata.TransactionHash),
				metadata.TransactionOrder,
				append([]uint64(nil), metadata.Labels...),
				append([]byte(nil), metadata.CBOR...),
				uint32(len(metadata.CBOR)),
				bytesOf32(metadata.ContentHash),
			); err != nil {
				_ = batch.Abort()
				return err
			}
		}
	}
	return batch.Send()
}

func (d *DB) InsertPeerObservations(
	ctx context.Context,
	observations []model.PeerObservation,
) error {
	if len(observations) == 0 {
		return nil
	}
	ids := make([]uuid.UUID, 0, len(observations))
	digests := make(map[uuid.UUID][32]byte, len(observations))
	unique := make([]model.PeerObservation, 0, len(observations))
	for _, observation := range observations {
		id := uuid.UUID(observation.ID)
		encoded, err := json.Marshal(observation)
		if err != nil {
			return fmt.Errorf("encode peer observation digest: %w", err)
		}
		digest := sha256.Sum256(encoded)
		if previous, duplicate := digests[id]; duplicate {
			if previous != digest {
				return fmt.Errorf("conflicting peer observations share ID %s", id)
			}
			continue
		}
		digests[id] = digest
		ids = append(ids, id)
		unique = append(unique, observation)
	}
	const existingQuery = `
SELECT observation_id, any(observation_digest), uniqExact(observation_digest)
FROM clicksync.peer_observations
WHERE observation_id IN ?
GROUP BY observation_id`
	rows, err := d.conn.Query(ctx, existingQuery, ids)
	if err != nil {
		return fmt.Errorf("precheck peer observations: %w", err)
	}
	existing := make(map[uuid.UUID]struct{}, len(ids))
	for rows.Next() {
		var id uuid.UUID
		var digest []byte
		var variants uint64
		if err := rows.Scan(&id, &digest, &variants); err != nil {
			rows.Close()
			return fmt.Errorf("scan peer observation precheck: %w", err)
		}
		expected, ok := digests[id]
		if !ok || variants != 1 || !bytes.Equal(digest, expected[:]) {
			rows.Close()
			return fmt.Errorf("stored peer observation %s conflicts with replay", id)
		}
		existing[id] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("iterate peer observation precheck: %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close peer observation precheck: %w", err)
	}
	missing := make([]model.PeerObservation, 0, len(unique))
	for _, observation := range unique {
		if _, ok := existing[uuid.UUID(observation.ID)]; !ok {
			missing = append(missing, observation)
		}
	}
	if len(missing) == 0 {
		return nil
	}
	const query = `INSERT INTO clicksync.peer_observations
(
    observation_id, observation_digest, observation_kind, peer_host,
    peer_address, operator_label, n2n_version, network_magic,
    observed_tip_slot, observed_tip_hash,
    observed_tip_block_number, checkpoint_slot, checkpoint_hash,
    checkpoint_block_number, checkpoint_is_byron_ebb, agreement_group, selected_body_source,
    body_hash_verified, point_verified, parent_verified, result, reason,
    observed_at
)`
	batch, err := d.conn.PrepareBatch(ctx, query)
	if err != nil {
		return err
	}
	for _, observation := range missing {
		observationDigest := digests[uuid.UUID(observation.ID)]
		var agreement any
		if observation.AgreementGroup != nil {
			agreement = uuid.UUID(*observation.AgreementGroup)
		}
		if err := batch.Append(
			uuid.UUID(observation.ID),
			observationDigest[:],
			observation.Kind,
			observation.PeerHost,
			observation.PeerAddress,
			observation.Operator,
			observation.N2NVersion,
			observation.NetworkMagic,
			observation.TipSlot,
			bytesOf32(observation.TipHash),
			observation.TipBlockNumber,
			observation.CheckpointSlot,
			nullableHash32(observation.CheckpointHash),
			observation.CheckpointBlockNumber,
			observation.CheckpointIsByronEBB,
			agreement,
			observation.SelectedBodySource,
			observation.BodyHashVerified,
			observation.PointVerified,
			observation.ParentVerified,
			observation.Result,
			observation.Reason,
			observation.ObservedAt.UTC(),
		); err != nil {
			_ = batch.Abort()
			return err
		}
	}
	return batch.Send()
}

func (d *DB) VerifyFactBatch(
	ctx context.Context,
	attempts []publication.Attempt,
) error {
	if len(attempts) == 0 {
		return errors.New("cannot verify an empty fact batch")
	}
	ids := make([]uint64, 0, len(attempts))
	expected := map[string]map[uint64]uint64{
		"transactions":         {},
		"inputs":               {},
		"outputs":              {},
		"datum_observations":   {},
		"withdrawals":          {},
		"redeemers":            {},
		"transaction_metadata": {},
	}
	byID := make(map[uint64]publication.Attempt, len(attempts))
	for _, attempt := range attempts {
		if _, duplicate := byID[attempt.PublicationID]; duplicate {
			return fmt.Errorf("duplicate publication ID %d in fact batch", attempt.PublicationID)
		}
		ids = append(ids, attempt.PublicationID)
		byID[attempt.PublicationID] = attempt
		expected["transactions"][attempt.PublicationID] = attempt.Counts.Transactions
		expected["inputs"][attempt.PublicationID] = attempt.Counts.Inputs
		expected["outputs"][attempt.PublicationID] = attempt.Counts.Outputs
		expected["datum_observations"][attempt.PublicationID] = attempt.Counts.DatumObservations
		expected["withdrawals"][attempt.PublicationID] = attempt.Counts.Withdrawals
		expected["redeemers"][attempt.PublicationID] = attempt.Counts.Redeemers
		expected["transaction_metadata"][attempt.PublicationID] = attempt.Counts.Metadata
	}
	for table, counts := range expected {
		if err := d.verifyBatchCounts(ctx, table, ids, counts); err != nil {
			return err
		}
	}
	const datumQuery = `
SELECT
    first_publication_id,
    uniqExact(datum_hash),
    countIf(byte_length != length(datum_cbor) OR datum_hash != content_hash)
FROM clicksync.datum_bodies
WHERE first_publication_id IN ?
GROUP BY first_publication_id`
	datumRows, err := d.conn.Query(ctx, datumQuery, ids)
	if err != nil {
		return fmt.Errorf("verify batch datum bodies: %w", err)
	}
	datumSeen := make(map[uint64]struct{}, len(attempts))
	for datumRows.Next() {
		var publicationID, distinct, conflicts uint64
		if err := datumRows.Scan(&publicationID, &distinct, &conflicts); err != nil {
			datumRows.Close()
			return fmt.Errorf("scan batch datum bodies: %w", err)
		}
		attempt, ok := byID[publicationID]
		if !ok || distinct != attempt.Counts.DatumBodies || conflicts != 0 {
			datumRows.Close()
			return fmt.Errorf("publication %d datum body content/count mismatch", publicationID)
		}
		datumSeen[publicationID] = struct{}{}
	}
	if err := datumRows.Err(); err != nil {
		datumRows.Close()
		return fmt.Errorf("iterate batch datum bodies: %w", err)
	}
	if err := datumRows.Close(); err != nil {
		return fmt.Errorf("close batch datum bodies: %w", err)
	}
	for _, attempt := range attempts {
		_, seen := datumSeen[attempt.PublicationID]
		if (attempt.Counts.DatumBodies != 0) != seen {
			return fmt.Errorf("publication %d datum body count mismatch", attempt.PublicationID)
		}
	}
	const blockQuery = `
SELECT
    publication_id,
    count(),
    uniqExact(facts_digest),
    any(facts_digest)
FROM clicksync.blocks
WHERE publication_id IN ?
GROUP BY publication_id`
	blockRows, err := d.conn.Query(ctx, blockQuery, ids)
	if err != nil {
		return fmt.Errorf("verify batch blocks: %w", err)
	}
	blockSeen := make(map[uint64]struct{}, len(attempts))
	for blockRows.Next() {
		var publicationID, count, variants uint64
		var digest []byte
		if err := blockRows.Scan(&publicationID, &count, &variants, &digest); err != nil {
			blockRows.Close()
			return fmt.Errorf("scan batch blocks: %w", err)
		}
		attempt, ok := byID[publicationID]
		if !ok || count != 1 || variants != 1 ||
			!bytes.Equal(digest, attempt.FactsDigest[:]) {
			blockRows.Close()
			return fmt.Errorf("publication %d block digest/count mismatch", publicationID)
		}
		blockSeen[publicationID] = struct{}{}
	}
	if err := blockRows.Err(); err != nil {
		blockRows.Close()
		return fmt.Errorf("iterate batch blocks: %w", err)
	}
	if err := blockRows.Close(); err != nil {
		return fmt.Errorf("close batch blocks: %w", err)
	}
	if len(blockSeen) != len(attempts) {
		return errors.New("physical microbatch block set is incomplete")
	}
	return d.verifyPersistedBatchContent(ctx, attempts)
}

func (d *DB) verifyBatchCounts(
	ctx context.Context,
	table string,
	publicationIDs []uint64,
	expected map[uint64]uint64,
) error {
	query := "SELECT publication_id, count() FROM clicksync." + table +
		" WHERE publication_id IN ? GROUP BY publication_id"
	rows, err := d.conn.Query(ctx, query, publicationIDs)
	if err != nil {
		return fmt.Errorf("count batch %s: %w", table, err)
	}
	seen := make(map[uint64]struct{}, len(expected))
	for rows.Next() {
		var publicationID, count uint64
		if err := rows.Scan(&publicationID, &count); err != nil {
			rows.Close()
			return fmt.Errorf("scan batch %s count: %w", table, err)
		}
		want, ok := expected[publicationID]
		if !ok || want != count {
			rows.Close()
			return fmt.Errorf("%s publication %d rows = %d, want %d", table, publicationID, count, want)
		}
		seen[publicationID] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("iterate batch %s counts: %w", table, err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close batch %s counts: %w", table, err)
	}
	for publicationID, count := range expected {
		_, ok := seen[publicationID]
		if (count != 0) != ok {
			return fmt.Errorf("%s publication %d count mismatch", table, publicationID)
		}
	}
	return nil
}

func (d *DB) InsertAdoptionBatch(
	ctx context.Context,
	attempts []publication.Attempt,
	firstEventSeq uint64,
) error {
	if len(attempts) == 0 {
		return nil
	}
	const query = `INSERT INTO clicksync.chain_events
(
    event_seq, publication_id, event_kind, active, rollback_id, block_hash,
    slot, block_number, is_byron_ebb, writer_id, recorded_at
)`
	batch, err := d.conn.PrepareBatch(ctx, query)
	if err != nil {
		return err
	}
	for index, attempt := range attempts {
		if err := batch.Append(
			firstEventSeq+uint64(index),
			attempt.PublicationID,
			"adoption",
			true,
			nil,
			bytesOf32(attempt.Block.Hash),
			attempt.Block.Slot,
			attempt.Block.Number,
			isByronEpochBoundaryBlock(attempt.Block),
			uuid.UUID(attempt.WriterID),
			attempt.InsertedAt,
		); err != nil {
			_ = batch.Abort()
			return err
		}
	}
	return batch.Send()
}

func (d *DB) AdoptionBatchCommitted(
	ctx context.Context,
	attempts []publication.Attempt,
	firstEventSeq uint64,
) (bool, error) {
	if len(attempts) == 0 {
		return false, errors.New("cannot read back an empty adoption batch")
	}
	lastEventSeq := firstEventSeq + uint64(len(attempts)) - 1
	const query = `
SELECT
    event_seq, publication_id, event_kind, active, rollback_id, block_hash,
    slot, block_number, is_byron_ebb, writer_id, recorded_at
FROM clicksync.chain_events
WHERE event_seq BETWEEN ? AND ?`
	rows, err := d.conn.Query(ctx, query, firstEventSeq, lastEventSeq)
	if err != nil {
		return false, fmt.Errorf("query adoption batch commit: %w", err)
	}
	defer rows.Close()
	seen := make(map[uint64]struct{}, len(attempts))
	rowCount := 0
	for rows.Next() {
		rowCount++
		var (
			eventSeq      uint64
			publicationID uint64
			eventKind     string
			active        bool
			rollbackID    *uuid.UUID
			hashBytes     []byte
			slot          uint64
			blockNumber   uint64
			isByronEBB    bool
			writerID      uuid.UUID
			recordedAt    time.Time
		)
		if err := rows.Scan(
			&eventSeq,
			&publicationID,
			&eventKind,
			&active,
			&rollbackID,
			&hashBytes,
			&slot,
			&blockNumber,
			&isByronEBB,
			&writerID,
			&recordedAt,
		); err != nil {
			return false, fmt.Errorf("scan adoption batch commit: %w", err)
		}
		if eventSeq < firstEventSeq || eventSeq > lastEventSeq {
			return false, errors.New("adoption batch readback returned an out-of-range event")
		}
		index := eventSeq - firstEventSeq
		attempt := attempts[index]
		if _, duplicate := seen[eventSeq]; duplicate ||
			publicationID != attempt.PublicationID ||
			eventKind != "adoption" ||
			!active ||
			rollbackID != nil ||
			!bytes.Equal(hashBytes, attempt.Block.Hash[:]) ||
			slot != attempt.Block.Slot ||
			blockNumber != attempt.Block.Number ||
			isByronEBB != isByronEpochBoundaryBlock(attempt.Block) ||
			writerID != uuid.UUID(attempt.WriterID) ||
			!recordedAt.Equal(attempt.InsertedAt) {
			return false, errors.New("adoption batch has duplicate or conflicting committed rows")
		}
		seen[eventSeq] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("iterate adoption batch commit: %w", err)
	}
	if rowCount == 0 {
		return false, nil
	}
	if rowCount != len(attempts) || len(seen) != len(attempts) {
		return false, errors.New("adoption batch is partially committed")
	}
	return true, nil
}

func (d *DB) ActiveDescendants(
	ctx context.Context,
	snapshot uint64,
	to publication.Point,
	maximumDepth uint32,
) ([]publication.Descendant, error) {
	if maximumDepth == 0 {
		return nil, errors.New("maximum rollback depth must be non-zero")
	}
	boundary, err := d.manifestStartPoint(ctx)
	if err != nil {
		return nil, err
	}
	if to.Origin {
		if !boundary.Origin {
			return nil, errors.New("rollback below the configured external intersection is forbidden")
		}
	} else if !samePoint(to, boundary) {
		// A non-boundary target must be encountered as an active ancestor in
		// the bounded parent walk below.
	}
	tip, err := d.committedTip(ctx, snapshot)
	if err != nil {
		return nil, err
	}
	if samePoint(tip, to) {
		return nil, nil
	}
	if tip.Origin {
		return nil, errors.New("committed tip is Origin and has no rollback descendants")
	}
	current, err := d.activeBlockByPoint(ctx, snapshot, tip)
	if err != nil {
		return nil, fmt.Errorf("resolve committed rollback tip: %w", err)
	}
	descendants := make([]publication.Descendant, 0, maximumDepth)
	for step := uint32(0); step <= maximumDepth; step++ {
		if samePoint(current.Point, to) {
			return descendants, nil
		}
		if step == maximumDepth {
			return nil, fmt.Errorf("rollback exceeds configured maximum depth %d", maximumDepth)
		}
		if current.Synthetic {
			if to.Origin {
				return descendants, nil
			}
			return nil, errors.New("rollback target is below permanent synthetic genesis facts")
		}
		descendants = append(descendants, publication.Descendant{
			PublicationID: current.PublicationID,
			Point:         current.Point,
		})
		if current.ParentHash == nil {
			if to.Origin {
				return descendants, nil
			}
			return nil, errors.New("rollback target is not on the active parent chain")
		}
		if !to.Origin &&
			current.Point.BlockNumber == to.BlockNumber+1 &&
			*current.ParentHash == to.Hash &&
			samePoint(to, boundary) {
			return descendants, nil
		}
		parent, found, err := d.activeBlockByHash(ctx, snapshot, *current.ParentHash)
		if err != nil {
			return nil, err
		}
		if !found {
			if to.Origin && boundary.Origin {
				return descendants, nil
			}
			return nil, errors.New("active parent chain ends before the requested rollback point")
		}
		current = parent
	}
	panic("unreachable rollback walk")
}

type activeBlock struct {
	PublicationID uint64
	Point         publication.Point
	ParentHash    *model.Hash32
	Synthetic     bool
}

func (d *DB) activeBlockByPoint(
	ctx context.Context,
	snapshot uint64,
	point publication.Point,
) (activeBlock, error) {
	block, found, err := d.activeBlockByHash(ctx, snapshot, point.Hash)
	if err != nil {
		return activeBlock{}, err
	}
	if !found || !samePoint(block.Point, point) {
		return activeBlock{}, errors.New("point is not an active committed block")
	}
	return block, nil
}

func (d *DB) activeBlockByHash(
	ctx context.Context,
	snapshot uint64,
	hash model.Hash32,
) (activeBlock, bool, error) {
	const candidatesQuery = `
SELECT publication_id, slot, block_number, parent_hash, synthetic, era, block_type
FROM clicksync.blocks
WHERE block_hash = ?`
	rows, err := d.conn.Query(ctx, candidatesQuery, string(hash[:]))
	if err != nil {
		return activeBlock{}, false, fmt.Errorf("query block-hash candidates: %w", err)
	}
	candidates := make(map[uint64]activeBlock)
	var ids []uint64
	for rows.Next() {
		var (
			publicationID, slot, blockNumber uint64
			parentBytes                      []byte
			synthetic                        bool
			era                              string
			blockType                        int16
		)
		if err := rows.Scan(
			&publicationID,
			&slot,
			&blockNumber,
			&parentBytes,
			&synthetic,
			&era,
			&blockType,
		); err != nil {
			rows.Close()
			return activeBlock{}, false, fmt.Errorf("scan block-hash candidate: %w", err)
		}
		var parent *model.Hash32
		if len(parentBytes) != 0 {
			converted, err := hash32(parentBytes)
			if err != nil {
				rows.Close()
				return activeBlock{}, false, err
			}
			parent = &converted
		}
		candidates[publicationID] = activeBlock{
			PublicationID: publicationID,
			Point: publication.Point{
				Slot:        slot,
				Hash:        hash,
				BlockNumber: blockNumber,
				IsByronEBB:  era == "Byron" && blockType == int16(ledger.BlockTypeByronEbb),
			},
			ParentHash: parent,
			Synthetic:  synthetic,
		}
		ids = append(ids, publicationID)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return activeBlock{}, false, fmt.Errorf("iterate block-hash candidates: %w", err)
	}
	rows.Close()
	if len(ids) == 0 {
		return activeBlock{}, false, nil
	}
	activeIDs, err := d.activeCandidatePublications(ctx, snapshot, ids)
	if err != nil {
		return activeBlock{}, false, err
	}
	if len(activeIDs) != 1 {
		if len(activeIDs) == 0 {
			return activeBlock{}, false, nil
		}
		return activeBlock{}, false, fmt.Errorf("block hash has %d active publications", len(activeIDs))
	}
	block, ok := candidates[activeIDs[0]]
	return block, ok, nil
}

func samePoint(left, right publication.Point) bool {
	if left.Origin || right.Origin {
		return left.Origin == right.Origin
	}
	return left.Slot == right.Slot &&
		left.Hash == right.Hash &&
		left.BlockNumber == right.BlockNumber &&
		left.IsByronEBB == right.IsByronEBB
}

func (d *DB) InsertInvalidations(
	ctx context.Context,
	commit publication.RollbackCommit,
	descendants []publication.Descendant,
) error {
	if len(descendants) == 0 {
		return errors.New("cannot insert empty rollback membership")
	}
	const query = `INSERT INTO clicksync.chain_events
(
    event_seq, publication_id, event_kind, active, rollback_id, block_hash,
    slot, block_number, is_byron_ebb, writer_id, recorded_at
)`
	batch, err := d.conn.PrepareBatch(ctx, query)
	if err != nil {
		return err
	}
	for _, descendant := range descendants {
		if err := batch.Append(
			commit.EventSeq,
			descendant.PublicationID,
			"invalidation",
			false,
			uuid.UUID(commit.RollbackID),
			bytesOf32(descendant.Point.Hash),
			descendant.Point.Slot,
			descendant.Point.BlockNumber,
			descendant.Point.IsByronEBB,
			uuid.UUID(commit.WriterID),
			commit.RecordedAt,
		); err != nil {
			_ = batch.Abort()
			return err
		}
	}
	return batch.Send()
}

func (d *DB) InsertRollbackHeader(
	ctx context.Context,
	commit publication.RollbackCommit,
) error {
	const query = `INSERT INTO clicksync.rollbacks
(
    rollback_id, event_seq, rollback_to_origin, rollback_to_slot,
    rollback_to_hash, rollback_to_block_number, rollback_to_is_byron_ebb,
    old_tip_slot, old_tip_hash, old_tip_block_number, old_tip_is_byron_ebb, depth,
    reason, observed_peers, observed_operators, corroboration_required,
    agreement_group, writer_id, recorded_at
)`
	var toSlot, toHash, toNumber any
	if !commit.To.Origin {
		toSlot = commit.To.Slot
		toHash = bytesOf32(commit.To.Hash)
		toNumber = commit.To.BlockNumber
	}
	var oldSlot, oldHash, oldNumber any
	if !commit.OldTip.Origin {
		oldSlot = commit.OldTip.Slot
		oldHash = bytesOf32(commit.OldTip.Hash)
		oldNumber = commit.OldTip.BlockNumber
	}
	var agreement any
	if commit.AgreementGroup != nil {
		agreement = uuid.UUID(*commit.AgreementGroup)
	}
	batch, err := d.conn.PrepareBatch(ctx, query)
	if err != nil {
		return err
	}
	if err := batch.Append(
		uuid.UUID(commit.RollbackID),
		commit.EventSeq,
		commit.To.Origin,
		toSlot,
		toHash,
		toNumber,
		commit.To.IsByronEBB,
		oldSlot,
		oldHash,
		oldNumber,
		commit.OldTip.IsByronEBB,
		commit.Depth,
		commit.Reason,
		commit.ObservedPeers,
		commit.ObservedOperators,
		commit.CorroborationRequired,
		agreement,
		uuid.UUID(commit.WriterID),
		commit.RecordedAt,
	); err != nil {
		_ = batch.Abort()
		return err
	}
	return batch.Send()
}

func (d *DB) RollbackCommitted(
	ctx context.Context,
	commit publication.RollbackCommit,
) (bool, error) {
	const query = `
SELECT
    rollback_to_origin, rollback_to_slot, rollback_to_hash, rollback_to_block_number,
    rollback_to_is_byron_ebb, old_tip_slot, old_tip_hash, old_tip_block_number,
    old_tip_is_byron_ebb, depth, reason,
    observed_peers, observed_operators, corroboration_required,
    agreement_group, writer_id, recorded_at
FROM clicksync.rollbacks
WHERE rollback_id = ?
  AND event_seq = ?`
	rows, err := d.conn.Query(ctx, query, uuid.UUID(commit.RollbackID), commit.EventSeq)
	if err != nil {
		return false, fmt.Errorf("query rollback commit: %w", err)
	}
	defer rows.Close()
	found := false
	for rows.Next() {
		found = true
		var (
			toOrigin              bool
			toSlot                *uint64
			toHashBytes           []byte
			toNumber              *uint64
			toIsByronEBB          bool
			oldSlot               *uint64
			oldHashBytes          []byte
			oldNumber             *uint64
			oldIsByronEBB         bool
			depth                 uint32
			reason                string
			peers                 []string
			operators             []string
			corroborationRequired uint16
			agreement             *uuid.UUID
			writer                uuid.UUID
			recordedAt            time.Time
		)
		if err := rows.Scan(
			&toOrigin,
			&toSlot,
			&toHashBytes,
			&toNumber,
			&toIsByronEBB,
			&oldSlot,
			&oldHashBytes,
			&oldNumber,
			&oldIsByronEBB,
			&depth,
			&reason,
			&peers,
			&operators,
			&corroborationRequired,
			&agreement,
			&writer,
			&recordedAt,
		); err != nil {
			return false, fmt.Errorf("scan rollback commit: %w", err)
		}
		if !rollbackRowMatches(
			commit,
			toOrigin,
			toSlot,
			toHashBytes,
			toNumber,
			toIsByronEBB,
			oldSlot,
			oldHashBytes,
			oldNumber,
			oldIsByronEBB,
			depth,
			reason,
			peers,
			operators,
			corroborationRequired,
			agreement,
			writer,
			recordedAt,
		) {
			return false, errors.New("rollback identity has conflicting committed rows")
		}
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("iterate rollback commit: %w", err)
	}
	return found, nil
}

func rollbackRowMatches(
	commit publication.RollbackCommit,
	toOrigin bool,
	toSlot *uint64,
	toHashBytes []byte,
	toNumber *uint64,
	toIsByronEBB bool,
	oldSlot *uint64,
	oldHashBytes []byte,
	oldNumber *uint64,
	oldIsByronEBB bool,
	depth uint32,
	reason string,
	peers []string,
	operators []string,
	corroborationRequired uint16,
	agreement *uuid.UUID,
	writer uuid.UUID,
	recordedAt time.Time,
) bool {
	if toOrigin != commit.To.Origin ||
		depth != commit.Depth ||
		reason != commit.Reason ||
		!equalStrings(peers, commit.ObservedPeers) ||
		!equalStrings(operators, commit.ObservedOperators) ||
		corroborationRequired != commit.CorroborationRequired ||
		writer != uuid.UUID(commit.WriterID) ||
		!recordedAt.Equal(commit.RecordedAt) {
		return false
	}
	if commit.To.Origin {
		if toSlot != nil || len(toHashBytes) != 0 || toNumber != nil || toIsByronEBB {
			return false
		}
	} else if toSlot == nil || *toSlot != commit.To.Slot ||
		toNumber == nil || *toNumber != commit.To.BlockNumber ||
		!bytes.Equal(toHashBytes, commit.To.Hash[:]) ||
		toIsByronEBB != commit.To.IsByronEBB {
		return false
	}
	if commit.OldTip.Origin {
		if oldSlot != nil || len(oldHashBytes) != 0 || oldNumber != nil || oldIsByronEBB {
			return false
		}
	} else if oldSlot == nil || *oldSlot != commit.OldTip.Slot ||
		oldNumber == nil || *oldNumber != commit.OldTip.BlockNumber ||
		!bytes.Equal(oldHashBytes, commit.OldTip.Hash[:]) ||
		oldIsByronEBB != commit.OldTip.IsByronEBB {
		return false
	}
	if commit.AgreementGroup == nil {
		return agreement == nil
	}
	return agreement != nil && *agreement == uuid.UUID(*commit.AgreementGroup)
}

func equalStrings(left, right []string) bool {
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

func isByronEpochBoundaryBlock(block model.Block) bool {
	return block.Era == "Byron" &&
		block.Type == int16(ledger.BlockTypeByronEbb)
}

func bytesOf32(hash model.Hash32) []byte {
	return append([]byte(nil), hash[:]...)
}

func bytesOf28(hash model.Hash28) []byte {
	return append([]byte(nil), hash[:]...)
}

func nullableHash32(hash *model.Hash32) any {
	if hash == nil {
		return nil
	}
	return bytesOf32(*hash)
}

func nullableHash28(hash *model.Hash28) any {
	if hash == nil {
		return nil
	}
	return bytesOf28(*hash)
}

func hash32(value []byte) (model.Hash32, error) {
	var hash model.Hash32
	if len(value) != len(hash) {
		return hash, fmt.Errorf("ClickHouse returned hash with length %d, want %d", len(value), len(hash))
	}
	copy(hash[:], value)
	return hash, nil
}

var _ publication.Backend = (*DB)(nil)
var _ publication.BatchBackend = (*DB)(nil)
