package clickhouse

import (
	"context"
	"errors"
	"fmt"

	"github.com/clicksync-project/clickout/internal/model"
)

func (store *Store) Redeemers(
	ctx context.Context,
	snapshot model.Snapshot,
	hash model.Hash32,
) ([]model.Redeemer, []model.PartialHistoryBoundary, error) {
	queryCtx, finish := store.instrument(ctx, "redeemers")
	defer finish()
	rows, err := store.conn.Query(
		queryCtx,
		redeemersByTxSQL,
		activeArguments(snapshot, hashArgument(hash))...,
	)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	result := make([]model.Redeemer, 0)
	boundaries := make([]model.PartialHistoryBoundary, 0)
	for rows.Next() {
		var (
			txHash         []byte
			rawPurpose     uint8
			purpose        string
			index          uint32
			dataCBOR       []byte
			dataLength     uint32
			dataHash       []byte
			memory         uint64
			steps          uint64
			applied        bool
			resolution     string
			targetTx       *string
			targetOutput   *uint32
			targetPolicy   *string
			targetReward   *string
			targetOrdinal  *uint32
			targetIdentity *string
			resolvedScript *string
		)
		if err := rows.Scan(
			&txHash,
			&rawPurpose,
			&purpose,
			&index,
			&dataCBOR,
			&dataLength,
			&dataHash,
			&memory,
			&steps,
			&applied,
			&resolution,
			&targetTx,
			&targetOutput,
			&targetPolicy,
			&targetReward,
			&targetOrdinal,
			&targetIdentity,
			&resolvedScript,
		); err != nil {
			return nil, nil, err
		}
		tx, err := model.Hash32FromBytes(txHash)
		if err != nil {
			return nil, nil, err
		}
		if uint32(len(dataCBOR)) != dataLength {
			return nil, nil, errors.New("redeemer data length mismatch")
		}
		storedDataHash, err := model.Hash32FromBytes(dataHash)
		if err != nil {
			return nil, nil, err
		}
		if storedDataHash != calculateContentHash(dataCBOR) {
			return nil, nil, errors.New("redeemer data hash mismatch")
		}
		redeemer := model.Redeemer{
			TxHash:     tx,
			PurposeTag: rawPurpose,
			Purpose:    purpose,
			Index:      index,
			DataCBOR:   model.Bytes(dataCBOR),
			Memory:     memory,
			Steps:      steps,
			Applied:    applied,
			Target: model.ResolvedTarget{
				Status: resolution,
			},
		}
		if targetTx != nil {
			target, err := model.Hash32FromBytes([]byte(*targetTx))
			if err != nil {
				return nil, nil, err
			}
			if targetOutput == nil {
				return nil, nil, errors.New("redeemer target transaction lacks output index")
			}
			ref := model.UTxORef{TxHash: target, Index: *targetOutput}
			redeemer.Target.SourceUTxO = &ref
		}
		if targetPolicy != nil {
			policy, err := model.PolicyIDFromBytes([]byte(*targetPolicy))
			if err != nil {
				return nil, nil, err
			}
			redeemer.Target.PolicyID = &policy
		}
		if targetReward != nil {
			redeemer.Target.RewardAccount = model.Bytes([]byte(*targetReward))
		}
		redeemer.Target.BodyOrdinal = targetOrdinal
		if targetIdentity != nil {
			redeemer.Target.ProcedureIdentity = model.Bytes([]byte(*targetIdentity))
		}
		if resolvedScript != nil {
			if len(*resolvedScript) != 28 {
				return nil, nil, fmt.Errorf("resolved script hash has %d bytes", len(*resolvedScript))
			}
			redeemer.Target.ScriptHash = model.Bytes([]byte(*resolvedScript))
		}
		result = append(result, redeemer)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	for index := range result {
		if result[index].Purpose != "spend" || result[index].Target.SourceUTxO == nil {
			continue
		}
		output, err := store.outputByRef(ctx, snapshot, *result[index].Target.SourceUTxO)
		if err == nil {
			result[index].Target.SourceOutput = &output
			continue
		}
		if errors.Is(err, ErrNotFound) && !snapshot.CompleteHistory {
			boundaries = append(boundaries, model.PartialHistoryBoundary{
				UTxO:   *result[index].Target.SourceUTxO,
				Reason: "resolved spend target output predates this partial-history dataset",
			})
			continue
		}
		return nil, nil, err
	}
	return result, boundaries, nil
}
