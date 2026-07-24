package clickhouse

import (
	"bytes"
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
	transactions, boundaries, err := store.transactionsByTx(
		queryCtx,
		snapshot,
		[]model.Hash32{hash},
	)
	if err != nil {
		return nil, nil, err
	}
	if len(transactions) != 1 || transactions[0].Hash != hash {
		return nil, nil, ErrConflictingRow
	}
	return transactions[0].Redeemers, boundaries, nil
}

func scanRedeemerRow(row scanner) (model.Hash32, model.Redeemer, error) {
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
	if err := row.Scan(
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
		return model.Hash32{}, model.Redeemer{}, err
	}
	owner, err := model.Hash32FromBytes(txHash)
	if err != nil {
		return model.Hash32{}, model.Redeemer{}, err
	}
	if uint32(len(dataCBOR)) != dataLength {
		return model.Hash32{}, model.Redeemer{}, errors.New(
			"redeemer data length mismatch",
		)
	}
	storedDataHash, err := model.Hash32FromBytes(dataHash)
	if err != nil {
		return model.Hash32{}, model.Redeemer{}, err
	}
	if len(dataCBOR) == 0 ||
		len(dataCBOR) > maximumTransactionContextBytes ||
		storedDataHash != calculateContentHash(dataCBOR) {
		return model.Hash32{}, model.Redeemer{}, errors.New(
			"redeemer data hash mismatch",
		)
	}
	redeemer := model.Redeemer{
		PurposeTag: rawPurpose,
		Purpose:    purpose,
		Index:      index,
		DataCBOR:   model.Bytes(bytes.Clone(dataCBOR)),
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
			return model.Hash32{}, model.Redeemer{}, err
		}
		if targetOutput == nil {
			return model.Hash32{}, model.Redeemer{}, errors.New(
				"redeemer target transaction lacks output index",
			)
		}
		ref := model.UTxORef{TxHash: target, Index: *targetOutput}
		redeemer.Target.SourceUTxO = &ref
	} else if targetOutput != nil {
		return model.Hash32{}, model.Redeemer{}, errors.New(
			"redeemer target output index lacks transaction",
		)
	}
	if targetPolicy != nil {
		policy, err := model.PolicyIDFromBytes([]byte(*targetPolicy))
		if err != nil {
			return model.Hash32{}, model.Redeemer{}, err
		}
		redeemer.Target.PolicyID = &policy
	}
	if targetReward != nil {
		redeemer.Target.RewardAccount = model.Bytes(
			bytes.Clone([]byte(*targetReward)),
		)
	}
	redeemer.Target.BodyOrdinal = targetOrdinal
	if targetIdentity != nil {
		redeemer.Target.ProcedureIdentity = model.Bytes(
			bytes.Clone([]byte(*targetIdentity)),
		)
	}
	if resolvedScript != nil {
		if len(*resolvedScript) != 28 {
			return model.Hash32{}, model.Redeemer{}, fmt.Errorf(
				"resolved script hash has %d bytes",
				len(*resolvedScript),
			)
		}
		redeemer.Target.ScriptHash = model.Bytes(
			bytes.Clone([]byte(*resolvedScript)),
		)
	}
	if err := validateRedeemerTarget(redeemer); err != nil {
		return model.Hash32{}, model.Redeemer{}, err
	}
	return owner, redeemer, nil
}

func validateRedeemerTarget(redeemer model.Redeemer) error {
	if redeemer.Target.Status != "resolved" {
		return fmt.Errorf("unsupported redeemer resolution status %q", redeemer.Target.Status)
	}
	switch redeemer.Purpose {
	case "spend":
		if redeemer.PurposeTag != 0 || redeemer.Target.SourceUTxO == nil ||
			redeemer.Target.PolicyID != nil ||
			len(redeemer.Target.RewardAccount) != 0 ||
			redeemer.Target.BodyOrdinal != nil ||
			len(redeemer.Target.ProcedureIdentity) != 0 {
			return errors.New("spend redeemer target is malformed")
		}
	case "mint":
		if redeemer.PurposeTag != 1 || redeemer.Target.PolicyID == nil ||
			redeemer.Target.SourceUTxO != nil ||
			len(redeemer.Target.RewardAccount) != 0 ||
			redeemer.Target.BodyOrdinal != nil ||
			len(redeemer.Target.ProcedureIdentity) != 0 ||
			len(redeemer.Target.ScriptHash) != 28 ||
			!bytes.Equal(redeemer.Target.ScriptHash, redeemer.Target.PolicyID[:]) {
			return errors.New("mint redeemer target is malformed")
		}
	case "reward":
		if redeemer.PurposeTag != 3 ||
			redeemer.Target.SourceUTxO != nil ||
			redeemer.Target.PolicyID != nil ||
			redeemer.Target.BodyOrdinal != nil ||
			len(redeemer.Target.ProcedureIdentity) != 0 {
			return errors.New("reward redeemer purpose tag is malformed")
		}
		reward := []byte(redeemer.Target.RewardAccount)
		if len(reward) != 29 {
			return errors.New("reward redeemer account is malformed")
		}
		if err := validateRewardAccount(reward, "script", reward[1:]); err != nil {
			return fmt.Errorf("reward redeemer: %w", err)
		}
		if len(redeemer.Target.ScriptHash) != 28 ||
			!bytes.Equal(redeemer.Target.ScriptHash, reward[1:]) {
			return errors.New("reward redeemer script hash disagrees with account")
		}
	case "certificate":
		if redeemer.PurposeTag != 2 || redeemer.Target.BodyOrdinal == nil ||
			redeemer.Target.SourceUTxO != nil ||
			redeemer.Target.PolicyID != nil ||
			len(redeemer.Target.RewardAccount) != 0 {
			return errors.New("certificate redeemer target is malformed")
		}
		return validateCompactTargetIdentity(redeemer, 18)
	case "vote":
		if redeemer.PurposeTag != 4 || redeemer.Target.BodyOrdinal == nil ||
			redeemer.Target.SourceUTxO != nil ||
			redeemer.Target.PolicyID != nil ||
			len(redeemer.Target.RewardAccount) != 0 ||
			len(redeemer.Target.ProcedureIdentity) != 29 ||
			redeemer.Target.ProcedureIdentity[0] > 4 {
			return errors.New("vote redeemer target identity is malformed")
		}
		voterType := redeemer.Target.ProcedureIdentity[0]
		voterHash := redeemer.Target.ProcedureIdentity[1:]
		if voterType == 1 || voterType == 3 {
			if len(redeemer.Target.ScriptHash) != 28 ||
				!bytes.Equal(redeemer.Target.ScriptHash, voterHash) {
				return errors.New("vote redeemer script hash disagrees with voter")
			}
		} else if len(redeemer.Target.ScriptHash) != 0 {
			return errors.New("key voter has a resolved script hash")
		}
	case "proposal":
		if redeemer.PurposeTag != 5 || redeemer.Target.BodyOrdinal == nil ||
			redeemer.Target.SourceUTxO != nil ||
			redeemer.Target.PolicyID != nil ||
			len(redeemer.Target.RewardAccount) != 0 {
			return errors.New("proposal redeemer target is malformed")
		}
		return validateCompactTargetIdentity(redeemer, 6)
	default:
		return fmt.Errorf("unsupported redeemer purpose %q", redeemer.Purpose)
	}
	return nil
}

func validateCompactTargetIdentity(redeemer model.Redeemer, maximumConstructor byte) error {
	identity := redeemer.Target.ProcedureIdentity
	if len(identity) != 34 {
		return fmt.Errorf("compact target identity has %d bytes, want 34", len(identity))
	}
	if identity[0] != redeemer.PurposeTag {
		return errors.New("compact target identity purpose tag disagrees with redeemer")
	}
	if identity[1] > maximumConstructor {
		return fmt.Errorf(
			"compact target identity constructor is %d, maximum %d",
			identity[1],
			maximumConstructor,
		)
	}
	return nil
}

func validateSpendScriptContext(redeemer model.Redeemer, output model.Output) error {
	switch output.PaymentCredentialKind {
	case "script":
		if len(output.PaymentCredentialHash) != 28 ||
			!bytes.Equal(output.PaymentCredentialHash, redeemer.Target.ScriptHash) {
			return errors.New("spend redeemer script hash disagrees with source output")
		}
	case "key", "none":
		if len(redeemer.Target.ScriptHash) != 0 {
			return errors.New("non-script source output has a resolved script hash")
		}
	default:
		return fmt.Errorf(
			"unsupported source payment credential kind %q",
			output.PaymentCredentialKind,
		)
	}
	return nil
}
