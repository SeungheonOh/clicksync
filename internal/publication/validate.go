package publication

import (
	"bytes"
	"errors"
	"fmt"
	"math"

	"golang.org/x/crypto/blake2b"

	"clicksync/internal/model"
)

const maximumContextCBORBytes = 2 * 1024 * 1024

func validateBlock(block model.Block) error {
	if block.Hash == (model.Hash32{}) {
		return errors.New("block hash is zero")
	}
	if !block.Synthetic && (!block.BodyHashVerified || !block.TransactionIDsVerified) {
		return errors.New("non-synthetic block lacks structural hash verification")
	}
	if block.ObservedAt.IsZero() {
		return errors.New("block observation time is missing")
	}
	datums := make(map[model.Hash32][]byte, len(block.Datums))
	for _, datum := range block.Datums {
		if len(datum.CBOR) == 0 || len(datum.CBOR) > maximumContextCBORBytes {
			return fmt.Errorf("datum %x length is outside the persisted bound", datum.Hash)
		}
		if calculated := model.Hash32(blake2b.Sum256(datum.CBOR)); calculated != datum.Hash {
			return fmt.Errorf("datum %x has an invalid content hash", datum.Hash)
		}
		if previous, duplicate := datums[datum.Hash]; duplicate && !bytes.Equal(previous, datum.CBOR) {
			return fmt.Errorf("datum %x has conflicting bodies", datum.Hash)
		}
		datums[datum.Hash] = datum.CBOR
	}
	transactionHashes := make(map[model.Hash32]struct{}, len(block.Transactions))
	outputRefs := make(map[OutputRef]struct{})
	for transactionIndex, transaction := range block.Transactions {
		if transaction.Hash == (model.Hash32{}) {
			return fmt.Errorf("transaction %d hash is zero", transactionIndex)
		}
		if _, duplicate := transactionHashes[transaction.Hash]; duplicate {
			return fmt.Errorf("duplicate transaction hash %x", transaction.Hash)
		}
		transactionHashes[transaction.Hash] = struct{}{}
		if uint64(transactionIndex) > math.MaxUint32 ||
			transaction.Order != uint32(transactionIndex) {
			return errors.New("transaction orders are not consecutive body indexes")
		}
		if err := validateTransaction(transaction, block.Synthetic, datums, outputRefs); err != nil {
			return fmt.Errorf("transaction %x: %w", transaction.Hash, err)
		}
	}
	return nil
}

func validateSource(
	source Source,
	observations []model.PeerObservation,
	synthetic bool,
) error {
	if synthetic {
		if source != OfficialMainnetGenesisSource() {
			return errors.New("synthetic block source is not the pinned official mainnet genesis")
		}
		if len(observations) != 0 {
			return errors.New("official genesis publication cannot carry peer observations")
		}
		return nil
	}
	if source.PeerHost == "" || source.PeerAddress == "" || source.Operator == "" {
		return errors.New("selected source host, address, and operator are required")
	}
	if source.N2NVersion < 7 || source.N2NVersion > 15 {
		return fmt.Errorf("selected source negotiated unsupported N2N version %d", source.N2NVersion)
	}
	if source.NetworkMagic != 764824073 {
		return fmt.Errorf("selected source network magic %d is not pinned mainnet", source.NetworkMagic)
	}
	for _, observation := range observations {
		if observation.ID == ([16]byte{}) ||
			observation.TipHash == (model.Hash32{}) ||
			observation.ObservedAt.IsZero() {
			return errors.New("peer observation identity, tip hash, and time are required")
		}
		switch observation.Kind {
		case "checkpoint", "source_change", "rollback", "disagreement":
		default:
			return fmt.Errorf("unknown peer observation kind %q", observation.Kind)
		}
		switch observation.Result {
		case "agreed", "disagreed", "unavailable", "quarantined":
		default:
			return fmt.Errorf("unknown peer observation result %q", observation.Result)
		}
		if observation.PeerHost == "" || observation.PeerAddress == "" || observation.Operator == "" {
			return errors.New("peer observation provenance is incomplete")
		}
		if observation.NetworkMagic != source.NetworkMagic {
			return errors.New("peer observation network magic disagrees with the selected source")
		}
		checkpointSet := observation.CheckpointSlot != nil ||
			observation.CheckpointHash != nil ||
			observation.CheckpointBlockNumber != nil ||
			observation.CheckpointIsByronEBB != nil
		if checkpointSet &&
			(observation.CheckpointSlot == nil ||
				observation.CheckpointHash == nil ||
				observation.CheckpointBlockNumber == nil ||
				observation.CheckpointIsByronEBB == nil) {
			return errors.New("peer checkpoint point is partially populated")
		}
		if observation.N2NVersion < 7 || observation.N2NVersion > 15 {
			return fmt.Errorf("peer observation negotiated unsupported N2N version %d", observation.N2NVersion)
		}
		if observation.SelectedBodySource {
			if observation.PeerHost != source.PeerHost ||
				observation.PeerAddress != source.PeerAddress ||
				observation.Operator != source.Operator ||
				observation.N2NVersion != source.N2NVersion ||
				observation.Result != "agreed" {
				return errors.New("selected peer observation disagrees with sampled source provenance")
			}
		}
	}
	return nil
}

func validateTransaction(
	transaction model.Transaction,
	synthetic bool,
	datums map[model.Hash32][]byte,
	outputRefs map[OutputRef]struct{},
) error {
	switch transaction.FlowKind {
	case "genesis":
		if !synthetic || !transaction.Phase2Valid {
			return errors.New("genesis flow requires a valid synthetic block")
		}
		if transaction.DeclaredFee != nil || transaction.EffectiveFee != nil {
			return errors.New("genesis flow must not invent a transaction fee")
		}
	case "regular":
		if !transaction.Phase2Valid {
			return errors.New("regular flow is marked phase-2 invalid")
		}
		if transaction.DeclaredFee == nil || transaction.EffectiveFee == nil {
			return errors.New("regular flow requires declared and effective fees")
		}
		if *transaction.EffectiveFee != *transaction.DeclaredFee {
			return errors.New("regular flow effective fee differs from declared fee")
		}
	case "collateral":
		if transaction.Phase2Valid {
			return errors.New("collateral flow is marked phase-2 valid")
		}
		if transaction.EffectiveFee != nil && *transaction.EffectiveFee == 0 {
			return errors.New("collateral flow has an invalid zero effective fee")
		}
	default:
		return fmt.Errorf("unknown flow kind %q", transaction.FlowKind)
	}
	if transaction.MintApplied != transaction.Phase2Valid && transaction.FlowKind != "genesis" {
		return errors.New("mint applied state disagrees with phase-2 validity")
	}
	if err := validateDeltas(transaction.Mint); err != nil {
		return err
	}
	type inputKey struct {
		Hash  model.Hash32
		Index uint32
	}
	inputKeys := make(map[inputKey]struct{}, len(transaction.Inputs))
	roleOrdinals := make(map[string]map[uint32]struct{})
	roleCounts := make(map[string]uint32)
	for _, input := range transaction.Inputs {
		if input.TransactionHash != transaction.Hash || input.TransactionOrder != transaction.Order {
			return errors.New("input transaction linkage mismatch")
		}
		switch input.Role {
		case "regular":
			if input.Consumed != transaction.Phase2Valid {
				return errors.New("regular input consumption disagrees with phase-2 validity")
			}
		case "collateral":
			if input.Consumed == transaction.Phase2Valid {
				return errors.New("collateral input consumption disagrees with phase-2 validity")
			}
		case "reference":
			if input.Consumed {
				return errors.New("reference input is consumed")
			}
		default:
			return fmt.Errorf("unknown input role %q", input.Role)
		}
		key := inputKey{
			Hash:  input.SourceHash,
			Index: input.SourceIndex,
		}
		if _, duplicate := inputKeys[key]; duplicate {
			return errors.New("duplicate input reference/body ordinal")
		}
		inputKeys[key] = struct{}{}
		ordinals := roleOrdinals[input.Role]
		if ordinals == nil {
			ordinals = make(map[uint32]struct{})
			roleOrdinals[input.Role] = ordinals
		}
		if _, duplicate := ordinals[input.BodyOrdinal]; duplicate {
			return fmt.Errorf("duplicate %s input body ordinal %d", input.Role, input.BodyOrdinal)
		}
		ordinals[input.BodyOrdinal] = struct{}{}
		roleCounts[input.Role]++
	}
	for role, ordinals := range roleOrdinals {
		for ordinal := uint32(0); ordinal < roleCounts[role]; ordinal++ {
			if _, ok := ordinals[ordinal]; !ok {
				return fmt.Errorf("%s input body ordinals are not consecutive", role)
			}
		}
	}
	outputBodyOrdinals := make(map[uint32]struct{}, len(transaction.Outputs))
	for outputPosition, output := range transaction.Outputs {
		if output.TransactionHash != transaction.Hash || output.TransactionOrder != transaction.Order {
			return errors.New("output transaction linkage mismatch")
		}
		ref := OutputRef{Hash: output.TransactionHash, Index: output.Index}
		if _, duplicate := outputRefs[ref]; duplicate {
			return fmt.Errorf("duplicate produced output %x#%d", ref.Hash, ref.Index)
		}
		outputRefs[ref] = struct{}{}
		if _, duplicate := outputBodyOrdinals[output.BodyOrdinal]; duplicate {
			return fmt.Errorf("duplicate output body ordinal %d", output.BodyOrdinal)
		}
		outputBodyOrdinals[output.BodyOrdinal] = struct{}{}
		switch transaction.FlowKind {
		case "regular":
			if output.Kind != "regular" {
				return errors.New("valid regular transaction produced a non-regular output")
			}
			if output.Index != uint32(outputPosition) || output.BodyOrdinal != uint32(outputPosition) {
				return errors.New("regular output indexes/body ordinals are not consecutive")
			}
		case "collateral":
			if output.Kind != "collateral_return" {
				return errors.New("invalid transaction persisted a non-collateral-return output")
			}
			if len(transaction.Outputs) != 1 || output.BodyOrdinal != 0 {
				return errors.New("collateral flow must have at most one body-ordinal-zero return")
			}
		case "genesis":
			if output.Kind != "genesis" {
				return errors.New("genesis transaction produced a non-genesis output")
			}
			if output.BodyOrdinal != uint32(outputPosition) {
				return errors.New("genesis output body ordinals are not consecutive")
			}
		}
		if len(output.Address) == 0 {
			return errors.New("produced output has an empty address")
		}
		if err := validateAssets(output.Assets); err != nil {
			return err
		}
		switch output.DatumKind {
		case "none":
			if output.DatumHash != nil {
				return errors.New("datum-none output has a datum hash")
			}
		case "hash":
			if output.DatumHash == nil {
				return errors.New("datum-hash output has no datum hash")
			}
		case "inline":
			if output.DatumHash == nil {
				return errors.New("inline datum output has no datum hash")
			}
			if _, ok := datums[*output.DatumHash]; !ok {
				return errors.New("inline datum body is missing from the block bundle")
			}
		default:
			return fmt.Errorf("unknown datum kind %q", output.DatumKind)
		}
		if (output.ReferenceScriptHash == nil) != (output.ReferenceScriptLanguage == nil) {
			return errors.New("reference script hash/language pairing is incomplete")
		}
	}
	for _, observation := range transaction.DatumObservations {
		if observation.TransactionHash != transaction.Hash ||
			observation.TransactionOrder != transaction.Order {
			return errors.New("datum observation transaction linkage mismatch")
		}
		if _, ok := datums[observation.Hash]; !ok {
			return fmt.Errorf("datum observation %x has no verified body", observation.Hash)
		}
		switch observation.SourceKind {
		case "inline_output":
			if observation.OutputIndex == nil {
				return errors.New("inline datum observation lacks output index")
			}
		case "witness":
			if observation.OutputIndex != nil {
				return errors.New("witness datum observation has output index")
			}
		default:
			return fmt.Errorf("unknown datum source kind %q", observation.SourceKind)
		}
	}
	withdrawalOrdinals := make(map[uint32]struct{}, len(transaction.Withdrawals))
	for withdrawalPosition, withdrawal := range transaction.Withdrawals {
		if withdrawal.TransactionHash != transaction.Hash ||
			withdrawal.TransactionOrder != transaction.Order {
			return errors.New("withdrawal transaction linkage mismatch")
		}
		if withdrawal.Applied != transaction.Phase2Valid {
			return errors.New("withdrawal applied state disagrees with phase-2 validity")
		}
		if len(withdrawal.RewardAccount) == 0 {
			return errors.New("withdrawal reward account is empty")
		}
		if withdrawal.CredentialKind != "key" && withdrawal.CredentialKind != "script" {
			return fmt.Errorf("unknown withdrawal credential kind %q", withdrawal.CredentialKind)
		}
		if withdrawal.BodyOrdinal != uint32(withdrawalPosition) {
			return errors.New("withdrawal body ordinals are not consecutive")
		}
		if _, duplicate := withdrawalOrdinals[withdrawal.BodyOrdinal]; duplicate {
			return errors.New("duplicate withdrawal body ordinal")
		}
		withdrawalOrdinals[withdrawal.BodyOrdinal] = struct{}{}
	}
	type redeemerKey struct {
		purpose string
		index   uint32
	}
	redeemerKeys := make(map[redeemerKey]struct{}, len(transaction.Redeemers))
	for _, redeemer := range transaction.Redeemers {
		if redeemer.TransactionHash != transaction.Hash ||
			redeemer.TransactionOrder != transaction.Order {
			return errors.New("redeemer transaction linkage mismatch")
		}
		if len(redeemer.DataCBOR) == 0 || len(redeemer.DataCBOR) > maximumContextCBORBytes {
			return errors.New("redeemer data length is outside the persisted bound")
		}
		if calculated := model.Hash32(blake2b.Sum256(redeemer.DataCBOR)); calculated != redeemer.DataHash {
			return errors.New("redeemer data hash mismatch")
		}
		if redeemer.Applied != transaction.Phase2Valid {
			return errors.New("redeemer applied state disagrees with phase-2 validity")
		}
		if err := validateRedeemerTarget(redeemer); err != nil {
			return err
		}
		key := redeemerKey{purpose: redeemer.Purpose, index: redeemer.Index}
		if _, duplicate := redeemerKeys[key]; duplicate {
			return errors.New("duplicate redeemer purpose/index")
		}
		redeemerKeys[key] = struct{}{}
	}
	if transaction.Metadata != nil {
		metadata := transaction.Metadata
		if metadata.TransactionHash != transaction.Hash ||
			metadata.TransactionOrder != transaction.Order {
			return errors.New("metadata transaction linkage mismatch")
		}
		if len(metadata.CBOR) == 0 || len(metadata.CBOR) > maximumContextCBORBytes {
			return errors.New("metadata length is outside the persisted bound")
		}
		if calculated := model.Hash32(blake2b.Sum256(metadata.CBOR)); calculated != metadata.ContentHash {
			return errors.New("metadata content hash mismatch")
		}
		for index := 1; index < len(metadata.Labels); index++ {
			if metadata.Labels[index] <= metadata.Labels[index-1] {
				return errors.New("metadata labels are not sorted and unique")
			}
		}
	}
	return nil
}

func validateAssets(assets []model.Asset) error {
	for index, asset := range assets {
		if len(asset.Name) > 32 {
			return errors.New("asset name exceeds 32 bytes")
		}
		if asset.Quantity == 0 {
			return errors.New("zero output asset quantity")
		}
		if index > 0 && compareAsset(
			assets[index-1].PolicyID,
			assets[index-1].Name,
			asset.PolicyID,
			asset.Name,
		) >= 0 {
			return errors.New("output assets are not strictly sorted and unique")
		}
	}
	return nil
}

func validateDeltas(assets []model.AssetDelta) error {
	for index, asset := range assets {
		if len(asset.Name) > 32 {
			return errors.New("mint asset name exceeds 32 bytes")
		}
		if asset.Quantity == 0 {
			return errors.New("zero mint quantity")
		}
		if index > 0 && compareAsset(
			assets[index-1].PolicyID,
			assets[index-1].Name,
			asset.PolicyID,
			asset.Name,
		) >= 0 {
			return errors.New("mint assets are not strictly sorted and unique")
		}
	}
	return nil
}

func compareAsset(
	leftPolicy model.Hash28,
	leftName []byte,
	rightPolicy model.Hash28,
	rightName []byte,
) int {
	if compared := bytes.Compare(leftPolicy[:], rightPolicy[:]); compared != 0 {
		return compared
	}
	return bytes.Compare(leftName, rightName)
}

func validateRedeemerTarget(redeemer model.Redeemer) error {
	switch redeemer.Purpose {
	case "spend":
		if redeemer.TargetTxHash == nil || redeemer.TargetOutputIndex == nil {
			return errors.New("spend redeemer target is unresolved")
		}
	case "mint":
		if redeemer.TargetPolicyID == nil {
			return errors.New("mint redeemer target is unresolved")
		}
	case "reward":
		if len(redeemer.TargetRewardAccount) == 0 {
			return errors.New("reward redeemer target is unresolved")
		}
	case "certificate", "vote", "proposal":
		if redeemer.TargetBodyOrdinal == nil {
			return fmt.Errorf("%s redeemer target is unresolved", redeemer.Purpose)
		}
	default:
		return fmt.Errorf("unsupported redeemer purpose %q", redeemer.Purpose)
	}
	return nil
}
