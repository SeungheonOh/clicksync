package clickhouse

import (
	"bytes"
	"errors"
	"math"
	"testing"

	"github.com/clicksync-project/clickout/internal/model"
)

func validDecodedRecord() decodedTransaction {
	txHash := repeatedHash(0x41)
	blockHash := repeatedHash(0x42)
	fee := uint64(1)
	credential := bytes.Repeat([]byte{0x33}, 28)
	return decodedTransaction{
		Transaction: model.Transaction{
			Hash:         txHash,
			BlockHash:    blockHash,
			BlockHeight:  1,
			Era:          "Conway",
			Phase2Valid:  true,
			FlowKind:     "regular",
			DeclaredFee:  &fee,
			EffectiveFee: &fee,
			MintApplied:  true,
			Mint:         []model.SignedAssetQuantity{},
			Inputs:       []model.Spend{},
			Outputs: []model.Output{{
				Ref: model.UTxORef{
					TxHash: txHash,
					Index:  0,
				},
				ProducingTx:           txHash,
				BodyOrdinal:           0,
				BlockHash:             blockHash,
				BlockHeight:           1,
				Kind:                  model.OutputRegular,
				Address:               append([]byte{0x61}, credential...),
				PaymentCredentialKind: "key",
				PaymentCredentialHash: model.Bytes(
					bytes.Clone(credential),
				),
				Assets:    []model.OutputAsset{},
				DatumKind: "none",
			}},
			Withdrawals: []model.Withdrawal{},
			Redeemers:   []model.Redeemer{},
			Datums:      []model.TransactionDatum{},
		},
		PublicationID: 7,
		Declared: transactionDeclaredCounts{
			Outputs: 1,
		},
	}
}

func TestDecodedTransactionDeclaredCountsAreExact(t *testing.T) {
	t.Parallel()
	base := validDecodedRecord()
	if err := validateDecodedTransaction(&base); err != nil {
		t.Fatalf("valid base: %v", err)
	}
	tests := map[string]func(*decodedTransaction){
		"regular inputs": func(record *decodedTransaction) {
			record.Declared.RegularInputs = 1
		},
		"collateral inputs": func(record *decodedTransaction) {
			record.Declared.CollateralInputs = 1
		},
		"reference inputs": func(record *decodedTransaction) {
			record.Declared.ReferenceInputs = 1
		},
		"outputs": func(record *decodedTransaction) {
			record.Declared.Outputs = 2
		},
		"withdrawals": func(record *decodedTransaction) {
			record.Declared.Withdrawals = 1
		},
		"redeemers": func(record *decodedTransaction) {
			record.Declared.Redeemers = 1
		},
		"metadata": func(record *decodedTransaction) {
			record.Declared.MetadataPresent = true
		},
		"datum observations": func(record *decodedTransaction) {
			record.Declared.DatumObservations = 1
		},
	}
	for name, mutate := range tests {
		mutate := mutate
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			record := validDecodedRecord()
			mutate(&record)
			if err := validateDecodedTransaction(&record); !errors.Is(
				err,
				ErrConflictingRow,
			) {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

func baseRedeemer(purpose string, tag uint8) model.Redeemer {
	return model.Redeemer{
		PurposeTag: tag,
		Purpose:    purpose,
		DataCBOR:   model.Bytes{0x01},
		Applied:    true,
		Target: model.ResolvedTarget{
			Status: "resolved",
		},
	}
}

func TestRedeemerLedgerPointerOracles(t *testing.T) {
	t.Parallel()
	low := model.UTxORef{TxHash: repeatedHash(1), Index: 7}
	high := model.UTxORef{TxHash: repeatedHash(2), Index: 0}
	policyLow := model.PolicyID{1}
	policyHigh := model.PolicyID{2}
	scriptAccount := func(fill byte) model.Bytes {
		return append([]byte{0xf1}, bytes.Repeat([]byte{fill}, 28)...)
	}
	accountLow := scriptAccount(1)
	accountHigh := scriptAccount(2)
	tests := []struct {
		name   string
		setup  func() model.Transaction
		mutate func(*model.Transaction)
	}{
		{
			name: "spend",
			setup: func() model.Transaction {
				redeemer := baseRedeemer("spend", 0)
				redeemer.Target.SourceUTxO = &low
				return model.Transaction{
					Phase2Valid: true,
					Inputs: []model.Spend{
						{Source: high, Role: model.InputRegular},
						{Source: low, Role: model.InputRegular},
					},
					Redeemers: []model.Redeemer{redeemer},
				}
			},
			mutate: func(transaction *model.Transaction) {
				transaction.Redeemers[0].Target.SourceUTxO = &high
			},
		},
		{
			name: "mint",
			setup: func() model.Transaction {
				redeemer := baseRedeemer("mint", 1)
				redeemer.Index = 1
				redeemer.Target.PolicyID = &policyHigh
				redeemer.Target.ScriptHash = model.Bytes(
					bytes.Clone(policyHigh[:]),
				)
				return model.Transaction{
					Phase2Valid: true,
					Mint: []model.SignedAssetQuantity{
						{PolicyID: policyLow, Name: model.Bytes{1}, Quantity: 1},
						{PolicyID: policyHigh, Name: model.Bytes{1}, Quantity: 1},
					},
					Redeemers: []model.Redeemer{redeemer},
				}
			},
			mutate: func(transaction *model.Transaction) {
				transaction.Redeemers[0].Target.PolicyID = &policyLow
				transaction.Redeemers[0].Target.ScriptHash =
					model.Bytes(bytes.Clone(policyLow[:]))
			},
		},
		{
			name: "reward",
			setup: func() model.Transaction {
				redeemer := baseRedeemer("reward", 3)
				redeemer.Index = 1
				redeemer.Target.RewardAccount =
					model.Bytes(bytes.Clone(accountHigh))
				redeemer.Target.ScriptHash =
					model.Bytes(bytes.Clone(accountHigh[1:]))
				return model.Transaction{
					Phase2Valid: true,
					Withdrawals: []model.Withdrawal{
						{RewardAccount: accountLow},
						{RewardAccount: accountHigh},
					},
					Redeemers: []model.Redeemer{redeemer},
				}
			},
			mutate: func(transaction *model.Transaction) {
				transaction.Redeemers[0].Target.RewardAccount =
					model.Bytes(bytes.Clone(accountLow))
				transaction.Redeemers[0].Target.ScriptHash =
					model.Bytes(bytes.Clone(accountLow[1:]))
			},
		},
		{
			name: "certificate",
			setup: func() model.Transaction {
				ordinal := uint32(0)
				redeemer := baseRedeemer("certificate", 2)
				redeemer.Target.BodyOrdinal = &ordinal
				redeemer.Target.ProcedureIdentity =
					append([]byte{2, 0}, make([]byte, 32)...)
				return model.Transaction{
					Phase2Valid: true,
					Redeemers:   []model.Redeemer{redeemer},
				}
			},
			mutate: mismatchTargetOrdinal,
		},
		{
			name: "vote",
			setup: func() model.Transaction {
				ordinal := uint32(0)
				redeemer := baseRedeemer("vote", 4)
				redeemer.Target.BodyOrdinal = &ordinal
				redeemer.Target.ProcedureIdentity =
					append([]byte{0}, make([]byte, 28)...)
				return model.Transaction{
					Phase2Valid: true,
					Redeemers:   []model.Redeemer{redeemer},
				}
			},
			mutate: mismatchTargetOrdinal,
		},
		{
			name: "proposal",
			setup: func() model.Transaction {
				ordinal := uint32(0)
				redeemer := baseRedeemer("proposal", 5)
				redeemer.Target.BodyOrdinal = &ordinal
				redeemer.Target.ProcedureIdentity =
					append([]byte{5, 0}, make([]byte, 32)...)
				return model.Transaction{
					Phase2Valid: true,
					Redeemers:   []model.Redeemer{redeemer},
				}
			},
			mutate: mismatchTargetOrdinal,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			transaction := test.setup()
			declared := transactionDeclaredCounts{
				Redeemers: uint32(len(transaction.Redeemers)),
			}
			if err := validateDecodedRedeemers(
				&transaction,
				declared,
			); err != nil {
				t.Fatalf("valid oracle: %v", err)
			}
			test.mutate(&transaction)
			if err := validateDecodedRedeemers(
				&transaction,
				declared,
			); !errors.Is(err, ErrConflictingRow) {
				t.Fatalf("mutated oracle error = %v", err)
			}
		})
	}
}

func mismatchTargetOrdinal(transaction *model.Transaction) {
	ordinal := uint32(1)
	transaction.Redeemers[0].Target.BodyOrdinal = &ordinal
}

func TestWithdrawalsUseCanonicalAccountAddressOrder(t *testing.T) {
	t.Parallel()
	withdrawal := func(kind string, fill byte, ordinal uint32) model.Withdrawal {
		header := byte(0xe1)
		if kind == "script" {
			header = 0xf1
		}
		credential := bytes.Repeat([]byte{fill}, 28)
		return model.Withdrawal{
			RewardAccount:  append([]byte{header}, credential...),
			BodyOrdinal:    ordinal,
			Applied:        true,
			CredentialKind: kind,
			CredentialHash: credential,
		}
	}
	transaction := model.Transaction{
		Phase2Valid: true,
		Withdrawals: []model.Withdrawal{
			withdrawal("script", 2, 0),
			withdrawal("key", 1, 1),
		},
	}
	declared := transactionDeclaredCounts{Withdrawals: 2}
	if err := validateDecodedWithdrawals(&transaction, declared); err != nil {
		t.Fatalf("canonical order: %v", err)
	}
	transaction.Withdrawals[0], transaction.Withdrawals[1] =
		transaction.Withdrawals[1], transaction.Withdrawals[0]
	transaction.Withdrawals[0].BodyOrdinal = 0
	transaction.Withdrawals[1].BodyOrdinal = 1
	if err := validateDecodedWithdrawals(
		&transaction,
		declared,
	); !errors.Is(err, ErrConflictingRow) {
		t.Fatalf("raw-byte order error = %v", err)
	}
}

func TestSpendRedeemerScriptContextMatrix(t *testing.T) {
	t.Parallel()
	ref := model.UTxORef{TxHash: repeatedHash(1), Index: 0}
	scriptHash := model.Bytes(bytes.Repeat([]byte{1}, 28))
	tests := []struct {
		name       string
		output     *model.Output
		scriptHash model.Bytes
		valid      bool
	}{
		{
			name: "resolved script",
			output: &model.Output{
				Ref:                   ref,
				PaymentCredentialKind: "script",
				PaymentCredentialHash: scriptHash,
			},
			scriptHash: scriptHash,
			valid:      true,
		},
		{
			name: "resolved script mismatch",
			output: &model.Output{
				Ref:                   ref,
				PaymentCredentialKind: "script",
				PaymentCredentialHash: scriptHash,
			},
			scriptHash: model.Bytes(bytes.Repeat([]byte{2}, 28)),
		},
		{
			name: "resolved key empty",
			output: &model.Output{
				Ref:                   ref,
				PaymentCredentialKind: "key",
			},
			valid: true,
		},
		{
			name: "resolved Byron empty",
			output: &model.Output{
				Ref:                   ref,
				PaymentCredentialKind: "none",
			},
			valid: true,
		},
		{name: "unresolved empty", valid: true},
		{name: "unresolved forged", scriptHash: scriptHash},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			redeemer := baseRedeemer("spend", 0)
			redeemer.Target.SourceUTxO = &ref
			redeemer.Target.SourceOutput = test.output
			redeemer.Target.ScriptHash =
				model.Bytes(bytes.Clone(test.scriptHash))
			transaction := model.Transaction{
				Phase2Valid: true,
				Inputs: []model.Spend{{
					Source:       ref,
					Role:         model.InputRegular,
					SourceOutput: test.output,
				}},
				Redeemers: []model.Redeemer{redeemer},
			}
			err := validateDecodedRedeemers(
				&transaction,
				transactionDeclaredCounts{Redeemers: 1},
			)
			if test.valid && err != nil {
				t.Fatal(err)
			}
			if !test.valid && !errors.Is(err, ErrConflictingRow) {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

func TestInputRoleOverlapMatrix(t *testing.T) {
	t.Parallel()
	txHash := repeatedHash(9)
	blockHash := repeatedHash(8)
	ref := model.UTxORef{TxHash: repeatedHash(1), Index: 0}
	spend := func(role model.InputRole) model.Spend {
		return model.Spend{
			Source:               ref,
			ConsumingTx:          txHash,
			ConsumingBlockHash:   blockHash,
			ConsumingBlockHeight: 1,
			Role:                 role,
			IsConsumed:           role == model.InputRegular,
		}
	}
	tests := []struct {
		name  string
		roles []model.InputRole
		valid bool
	}{
		{"regular collateral", []model.InputRole{model.InputRegular, model.InputCollateral}, true},
		{"collateral reference", []model.InputRole{model.InputCollateral, model.InputReference}, true},
		{"regular reference", []model.InputRole{model.InputRegular, model.InputReference}, false},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			transaction := model.Transaction{
				Hash:        txHash,
				BlockHash:   blockHash,
				BlockHeight: 1,
				Phase2Valid: true,
			}
			declared := transactionDeclaredCounts{}
			for _, role := range test.roles {
				transaction.Inputs = append(transaction.Inputs, spend(role))
				switch role {
				case model.InputRegular:
					declared.RegularInputs++
				case model.InputCollateral:
					declared.CollateralInputs++
				case model.InputReference:
					declared.ReferenceInputs++
				}
			}
			err := validateDecodedInputs(&transaction, declared)
			if test.valid && err != nil {
				t.Fatal(err)
			}
			if !test.valid && !errors.Is(err, ErrConflictingRow) {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

func TestHeaderDeclaredCountsRejectUInt32WithoutAllocation(t *testing.T) {
	t.Parallel()
	record := decodedTransaction{
		Transaction: model.Transaction{Hash: repeatedHash(1)},
		Declared: transactionDeclaredCounts{
			RegularInputs: math.MaxUint32,
		},
	}
	if err := validateDeclaredTransactionBounds(&record); !errors.Is(
		err,
		ErrResourceLimit,
	) {
		t.Fatalf("error = %v", err)
	}
}

func TestContextBodyPoolAndOccurrenceBudgets(t *testing.T) {
	t.Parallel()
	pool := newContextBodyPool()
	body := model.Bytes{1, 2, 3}
	hash := calculateContentHash(body)
	first, err := pool.retain("test", hash, body)
	if err != nil {
		t.Fatal(err)
	}
	second, err := pool.retain("test", hash, model.Bytes{1, 2, 3})
	if err != nil {
		t.Fatal(err)
	}
	if &first[0] != &second[0] || pool.total != 3 {
		t.Fatal("equal verified bodies were not interned")
	}
	pool.total = maximumMaterializedContextBytes - 1
	one := model.Bytes{4}
	if _, err := pool.retain(
		"test",
		calculateContentHash(one),
		one,
	); err != nil {
		t.Fatalf("exact unique boundary: %v", err)
	}
	over := model.Bytes{5}
	if _, err := pool.retain(
		"test",
		calculateContentHash(over),
		over,
	); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("unique over-boundary error = %v", err)
	}
	for raw, want := range map[uint64]uint64{0: 0, 1: 4, 2: 4, 3: 4, 4: 8} {
		got, ok := base64PayloadBytes(raw)
		if !ok || got != want {
			t.Fatalf("base64PayloadBytes(%d) = %d,%v want %d", raw, got, ok, want)
		}
	}
	var occurrences contextOccurrenceBudget
	if err := occurrences.add(
		"test",
		3,
		maximumContextOccurrenceBytes/4,
	); err != nil {
		t.Fatalf("exact occurrence boundary: %v", err)
	}
	if err := occurrences.add("test", 1, 1); !errors.Is(
		err,
		ErrResourceLimit,
	) {
		t.Fatalf("occurrence over-boundary error = %v", err)
	}
}

func TestTransactionEraAndFeeMatrix(t *testing.T) {
	t.Parallel()
	for _, era := range []string{
		"Byron", "Shelley", "Allegra", "Mary", "Alonzo", "Babbage",
		"Conway", "Dijkstra",
	} {
		if !validTransactionEra(era) {
			t.Fatalf("canonical era %q rejected", era)
		}
	}
	for _, era := range []string{"", "conway", "Future"} {
		if validTransactionEra(era) {
			t.Fatalf("unsupported era %q accepted", era)
		}
	}
	byron := validDecodedRecord()
	byron.Transaction.Era = "Byron"
	byron.Transaction.DeclaredFee = nil
	byron.Transaction.EffectiveFee = nil
	byron.Transaction.Outputs[0].Address = testByronAddress()
	byron.Transaction.Outputs[0].PaymentCredentialKind = "none"
	byron.Transaction.Outputs[0].PaymentCredentialHash = nil
	if err := validateDecodedTransaction(&byron); err != nil {
		t.Fatalf("ordinary Byron transaction: %v", err)
	}
	fee := uint64(1)
	byron.Transaction.DeclaredFee = &fee
	byron.Transaction.EffectiveFee = &fee
	if err := validateDecodedTransaction(&byron); !errors.Is(
		err,
		ErrConflictingRow,
	) {
		t.Fatalf("Byron fee error = %v", err)
	}
	nonByron := validDecodedRecord()
	nonByron.Transaction.DeclaredFee = nil
	nonByron.Transaction.EffectiveFee = nil
	if err := validateDecodedTransaction(&nonByron); !errors.Is(
		err,
		ErrConflictingRow,
	) {
		t.Fatalf("non-Byron nil fee error = %v", err)
	}
}

func TestOutputEraCapabilityBoundaries(t *testing.T) {
	t.Parallel()
	base := validDecodedRecord().Transaction.Outputs[0]
	policy := model.PolicyID{1}
	script := policy
	tests := []struct {
		name      string
		era       string
		synthetic bool
		mutate    func(*model.Output)
		valid     bool
	}{
		{"Shelley base", "Shelley", false, func(*model.Output) {}, true},
		{"Shelley asset", "Shelley", false, func(output *model.Output) {
			output.Assets = []model.OutputAsset{{PolicyID: policy, Name: model.Bytes{1}, Quantity: 1}}
		}, false},
		{"Mary asset", "Mary", false, func(output *model.Output) {
			output.Assets = []model.OutputAsset{{PolicyID: policy, Name: model.Bytes{1}, Quantity: 1}}
		}, true},
		{"Alonzo datum hash", "Alonzo", false, func(output *model.Output) {
			hash := repeatedHash(3)
			output.DatumKind = "hash"
			output.DatumHash = &hash
		}, true},
		{"Alonzo inline", "Alonzo", false, func(output *model.Output) {
			hash := calculateContentHash([]byte{1})
			output.DatumKind = "inline"
			output.DatumHash = &hash
			output.InlineDatumCBOR = model.Bytes{1}
		}, false},
		{"Babbage plutus v2", "Babbage", false, func(output *model.Output) {
			output.ReferenceScriptHash = &script
			output.ReferenceScriptLanguage = "plutus_v2"
		}, true},
		{"Babbage plutus v3", "Babbage", false, func(output *model.Output) {
			output.ReferenceScriptHash = &script
			output.ReferenceScriptLanguage = "plutus_v3"
		}, false},
		{"Conway plutus v3", "Conway", false, func(output *model.Output) {
			output.ReferenceScriptHash = &script
			output.ReferenceScriptLanguage = "plutus_v3"
		}, true},
		{"Dijkstra plutus v4", "Dijkstra", false, func(output *model.Output) {
			output.ReferenceScriptHash = &script
			output.ReferenceScriptLanguage = "plutus_v4"
		}, true},
		{"ordinary genesis kind", "Conway", false, func(output *model.Output) {
			output.Kind = model.OutputGenesis
		}, false},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			output := base
			test.mutate(&output)
			err := validateOutputEraCapabilities(
				output,
				test.era,
				test.synthetic,
			)
			if test.valid && err != nil {
				t.Fatal(err)
			}
			if !test.valid && !errors.Is(err, ErrConflictingRow) {
				t.Fatalf("error = %v", err)
			}
		})
	}
}
