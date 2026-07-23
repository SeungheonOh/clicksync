package clickhouse

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/clicksync-project/clickout/internal/model"
)

func TestEveryAddressAndTracePhaseUsesThrowModeResourceSettings(t *testing.T) {
	t.Parallel()
	phases := []struct {
		name   string
		limits phaseLimits
	}{
		{"address_cursor_hash", resultPhaseLimits(1)},
		{"address_candidates", candidatePhaseLimits(101)},
		{"address_membership", hydrationPhaseLimits(100)},
		{"address_spends", hydrationPhaseLimits(200)},
		{"address_outputs", hydrationPhaseLimits(100)},
		{"inline_datums_batch", hydrationPhaseLimits(100)},
		{"trace_forward_candidates", candidatePhaseLimits(101)},
		{"trace_reverse_candidates", candidatePhaseLimits(101)},
		{"trace_transactions", traceHydrationPhaseLimits()},
		{"trace_consumed_inputs", traceHydrationPhaseLimits()},
		{"trace_output_values", traceHydrationPhaseLimits()},
		{"trace_produced_outputs", traceHydrationPhaseLimits()},
		{"trace_applied_withdrawals", traceHydrationPhaseLimits()},
	}
	for _, phase := range phases {
		phase := phase
		t.Run(phase.name, func(t *testing.T) {
			t.Parallel()
			settings := settingsForPhase(phase.limits, 30*time.Second)
			for _, key := range []string{
				"max_rows_to_read",
				"max_bytes_to_read",
				"max_result_rows",
				"max_result_bytes",
				"max_memory_usage",
			} {
				value, ok := settings[key]
				if !ok || value == uint64(0) {
					t.Fatalf("%s has no nonzero %s setting: %#v", phase.name, key, settings)
				}
			}
			if settings["read_overflow_mode"] != "throw" ||
				settings["result_overflow_mode"] != "throw" {
				t.Fatalf("%s does not use throw overflow modes: %#v", phase.name, settings)
			}
		})
	}
}

func TestLimitErrorsAreTypedWithTheirPhase(t *testing.T) {
	t.Parallel()
	err := mapQueryError(
		"trace_produced_outputs",
		errors.New("Memory limit (total) exceeded: max_memory_usage"),
	)
	if !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("limit error is not typed: %v", err)
	}
	var typed *ResourceLimitError
	if !errors.As(err, &typed) || typed.Phase != "trace_produced_outputs" {
		t.Fatalf("limit phase was lost: %#v", err)
	}
	ordinary := errors.New("network connection reset")
	if got := mapQueryError("trace_transactions", ordinary); got != ordinary {
		t.Fatalf("ordinary error was reclassified: %v", got)
	}
}

func TestTraceCandidateRowsMustBeStrictlySortedAndUnique(t *testing.T) {
	t.Parallel()
	for _, values := range [][]model.Hash32{
		{repeatedHash(2), repeatedHash(1)},
		{repeatedHash(1), repeatedHash(1)},
	} {
		rows := &fixedHashRows{values: values}
		if _, err := scanCandidateHashes(rows); err == nil {
			t.Fatalf("candidate sequence %#v was accepted", values)
		}
	}
	rows := &fixedHashRows{values: []model.Hash32{repeatedHash(1), repeatedHash(2)}}
	if values, err := scanCandidateHashes(rows); err != nil || len(values) != 2 {
		t.Fatalf("valid candidate sequence rejected: %#v, %v", values, err)
	}
}

func TestTraceCandidateSentinelDoesNotHydratePastBudget(t *testing.T) {
	t.Parallel()
	candidates := []model.Hash32{repeatedHash(1), repeatedHash(2)}
	accepted, truncated := acceptTraceCandidates(candidates, 1)
	if !truncated || len(accepted) != 1 || accepted[0] != candidates[0] {
		t.Fatalf("sentinel split = %#v truncated=%v", accepted, truncated)
	}
}

func TestHyperedgeNodeBudgetIsAtomicAndCountsUniqueUTxOs(t *testing.T) {
	t.Parallel()
	edge := model.FlowHyperedge{
		Transaction: repeatedHash(9),
		Inputs: []model.Spend{
			{Source: model.UTxORef{TxHash: repeatedHash(1), Index: 0}},
			{Source: model.UTxORef{TxHash: repeatedHash(1), Index: 0}, Role: model.InputReference},
		},
		ProducedOutputs: []model.Output{
			{Ref: model.UTxORef{TxHash: repeatedHash(9), Index: 0}},
			{Ref: model.UTxORef{TxHash: repeatedHash(9), Index: 1}},
		},
	}
	if err := validateHyperedgeResources(edge, 3); err != nil {
		t.Fatal(err)
	}
	err := validateHyperedgeResources(edge, 2)
	if !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("oversized edge was not rejected atomically: %v", err)
	}
}

func TestInputRowsUseRoleRankOrdinalAndStableTiebreakers(t *testing.T) {
	t.Parallel()
	tx := repeatedHash(9)
	block := repeatedHash(8)
	shared := model.UTxORef{TxHash: repeatedHash(1), Index: 0}
	rows := []model.Spend{
		{Source: shared, ConsumingTx: tx, ConsumingBlockHash: block, Role: model.InputRegular},
		{Source: shared, ConsumingTx: tx, ConsumingBlockHash: block, Role: model.InputCollateral},
		{Source: shared, ConsumingTx: tx, ConsumingBlockHash: block, Role: model.InputReference},
	}
	if err := validateSpendRows(rows, spendByTransaction); err != nil {
		t.Fatalf("legal same-ref role overlap rejected: %v", err)
	}
	if err := validateCompleteSpendRows(rows); err != nil {
		t.Fatalf("complete mixed-role inputs rejected: %v", err)
	}
	duplicateOrdinal := append([]model.Spend(nil), rows...)
	duplicateOrdinal = append(duplicateOrdinal, model.Spend{
		Source:      model.UTxORef{TxHash: repeatedHash(2), Index: 0},
		ConsumingTx: tx,
		Role:        model.InputReference,
		BodyOrdinal: 0,
	})
	if err := validateSpendRows(duplicateOrdinal, spendByTransaction); err == nil {
		t.Fatal("duplicate role ordinal was accepted")
	}
	outOfOrder := []model.Spend{rows[1], rows[0]}
	if err := validateSpendRows(outOfOrder, spendByTransaction); err == nil {
		t.Fatal("collateral-before-regular ordering was accepted")
	}
	for name, sql := range map[string]string{
		"transaction": inputsByTxSQL,
		"uses":        usesByRefSQL,
	} {
		for _, fragment := range []string{
			"i.role = 'regular'",
			"i.role = 'collateral'",
			"i.role = 'reference'",
			"i.source_tx_hash",
			"i.source_output_index",
		} {
			if !strings.Contains(sql, fragment) {
				t.Fatalf("%s input SQL omitted %q", name, fragment)
			}
		}
	}
}

func TestOutputRowsRejectDuplicateOrdinalsReferencesAndTies(t *testing.T) {
	t.Parallel()
	tx := repeatedHash(7)
	valid := []model.Output{
		{
			Ref:         model.UTxORef{TxHash: tx, Index: 0},
			ProducingTx: tx,
			BodyOrdinal: 0,
		},
		{
			Ref:         model.UTxORef{TxHash: tx, Index: 1},
			ProducingTx: tx,
			BodyOrdinal: 1,
		},
	}
	if err := validateOutputRows(valid); err != nil {
		t.Fatal(err)
	}
	duplicateOrdinal := append([]model.Output(nil), valid...)
	duplicateOrdinal[1].BodyOrdinal = 0
	if err := validateOutputRows(duplicateOrdinal); err == nil {
		t.Fatal("duplicate output body ordinal was accepted")
	}
	duplicateReference := append([]model.Output(nil), valid...)
	duplicateReference[1].Ref = duplicateReference[0].Ref
	if err := validateOutputRows(duplicateReference); err == nil {
		t.Fatal("duplicate output reference was accepted")
	}
	reversed := []model.Output{valid[1], valid[0]}
	if err := validateOutputRows(reversed); err == nil {
		t.Fatal("out-of-order output rows were accepted")
	}
	gap := append([]model.Output(nil), valid...)
	gap[1].BodyOrdinal = 2
	if err := validateCompleteOutputRows(gap); err == nil {
		t.Fatal("non-consecutive complete output ordinals were accepted")
	}
}

type fixedHashRows struct {
	values []model.Hash32
	index  int
}

func (rows *fixedHashRows) Next() bool {
	return rows.index < len(rows.values)
}

func (rows *fixedHashRows) Scan(dest ...any) error {
	raw := append([]byte(nil), rows.values[rows.index][:]...)
	rows.index++
	*(dest[0].(*[]byte)) = raw
	return nil
}

func (*fixedHashRows) Err() error {
	return nil
}
