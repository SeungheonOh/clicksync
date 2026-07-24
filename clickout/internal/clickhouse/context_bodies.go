package clickhouse

import (
	"bytes"
	"fmt"

	"github.com/clicksync-project/clickout/internal/model"
)

const (
	maximumMaterializedContextBytes = 64 * 1024 * 1024
	maximumContextOccurrenceBytes   = 64 * 1024 * 1024
)

// contextBodyPool owns and interns the immutable CBOR fragments retained by
// one decoded response. The same verified content hash is projected into
// datum observations, outputs, and redeemer targets without cloning its body.
type contextBodyPool struct {
	bodies map[string]model.Bytes
	total  uint64
}

type contextOccurrenceBudget struct {
	encodedBytes uint64
}

func base64PayloadBytes(rawBytes uint64) (uint64, bool) {
	if rawBytes > ^uint64(0)-2 {
		return 0, false
	}
	groups := (rawBytes + 2) / 3
	if groups > ^uint64(0)/4 {
		return 0, false
	}
	return groups * 4, true
}

func (budget *contextOccurrenceBudget) add(
	phase string,
	rawBytes uint64,
	placements uint64,
) error {
	encoded, ok := base64PayloadBytes(rawBytes)
	if !ok || (encoded != 0 && placements > ^uint64(0)/encoded) {
		return &ResourceLimitError{
			Phase: phase,
			Cause: fmt.Errorf("context occurrence byte accounting overflows"),
		}
	}
	added := encoded * placements
	if added > maximumContextOccurrenceBytes-budget.encodedBytes {
		return &ResourceLimitError{
			Phase: phase,
			Cause: fmt.Errorf(
				"serialized context body occurrences exceed %d bytes",
				maximumContextOccurrenceBytes,
			),
		}
	}
	budget.encodedBytes += added
	return nil
}

func validateTransactionContextOccurrences(
	transactions []model.Transaction,
) error {
	var budget contextOccurrenceBudget
	const phase = "transaction_context_occurrences"
	for _, transaction := range transactions {
		for _, redeemer := range transaction.Redeemers {
			if err := budget.add(
				phase,
				uint64(len(redeemer.DataCBOR)),
				1,
			); err != nil {
				return err
			}
			if output := redeemer.Target.SourceOutput; output != nil {
				if err := addOutputContextOccurrence(
					&budget,
					phase,
					*output,
				); err != nil {
					return err
				}
			}
		}
		if transaction.Metadata != nil {
			if err := budget.add(
				phase,
				uint64(len(transaction.Metadata.MapCBOR)),
				1,
			); err != nil {
				return err
			}
		}
		for _, datum := range transaction.Datums {
			if err := budget.add(
				phase,
				uint64(len(datum.BodyCBOR)),
				1,
			); err != nil {
				return err
			}
		}
		for _, output := range transaction.Outputs {
			if err := addOutputContextOccurrence(
				&budget,
				phase,
				output,
			); err != nil {
				return err
			}
		}
		for _, input := range transaction.Inputs {
			if input.SourceOutput == nil {
				continue
			}
			if err := addOutputContextOccurrence(
				&budget,
				phase,
				*input.SourceOutput,
			); err != nil {
				return err
			}
		}
	}
	return nil
}

func addOutputContextOccurrence(
	budget *contextOccurrenceBudget,
	phase string,
	output model.Output,
) error {
	if output.DatumKind != "inline" {
		return nil
	}
	return budget.add(
		phase,
		uint64(len(output.InlineDatumCBOR)),
		1,
	)
}

func validateOutputContextOccurrences(
	phase string,
	outputs []model.Output,
) error {
	var budget contextOccurrenceBudget
	for _, output := range outputs {
		if err := addOutputContextOccurrence(
			&budget,
			phase,
			output,
		); err != nil {
			return err
		}
	}
	return nil
}

func addFlowHyperedgeContextOccurrences(
	budget *contextOccurrenceBudget,
	phase string,
	edge model.FlowHyperedge,
) error {
	for _, redeemer := range edge.Transaction.Redeemers {
		if err := budget.add(
			phase,
			uint64(len(redeemer.DataCBOR)),
			1,
		); err != nil {
			return err
		}
		if redeemer.Target.SourceOutput != nil {
			if err := addOutputContextOccurrence(
				budget,
				phase,
				*redeemer.Target.SourceOutput,
			); err != nil {
				return err
			}
		}
	}
	if edge.Transaction.Metadata != nil {
		if err := budget.add(
			phase,
			uint64(len(edge.Transaction.Metadata.MapCBOR)),
			1,
		); err != nil {
			return err
		}
	}
	for _, datum := range edge.Transaction.Datums {
		if err := budget.add(
			phase,
			uint64(len(datum.BodyCBOR)),
			1,
		); err != nil {
			return err
		}
	}
	for _, output := range edge.Outputs {
		if err := addOutputContextOccurrence(
			budget,
			phase,
			output,
		); err != nil {
			return err
		}
	}
	for _, input := range edge.Inputs {
		if input.SourceOutput == nil {
			continue
		}
		if err := addOutputContextOccurrence(
			budget,
			phase,
			*input.SourceOutput,
		); err != nil {
			return err
		}
	}
	return nil
}

func validateOutputStateContextOccurrences(state model.OutputState) error {
	var budget contextOccurrenceBudget
	const phase = "utxo_context_occurrences"
	if err := addOutputContextOccurrence(
		&budget,
		phase,
		state.Output,
	); err != nil {
		return err
	}
	if state.Consumption != nil {
		return addFlowHyperedgeContextOccurrences(
			&budget,
			phase,
			*state.Consumption,
		)
	}
	return nil
}

func newContextBodyPool() *contextBodyPool {
	return &contextBodyPool{bodies: make(map[string]model.Bytes)}
}

func (pool *contextBodyPool) retain(
	phase string,
	hash model.Hash32,
	body model.Bytes,
) (model.Bytes, error) {
	key := hash.String()
	if retained, exists := pool.bodies[key]; exists {
		if !bytes.Equal(retained, body) {
			return nil, fmt.Errorf(
				"%w: context bodies share a hash but differ",
				ErrConflictingRow,
			)
		}
		return retained, nil
	}
	size := uint64(len(body))
	if size > maximumMaterializedContextBytes-pool.total {
		return nil, &ResourceLimitError{
			Phase: phase,
			Cause: fmt.Errorf(
				"unique retained context bodies exceed %d bytes",
				maximumMaterializedContextBytes,
			),
		}
	}
	pool.total += size
	pool.bodies[key] = body
	return body, nil
}
