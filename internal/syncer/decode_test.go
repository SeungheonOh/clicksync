package syncer

import (
	"testing"

	"cardano-clicksync/internal/model"
)

func TestFactRows(t *testing.T) {
	block := model.Block{
		Datums: make([]model.DatumBody, 2),
		Transactions: []model.Transaction{
			{
				Inputs:            make([]model.Input, 3),
				Outputs:           make([]model.Output, 4),
				DatumObservations: make([]model.DatumObservation, 5),
				Withdrawals:       make([]model.Withdrawal, 6),
				Redeemers:         make([]model.Redeemer, 7),
				Metadata:          &model.Metadata{},
			},
		},
	}
	if got, want := FactRows(block), uint64(30); got != want {
		t.Fatalf("FactRows = %d, want %d", got, want)
	}
}
