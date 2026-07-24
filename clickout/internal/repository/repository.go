package repository

import (
	"context"

	"github.com/clicksync-project/clickout/internal/model"
)

type AddressQuery struct {
	Address []byte
	State   string
	Limit   uint32
	LastKey string
}

type TraceDirection string

const (
	Forward TraceDirection = "forward"
	Reverse TraceDirection = "reverse"
)

type TraceSeed struct {
	UTxO    *model.UTxORef
	Tx      *model.Hash32
	Address []byte
}

type TraceQuery struct {
	Direction   TraceDirection
	Seed        TraceSeed
	SeedLastKey string
	Asset       model.AssetSelector
}

type TraceSeedResult struct {
	UTxOs              []model.UTxORef
	Truncated          bool
	ContinuationCursor string
}

type ExpansionBudget struct {
	MaxEdges            uint32
	MaxNodes            uint32
	ExcludeTransactions []model.Hash32
}

type ExpansionResult struct {
	Hyperedges []model.FlowHyperedge
	Truncated  bool
}

// Reader is the complete Clickout/Clicksync boundary. Implementations consume
// only the documented ClickHouse schema and always receive the one immutable
// snapshot captured for the enclosing request.
type Reader interface {
	Snapshot(context.Context, model.AtPoint) (model.Snapshot, error)
	ValidateSnapshotBeforeRead(context.Context, model.Snapshot) (model.Snapshot, error)
	FinishSnapshot(context.Context, model.Snapshot) (model.Snapshot, error)
	UTxO(context.Context, model.Snapshot, model.UTxORef) (model.OutputState, []model.PartialHistoryBoundary, error)
	Transaction(context.Context, model.Snapshot, model.Hash32) (model.Transaction, []model.PartialHistoryBoundary, error)
	Address(context.Context, model.Snapshot, AddressQuery) (model.AddressPage, []model.PartialHistoryBoundary, error)
	Datum(context.Context, model.Snapshot, model.Hash32) (model.Datum, error)
	Redeemers(context.Context, model.Snapshot, model.Hash32) ([]model.Redeemer, []model.PartialHistoryBoundary, error)
	Metadata(context.Context, model.Snapshot, model.Hash32) (model.TransactionMetadata, error)
	Withdrawals(context.Context, model.Snapshot, model.Hash32) ([]model.Withdrawal, error)

	TraceSeeds(context.Context, model.Snapshot, TraceQuery, uint32) (TraceSeedResult, []model.PartialHistoryBoundary, error)
	ExpandForward(context.Context, model.Snapshot, []model.UTxORef, model.AssetSelector, ExpansionBudget) (ExpansionResult, []model.PartialHistoryBoundary, error)
	ExpandReverse(context.Context, model.Snapshot, []model.UTxORef, model.AssetSelector, ExpansionBudget) (ExpansionResult, []model.PartialHistoryBoundary, error)
}
