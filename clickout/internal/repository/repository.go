package repository

import (
	"context"

	"github.com/clicksync-project/clickout/internal/limits"
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
	UTxOs                []model.UTxORef
	TruncationReason     model.TruncationReason
	ContinuationCursor   string
	ContinuationFrontier []model.UTxORef
}

type ExpansionBudget struct {
	KnownUTxOs          []model.UTxORef
	MaxEdges            uint32
	MaxNodes            uint32
	ExcludeTransactions []model.Hash32
}

type ExpansionResult struct {
	Hyperedges       []model.FlowHyperedge
	TruncationReason model.TruncationReason
}

func (result TraceSeedResult) Valid() bool {
	if !strictlySortedRefs(result.UTxOs) ||
		!strictlySortedRefs(result.ContinuationFrontier) ||
		len(result.UTxOs)+len(result.ContinuationFrontier) >
			int(limits.HardMaxTraceNodes) {
		return false
	}
	switch result.TruncationReason {
	case "":
		return result.ContinuationCursor == "" &&
			len(result.ContinuationFrontier) == 0
	case model.TruncationAddressSeedLimit:
		return result.ContinuationCursor != "" &&
			len(result.ContinuationFrontier) == 0
	case model.TruncationMaxNodes:
		return result.ContinuationCursor == "" &&
			len(result.ContinuationFrontier) > 0 &&
			(len(result.UTxOs) == 0 ||
				result.UTxOs[len(result.UTxOs)-1].String() <
					result.ContinuationFrontier[0].String())
	default:
		return false
	}
}

func strictlySortedRefs(refs []model.UTxORef) bool {
	previous := ""
	for _, ref := range refs {
		current := ref.String()
		if previous != "" && previous >= current {
			return false
		}
		previous = current
	}
	return true
}

func (budget ExpansionBudget) Valid() bool {
	if budget.MaxEdges == 0 ||
		budget.MaxEdges > limits.HardMaxTraceEdges ||
		budget.MaxNodes == 0 ||
		budget.MaxNodes > limits.HardMaxTraceNodes ||
		len(budget.KnownUTxOs) > int(budget.MaxNodes) ||
		len(budget.ExcludeTransactions) > int(budget.MaxEdges) {
		return false
	}
	if !strictlySortedRefs(budget.KnownUTxOs) {
		return false
	}
	previous := ""
	for _, hash := range budget.ExcludeTransactions {
		current := hash.String()
		if previous != "" && previous >= current {
			return false
		}
		previous = current
	}
	return true
}

func (result ExpansionResult) Valid() bool {
	return result.TruncationReason == "" ||
		result.TruncationReason == model.TruncationMaxNodes ||
		result.TruncationReason == model.TruncationMaxEdges
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
