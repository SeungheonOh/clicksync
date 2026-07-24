package repository

import (
	"testing"

	"github.com/clicksync-project/clickout/internal/model"
)

func TestExpansionBudgetRequiresGlobalSortedUniqueSets(t *testing.T) {
	t.Parallel()
	first := model.UTxORef{TxHash: model.Hash32{1}, Index: 1}
	second := model.UTxORef{TxHash: model.Hash32{2}, Index: 1}
	valid := ExpansionBudget{
		KnownUTxOs:          []model.UTxORef{first, second},
		MaxEdges:            10,
		MaxNodes:            20,
		ExcludeTransactions: []model.Hash32{{3}, {4}},
	}
	if !valid.Valid() {
		t.Fatal("valid expansion budget rejected")
	}
	for _, invalid := range []ExpansionBudget{
		{MaxNodes: 1},
		{MaxEdges: 1},
		{
			MaxEdges: 10,
			MaxNodes: 100_001,
		},
		{
			MaxEdges: 100_001,
			MaxNodes: 10,
		},
		{
			KnownUTxOs: []model.UTxORef{first, second},
			MaxEdges:   1,
			MaxNodes:   1,
		},
		{
			MaxEdges:            1,
			MaxNodes:            1,
			ExcludeTransactions: []model.Hash32{{3}, {4}},
		},
		{
			KnownUTxOs: []model.UTxORef{second, first},
			MaxEdges:   1,
			MaxNodes:   1,
		},
		{
			KnownUTxOs: []model.UTxORef{first, first},
			MaxEdges:   1,
			MaxNodes:   1,
		},
		{
			MaxEdges:            1,
			MaxNodes:            1,
			ExcludeTransactions: []model.Hash32{{4}, {3}},
		},
		{
			MaxEdges:            1,
			MaxNodes:            1,
			ExcludeTransactions: []model.Hash32{{3}, {3}},
		},
	} {
		if invalid.Valid() {
			t.Errorf("invalid expansion budget accepted: %#v", invalid)
		}
	}
}

func TestRepositoryResultsCarryTypedTruncationReasons(t *testing.T) {
	t.Parallel()
	first := model.UTxORef{TxHash: model.Hash32{1}}
	second := model.UTxORef{TxHash: model.Hash32{2}}
	if !(TraceSeedResult{}).Valid() ||
		!(TraceSeedResult{
			UTxOs: []model.UTxORef{first, second},
		}).Valid() ||
		!(TraceSeedResult{
			TruncationReason:   model.TruncationAddressSeedLimit,
			ContinuationCursor: "cursor",
		}).Valid() ||
		!(TraceSeedResult{
			UTxOs:                []model.UTxORef{first},
			TruncationReason:     model.TruncationMaxNodes,
			ContinuationFrontier: []model.UTxORef{second},
		}).Valid() ||
		(TraceSeedResult{
			TruncationReason: model.TruncationMaxEdges,
		}).Valid() ||
		(TraceSeedResult{
			TruncationReason:   model.TruncationAddressSeedLimit,
			ContinuationCursor: "",
		}).Valid() ||
		(TraceSeedResult{
			ContinuationCursor: "cursor",
		}).Valid() ||
		(TraceSeedResult{
			UTxOs: []model.UTxORef{second, first},
		}).Valid() ||
		(TraceSeedResult{
			UTxOs:                []model.UTxORef{second},
			TruncationReason:     model.TruncationMaxNodes,
			ContinuationFrontier: []model.UTxORef{first},
		}).Valid() ||
		(TraceSeedResult{
			UTxOs:                []model.UTxORef{first},
			TruncationReason:     model.TruncationAddressSeedLimit,
			ContinuationCursor:   "cursor",
			ContinuationFrontier: []model.UTxORef{second},
		}).Valid() {
		t.Fatal("trace seed truncation validation mismatch")
	}
	if !(ExpansionResult{}).Valid() ||
		!(ExpansionResult{
			TruncationReason: model.TruncationMaxNodes,
		}).Valid() ||
		!(ExpansionResult{
			TruncationReason: model.TruncationMaxEdges,
		}).Valid() ||
		(ExpansionResult{
			TruncationReason: model.TruncationLayerTimeout,
		}).Valid() {
		t.Fatal("expansion truncation validation mismatch")
	}
}
