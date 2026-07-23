package app

import (
	"context"
	"errors"
	"testing"

	"github.com/clicksync-project/clickout/internal/cli"
	chstore "github.com/clicksync-project/clickout/internal/clickhouse"
	"github.com/clicksync-project/clickout/internal/cursor"
	"github.com/clicksync-project/clickout/internal/limits"
	"github.com/clicksync-project/clickout/internal/model"
	"github.com/clicksync-project/clickout/internal/repository"
)

func TestAddressCursorPinsPreviousSnapshot(t *testing.T) {
	t.Parallel()
	raw := []byte{0x01, 0xff, 0x80}
	encoded, err := cursor.Encode(cursor.Value{
		Scope:         chstore.AddressScope(raw, "current"),
		SnapshotEvent: 7,
		LastKey:       "opaque-repository-key",
	})
	if err != nil {
		t.Fatal(err)
	}
	reader := &fakeReader{
		snapshot: model.Snapshot{
			Event:           7,
			CompleteHistory: false,
			TrustMode:       model.TrustPeerObserved,
		},
	}
	result, err := New(reader).Execute(context.Background(), cli.Invocation{
		Command: "address",
		Address: "hex:01ff80",
		State:   "current",
		Limit:   1000,
		Cursor:  encoded,
		At:      model.AtPoint{Tip: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	response, ok := result.(model.Response[model.AddressPage])
	if !ok || response.Snapshot.Event != 7 {
		t.Fatalf("unexpected response: %#v", result)
	}
	if reader.snapshotCalls != 1 || reader.lastAt.Event == nil || *reader.lastAt.Event != 7 {
		t.Fatalf("cursor did not pin snapshot: calls=%d at=%#v", reader.snapshotCalls, reader.lastAt)
	}
	if reader.lastAddress.LastKey != "opaque-repository-key" ||
		reader.lastAddress.Snapshot.Event != 7 {
		t.Fatalf("snapshot/key not threaded to page: %#v", reader.lastAddress)
	}
}

func TestTraceAddressSeedCursorPinsSnapshotAndPassesRepositoryKey(t *testing.T) {
	t.Parallel()
	raw := []byte{0x61, 0x01}
	encoded, err := cursor.Encode(cursor.Value{
		Scope:         chstore.AddressScope(raw, "history"),
		SnapshotEvent: 17,
		LastKey:       "physical-address-key",
	})
	if err != nil {
		t.Fatal(err)
	}
	reader := &fakeReader{
		snapshot: model.Snapshot{
			Event:           17,
			CompleteHistory: false,
			TrustMode:       model.TrustPeerObserved,
		},
		seeds: repository.TraceSeedResult{
			Truncated:          true,
			ContinuationCursor: "next-seed-cursor",
		},
	}
	invocation := traceInvocation(ref(1, 0), limits.DefaultTrace())
	invocation.Trace.Seed = repository.TraceSeed{}
	invocation.Trace.Address = "hex:6101"
	invocation.Trace.SeedCursor = encoded
	result, err := New(reader).Execute(context.Background(), invocation)
	if err != nil {
		t.Fatal(err)
	}
	response := result.(model.Response[model.Trace])
	if reader.lastAt.Event == nil || *reader.lastAt.Event != 17 ||
		reader.lastTraceQuery.SeedLastKey != "physical-address-key" {
		t.Fatalf("seed cursor was not pinned/passed: at=%#v query=%#v", reader.lastAt, reader.lastTraceQuery)
	}
	if response.Truncation.ContinuationCursor != "next-seed-cursor" ||
		!response.Truncation.LosslessResume {
		t.Fatalf("seed continuation was not actionable: %#v", response.Truncation)
	}
}

func TestTraceAddressSeedBudgetDoesNotExceedNodeBudget(t *testing.T) {
	t.Parallel()
	traceLimits := limits.DefaultTrace()
	traceLimits.MaxNodes = 3
	reader := &fakeReader{
		snapshot: model.Snapshot{
			Event:           4,
			CompleteHistory: false,
			TrustMode:       model.TrustPeerObserved,
		},
	}
	invocation := traceInvocation(ref(1, 0), traceLimits)
	invocation.Trace.Seed = repository.TraceSeed{}
	invocation.Trace.Address = "hex:6101"
	if _, err := New(reader).Execute(context.Background(), invocation); err != nil {
		t.Fatal(err)
	}
	if reader.lastSeedLimit != 3 {
		t.Fatalf("address seed limit = %d, want node budget 3", reader.lastSeedLimit)
	}
}

func TestForwardBFSHandlesConvergenceAndCycle(t *testing.T) {
	t.Parallel()
	a := ref(1, 0)
	b := ref(2, 0)
	c := ref(3, 0)
	tx1 := hash(11)
	tx2 := hash(12)
	reader := &fakeReader{
		snapshot: model.Snapshot{
			Event:           9,
			CompleteHistory: true,
			TrustMode:       model.TrustPeerObserved,
		},
		seeds: repository.TraceSeedResult{UTxOs: []model.UTxORef{a}},
		expandForward: func(
			ctx context.Context,
			snapshot model.Snapshot,
			frontier []model.UTxORef,
		) ([]model.FlowHyperedge, error) {
			if snapshot.Event != 9 {
				t.Fatalf("layer used snapshot %d", snapshot.Event)
			}
			switch frontier[0] {
			case a:
				return []model.FlowHyperedge{{
					Transaction:     tx1,
					ProducedOutputs: []model.Output{adaOutput(b), adaOutput(c)},
				}}, nil
			case b:
				return []model.FlowHyperedge{{
					Transaction:     tx2,
					ProducedOutputs: []model.Output{adaOutput(a)},
				}}, nil
			default:
				return []model.FlowHyperedge{{
					Transaction:     tx2,
					ProducedOutputs: []model.Output{adaOutput(a)},
				}}, nil
			}
		},
	}
	invocation := traceInvocation(a, limits.DefaultTrace())
	result, err := New(reader).Execute(context.Background(), invocation)
	if err != nil {
		t.Fatal(err)
	}
	response, ok := result.(model.Response[model.Trace])
	if !ok {
		t.Fatalf("unexpected response type %T", result)
	}
	if response.Data.Visited != 3 || len(response.Data.Hyperedges) != 2 ||
		response.Truncation.Truncated || reader.snapshotCalls != 1 {
		t.Fatalf("unexpected convergent trace: %#v", response)
	}
}

func TestTraceNodeCapIsExplicitlyNotLossless(t *testing.T) {
	t.Parallel()
	a := ref(1, 0)
	b := ref(2, 0)
	traceLimits := limits.DefaultTrace()
	traceLimits.MaxNodes = 1
	reader := &fakeReader{
		snapshot: model.Snapshot{
			Event:           5,
			CompleteHistory: false,
			TrustMode:       model.TrustPeerObserved,
		},
		seeds: repository.TraceSeedResult{UTxOs: []model.UTxORef{a}},
		expandForward: func(
			context.Context,
			model.Snapshot,
			[]model.UTxORef,
		) ([]model.FlowHyperedge, error) {
			return []model.FlowHyperedge{{
				Transaction:     hash(9),
				ProducedOutputs: []model.Output{adaOutput(b)},
			}}, nil
		},
	}
	result, err := New(reader).Execute(context.Background(), traceInvocation(a, traceLimits))
	if err != nil {
		t.Fatal(err)
	}
	response := result.(model.Response[model.Trace])
	if !response.Truncation.Truncated || response.Truncation.Reason != "max_nodes" ||
		response.Truncation.LosslessResume ||
		len(response.Truncation.ContinuationFrontier) != 1 ||
		response.Truncation.ContinuationFrontier[0] != b {
		t.Fatalf("unexpected node-cap response: %#v", response.Truncation)
	}
}

func TestTraceEdgeCapIsDeterministicAndExplicit(t *testing.T) {
	t.Parallel()
	a := ref(1, 0)
	b := ref(2, 0)
	traceLimits := limits.DefaultTrace()
	traceLimits.MaxEdges = 1
	reader := &fakeReader{
		snapshot: model.Snapshot{
			Event:           5,
			CompleteHistory: false,
			TrustMode:       model.TrustPeerObserved,
		},
		seeds: repository.TraceSeedResult{UTxOs: []model.UTxORef{b, a}},
		expandForward: func(
			context.Context,
			model.Snapshot,
			[]model.UTxORef,
		) ([]model.FlowHyperedge, error) {
			return []model.FlowHyperedge{{Transaction: hash(8)}}, nil
		},
		forwardTruncated: true,
	}
	result, err := New(reader).Execute(context.Background(), traceInvocation(a, traceLimits))
	if err != nil {
		t.Fatal(err)
	}
	response := result.(model.Response[model.Trace])
	if !response.Truncation.Truncated || response.Truncation.Reason != "max_edges" ||
		response.Truncation.LosslessResume ||
		len(response.Data.Hyperedges) != 1 ||
		response.Data.Hyperedges[0].Transaction != hash(8) ||
		len(response.Truncation.ContinuationFrontier) != 2 ||
		response.Truncation.ContinuationFrontier[0] != a ||
		response.Truncation.ContinuationFrontier[1] != b {
		t.Fatalf("unexpected edge-cap response: %#v", response)
	}
	if len(reader.expansionBudgets) != 1 ||
		reader.expansionBudgets[0].MaxEdges != 1 {
		t.Fatalf("repository did not receive one-edge budget: %#v", reader.expansionBudgets)
	}
}

func TestTraceMultiBatchPassesRemainingBudgetsAndExclusions(t *testing.T) {
	t.Parallel()
	a := ref(1, 0)
	b := ref(2, 0)
	traceLimits := limits.DefaultTrace()
	traceLimits.MaxDepth = 1
	traceLimits.MaxEdges = 3
	traceLimits.FrontierBatch = 1
	reader := &fakeReader{
		snapshot: model.Snapshot{
			Event:           5,
			CompleteHistory: false,
			TrustMode:       model.TrustPeerObserved,
		},
		seeds: repository.TraceSeedResult{UTxOs: []model.UTxORef{a, b}},
		expandForward: func(
			_ context.Context,
			_ model.Snapshot,
			frontier []model.UTxORef,
		) ([]model.FlowHyperedge, error) {
			return []model.FlowHyperedge{{Transaction: hash(frontier[0].TxHash[0] + 10)}}, nil
		},
	}
	if _, err := New(reader).Execute(
		context.Background(),
		traceInvocation(a, traceLimits),
	); err != nil {
		t.Fatal(err)
	}
	if len(reader.expansionBudgets) != 2 ||
		reader.expansionBudgets[0].MaxEdges != 3 ||
		len(reader.expansionBudgets[0].ExcludeTransactions) != 0 ||
		reader.expansionBudgets[1].MaxEdges != 2 ||
		len(reader.expansionBudgets[1].ExcludeTransactions) != 1 {
		t.Fatalf("remaining budgets/exclusions = %#v", reader.expansionBudgets)
	}
}

func TestReverseBFSUsesConsumedSourceValues(t *testing.T) {
	t.Parallel()
	target := ref(3, 0)
	sourceA := ref(1, 0)
	sourceB := ref(2, 0)
	reader := &fakeReader{
		snapshot: model.Snapshot{
			Event:           7,
			CompleteHistory: true,
			TrustMode:       model.TrustPeerObserved,
		},
		seeds: repository.TraceSeedResult{UTxOs: []model.UTxORef{target}},
		expandReverse: func(
			_ context.Context,
			snapshot model.Snapshot,
			frontier []model.UTxORef,
		) ([]model.FlowHyperedge, error) {
			if snapshot.Event != 7 || len(frontier) != 1 || frontier[0] != target {
				t.Fatalf("unexpected reverse frontier: %#v at %#v", frontier, snapshot)
			}
			return []model.FlowHyperedge{{
				Transaction: hash(9),
				ConsumedInputValues: []model.Output{
					adaOutput(sourceB),
					adaOutput(sourceA),
				},
				ProducedOutputs: []model.Output{adaOutput(target)},
			}}, nil
		},
	}
	invocation := traceInvocation(target, limits.DefaultTrace())
	invocation.Trace.Direction = repository.Reverse
	invocation.Trace.Limits.MaxDepth = 1
	result, err := New(reader).Execute(context.Background(), invocation)
	if err != nil {
		t.Fatal(err)
	}
	response := result.(model.Response[model.Trace])
	if response.Data.Visited != 3 || len(response.Data.Hyperedges) != 1 ||
		!response.Truncation.Truncated || response.Truncation.Reason != "max_depth" ||
		len(response.Truncation.ContinuationFrontier) != 2 ||
		response.Truncation.ContinuationFrontier[0] != sourceA ||
		response.Truncation.ContinuationFrontier[1] != sourceB {
		t.Fatalf("unexpected reverse trace: %#v", response)
	}
}

func TestCallerCancellationIsNotReportedAsLayerTimeout(t *testing.T) {
	t.Parallel()
	a := ref(1, 0)
	reader := &fakeReader{
		snapshot: model.Snapshot{
			Event:           5,
			CompleteHistory: false,
			TrustMode:       model.TrustPeerObserved,
		},
		seeds: repository.TraceSeedResult{UTxOs: []model.UTxORef{a}},
		expandForward: func(
			ctx context.Context,
			_ model.Snapshot,
			_ []model.UTxORef,
		) ([]model.FlowHyperedge, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := New(reader).Execute(ctx, traceInvocation(a, limits.DefaultTrace())); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected caller cancellation, got %v", err)
	}
}

func traceInvocation(seed model.UTxORef, traceLimits limits.Trace) cli.Invocation {
	return cli.Invocation{
		Command: "trace",
		At:      model.AtPoint{Tip: true},
		Trace: cli.TraceInvocation{
			Direction: repository.Forward,
			Seed:      repository.TraceSeed{UTxO: &seed},
			Asset:     model.AssetSelector{ADA: true},
			Limits:    traceLimits,
			Format:    "jsonl",
		},
	}
}

func hash(value byte) model.Hash32 {
	var result model.Hash32
	for index := range result {
		result[index] = value
	}
	return result
}

func ref(value byte, index uint32) model.UTxORef {
	return model.UTxORef{TxHash: hash(value), Index: index}
}

func adaOutput(reference model.UTxORef) model.Output {
	return model.Output{Ref: reference, ProducingTx: reference.TxHash, Lovelace: 1}
}

type addressCall struct {
	Snapshot model.Snapshot
	repository.AddressQuery
}

type fakeReader struct {
	snapshot         model.Snapshot
	snapshotCalls    int
	lastAt           model.AtPoint
	lastAddress      addressCall
	seeds            repository.TraceSeedResult
	lastTraceQuery   repository.TraceQuery
	lastSeedLimit    uint32
	expansionBudgets []repository.ExpansionBudget
	forwardTruncated bool
	expandForward    func(context.Context, model.Snapshot, []model.UTxORef) ([]model.FlowHyperedge, error)
	expandReverse    func(context.Context, model.Snapshot, []model.UTxORef) ([]model.FlowHyperedge, error)
}

func (reader *fakeReader) Snapshot(_ context.Context, at model.AtPoint) (model.Snapshot, error) {
	reader.snapshotCalls++
	reader.lastAt = at
	return reader.snapshot, nil
}

func (reader *fakeReader) UTxO(
	context.Context,
	model.Snapshot,
	model.UTxORef,
) (model.OutputState, []model.PartialHistoryBoundary, error) {
	return model.OutputState{}, nil, nil
}

func (reader *fakeReader) Transaction(
	context.Context,
	model.Snapshot,
	model.Hash32,
) (model.Transaction, []model.PartialHistoryBoundary, error) {
	return model.Transaction{}, nil, nil
}

func (reader *fakeReader) Address(
	_ context.Context,
	snapshot model.Snapshot,
	query repository.AddressQuery,
) (model.AddressPage, []model.PartialHistoryBoundary, error) {
	reader.lastAddress = addressCall{Snapshot: snapshot, AddressQuery: query}
	return model.AddressPage{
		Address: model.Bytes(query.Address),
		State:   query.State,
		Items:   make([]model.OutputState, 0),
	}, nil, nil
}

func (reader *fakeReader) Datum(context.Context, model.Snapshot, model.Hash32) (model.Datum, error) {
	return model.Datum{}, nil
}

func (reader *fakeReader) Redeemers(
	context.Context,
	model.Snapshot,
	model.Hash32,
) ([]model.Redeemer, []model.PartialHistoryBoundary, error) {
	return nil, nil, nil
}

func (reader *fakeReader) Metadata(
	context.Context,
	model.Snapshot,
	model.Hash32,
) (model.TransactionMetadata, error) {
	return model.TransactionMetadata{}, nil
}

func (reader *fakeReader) Withdrawals(
	context.Context,
	model.Snapshot,
	model.Hash32,
) ([]model.Withdrawal, error) {
	return nil, nil
}

func (reader *fakeReader) TraceSeeds(
	_ context.Context,
	_ model.Snapshot,
	query repository.TraceQuery,
	limit uint32,
) (repository.TraceSeedResult, []model.PartialHistoryBoundary, error) {
	reader.lastTraceQuery = query
	reader.lastSeedLimit = limit
	return reader.seeds, nil, nil
}

func (reader *fakeReader) ExpandForward(
	ctx context.Context,
	snapshot model.Snapshot,
	frontier []model.UTxORef,
	_ model.AssetSelector,
	budget repository.ExpansionBudget,
) (repository.ExpansionResult, []model.PartialHistoryBoundary, error) {
	reader.expansionBudgets = append(reader.expansionBudgets, budget)
	edges, err := reader.expandForward(ctx, snapshot, frontier)
	return repository.ExpansionResult{
		Hyperedges: edges,
		Truncated:  reader.forwardTruncated,
	}, nil, err
}

func (reader *fakeReader) ExpandReverse(
	ctx context.Context,
	snapshot model.Snapshot,
	frontier []model.UTxORef,
	_ model.AssetSelector,
	budget repository.ExpansionBudget,
) (repository.ExpansionResult, []model.PartialHistoryBoundary, error) {
	reader.expansionBudgets = append(reader.expansionBudgets, budget)
	if reader.expandReverse == nil {
		return repository.ExpansionResult{}, nil, nil
	}
	edges, err := reader.expandReverse(ctx, snapshot, frontier)
	return repository.ExpansionResult{Hyperedges: edges}, nil, err
}
