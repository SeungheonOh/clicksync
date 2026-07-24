package app

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

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
	snapshot := testSnapshot(7, false)
	encoded, err := cursor.Encode(cursor.Value{
		Scope:    chstore.AddressScope(raw, "current"),
		Snapshot: snapshot,
		LastKey:  "opaque-repository-key",
	})
	if err != nil {
		t.Fatal(err)
	}
	reader := &fakeReader{
		snapshot: snapshot,
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
	if !ok || !response.Snapshot.SamePin(snapshot) {
		t.Fatalf("unexpected response: %#v", result)
	}
	if reader.snapshotCalls != 0 || reader.preCalls != 1 ||
		reader.finishCalls != 1 {
		t.Fatalf(
			"cursor acquired instead of validating: snapshot=%d pre=%d finish=%d",
			reader.snapshotCalls,
			reader.preCalls,
			reader.finishCalls,
		)
	}
	if reader.lastAddress.LastKey != "opaque-repository-key" ||
		!reader.lastAddress.Snapshot.SamePin(snapshot) {
		t.Fatalf("snapshot/key not threaded to page: %#v", reader.lastAddress)
	}
}

func TestTraceAddressSeedCursorPinsSnapshotAndPassesRepositoryKey(t *testing.T) {
	t.Parallel()
	raw := []byte{0x61, 0x01}
	snapshot := testSnapshot(17, false)
	scope := chstore.TraceAddressScope(
		raw,
		repository.Forward,
		model.AssetSelector{ADA: true},
	)
	encoded, err := cursor.Encode(cursor.Value{
		Scope:    scope,
		Snapshot: snapshot,
		LastKey:  "physical-address-key",
	})
	if err != nil {
		t.Fatal(err)
	}
	next, err := cursor.Encode(cursor.Value{
		Scope:    scope,
		Snapshot: snapshot,
		LastKey:  "next-key",
	})
	if err != nil {
		t.Fatal(err)
	}
	reader := &fakeReader{
		snapshot: snapshot,
		seeds: repository.TraceSeedResult{
			Truncated:          true,
			ContinuationCursor: next,
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
	if reader.snapshotCalls != 0 || reader.preCalls != 1 ||
		reader.finishCalls != 1 ||
		reader.lastTraceQuery.SeedLastKey != "physical-address-key" {
		t.Fatalf(
			"seed cursor lifecycle/query = snapshot:%d pre:%d finish:%d query:%#v",
			reader.snapshotCalls,
			reader.preCalls,
			reader.finishCalls,
			reader.lastTraceQuery,
		)
	}
	if response.Truncation.ContinuationCursor == "" ||
		response.Truncation.LosslessResume {
		t.Fatalf("seed continuation was not actionable: %#v", response.Truncation)
	}
}

func TestTraceAddressSeedBudgetDoesNotExceedNodeBudget(t *testing.T) {
	t.Parallel()
	traceLimits := limits.DefaultTrace()
	traceLimits.MaxNodes = 3
	reader := &fakeReader{
		snapshot: testSnapshot(4, false),
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
		snapshot: testSnapshot(9, true),
		seeds:    repository.TraceSeedResult{UTxOs: []model.UTxORef{a}},
		expandForward: func(
			ctx context.Context,
			snapshot model.Snapshot,
			frontier []model.UTxORef,
		) ([]model.FlowHyperedge, error) {
			if snapshot.QueryHead.EventSeq != 9 {
				t.Fatalf("layer used snapshot %d", snapshot.QueryHead.EventSeq)
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
		snapshot: testSnapshot(5, false),
		seeds:    repository.TraceSeedResult{UTxOs: []model.UTxORef{a}},
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
		snapshot: testSnapshot(5, false),
		seeds:    repository.TraceSeedResult{UTxOs: []model.UTxORef{b, a}},
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
		snapshot: testSnapshot(5, false),
		seeds:    repository.TraceSeedResult{UTxOs: []model.UTxORef{a, b}},
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
		snapshot: testSnapshot(7, true),
		seeds:    repository.TraceSeedResult{UTxOs: []model.UTxORef{target}},
		expandReverse: func(
			_ context.Context,
			snapshot model.Snapshot,
			frontier []model.UTxORef,
		) ([]model.FlowHyperedge, error) {
			if snapshot.QueryHead.EventSeq != 7 ||
				len(frontier) != 1 ||
				frontier[0] != target {
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
		snapshot: testSnapshot(5, false),
		seeds:    repository.TraceSeedResult{UTxOs: []model.UTxORef{a}},
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

func TestEngineLifecycleOrderFreshResumeAndTrace(t *testing.T) {
	t.Parallel()
	snapshot := testSnapshot(12, false)
	hashValue := hash(0x42)
	fresh := &fakeReader{snapshot: snapshot}
	if _, err := New(fresh).Execute(context.Background(), cli.Invocation{
		Command: "datum",
		At:      model.AtPoint{Tip: true},
		Hash:    &hashValue,
	}); err != nil {
		t.Fatal(err)
	}
	assertOrder(t, fresh.order, "snapshot", "pre", "datum", "finish")

	raw := []byte{0x61, 0x01}
	encoded, err := cursor.Encode(cursor.Value{
		Scope:    chstore.AddressScope(raw, "history"),
		Snapshot: snapshot,
		LastKey:  "resume",
	})
	if err != nil {
		t.Fatal(err)
	}
	resume := &fakeReader{snapshot: snapshot}
	if _, err := New(resume).Execute(context.Background(), cli.Invocation{
		Command: "address",
		Address: "hex:6101",
		State:   "history",
		Limit:   10,
		Cursor:  encoded,
	}); err != nil {
		t.Fatal(err)
	}
	assertOrder(t, resume.order, "pre", "address", "finish")

	seed := ref(1, 0)
	traceReader := &fakeReader{
		snapshot: snapshot,
		seeds: repository.TraceSeedResult{
			UTxOs: []model.UTxORef{seed},
		},
		expandForward: func(
			context.Context,
			model.Snapshot,
			[]model.UTxORef,
		) ([]model.FlowHyperedge, error) {
			return nil, nil
		},
	}
	traceLimits := limits.DefaultTrace()
	traceLimits.MaxDepth = 1
	if _, err := New(traceReader).Execute(
		context.Background(),
		traceInvocation(seed, traceLimits),
	); err != nil {
		t.Fatal(err)
	}
	assertOrder(
		t,
		traceReader.order,
		"snapshot",
		"pre",
		"seeds",
		"forward",
		"finish",
	)
}

func TestEngineUsesCanonicalPreAndFinishDiagnostics(t *testing.T) {
	t.Parallel()
	snapshot := testSnapshot(13, false)
	pre := snapshot
	pre.Diagnostics.TrustStatus = "checking"
	pre.Diagnostics.TrustBasis = "primary_only"
	pre.Diagnostics.TrustReason = "pre"
	finished := snapshot
	finished.Diagnostics.TrustReason = "finish"
	reader := &fakeReader{
		snapshot: snapshot,
		pre: func(model.Snapshot) (model.Snapshot, error) {
			return pre, nil
		},
		finish: func(model.Snapshot) (model.Snapshot, error) {
			return finished, nil
		},
	}
	hashValue := hash(0x43)
	result, err := New(reader).Execute(
		context.Background(),
		cli.Invocation{
			Command: "datum",
			At:      model.AtPoint{Tip: true},
			Hash:    &hashValue,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	response := result.(model.Response[model.Datum])
	if reader.datumSnapshot.Diagnostics != pre.Diagnostics ||
		response.Snapshot.Diagnostics != finished.Diagnostics {
		t.Fatalf(
			"diagnostics pre=%+v fact=%+v finish=%+v response=%+v",
			pre.Diagnostics,
			reader.datumSnapshot.Diagnostics,
			finished.Diagnostics,
			response.Snapshot.Diagnostics,
		)
	}
}

func TestAddressCursorDiagnosticsAreCanonicalizedEndToEnd(t *testing.T) {
	t.Parallel()
	raw := []byte{0x61, 0x02}
	canonical := testSnapshot(14, false)
	forged := canonical
	forged.Diagnostics.Physical.EventSeq += 100
	forged.Diagnostics.Physical.Point = model.Point{
		Slot:        999,
		Hash:        hash(0x66),
		BlockNumber: 999,
	}
	forged.Diagnostics.TrustStatus = "checking"
	forged.Diagnostics.TrustBasis = "primary_only"
	forged.Diagnostics.TrustReason = "forged"
	encoded, err := cursor.Encode(cursor.Value{
		Scope:    chstore.AddressScope(raw, "current"),
		Snapshot: forged,
		LastKey:  "resume",
	})
	if err != nil {
		t.Fatal(err)
	}
	pre := canonical
	pre.Diagnostics.TrustReason = "pre canonical"
	finished := canonical
	finished.Diagnostics.TrustReason = "finish canonical"
	reader := &fakeReader{
		snapshot:         canonical,
		addressCursorKey: "next",
		pre: func(value model.Snapshot) (model.Snapshot, error) {
			if !value.SamePin(canonical) {
				return model.Snapshot{}, cursor.ErrInvalid
			}
			return pre, nil
		},
		finish: func(value model.Snapshot) (model.Snapshot, error) {
			if !value.SamePin(canonical) {
				return model.Snapshot{}, cursor.ErrInvalid
			}
			return finished, nil
		},
	}
	result, err := New(reader).Execute(context.Background(), cli.Invocation{
		Command: "address",
		Address: "hex:6102",
		State:   "current",
		Limit:   10,
		Cursor:  encoded,
	})
	if err != nil {
		t.Fatal(err)
	}
	response := result.(model.Response[model.AddressPage])
	if reader.lastAddress.Snapshot.Diagnostics != pre.Diagnostics ||
		response.Snapshot.Diagnostics != finished.Diagnostics {
		t.Fatalf("canonical diagnostics were not threaded: %#v", response)
	}
	next, err := cursor.Decode(
		response.Data.Cursor,
		chstore.AddressScope(raw, "current"),
	)
	if err != nil {
		t.Fatal(err)
	}
	if next.Snapshot.Diagnostics != finished.Diagnostics ||
		!next.Snapshot.SamePin(canonical) {
		t.Fatalf("next cursor diagnostics/pin = %+v", next.Snapshot)
	}
	assertOrder(t, reader.order, "pre", "address", "finish")
}

func TestRecomputedCursorPinMutationsReachPrevalidationAndFail(t *testing.T) {
	t.Parallel()
	raw := []byte{0x61, 0x03}
	canonical := testSnapshot(15, false)
	rejected := errors.New("pin rejected")
	tests := map[string]func(*model.Snapshot){
		"identity": func(value *model.Snapshot) {
			value.Identity.DatasetID[0]++
		},
		"generation": func(value *model.Snapshot) {
			value.VisibilityGeneration++
		},
		"head": func(value *model.Snapshot) {
			point := value.QueryHead.Point
			point.Slot++
			point.Hash[0]++
			point.BlockNumber++
			head := model.Head{
				EventSeq: value.QueryHead.EventSeq + 1,
				Point:    point,
			}
			value.AuthorityEffective = head
			value.QueryHead = head
			value.Cutoff.AdoptionEventSeq = head.EventSeq
			value.Diagnostics.Physical = head
		},
		"cutoff": func(value *model.Snapshot) {
			value.Cutoff.PublicationID++
		},
		"selector": func(value *model.Snapshot) {
			requested := hash(0x77)
			selected := value.QueryHead.Point
			value.Selector = model.SnapshotSelector{
				Mode:                  model.SnapshotAtBlock,
				RequestedBlockHash:    &requested,
				SelectedPublicationID: value.Cutoff.PublicationID,
				SelectedPoint:         &selected,
			}
		},
	}
	for name, mutate := range tests {
		name, mutate := name, mutate
		t.Run(name, func(t *testing.T) {
			mutated := canonical
			mutate(&mutated)
			if !mutated.Valid() {
				t.Fatal("mutation fixture is not structurally valid")
			}
			encoded, err := cursor.Encode(cursor.Value{
				Scope:    chstore.AddressScope(raw, "history"),
				Snapshot: mutated,
				LastKey:  "resume",
			})
			if err != nil {
				t.Fatal(err)
			}
			reader := &fakeReader{
				snapshot: canonical,
				pre: func(value model.Snapshot) (model.Snapshot, error) {
					if !value.SamePin(canonical) {
						return model.Snapshot{}, rejected
					}
					return canonical, nil
				},
			}
			result, err := New(reader).Execute(
				context.Background(),
				cli.Invocation{
					Command: "address",
					Address: "hex:6103",
					State:   "history",
					Limit:   10,
					Cursor:  encoded,
				},
			)
			if result != nil || !errors.Is(err, rejected) {
				t.Fatalf("result/error = %#v, %v", result, err)
			}
			assertOrder(t, reader.order, "pre")
		})
	}
}

func TestEngineRejectsRepositoryPinSubstitution(t *testing.T) {
	t.Parallel()
	hashValue := hash(0x44)
	invocation := cli.Invocation{
		Command: "datum",
		At:      model.AtPoint{Tip: true},
		Hash:    &hashValue,
	}
	for _, phase := range []string{"pre", "finish"} {
		phase := phase
		t.Run(phase, func(t *testing.T) {
			snapshot := testSnapshot(16, false)
			mutate := func(value model.Snapshot) (model.Snapshot, error) {
				value.VisibilityGeneration++
				return value, nil
			}
			reader := &fakeReader{snapshot: snapshot}
			if phase == "pre" {
				reader.pre = mutate
			} else {
				reader.finish = mutate
			}
			result, err := New(reader).Execute(
				context.Background(),
				invocation,
			)
			if result != nil || !errors.Is(err, errSnapshotPinChanged) {
				t.Fatalf("result/error = %#v, %v", result, err)
			}
			if phase == "pre" {
				assertOrder(t, reader.order, "snapshot", "pre")
			} else {
				assertOrder(
					t,
					reader.order,
					"snapshot",
					"pre",
					"datum",
					"finish",
				)
			}
		})
	}
}

func TestFinishFailureSuppressesNormalAndPartialUTxOResults(t *testing.T) {
	t.Parallel()
	finishErr := errors.New("finish failed")
	hashValue := hash(0x45)
	normal := &fakeReader{
		snapshot: testSnapshot(17, false),
		finish: func(model.Snapshot) (model.Snapshot, error) {
			return model.Snapshot{}, finishErr
		},
	}
	result, err := New(normal).Execute(
		context.Background(),
		cli.Invocation{
			Command: "datum",
			At:      model.AtPoint{Tip: true},
			Hash:    &hashValue,
		},
	)
	if result != nil || !errors.Is(err, finishErr) {
		t.Fatalf("normal result/error = %#v, %v", result, err)
	}

	source := ref(9, 0)
	partial := &fakeReader{
		snapshot: testSnapshot(18, false),
		utxo: func(
			model.Snapshot,
		) (model.OutputState, []model.PartialHistoryBoundary, error) {
			return model.OutputState{},
				[]model.PartialHistoryBoundary{{
					UTxO:   source,
					Reason: "partial",
				}},
				chstore.ErrNotFound
		},
		finish: func(model.Snapshot) (model.Snapshot, error) {
			return model.Snapshot{}, finishErr
		},
	}
	result, err = New(partial).Execute(
		context.Background(),
		cli.Invocation{
			Command: "utxo",
			At:      model.AtPoint{Tip: true},
			UTxO:    &source,
		},
	)
	if result != nil || !errors.Is(err, finishErr) {
		t.Fatalf("partial result/error = %#v, %v", result, err)
	}
	assertOrder(t, partial.order, "snapshot", "pre", "utxo", "finish")
}

func TestTraceSeedCursorWithoutAddressIsRejectedBeforeRead(t *testing.T) {
	t.Parallel()
	seed := ref(1, 0)
	invocation := traceInvocation(seed, limits.DefaultTrace())
	invocation.Trace.SeedCursor = "not-allowed"
	reader := &fakeReader{snapshot: testSnapshot(19, false)}
	result, err := New(reader).Execute(context.Background(), invocation)
	if result != nil || !errors.Is(err, cursor.ErrInvalid) ||
		len(reader.order) != 0 {
		t.Fatalf("result/error/order = %#v, %v, %#v", result, err, reader.order)
	}
}

func assertOrder(t *testing.T, got []string, want ...string) {
	t.Helper()
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("order = %v, want %v", got, want)
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

func testSnapshot(event uint64, complete bool) model.Snapshot {
	start := model.Point{
		Slot:        1,
		Hash:        hash(0x70),
		BlockNumber: 1,
	}
	if complete {
		start = model.Point{Origin: true}
	}
	point := model.Point{
		Slot:        event,
		Hash:        hash(byte(event + 1)),
		BlockNumber: event,
	}
	head := model.Head{EventSeq: event, Point: point}
	return model.Snapshot{
		Identity: model.SnapshotIdentity{
			DatasetID:              model.DatasetID{1},
			SchemaContractHash:     hash(2),
			NetworkMagic:           764824073,
			NetworkName:            "mainnet",
			ByronGenesisID:         hash(3),
			ByronGenesisJSONHash:   hash(4),
			ShelleyGenesisID:       hash(5),
			ShelleyGenesisJSONHash: hash(6),
			Start:                  start,
			TrustMode:              model.TrustPeerObserved,
			CreatedAt: time.Date(
				2026, 7, 23, 1, 2, 3, 0, time.UTC,
			),
			CompleteHistory: complete,
		},
		VisibilityGeneration: 1,
		AuthorityEffective:   head,
		QueryHead:            head,
		Cutoff: model.Cutoff{
			AdoptionEventSeq: event,
			PublicationID:    event,
		},
		Selector: model.SnapshotSelector{Mode: model.SnapshotAtTip},
		Diagnostics: model.SnapshotDiagnostics{
			Physical:    head,
			TrustStatus: "agreed",
			TrustBasis:  "sampled_peer",
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
	preCalls         int
	finishCalls      int
	lastAt           model.AtPoint
	lastAddress      addressCall
	datumSnapshot    model.Snapshot
	seeds            repository.TraceSeedResult
	lastTraceQuery   repository.TraceQuery
	lastSeedLimit    uint32
	expansionBudgets []repository.ExpansionBudget
	forwardTruncated bool
	expandForward    func(context.Context, model.Snapshot, []model.UTxORef) ([]model.FlowHyperedge, error)
	expandReverse    func(context.Context, model.Snapshot, []model.UTxORef) ([]model.FlowHyperedge, error)
	pre              func(model.Snapshot) (model.Snapshot, error)
	finish           func(model.Snapshot) (model.Snapshot, error)
	utxo             func(model.Snapshot) (model.OutputState, []model.PartialHistoryBoundary, error)
	addressCursorKey string
	order            []string
}

func (reader *fakeReader) Snapshot(_ context.Context, at model.AtPoint) (model.Snapshot, error) {
	reader.snapshotCalls++
	reader.lastAt = at
	reader.order = append(reader.order, "snapshot")
	return reader.snapshot, nil
}

func (reader *fakeReader) ValidateSnapshotBeforeRead(
	_ context.Context,
	snapshot model.Snapshot,
) (model.Snapshot, error) {
	reader.preCalls++
	reader.order = append(reader.order, "pre")
	if reader.pre != nil {
		return reader.pre(snapshot)
	}
	return snapshot, nil
}

func (reader *fakeReader) FinishSnapshot(
	_ context.Context,
	snapshot model.Snapshot,
) (model.Snapshot, error) {
	reader.finishCalls++
	reader.order = append(reader.order, "finish")
	if reader.finish != nil {
		return reader.finish(snapshot)
	}
	return snapshot, nil
}

func (reader *fakeReader) UTxO(
	_ context.Context,
	snapshot model.Snapshot,
	_ model.UTxORef,
) (model.OutputState, []model.PartialHistoryBoundary, error) {
	reader.order = append(reader.order, "utxo")
	if reader.utxo != nil {
		return reader.utxo(snapshot)
	}
	return model.OutputState{}, nil, nil
}

func (reader *fakeReader) Transaction(
	context.Context,
	model.Snapshot,
	model.Hash32,
) (model.Transaction, []model.PartialHistoryBoundary, error) {
	reader.order = append(reader.order, "tx")
	return model.Transaction{}, nil, nil
}

func (reader *fakeReader) Address(
	_ context.Context,
	snapshot model.Snapshot,
	query repository.AddressQuery,
) (model.AddressPage, []model.PartialHistoryBoundary, error) {
	reader.order = append(reader.order, "address")
	reader.lastAddress = addressCall{Snapshot: snapshot, AddressQuery: query}
	page := model.AddressPage{
		Address: model.Bytes(query.Address),
		State:   query.State,
		Items:   make([]model.OutputState, 0),
	}
	if reader.addressCursorKey != "" {
		var err error
		page.Cursor, err = cursor.Encode(cursor.Value{
			Scope:    chstore.AddressScope(query.Address, query.State),
			Snapshot: snapshot,
			LastKey:  reader.addressCursorKey,
		})
		if err != nil {
			return model.AddressPage{}, nil, err
		}
	}
	return page, nil, nil
}

func (reader *fakeReader) Datum(
	_ context.Context,
	snapshot model.Snapshot,
	_ model.Hash32,
) (model.Datum, error) {
	reader.order = append(reader.order, "datum")
	reader.datumSnapshot = snapshot
	return model.Datum{}, nil
}

func (reader *fakeReader) Redeemers(
	context.Context,
	model.Snapshot,
	model.Hash32,
) ([]model.Redeemer, []model.PartialHistoryBoundary, error) {
	reader.order = append(reader.order, "redeemers")
	return nil, nil, nil
}

func (reader *fakeReader) Metadata(
	context.Context,
	model.Snapshot,
	model.Hash32,
) (model.TransactionMetadata, error) {
	reader.order = append(reader.order, "metadata")
	return model.TransactionMetadata{}, nil
}

func (reader *fakeReader) Withdrawals(
	context.Context,
	model.Snapshot,
	model.Hash32,
) ([]model.Withdrawal, error) {
	reader.order = append(reader.order, "withdrawals")
	return nil, nil
}

func (reader *fakeReader) TraceSeeds(
	_ context.Context,
	_ model.Snapshot,
	query repository.TraceQuery,
	limit uint32,
) (repository.TraceSeedResult, []model.PartialHistoryBoundary, error) {
	reader.order = append(reader.order, "seeds")
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
	reader.order = append(reader.order, "forward")
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
	reader.order = append(reader.order, "reverse")
	reader.expansionBudgets = append(reader.expansionBudgets, budget)
	if reader.expandReverse == nil {
		return repository.ExpansionResult{}, nil, nil
	}
	edges, err := reader.expandReverse(ctx, snapshot, frontier)
	return repository.ExpansionResult{Hyperedges: edges}, nil, err
}
