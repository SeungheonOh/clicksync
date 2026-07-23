package syncer

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/protocol/chainsync"
	pcommon "github.com/blinklabs-io/gouroboros/protocol/common"
	utxorpc "github.com/utxorpc/go-codegen/utxorpc/v1alpha/cardano"

	"clicksync/internal/n2n"
)

func TestSupervisorCorroboratesOlderStableCheckpointBeforeFollow(t *testing.T) {
	newest := testPoint(30, 0x30)
	stable := testPoint(20, 0x20)
	block := testBlock(21, 21, 0x21)
	tip := tipForBlock(block)
	candidates := &fakeCandidates{points: []n2n.ChainPoint{
		chainPointFromPoint(newest),
		chainPointFromPoint(stable),
	}}
	observer := &fakeObserver{}
	handler := &fakeHandler{
		reconcile: func(_ context.Context, got n2n.ChainPoint, evidence SourceEvidence) (CommitOutcome, error) {
			if !pointsEqual(got.Point, stable) ||
				!chainPointsEqual(evidence.Checkpoint, chainPointFromPoint(stable)) {
				t.Fatalf("reconcile checkpoint = %#v / %#v, want stable", got, evidence.Checkpoint)
			}
			if len(evidence.CheckpointMembers) != 2 ||
				evidence.CheckpointMembers[0].Peer.Operator ==
					evidence.CheckpointMembers[1].Peer.Operator {
				t.Fatalf("checkpoint members = %#v", evidence.CheckpointMembers)
			}
			if evidence.Primary.Peer.Address != "198.51.100.7:3001" ||
				evidence.Primary.N2NVersion != 14 {
				t.Fatalf("publication received Probe rather than Follow provenance: %#v", evidence.Primary)
			}
			return CommitOutcome{}, nil
		},
		rollForward: committingForward,
	}
	ctx, cancel := context.WithCancel(context.Background())
	transport := &fakeTransport{}
	transport.probe = func(_ context.Context, peer n2n.Peer, point pcommon.Point) (ProbeResult, error) {
		accepted := !pointsEqual(point, newest) || peer.Operator == "operator-a"
		tipSlot := uint64(40)
		if peer.Operator == "operator-b" {
			tipSlot = 41
		}
		return ProbeResult{
			Accepted:   accepted,
			Tip:        chainsync.Tip{Point: testPoint(tipSlot, byte(tipSlot)), BlockNumber: tipSlot},
			N2NVersion: 15,
			Address:    "192.0.2." + peer.Operator[len(peer.Operator)-1:],
		}, nil
	}
	transport.follow = func(
		ctx context.Context,
		peer n2n.Peer,
		got []n2n.ChainPoint,
		target n2n.Handler,
	) error {
		if len(got) != 1 || !pointsEqual(got[0].Point, stable) {
			t.Fatalf("Follow candidates = %#v, want exact stable singleton", got)
		}
		peer.Address = "198.51.100.7:3001"
		peer.N2NVersion = 14
		actualTip := chainsync.Tip{Point: testPoint(40, 0x40), BlockNumber: 40}
		peer.Tip = &actualTip
		if err := target.Reconcile(ctx, got[0], peer); err != nil {
			return err
		}
		if err := target.RollForward(ctx, block, tip, peer); err != nil {
			return err
		}
		cancel()
		return ctx.Err()
	}
	supervisor := newTestSupervisor(t, baseConfig(), candidates, handler, observer, transport)
	if err := supervisor.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("shutdown error = %v", err)
	}
	if !slices.ContainsFunc(observer.observations, func(value Observation) bool {
		return value.Kind == "disagreement" &&
			value.Peer.Operator == "operator-b" &&
			pointsEqual(value.Checkpoint, newest)
	}) {
		t.Fatal("newest-point disagreement was not persisted")
	}
	if !slices.ContainsFunc(observer.observations, func(value Observation) bool {
		return value.Kind == "source_change" &&
			value.Peer.Address == "198.51.100.7:3001" &&
			value.N2NVersion == 14
	}) {
		t.Fatal("actual Follow source/version/address was not persisted")
	}
	if candidates.calls != 1 {
		t.Fatalf("candidate loads = %d, want 1", candidates.calls)
	}
}

func TestPartialDatasetRejectedBoundaryNeverFallsBackToOrigin(t *testing.T) {
	boundary := testPoint(10, 0x10)
	ctx, cancel := context.WithCancel(context.Background())
	probes := 0
	transport := &fakeTransport{
		probe: func(
			_ context.Context,
			_ n2n.Peer,
			_ pcommon.Point,
		) (ProbeResult, error) {
			probes++
			if probes == 4 {
				cancel()
			}
			return ProbeResult{Accepted: false}, nil
		},
	}
	supervisor := newTestSupervisor(
		t,
		baseConfig(),
		&fakeCandidates{points: []n2n.ChainPoint{chainPointFromPoint(boundary)}},
		&fakeHandler{},
		&fakeObserver{},
		transport,
	)
	supervisor.wait = waitContext
	err := supervisor.Run(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v", err)
	}
	if len(transport.follows) != 0 {
		t.Fatalf("Follow called after rejected boundary: %#v", transport.follows)
	}
	for _, probe := range transport.probes {
		if pointIsOrigin(probe.point) {
			t.Fatalf("partial dataset probed Origin: %#v", transport.probes)
		}
		if !pointsEqual(probe.point, boundary) {
			t.Fatalf("unexpected probe point: %#v", probe.point)
		}
	}
}

func TestSupervisorRetriesUnavailableCorroborationThenRecovers(t *testing.T) {
	checkpoint := testChainPoint(10, 10, 0x10)
	config := baseConfig()
	config.InitialBackoff = time.Second
	config.MaxBackoff = 2 * time.Second
	ctx, cancel := context.WithCancel(context.Background())
	round := 0
	transport := &fakeTransport{
		probe: func(
			_ context.Context,
			peer n2n.Peer,
			_ pcommon.Point,
		) (ProbeResult, error) {
			if peer.Operator == "operator-a" {
				round++
			}
			if round <= 3 {
				return ProbeResult{}, RetryableTransportError(errors.New("dial unavailable"))
			}
			return ProbeResult{
				Accepted:   true,
				Tip:        chainsync.Tip{Point: testPoint(12, 0x12), BlockNumber: 12},
				N2NVersion: 15,
				Address:    "192.0.2.10:3001",
			}, nil
		},
		follow: func(
			ctx context.Context,
			peer n2n.Peer,
			points []n2n.ChainPoint,
			handler n2n.Handler,
		) error {
			peer = prepareFollowPeer(
				peer,
				chainsync.Tip{Point: testPoint(12, 0x12), BlockNumber: 12},
			)
			if err := handler.Reconcile(ctx, points[0], peer); err != nil {
				return err
			}
			cancel()
			return ctx.Err()
		},
	}
	candidates := &fakeCandidates{points: []n2n.ChainPoint{checkpoint}}
	supervisor := newTestSupervisor(
		t,
		config,
		candidates,
		&fakeHandler{},
		&fakeObserver{},
		transport,
	)
	var waits []time.Duration
	supervisor.wait = func(_ context.Context, duration time.Duration) error {
		waits = append(waits, duration)
		return nil
	}
	if err := supervisor.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v", err)
	}
	if !slices.Equal(waits, []time.Duration{time.Second, 2 * time.Second, 2 * time.Second}) {
		t.Fatalf("backoffs = %#v", waits)
	}
	if candidates.calls != 4 || len(transport.follows) != 1 {
		t.Fatalf("candidate loads=%d follows=%d", candidates.calls, len(transport.follows))
	}
}

func TestPartialByronBoundaryFallsBackFromSameSlotSuccessorToEBB(t *testing.T) {
	ebb := n2n.NewByronEBBChainPoint(testPoint(21_600, 0xe0), 20_000)
	successor := testChainPoint(21_600, 20_001, 0xe1)
	ctx, cancel := context.WithCancel(context.Background())
	handler := &fakeHandler{
		reconcile: func(
			_ context.Context,
			point n2n.ChainPoint,
			_ SourceEvidence,
		) (CommitOutcome, error) {
			if !chainPointsEqual(point, ebb) {
				t.Fatalf("reconciled point = %#v, want EBB %#v", point, ebb)
			}
			return CommitOutcome{}, nil
		},
	}
	transport := &fakeTransport{
		probe: func(
			_ context.Context,
			peer n2n.Peer,
			point pcommon.Point,
		) (ProbeResult, error) {
			return ProbeResult{
				Accepted:   pointsEqual(point, ebb.Point),
				Tip:        chainsync.Tip{Point: testPoint(22_000, 0xf0), BlockNumber: 20_100},
				N2NVersion: 15,
				Address:    "192.0.2." + peer.Operator[len(peer.Operator)-1:] + ":3001",
			}, nil
		},
		follow: func(
			ctx context.Context,
			peer n2n.Peer,
			points []n2n.ChainPoint,
			target n2n.Handler,
		) error {
			if len(points) != 1 || !chainPointsEqual(points[0], ebb) {
				t.Fatalf("Follow points = %#v, want exact EBB singleton", points)
			}
			peer = prepareFollowPeer(
				peer,
				chainsync.Tip{Point: testPoint(22_000, 0xf0), BlockNumber: 20_100},
			)
			if err := target.Reconcile(ctx, ebb, peer); err != nil {
				return err
			}
			cancel()
			return ctx.Err()
		},
	}
	supervisor := newTestSupervisor(
		t,
		baseConfig(),
		&fakeCandidates{points: []n2n.ChainPoint{successor, ebb}},
		handler,
		&fakeObserver{},
		transport,
	)
	if err := supervisor.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("shutdown error = %v", err)
	}
	for _, probe := range transport.probes {
		if pointIsOrigin(probe.point) {
			t.Fatal("partial Byron fallback widened to Origin")
		}
	}
	if len(transport.follows) != 1 {
		t.Fatalf("Follow calls = %d, want 1", len(transport.follows))
	}
}

func TestCompleteOriginDatasetMayFallbackToOrigin(t *testing.T) {
	config := baseConfig()
	config.AllowOrigin = true
	newest := testPoint(10, 0x10)
	ctx, cancel := context.WithCancel(context.Background())
	transport := &fakeTransport{
		probe: func(
			_ context.Context,
			_ n2n.Peer,
			point pcommon.Point,
		) (ProbeResult, error) {
			return ProbeResult{
				Accepted:   pointIsOrigin(point),
				Tip:        chainsync.Tip{Point: testPoint(20, 0x20), BlockNumber: 20},
				N2NVersion: 15,
				Address:    "192.0.2.10:3001",
			}, nil
		},
		follow: func(
			ctx context.Context,
			peer n2n.Peer,
			points []n2n.ChainPoint,
			handler n2n.Handler,
		) error {
			if len(points) != 1 || !pointIsOrigin(points[0].Point) {
				t.Fatalf("Origin fallback Follow points = %#v", points)
			}
			peer = prepareFollowPeer(
				peer,
				chainsync.Tip{Point: testPoint(20, 0x20), BlockNumber: 20},
			)
			if err := handler.Reconcile(ctx, points[0], peer); err != nil {
				return err
			}
			cancel()
			return ctx.Err()
		},
	}
	supervisor := newTestSupervisor(
		t,
		config,
		&fakeCandidates{points: []n2n.ChainPoint{chainPointFromPoint(newest)}},
		&fakeHandler{},
		&fakeObserver{},
		transport,
	)
	if err := supervisor.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Origin completion error = %v", err)
	}
	originProbes := 0
	for _, probe := range transport.probes {
		if pointIsOrigin(probe.point) {
			originProbes++
		}
	}
	if originProbes != 2 {
		t.Fatalf("Origin probes = %d, want 2", originProbes)
	}
}

func TestSupervisorRetriesTransportIndefinitelyThenRecovers(t *testing.T) {
	config := baseConfig()
	config.InitialBackoff = time.Second
	config.MaxBackoff = 2 * time.Second
	candidates := &fakeCandidates{points: []n2n.ChainPoint{
		testChainPoint(10, 10, 0x10),
	}}
	observer := &fakeObserver{}
	transport := &fakeTransport{}
	ctx, cancel := context.WithCancel(context.Background())
	probeRound := 0
	transport.probe = func(_ context.Context, peer n2n.Peer, point pcommon.Point) (ProbeResult, error) {
		if peer.Operator == "operator-a" {
			probeRound++
		}
		return ProbeResult{
			Accepted:   true,
			Tip:        chainsync.Tip{Point: testPoint(12, 0x12), BlockNumber: 12},
			N2NVersion: 15,
			Address:    fmt.Sprintf("192.0.2.%d", probeRound),
		}, nil
	}
	transport.follow = func(
		ctx context.Context,
		peer n2n.Peer,
		points []n2n.ChainPoint,
		handler n2n.Handler,
	) error {
		peer.N2NVersion = 15
		actualTip := chainsync.Tip{Point: testPoint(12, 0x12), BlockNumber: 12}
		peer.Tip = &actualTip
		if err := handler.Reconcile(ctx, points[0], peer); err != nil {
			return err
		}
		if len(transport.follows) == 4 {
			cancel()
			return ctx.Err()
		}
		return RetryableTransportError(errors.New("connection dropped"))
	}
	supervisor := newTestSupervisor(
		t,
		config,
		candidates,
		&fakeHandler{},
		observer,
		transport,
	)
	var waits []time.Duration
	supervisor.wait = func(_ context.Context, duration time.Duration) error {
		waits = append(waits, duration)
		return nil
	}
	err := supervisor.Run(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v", err)
	}
	if got := peerHosts(transport.follows); !slices.Equal(got, []string{"relay-a:3001", "relay-b:3001", "relay-a:3001", "relay-b:3001"}) {
		t.Fatalf("follow rotation = %#v", got)
	}
	if got := peerAddresses(transport.follows); !slices.Equal(got, []string{"192.0.2.1", "192.0.2.2", "192.0.2.3", "192.0.2.4"}) {
		t.Fatalf("freshly resolved addresses = %#v", got)
	}
	if !slices.Equal(waits, []time.Duration{time.Second, 2 * time.Second, 2 * time.Second}) {
		t.Fatalf("backoffs = %#v", waits)
	}
	if candidates.calls != 4 {
		t.Fatalf("candidate reloads = %d, want 4", candidates.calls)
	}
	sourceSelections := observationsOfKind(observer.observations, "source_change")
	if len(sourceSelections) != 4 ||
		sourceSelections[0].Reason != "initial selection" ||
		sourceSelections[1].Reason != "peer rotation" ||
		sourceSelections[2].Reason != "peer rotation" ||
		sourceSelections[3].Reason != "peer rotation" {
		t.Fatalf("source selections = %#v", sourceSelections)
	}
}

func TestSupervisorContextCancellationInterruptsReconnectBackoff(t *testing.T) {
	config := baseConfig()
	config.InitialBackoff = time.Hour
	config.MaxBackoff = time.Hour
	transport := acceptingTransport()
	transport.follow = func(
		ctx context.Context,
		peer n2n.Peer,
		points []n2n.ChainPoint,
		handler n2n.Handler,
	) error {
		peer = prepareFollowPeer(
			peer,
			chainsync.Tip{Point: testPoint(12, 0x12), BlockNumber: 12},
		)
		if err := handler.Reconcile(ctx, points[0], peer); err != nil {
			return err
		}
		return RetryableTransportError(errors.New("connection dropped"))
	}
	supervisor := newTestSupervisor(
		t,
		config,
		&fakeCandidates{points: []n2n.ChainPoint{testChainPoint(10, 10, 0x10)}},
		&fakeHandler{},
		&fakeObserver{},
		transport,
	)
	ctx, cancel := context.WithCancel(context.Background())
	waitStarted := make(chan struct{})
	supervisor.wait = func(ctx context.Context, duration time.Duration) error {
		if duration != time.Hour {
			t.Fatalf("backoff = %s, want one hour", duration)
		}
		close(waitStarted)
		return waitContext(ctx, duration)
	}
	go func() {
		<-waitStarted
		cancel()
	}()
	if err := supervisor.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v", err)
	}
	if len(transport.follows) != 1 {
		t.Fatalf("follow calls = %d, want 1", len(transport.follows))
	}
}

func TestPeerDataViolationPersistsExactSourceAndQuarantinesOperator(t *testing.T) {
	checkpoint := testPoint(10, 0x10)
	candidates := &fakeCandidates{points: []n2n.ChainPoint{
		chainPointFromPoint(checkpoint),
	}}
	observer := &fakeObserver{}
	transport := acceptingTransport()
	transport.follow = func(
		ctx context.Context,
		peer n2n.Peer,
		points []n2n.ChainPoint,
		handler n2n.Handler,
	) error {
		peer.Address = "198.51.100.44:3001"
		peer.N2NVersion = 15
		tip := chainsync.Tip{Point: clonePoint(checkpoint), BlockNumber: 10}
		peer.Tip = &tip
		if err := handler.Reconcile(ctx, points[0], peer); err != nil {
			return err
		}
		return &n2n.PeerDataViolation{
			Kind:  "header_body_mismatch",
			Point: testPoint(11, 0x11),
			Err:   errors.New("wrong block hash"),
		}
	}
	supervisor := newTestSupervisor(
		t,
		baseConfig(),
		candidates,
		&fakeHandler{},
		observer,
		transport,
	)
	err := supervisor.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "quarantine leaves 1 independent operators") {
		t.Fatalf("error = %v", err)
	}
	violations := slices.DeleteFunc(
		slices.Clone(observer.observations),
		func(value Observation) bool {
			return value.Kind != "disagreement" ||
				!strings.HasPrefix(value.Reason, "peer_data_violation:")
		},
	)
	if len(violations) != 1 ||
		violations[0].Result != "quarantined" ||
		violations[0].Peer.Host != "relay-a:3001" ||
		violations[0].Peer.Address != "198.51.100.44:3001" ||
		violations[0].Peer.Operator != "operator-a" {
		t.Fatalf("peer-data observations = %#v", violations)
	}
}

func TestPeerDataQuarantineRotatesOnlyWhenCorroborationRemains(t *testing.T) {
	config := baseConfig()
	config.Peers = append(
		config.Peers,
		testPeer("relay-c:3001", "operator-c"),
	)
	checkpoint := testPoint(10, 0x10)
	block := testBlock(11, 11, 0x11)
	ctx, cancel := context.WithCancel(context.Background())
	transport := acceptingTransport()
	transport.follow = func(
		ctx context.Context,
		peer n2n.Peer,
		points []n2n.ChainPoint,
		handler n2n.Handler,
	) error {
		peer = prepareFollowPeer(
			peer,
			chainsync.Tip{Point: clonePoint(checkpoint), BlockNumber: 10},
		)
		if err := handler.Reconcile(ctx, points[0], peer); err != nil {
			return err
		}
		if peer.Operator == "operator-a" {
			return &n2n.PeerDataViolation{
				Kind:  "extra_block",
				Point: testPoint(11, 0x11),
				Err:   errors.New("extra body"),
			}
		}
		if err := handler.RollForward(ctx, block, tipForBlock(block), peer); err != nil {
			return err
		}
		cancel()
		return ctx.Err()
	}
	supervisor := newTestSupervisor(
		t,
		config,
		&fakeCandidates{points: []n2n.ChainPoint{chainPointFromPoint(checkpoint)}},
		&fakeHandler{rollForward: committingForward},
		&fakeObserver{},
		transport,
	)
	if err := supervisor.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("shutdown error = %v", err)
	}
	if got := peerHosts(transport.follows); !slices.Equal(
		got,
		[]string{"relay-a:3001", "relay-c:3001"},
	) {
		t.Fatalf("follow rotation after quarantine = %#v", got)
	}
	countAProbes := 0
	for _, call := range transport.probes {
		if call.peer.Operator == "operator-a" {
			countAProbes++
		}
	}
	if countAProbes != 1 {
		t.Fatalf("quarantined operator probe count = %d, want 1", countAProbes)
	}
}

func TestRepeatedExactRangeUnavailableQuarantinesSource(t *testing.T) {
	checkpoint := testPoint(10, 0x10)
	start := testPoint(11, 0x11)
	end := testPoint(20, 0x20)
	observer := &fakeObserver{}
	transport := acceptingTransport()
	transport.follow = func(
		ctx context.Context,
		peer n2n.Peer,
		points []n2n.ChainPoint,
		handler n2n.Handler,
	) error {
		peer = prepareFollowPeer(
			peer,
			chainsync.Tip{Point: clonePoint(checkpoint), BlockNumber: 10},
		)
		if err := handler.Reconcile(ctx, points[0], peer); err != nil {
			return err
		}
		return &n2n.RangeUnavailable{
			Start: start,
			End:   end,
			Err:   errors.New("short range"),
		}
	}
	supervisor := newTestSupervisor(
		t,
		baseConfig(),
		&fakeCandidates{points: []n2n.ChainPoint{chainPointFromPoint(checkpoint)}},
		&fakeHandler{},
		observer,
		transport,
	)
	err := supervisor.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "quarantine leaves 1 independent operators") {
		t.Fatalf("error = %v", err)
	}
	if got := peerHosts(transport.follows); !slices.Equal(
		got,
		[]string{"relay-a:3001", "relay-b:3001", "relay-a:3001"},
	) {
		t.Fatalf("range retry rotation = %#v", got)
	}
	observations := slices.DeleteFunc(
		slices.Clone(observer.observations),
		func(value Observation) bool {
			return value.Kind != "disagreement" ||
				!strings.HasPrefix(value.Reason, "range_unavailable:")
		},
	)
	if len(observations) != 3 ||
		observations[0].Result != "unavailable" ||
		observations[1].Result != "unavailable" ||
		observations[2].Result != "quarantined" {
		t.Fatalf("range observations = %#v", observations)
	}
}

func TestRangeRetryStateIsBoundedPerOperatorAndClearsOnProgress(t *testing.T) {
	state := make(rangeRetryState)
	for index := 0; index < 10_000; index++ {
		key := fmt.Sprintf("range-%d", index)
		if state.repeated("operator-a", key) {
			t.Fatalf("new range %q was classified as repeated", key)
		}
	}
	if len(state) != 1 {
		t.Fatalf("range retry entries = %d, want one per operator", len(state))
	}
	if !state.repeated("operator-a", "range-9999") {
		t.Fatal("exact current range retry was not recognized")
	}
	if state.repeated("operator-b", "range-1") {
		t.Fatal("first range for independent operator was repeated")
	}
	if len(state) != 2 {
		t.Fatalf("range retry entries = %d, want two operators", len(state))
	}
	state.clear("operator-a")
	if state.repeated("operator-a", "range-9999") {
		t.Fatal("successful progress did not clear exact retry state")
	}
}

func TestCommittedPostActionFailureIsTerminalAndNeverRotates(t *testing.T) {
	before := testPoint(10, 0x10)
	block := testBlock(11, 11, 0x11)
	candidates := &fakeCandidates{points: []n2n.ChainPoint{chainPointFromPoint(before)}}
	forwardCalls := 0
	handler := &fakeHandler{
		rollForward: func(
			_ context.Context,
			block lcommon.Block,
			tip chainsync.Tip,
			_ SourceEvidence,
		) (CommitOutcome, error) {
			forwardCalls++
			last := n2n.NewChainPoint(
				pcommon.NewPoint(block.SlotNumber(), block.Hash().Bytes()),
				block.BlockNumber(),
			)
			return CommitOutcome{
				Accepted:           true,
				Committed:          true,
				CommittedBlocks:    1,
				LastCommittedPoint: &last,
				LastCommittedTip:   &tip,
			}, errors.New("manifest cache update failed")
		},
	}
	transport := acceptingTransport()
	transport.follow = func(
		ctx context.Context,
		peer n2n.Peer,
		points []n2n.ChainPoint,
		target n2n.Handler,
	) error {
		peer = prepareFollowPeer(
			peer,
			chainsync.Tip{
				Point:       clonePoint(points[0].Point),
				BlockNumber: points[0].BlockNumber,
			},
		)
		if err := target.Reconcile(ctx, points[0], peer); err != nil {
			return err
		}
		return target.RollForward(ctx, block, tipForBlock(block), peer)
	}
	supervisor := newTestSupervisor(
		t,
		baseConfig(),
		candidates,
		handler,
		&fakeObserver{},
		transport,
	)
	if err := supervisor.Run(context.Background()); err == nil ||
		!strings.Contains(err.Error(), "manifest cache update failed") {
		t.Fatalf("error = %v", err)
	}
	if forwardCalls != 1 {
		t.Fatalf("roll-forward publications = %d, want exactly 1", forwardCalls)
	}
	if candidates.calls != 1 || len(transport.follows) != 1 {
		t.Fatalf(
			"terminal handler failure retried: candidate loads=%d follows=%d",
			candidates.calls,
			len(transport.follows),
		)
	}
}

func TestRollbackRequiresAndCarriesIndependentConfirmation(t *testing.T) {
	primary := actualPeer(
		testPeer("relay-a:3001", "operator-a"),
		"198.51.100.1:3001",
		15,
	)
	secondary := testPeer("relay-b:3001", "operator-b")
	rollbackPoint := testPoint(8, 0x08)
	rollbackTarget := n2n.NewByronEBBChainPoint(rollbackPoint, 777)
	branchTip := chainsync.Tip{
		Point:       testPoint(12, 0x12),
		BlockNumber: 800,
	}
	var evidence RollbackEvidence
	var delegatedTarget n2n.ChainPoint
	delegateCalls := 0
	delegate := &fakeHandler{
		rollBackward: func(
			_ context.Context,
			gotTarget n2n.ChainPoint,
			_ chainsync.Tip,
			got RollbackEvidence,
		) (CommitOutcome, error) {
			delegateCalls++
			delegatedTarget = gotTarget
			evidence = got
			return CommitOutcome{Committed: true}, nil
		},
	}
	transport := &fakeTransport{
		rollbackProbe: func(
			_ context.Context,
			peer n2n.Peer,
			target pcommon.Point,
			branch pcommon.Point,
		) (RollbackProbeResult, error) {
			if peer.Operator != "operator-b" ||
				!pointsEqual(target, rollbackPoint) ||
				!pointsEqual(branch, branchTip.Point) {
				t.Fatalf(
					"rollback probe = %s target:%#v branch:%#v",
					peer.Operator,
					target,
					branch,
				)
			}
			return RollbackProbeResult{
				TargetAccepted: true,
				BranchAccepted: true,
				Tip: chainsync.Tip{
					Point:       testPoint(13, 0x13),
					BlockNumber: 801,
				},
				N2NVersion: 15,
				Address:    "198.51.100.2:3001",
			}, nil
		},
	}
	supervisor := newTestSupervisor(
		t,
		baseConfig(),
		&fakeCandidates{},
		delegate,
		&fakeObserver{},
		transport,
	)
	attempt := &attemptHandler{
		supervisor: supervisor,
		evidence: SourceEvidence{
			Primary:    PeerEvidence{Peer: primary, N2NVersion: 15},
			Checkpoint: testChainPoint(10, 10, 0x10),
			CheckpointMembers: []PeerEvidence{
				{Peer: primary, N2NVersion: 15},
				{Peer: secondary, N2NVersion: 15},
			},
		},
		delegate:        delegate,
		committedBlocks: new(uint64),
	}
	if err := attempt.RollBackward(
		context.Background(),
		rollbackTarget,
		branchTip,
		primary,
	); err != nil {
		t.Fatal(err)
	}
	if delegateCalls != 1 || len(evidence.Confirmations) != 2 ||
		!chainPointsEqual(delegatedTarget, rollbackTarget) ||
		!chainPointsEqual(evidence.Target, rollbackTarget) ||
		!pointsEqual(evidence.BranchTip.Point, branchTip.Point) ||
		evidence.Confirmations[0].Membership.Peer.Operator != "operator-a" ||
		evidence.Confirmations[1].Membership.Peer.Operator != "operator-b" ||
		!chainPointsEqual(evidence.Confirmations[1].Target, rollbackTarget) ||
		!pointsEqual(
			evidence.Confirmations[1].BranchTip.Point,
			branchTip.Point,
		) ||
		evidence.Confirmations[0].Method !=
			RollbackProofFollowBlockFetch ||
		evidence.Confirmations[1].Method !=
			RollbackProofPairedSingleton ||
		len(transport.rollbackProbes) != 1 {
		t.Fatalf("typed rollback evidence = %#v, calls=%d", evidence, delegateCalls)
	}
}

func TestRollbackCommonBranchTipCannotConfirmUnrelatedTarget(t *testing.T) {
	primary := actualPeer(
		testPeer("relay-a:3001", "operator-a"),
		"198.51.100.1:3001",
		15,
	)
	target := testChainPoint(8, 8, 0x08)
	branchTip := chainsync.Tip{
		Point:       testPoint(12, 0x12),
		BlockNumber: 12,
	}
	delegateCalls := 0
	delegate := &fakeHandler{
		rollBackward: func(
			context.Context,
			n2n.ChainPoint,
			chainsync.Tip,
			RollbackEvidence,
		) (CommitOutcome, error) {
			delegateCalls++
			return CommitOutcome{Committed: true}, nil
		},
	}
	observer := &fakeObserver{}
	transport := &fakeTransport{
		rollbackProbe: func(
			_ context.Context,
			_ n2n.Peer,
			gotTarget pcommon.Point,
			gotBranch pcommon.Point,
		) (RollbackProbeResult, error) {
			if !pointsEqual(gotTarget, target.Point) ||
				!pointsEqual(gotBranch, branchTip.Point) {
				t.Fatalf(
					"pair proof target=%#v branch=%#v",
					gotTarget,
					gotBranch,
				)
			}
			return RollbackProbeResult{
				TargetAccepted: false,
				BranchAccepted: true,
				Tip: chainsync.Tip{
					Point:       testPoint(20, 0x20),
					BlockNumber: 20,
				},
				N2NVersion: 15,
				Address:    "198.51.100.2:3001",
			}, nil
		},
	}
	supervisor := newTestSupervisor(
		t,
		baseConfig(),
		&fakeCandidates{},
		delegate,
		observer,
		transport,
	)
	quarantined := make(map[string]struct{})
	attempt := &attemptHandler{
		supervisor: supervisor,
		evidence: SourceEvidence{
			Primary: PeerEvidence{
				Peer:       primary,
				Tip:        branchTip,
				N2NVersion: 15,
			},
		},
		delegate:        delegate,
		committedBlocks: new(uint64),
		quarantined:     quarantined,
	}
	err := attempt.RollBackward(
		context.Background(),
		target,
		branchTip,
		primary,
	)
	if err == nil || delegateCalls != 0 {
		t.Fatalf("error=%v delegate calls=%d", err, delegateCalls)
	}
	if len(transport.rollbackProbes) != 1 ||
		len(observer.observations) != 1 ||
		observer.observations[0].Result != "quarantined" ||
		!strings.Contains(
			observer.observations[0].Reason,
			"exact rollback target",
		) {
		t.Fatalf(
			"probes=%#v observations=%#v",
			transport.rollbackProbes,
			observer.observations,
		)
	}
	if _, ok := quarantined["operator-b"]; !ok {
		t.Fatalf("disagreeing operator not quarantined: %#v", quarantined)
	}
}

func TestMalformedRollbackDoesNotCrossPendingBranchBarrier(t *testing.T) {
	primary := actualPeer(
		testPeer("relay-a:3001", "operator-a"),
		"198.51.100.1:3001",
		15,
	)
	barrierCalls := 0
	delegate := &fakeHandler{
		rollbackObserved: func(
			context.Context,
			n2n.ChainPoint,
			chainsync.Tip,
		) error {
			barrierCalls++
			return nil
		},
	}
	attempt := &attemptHandler{
		supervisor: &Supervisor{
			config:    baseConfig(),
			handler:   delegate,
			observer:  &fakeObserver{},
			transport: &fakeTransport{},
			now:       time.Now,
		},
		evidence: SourceEvidence{
			Primary: PeerEvidence{
				Peer:       primary,
				N2NVersion: 15,
			},
		},
		delegate:        delegate,
		committedBlocks: new(uint64),
	}
	err := attempt.RollBackward(
		context.Background(),
		testChainPoint(13, 13, 0x13),
		chainsync.Tip{
			Point:       testPoint(12, 0x12),
			BlockNumber: 12,
		},
		primary,
	)
	if err == nil ||
		!strings.Contains(err.Error(), "after the reported branch tip") ||
		barrierCalls != 0 {
		t.Fatalf("barrier calls=%d error=%v", barrierCalls, err)
	}
}

func TestRollbackRejectsCommittedPrefixTailDifferentFromTarget(t *testing.T) {
	primary := actualPeer(
		testPeer("relay-a:3001", "operator-a"),
		"198.51.100.1:3001",
		15,
	)
	target := testChainPoint(8, 8, 0x08)
	wrongTail := testChainPoint(9, 9, 0x09)
	branchTip := chainsync.Tip{Point: testPoint(12, 0x12), BlockNumber: 12}
	delegate := &fakeHandler{
		rollBackward: func(
			_ context.Context,
			_ n2n.ChainPoint,
			_ chainsync.Tip,
			_ RollbackEvidence,
		) (CommitOutcome, error) {
			tip := chainsync.Tip{
				Point:       clonePoint(wrongTail.Point),
				BlockNumber: wrongTail.BlockNumber,
			}
			return CommitOutcome{
				Committed:          true,
				CommittedBlocks:    1,
				LastCommittedPoint: &wrongTail,
				LastCommittedTip:   &tip,
			}, nil
		},
	}
	transport := &fakeTransport{
		rollbackProbe: func(
			_ context.Context,
			_ n2n.Peer,
			_ pcommon.Point,
			_ pcommon.Point,
		) (RollbackProbeResult, error) {
			return RollbackProbeResult{
				TargetAccepted: true,
				BranchAccepted: true,
				Tip:            branchTip,
				N2NVersion:     15,
				Address:        "198.51.100.2:3001",
			}, nil
		},
	}
	supervisor := newTestSupervisor(
		t,
		baseConfig(),
		&fakeCandidates{},
		delegate,
		&fakeObserver{},
		transport,
	)
	attempt := &attemptHandler{
		supervisor: supervisor,
		evidence: SourceEvidence{
			Primary: PeerEvidence{Peer: primary, Tip: branchTip, N2NVersion: 15},
		},
		delegate:        delegate,
		committedBlocks: new(uint64),
	}
	err := attempt.RollBackward(context.Background(), target, branchTip, primary)
	if err == nil || !strings.Contains(err.Error(), "differs from the exact rollback target") {
		t.Fatalf("error = %v", err)
	}
	if *attempt.committedBlocks != 0 {
		t.Fatalf("corrupt prefix changed committed counter to %d", *attempt.committedBlocks)
	}
}

func TestRollbackDisagreementQuarantinesBeforeHandlerCommit(t *testing.T) {
	primary := actualPeer(
		testPeer("relay-a:3001", "operator-a"),
		"198.51.100.1:3001",
		15,
	)
	secondary := testPeer("relay-b:3001", "operator-b")
	delegateCalls := 0
	delegate := &fakeHandler{
		rollBackward: func(
			context.Context,
			n2n.ChainPoint,
			chainsync.Tip,
			RollbackEvidence,
		) (CommitOutcome, error) {
			delegateCalls++
			return CommitOutcome{Committed: true}, nil
		},
	}
	observer := &fakeObserver{}
	transport := &fakeTransport{
		rollbackProbe: func(
			context.Context,
			n2n.Peer,
			pcommon.Point,
			pcommon.Point,
		) (RollbackProbeResult, error) {
			return RollbackProbeResult{
				TargetAccepted: false,
				BranchAccepted: true,
			}, nil
		},
	}
	supervisor := newTestSupervisor(
		t,
		baseConfig(),
		&fakeCandidates{},
		delegate,
		observer,
		transport,
	)
	attempt := &attemptHandler{
		supervisor: supervisor,
		evidence: SourceEvidence{
			Primary: PeerEvidence{Peer: primary},
			CheckpointMembers: []PeerEvidence{
				{Peer: primary},
				{Peer: secondary},
			},
		},
		delegate:        delegate,
		committedBlocks: new(uint64),
	}
	err := attempt.RollBackward(
		context.Background(),
		testChainPoint(8, 8, 0x08),
		chainsync.Tip{Point: testPoint(12, 0x12), BlockNumber: 12},
		primary,
	)
	if err == nil ||
		!strings.Contains(err.Error(), "peer quarantine leaves 1") {
		t.Fatalf("error = %v", err)
	}
	if delegateCalls != 0 {
		t.Fatalf("rollback handler called %d times before confirmation", delegateCalls)
	}
	if len(observer.observations) != 1 ||
		observer.observations[0].Result != "quarantined" {
		t.Fatalf("rollback observations = %#v", observer.observations)
	}
}

func TestPeriodicObservationsHappenAfterCommits(t *testing.T) {
	first := testBlock(11, 11, 0x11)
	second := testBlock(12, 12, 0x12)
	config := baseConfig()
	config.CheckpointEveryBlocks = 1
	ctx, cancel := context.WithCancel(context.Background())
	observer := &fakeObserver{}
	transport := acceptingTransport()
	transport.follow = func(
		ctx context.Context,
		peer n2n.Peer,
		points []n2n.ChainPoint,
		target n2n.Handler,
	) error {
		peer = prepareFollowPeer(
			peer,
			chainsync.Tip{
				Point:       clonePoint(points[0].Point),
				BlockNumber: points[0].BlockNumber,
			},
		)
		if err := target.Reconcile(ctx, points[0], peer); err != nil {
			return err
		}
		if err := target.RollForward(
			ctx,
			first,
			chainsync.Tip{Point: testPoint(12, 0x12), BlockNumber: 12},
			peer,
		); err != nil {
			return err
		}
		if err := target.RollForward(ctx, second, tipForBlock(second), peer); err != nil {
			return err
		}
		cancel()
		return ctx.Err()
	}
	supervisor := newTestSupervisor(
		t,
		config,
		&fakeCandidates{points: []n2n.ChainPoint{testChainPoint(10, 10, 0x10)}},
		&fakeHandler{rollForward: committingForward},
		observer,
		transport,
	)
	if err := supervisor.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("shutdown error = %v", err)
	}
	periodic := slices.DeleteFunc(
		slices.Clone(observer.observations),
		func(value Observation) bool {
			return value.Kind != "checkpoint" ||
				!strings.HasPrefix(value.Reason, "periodic ")
		},
	)
	if len(periodic) != 4 ||
		!pointsEqual(periodic[0].Checkpoint, testPoint(11, 0x11)) ||
		!pointsEqual(periodic[1].Checkpoint, testPoint(11, 0x11)) ||
		!pointsEqual(periodic[2].Checkpoint, testPoint(12, 0x12)) ||
		!pointsEqual(periodic[3].Checkpoint, testPoint(12, 0x12)) {
		t.Fatalf("periodic observations = %#v", periodic)
	}
}

func TestRollForwardCheckpointsExactPriorByronEBBTail(t *testing.T) {
	primary := actualPeer(
		testPeer("relay-a:3001", "operator-a"),
		"198.51.100.1:3001",
		15,
	)
	secondary := testPeer("relay-b:3001", "operator-b")
	tailPoint := testPoint(11, 0xeb)
	tail := n2n.NewByronEBBChainPoint(tailPoint, 10)
	tailTip := chainsync.Tip{Point: testPoint(12, 0x12), BlockNumber: 12}
	callback := testBlock(11, 11, 0x11)
	handler := &fakeHandler{
		rollForward: func(
			context.Context,
			lcommon.Block,
			chainsync.Tip,
			SourceEvidence,
		) (CommitOutcome, error) {
			clonedTail := cloneChainPoint(tail)
			clonedTip := cloneTip(tailTip)
			return CommitOutcome{
				Accepted:           true,
				Committed:          true,
				CommittedBlocks:    1,
				LastCommittedPoint: &clonedTail,
				LastCommittedTip:   &clonedTip,
			}, nil
		},
	}
	observer := &fakeObserver{}
	transport := &fakeTransport{
		probe: func(
			context.Context,
			n2n.Peer,
			pcommon.Point,
		) (ProbeResult, error) {
			return ProbeResult{
				Accepted:   true,
				Tip:        cloneTip(tailTip),
				N2NVersion: 15,
			}, nil
		},
	}
	config := baseConfig()
	config.CheckpointEveryBlocks = 1
	supervisor := newTestSupervisor(
		t,
		config,
		&fakeCandidates{},
		handler,
		observer,
		transport,
	)
	attempt := &attemptHandler{
		supervisor: supervisor,
		evidence: SourceEvidence{
			Primary: PeerEvidence{Peer: primary, N2NVersion: 15},
			CheckpointMembers: []PeerEvidence{
				{Peer: primary, N2NVersion: 15},
				{Peer: secondary, N2NVersion: 15},
			},
		},
		delegate:        handler,
		committedBlocks: new(uint64),
	}
	if err := attempt.RollForward(
		context.Background(),
		callback,
		tailTip,
		primary,
	); err != nil {
		t.Fatal(err)
	}
	periodic := observationsOfKind(observer.observations, "checkpoint")
	if len(periodic) != 2 {
		t.Fatalf("checkpoint observations = %#v", periodic)
	}
	for _, observation := range periodic {
		if !pointsEqual(observation.Checkpoint, tailPoint) ||
			observation.CheckpointBlockNumber != 10 ||
			observation.CheckpointIsByronEBB == nil ||
			!*observation.CheckpointIsByronEBB {
			t.Fatalf("checkpoint did not use exact Byron EBB tail: %#v", observation)
		}
	}
}

func TestCommittedTailContractRejectsMissingOrMismatchedMetadata(t *testing.T) {
	if err := validateCommittedTail(CommitOutcome{
		Committed:       true,
		CommittedBlocks: 1,
	}); err == nil {
		t.Fatal("accepted committed prefix without exact tail and peer tip")
	}
	block := testBlock(11, 11, 0x11)
	point := testPoint(11, 0x11)
	tip := tipForBlock(block)
	wrongHeight := n2n.NewChainPoint(point, 10)
	outcome := CommitOutcome{
		Committed:          true,
		CommittedBlocks:    1,
		LastCommittedPoint: &wrongHeight,
		LastCommittedTip:   &tip,
	}
	if err := validateCommittedTail(outcome); err != nil {
		t.Fatal(err)
	}
	if err := validateTailAtOrBeforeBlock(wrongHeight, block); err == nil {
		t.Fatal("accepted callback point with mismatched committed height")
	}
}

func TestUncommittedHandlerFailureIsTerminal(t *testing.T) {
	handler := &fakeHandler{
		rollForward: func(
			context.Context,
			lcommon.Block,
			chainsync.Tip,
			SourceEvidence,
		) (CommitOutcome, error) {
			return CommitOutcome{}, errors.New("facts insert failed")
		},
	}
	transport := acceptingTransport()
	transport.follow = oneBlockFollow(testBlock(11, 11, 0x11))
	supervisor := newTestSupervisor(
		t,
		baseConfig(),
		&fakeCandidates{points: []n2n.ChainPoint{testChainPoint(10, 10, 0x10)}},
		handler,
		&fakeObserver{},
		transport,
	)
	err := supervisor.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "facts insert failed") {
		t.Fatalf("error = %v", err)
	}
}

func TestMicrobatchStagesThenFinalizesOnContextShutdown(t *testing.T) {
	blocks := []testLedgerBlock{
		testBlock(11, 11, 0x11),
		testBlock(12, 12, 0x12),
		testBlock(13, 13, 0x13),
	}
	callbacks := 0
	handler := &fakeHandler{
		rollForward: func(
			_ context.Context,
			block lcommon.Block,
			tip chainsync.Tip,
			_ SourceEvidence,
		) (CommitOutcome, error) {
			callbacks++
			switch callbacks {
			case 1:
				return CommitOutcome{Accepted: true}, nil
			case 2:
				last := n2n.NewChainPoint(
					pcommon.NewPoint(block.SlotNumber(), block.Hash().Bytes()),
					block.BlockNumber(),
				)
				return CommitOutcome{
					Accepted:           true,
					Committed:          true,
					CommittedBlocks:    2,
					LastCommittedPoint: &last,
					LastCommittedTip:   &tip,
				}, nil
			case 3:
				return CommitOutcome{Accepted: true}, nil
			default:
				t.Fatal("unexpected roll-forward")
				return CommitOutcome{}, nil
			}
		},
		endAttempt: func(_ context.Context, end AttemptEnd) (CommitOutcome, error) {
			if end.Cause != "context_shutdown" {
				t.Fatalf("attempt end = %#v", end)
			}
			point := testPoint(13, 0x13)
			tip := chainsync.Tip{Point: clonePoint(point), BlockNumber: 13}
			last := n2n.NewChainPoint(point, tip.BlockNumber)
			return CommitOutcome{
				Committed:          true,
				CommittedBlocks:    1,
				LastCommittedPoint: &last,
				LastCommittedTip:   &tip,
			}, nil
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	transport := acceptingTransport()
	transport.follow = func(
		ctx context.Context,
		peer n2n.Peer,
		points []n2n.ChainPoint,
		target n2n.Handler,
	) error {
		peer = prepareFollowPeer(
			peer,
			chainsync.Tip{
				Point:       clonePoint(points[0].Point),
				BlockNumber: points[0].BlockNumber,
			},
		)
		if err := target.Reconcile(ctx, points[0], peer); err != nil {
			return err
		}
		for _, block := range blocks {
			if err := target.RollForward(ctx, block, tipForBlock(block), peer); err != nil {
				return err
			}
		}
		cancel()
		return ctx.Err()
	}
	config := baseConfig()
	supervisor := newTestSupervisor(
		t,
		config,
		&fakeCandidates{points: []n2n.ChainPoint{testChainPoint(10, 10, 0x10)}},
		handler,
		&fakeObserver{},
		transport,
	)
	if err := supervisor.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("shutdown error = %v", err)
	}
	if callbacks != 3 {
		t.Fatalf("roll-forward callbacks = %d", callbacks)
	}
	if len(transport.follows) != 1 {
		t.Fatalf("Follow attempts = %d, want 1", len(transport.follows))
	}
}

func TestPeriodicCheckpointDisagreementStopsAfterCommittedAdoption(t *testing.T) {
	start := testPoint(10, 0x10)
	block := testBlock(11, 11, 0x11)
	transport := acceptingTransport()
	transport.probe = func(_ context.Context, peer n2n.Peer, point pcommon.Point) (ProbeResult, error) {
		accepted := pointsEqual(point, start) || peer.Operator == "operator-a"
		return ProbeResult{
			Accepted:   accepted,
			Tip:        chainsync.Tip{Point: testPoint(20, 0x20), BlockNumber: 20},
			N2NVersion: 15,
			Address:    "192.0.2.20:3001",
		}, nil
	}
	transport.follow = oneBlockFollow(block)
	config := baseConfig()
	config.CheckpointEveryBlocks = 1
	observer := &fakeObserver{}
	supervisor := newTestSupervisor(
		t,
		config,
		&fakeCandidates{points: []n2n.ChainPoint{chainPointFromPoint(start)}},
		&fakeHandler{rollForward: committingForward},
		observer,
		transport,
	)
	err := supervisor.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "periodic corroboration failure") {
		t.Fatalf("error = %v", err)
	}
	if len(transport.follows) != 1 {
		t.Fatalf("committed disagreement rotated source %d times", len(transport.follows))
	}
	if !slices.ContainsFunc(observer.observations, func(value Observation) bool {
		return value.Kind == "disagreement" &&
			value.Result == "quarantined" &&
			pointsEqual(value.Checkpoint, testPoint(11, 0x11))
	}) {
		t.Fatalf("observations = %#v", observer.observations)
	}
}

func TestDuplicateOperatorCannotSatisfyRollbackCorroboration(t *testing.T) {
	primary := actualPeer(
		testPeer("relay-a:3001", "operator-a"),
		"198.51.100.1:3001",
		15,
	)
	config := baseConfig()
	config.Peers = []n2n.Peer{
		testPeer("relay-a:3001", "operator-a"),
		testPeer("relay-b1:3001", "operator-b"),
		testPeer("relay-b2:3001", "operator-b"),
		testPeer("relay-c:3001", "operator-c"),
	}
	config.Corroboration = 3
	config.RollbackConfirmations = 3
	var probed []string
	transport := &fakeTransport{
		rollbackProbe: func(
			_ context.Context,
			peer n2n.Peer,
			_ pcommon.Point,
			_ pcommon.Point,
		) (RollbackProbeResult, error) {
			probed = append(probed, peer.Host)
			accepted := peer.Operator == "operator-b"
			return RollbackProbeResult{
				TargetAccepted: accepted,
				BranchAccepted: accepted,
				Tip: chainsync.Tip{
					Point:       testPoint(20, 0x20),
					BlockNumber: 20,
				},
			}, nil
		},
	}
	delegateCalls := 0
	handler := &fakeHandler{
		rollBackward: func(
			context.Context,
			n2n.ChainPoint,
			chainsync.Tip,
			RollbackEvidence,
		) (CommitOutcome, error) {
			delegateCalls++
			return CommitOutcome{Committed: true}, nil
		},
	}
	// Construct the attempt directly to verify its defense-in-depth dedupe.
	// New rejects this duplicate-operator configuration at the API boundary.
	supervisor := &Supervisor{
		config:    config,
		handler:   handler,
		observer:  &fakeObserver{},
		transport: transport,
		now: func() time.Time {
			return time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
		},
	}
	attempt := &attemptHandler{
		supervisor: supervisor,
		evidence: SourceEvidence{
			Primary: PeerEvidence{Peer: primary},
		},
		delegate:        handler,
		committedBlocks: new(uint64),
	}
	err := attempt.RollBackward(
		context.Background(),
		testChainPoint(8, 8, 0x08),
		chainsync.Tip{Point: testPoint(12, 0x12), BlockNumber: 12},
		primary,
	)
	if err == nil ||
		!strings.Contains(err.Error(), "peer quarantine leaves 2") {
		t.Fatalf("error = %v", err)
	}
	if delegateCalls != 0 {
		t.Fatalf("duplicate operator reached rollback handler")
	}
	if !slices.Equal(probed, []string{"relay-b1:3001", "relay-c:3001"}) {
		t.Fatalf("rollback probes = %#v", probed)
	}
}

func TestContextStopsBlockedFollowWithoutRestart(t *testing.T) {
	transport := acceptingTransport()
	transport.follow = func(
		ctx context.Context,
		_ n2n.Peer,
		_ []n2n.ChainPoint,
		_ n2n.Handler,
	) error {
		<-ctx.Done()
		return ctx.Err()
	}
	supervisor := newTestSupervisor(
		t,
		baseConfig(),
		&fakeCandidates{points: []n2n.ChainPoint{testChainPoint(10, 10, 0x10)}},
		&fakeHandler{},
		&fakeObserver{},
		transport,
	)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := supervisor.Run(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v", err)
	}
	if len(transport.follows) != 0 {
		t.Fatalf("Follow called after cancellation: %#v", transport.follows)
	}
}

func TestCandidateOrderAndIndependentOperatorValidation(t *testing.T) {
	for name, mutate := range map[string]func(*Config){
		"same operator": func(config *Config) {
			config.Peers[1].Operator = config.Peers[0].Operator
		},
		"case-folded operator": func(config *Config) {
			config.Peers[1].Operator = strings.ToUpper(config.Peers[0].Operator)
		},
		"case-folded host": func(config *Config) {
			config.Peers[1].Host = strings.ToUpper(config.Peers[0].Host)
		},
	} {
		t.Run(name, func(t *testing.T) {
			config := baseConfig()
			mutate(&config)
			if _, err := New(
				config,
				&fakeCandidates{},
				&fakeHandler{},
				&fakeObserver{},
				&fakeTransport{},
			); err == nil {
				t.Fatal("accepted duplicate peer identity")
			}
		})
	}
	for name, points := range map[string][]n2n.ChainPoint{
		"ascending": {
			testChainPoint(10, 10, 0x10),
			testChainPoint(11, 11, 0x11),
		},
		"origin_middle": {
			testChainPoint(10, 10, 0x10),
			n2n.NewChainPointOrigin(),
			testChainPoint(9, 9, 0x09),
		},
		"duplicate": {
			testChainPoint(10, 10, 0x10),
			testChainPoint(10, 10, 0x10),
		},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := orderedCandidates(points, true); err == nil {
				t.Fatal("expected candidate-order rejection")
			}
		})
	}
	if _, err := orderedCandidates(
		[]n2n.ChainPoint{n2n.NewChainPointOrigin()},
		false,
	); err == nil {
		t.Fatal("partial dataset accepted Origin candidate")
	}
	equalHeight, err := orderedCandidates(
		[]n2n.ChainPoint{
			n2n.NewByronEBBChainPoint(testPoint(11, 0x11), 10),
			testChainPoint(10, 10, 0x10),
		},
		false,
	)
	if err != nil || len(equalHeight) != 2 {
		t.Fatalf("valid equal-height Byron restart candidates: %#v, %v", equalHeight, err)
	}
	equalSlot, err := orderedCandidates(
		[]n2n.ChainPoint{
			testChainPoint(11, 11, 0x12),
			n2n.NewByronEBBChainPoint(testPoint(11, 0x11), 10),
		},
		false,
	)
	if err != nil || len(equalSlot) != 2 {
		t.Fatalf("valid equal-slot Byron restart candidates: %#v, %v", equalSlot, err)
	}
	for name, points := range map[string][]n2n.ChainPoint{
		"equal height regular pair": {
			testChainPoint(11, 10, 0x11),
			testChainPoint(10, 10, 0x10),
		},
		"equal height wrong EBB side": {
			testChainPoint(11, 10, 0x11),
			n2n.NewByronEBBChainPoint(testPoint(10, 0x10), 10),
		},
		"equal slot regular pair": {
			testChainPoint(11, 11, 0x12),
			testChainPoint(11, 10, 0x11),
		},
		"equal slot wrong height": {
			testChainPoint(11, 12, 0x12),
			n2n.NewByronEBBChainPoint(testPoint(11, 0x11), 10),
		},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := orderedCandidates(points, false); err == nil {
				t.Fatal("accepted corrupt Byron candidate shape")
			}
		})
	}
	if _, err := orderedCandidates(
		[]n2n.ChainPoint{
			testChainPoint(11, 10, 0x11),
			testChainPoint(10, 11, 0x10),
		},
		false,
	); err == nil {
		t.Fatal("older candidate with increasing height was accepted")
	}
}

type fakeCandidates struct {
	points []n2n.ChainPoint
	load   func(int) []n2n.ChainPoint
	calls  int
}

func (f *fakeCandidates) IntersectionCandidates(context.Context) ([]n2n.ChainPoint, error) {
	f.calls++
	points := f.points
	if f.load != nil {
		points = f.load(f.calls)
	}
	ret := make([]n2n.ChainPoint, len(points))
	for index, point := range points {
		ret[index] = cloneChainPoint(point)
	}
	return ret, nil
}

type fakeObserver struct {
	observations []Observation
	err          error
}

func (f *fakeObserver) Observe(_ context.Context, observation Observation) error {
	if f.err != nil {
		return f.err
	}
	observation.Checkpoint = clonePoint(observation.Checkpoint)
	observation.Tip = cloneTip(observation.Tip)
	f.observations = append(f.observations, observation)
	return nil
}

type probeCall struct {
	peer  n2n.Peer
	point pcommon.Point
}

type rollbackProbeCall struct {
	peer   n2n.Peer
	target pcommon.Point
	branch pcommon.Point
}

type followCall struct {
	peer       n2n.Peer
	candidates []n2n.ChainPoint
}

type fakeTransport struct {
	probe         func(context.Context, n2n.Peer, pcommon.Point) (ProbeResult, error)
	rollbackProbe func(
		context.Context,
		n2n.Peer,
		pcommon.Point,
		pcommon.Point,
	) (RollbackProbeResult, error)
	follow         func(context.Context, n2n.Peer, []n2n.ChainPoint, n2n.Handler) error
	probes         []probeCall
	rollbackProbes []rollbackProbeCall
	follows        []followCall
}

func (f *fakeTransport) Probe(
	ctx context.Context,
	peer n2n.Peer,
	point pcommon.Point,
) (ProbeResult, error) {
	f.probes = append(f.probes, probeCall{peer: peer, point: clonePoint(point)})
	if f.probe == nil {
		return ProbeResult{}, errors.New("unexpected Probe")
	}
	return f.probe(ctx, peer, point)
}

func (f *fakeTransport) ProbeRollback(
	ctx context.Context,
	peer n2n.Peer,
	target pcommon.Point,
	branch pcommon.Point,
) (RollbackProbeResult, error) {
	f.rollbackProbes = append(f.rollbackProbes, rollbackProbeCall{
		peer:   peer,
		target: clonePoint(target),
		branch: clonePoint(branch),
	})
	if f.rollbackProbe == nil {
		return RollbackProbeResult{}, errors.New("unexpected ProbeRollback")
	}
	return f.rollbackProbe(ctx, peer, target, branch)
}

func (f *fakeTransport) Follow(
	ctx context.Context,
	peer n2n.Peer,
	candidates []n2n.ChainPoint,
	handler n2n.Handler,
) error {
	cloned := make([]n2n.ChainPoint, len(candidates))
	for index, point := range candidates {
		cloned[index] = cloneChainPoint(point)
	}
	f.follows = append(f.follows, followCall{peer: peer, candidates: cloned})
	if f.follow == nil {
		return errors.New("unexpected Follow")
	}
	return f.follow(ctx, peer, candidates, handler)
}

type fakeHandler struct {
	reconcile        func(context.Context, n2n.ChainPoint, SourceEvidence) (CommitOutcome, error)
	rollForward      func(context.Context, lcommon.Block, chainsync.Tip, SourceEvidence) (CommitOutcome, error)
	rollbackObserved func(context.Context, n2n.ChainPoint, chainsync.Tip) error
	rollBackward     func(
		context.Context,
		n2n.ChainPoint,
		chainsync.Tip,
		RollbackEvidence,
	) (CommitOutcome, error)
	endAttempt func(context.Context, AttemptEnd) (CommitOutcome, error)
}

func (f *fakeHandler) RollbackObserved(
	ctx context.Context,
	point n2n.ChainPoint,
	tip chainsync.Tip,
) error {
	if f.rollbackObserved == nil {
		return nil
	}
	return f.rollbackObserved(ctx, point, tip)
}

func (f *fakeHandler) Reconcile(
	ctx context.Context,
	point n2n.ChainPoint,
	evidence SourceEvidence,
) (CommitOutcome, error) {
	if f.reconcile == nil {
		return CommitOutcome{}, nil
	}
	return f.reconcile(ctx, point, evidence)
}

func (f *fakeHandler) RollForward(
	ctx context.Context,
	block lcommon.Block,
	tip chainsync.Tip,
	evidence SourceEvidence,
) (CommitOutcome, error) {
	if f.rollForward == nil {
		return CommitOutcome{}, errors.New("unexpected RollForward")
	}
	return f.rollForward(ctx, block, tip, evidence)
}

func (f *fakeHandler) RollBackward(
	ctx context.Context,
	point n2n.ChainPoint,
	tip chainsync.Tip,
	evidence RollbackEvidence,
) (CommitOutcome, error) {
	if f.rollBackward == nil {
		return CommitOutcome{}, errors.New("unexpected RollBackward")
	}
	return f.rollBackward(ctx, point, tip, evidence)
}

func (f *fakeHandler) EndAttempt(
	ctx context.Context,
	end AttemptEnd,
) (CommitOutcome, error) {
	if f.endAttempt == nil {
		return CommitOutcome{}, nil
	}
	return f.endAttempt(ctx, end)
}

func baseConfig() Config {
	return Config{
		Peers: []n2n.Peer{
			testPeer("relay-a:3001", "operator-a"),
			testPeer("relay-b:3001", "operator-b"),
		},
		Corroboration:         2,
		InitialBackoff:        time.Millisecond,
		MaxBackoff:            4 * time.Millisecond,
		RollbackConfirmations: 2,
		CheckpointEveryBlocks: 100,
		FinalizeTimeout:       time.Second,
	}
}

func newTestSupervisor(
	t *testing.T,
	config Config,
	candidates CandidateSource,
	handler Handler,
	observer Observer,
	transport Transport,
) *Supervisor {
	t.Helper()
	supervisor, err := New(config, candidates, handler, observer, transport)
	if err != nil {
		t.Fatal(err)
	}
	supervisor.now = func() time.Time {
		return time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	}
	return supervisor
}

func acceptingTransport() *fakeTransport {
	return &fakeTransport{
		probe: func(_ context.Context, _ n2n.Peer, point pcommon.Point) (ProbeResult, error) {
			return ProbeResult{
				Accepted:   true,
				Tip:        chainsync.Tip{Point: clonePoint(point), BlockNumber: point.Slot},
				N2NVersion: 15,
				Address:    "192.0.2.10:3001",
			}, nil
		},
	}
}

func oneBlockFollow(block lcommon.Block) func(
	context.Context,
	n2n.Peer,
	[]n2n.ChainPoint,
	n2n.Handler,
) error {
	return func(
		ctx context.Context,
		peer n2n.Peer,
		points []n2n.ChainPoint,
		handler n2n.Handler,
	) error {
		actualTip := chainsync.Tip{
			Point:       clonePoint(points[0].Point),
			BlockNumber: points[0].BlockNumber,
		}
		peer = prepareFollowPeer(peer, actualTip)
		if err := handler.Reconcile(ctx, points[0], peer); err != nil {
			return err
		}
		return handler.RollForward(ctx, block, tipForBlock(block), peer)
	}
}

func committingForward(
	_ context.Context,
	block lcommon.Block,
	tip chainsync.Tip,
	_ SourceEvidence,
) (CommitOutcome, error) {
	last := n2n.NewChainPoint(
		pcommon.NewPoint(block.SlotNumber(), block.Hash().Bytes()),
		block.BlockNumber(),
	)
	if isByronEBBBlock(block) {
		last = n2n.NewByronEBBChainPoint(last.Point, last.BlockNumber)
	}
	return CommitOutcome{
		Accepted:           true,
		Committed:          true,
		CommittedBlocks:    1,
		LastCommittedPoint: &last,
		LastCommittedTip:   &tip,
	}, nil
}

func testPeer(host, operator string) n2n.Peer {
	return n2n.Peer{Host: host, Operator: operator}
}

func actualPeer(peer n2n.Peer, address string, version uint16) n2n.Peer {
	peer.Address = address
	peer.N2NVersion = version
	return peer
}

func prepareFollowPeer(peer n2n.Peer, tip chainsync.Tip) n2n.Peer {
	peer.N2NVersion = 15
	peer.Tip = &tip
	return peer
}

func testPoint(slot uint64, fill byte) pcommon.Point {
	return pcommon.NewPoint(slot, bytesOf(fill, 32))
}

func testChainPoint(slot, blockNumber uint64, fill byte) n2n.ChainPoint {
	return n2n.NewChainPoint(testPoint(slot, fill), blockNumber)
}

func chainPointFromPoint(point pcommon.Point) n2n.ChainPoint {
	if pointIsOrigin(point) {
		return n2n.NewChainPointOrigin()
	}
	return n2n.NewChainPoint(point, point.Slot)
}

func pointIsOrigin(point pcommon.Point) bool {
	return point.Slot == 0 && len(point.Hash) == 0
}

func peerHosts(calls []followCall) []string {
	ret := make([]string, len(calls))
	for index, call := range calls {
		ret[index] = call.peer.Host
	}
	return ret
}

func peerAddresses(calls []followCall) []string {
	ret := make([]string, len(calls))
	for index, call := range calls {
		ret[index] = call.peer.Address
	}
	return ret
}

func observationsOfKind(observations []Observation, kind string) []Observation {
	return slices.DeleteFunc(slices.Clone(observations), func(value Observation) bool {
		return value.Kind != kind
	})
}

type testLedgerBlock struct {
	slot   uint64
	number uint64
	hash   lcommon.Blake2b256
}

func testBlock(slot, number uint64, fill byte) testLedgerBlock {
	var hash lcommon.Blake2b256
	copy(hash[:], bytesOf(fill, len(hash)))
	return testLedgerBlock{slot: slot, number: number, hash: hash}
}

func tipForBlock(block lcommon.Block) chainsync.Tip {
	return chainsync.Tip{
		Point:       pcommon.NewPoint(block.SlotNumber(), block.Hash().Bytes()),
		BlockNumber: block.BlockNumber(),
	}
}

func (b testLedgerBlock) Hash() lcommon.Blake2b256          { return b.hash }
func (b testLedgerBlock) PrevHash() lcommon.Blake2b256      { return lcommon.Blake2b256{} }
func (b testLedgerBlock) BlockNumber() uint64               { return b.number }
func (b testLedgerBlock) SlotNumber() uint64                { return b.slot }
func (b testLedgerBlock) IssuerVkey() lcommon.IssuerVkey    { return lcommon.IssuerVkey{} }
func (b testLedgerBlock) BlockBodySize() uint64             { return 0 }
func (b testLedgerBlock) Era() lcommon.Era                  { return lcommon.Era{Name: "test"} }
func (b testLedgerBlock) Cbor() []byte                      { return nil }
func (b testLedgerBlock) BlockBodyHash() lcommon.Blake2b256 { return lcommon.Blake2b256{} }
func (b testLedgerBlock) Header() lcommon.BlockHeader       { return b }
func (b testLedgerBlock) Type() int                         { return 0 }
func (b testLedgerBlock) Transactions() []lcommon.Transaction {
	return nil
}
func (b testLedgerBlock) Utxorpc() (*utxorpc.Block, error) { return nil, nil }

func bytesOf(value byte, count int) []byte {
	ret := make([]byte, count)
	for index := range ret {
		ret[index] = value
	}
	return ret
}
