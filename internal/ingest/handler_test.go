package ingest

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/protocol/chainsync"
	pcommon "github.com/blinklabs-io/gouroboros/protocol/common"
	utxorpc "github.com/utxorpc/go-codegen/utxorpc/v1alpha/cardano"

	"clicksync/internal/n2n"
	"clicksync/internal/publication"
	"clicksync/internal/syncer"
)

type fakePublisher struct {
	batches        []publication.Batch
	rollbacks      []publication.RollbackRequest
	publish        func(publication.Batch) (publication.BatchResult, error)
	publishContext func(
		context.Context,
		publication.Batch,
	) (publication.BatchResult, error)
	rollback func(publication.RollbackRequest) error
}

func (publisher *fakePublisher) PublishBatch(
	ctx context.Context,
	batch publication.Batch,
) (publication.BatchResult, error) {
	publisher.batches = append(publisher.batches, batch)
	if publisher.publishContext != nil {
		return publisher.publishContext(ctx, batch)
	}
	if publisher.publish != nil {
		return publisher.publish(batch)
	}
	last := batch.Items[len(batch.Items)-1].Block
	ids := make([]uint64, len(batch.Items))
	for index := range ids {
		ids[index] = uint64(index + 1)
	}
	return publication.BatchResult{
		PublicationIDs: ids,
		FirstEventSeq:  1,
		LastEventSeq:   uint64(len(ids)),
		LastCommitted: publication.Point{
			Slot:        last.Slot,
			Hash:        last.Hash,
			BlockNumber: last.Number,
			IsByronEBB: last.Era == "Byron" &&
				last.Type == int16(ledger.BlockTypeByronEbb),
		},
	}, nil
}

func (publisher *fakePublisher) Rollback(
	_ context.Context,
	request publication.RollbackRequest,
) error {
	publisher.rollbacks = append(publisher.rollbacks, request)
	if publisher.rollback != nil {
		return publisher.rollback(request)
	}
	return nil
}

type fakeChainState struct {
	tip publication.Point
}

func (*fakeChainState) CommittedSnapshot(context.Context) (uint64, error) {
	return 1, nil
}

func (state *fakeChainState) CommittedTip(
	context.Context,
	uint64,
) (publication.Point, error) {
	return state.tip, nil
}

func TestHandlerStagesThenFinalizesExactCommittedTail(t *testing.T) {
	publisher := &fakePublisher{}
	handler := newTestHandlerWithState(
		t,
		publisher,
		&fakeChainState{tip: publication.Point{Origin: true}},
	)
	block := adapterTestBlock{slot: 10, number: 10, hash: adapterHash(0x10)}
	tip := adapterTip(block)
	outcome, err := handler.RollForward(
		context.Background(),
		block,
		tip,
		adapterEvidence(tip),
	)
	if err != nil {
		t.Fatal(err)
	}
	if !outcome.Accepted || outcome.Committed || len(publisher.batches) != 0 {
		t.Fatalf("staging outcome = %#v, batches=%d", outcome, len(publisher.batches))
	}
	outcome, err = handler.EndAttempt(
		context.Background(),
		syncer.AttemptEnd{Cause: "shutdown"},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !outcome.Committed ||
		outcome.CommittedBlocks != 1 ||
		outcome.LastCommittedPoint == nil ||
		outcome.LastCommittedPoint.BlockNumber != block.number ||
		outcome.LastCommittedTip == nil ||
		outcome.LastCommittedTip.BlockNumber != tip.BlockNumber {
		t.Fatalf("final outcome = %#v", outcome)
	}
	if len(publisher.batches) != 1 || len(publisher.batches[0].Items) != 1 {
		t.Fatalf("published batches = %#v", publisher.batches)
	}
}

func TestPhysicalBatchTimerStaysAnchoredToFirstStagedBlock(t *testing.T) {
	publisher := &fakePublisher{}
	handler := newTestHandler(t, publisher)
	first := adapterTestBlock{
		slot:   11,
		number: 11,
		hash:   adapterHash(0x11),
	}
	second := adapterTestBlock{
		slot:   12,
		number: 12,
		hash:   adapterHash(0x12),
	}
	if _, err := handler.RollForward(
		context.Background(),
		first,
		adapterTip(first),
		adapterEvidence(adapterTip(first)),
	); err != nil {
		t.Fatal(err)
	}
	firstTimer := handler.timer
	if firstTimer == nil {
		t.Fatal("first staged block did not arm physical batch timer")
	}
	if _, err := handler.RollForward(
		context.Background(),
		second,
		adapterTip(second),
		adapterEvidence(adapterTip(second)),
	); err != nil {
		t.Fatal(err)
	}
	if handler.timer != firstTimer {
		t.Fatal("later staged block reset the first-staged flush deadline")
	}
	if err := handler.RollbackObserved(
		context.Background(),
		n2n.NewChainPoint(
			pcommon.NewPoint(first.slot, first.hash.Bytes()),
			first.number,
		),
		adapterTip(second),
	); err != nil {
		t.Fatal(err)
	}
	handler.flushFromTimer()
	if len(publisher.batches) != 0 {
		t.Fatalf(
			"stale timer published after rollback barrier: %#v",
			publisher.batches,
		)
	}
}

func TestExpiredPhysicalBatchTimerFlushesAfterHandlerMutexContention(t *testing.T) {
	publisher := &fakePublisher{}
	handler := newTestHandler(t, publisher)
	handler.config.FlushAfter = 10 * time.Second
	for number := uint64(11); number <= 12; number++ {
		block := adapterTestBlock{
			slot:   number,
			number: number,
			hash:   adapterHash(byte(number)),
		}
		if _, err := handler.RollForward(
			context.Background(),
			block,
			adapterTip(block),
			adapterEvidence(adapterTip(block)),
		); err != nil {
			t.Fatal(err)
		}
	}

	handler.mu.Lock()
	if handler.timer == nil || !handler.timer.Stop() {
		handler.mu.Unlock()
		t.Fatal("physical batch timer was not armed before contention")
	}
	timerStarted := make(chan struct{})
	timerDone := make(chan struct{})
	go func() {
		close(timerStarted)
		handler.flushFromTimer()
		close(timerDone)
	}()
	<-timerStarted
	select {
	case <-timerDone:
		handler.mu.Unlock()
		t.Fatal("timer callback bypassed handler mutex")
	case <-time.After(10 * time.Millisecond):
	}
	handler.mu.Unlock()
	select {
	case <-timerDone:
	case <-time.After(time.Second):
		t.Fatal("expired timer did not flush at first opportunity")
	}
	if len(publisher.batches) != 1 ||
		len(publisher.batches[0].Items) != 2 {
		t.Fatalf("timer batches = %#v", publisher.batches)
	}
}

func TestTimerWaitingOnMutexDefersToExhaustedSharedShutdownBudget(t *testing.T) {
	runCtx, cancelRun := context.WithCancel(context.Background())
	budget, err := syncer.NewShutdownBudget(30 * time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	publisher := &fakePublisher{}
	handler := newTimerBudgetHandler(
		t,
		runCtx,
		publisher,
		budget,
		time.Second,
	)
	stageTimerTestBlock(t, handler)

	handler.mu.Lock()
	if handler.timer == nil || !handler.timer.Stop() {
		handler.mu.Unlock()
		t.Fatal("physical batch timer was not armed")
	}
	timerDone := make(chan struct{})
	go func() {
		handler.flushFromTimer()
		close(timerDone)
	}()
	cancelRun()
	shutdownCtx, stopShutdown := budget.Context(runCtx)
	<-shutdownCtx.Done()
	stopShutdown()
	handler.mu.Unlock()
	select {
	case <-timerDone:
	case <-time.After(time.Second):
		t.Fatal("timer callback remained blocked after mutex release")
	}
	handler.mu.Lock()
	pending := len(handler.pending)
	terminalErr := handler.terminalErr
	handler.mu.Unlock()
	if len(publisher.batches) != 0 || pending != 1 || terminalErr != nil {
		t.Fatalf(
			"batches=%d pending=%d terminal=%v",
			len(publisher.batches),
			pending,
			terminalErr,
		)
	}
}

func TestTimerPublishBegunBeforeCancellationStopsAtSharedDeadline(t *testing.T) {
	runCtx, cancelRun := context.WithCancel(context.Background())
	budget, err := syncer.NewShutdownBudget(30 * time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	publishStarted := make(chan struct{})
	publisher := &fakePublisher{
		publishContext: func(
			ctx context.Context,
			_ publication.Batch,
		) (publication.BatchResult, error) {
			close(publishStarted)
			<-ctx.Done()
			return publication.BatchResult{}, ctx.Err()
		},
	}
	handler := newTimerBudgetHandler(
		t,
		runCtx,
		publisher,
		budget,
		time.Second,
	)
	stageTimerTestBlock(t, handler)
	stopTimerForManualCallback(t, handler)
	timerDone := make(chan struct{})
	go func() {
		handler.flushFromTimer()
		close(timerDone)
	}()
	<-publishStarted
	signaledAt := time.Now()
	cancelRun()
	select {
	case <-timerDone:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("timer publication ignored shared shutdown deadline")
	}
	if elapsed := time.Since(signaledAt); elapsed > 180*time.Millisecond {
		t.Fatalf("timer publication took %s after cancellation", elapsed)
	}
	handler.mu.Lock()
	terminalErr := handler.terminalErr
	handler.mu.Unlock()
	if !errors.Is(terminalErr, context.Canceled) {
		t.Fatalf("timer terminal error = %v", terminalErr)
	}
	auditCtx, stopAudit := budget.Context(runCtx)
	defer stopAudit()
	if !errors.Is(auditCtx.Err(), context.DeadlineExceeded) {
		t.Fatalf("shared audit context remained live: %v", auditCtx.Err())
	}
}

func TestNormalTimerPublishRetainsIndependentFlushTimeout(t *testing.T) {
	const (
		flushTimeout  = 20 * time.Millisecond
		shutdownLimit = 250 * time.Millisecond
	)
	budget, err := syncer.NewShutdownBudget(shutdownLimit)
	if err != nil {
		t.Fatal(err)
	}
	publishStarted := make(chan struct{})
	publisher := &fakePublisher{
		publishContext: func(
			ctx context.Context,
			_ publication.Batch,
		) (publication.BatchResult, error) {
			close(publishStarted)
			<-ctx.Done()
			return publication.BatchResult{}, ctx.Err()
		},
	}
	handler := newTimerBudgetHandler(
		t,
		context.Background(),
		publisher,
		budget,
		flushTimeout,
	)
	stageTimerTestBlock(t, handler)
	stopTimerForManualCallback(t, handler)
	timerDone := make(chan struct{})
	startedAt := time.Now()
	go func() {
		handler.flushFromTimer()
		close(timerDone)
	}()
	<-publishStarted
	select {
	case <-timerDone:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("normal timer ignored its independent flush timeout")
	}
	if elapsed := time.Since(startedAt); elapsed > 150*time.Millisecond {
		t.Fatalf("normal timer took %s for %s timeout", elapsed, flushTimeout)
	}
	handler.mu.Lock()
	terminalErr := handler.terminalErr
	handler.mu.Unlock()
	if !errors.Is(terminalErr, context.DeadlineExceeded) {
		t.Fatalf("normal timer terminal error = %v", terminalErr)
	}
	shutdownCtx, stopShutdown := budget.Context(context.Background())
	defer stopShutdown()
	deadline, ok := shutdownCtx.Deadline()
	if !ok || time.Until(deadline) < 150*time.Millisecond {
		t.Fatalf("normal timer consumed shared shutdown budget: %v", deadline)
	}
}

func TestNormalTimerMutexTimeoutRearmsPendingBatch(t *testing.T) {
	budget, err := syncer.NewShutdownBudget(250 * time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	publisher := &fakePublisher{}
	handler := newTimerBudgetHandler(
		t,
		context.Background(),
		publisher,
		budget,
		20*time.Millisecond,
	)
	stageTimerTestBlock(t, handler)
	handler.mu.Lock()
	if handler.timer == nil || !handler.timer.Stop() {
		handler.mu.Unlock()
		t.Fatal("physical batch timer was not armed")
	}
	timerDone := make(chan struct{})
	go func() {
		handler.flushFromTimer()
		close(timerDone)
	}()
	<-time.After(40 * time.Millisecond)
	handler.mu.Unlock()
	select {
	case <-timerDone:
	case <-time.After(time.Second):
		t.Fatal("normal timer remained blocked after mutex release")
	}
	handler.mu.Lock()
	pending := len(handler.pending)
	rearmed := handler.timer != nil
	handler.stopTimerLocked()
	terminalErr := handler.terminalErr
	handler.mu.Unlock()
	if len(publisher.batches) != 0 ||
		pending != 1 ||
		!rearmed ||
		terminalErr != nil {
		t.Fatalf(
			"batches=%d pending=%d rearmed=%t terminal=%v",
			len(publisher.batches),
			pending,
			rearmed,
			terminalErr,
		)
	}
}

func TestObservedRollbackBarrierPreventsAttemptFinalizationFromPublishingDescendants(
	t *testing.T,
) {
	for _, scenario := range []string{
		"paired proof unavailable",
		"paired proof disagreement",
		"context cancellation during proof",
	} {
		t.Run(scenario, func(t *testing.T) {
			publisher := &fakePublisher{}
			checkpoint := testAdapterChainPoint(10, 0x10)
			handler := newTestHandlerWithState(
				t,
				publisher,
				&fakeChainState{tip: publicationPoint(checkpoint)},
			)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			transport := &rollbackBarrierTransport{
				scenario:   scenario,
				cancel:     cancel,
				checkpoint: checkpoint,
				block: adapterTestBlock{
					slot:   11,
					number: 11,
					hash:   adapterHash(0x11),
				},
				branchTip: chainsync.Tip{
					Point:       pcommon.NewPoint(12, adapterHash(0x12).Bytes()),
					BlockNumber: 12,
				},
			}
			supervisor, err := syncer.New(
				syncer.Config{
					Peers: []n2n.Peer{
						{Host: "relay-a:3001", Operator: "operator-a"},
						{Host: "relay-b:3001", Operator: "operator-b"},
					},
					Corroboration:         2,
					InitialBackoff:        time.Millisecond,
					MaxBackoff:            2 * time.Millisecond,
					RollbackConfirmations: 2,
					CheckpointEveryBlocks: 100,
					FinalizeTimeout:       time.Second,
				},
				fixedCandidateSource{checkpoint},
				handler,
				noopSyncObserver{},
				transport,
			)
			if err != nil {
				t.Fatal(err)
			}
			if err := supervisor.Run(ctx); err == nil {
				t.Fatal("failed rollback proof unexpectedly completed sync")
			}
			if len(publisher.batches) != 0 ||
				len(publisher.rollbacks) != 0 {
				t.Fatalf(
					"failed proof published batches=%d rollbacks=%d",
					len(publisher.batches),
					len(publisher.rollbacks),
				)
			}
		})
	}
}

type fixedCandidateSource []n2n.ChainPoint

func (source fixedCandidateSource) IntersectionCandidates(
	context.Context,
) ([]n2n.ChainPoint, error) {
	return append([]n2n.ChainPoint(nil), source...), nil
}

type noopSyncObserver struct{}

func (noopSyncObserver) Observe(
	context.Context,
	syncer.Observation,
) error {
	return nil
}

type rollbackBarrierTransport struct {
	scenario             string
	cancel               context.CancelFunc
	stopAfterUnavailable bool
	checkpoint           n2n.ChainPoint
	rollbackTarget       n2n.ChainPoint
	block                adapterTestBlock
	blocks               []adapterTestBlock
	branchTip            chainsync.Tip
	beforeRollback       func()
}

func (transport *rollbackBarrierTransport) Probe(
	_ context.Context,
	peer n2n.Peer,
	_ pcommon.Point,
) (syncer.ProbeResult, error) {
	if transport.stopAfterUnavailable {
		transport.cancel()
		return syncer.ProbeResult{}, context.Canceled
	}
	return syncer.ProbeResult{
		Accepted: true,
		Tip: chainsync.Tip{
			Point:       transport.branchTip.Point,
			BlockNumber: transport.branchTip.BlockNumber,
		},
		N2NVersion: 15,
		Address:    peer.Host,
	}, nil
}

func (transport *rollbackBarrierTransport) ProbeRollback(
	_ context.Context,
	_ n2n.Peer,
	_ pcommon.Point,
	_ pcommon.Point,
) (syncer.RollbackProbeResult, error) {
	switch transport.scenario {
	case "paired proof unavailable":
		transport.stopAfterUnavailable = true
		return syncer.RollbackProbeResult{},
			syncer.RetryableTransportError(errors.New("relay unavailable"))
	case "paired proof disagreement":
		return syncer.RollbackProbeResult{
			TargetAccepted: false,
			BranchAccepted: true,
			Tip:            transport.branchTip,
			N2NVersion:     15,
			Address:        "relay-b:3001",
		}, nil
	case "context cancellation during proof":
		transport.cancel()
		return syncer.RollbackProbeResult{}, context.Canceled
	case "paired proof success":
		return syncer.RollbackProbeResult{
			TargetAccepted: true,
			BranchAccepted: true,
			Tip:            transport.branchTip,
			N2NVersion:     15,
			Address:        "relay-b:3001",
		}, nil
	default:
		return syncer.RollbackProbeResult{}, errors.New("unknown scenario")
	}
}

func (transport *rollbackBarrierTransport) Follow(
	ctx context.Context,
	peer n2n.Peer,
	candidates []n2n.ChainPoint,
	handler n2n.Handler,
) error {
	if len(candidates) != 1 {
		return errors.New("expected one selected checkpoint")
	}
	peer.Address = peer.Host
	peer.N2NVersion = 15
	peer.Tip = &transport.branchTip
	if err := handler.Reconcile(ctx, candidates[0], peer); err != nil {
		return err
	}
	blocks := transport.blocks
	if len(blocks) == 0 {
		blocks = []adapterTestBlock{transport.block}
	}
	for _, block := range blocks {
		if err := handler.RollForward(
			ctx,
			block,
			transport.branchTip,
			peer,
		); err != nil {
			return err
		}
	}
	if transport.beforeRollback != nil {
		transport.beforeRollback()
	}
	target := transport.rollbackTarget
	if len(target.Point.Hash) == 0 {
		target = transport.checkpoint
	}
	err := handler.RollBackward(
		ctx,
		target,
		transport.branchTip,
		peer,
	)
	if err == nil && transport.scenario == "paired proof success" {
		transport.cancel()
		return ctx.Err()
	}
	return err
}

func TestProvenPendingRollbackTargetAcknowledgesDurableTipAndReplays(t *testing.T) {
	publisher := &fakePublisher{}
	handler := newTestHandlerWithState(
		t,
		publisher,
		&fakeChainState{tip: publication.Point{Origin: true}},
	)
	var blocks []adapterTestBlock
	for number := uint64(10); number <= 12; number++ {
		block := adapterTestBlock{
			slot:   number,
			number: number,
			hash:   adapterHash(byte(number)),
		}
		blocks = append(blocks, block)
		tip := adapterTip(block)
		if _, err := handler.RollForward(
			context.Background(),
			block,
			tip,
			adapterEvidence(tip),
		); err != nil {
			t.Fatal(err)
		}
	}
	target := n2n.NewChainPoint(
		pcommon.NewPoint(blocks[0].slot, blocks[0].hash.Bytes()),
		blocks[0].number,
	)
	if err := handler.RollbackObserved(
		context.Background(),
		target,
		adapterTip(blocks[2]),
	); err != nil {
		t.Fatal(err)
	}
	outcome, err := handler.RollBackward(
		context.Background(),
		target,
		adapterTip(blocks[2]),
		adapterRollbackEvidence(target, adapterTip(blocks[2])),
	)
	var replay *syncer.RollbackReplayRequiredError
	if !errors.As(err, &replay) {
		t.Fatalf("error = %T %v", err, err)
	}
	if !outcome.Committed ||
		outcome.CommittedBlocks != 0 ||
		outcome.LastCommittedPoint != nil {
		t.Fatalf("rollback outcome = %#v", outcome)
	}
	if len(publisher.batches) != 0 {
		t.Fatalf("staged target was published: %#v", publisher.batches)
	}
	if len(publisher.rollbacks) != 1 ||
		!publisher.rollbacks[0].To.Origin ||
		!samePublicationPoint(
			publicationPoint(replay.ObservedTarget),
			publicationPoint(target),
		) ||
		replay.DurableTip.Point.Slot != 0 ||
		len(replay.DurableTip.Point.Hash) != 0 {
		t.Fatalf("rollback requests = %#v", publisher.rollbacks)
	}
}

func TestUnconfirmedRollbackToPendingTargetPublishesNothing(t *testing.T) {
	publisher := &fakePublisher{}
	handler := newTestHandler(t, publisher)
	var blocks []adapterTestBlock
	for number := uint64(11); number <= 12; number++ {
		block := adapterTestBlock{
			slot:   number,
			number: number,
			hash:   adapterHash(byte(number)),
		}
		blocks = append(blocks, block)
		if _, err := handler.RollForward(
			context.Background(),
			block,
			adapterTip(block),
			adapterEvidence(adapterTip(block)),
		); err != nil {
			t.Fatal(err)
		}
	}
	target := n2n.NewChainPoint(
		pcommon.NewPoint(blocks[0].slot, blocks[0].hash.Bytes()),
		blocks[0].number,
	)
	if err := handler.RollbackObserved(
		context.Background(),
		target,
		adapterTip(blocks[1]),
	); err != nil {
		t.Fatal(err)
	}
	outcome, err := handler.EndAttempt(
		context.Background(),
		syncer.AttemptEnd{Cause: "rollback_proof_unavailable"},
	)
	if err != nil {
		t.Fatal(err)
	}
	if outcome.Committed ||
		outcome.CommittedBlocks != 0 ||
		len(publisher.batches) != 0 ||
		len(publisher.rollbacks) != 0 {
		t.Fatalf(
			"outcome=%#v batches=%#v rollbacks=%#v",
			outcome,
			publisher.batches,
			publisher.rollbacks,
		)
	}
}

func TestAgedProofNeverPublishesStalePendingPrefix(t *testing.T) {
	publisher := &fakePublisher{}
	handler := newTestHandlerWithState(
		t,
		publisher,
		&fakeChainState{tip: publication.Point{Origin: true}},
	)
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	handler.config.Now = func() time.Time { return now }
	first := adapterTestBlock{
		slot:   11,
		number: 11,
		hash:   adapterHash(0x11),
	}
	second := adapterTestBlock{
		slot:   12,
		number: 12,
		hash:   adapterHash(0x12),
	}
	for _, block := range []adapterTestBlock{first, second} {
		if _, err := handler.RollForward(
			context.Background(),
			block,
			adapterTip(block),
			adapterEvidence(adapterTip(block)),
		); err != nil {
			t.Fatal(err)
		}
	}
	target := n2n.NewChainPoint(
		pcommon.NewPoint(first.slot, first.hash.Bytes()),
		first.number,
	)
	branchTip := adapterTip(second)
	if err := handler.RollbackObserved(
		context.Background(),
		target,
		branchTip,
	); err != nil {
		t.Fatal(err)
	}
	now = now.Add(publication.MaxBatchAge + time.Second)
	outcome, err := handler.RollBackward(
		context.Background(),
		target,
		branchTip,
		adapterRollbackEvidence(target, branchTip),
	)
	var replay *syncer.RollbackReplayRequiredError
	if !errors.As(err, &replay) ||
		!outcome.Committed ||
		outcome.CommittedBlocks != 0 ||
		len(publisher.batches) != 0 ||
		len(publisher.rollbacks) != 1 ||
		!publisher.rollbacks[0].To.Origin {
		t.Fatalf(
			"outcome=%#v error=%v batches=%#v rollbacks=%#v",
			outcome,
			err,
			publisher.batches,
			publisher.rollbacks,
		)
	}
}

func TestTimerPublicationBeforeRollbackCannotReportInvalidatedTail(t *testing.T) {
	publisher := &fakePublisher{}
	handler := newTestHandler(t, publisher)
	var blocks []adapterTestBlock
	for number := uint64(11); number <= 12; number++ {
		block := adapterTestBlock{
			slot:   number,
			number: number,
			hash:   adapterHash(byte(number)),
		}
		blocks = append(blocks, block)
		if _, err := handler.RollForward(
			context.Background(),
			block,
			adapterTip(block),
			adapterEvidence(adapterTip(block)),
		); err != nil {
			t.Fatal(err)
		}
	}
	handler.flushFromTimer()
	if len(publisher.batches) != 1 {
		t.Fatalf("timer batches = %#v", publisher.batches)
	}
	target := n2n.NewChainPoint(
		pcommon.NewPoint(blocks[0].slot, blocks[0].hash.Bytes()),
		blocks[0].number,
	)
	branchTip := adapterTip(blocks[1])
	if err := handler.RollbackObserved(
		context.Background(),
		target,
		branchTip,
	); err != nil {
		t.Fatal(err)
	}
	outcome, err := handler.RollBackward(
		context.Background(),
		target,
		branchTip,
		adapterRollbackEvidence(target, branchTip),
	)
	if err != nil {
		t.Fatal(err)
	}
	if !outcome.Committed ||
		outcome.CommittedBlocks != 0 ||
		outcome.LastCommittedPoint != nil ||
		outcome.LastCommittedTip != nil ||
		len(publisher.rollbacks) != 1 {
		t.Fatalf(
			"outcome=%#v rollbacks=%#v",
			outcome,
			publisher.rollbacks,
		)
	}
}

func TestSupervisorAcceptsTimerFlushedRollbackWithoutStaleTail(t *testing.T) {
	publisher := &fakePublisher{}
	checkpoint := testAdapterChainPoint(10, 0x10)
	target := testAdapterChainPoint(11, 0x11)
	handler := newTestHandlerWithState(
		t,
		publisher,
		&fakeChainState{tip: publicationPoint(checkpoint)},
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	transport := &rollbackBarrierTransport{
		scenario:       "paired proof success",
		cancel:         cancel,
		checkpoint:     checkpoint,
		rollbackTarget: target,
		blocks: []adapterTestBlock{
			{slot: 11, number: 11, hash: adapterHash(0x11)},
			{slot: 12, number: 12, hash: adapterHash(0x12)},
		},
		branchTip: chainsync.Tip{
			Point:       pcommon.NewPoint(12, adapterHash(0x12).Bytes()),
			BlockNumber: 12,
		},
		beforeRollback: handler.flushFromTimer,
	}
	supervisor, err := syncer.New(
		syncer.Config{
			Peers: []n2n.Peer{
				{Host: "relay-a:3001", Operator: "operator-a"},
				{Host: "relay-b:3001", Operator: "operator-b"},
			},
			Corroboration:         2,
			InitialBackoff:        time.Millisecond,
			MaxBackoff:            2 * time.Millisecond,
			RollbackConfirmations: 2,
			CheckpointEveryBlocks: 100,
			FinalizeTimeout:       time.Second,
		},
		fixedCandidateSource{checkpoint},
		handler,
		noopSyncObserver{},
		transport,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := supervisor.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("supervisor treated stale timer tail as terminal: %v", err)
	}
	if len(publisher.batches) != 1 ||
		len(publisher.batches[0].Items) != 2 ||
		len(publisher.rollbacks) != 1 ||
		!samePublicationPoint(
			publisher.rollbacks[0].To,
			publicationPoint(target),
		) {
		t.Fatalf(
			"batches=%#v rollbacks=%#v",
			publisher.batches,
			publisher.rollbacks,
		)
	}
}

func TestCommittedPublicationErrorIsReportedOnlyByFinalizer(t *testing.T) {
	publisher := &fakePublisher{
		publish: func(batch publication.Batch) (publication.BatchResult, error) {
			last := batch.Items[len(batch.Items)-1].Block
			return publication.BatchResult{
					PublicationIDs: []uint64{7},
					FirstEventSeq:  9,
					LastEventSeq:   9,
					LastCommitted: publication.Point{
						Slot:        last.Slot,
						Hash:        last.Hash,
						BlockNumber: last.Number,
					},
				}, &publication.CommittedError{
					PublicationID: 7,
					EventSeq:      9,
					Err:           errors.New("manifest cache"),
				}
		},
	}
	handler := newTestHandler(t, publisher)
	block := adapterTestBlock{slot: 10, number: 10, hash: adapterHash(0x10)}
	tip := adapterTip(block)
	if _, err := handler.RollForward(
		context.Background(),
		block,
		tip,
		adapterEvidence(tip),
	); err != nil {
		t.Fatal(err)
	}
	outcome, err := handler.EndAttempt(
		context.Background(),
		syncer.AttemptEnd{Cause: "shutdown"},
	)
	if err == nil || !outcome.Committed || outcome.CommittedBlocks != 1 {
		t.Fatalf("outcome=%#v error=%v", outcome, err)
	}
}

func TestHandlerRejectsPartialIdentitySetAndMismatchedEBBTail(t *testing.T) {
	for name, publish := range map[string]func(publication.Batch) (publication.BatchResult, error){
		"partial identities": func(batch publication.Batch) (publication.BatchResult, error) {
			last := batch.Items[0].Block
			return publication.BatchResult{
				PublicationIDs: []uint64{1, 2},
				FirstEventSeq:  1,
				LastEventSeq:   2,
				LastCommitted: publication.Point{
					Slot:        last.Slot,
					Hash:        last.Hash,
					BlockNumber: last.Number,
				},
			}, nil
		},
		"mismatched EBB type": func(batch publication.Batch) (publication.BatchResult, error) {
			last := batch.Items[0].Block
			return publication.BatchResult{
				PublicationIDs: []uint64{1},
				FirstEventSeq:  1,
				LastEventSeq:   1,
				LastCommitted: publication.Point{
					Slot:        last.Slot,
					Hash:        last.Hash,
					BlockNumber: last.Number,
					IsByronEBB:  false,
				},
			}, nil
		},
		"arbitrary error with full identities": func(batch publication.Batch) (publication.BatchResult, error) {
			last := batch.Items[0].Block
			return publication.BatchResult{
				PublicationIDs: []uint64{1},
				FirstEventSeq:  1,
				LastEventSeq:   1,
				LastCommitted: publication.Point{
					Slot:        last.Slot,
					Hash:        last.Hash,
					BlockNumber: last.Number,
					IsByronEBB:  true,
				},
			}, errors.New("untyped insert failure")
		},
	} {
		t.Run(name, func(t *testing.T) {
			publisher := &fakePublisher{publish: publish}
			handler := newTestHandler(t, publisher)
			block := adapterTestBlock{
				slot:   21_600,
				number: 20_000,
				hash:   adapterHash(0xee),
				era:    "Byron",
				kind:   int(ledger.BlockTypeByronEbb),
			}
			tip := adapterTip(block)
			if _, err := handler.RollForward(
				context.Background(),
				block,
				tip,
				adapterEvidence(tip),
			); err != nil {
				t.Fatal(err)
			}
			outcome, err := handler.EndAttempt(
				context.Background(),
				syncer.AttemptEnd{Cause: "shutdown"},
			)
			if err == nil || outcome.Committed {
				t.Fatalf("outcome=%#v error=%v", outcome, err)
			}
		})
	}
}

func TestSizeTriggeredCommittedPrefixDoesNotAcknowledgeCurrentBlock(t *testing.T) {
	publisher := &fakePublisher{
		publish: func(batch publication.Batch) (publication.BatchResult, error) {
			last := batch.Items[len(batch.Items)-1].Block
			return publication.BatchResult{
					PublicationIDs: []uint64{7},
					FirstEventSeq:  9,
					LastEventSeq:   9,
					LastCommitted: publication.Point{
						Slot:        last.Slot,
						Hash:        last.Hash,
						BlockNumber: last.Number,
					},
				}, &publication.CommittedError{
					PublicationID: 7,
					EventSeq:      9,
					Err:           errors.New("manifest cache"),
				}
		},
	}
	handler := newTestHandler(t, publisher)
	first := adapterTestBlock{slot: 10, number: 10, hash: adapterHash(0x10)}
	firstTip := adapterTip(first)
	if _, err := handler.RollForward(
		context.Background(),
		first,
		firstTip,
		adapterEvidence(firstTip),
	); err != nil {
		t.Fatal(err)
	}
	handler.mu.Lock()
	handler.pendingRows = publication.MaxBatchRows
	handler.mu.Unlock()
	current := adapterTestBlock{slot: 11, number: 11, hash: adapterHash(0x11)}
	outcome, err := handler.RollForward(
		context.Background(),
		current,
		adapterTip(current),
		adapterEvidence(adapterTip(current)),
	)
	if err == nil ||
		outcome.Accepted ||
		!outcome.Committed ||
		outcome.CommittedBlocks != 1 ||
		outcome.LastCommittedPoint == nil ||
		outcome.LastCommittedPoint.BlockNumber != first.number {
		t.Fatalf("outcome=%#v error=%v", outcome, err)
	}
}

func TestNormalizationFailureFinalizesSafePendingPrefix(t *testing.T) {
	publisher := &fakePublisher{}
	handler := newTestHandler(t, publisher)
	first := adapterTestBlock{slot: 10, number: 10, hash: adapterHash(0x10)}
	tip := adapterTip(first)
	if _, err := handler.RollForward(
		context.Background(),
		first,
		tip,
		adapterEvidence(tip),
	); err != nil {
		t.Fatal(err)
	}
	outcome, err := handler.RollForward(
		context.Background(),
		nil,
		tip,
		adapterEvidence(tip),
	)
	if err == nil ||
		outcome.Accepted ||
		!outcome.Committed ||
		outcome.CommittedBlocks != 1 ||
		outcome.LastCommittedPoint == nil ||
		outcome.LastCommittedPoint.BlockNumber != first.number {
		t.Fatalf("outcome=%#v error=%v", outcome, err)
	}
}

func TestRollbackReportsRetainedAdoptionWhenHeaderFailsPrecommit(t *testing.T) {
	publisher := &fakePublisher{
		rollback: func(publication.RollbackRequest) error {
			return errors.New("rollback header precommit failure")
		},
	}
	handler := newTestHandler(t, publisher)
	block := adapterTestBlock{slot: 10, number: 10, hash: adapterHash(0x10)}
	tip := adapterTip(block)
	if _, err := handler.RollForward(
		context.Background(),
		block,
		tip,
		adapterEvidence(tip),
	); err != nil {
		t.Fatal(err)
	}
	target := n2n.NewChainPoint(
		pcommon.NewPoint(block.slot, block.hash.Bytes()),
		block.number,
	)
	outcome, err := handler.RollBackward(
		context.Background(),
		target,
		tip,
		adapterRollbackEvidence(target, tip),
	)
	if err == nil ||
		!outcome.Committed ||
		outcome.CommittedBlocks != 1 ||
		outcome.LastCommittedPoint == nil ||
		!samePublicationPoint(publicationPoint(*outcome.LastCommittedPoint), publicationPoint(target)) {
		t.Fatalf("outcome=%#v error=%v", outcome, err)
	}
	if len(publisher.rollbacks) != 1 {
		t.Fatalf("rollback calls = %d, want 1", len(publisher.rollbacks))
	}
}

func newTestHandler(t *testing.T, publisher *fakePublisher) *Handler {
	return newTestHandlerWithState(t, publisher, &fakeChainState{})
}

func newTimerBudgetHandler(
	t *testing.T,
	runCtx context.Context,
	publisher *fakePublisher,
	budget *syncer.ShutdownBudget,
	flushTimeout time.Duration,
) *Handler {
	t.Helper()
	handler, err := NewHandler(
		runCtx,
		publisher,
		&fakeChainState{},
		HandlerConfig{
			NetworkMagic:          n2n.MainnetNetworkMagic,
			RollbackMaximumDepth:  100,
			RollbackCorroboration: 2,
			FlushAfter:            500 * time.Millisecond,
			FlushTimeout:          flushTimeout,
			ShutdownBudget:        budget,
			Now:                   time.Now,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	// Keep the real timer out of deterministic manual-callback tests.
	handler.config.FlushAfter = 10 * time.Second
	return handler
}

func stageTimerTestBlock(t *testing.T, handler *Handler) {
	t.Helper()
	block := adapterTestBlock{
		slot:   11,
		number: 11,
		hash:   adapterHash(0x11),
	}
	if _, err := handler.RollForward(
		context.Background(),
		block,
		adapterTip(block),
		adapterEvidence(adapterTip(block)),
	); err != nil {
		t.Fatal(err)
	}
}

func stopTimerForManualCallback(t *testing.T, handler *Handler) {
	t.Helper()
	handler.mu.Lock()
	defer handler.mu.Unlock()
	if handler.timer == nil || !handler.timer.Stop() {
		t.Fatal("physical batch timer was not armed")
	}
}

func newTestHandlerWithState(
	t *testing.T,
	publisher *fakePublisher,
	state *fakeChainState,
) *Handler {
	t.Helper()
	handler, err := NewHandler(
		context.Background(),
		publisher,
		state,
		HandlerConfig{
			NetworkMagic:          n2n.MainnetNetworkMagic,
			RollbackMaximumDepth:  100,
			RollbackCorroboration: 2,
			FlushAfter:            500 * time.Millisecond,
			Now:                   time.Now,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	return handler
}

func testAdapterChainPoint(number uint64, fill byte) n2n.ChainPoint {
	return n2n.NewChainPoint(
		pcommon.NewPoint(number, adapterHash(fill).Bytes()),
		number,
	)
}

func adapterEvidence(tip chainsync.Tip) syncer.SourceEvidence {
	return syncer.SourceEvidence{
		Primary: syncer.PeerEvidence{
			Peer: n2n.Peer{
				Host:       "relay-a:3001",
				Address:    "192.0.2.1:3001",
				Operator:   "operator-a",
				N2NVersion: 15,
			},
			Tip:        tip,
			N2NVersion: 15,
		},
		CheckpointMembers: []syncer.PeerEvidence{
			{Peer: n2n.Peer{Host: "relay-a:3001", Operator: "operator-a"}},
			{Peer: n2n.Peer{Host: "relay-b:3001", Operator: "operator-b"}},
		},
	}
}

func adapterRollbackEvidence(
	target n2n.ChainPoint,
	tip chainsync.Tip,
) syncer.RollbackEvidence {
	source := adapterEvidence(tip)
	confirmations := make(
		[]syncer.RollbackConfirmation,
		0,
		len(source.CheckpointMembers),
	)
	for index, member := range source.CheckpointMembers {
		member.Tip = tip
		member.N2NVersion = 15
		if member.Peer.Address == "" {
			member.Peer.Address = fmt.Sprintf("192.0.2.%d:3001", index+1)
		}
		confirmations = append(confirmations, syncer.RollbackConfirmation{
			Target:     target,
			BranchTip:  tip,
			Membership: member,
			Method:     syncer.RollbackProofPairedSingleton,
		})
	}
	confirmations[0].Method = syncer.RollbackProofFollowBlockFetch
	return syncer.RollbackEvidence{
		Source:        source,
		Target:        target,
		BranchTip:     tip,
		Confirmations: confirmations,
	}
}

func adapterTip(block adapterTestBlock) chainsync.Tip {
	return chainsync.Tip{
		Point:       pcommon.NewPoint(block.slot, block.hash.Bytes()),
		BlockNumber: block.number,
	}
}

func adapterHash(value byte) lcommon.Blake2b256 {
	var ret lcommon.Blake2b256
	for index := range ret {
		ret[index] = value
	}
	return ret
}

type adapterTestBlock struct {
	slot   uint64
	number uint64
	hash   lcommon.Blake2b256
	era    string
	kind   int
}

func (block adapterTestBlock) Hash() lcommon.Blake2b256     { return block.hash }
func (block adapterTestBlock) PrevHash() lcommon.Blake2b256 { return adapterHash(0x01) }
func (block adapterTestBlock) BlockNumber() uint64          { return block.number }
func (block adapterTestBlock) SlotNumber() uint64           { return block.slot }
func (adapterTestBlock) IssuerVkey() lcommon.IssuerVkey     { return lcommon.IssuerVkey{} }
func (adapterTestBlock) BlockBodySize() uint64              { return 0 }
func (block adapterTestBlock) Era() lcommon.Era {
	if block.era == "" {
		return lcommon.Era{Name: "Conway"}
	}
	return lcommon.Era{Name: block.era}
}
func (adapterTestBlock) Cbor() []byte                      { return nil }
func (adapterTestBlock) BlockBodyHash() lcommon.Blake2b256 { return lcommon.Blake2b256{} }
func (block adapterTestBlock) Header() lcommon.BlockHeader { return block }
func (block adapterTestBlock) Type() int {
	return block.kind
}
func (adapterTestBlock) Transactions() []lcommon.Transaction { return nil }
func (adapterTestBlock) Utxorpc() (*utxorpc.Block, error)    { return nil, nil }
