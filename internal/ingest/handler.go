// Package ingest adapts the verified direct-N2N stream to append-only
// publication without retaining raw block or transaction CBOR.
package ingest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/protocol/chainsync"
	pcommon "github.com/blinklabs-io/gouroboros/protocol/common"

	"clicksync/internal/model"
	"clicksync/internal/n2n"
	"clicksync/internal/normalize"
	"clicksync/internal/publication"
	"clicksync/internal/syncer"
)

const (
	defaultFlushAfter   = 500 * time.Millisecond
	defaultFlushTimeout = 30 * time.Second
)

type Publisher interface {
	PublishBatch(context.Context, publication.Batch) (publication.BatchResult, error)
	Rollback(context.Context, publication.RollbackRequest) error
}

type ChainState interface {
	CommittedSnapshot(context.Context) (uint64, error)
	CommittedTip(context.Context, uint64) (publication.Point, error)
}

type HandlerConfig struct {
	NetworkMagic          uint32
	RollbackMaximumDepth  uint32
	RollbackCorroboration int
	FlushAfter            time.Duration
	FlushTimeout          time.Duration
	Now                   func() time.Time
	Cancel                context.CancelCauseFunc
}

type stagedBlock struct {
	item         publication.BatchItem
	point        n2n.ChainPoint
	tip          chainsync.Tip
	encodedBytes int
	rows         uint64
}

type observedRollback struct {
	target           n2n.ChainPoint
	targetWasPending bool
}

type Handler struct {
	publisher Publisher
	state     ChainState
	config    HandlerConfig
	runCtx    context.Context

	mu           sync.Mutex
	pending      []stagedBlock
	pendingBytes int
	pendingRows  uint64
	firstStaged  time.Time
	timer        *time.Timer
	rollbackSeen *observedRollback
	unreported   syncer.CommitOutcome
	terminalErr  error
	timerStopped bool
}

func NewHandler(
	runCtx context.Context,
	publisher Publisher,
	state ChainState,
	config HandlerConfig,
) (*Handler, error) {
	if runCtx == nil || publisher == nil || state == nil {
		return nil, errors.New("run context, publisher, and chain state are required")
	}
	if config.NetworkMagic != n2n.MainnetNetworkMagic {
		return nil, fmt.Errorf("handler network magic %d is not pinned mainnet", config.NetworkMagic)
	}
	if config.RollbackMaximumDepth == 0 {
		return nil, errors.New("rollback maximum depth must be positive")
	}
	if config.RollbackCorroboration < 2 {
		return nil, errors.New("rollback corroboration must require at least two operators")
	}
	if config.FlushAfter == 0 {
		config.FlushAfter = defaultFlushAfter
	}
	if config.FlushAfter <= 0 || config.FlushAfter >= publication.MaxBatchAge {
		return nil, fmt.Errorf(
			"flush interval must be positive and less than %s",
			publication.MaxBatchAge,
		)
	}
	if config.FlushTimeout == 0 {
		config.FlushTimeout = defaultFlushTimeout
	}
	if config.FlushTimeout <= 0 {
		return nil, errors.New("flush timeout must be positive")
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	return &Handler{
		publisher: publisher,
		state:     state,
		config:    config,
		runCtx:    runCtx,
	}, nil
}

func (handler *Handler) Reconcile(
	ctx context.Context,
	point n2n.ChainPoint,
	evidence syncer.SourceEvidence,
) (syncer.CommitOutcome, error) {
	handler.mu.Lock()
	defer handler.mu.Unlock()
	if handler.terminalErr != nil {
		return syncer.CommitOutcome{}, handler.terminalErr
	}
	if len(handler.pending) != 0 {
		return syncer.CommitOutcome{}, handler.failLocked(
			errors.New("reconciliation encountered an unfinalized physical batch"),
		)
	}
	tip, err := handler.committedTip(ctx)
	if err != nil {
		return syncer.CommitOutcome{}, handler.failLocked(err)
	}
	target := publicationPoint(point)
	if samePublicationPoint(tip, target) {
		return syncer.CommitOutcome{}, nil
	}
	request, err := handler.rollbackRequest(
		target,
		"reconcile selected corroborated intersection",
		evidence.CheckpointMembers,
	)
	if err != nil {
		return syncer.CommitOutcome{}, handler.failLocked(err)
	}
	if err := handler.publisher.Rollback(ctx, request); err != nil {
		var committed *publication.CommittedError
		outcome := syncer.CommitOutcome{}
		if errors.As(err, &committed) {
			outcome.Committed = true
		}
		return outcome, handler.failLocked(err)
	}
	return syncer.CommitOutcome{Committed: true}, nil
}

func (handler *Handler) RollForward(
	ctx context.Context,
	block lcommon.Block,
	tip chainsync.Tip,
	evidence syncer.SourceEvidence,
) (syncer.CommitOutcome, error) {
	normalized, err := normalize.Bundle(block)
	if err != nil {
		handler.mu.Lock()
		defer handler.mu.Unlock()
		return handler.failAfterPendingLocked(
			ctx,
			fmt.Errorf("normalize verified block: %w", err),
		)
	}
	normalized.ObservedAt = handler.config.Now().UTC()
	staged, err := handler.newStagedBlock(normalized, tip, evidence)
	if err != nil {
		handler.mu.Lock()
		defer handler.mu.Unlock()
		return handler.failAfterPendingLocked(ctx, err)
	}

	handler.mu.Lock()
	defer handler.mu.Unlock()
	if handler.terminalErr != nil {
		return handler.terminalOutcomeLocked(false)
	}
	if !handler.fitsLocked(staged) && len(handler.pending) > 0 {
		handler.flushLocked(ctx)
		if handler.terminalErr != nil {
			return handler.terminalOutcomeLocked(false)
		}
	}
	if !handler.fitsLocked(staged) {
		handler.failLocked(
			errors.New("single normalized block exceeds physical microbatch bounds"),
		)
		return handler.terminalOutcomeLocked(false)
	}
	if len(handler.pending) == 0 {
		handler.firstStaged = handler.config.Now().UTC()
	}
	handler.pending = append(handler.pending, staged)
	handler.pendingRows += staged.rows
	handler.pendingBytes = appendEncodedBytes(
		handler.pendingBytes,
		len(handler.pending)-1,
		staged.encodedBytes,
	)
	handler.armTimerLocked()
	if handler.atLimitLocked() {
		handler.flushLocked(ctx)
		if handler.terminalErr != nil {
			return handler.terminalOutcomeLocked(true)
		}
	}
	outcome := handler.takeUnreportedLocked()
	outcome.Accepted = true
	return outcome, nil
}

func (handler *Handler) RollBackward(
	ctx context.Context,
	point n2n.ChainPoint,
	tip chainsync.Tip,
	evidence syncer.RollbackEvidence,
) (syncer.CommitOutcome, error) {
	handler.mu.Lock()
	defer handler.mu.Unlock()
	handler.stopTimerLocked()
	if handler.terminalErr != nil {
		return handler.terminalOutcomeLocked(false)
	}
	confirmations, err := handler.rollbackConfirmationEvidence(
		point,
		tip,
		evidence,
	)
	if err != nil {
		handler.failLocked(err)
		return handler.terminalOutcomeLocked(false)
	}
	if handler.rollbackSeen != nil &&
		!samePublicationPoint(
			publicationPoint(handler.rollbackSeen.target),
			publicationPoint(point),
		) {
		handler.failLocked(
			errors.New(
				"confirmed rollback target differs from observed barrier target",
			),
		)
		return handler.terminalOutcomeLocked(false)
	}
	if handler.rollbackSeen != nil &&
		handler.rollbackSeen.targetWasPending {
		durable, durableErr := handler.committedTip(ctx)
		if durableErr != nil {
			handler.failLocked(durableErr)
			return handler.terminalOutcomeLocked(false)
		}
		durablePoint, durableErr := chainPointFromPublication(durable)
		if durableErr != nil {
			handler.failLocked(durableErr)
			return handler.terminalOutcomeLocked(false)
		}
		request, requestErr := handler.rollbackRequest(
			durable,
			"corroborated rollback target was staged only; reconnect from actual durable tip",
			confirmations,
		)
		if requestErr != nil {
			handler.failLocked(requestErr)
			return handler.terminalOutcomeLocked(false)
		}
		if publishErr := handler.publisher.Rollback(
			ctx,
			request,
		); publishErr != nil {
			var committed *publication.CommittedError
			if errors.As(publishErr, &committed) {
				handler.unreported.Committed = true
			}
			handler.failLocked(publishErr)
			return handler.terminalOutcomeLocked(false)
		}
		handler.rollbackSeen = nil
		outcome := handler.takeUnreportedLocked()
		outcome.Committed = true
		return outcome, &syncer.RollbackReplayRequiredError{
			ObservedTarget: cloneChainPointValue(point),
			DurableTip:     durablePoint,
		}
	}
	handler.rollbackSeen = nil
	target := publicationPoint(point)
	retained := -1
	for index := range handler.pending {
		if samePublicationPoint(publicationPoint(handler.pending[index].point), target) {
			retained = index
			break
		}
	}
	if retained >= 0 {
		handler.pending = handler.pending[:retained+1]
		handler.recountPendingLocked()
		handler.flushLocked(ctx)
		if handler.terminalErr != nil {
			return handler.terminalOutcomeLocked(false)
		}
	} else {
		handler.clearPendingLocked()
	}
	request, err := handler.rollbackRequest(
		target,
		"corroborated remote rollback",
		confirmations,
	)
	if err != nil {
		handler.failLocked(err)
		return handler.terminalOutcomeLocked(false)
	}
	// The coordinator records a depth-zero append-only rollback header when
	// target is already the exact authoritative tip. This is required after
	// pending-only descendants are discarded; no in-memory state is treated
	// as a durable rollback acknowledgement.
	if err := handler.publisher.Rollback(ctx, request); err != nil {
		var committed *publication.CommittedError
		if errors.As(err, &committed) {
			handler.unreported.Committed = true
		}
		handler.failLocked(err)
		return handler.terminalOutcomeLocked(false)
	}
	outcome := handler.takeUnreportedLocked()
	outcome.Committed = true
	return outcome, nil
}

// RollbackObserved is a non-durable safety barrier. Pending blocks are never
// published after a valid rollback callback. Whether the exact target was
// pending is remembered so successful proof can acknowledge the actual
// durable tip and force replay instead of pretending the target was durable.
func (handler *Handler) RollbackObserved(
	_ context.Context,
	target n2n.ChainPoint,
	_ chainsync.Tip,
) error {
	handler.mu.Lock()
	defer handler.mu.Unlock()
	handler.stopTimerLocked()
	targetWasPending := false
	for _, staged := range handler.pending {
		if samePublicationPoint(
			publicationPoint(staged.point),
			publicationPoint(target),
		) {
			targetWasPending = true
			break
		}
	}
	handler.clearPendingLocked()
	handler.rollbackSeen = &observedRollback{
		target:           cloneChainPointValue(target),
		targetWasPending: targetWasPending,
	}
	if handler.unreported.LastCommittedPoint != nil &&
		!samePublicationPoint(
			publicationPoint(*handler.unreported.LastCommittedPoint),
			publicationPoint(target),
		) {
		// The adoption is durable, but a post-rollback outcome must never
		// report a descendant as the retained tail.
		handler.unreported = syncer.CommitOutcome{}
	}
	return handler.terminalErr
}

func (handler *Handler) rollbackConfirmationEvidence(
	target n2n.ChainPoint,
	branchTip chainsync.Tip,
	evidence syncer.RollbackEvidence,
) ([]syncer.PeerEvidence, error) {
	if !samePublicationPoint(
		publicationPoint(target),
		publicationPoint(evidence.Target),
	) {
		return nil, errors.New(
			"rollback evidence target differs from callback target",
		)
	}
	if !sameTip(branchTip, evidence.BranchTip) {
		return nil, errors.New(
			"rollback evidence branch tip differs from callback tip",
		)
	}
	ret := make([]syncer.PeerEvidence, 0, len(evidence.Confirmations))
	primaryFound := false
	for _, confirmation := range evidence.Confirmations {
		if !samePublicationPoint(
			publicationPoint(target),
			publicationPoint(confirmation.Target),
		) {
			return nil, errors.New(
				"rollback confirmation target metadata differs",
			)
		}
		if !sameTip(branchTip, confirmation.BranchTip) {
			return nil, errors.New(
				"rollback confirmation branch tip differs",
			)
		}
		member := confirmation.Membership
		if member.Peer.Host == "" || member.Peer.Operator == "" {
			return nil, errors.New(
				"rollback confirmation has incomplete session identity",
			)
		}
		if member.Peer.Operator == evidence.Source.Primary.Peer.Operator {
			if member.Peer.Host != evidence.Source.Primary.Peer.Host {
				return nil, errors.New(
					"rollback primary confirmation host differs",
				)
			}
			if confirmation.Method != syncer.RollbackProofFollowBlockFetch {
				return nil, errors.New(
					"rollback primary confirmation lacks Follow BlockFetch proof",
				)
			}
			primaryFound = true
		} else if confirmation.Method != syncer.RollbackProofPairedSingleton {
			return nil, errors.New(
				"rollback secondary confirmation lacks paired singleton proof",
			)
		}
		ret = append(ret, member)
	}
	if !primaryFound {
		return nil, errors.New(
			"rollback confirmations omit the callback source",
		)
	}
	return ret, nil
}

func (handler *Handler) EndAttempt(
	ctx context.Context,
	_ syncer.AttemptEnd,
) (syncer.CommitOutcome, error) {
	handler.mu.Lock()
	defer handler.mu.Unlock()
	handler.stopTimerLocked()
	if handler.rollbackSeen != nil {
		handler.clearPendingLocked()
		handler.rollbackSeen = nil
	} else if handler.terminalErr == nil && len(handler.pending) > 0 {
		handler.flushLocked(ctx)
	}
	outcome := handler.takeUnreportedLocked()
	return outcome, handler.terminalErr
}

func (handler *Handler) newStagedBlock(
	block model.Block,
	tip chainsync.Tip,
	evidence syncer.SourceEvidence,
) (stagedBlock, error) {
	primary := evidence.Primary
	if primary.Peer.Host == "" ||
		primary.Peer.Address == "" ||
		primary.Peer.Operator == "" ||
		primary.N2NVersion == 0 {
		return stagedBlock{}, errors.New("selected N2N source evidence is incomplete")
	}
	point, err := chainPointFromBlock(block)
	if err != nil {
		return stagedBlock{}, err
	}
	item := publication.BatchItem{
		Block: block,
		Source: publication.Source{
			PeerHost:     primary.Peer.Host,
			PeerAddress:  primary.Peer.Address,
			Operator:     primary.Peer.Operator,
			N2NVersion:   primary.N2NVersion,
			NetworkMagic: handler.config.NetworkMagic,
		},
	}
	encoded, err := json.Marshal(item)
	if err != nil {
		return stagedBlock{}, fmt.Errorf("size normalized block: %w", err)
	}
	return stagedBlock{
		item:         item,
		point:        point,
		tip:          cloneTip(tip),
		encodedBytes: len(encoded),
		rows:         factRows(item),
	}, nil
}

func (handler *Handler) rollbackRequest(
	to publication.Point,
	reason string,
	evidence []syncer.PeerEvidence,
) (publication.RollbackRequest, error) {
	operators := make(map[string]struct{}, len(evidence))
	observers := make([]publication.RollbackObserver, 0, len(evidence))
	for _, item := range evidence {
		if item.Peer.Host == "" || item.Peer.Operator == "" {
			return publication.RollbackRequest{}, errors.New("rollback evidence has incomplete peer identity")
		}
		if _, duplicate := operators[item.Peer.Operator]; duplicate {
			continue
		}
		operators[item.Peer.Operator] = struct{}{}
		observers = append(observers, publication.RollbackObserver{
			Peer:     item.Peer.Host,
			Operator: item.Peer.Operator,
		})
	}
	if len(operators) < handler.config.RollbackCorroboration {
		return publication.RollbackRequest{}, fmt.Errorf(
			"rollback has %d independent operators, requires %d",
			len(operators),
			handler.config.RollbackCorroboration,
		)
	}
	return publication.RollbackRequest{
		To:                    to,
		Reason:                reason,
		Observers:             observers,
		MaximumDepth:          handler.config.RollbackMaximumDepth,
		RequiredCorroboration: handler.config.RollbackCorroboration,
		RecordedAt:            handler.config.Now().UTC(),
	}, nil
}

func (handler *Handler) committedTip(ctx context.Context) (publication.Point, error) {
	snapshot, err := handler.state.CommittedSnapshot(ctx)
	if err != nil {
		return publication.Point{}, fmt.Errorf("read authoritative snapshot: %w", err)
	}
	tip, err := handler.state.CommittedTip(ctx, snapshot)
	if err != nil {
		return publication.Point{}, fmt.Errorf("read authoritative tip: %w", err)
	}
	return tip, nil
}

func (handler *Handler) fitsLocked(next stagedBlock) bool {
	if handler.pendingRows > publication.MaxBatchRows {
		return false
	}
	return len(handler.pending)+1 <= publication.MaxBatchBlocks &&
		next.rows <= publication.MaxBatchRows-handler.pendingRows &&
		appendEncodedBytes(
			handler.pendingBytes,
			len(handler.pending),
			next.encodedBytes,
		) <= publication.MaxBatchBytes
}

func (handler *Handler) atLimitLocked() bool {
	return len(handler.pending) >= publication.MaxBatchBlocks ||
		handler.pendingRows >= publication.MaxBatchRows ||
		handler.pendingBytes >= publication.MaxBatchBytes
}

func (handler *Handler) armTimerLocked() {
	handler.stopTimerLocked()
	handler.timerStopped = false
	handler.timer = time.AfterFunc(handler.config.FlushAfter, handler.flushFromTimer)
}

func (handler *Handler) stopTimerLocked() {
	if handler.timer != nil {
		handler.timer.Stop()
		handler.timer = nil
	}
	handler.timerStopped = true
}

func (handler *Handler) flushFromTimer() {
	handler.mu.Lock()
	defer handler.mu.Unlock()
	handler.timer = nil
	if handler.timerStopped ||
		handler.terminalErr != nil ||
		len(handler.pending) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(
		context.WithoutCancel(handler.runCtx),
		handler.config.FlushTimeout,
	)
	defer cancel()
	handler.flushLocked(ctx)
}

func (handler *Handler) flushLocked(ctx context.Context) {
	if len(handler.pending) == 0 ||
		handler.terminalErr != nil {
		return
	}
	handler.stopTimerLocked()
	staged := append([]stagedBlock(nil), handler.pending...)
	items := make([]publication.BatchItem, len(staged))
	for index := range staged {
		items[index] = staged[index].item
	}
	firstStaged := handler.firstStaged
	handler.clearPendingLocked()
	result, err := handler.publisher.PublishBatch(ctx, publication.Batch{
		Items:         items,
		FirstStagedAt: firstStaged,
	})
	resultErr := validateBatchResult(staged, result, err)
	if resultErr == nil && len(result.PublicationIDs) > 0 {
		last := staged[len(staged)-1]
		handler.addUnreportedLocked(syncer.CommitOutcome{
			Committed:          true,
			CommittedBlocks:    uint64(len(result.PublicationIDs)),
			LastCommittedPoint: cloneChainPointPointer(last.point),
			LastCommittedTip:   cloneTipPointer(last.tip),
		})
	}
	if resultErr != nil {
		err = errors.Join(err, resultErr)
	}
	if err != nil {
		handler.failLocked(fmt.Errorf("publish physical microbatch: %w", err))
	}
}

func (handler *Handler) terminalOutcomeLocked(
	accepted bool,
) (syncer.CommitOutcome, error) {
	outcome := handler.takeUnreportedLocked()
	outcome.Accepted = accepted
	return outcome, handler.terminalErr
}

func (handler *Handler) failAfterPendingLocked(
	ctx context.Context,
	cause error,
) (syncer.CommitOutcome, error) {
	if handler.terminalErr == nil && len(handler.pending) > 0 {
		handler.flushLocked(ctx)
	}
	if handler.terminalErr == nil {
		handler.failLocked(cause)
	} else {
		handler.terminalErr = errors.Join(handler.terminalErr, cause)
	}
	return handler.terminalOutcomeLocked(false)
}

func (handler *Handler) clearPendingLocked() {
	handler.pending = nil
	handler.pendingBytes = 0
	handler.pendingRows = 0
	handler.firstStaged = time.Time{}
}

func (handler *Handler) recountPendingLocked() {
	handler.pendingBytes = 0
	handler.pendingRows = 0
	for index, item := range handler.pending {
		handler.pendingBytes = appendEncodedBytes(
			handler.pendingBytes,
			index,
			item.encodedBytes,
		)
		handler.pendingRows += item.rows
	}
	if len(handler.pending) == 0 {
		handler.firstStaged = time.Time{}
	}
}

func appendEncodedBytes(current, count, item int) int {
	if count == 0 {
		return 2 + item
	}
	return current + 1 + item
}

func validateBatchResult(
	staged []stagedBlock,
	result publication.BatchResult,
	publishErr error,
) error {
	count := len(result.PublicationIDs)
	if count != 0 && count != len(staged) {
		return fmt.Errorf(
			"publisher returned partial committed identity set %d for %d staged blocks",
			count,
			len(staged),
		)
	}
	if count == 0 {
		if publishErr == nil {
			return errors.New("publisher succeeded without committed publication identities")
		}
		return nil
	}
	if publishErr != nil {
		var committed *publication.CommittedError
		if !errors.As(publishErr, &committed) {
			return errors.New(
				"publisher returned committed identities with a non-committed error type",
			)
		}
		if committed.PublicationID != result.PublicationIDs[count-1] ||
			committed.EventSeq != result.LastEventSeq {
			return errors.New(
				"publisher committed error identity does not match the result tail",
			)
		}
	}
	seen := make(map[uint64]struct{}, count)
	for _, id := range result.PublicationIDs {
		if id == 0 {
			return errors.New("publisher returned zero publication identity")
		}
		if _, duplicate := seen[id]; duplicate {
			return errors.New("publisher returned duplicate publication identities")
		}
		seen[id] = struct{}{}
	}
	expected := publicationPoint(staged[len(staged)-1].point)
	if !samePublicationPoint(result.LastCommitted, expected) {
		return fmt.Errorf(
			"publisher committed tail %#v differs from staged tail %#v",
			result.LastCommitted,
			expected,
		)
	}
	if result.FirstEventSeq == 0 || result.LastEventSeq < result.FirstEventSeq {
		return errors.New("publisher returned an invalid committed event interval")
	}
	if result.LastEventSeq-result.FirstEventSeq+1 != uint64(count) {
		return errors.New("publisher committed event interval does not cover the physical batch")
	}
	return nil
}

func (handler *Handler) addUnreportedLocked(outcome syncer.CommitOutcome) {
	if outcome.CommittedBlocks == 0 {
		return
	}
	handler.unreported.Committed = true
	handler.unreported.CommittedBlocks += outcome.CommittedBlocks
	handler.unreported.LastCommittedPoint = outcome.LastCommittedPoint
	handler.unreported.LastCommittedTip = outcome.LastCommittedTip
}

func (handler *Handler) takeUnreportedLocked() syncer.CommitOutcome {
	outcome := handler.unreported
	handler.unreported = syncer.CommitOutcome{}
	return outcome
}

func (handler *Handler) failLocked(err error) error {
	if err == nil {
		return nil
	}
	if handler.terminalErr == nil {
		handler.terminalErr = err
		if handler.config.Cancel != nil {
			handler.config.Cancel(err)
		}
	}
	return handler.terminalErr
}

func factRows(item publication.BatchItem) uint64 {
	rows := uint64(1 + len(item.PeerObservations))
	for _, transaction := range item.Block.Transactions {
		rows += 1 +
			uint64(len(transaction.Inputs)) +
			uint64(len(transaction.Outputs)) +
			uint64(len(transaction.DatumObservations)) +
			uint64(len(transaction.Withdrawals)) +
			uint64(len(transaction.Redeemers))
		if transaction.Metadata != nil {
			rows++
		}
	}
	rows += uint64(len(item.Block.Datums))
	return rows
}

func chainPointFromBlock(block model.Block) (n2n.ChainPoint, error) {
	if block.Hash == (model.Hash32{}) {
		return n2n.ChainPoint{}, errors.New("normalized block hash is zero")
	}
	point := pcommon.NewPoint(block.Slot, append([]byte(nil), block.Hash[:]...))
	if block.Era == "Byron" && block.Type == int16(ledger.BlockTypeByronEbb) {
		return n2n.NewByronEBBChainPoint(point, block.Number), nil
	}
	return n2n.NewChainPoint(point, block.Number), nil
}

func publicationPoint(point n2n.ChainPoint) publication.Point {
	if len(point.Point.Hash) == 0 {
		return publication.Point{Origin: true}
	}
	var hash model.Hash32
	copy(hash[:], point.Point.Hash)
	return publication.Point{
		Slot:        point.Point.Slot,
		Hash:        hash,
		BlockNumber: point.BlockNumber,
		IsByronEBB:  point.IsByronEBB,
	}
}

func chainPointFromPublication(
	point publication.Point,
) (n2n.ChainPoint, error) {
	if point.Origin {
		return n2n.NewChainPointOrigin(), nil
	}
	if point.Hash == (model.Hash32{}) {
		return n2n.ChainPoint{}, errors.New(
			"durable publication tip has zero hash",
		)
	}
	raw := pcommon.NewPoint(point.Slot, point.Hash[:])
	if point.IsByronEBB {
		return n2n.NewByronEBBChainPoint(raw, point.BlockNumber), nil
	}
	return n2n.NewChainPoint(raw, point.BlockNumber), nil
}

func samePublicationPoint(left, right publication.Point) bool {
	if left.Origin || right.Origin {
		return left.Origin == right.Origin
	}
	return left.Slot == right.Slot &&
		left.Hash == right.Hash &&
		left.BlockNumber == right.BlockNumber &&
		left.IsByronEBB == right.IsByronEBB
}

func sameTip(left, right chainsync.Tip) bool {
	return left.BlockNumber == right.BlockNumber &&
		left.Point.Slot == right.Point.Slot &&
		bytes.Equal(left.Point.Hash, right.Point.Hash)
}

func cloneTip(value chainsync.Tip) chainsync.Tip {
	return chainsync.Tip{
		Point:       pcommon.NewPoint(value.Point.Slot, append([]byte(nil), value.Point.Hash...)),
		BlockNumber: value.BlockNumber,
	}
}

func cloneTipPointer(value chainsync.Tip) *chainsync.Tip {
	ret := cloneTip(value)
	return &ret
}

func cloneChainPointPointer(value n2n.ChainPoint) *n2n.ChainPoint {
	ret := cloneChainPointValue(value)
	return &ret
}

func cloneChainPointValue(value n2n.ChainPoint) n2n.ChainPoint {
	if value.IsByronEBB {
		return n2n.NewByronEBBChainPoint(
			value.Point,
			value.BlockNumber,
		)
	}
	return n2n.NewChainPoint(value.Point, value.BlockNumber)
}
