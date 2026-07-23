package n2n

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/protocol/blockfetch"
	"github.com/blinklabs-io/gouroboros/protocol/chainsync"
	pcommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

type Peer struct {
	Host       string
	Address    string
	Operator   string
	N2NVersion uint16
	Tip        *chainsync.Tip
}

type Handler interface {
	Reconcile(context.Context, ChainPoint, Peer) error
	RollForward(context.Context, lcommon.Block, chainsync.Tip, Peer) error
	RollBackward(context.Context, ChainPoint, chainsync.Tip, Peer) error
}

type connection struct {
	peer     Peer
	conn     *ouroboros.Connection
	asyncErr <-chan error
	events   chan chainEvent
	cancel   context.CancelCauseFunc
	wg       sync.WaitGroup

	fetchMu        sync.Mutex
	pendingHeaders []expectedHeader
	activeFetch    *rangeFetch
	expectedParent *ChainPoint
	lastHeader     *expectedHeader
	rangeRequests  uint64
	rangeBlocks    uint64

	requestBlockRange func(pcommon.Point, pcommon.Point) error
	fetchSingleBlock  func(pcommon.Point) (lcommon.Block, error)
	blockFetchDone    <-chan struct{}
	chainSyncDone     <-chan struct{}
}

type chainEvent struct {
	block    lcommon.Block
	rollback *ChainPoint
	tip      chainsync.Tip
	done     chan error
}

type expectedHeader struct {
	header lcommon.BlockHeader
	point  pcommon.Point
	tip    chainsync.Tip
}

type rangeFetch struct {
	expected []expectedHeader
	next     int
	done     chan error
	once     sync.Once
}

type DialConfig struct {
	NetworkMagic    uint32
	QueueCapacity   int
	HeaderBatchSize int
	DialTimeout     time.Duration
	BlockTimeout    time.Duration
	Operator        string
}

const MainnetNetworkMagic = uint32(764824073)

const MainnetEpoch0EBBHash = "89d9b5a5b8ddc8d7e5a6795e9774d97faf1efea59b2caf7eaf9f8c5b32059df4"

func RunPeer(
	ctx context.Context,
	address string,
	cfg DialConfig,
	candidates []ChainPoint,
	handler Handler,
	logger *slog.Logger,
) error {
	if handler == nil {
		return errors.New("nil ChainSync event handler")
	}
	if logger == nil {
		return errors.New("nil logger")
	}
	if err := validateDialConfig(cfg); err != nil {
		return err
	}
	runCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(context.Canceled)
	current := &connection{
		peer: Peer{
			Host:     address,
			Address:  address,
			Operator: cfg.Operator,
		},
		events:         make(chan chainEvent, cfg.QueueCapacity),
		cancel:         cancel,
		pendingHeaders: make([]expectedHeader, 0, cfg.HeaderBatchSize),
	}
	conn, asyncErr, err := dial(
		address,
		cfg,
		current.onRollForward,
		current.onRollBackward,
		current.onBlock,
		current.onBatchDone,
		logger,
	)
	if err != nil {
		return err
	}
	current.conn = conn
	current.asyncErr = asyncErr
	current.requestBlockRange = conn.BlockFetch().Client.GetBlockRange
	current.fetchSingleBlock = conn.BlockFetch().Client.GetBlock
	current.blockFetchDone = conn.BlockFetch().Client.DoneChan()
	current.chainSyncDone = conn.ChainSync().Client.DoneChan()
	version, _ := conn.ProtocolVersion()
	current.peer.N2NVersion = version
	if remote := conn.Id().RemoteAddr; remote != nil {
		current.peer.Address = remote.String()
	}
	defer conn.Close()

	workerErr := make(chan error, 1)
	current.wg.Add(1)
	go func() {
		defer current.wg.Done()
		workerErr <- current.process(runCtx, handler)
	}()
	defer func() {
		cancel(context.Canceled)
		current.wg.Wait()
	}()

	chosen, err := reconcileAndSync(
		conn.ChainSync().Client,
		candidates,
		func(point ChainPoint) error {
			tip, tipErr := conn.ChainSync().Client.GetCurrentTip()
			if tipErr != nil {
				return fmt.Errorf("read actual Follow connection tip: %w", tipErr)
			}
			current.peer.Tip = tip
			if err := current.resetHeaderChain(point); err != nil {
				return err
			}
			return handler.Reconcile(runCtx, point, current.peer)
		},
	)
	if err != nil {
		return err
	}
	logger.Info(
		"direct ChainSync started",
		"peer", address,
		"operator", cfg.Operator,
		"n2n_version", version,
		"intersection_slot", chosen.Point.Slot,
		"intersection_hash", safePointHash(chosen.Point),
		"intersection_block_number", chosen.BlockNumber,
	)

	select {
	case <-runCtx.Done():
		return context.Cause(runCtx)
	case err := <-workerErr:
		return err
	case err, ok := <-asyncErr:
		if !ok {
			return protocolChannelClosure(runCtx, cancel, workerErr)
		}
		return classifyPeerProtocolError(err, current.currentFetchPoint())
	}
}

func protocolChannelClosure(
	runCtx context.Context,
	cancel context.CancelCauseFunc,
	workerErr <-chan error,
) error {
	closed := &ProtocolChannelClosed{}
	cancel(closed)
	err := <-workerErr
	if err == nil ||
		errors.Is(err, closed) ||
		errors.Is(err, context.Canceled) {
		return closed
	}
	// A handler/store failure or typed validation failure which raced channel
	// closure is more specific and must remain terminal.
	return err
}

func classifyPeerProtocolError(err error, point pcommon.Point) error {
	var validationError *lcommon.ValidationError
	if errors.As(err, &validationError) {
		return peerDataViolation(
			"decoded_block_validation",
			point,
			fmt.Errorf("peer protocol: %w", err),
		)
	}
	return fmt.Errorf("peer protocol: %w", err)
}

func (c *connection) process(ctx context.Context, handler Handler) error {
	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case event := <-c.events:
			var err error
			if event.rollback != nil {
				err = handler.RollBackward(ctx, *event.rollback, event.tip, c.peer)
			} else {
				err = handler.RollForward(ctx, event.block, event.tip, c.peer)
			}
			if err != nil {
				if event.done != nil {
					event.done <- err
				}
				c.cancel(err)
				return err
			}
			if event.done != nil {
				event.done <- nil
			}
		}
	}
}

func (c *connection) onRollForward(
	_ chainsync.CallbackContext,
	_ uint,
	headerValue any,
	tip chainsync.Tip,
) error {
	header, ok := headerValue.(lcommon.BlockHeader)
	if !ok || header == nil {
		return peerDataViolation(
			"unexpected_header",
			pcommon.Point{},
			fmt.Errorf("ChainSync returned unexpected header type %T", headerValue),
		)
	}
	point := pcommon.NewPoint(header.SlotNumber(), header.Hash().Bytes())
	c.fetchMu.Lock()
	if c.activeFetch != nil {
		c.fetchMu.Unlock()
		return peerDataViolation(
			"interleaved_roll_forward",
			point,
			errors.New("ChainSync roll-forward interleaved an active BlockFetch range"),
		)
	}
	var previous *expectedHeader
	if len(c.pendingHeaders) > 0 {
		previous = &c.pendingHeaders[len(c.pendingHeaders)-1]
	} else if c.lastHeader != nil {
		previous = c.lastHeader
	}
	if err := validateHeaderContinuity(
		c.expectedParent,
		previous,
		header,
		point,
	); err != nil {
		c.fetchMu.Unlock()
		return err
	}
	c.pendingHeaders = append(c.pendingHeaders, expectedHeader{
		header: header,
		point:  point,
		tip:    tip,
	})
	flush := shouldFlushHeaderRange(
		len(c.pendingHeaders),
		cap(c.pendingHeaders),
		point,
		tip,
	)
	c.fetchMu.Unlock()
	if !flush {
		return nil
	}
	return c.flushHeaderRange()
}

func (c *connection) onRollBackward(
	_ chainsync.CallbackContext,
	point pcommon.Point,
	tip chainsync.Tip,
) error {
	parent, err := c.resolveRollbackParent(point)
	if err != nil {
		return err
	}
	if err := c.resetHeaderChain(parent); err != nil {
		return err
	}
	done := make(chan error, 1)
	select {
	case c.events <- chainEvent{rollback: &parent, tip: tip, done: done}:
	case <-c.chainSyncDone:
		return errors.New("ChainSync stopped during rollback backpressure")
	}
	select {
	case err := <-done:
		return err
	case <-c.chainSyncDone:
		return errors.New("ChainSync stopped before rollback commit completed")
	}
}

func (c *connection) resetHeaderChain(point ChainPoint) error {
	c.fetchMu.Lock()
	defer c.fetchMu.Unlock()
	if c.activeFetch != nil {
		return peerDataViolation(
			"interleaved_rollback",
			pcommon.Point{},
			errors.New("ChainSync rollback interleaved an active BlockFetch range"),
		)
	}
	c.pendingHeaders = c.pendingHeaders[:0]
	c.lastHeader = nil
	cloned := NewChainPoint(point.Point, point.BlockNumber)
	if point.IsByronEBB {
		cloned = NewByronEBBChainPoint(point.Point, point.BlockNumber)
	}
	c.expectedParent = &cloned
	return nil
}

func (c *connection) resolveRollbackParent(
	point pcommon.Point,
) (ChainPoint, error) {
	if isOrigin(point) {
		return NewChainPointOrigin(), nil
	}
	if c.fetchSingleBlock == nil {
		return ChainPoint{}, errors.New("rollback BlockFetch is not configured")
	}
	block, err := c.fetchSingleBlock(point)
	if err != nil {
		if isNetworkFailure(err) {
			return ChainPoint{}, fmt.Errorf("fetch rollback target: %w", err)
		}
		var validationError *lcommon.ValidationError
		if errors.As(err, &validationError) {
			return ChainPoint{}, peerDataViolation(
				"rollback_block_validation",
				point,
				err,
			)
		}
		return ChainPoint{}, &RangeUnavailable{
			Start: point,
			End:   point,
			Err:   err,
		}
	}
	if block == nil ||
		block.SlotNumber() != point.Slot ||
		!bytes.Equal(block.Hash().Bytes(), point.Hash) {
		return ChainPoint{}, peerDataViolation(
			"rollback_point_mismatch",
			point,
			errors.New("BlockFetch body does not match rollback point"),
		)
	}
	if isByronEpochBoundaryHeader(block.Header()) {
		return NewByronEBBChainPoint(point, block.BlockNumber()), nil
	}
	return NewChainPoint(point, block.BlockNumber()), nil
}

func (c *connection) flushHeaderRange() error {
	c.fetchMu.Lock()
	if c.activeFetch != nil {
		c.fetchMu.Unlock()
		return errors.New("BlockFetch range already active")
	}
	if len(c.pendingHeaders) == 0 {
		c.fetchMu.Unlock()
		return nil
	}
	state := &rangeFetch{
		expected: append([]expectedHeader(nil), c.pendingHeaders...),
		done:     make(chan error, 1),
	}
	c.pendingHeaders = c.pendingHeaders[:0]
	c.activeFetch = state
	c.rangeRequests++
	c.fetchMu.Unlock()

	start := state.expected[0].point
	end := state.expected[len(state.expected)-1].point
	if c.requestBlockRange == nil {
		c.fetchMu.Lock()
		if c.activeFetch == state {
			c.activeFetch = nil
		}
		c.fetchMu.Unlock()
		return errors.New("BlockFetch range requester is not configured")
	}
	if err := c.requestBlockRange(start, end); err != nil {
		c.fetchMu.Lock()
		if c.activeFetch == state {
			c.activeFetch = nil
		}
		c.fetchMu.Unlock()
		if isNetworkFailure(err) {
			return fmt.Errorf("request BlockFetch range: %w", err)
		}
		return &RangeUnavailable{Start: start, End: end, Err: err}
	}
	select {
	case err := <-state.done:
		return err
	case <-c.blockFetchDone:
		return errors.New("BlockFetch stopped before range completion")
	}
}

func (c *connection) onBlock(
	_ blockfetch.CallbackContext,
	_ uint,
	block lcommon.Block,
) error {
	c.fetchMu.Lock()
	state := c.activeFetch
	if state == nil {
		c.fetchMu.Unlock()
		return peerDataViolation(
			"unexpected_block",
			pcommon.Point{},
			errors.New("BlockFetch delivered a block without an active range"),
		)
	}
	if state.next >= len(state.expected) {
		c.fetchMu.Unlock()
		var point pcommon.Point
		if len(state.expected) > 0 {
			point = state.expected[len(state.expected)-1].point
		}
		return c.abortFetch(
			state,
			peerDataViolation(
				"extra_block",
				point,
				errors.New("BlockFetch delivered more blocks than requested headers"),
			),
		)
	}
	expected, err := matchRangeBlock(state.expected, state.next, block)
	c.fetchMu.Unlock()
	if err != nil {
		return c.abortFetch(state, err)
	}

	c.fetchMu.Lock()
	if c.activeFetch != state || state.next >= len(state.expected) ||
		!pointsEqual(state.expected[state.next].point, expected.point) {
		c.fetchMu.Unlock()
		return c.abortFetch(
			state,
			errors.New("BlockFetch range state changed during block verification"),
		)
	}
	state.next++
	c.fetchMu.Unlock()
	select {
	case c.events <- chainEvent{block: block, tip: expected.tip}:
		return nil
	case <-c.blockFetchDone:
		err := errors.New("BlockFetch stopped during publication backpressure")
		return c.abortFetch(state, err)
	}
}

func (c *connection) onBatchDone(_ blockfetch.CallbackContext) error {
	c.fetchMu.Lock()
	state := c.activeFetch
	if state == nil {
		c.fetchMu.Unlock()
		return peerDataViolation(
			"unexpected_batch_done",
			pcommon.Point{},
			errors.New("BlockFetch completed without an active range"),
		)
	}
	var err error
	if state.next != len(state.expected) {
		err = &RangeUnavailable{
			Start: state.expected[0].point,
			End:   state.expected[len(state.expected)-1].point,
			Err: fmt.Errorf(
				"BlockFetch range returned %d blocks for %d ChainSync headers",
				state.next,
				len(state.expected),
			),
		}
	}
	c.activeFetch = nil
	if err == nil {
		c.rangeBlocks += uint64(state.next)
		last := state.expected[len(state.expected)-1]
		c.lastHeader = &last
		c.expectedParent = nil
	}
	c.fetchMu.Unlock()
	state.finish(err)
	return err
}

func (c *connection) abortFetch(state *rangeFetch, err error) error {
	c.fetchMu.Lock()
	if c.activeFetch == state {
		c.activeFetch = nil
	}
	c.fetchMu.Unlock()
	state.finish(err)
	return err
}

func (c *connection) currentFetchPoint() pcommon.Point {
	c.fetchMu.Lock()
	defer c.fetchMu.Unlock()
	if c.activeFetch == nil ||
		c.activeFetch.next >= len(c.activeFetch.expected) {
		return pcommon.Point{}
	}
	point := c.activeFetch.expected[c.activeFetch.next].point
	return pcommon.NewPoint(point.Slot, bytes.Clone(point.Hash))
}

func validateHeaderContinuity(
	parent *ChainPoint,
	previous *expectedHeader,
	header lcommon.BlockHeader,
	point pcommon.Point,
) error {
	if previous != nil {
		if header.PrevHash() != previous.header.Hash() {
			return peerDataViolation(
				"header_parent_mismatch",
				point,
				errors.New("ChainSync header parent hash does not match prior header"),
			)
		}
		if err := validateHeaderTransition(
			previous.header.SlotNumber(),
			previous.header.BlockNumber(),
			isByronEpochBoundaryHeader(previous.header),
			header,
		); err != nil {
			return peerDataViolation(err.kind, point, err)
		}
		return nil
	}
	if parent == nil {
		return errors.New("ChainSync header continuity has no reconciled parent")
	}
	if isOrigin(parent.Point) {
		if !isByronEpochBoundaryHeader(header) ||
			header.SlotNumber() != 0 ||
			header.BlockNumber() != 0 ||
			!strings.EqualFold(header.Hash().String(), MainnetEpoch0EBBHash) {
			return peerDataViolation(
				"origin_first_header",
				point,
				fmt.Errorf(
					"mainnet Origin must begin at decoded Byron EBB 0:0:%s, got %T slot=%d block=%d hash=%s",
					MainnetEpoch0EBBHash,
					header,
					header.SlotNumber(),
					header.BlockNumber(),
					header.Hash().String(),
				),
			)
		}
		return nil
	}
	if !bytes.Equal(header.PrevHash().Bytes(), parent.Point.Hash) {
		return peerDataViolation(
			"header_parent_mismatch",
			point,
			errors.New("first ChainSync header parent does not match checkpoint"),
		)
	}
	if err := validateHeaderTransition(
		parent.Point.Slot,
		parent.BlockNumber,
		parent.IsByronEBB,
		header,
	); err != nil {
		return peerDataViolation(err.kind, point, err)
	}
	return nil
}

type headerTransitionError struct {
	kind string
	err  error
}

func (e *headerTransitionError) Error() string {
	if e == nil {
		return "invalid header transition"
	}
	return e.err.Error()
}

func (e *headerTransitionError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.err
}

func validateHeaderTransition(
	previousSlot uint64,
	previousBlockNumber uint64,
	previousIsByronEBB bool,
	header lcommon.BlockHeader,
) *headerTransitionError {
	currentSlot := header.SlotNumber()
	currentBlockNumber := header.BlockNumber()
	currentIsByronEBB := isByronEpochBoundaryHeader(header)
	if currentIsByronEBB {
		if currentBlockNumber != previousBlockNumber {
			return &headerTransitionError{
				kind: "header_height_gap",
				err: fmt.Errorf(
					"Byron EBB block number %d must equal predecessor %d",
					currentBlockNumber,
					previousBlockNumber,
				),
			}
		}
		if currentSlot <= previousSlot {
			return &headerTransitionError{
				kind: "header_slot_order",
				err: fmt.Errorf(
					"Byron EBB slot %d must follow predecessor slot %d",
					currentSlot,
					previousSlot,
				),
			}
		}
		return nil
	}
	if previousBlockNumber == ^uint64(0) ||
		currentBlockNumber != previousBlockNumber+1 {
		return &headerTransitionError{
			kind: "header_height_gap",
			err: fmt.Errorf(
				"regular block number %d must follow predecessor %d",
				currentBlockNumber,
				previousBlockNumber,
			),
		}
	}
	if currentSlot < previousSlot ||
		currentSlot == previousSlot && !previousIsByronEBB {
		return &headerTransitionError{
			kind: "header_slot_order",
			err: fmt.Errorf(
				"regular block slot %d does not legally follow predecessor slot %d (predecessor EBB=%t)",
				currentSlot,
				previousSlot,
				previousIsByronEBB,
			),
		}
	}
	return nil
}

func isByronEpochBoundaryHeader(header lcommon.BlockHeader) bool {
	_, ok := header.(*byron.ByronEpochBoundaryBlockHeader)
	return ok
}

func (f *rangeFetch) finish(err error) {
	f.once.Do(func() {
		f.done <- err
	})
}

func shouldFlushHeaderRange(
	pending, limit int,
	point pcommon.Point,
	tip chainsync.Tip,
) bool {
	return pending >= limit || pointsEqual(point, tip.Point)
}

func matchRangeBlock(
	expected []expectedHeader,
	index int,
	block lcommon.Block,
) (expectedHeader, error) {
	if index < 0 || index >= len(expected) {
		return expectedHeader{}, errors.New("BlockFetch block is outside expected header range")
	}
	value := expected[index]
	if err := verifyHeaderBody(value.header, block, value.point); err != nil {
		return expectedHeader{}, peerDataViolation(
			"header_body_mismatch",
			value.point,
			fmt.Errorf(
				"verify ranged block %d:%s: %w",
				value.point.Slot,
				safePointHash(value.point),
				err,
			),
		)
	}
	return value, nil
}

func dial(
	address string,
	cfg DialConfig,
	rollForward chainsync.RollForwardFunc,
	rollBackward chainsync.RollBackwardFunc,
	blockFunc blockfetch.BlockFunc,
	batchDoneFunc blockfetch.BatchDoneFunc,
	logger *slog.Logger,
) (*ouroboros.Connection, <-chan error, error) {
	if err := validateDialConfig(cfg); err != nil {
		return nil, nil, err
	}
	blockCfg, err := newBlockFetchConfig(
		cfg,
		blockFunc,
		batchDoneFunc,
	)
	if err != nil {
		return nil, nil, err
	}
	chainCfg := newChainSyncConfig(cfg, rollForward, rollBackward)
	errs := make(chan error, 16)
	asyncErr := make(chan error, 1)
	go func() {
		defer close(asyncErr)
		for err := range errs {
			if err == nil {
				continue
			}
			select {
			case asyncErr <- err:
			default:
			}
		}
	}()
	conn, err := ouroboros.NewConnection(
		ouroboros.WithNetworkMagic(cfg.NetworkMagic),
		ouroboros.WithNodeToNode(true),
		ouroboros.WithKeepAlive(true),
		ouroboros.WithErrorChan(errs),
		ouroboros.WithLogger(logger),
		ouroboros.WithBlockFetchConfig(blockCfg),
		ouroboros.WithChainSyncConfig(chainCfg),
	)
	if err != nil {
		return nil, nil, err
	}
	if err := conn.DialTimeout("tcp", address, cfg.DialTimeout); err != nil {
		_ = conn.Close()
		return nil, nil, err
	}
	version, _ := conn.ProtocolVersion()
	if version < 7 || version > 15 {
		_ = conn.Close()
		return nil, nil, peerDataViolation(
			"unsupported_n2n_version",
			pcommon.Point{},
			fmt.Errorf("peer negotiated unsupported N2N version %d", version),
		)
	}
	return conn, asyncErr, nil
}

func validateDialConfig(cfg DialConfig) error {
	if cfg.NetworkMagic != MainnetNetworkMagic {
		return fmt.Errorf(
			"N2N network magic must be mainnet magic %d, got %d",
			MainnetNetworkMagic,
			cfg.NetworkMagic,
		)
	}
	if cfg.QueueCapacity < 1 || cfg.QueueCapacity > 32 ||
		cfg.HeaderBatchSize < 1 || cfg.HeaderBatchSize > 32 {
		return errors.New("invalid N2N dial configuration")
	}
	return nil
}

func newBlockFetchConfig(
	cfg DialConfig,
	blockFunc blockfetch.BlockFunc,
	batchDoneFunc blockfetch.BatchDoneFunc,
) (blockfetch.Config, error) {
	ret, err := blockfetch.NewConfig(
		blockfetch.WithBatchStartTimeout(cfg.BlockTimeout),
		blockfetch.WithBlockTimeout(cfg.BlockTimeout),
		blockfetch.WithRecvQueueSize(cfg.QueueCapacity),
		blockfetch.WithBlockFunc(blockFunc),
		blockfetch.WithBatchDoneFunc(batchDoneFunc),
	)
	if err != nil {
		return blockfetch.Config{}, err
	}
	if ret.SkipBlockValidation {
		return blockfetch.Config{}, errors.New("BlockFetch body-hash validation is disabled")
	}
	return ret, nil
}

func newChainSyncConfig(
	cfg DialConfig,
	rollForward chainsync.RollForwardFunc,
	rollBackward chainsync.RollBackwardFunc,
) chainsync.Config {
	chainCfg := chainsync.NewConfig(
		chainsync.WithPipelineLimit(cfg.QueueCapacity),
		chainsync.WithRecvQueueSize(cfg.QueueCapacity),
		chainsync.WithIntersectTimeout(cfg.DialTimeout),
		chainsync.WithBlockTimeout(cfg.BlockTimeout),
		chainsync.WithRollForwardFunc(rollForward),
		chainsync.WithRollBackwardFunc(rollBackward),
	)
	return chainCfg
}

func verifyHeaderBody(
	header lcommon.BlockHeader,
	block lcommon.Block,
	requested pcommon.Point,
) error {
	if block == nil {
		return errors.New("BlockFetch returned nil block")
	}
	if block.SlotNumber() != requested.Slot || header.SlotNumber() != requested.Slot {
		return errors.New("ChainSync header and BlockFetch body slot mismatch")
	}
	if block.Hash() != header.Hash() ||
		!strings.EqualFold(block.Hash().String(), hex.EncodeToString(requested.Hash)) {
		return errors.New("ChainSync header and BlockFetch body hash mismatch")
	}
	if block.BlockNumber() != header.BlockNumber() {
		return errors.New("ChainSync header and BlockFetch body height mismatch")
	}
	return nil
}

func ParsePoint(value string) (pcommon.Point, error) {
	if strings.EqualFold(strings.TrimSpace(value), "origin") {
		return pcommon.NewPointOrigin(), nil
	}
	slotText, hashText, ok := strings.Cut(value, ":")
	if !ok {
		return pcommon.Point{}, errors.New("point must be SLOT:64_HEX_HASH or origin")
	}
	slot, err := strconv.ParseUint(slotText, 10, 64)
	if err != nil {
		return pcommon.Point{}, fmt.Errorf("parse point slot: %w", err)
	}
	hash, err := hex.DecodeString(hashText)
	if err != nil {
		return pcommon.Point{}, fmt.Errorf("parse point hash: %w", err)
	}
	if len(hash) != 32 {
		return pcommon.Point{}, fmt.Errorf("point hash must be 32 bytes, got %d", len(hash))
	}
	return pcommon.NewPoint(slot, hash), nil
}

func safePointHash(point pcommon.Point) string {
	if isOrigin(point) {
		return "origin"
	}
	return hex.EncodeToString(point.Hash)
}

func pointsEqual(left, right pcommon.Point) bool {
	return left.Slot == right.Slot && bytes.Equal(left.Hash, right.Hash)
}
