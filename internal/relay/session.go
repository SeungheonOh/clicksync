package relay

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"cardano-clicksync/internal/model"

	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/protocol"
	"github.com/blinklabs-io/gouroboros/protocol/blockfetch"
	"github.com/blinklabs-io/gouroboros/protocol/chainsync"
	pcommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

type intersectionClient interface {
	GetAvailableBlockRange([]pcommon.Point) (pcommon.Point, pcommon.Point, error)
}

type rangeClient interface {
	GetBlockRange(pcommon.Point, pcommon.Point) error
	DoneChan() <-chan struct{}
}

type expectedHeader struct {
	point     model.Point
	tip       model.Point
	blockType uint
}

type activeRange struct {
	headers []expectedHeader
	next    int
	done    chan error
	once    sync.Once
}

func (r *activeRange) finish(err error) {
	r.once.Do(func() {
		r.done <- err
	})
}

type Session struct {
	config Config
	logger *slog.Logger

	identityMu sync.RWMutex
	identity   model.RelayIdentity

	runMu        sync.Mutex
	started      bool
	runCtx       context.Context
	cancel       context.CancelCauseFunc
	conn         *ouroboros.Connection
	rangeClient  rangeClient
	intersection model.Point
	hasIntersect bool
	cause        error

	rangeMu sync.Mutex
	pending []expectedHeader
	active  *activeRange

	rawMu     sync.Mutex
	rawQueued int64
	rawWake   chan struct{}

	events chan Event
	ready  chan struct{}
	done   chan struct{}

	readyOnce sync.Once
	doneOnce  sync.Once
}

func validateConfig(config Config, logger *slog.Logger) error {
	var err error
	switch {
	case logger == nil:
		err = errors.New("logger is required")
	case config.RelayIndex < 0:
		err = errors.New("relay index must be non-negative")
	case strings.TrimSpace(config.Host) == "":
		err = errors.New("relay host is required")
	case strings.TrimSpace(config.Operator) == "":
		err = errors.New("relay operator is required")
	case config.NetworkMagic == 0:
		err = errors.New("network magic must be non-zero")
	case config.HeaderBatchSize < 1 || config.HeaderBatchSize > blockfetch.MaxRecvQueueSize:
		err = fmt.Errorf("header batch size must be in 1..%d", blockfetch.MaxRecvQueueSize)
	case config.ProtocolQueueSize < config.HeaderBatchSize ||
		config.ProtocolQueueSize > blockfetch.MaxRecvQueueSize:
		err = fmt.Errorf(
			"protocol queue size must be between header batch size and %d",
			blockfetch.MaxRecvQueueSize,
		)
	case config.RelayQueueSize < 1 || config.RelayQueueSize > 4096:
		err = errors.New("relay queue size must be in 1..4096")
	case config.RawQueueBytes <= 0:
		err = errors.New("raw queue byte limit must be positive")
	case config.DialTimeout <= 0:
		err = errors.New("dial timeout must be positive")
	case config.BlockTimeout <= 0:
		err = errors.New("block timeout must be positive")
	}
	if err == nil {
		return nil
	}
	return &Error{
		Kind:      FailureConfig,
		Operation: "configure",
		Relay:     config.Host,
		Err:       err,
	}
}

// Run owns the connection until it fails or ctx is canceled. A Session is
// single-use so its bounded event channel has an unambiguous terminal close.
func (s *Session) Run(ctx context.Context, candidates []model.Point) (retErr error) {
	if ctx == nil {
		return wrapFailure("run", s.config.Host, errors.New("context is required"))
	}
	if err := validateCandidates(candidates); err != nil {
		return wrapFailure("intersect", s.config.Host, err)
	}
	runCtx, cancel, err := s.begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		cancel(retErr)
		s.closeConnection()
		s.finish(retErr)
	}()

	blockConfig, chainConfig, err := s.protocolConfigs()
	if err != nil {
		return wrapFailure("configure protocols", s.config.Host, err)
	}
	asyncErrors := make(chan error, 16)
	conn, err := ouroboros.NewConnection(
		ouroboros.WithNetworkMagic(s.config.NetworkMagic),
		ouroboros.WithNodeToNode(true),
		ouroboros.WithKeepAlive(true),
		ouroboros.WithErrorChan(asyncErrors),
		ouroboros.WithLogger(s.logger),
		ouroboros.WithBlockFetchConfig(blockConfig),
		ouroboros.WithChainSyncConfig(chainConfig),
	)
	if err != nil {
		return wrapFailure("create connection", s.config.Host, err)
	}
	s.runMu.Lock()
	s.conn = conn
	s.runMu.Unlock()
	if err := conn.DialTimeout("tcp", s.config.Host, s.config.DialTimeout); err != nil {
		return wrapFailure("connect", s.config.Host, err)
	}
	blockFetch := conn.BlockFetch()
	chainSync := conn.ChainSync()
	if blockFetch == nil || blockFetch.Client == nil ||
		chainSync == nil || chainSync.Client == nil {
		return protocolFailure(
			"initialize protocols",
			s.config.Host,
			errors.New("required mini-protocol was not initialized after handshake"),
		)
	}
	s.runMu.Lock()
	s.rangeClient = blockFetch.Client
	s.runMu.Unlock()
	s.captureNegotiatedIdentity(conn)

	closeWatcherDone := make(chan struct{})
	go func() {
		defer close(closeWatcherDone)
		<-runCtx.Done()
		_ = conn.Close()
	}()
	defer func() {
		cancel(retErr)
		<-closeWatcherDone
	}()

	chosen, err := selectIntersection(chainSync.Client, candidates)
	if err != nil {
		return wrapFailure("intersect", s.config.Host, err)
	}
	s.runMu.Lock()
	s.intersection = chosen
	s.hasIntersect = true
	s.runMu.Unlock()
	if err := chainSync.Client.Sync(
		[]pcommon.Point{toProtocolPoint(chosen)},
	); err != nil {
		return wrapFailure("start ChainSync", s.config.Host, err)
	}
	s.signalReady()

	select {
	case <-runCtx.Done():
		return wrapFailure("run", s.config.Host, context.Cause(runCtx))
	case err := <-asyncErrors:
		if cause := context.Cause(runCtx); cause != nil {
			return wrapFailure("run", s.config.Host, cause)
		}
		return wrapFailure("peer protocol", s.config.Host, err)
	case <-chainSync.Client.DoneChan():
		if cause := context.Cause(runCtx); cause != nil {
			return wrapFailure("run", s.config.Host, cause)
		}
		return wrapFailure(
			"ChainSync disconnected",
			s.config.Host,
			protocol.ErrProtocolShuttingDown,
		)
	case <-blockFetch.Client.DoneChan():
		if cause := context.Cause(runCtx); cause != nil {
			return wrapFailure("run", s.config.Host, cause)
		}
		return wrapFailure(
			"BlockFetch disconnected",
			s.config.Host,
			protocol.ErrProtocolShuttingDown,
		)
	}
}

func (s *Session) begin(
	parent context.Context,
) (context.Context, context.CancelCauseFunc, error) {
	s.runMu.Lock()
	defer s.runMu.Unlock()
	if s.started {
		return nil, nil, protocolFailure(
			"run",
			s.config.Host,
			errors.New("session has already run"),
		)
	}
	s.started = true
	s.runCtx, s.cancel = context.WithCancelCause(parent)
	return s.runCtx, s.cancel, nil
}

func (s *Session) finish(cause error) {
	s.runMu.Lock()
	s.cause = cause
	s.runMu.Unlock()
	s.signalReady()
	s.doneOnce.Do(func() {
		close(s.events)
		for event := range s.events {
			s.releaseRaw(event)
		}
		close(s.done)
	})
}

func (s *Session) closeConnection() {
	s.runMu.Lock()
	conn := s.conn
	s.runMu.Unlock()
	if conn != nil {
		_ = conn.Close()
	}
}

func (s *Session) signalReady() {
	s.readyOnce.Do(func() {
		close(s.ready)
	})
}

func (s *Session) Ready() <-chan struct{} {
	return s.ready
}

func (s *Session) Done() <-chan struct{} {
	return s.done
}

func (s *Session) Cause() error {
	s.runMu.Lock()
	defer s.runMu.Unlock()
	return s.cause
}

func (s *Session) Identity() model.RelayIdentity {
	s.identityMu.RLock()
	defer s.identityMu.RUnlock()
	return s.identity
}

func (s *Session) Intersection() (model.Point, bool) {
	s.runMu.Lock()
	defer s.runMu.Unlock()
	return s.intersection, s.hasIntersect
}

func (s *Session) QueueDepth() (int, int) {
	return len(s.events), cap(s.events)
}

func (s *Session) RawQueueDepth() (int64, int64) {
	s.rawMu.Lock()
	defer s.rawMu.Unlock()
	return s.rawQueued, s.config.RawQueueBytes
}

func (s *Session) Next(ctx context.Context) (Event, error) {
	if ctx == nil {
		return Event{}, errors.New("relay reader context is required")
	}
	select {
	case <-ctx.Done():
		return Event{}, context.Cause(ctx)
	case event, ok := <-s.events:
		if ok {
			s.releaseRaw(event)
			return event, nil
		}
		if cause := s.Cause(); cause != nil {
			return Event{}, cause
		}
		return Event{}, errors.New("relay event stream closed")
	}
}

func (s *Session) captureNegotiatedIdentity(conn *ouroboros.Connection) {
	identity := s.Identity()
	identity.N2NVersion, _ = conn.ProtocolVersion()
	if remote := conn.Id().RemoteAddr; remote != nil {
		identity.Address = remote.String()
	}
	s.identityMu.Lock()
	s.identity = identity
	s.identityMu.Unlock()
}

func (s *Session) protocolConfigs() (
	blockfetch.Config,
	chainsync.Config,
	error,
) {
	blockConfig, err := blockfetch.NewConfig(
		blockfetch.WithBatchStartTimeout(s.config.BlockTimeout),
		blockfetch.WithBlockTimeout(s.config.BlockTimeout),
		blockfetch.WithRecvQueueSize(s.config.ProtocolQueueSize),
		blockfetch.WithBlockRawFunc(s.onRawBlock),
		blockfetch.WithBatchDoneFunc(s.onBatchDone),
	)
	if err != nil {
		return blockfetch.Config{}, chainsync.Config{}, err
	}
	// A raw callback does not invoke the decoded-block path. Keep this flag
	// explicit so a future callback change cannot silently re-enable body
	// validation.
	blockConfig.SkipBlockValidation = true

	chainQueue := min(s.config.ProtocolQueueSize, chainsync.MaxRecvQueueSize)
	chainConfig := chainsync.NewConfig(
		chainsync.WithPipelineLimit(chainQueue),
		chainsync.WithRecvQueueSize(chainQueue),
		chainsync.WithIntersectTimeout(s.config.DialTimeout),
		chainsync.WithBlockTimeout(s.config.BlockTimeout),
		chainsync.WithRollForwardFunc(s.onRollForward),
		chainsync.WithRollBackwardFunc(s.onRollBackward),
	)
	chainConfig.SkipBlockValidation = true
	return blockConfig, chainConfig, nil
}

func (s *Session) onRollForward(
	_ chainsync.CallbackContext,
	blockType uint,
	value any,
	tip chainsync.Tip,
) error {
	header, ok := value.(lcommon.BlockHeader)
	if !ok || header == nil {
		return s.callbackError(protocolFailure(
			"ChainSync roll-forward",
			s.config.Host,
			fmt.Errorf("unexpected header type %T", value),
		))
	}
	point, err := pointFromHeader(header)
	if err != nil {
		return s.callbackError(protocolFailure(
			"ChainSync roll-forward",
			s.config.Host,
			err,
		))
	}
	tipPoint, err := pointFromTip(tip)
	if err != nil {
		return s.callbackError(protocolFailure(
			"ChainSync tip",
			s.config.Host,
			err,
		))
	}

	s.rangeMu.Lock()
	if s.active != nil {
		s.rangeMu.Unlock()
		return s.callbackError(protocolFailure(
			"ChainSync roll-forward",
			s.config.Host,
			errors.New("header arrived during an active BlockFetch range"),
		))
	}
	s.pending = append(s.pending, expectedHeader{
		point:     point,
		tip:       tipPoint,
		blockType: blockType,
	})
	flush := len(s.pending) >= s.config.HeaderBatchSize ||
		sameModelProtocolPoint(point, tip.Point)
	s.rangeMu.Unlock()
	if !flush {
		return nil
	}
	if err := s.flushPending(); err != nil {
		return s.callbackError(err)
	}
	return nil
}

func (s *Session) onRollBackward(
	_ chainsync.CallbackContext,
	point pcommon.Point,
	tip chainsync.Tip,
) error {
	// Pending roll-forwards happened before this rollback and must remain in
	// the relay's event order even when the header range was not full.
	if err := s.flushPending(); err != nil {
		return s.callbackError(err)
	}
	target, err := pointFromProtocol(point)
	if err != nil {
		return s.callbackError(protocolFailure(
			"ChainSync rollback",
			s.config.Host,
			err,
		))
	}
	tipPoint, err := pointFromTip(tip)
	if err != nil {
		return s.callbackError(protocolFailure(
			"ChainSync rollback tip",
			s.config.Host,
			err,
		))
	}
	event := Event{
		Kind:       Rollback,
		Point:      target,
		Tip:        tipPoint,
		Relay:      s.Identity(),
		ObservedAt: time.Now().UTC(),
	}
	if err := s.emit(event, false); err != nil {
		return s.callbackError(err)
	}
	return nil
}

func (s *Session) flushPending() error {
	s.rangeMu.Lock()
	if s.active != nil {
		s.rangeMu.Unlock()
		return protocolFailure(
			"BlockFetch range",
			s.config.Host,
			errors.New("range already active"),
		)
	}
	if len(s.pending) == 0 {
		s.rangeMu.Unlock()
		return nil
	}
	state := &activeRange{
		headers: append([]expectedHeader(nil), s.pending...),
		done:    make(chan error, 1),
	}
	s.pending = s.pending[:0]
	s.active = state
	s.rangeMu.Unlock()

	client := s.currentRangeClient()
	if client == nil {
		err := protocolFailure(
			"BlockFetch range",
			s.config.Host,
			errors.New("range client is unavailable"),
		)
		s.abortRange(state, err)
		return err
	}
	start := toProtocolPoint(state.headers[0].point)
	end := toProtocolPoint(state.headers[len(state.headers)-1].point)
	if err := client.GetBlockRange(start, end); err != nil {
		err = wrapFailure("request BlockFetch range", s.config.Host, err)
		s.abortRange(state, err)
		return err
	}

	ctx := s.currentContext()
	if ctx == nil {
		err := protocolFailure(
			"BlockFetch range",
			s.config.Host,
			errors.New("session context is unavailable"),
		)
		s.abortRange(state, err)
		return err
	}
	select {
	case err := <-state.done:
		return err
	case <-ctx.Done():
		err := context.Cause(ctx)
		s.abortRange(state, err)
		return err
	case <-client.DoneChan():
		err := wrapFailure(
			"BlockFetch range disconnected",
			s.config.Host,
			protocol.ErrProtocolShuttingDown,
		)
		s.abortRange(state, err)
		return err
	}
}

func (s *Session) onRawBlock(
	_ blockfetch.CallbackContext,
	blockType uint,
	raw []byte,
) error {
	if len(raw) == 0 {
		return s.callbackError(protocolFailure(
			"BlockFetch raw block",
			s.config.Host,
			errors.New("empty raw block callback"),
		))
	}
	s.rangeMu.Lock()
	state := s.active
	if state == nil {
		s.rangeMu.Unlock()
		return s.callbackError(protocolFailure(
			"BlockFetch raw block",
			s.config.Host,
			errors.New("block arrived without an active range"),
		))
	}
	if state.next >= len(state.headers) {
		s.rangeMu.Unlock()
		err := protocolFailure(
			"BlockFetch raw block",
			s.config.Host,
			fmt.Errorf("extra block after %d expected callbacks", len(state.headers)),
		)
		s.abortRange(state, err)
		return s.callbackError(err)
	}
	expected := state.headers[state.next]
	if blockType != expected.blockType {
		s.rangeMu.Unlock()
		err := protocolFailure(
			"BlockFetch raw block",
			s.config.Host,
			fmt.Errorf(
				"callback %d has block type %d, want %d",
				state.next,
				blockType,
				expected.blockType,
			),
		)
		s.abortRange(state, err)
		return s.callbackError(err)
	}
	state.next++
	s.rangeMu.Unlock()

	event := Event{
		Kind:       Forward,
		Point:      expected.point,
		Tip:        expected.tip,
		BlockType:  blockType,
		RawLength:  uint64(len(raw)),
		Digest:     RawBlockDigest(blockType, raw),
		Relay:      s.Identity(),
		ObservedAt: time.Now().UTC(),
	}
	rawReserved := false
	if s.config.RelayIndex == 0 {
		ctx := s.currentContext()
		if ctx == nil {
			err := protocolFailure(
				"reserve raw event",
				s.config.Host,
				errors.New("session context is unavailable"),
			)
			s.abortRange(state, err)
			return s.callbackError(err)
		}
		if err := s.reserveRaw(ctx, int64(len(raw))); err != nil {
			s.abortRange(state, err)
			return s.callbackError(err)
		}
		rawReserved = true
		event.RawCBOR = bytes.Clone(raw)
	}
	if err := s.emit(event, rawReserved); err != nil {
		s.abortRange(state, err)
		return s.callbackError(err)
	}
	return nil
}

func (s *Session) onBatchDone(_ blockfetch.CallbackContext) error {
	s.rangeMu.Lock()
	state := s.active
	if state == nil {
		s.rangeMu.Unlock()
		return s.callbackError(protocolFailure(
			"BlockFetch batch done",
			s.config.Host,
			errors.New("batch completed without an active range"),
		))
	}
	s.active = nil
	var err error
	if state.next != len(state.headers) {
		err = protocolFailure(
			"BlockFetch batch done",
			s.config.Host,
			fmt.Errorf(
				"received %d raw callbacks for %d headers",
				state.next,
				len(state.headers),
			),
		)
	}
	s.rangeMu.Unlock()
	state.finish(err)
	if err != nil {
		return s.callbackError(err)
	}
	return nil
}

func (s *Session) abortRange(state *activeRange, err error) {
	s.rangeMu.Lock()
	if s.active == state {
		s.active = nil
	}
	s.rangeMu.Unlock()
	state.finish(err)
}

func (s *Session) emit(event Event, rawReserved bool) error {
	ctx := s.currentContext()
	if ctx == nil {
		return protocolFailure(
			"emit event",
			s.config.Host,
			errors.New("session context is unavailable"),
		)
	}
	if len(event.RawCBOR) != 0 && !rawReserved {
		return protocolFailure(
			"emit event",
			s.config.Host,
			errors.New("raw event has no byte reservation"),
		)
	}
	select {
	case s.events <- event:
		return nil
	case <-ctx.Done():
		s.releaseRaw(event)
		return context.Cause(ctx)
	}
}

func (s *Session) reserveRaw(ctx context.Context, size int64) error {
	if size == 0 {
		return nil
	}
	if size > s.config.RawQueueBytes {
		return &Error{
			Kind:      FailureBound,
			Operation: "reserve raw event",
			Relay:     s.config.Host,
			Err: fmt.Errorf(
				"raw block is %d bytes, queue limit is %d",
				size,
				s.config.RawQueueBytes,
			),
		}
	}
	for {
		s.rawMu.Lock()
		if s.rawQueued <= s.config.RawQueueBytes-size {
			s.rawQueued += size
			s.rawMu.Unlock()
			return nil
		}
		s.rawMu.Unlock()
		select {
		case <-s.rawWake:
		case <-ctx.Done():
			return context.Cause(ctx)
		}
	}
}

func (s *Session) releaseRaw(event Event) {
	size := int64(len(event.RawCBOR))
	if size == 0 {
		return
	}
	s.rawMu.Lock()
	if size > s.rawQueued {
		s.rawQueued = 0
	} else {
		s.rawQueued -= size
	}
	s.rawMu.Unlock()
	select {
	case s.rawWake <- struct{}{}:
	default:
	}
}

func (s *Session) callbackError(err error) error {
	s.runMu.Lock()
	cancel := s.cancel
	s.runMu.Unlock()
	if cancel != nil {
		cancel(err)
	}
	return err
}

func (s *Session) currentContext() context.Context {
	s.runMu.Lock()
	defer s.runMu.Unlock()
	return s.runCtx
}

func (s *Session) currentRangeClient() rangeClient {
	s.runMu.Lock()
	defer s.runMu.Unlock()
	return s.rangeClient
}

func selectIntersection(
	client intersectionClient,
	candidates []model.Point,
) (model.Point, error) {
	for _, candidate := range candidates {
		_, _, err := client.GetAvailableBlockRange(
			[]pcommon.Point{toProtocolPoint(candidate)},
		)
		switch {
		case err == nil:
			return candidate, nil
		case errors.Is(err, chainsync.ErrIntersectNotFound):
			continue
		default:
			return model.Point{}, err
		}
	}
	return model.Point{}, &Error{
		Kind:      FailureIntersection,
		Operation: "find intersection",
		Err:       errors.New("none of the supplied points was accepted"),
	}
}

func validateCandidates(candidates []model.Point) error {
	if len(candidates) == 0 {
		return &Error{
			Kind:      FailureIntersection,
			Operation: "validate candidates",
			Err:       errors.New("at least one intersection candidate is required"),
		}
	}
	for index, candidate := range candidates {
		if candidate.Origin {
			if index != len(candidates)-1 {
				return errors.New("origin intersection candidate must be last")
			}
			continue
		}
		if candidate.Hash == (model.Hash32{}) {
			return fmt.Errorf(
				"intersection candidate at slot %d has an empty hash",
				candidate.Slot,
			)
		}
	}
	return nil
}

func pointFromHeader(header lcommon.BlockHeader) (model.Point, error) {
	hash := header.Hash().Bytes()
	if len(hash) != len(model.Hash32{}) {
		return model.Point{}, fmt.Errorf("header hash has %d bytes, want 32", len(hash))
	}
	var ret model.Point
	ret.Slot = header.SlotNumber()
	ret.BlockNumber = header.BlockNumber()
	copy(ret.Hash[:], hash)
	_, ret.IsByronEBB = header.(*byron.ByronEpochBoundaryBlockHeader)
	return ret, nil
}

func pointFromTip(tip chainsync.Tip) (model.Point, error) {
	ret, err := pointFromProtocol(tip.Point)
	if err != nil {
		return model.Point{}, err
	}
	ret.BlockNumber = tip.BlockNumber
	return ret, nil
}

func pointFromProtocol(point pcommon.Point) (model.Point, error) {
	if point.Slot == 0 && len(point.Hash) == 0 {
		return model.Point{Origin: true}, nil
	}
	if len(point.Hash) != len(model.Hash32{}) {
		return model.Point{}, fmt.Errorf(
			"point at slot %d has %d hash bytes, want 32",
			point.Slot,
			len(point.Hash),
		)
	}
	var ret model.Point
	ret.Slot = point.Slot
	copy(ret.Hash[:], point.Hash)
	return ret, nil
}

func toProtocolPoint(point model.Point) pcommon.Point {
	if point.Origin {
		return pcommon.NewPointOrigin()
	}
	return pcommon.NewPoint(point.Slot, bytes.Clone(point.Hash[:]))
}

func sameProtocolPoint(left, right pcommon.Point) bool {
	return left.Slot == right.Slot && bytes.Equal(left.Hash, right.Hash)
}

func sameModelProtocolPoint(left model.Point, right pcommon.Point) bool {
	return !left.Origin &&
		left.Slot == right.Slot &&
		bytes.Equal(left.Hash[:], right.Hash)
}
