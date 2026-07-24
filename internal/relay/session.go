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

type chainEvent struct {
	kind       EventKind
	header     expectedHeader
	rollback   Event
	atTip      bool
	observedAt time.Time
}

type activeRange struct {
	headers        []expectedHeader
	next           int
	rawBytes       uint64
	rawBudgetWait  time.Duration
	eventSendWait  time.Duration
	requestStarted time.Time
	firstRaw       time.Time
	lastRaw        time.Time
	completed      time.Time
	headerCollect  time.Duration
	getRangeWait   time.Duration
	interRangeIdle time.Duration
	preparedBefore bool
	hasPriorRange  bool
	done           chan error
	once           sync.Once
}

type fetchJob struct {
	kind          EventKind
	headers       []expectedHeader
	rollback      Event
	headerStarted time.Time
	readyAt       time.Time
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
	intersection model.Point
	hasIntersect bool
	cause        error

	chainEvents chan chainEvent
	fetchJobs   chan fetchJob

	rangeMu sync.Mutex
	active  *activeRange

	rawMu     sync.Mutex
	rawQueued int64
	rawWake   chan struct{}

	events chan Event
	ready  chan struct{}
	done   chan struct{}

	readyOnce sync.Once
	doneOnce  sync.Once

	fetchMetrics fetchMetrics
	fetchTotals  fetchTotals
	fetchLogAt   time.Time
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
	case config.BlockFetchRangeBlocks < 1 || config.BlockFetchRangeBlocks > 8192:
		err = errors.New("BlockFetch range must be in 1..8192")
	case config.BlockFetchQueueSize < 1 ||
		config.BlockFetchQueueSize > blockfetch.MaxRecvQueueSize:
		err = fmt.Errorf(
			"BlockFetch queue size must be in 1..%d",
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
	var miniProtocolDone []<-chan struct{}
	var workers sync.WaitGroup
	defer func() {
		cancel(retErr)
		s.closeConnection()
		for _, done := range miniProtocolDone {
			<-done
		}
		workers.Wait()
		s.finish(retErr)
	}()

	blockConfig, chainConfig, err := s.protocolConfigs()
	if err != nil {
		return wrapFailure("configure protocols", s.config.Host, err)
	}
	asyncErrors := make(chan error, 16)
	chainSyncErrors := make(chan error, 1)
	conn, err := ouroboros.NewConnection(
		ouroboros.WithNetworkMagic(s.config.NetworkMagic),
		ouroboros.WithNodeToNode(true),
		ouroboros.WithKeepAlive(true),
		ouroboros.WithDelayMuxerStart(true),
		ouroboros.WithDelayProtocolStart(true),
		ouroboros.WithErrorChan(asyncErrors),
		ouroboros.WithLogger(s.logger),
		ouroboros.WithBlockFetchConfig(blockConfig),
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
	if blockFetch == nil || blockFetch.Client == nil || conn.Muxer() == nil {
		return protocolFailure(
			"initialize protocols",
			s.config.Host,
			errors.New("required mini-protocol was not initialized after handshake"),
		)
	}
	chainSync, err := newChainSyncClient(protocol.ProtocolOptions{
		ConnectionId: conn.Id(),
		Muxer:        conn.Muxer(),
		Logger:       s.logger,
		ErrorChan:    chainSyncErrors,
		Mode:         protocol.ProtocolModeNodeToNode,
		Role:         protocol.ProtocolRoleClient,
	}, chainConfig)
	if err != nil {
		return wrapFailure("initialize ChainSync", s.config.Host, err)
	}
	chainSync.Start()
	defer chainSync.Stop()
	blockFetch.Client.Start()
	miniProtocolDone = []<-chan struct{}{
		chainSync.DoneChan(),
		blockFetch.Client.DoneChan(),
	}
	if keepAlive := conn.KeepAlive(); keepAlive != nil &&
		keepAlive.Client != nil {
		keepAlive.Client.Start()
		miniProtocolDone = append(
			miniProtocolDone,
			keepAlive.Client.DoneChan(),
		)
	}
	conn.Muxer().Start()
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

	startWorker := func(operation string, run func() error) {
		workers.Add(1)
		go func() {
			defer workers.Done()
			err := run()
			if err == nil {
				err = fmt.Errorf("%s ended without an error", operation)
			}
			if context.Cause(runCtx) == nil {
				cancel(err)
			}
		}()
	}
	startWorker("ChainSync range builder", func() error {
		return s.runRangeBuilder(runCtx)
	})
	startWorker("BlockFetch loop", func() error {
		return s.runFetchLoop(runCtx, blockFetch.Client)
	})

	chosen, err := selectIntersection(chainSync, candidates)
	if err != nil {
		return wrapFailure("intersect", s.config.Host, err)
	}
	s.runMu.Lock()
	s.intersection = chosen
	s.hasIntersect = true
	s.runMu.Unlock()
	if err := chainSync.Sync(
		[]pcommon.Point{toProtocolPoint(chosen)},
	); err != nil {
		return wrapFailure("start ChainSync", s.config.Host, err)
	}
	s.signalReady()

	select {
	case <-runCtx.Done():
		return wrapFailure("run", s.config.Host, context.Cause(runCtx))
	case err, ok := <-asyncErrors:
		if cause := context.Cause(runCtx); cause != nil {
			return wrapFailure("run", s.config.Host, cause)
		}
		if !ok {
			err = protocol.ErrProtocolShuttingDown
		}
		return wrapFailure("peer protocol", s.config.Host, err)
	case err := <-chainSyncErrors:
		return wrapFailure("ChainSync protocol", s.config.Host, err)
	case <-chainSync.DoneChan():
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
	s.fetchMetrics.start(time.Now())
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
		blockfetch.WithRecvQueueSize(s.config.BlockFetchQueueSize),
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

	chainConfig := chainsync.NewConfig(
		chainsync.WithPipelineLimit(chainSyncMaxOutstanding),
		chainsync.WithRecvQueueSize(chainsync.MaxRecvQueueSize),
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

	event := chainEvent{
		kind: Forward,
		header: expectedHeader{
			point:     point,
			tip:       tipPoint,
			blockType: blockType,
		},
		atTip:      sameModelProtocolPoint(point, tip.Point),
		observedAt: time.Now().UTC(),
	}
	if err := s.enqueueChainEvent(event); err != nil {
		return s.callbackError(err)
	}
	return nil
}

func (s *Session) onRollBackward(
	_ chainsync.CallbackContext,
	point pcommon.Point,
	tip chainsync.Tip,
) error {
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
	observedAt := time.Now().UTC()
	event := chainEvent{
		kind:       Rollback,
		observedAt: observedAt,
		rollback: Event{
			Kind:       Rollback,
			Point:      target,
			Tip:        tipPoint,
			Relay:      s.Identity(),
			ObservedAt: observedAt,
		},
	}
	if err := s.enqueueChainEvent(event); err != nil {
		return s.callbackError(err)
	}
	return nil
}

// ChainSync callbacks do only the point conversion above and this bounded
// enqueue. Returning quickly lets gOuroboros replenish its RequestNext
// pipeline without waiting for BlockFetch or downstream publication.
func (s *Session) enqueueChainEvent(event chainEvent) error {
	ctx := s.currentContext()
	if ctx == nil {
		return protocolFailure(
			"enqueue ChainSync event",
			s.config.Host,
			errors.New("session context is unavailable"),
		)
	}
	started := time.Now()
	select {
	case s.chainEvents <- event:
		s.fetchMetrics.observeChainEvent(
			event.kind,
			time.Since(started),
			len(s.chainEvents),
		)
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

// runRangeBuilder is the sole owner of the pending header batch. The input
// FIFO preserves ChainSync order; the output FIFO makes a rollback a strict
// barrier between all ranges before and after it.
func (s *Session) runRangeBuilder(ctx context.Context) error {
	if ctx == nil {
		return protocolFailure(
			"run ChainSync range builder",
			s.config.Host,
			errors.New("context is required"),
		)
	}
	pending := make(
		[]expectedHeader,
		0,
		s.config.BlockFetchRangeBlocks,
	)
	var headerStarted time.Time
	flush := func() error {
		if len(pending) == 0 {
			return nil
		}
		readyAt := time.Now()
		job := fetchJob{
			kind:          Forward,
			headers:       pending,
			headerStarted: headerStarted,
			readyAt:       readyAt,
		}
		pending = make(
			[]expectedHeader,
			0,
			s.config.BlockFetchRangeBlocks,
		)
		headerStarted = time.Time{}
		s.fetchMetrics.observePending(0)
		return s.enqueueJob(ctx, job)
	}

	for {
		var event chainEvent
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case event = <-s.chainEvents:
		}
		switch event.kind {
		case Forward:
			if len(pending) == 0 {
				headerStarted = event.observedAt
			}
			pending = append(pending, event.header)
			s.fetchMetrics.observePending(len(pending))
			if len(pending) >= s.config.BlockFetchRangeBlocks ||
				event.atTip {
				if err := flush(); err != nil {
					return err
				}
			}
		case Rollback:
			if err := flush(); err != nil {
				return err
			}
			if err := s.enqueueJob(ctx, fetchJob{
				kind:     Rollback,
				rollback: event.rollback,
			}); err != nil {
				return err
			}
		default:
			return protocolFailure(
				"run ChainSync range builder",
				s.config.Host,
				errors.New("invalid ChainSync event"),
			)
		}
	}
}

func (s *Session) enqueueJob(ctx context.Context, job fetchJob) error {
	started := time.Now()
	select {
	case s.fetchJobs <- job:
		s.fetchMetrics.observeJob(time.Since(started), len(s.fetchJobs))
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

func (s *Session) runFetchLoop(
	ctx context.Context,
	client rangeClient,
) error {
	if ctx == nil || client == nil {
		return protocolFailure(
			"run BlockFetch loop",
			s.config.Host,
			errors.New("context and range client are required"),
		)
	}
	var previousRangeDone time.Time
	for {
		if ctx.Err() != nil {
			return context.Cause(ctx)
		}
		var job fetchJob
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case job = <-s.fetchJobs:
		}
		switch job.kind {
		case Rollback:
			if len(job.headers) != 0 {
				return protocolFailure(
					"run BlockFetch loop",
					s.config.Host,
					errors.New("rollback job contains headers"),
				)
			}
			if err := s.emit(job.rollback, false); err != nil {
				return err
			}
			previousRangeDone = time.Time{}
		case Forward:
			if len(job.headers) == 0 {
				return protocolFailure(
					"run BlockFetch loop",
					s.config.Host,
					errors.New("fetch job has no headers"),
				)
			}
			completed, err := s.fetchRange(
				ctx,
				client,
				job,
				previousRangeDone,
			)
			if err != nil {
				return err
			}
			previousRangeDone = completed
		default:
			return protocolFailure(
				"run BlockFetch loop",
				s.config.Host,
				errors.New("invalid relay job"),
			)
		}
	}
}

func (s *Session) fetchRange(
	ctx context.Context,
	client rangeClient,
	job fetchJob,
	previousRangeDone time.Time,
) (time.Time, error) {
	requestStarted := time.Now()
	state := &activeRange{
		headers:        job.headers,
		requestStarted: requestStarted,
		headerCollect:  max(job.readyAt.Sub(job.headerStarted), 0),
		done:           make(chan error, 1),
	}
	if !previousRangeDone.IsZero() {
		state.hasPriorRange = true
		state.interRangeIdle = max(
			requestStarted.Sub(previousRangeDone),
			0,
		)
		state.preparedBefore = !job.readyAt.After(previousRangeDone)
	}
	s.rangeMu.Lock()
	if s.active != nil {
		s.rangeMu.Unlock()
		return time.Time{}, protocolFailure(
			"BlockFetch range",
			s.config.Host,
			errors.New("range already active"),
		)
	}
	s.active = state
	s.rangeMu.Unlock()

	start := toProtocolPoint(state.headers[0].point)
	end := toProtocolPoint(state.headers[len(state.headers)-1].point)
	getRangeStarted := time.Now()
	if err := client.GetBlockRange(start, end); err != nil {
		err = wrapFailure("request BlockFetch range", s.config.Host, err)
		s.abortRange(state, err)
		return time.Time{}, err
	}
	state.getRangeWait = time.Since(getRangeStarted)

	select {
	case err := <-state.done:
		if err != nil {
			return time.Time{}, err
		}
		s.observeCompletedRange(state)
		return state.completed, nil
	case <-ctx.Done():
		err := context.Cause(ctx)
		s.abortRange(state, err)
		return time.Time{}, err
	case <-client.DoneChan():
		err := wrapFailure(
			"BlockFetch range disconnected",
			s.config.Host,
			protocol.ErrProtocolShuttingDown,
		)
		s.abortRange(state, err)
		return time.Time{}, err
	}
}

func (s *Session) onRawBlock(
	_ blockfetch.CallbackContext,
	blockType uint,
	raw []byte,
) error {
	observedAt := time.Now().UTC()
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
	if state.firstRaw.IsZero() {
		state.firstRaw = observedAt
	}
	state.lastRaw = observedAt
	state.rawBytes += uint64(len(raw))
	s.rangeMu.Unlock()

	event := Event{
		Kind:       Forward,
		Point:      expected.point,
		Tip:        expected.tip,
		BlockType:  blockType,
		RawLength:  uint64(len(raw)),
		Digest:     RawBlockDigest(blockType, raw),
		Relay:      s.Identity(),
		ObservedAt: observedAt,
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
		reserveStarted := time.Now()
		if err := s.reserveRaw(ctx, int64(len(raw))); err != nil {
			s.abortRange(state, err)
			return s.callbackError(err)
		}
		state.rawBudgetWait += time.Since(reserveStarted)
		rawReserved = true
		event.RawCBOR = bytes.Clone(raw)
	}
	emitStarted := time.Now()
	if err := s.emit(event, rawReserved); err != nil {
		s.abortRange(state, err)
		return s.callbackError(err)
	}
	state.eventSendWait += time.Since(emitStarted)
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
	state.completed = time.Now()
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
		s.fetchMetrics.observeEventDepth(len(s.events))
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
			queued := s.rawQueued
			s.rawMu.Unlock()
			s.fetchMetrics.observeRawDepth(queued)
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
