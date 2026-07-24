package syncer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"cardano-clicksync/internal/agreement"
	"cardano-clicksync/internal/config"
	"cardano-clicksync/internal/model"
	"cardano-clicksync/internal/pipeline"
	"cardano-clicksync/internal/relay"
	"cardano-clicksync/internal/store"
)

var (
	errAttemptStopped        = errors.New("sync attempt stopped")
	errSessionCleanupTimeout = errors.New("relay session cleanup deadline exceeded")

	// ErrShutdownTimeout means shutdown could not be proven complete before
	// its deadline. A process owner must terminate without releasing the
	// writer lock or closing the store underneath the abandoned work.
	ErrShutdownTimeout = errors.New("sync shutdown deadline exceeded")
)

type RuntimeStore interface {
	PublicationStore
	State(context.Context, uint32) (store.State, error)
}

type RelaySession interface {
	agreement.Source
	Run(context.Context, []model.Point) error
	Ready() <-chan struct{}
	Intersection() (model.Point, bool)
	Identity() model.RelayIdentity
	Cause() error
}

type SessionFactory func(relay.Config, *slog.Logger) (RelaySession, error)

type Runner struct {
	Config           config.Sync
	Store            RuntimeStore
	Lock             store.Lock
	Logger           *slog.Logger
	Metrics          *Metrics
	NewSession       SessionFactory
	Decode           pipeline.Decode[model.AgreedEvent, DecodedBlock]
	ProgressInterval time.Duration
}

type attemptOutcome struct {
	progress  bool
	graceful  bool
	retryable bool
	fallback  []model.Point
}

type intersectionDifference struct {
	chosen   []model.Point
	fallback []model.Point
}

func (e *intersectionDifference) Error() string {
	return fmt.Sprintf(
		"relays selected different intersections (%d selections)",
		len(e.chosen),
	)
}

type stageFailure struct {
	stage string
	err   error
}

func (e *stageFailure) Error() string {
	return fmt.Sprintf("%s stage failed: %v", e.stage, e.err)
}

func (e *stageFailure) Unwrap() error {
	return e.err
}

func (r Runner) Run(ctx context.Context) error {
	if ctx == nil {
		return errors.New("runner context is required")
	}
	if err := r.validate(); err != nil {
		return err
	}
	done := make(chan error, 1)
	go func() {
		done <- r.run(ctx)
	}()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		timer := time.NewTimer(r.Config.ShutdownTimeout)
		defer timer.Stop()
		select {
		case err := <-done:
			return err
		case <-timer.C:
			return ErrShutdownTimeout
		}
	}
}

func (r Runner) run(ctx context.Context) error {
	if r.Metrics == nil {
		r.Metrics = &Metrics{}
	}
	if r.NewSession == nil {
		r.NewSession = func(
			cfg relay.Config,
			logger *slog.Logger,
		) (RelaySession, error) {
			return relay.New(cfg, logger)
		}
	}
	if r.Decode == nil {
		r.Decode = DecodeAgreedBlock
	}
	if r.ProgressInterval <= 0 {
		r.ProgressInterval = 10 * time.Second
	}

	progressDone := make(chan struct{})
	go r.logProgress(ctx, progressDone)
	defer close(progressDone)

	backoff := r.Config.ReconnectInitial
	var (
		forcedCandidates []model.Point
		forcedSnapshot   uint64
	)
	for {
		if ctx.Err() != nil {
			return nil
		}
		state, err := r.Store.State(ctx, r.Config.RollbackDepth)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("recover durable sync state: %w", err)
		}
		candidates := pointsFromStore(state.Intersections)
		if len(forcedCandidates) > 0 {
			if state.Snapshot == forcedSnapshot {
				candidates = append([]model.Point(nil), forcedCandidates...)
			} else {
				forcedCandidates = nil
			}
		}
		if len(candidates) == 0 {
			return errors.New("store returned no relay intersection candidates")
		}

		r.Metrics.observeAttempt()
		outcome, err := r.runAttempt(ctx, state.Tip.Model(), candidates)
		if outcome.graceful {
			return err
		}
		if err == nil {
			return errors.New("sync attempt ended without a result")
		}
		if !outcome.retryable {
			return err
		}
		if len(outcome.fallback) > 0 {
			forcedCandidates = append(
				forcedCandidates[:0],
				outcome.fallback...,
			)
			forcedSnapshot = state.Snapshot
		}
		wait := backoff
		if outcome.progress {
			wait = r.Config.ReconnectInitial
			backoff = r.Config.ReconnectInitial
		} else {
			backoff = doubledDuration(
				backoff,
				r.Config.ReconnectMaximum,
			)
		}
		r.Metrics.observeReconnect()
		attributes := []any{
			"error", err,
			"backoff", wait,
			"made_progress", outcome.progress,
		}
		attributes = append(attributes, retryDiagnostics(err)...)
		r.Logger.Warn("restarting complete relay set", attributes...)
		if err := waitContext(ctx, wait); err != nil {
			return nil
		}
	}
}

func (r Runner) validate() error {
	switch {
	case r.Store == nil:
		return errors.New("runner store is required")
	case r.Lock == nil:
		return errors.New("runner writer lock is required")
	case r.Logger == nil:
		return errors.New("runner logger is required")
	}
	if err := r.Config.Validate(); err != nil {
		return fmt.Errorf("runner configuration: %w", err)
	}
	return nil
}

func (r Runner) runAttempt(
	parent context.Context,
	durableTip model.Point,
	candidates []model.Point,
) (outcome attemptOutcome, retErr error) {
	var durableProgress atomic.Bool
	attemptCtx, cancelAttempt := context.WithCancelCause(context.Background())
	attemptFinished := make(chan struct{})
	shutdownExpired := make(chan struct{})
	parentWatchDone := make(chan struct{})
	go func() {
		defer close(parentWatchDone)
		select {
		case <-parent.Done():
			cancelAttempt(context.Cause(parent))
			timer := time.NewTimer(r.Config.ShutdownTimeout)
			defer timer.Stop()
			select {
			case <-timer.C:
				close(shutdownExpired)
			case <-attemptFinished:
			}
		case <-attemptFinished:
		}
	}()

	sessions := make([]RelaySession, len(r.Config.Relays))
	for index, configured := range r.Config.Relays {
		session, err := r.NewSession(
			relay.Config{
				RelayIndex:        index,
				Host:              configured.Host,
				Operator:          configured.Operator,
				NetworkMagic:      r.Config.NetworkMagic,
				ProtocolQueueSize: r.Config.ProtocolQueueSize,
				HeaderBatchSize:   r.Config.HeaderBatchSize,
				RelayQueueSize:    r.Config.RelayQueueSize,
				RawQueueBytes:     r.Config.AgreedQueueBytes,
				DialTimeout:       r.Config.DialTimeout,
				BlockTimeout:      r.Config.ProtocolTimeout,
			},
			r.Logger.With(
				"relay_index", index,
				"relay_host", configured.Host,
				"relay_operator", configured.Operator,
			),
		)
		if err != nil {
			cancelAttempt(err)
			close(attemptFinished)
			<-parentWatchDone
			return attemptOutcome{}, fmt.Errorf(
				"create relay session %d: %w",
				index,
				err,
			)
		}
		sessions[index] = session
	}

	var sessionWait sync.WaitGroup
	sessionWait.Add(len(sessions))
	for index, session := range sessions {
		go func() {
			defer sessionWait.Done()
			err := session.Run(attemptCtx, candidates)
			if err == nil {
				err = errors.New("relay session ended without an error")
			}
			if context.Cause(attemptCtx) == nil {
				cancelAttempt(fmt.Errorf("relay session %d: %w", index, err))
			}
		}()
	}
	defer func() {
		cancelAttempt(errAttemptStopped)
		sessionsDone := make(chan struct{})
		go func() {
			sessionWait.Wait()
			close(sessionsDone)
		}()
		if parent.Err() != nil {
			select {
			case <-sessionsDone:
			case <-shutdownExpired:
				outcome = attemptOutcome{}
				retErr = errors.Join(retErr, ErrShutdownTimeout)
			}
		} else {
			timer := time.NewTimer(r.Config.ProtocolTimeout)
			select {
			case <-sessionsDone:
			case <-timer.C:
				outcome = attemptOutcome{}
				retErr = errors.Join(retErr, errSessionCleanupTimeout)
			}
			timer.Stop()
		}
		close(attemptFinished)
		<-parentWatchDone
	}()

	chosen := make([]model.Point, len(sessions))
	for index, session := range sessions {
		select {
		case <-session.Ready():
			point, ok := session.Intersection()
			if !ok {
				err := session.Cause()
				if err == nil {
					err = errors.New("relay stopped before selecting an intersection")
				}
				if parent.Err() != nil {
					return attemptOutcome{graceful: true}, nil
				}
				return attemptOutcome{retryable: true}, fmt.Errorf(
					"relay %d did not intersect: %w",
					index,
					err,
				)
			}
			chosen[index] = point
		case <-attemptCtx.Done():
			if parent.Err() != nil {
				return attemptOutcome{graceful: true}, nil
			}
			return attemptOutcome{retryable: true}, context.Cause(attemptCtx)
		}
	}
	if fallback, same, err := commonIntersection(candidates, chosen); err != nil {
		return attemptOutcome{}, err
	} else if !same {
		r.Metrics.observeMismatch()
		difference := &intersectionDifference{
			chosen:   append([]model.Point(nil), chosen...),
			fallback: fallback,
		}
		return attemptOutcome{
			retryable: true,
			fallback:  fallback,
		}, difference
	}
	if chosen[0] != durableTip {
		relays := make([]model.RelayIdentity, len(sessions))
		for index, session := range sessions {
			relays[index] = session.Identity()
		}
		if err := (StoreSink{
			Store:          r.Store,
			Lock:           r.Lock,
			MaximumDepth:   r.Config.RollbackDepth,
			Metrics:        r.Metrics,
			RollbackReason: "unanimous relay intersection rollback",
			OnProgress: func() {
				durableProgress.Store(true)
			},
		}).Rollback(parent, model.AgreedEvent{
			Kind:       model.EventRollback,
			Point:      chosen[0],
			Relays:     relays,
			ObservedAt: time.Now().UTC(),
		}); err != nil {
			if parent.Err() != nil {
				return attemptOutcome{graceful: true}, err
			}
			return attemptOutcome{}, fmt.Errorf(
				"reconcile durable tip to unanimous relay intersection: %w",
				err,
			)
		}
	}

	engineSources := make([]agreement.Source, len(sessions))
	for index, session := range sessions {
		engineSources[index] = session
	}
	engine, err := agreement.New(engineSources...)
	if err != nil {
		return attemptOutcome{}, fmt.Errorf("create agreement engine: %w", err)
	}
	return r.runPipeline(
		parent,
		attemptCtx,
		cancelAttempt,
		engine,
		shutdownExpired,
		&durableProgress,
	)
}

func (r Runner) runPipeline(
	parent context.Context,
	attemptCtx context.Context,
	cancelAttempt context.CancelCauseFunc,
	engine *agreement.Engine,
	shutdownExpired <-chan struct{},
	durableProgress *atomic.Bool,
) (attemptOutcome, error) {
	if durableProgress == nil {
		durableProgress = &atomic.Bool{}
	}
	progress := func() bool {
		return durableProgress.Load()
	}
	processCtx, cancelProcess := context.WithCancelCause(context.Background())
	defer cancelProcess(errAttemptStopped)
	defer r.Metrics.observeAgreedQueue(0, 0)

	budget, err := pipeline.NewByteBudget(r.Config.AgreedQueueBytes)
	if err != nil {
		return attemptOutcome{}, err
	}
	input := make(
		chan pipeline.Event[model.AgreedEvent, model.AgreedEvent],
		r.Config.AgreedQueueSize,
	)
	ordered := make(
		chan pipeline.Result[DecodedBlock, model.AgreedEvent],
	)
	mapperDone := make(chan error, 1)
	batcherDone := make(chan error, 1)

	releaseInput := func(size int64) error {
		if err := budget.Release(size); err != nil {
			return err
		}
		used, _ := budget.Usage()
		r.Metrics.observeAgreedQueue(len(input), used)
		return nil
	}
	mapper := pipeline.OrderedMapper[
		model.AgreedEvent,
		DecodedBlock,
		model.AgreedEvent,
	]{
		Workers:  r.Config.NormalizeWorkers,
		Window:   r.Config.ReorderSize,
		MaxBytes: r.Config.ReorderBytes,
		Decode: func(
			ctx context.Context,
			event model.AgreedEvent,
		) (DecodedBlock, error) {
			started := time.Now()
			block, err := r.Decode(ctx, event)
			r.Metrics.observeNormalize(time.Since(started))
			return block, err
		},
		ReleaseInputBytes: releaseInput,
	}
	go func() {
		err := mapper.Run(processCtx, input, ordered)
		if err != nil && context.Cause(processCtx) == nil {
			failure := &stageFailure{stage: "normalize", err: err}
			cancelProcess(failure)
			cancelAttempt(failure)
			err = failure
		}
		close(ordered)
		mapperDone <- err
	}()

	sink := StoreSink{
		Store:        r.Store,
		Lock:         r.Lock,
		MaximumDepth: r.Config.RollbackDepth,
		Metrics:      r.Metrics,
		OnProgress: func() {
			durableProgress.Store(true)
		},
	}
	batcher := Batcher{
		Sink: sink,
		Limits: BatchLimits{
			Blocks: r.Config.BatchBlocks,
			Bytes:  r.Config.BatchBytes,
			Rows:   r.Config.BatchRows,
			Age:    r.Config.BatchAge,
		},
	}
	go func() {
		err := batcher.Run(processCtx, ordered)
		if err != nil && context.Cause(processCtx) == nil {
			failure := &stageFailure{stage: "publication", err: err}
			cancelProcess(failure)
			cancelAttempt(failure)
			err = failure
		}
		batcherDone <- err
	}()

	produced := r.produceAgreed(
		attemptCtx,
		processCtx,
		engine,
		input,
		budget,
	)
	gracefulAtStop := parent.Err() != nil
	var attemptErr error
	switch {
	case gracefulAtStop:
	case context.Cause(processCtx) != nil:
		attemptErr = context.Cause(processCtx)
	case produced.retryable:
		attemptErr = produced.err
		cancelProcess(attemptErr)
	default:
		attemptErr = produced.err
		cancelProcess(attemptErr)
	}
	close(input)

	type stageResults struct {
		mapper  error
		batcher error
	}
	stagesDone := make(chan stageResults, 1)
	go func() {
		stagesDone <- stageResults{
			mapper:  <-mapperDone,
			batcher: <-batcherDone,
		}
	}()
	var stages stageResults
	select {
	case stages = <-stagesDone:
	case <-shutdownExpired:
		cancelProcess(ErrShutdownTimeout)
		return attemptOutcome{progress: progress()}, ErrShutdownTimeout
	}

	graceful := parent.Err() != nil
	if graceful {
		if cause := context.Cause(processCtx); cause != nil {
			return attemptOutcome{progress: progress()}, cause
		}
		if stages.mapper != nil {
			return attemptOutcome{progress: progress()}, stages.mapper
		}
		if stages.batcher != nil {
			return attemptOutcome{progress: progress()}, stages.batcher
		}
		return attemptOutcome{
			progress: progress(),
			graceful: true,
		}, nil
	}
	if cause := context.Cause(processCtx); cause != nil {
		var failure *stageFailure
		if errors.As(cause, &failure) {
			return attemptOutcome{progress: progress()}, cause
		}
		if errors.Is(cause, ErrShutdownTimeout) {
			return attemptOutcome{progress: progress()}, cause
		}
	}
	return attemptOutcome{
		progress:  progress(),
		retryable: produced.retryable,
	}, attemptErr
}

type producerResult struct {
	retryable bool
	err       error
}

func (r Runner) produceAgreed(
	attemptCtx context.Context,
	processCtx context.Context,
	engine *agreement.Engine,
	input chan<- pipeline.Event[model.AgreedEvent, model.AgreedEvent],
	budget *pipeline.ByteBudget,
) producerResult {
	result := func(retryable bool, err error) producerResult {
		return producerResult{
			retryable: retryable,
			err:       err,
		}
	}
	for {
		started := time.Now()
		event, err := engine.Next(attemptCtx)
		r.Metrics.observeAgreement(time.Since(started))
		if err != nil {
			var difference *agreement.Difference
			if errors.As(err, &difference) {
				r.Metrics.observeMismatch()
			}
			if cause := context.Cause(processCtx); cause != nil {
				return result(false, cause)
			}
			if cause := context.Cause(attemptCtx); cause != nil {
				err = cause
			}
			return result(true, err)
		}
		value := event
		item := pipeline.Event[model.AgreedEvent, model.AgreedEvent]{}
		var retained int64
		switch event.Kind {
		case model.EventForward:
			if event.RawLength > math.MaxInt64 {
				return result(
					false,
					fmt.Errorf(
						"agreed raw length %d exceeds process limits",
						event.RawLength,
					),
				)
			}
			retained = int64(event.RawLength)
			if retained != int64(len(event.RawCBOR)) {
				return result(
					false,
					fmt.Errorf(
						"agreed raw length %d differs from retained bytes %d",
						event.RawLength,
						len(event.RawCBOR),
					),
				)
			}
			if err := budget.Acquire(attemptCtx, retained); err != nil {
				if cause := context.Cause(processCtx); cause != nil {
					return result(false, cause)
				}
				if cause := context.Cause(attemptCtx); cause != nil {
					return result(true, cause)
				}
				return result(false, err)
			}
			item.Forward = &value
			item.Bytes = retained
		case model.EventRollback:
			item.Rollback = &value
		default:
			return result(
				false,
				fmt.Errorf("agreement emitted unknown event kind %d", event.Kind),
			)
		}

		select {
		case input <- item:
			if event.Kind == model.EventForward {
				r.Metrics.observeAgreed(event.RawLength)
			}
			used, _ := budget.Usage()
			r.Metrics.observeAgreedQueue(len(input), used)
		case <-attemptCtx.Done():
			if retained > 0 {
				_ = budget.Release(retained)
			}
			if cause := context.Cause(processCtx); cause != nil {
				return result(false, cause)
			}
			return result(true, context.Cause(attemptCtx))
		case <-processCtx.Done():
			if retained > 0 {
				_ = budget.Release(retained)
			}
			return result(false, context.Cause(processCtx))
		}
	}
}

func commonIntersection(
	candidates []model.Point,
	chosen []model.Point,
) (fallback []model.Point, same bool, err error) {
	if len(chosen) == 0 {
		return nil, false, errors.New("no relay intersections were selected")
	}
	same = true
	oldestIndex := -1
	for index, point := range chosen {
		if index > 0 && point != chosen[0] {
			same = false
		}
		candidateIndex := -1
		for candidate := range candidates {
			if candidates[candidate] == point {
				candidateIndex = candidate
				break
			}
		}
		if candidateIndex < 0 {
			return nil, false, fmt.Errorf(
				"relay %d selected an unknown intersection",
				index,
			)
		}
		if candidateIndex > oldestIndex {
			oldestIndex = candidateIndex
		}
	}
	if same {
		return nil, true, nil
	}
	return append([]model.Point(nil), candidates[oldestIndex:]...), false, nil
}

func pointsFromStore(values []store.Point) []model.Point {
	ret := make([]model.Point, len(values))
	for index, value := range values {
		ret[index] = value.Model()
	}
	return ret
}

func doubledDuration(value, maximum time.Duration) time.Duration {
	if value >= maximum || value > time.Duration(math.MaxInt64/2) {
		return maximum
	}
	value *= 2
	if value > maximum {
		return maximum
	}
	return value
}

func waitContext(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

func retryDiagnostics(err error) []any {
	var attributes []any
	var intersections *intersectionDifference
	if errors.As(err, &intersections) {
		attributes = append(
			attributes,
			"failure_kind", "intersection_selection",
			"chosen_intersections", intersections.chosen,
			"retry_candidates", intersections.fallback,
		)
	}
	var difference *agreement.Difference
	if errors.As(err, &difference) {
		attributes = append(
			attributes,
			"failure_kind", "agreement_difference",
			"difference_field", difference.Field,
			"relay_diagnostics", difference.Diagnostics,
			"total_relays", difference.TotalRelays,
			"diagnostics_truncated", difference.Truncated,
		)
	}
	var source *agreement.SourceError
	if errors.As(err, &source) {
		attributes = append(
			attributes,
			"failure_kind", "relay_source",
			"relay_index", source.RelayIndex,
		)
	}
	var relayFailure *relay.Error
	if errors.As(err, &relayFailure) {
		attributes = append(
			attributes,
			"relay_failure_kind", relayFailure.Kind,
			"relay_operation", relayFailure.Operation,
			"relay_host", relayFailure.Relay,
		)
	}
	return attributes
}

func (r Runner) logProgress(ctx context.Context, done <-chan struct{}) {
	ticker := time.NewTicker(r.ProgressInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			snapshot := r.Metrics.Snapshot()
			r.Logger.Info(
				"sync progress",
				"attempts", snapshot.Attempts,
				"reconnects", snapshot.Reconnects,
				"agreed_blocks", snapshot.AgreedBlocks,
				"agreed_bytes", snapshot.AgreedBytes,
				"published_blocks", snapshot.PublishedBlocks,
				"published_batches", snapshot.PublishedBatches,
				"published_rows", snapshot.PublishedRows,
				"rollbacks", snapshot.Rollbacks,
				"agreement_calls", snapshot.AgreementCalls,
				"agreement_mismatches", snapshot.AgreementMismatches,
				"normalized_blocks", snapshot.NormalizedBlocks,
				"agreed_queue_items", snapshot.AgreedQueueItems,
				"agreed_queue_bytes", snapshot.AgreedQueueBytes,
				"agreed_queue_high_items", snapshot.AgreedQueueHighItems,
				"agreed_queue_high_bytes", snapshot.AgreedQueueHighBytes,
				"agreement_wait_avg", snapshot.AgreementWaitAvg,
				"normalize_avg", snapshot.NormalizeAvg,
				"publish_avg", snapshot.PublishAvg,
				"agreed_blocks_per_second", snapshot.AgreedBlocksPerSec,
				"published_blocks_per_second", snapshot.PublishedPerSec,
			)
		case <-ctx.Done():
			return
		case <-done:
			return
		}
	}
}
