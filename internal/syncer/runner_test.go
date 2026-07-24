package syncer

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cardano-clicksync/internal/agreement"
	"cardano-clicksync/internal/config"
	"cardano-clicksync/internal/model"
	"cardano-clicksync/internal/relay"
	"cardano-clicksync/internal/store"
)

type runnerStore struct {
	mu           sync.Mutex
	state        store.State
	stateStarted chan struct{}
	stateRelease chan struct{}
	published    [][]store.Candidate
	rollbacks    []store.RollbackRequest
	committed    chan struct{}
	rollbackHook func()
}

func (s *runnerStore) State(
	context.Context,
	uint32,
) (store.State, error) {
	if s.stateStarted != nil {
		select {
		case s.stateStarted <- struct{}{}:
		default:
		}
	}
	if s.stateRelease != nil {
		<-s.stateRelease
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	state := s.state
	state.Canonical = append([]store.CanonicalBlock(nil), state.Canonical...)
	state.Intersections = append([]store.Point(nil), state.Intersections...)
	return state, nil
}

func (s *runnerStore) Publish(
	_ context.Context,
	_ store.Lock,
	values []store.Candidate,
) (store.Commit, error) {
	s.mu.Lock()
	s.published = append(
		s.published,
		append([]store.Candidate(nil), values...),
	)
	s.mu.Unlock()
	if s.committed != nil {
		select {
		case s.committed <- struct{}{}:
		default:
		}
	}
	return store.Commit{Committed: true}, nil
}

func (s *runnerStore) Rollback(
	_ context.Context,
	_ store.Lock,
	request store.RollbackRequest,
) (store.RollbackCommit, error) {
	s.mu.Lock()
	s.rollbacks = append(s.rollbacks, request)
	hook := s.rollbackHook
	s.mu.Unlock()
	if hook != nil {
		hook()
	}
	return store.RollbackCommit{Committed: true}, nil
}

type fakeRelaySession struct {
	intersection model.Point
	identity     model.RelayIdentity
	events       chan relay.Event
	ready        chan struct{}
	readyOnce    sync.Once
	mu           sync.Mutex
	cause        error
	candidates   []model.Point
}

func newFakeRelaySession(
	intersection model.Point,
	events ...relay.Event,
) *fakeRelaySession {
	eventChannel := make(chan relay.Event, len(events))
	for _, event := range events {
		eventChannel <- event
	}
	return &fakeRelaySession{
		intersection: intersection,
		events:       eventChannel,
		ready:        make(chan struct{}),
	}
}

func (s *fakeRelaySession) Run(
	ctx context.Context,
	candidates []model.Point,
) error {
	s.mu.Lock()
	s.candidates = append([]model.Point(nil), candidates...)
	s.mu.Unlock()
	s.readyOnce.Do(func() {
		close(s.ready)
	})
	<-ctx.Done()
	err := context.Cause(ctx)
	s.mu.Lock()
	s.cause = err
	s.mu.Unlock()
	return err
}

func (s *fakeRelaySession) Next(ctx context.Context) (relay.Event, error) {
	select {
	case event := <-s.events:
		return event, nil
	case <-ctx.Done():
		return relay.Event{}, context.Cause(ctx)
	}
}

func (s *fakeRelaySession) Ready() <-chan struct{} {
	return s.ready
}

func (s *fakeRelaySession) Intersection() (model.Point, bool) {
	return s.intersection, true
}

func (s *fakeRelaySession) Identity() model.RelayIdentity {
	return s.identity
}

func (s *fakeRelaySession) Cause() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cause
}

func TestRunnerGracefulShutdownFlushesAgreedPrefix(t *testing.T) {
	cfg := validRunnerConfig()
	point := model.Point{
		Slot:        10,
		Hash:        model.Hash32{1},
		BlockNumber: 10,
	}
	raw := []byte{0x82, 0x01, 0x02}
	digest := relay.RawBlockDigest(7, raw)
	relayEvent := func(index int) relay.Event {
		event := relay.Event{
			Kind:      relay.Forward,
			Point:     point,
			BlockType: 7,
			RawLength: uint64(len(raw)),
			Digest:    digest,
			Relay: model.RelayIdentity{
				Host:       cfg.Relays[index].Host,
				Address:    cfg.Relays[index].Host,
				Operator:   cfg.Relays[index].Operator,
				N2NVersion: 15,
			},
		}
		if index == 0 {
			event.RawCBOR = append([]byte(nil), raw...)
		}
		return event
	}
	sessions := []*fakeRelaySession{
		newFakeRelaySession(point, relayEvent(0)),
		newFakeRelaySession(point, relayEvent(1)),
	}
	backend := &runnerStore{
		state: store.State{
			Intersections: []store.Point{store.PointFromModel(point)},
		},
		committed: make(chan struct{}, 1),
	}
	decoded := make(chan struct{}, 1)
	runner := Runner{
		Config:  cfg,
		Store:   backend,
		Lock:    heldLock{},
		Logger:  discardLogger(),
		Metrics: &Metrics{},
		NewSession: func(
			cfg relay.Config,
			_ *slog.Logger,
		) (RelaySession, error) {
			return sessions[cfg.RelayIndex], nil
		},
		Decode: func(
			_ context.Context,
			event model.AgreedEvent,
		) (DecodedBlock, error) {
			decoded <- struct{}{}
			return DecodedBlock{
				Block: model.Block{
					Hash:   event.Point.Hash,
					Slot:   event.Point.Slot,
					Number: event.Point.BlockNumber,
				},
				ChainPoint:  event.Point,
				ContentHash: event.ContentHash,
				RawLength:   event.RawLength,
				Relays:      event.Relays,
			}, nil
		},
		ProgressInterval: time.Hour,
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- runner.Run(ctx)
	}()
	select {
	case <-decoded:
		cancel()
	case <-time.After(time.Second):
		t.Fatal("agreed block did not reach the decoder")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("runner shutdown: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("runner did not complete graceful shutdown")
	}
	backend.mu.Lock()
	defer backend.mu.Unlock()
	if len(backend.published) != 1 ||
		len(backend.published[0]) != 1 {
		t.Fatalf("final batches = %#v, want one block", backend.published)
	}
}

func TestRunnerShutdownDeadlineBoundsNonCancelableDecode(t *testing.T) {
	cfg := validRunnerConfig()
	cfg.ShutdownTimeout = 30 * time.Millisecond
	point := model.Point{
		Slot:        10,
		Hash:        model.Hash32{1},
		BlockNumber: 10,
	}
	raw := []byte{1}
	digest := relay.RawBlockDigest(7, raw)
	event := func(index int) relay.Event {
		value := relay.Event{
			Kind:      relay.Forward,
			Point:     point,
			BlockType: 7,
			RawLength: 1,
			Digest:    digest,
			Relay: model.RelayIdentity{
				Host:     cfg.Relays[index].Host,
				Address:  cfg.Relays[index].Host,
				Operator: cfg.Relays[index].Operator,
			},
		}
		if index == 0 {
			value.RawCBOR = raw
		}
		return value
	}
	sessions := []*fakeRelaySession{
		newFakeRelaySession(point, event(0)),
		newFakeRelaySession(point, event(1)),
	}
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	runner := Runner{
		Config: cfg,
		Store: &runnerStore{state: store.State{
			Intersections: []store.Point{store.PointFromModel(point)},
			Tip:           store.PointFromModel(point),
		}},
		Lock:    heldLock{},
		Logger:  discardLogger(),
		Metrics: &Metrics{},
		NewSession: func(
			cfg relay.Config,
			_ *slog.Logger,
		) (RelaySession, error) {
			return sessions[cfg.RelayIndex], nil
		},
		Decode: func(
			context.Context,
			model.AgreedEvent,
		) (DecodedBlock, error) {
			started <- struct{}{}
			<-release
			return DecodedBlock{}, nil
		},
		ProgressInterval: time.Hour,
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- runner.Run(ctx)
	}()
	select {
	case <-started:
		cancel()
	case <-time.After(time.Second):
		close(release)
		t.Fatal("decoder did not start")
	}
	select {
	case err := <-done:
		if !errors.Is(err, ErrShutdownTimeout) {
			close(release)
			t.Fatalf("shutdown error = %v, want deadline", err)
		}
	case <-time.After(500 * time.Millisecond):
		close(release)
		t.Fatal("shutdown exceeded its configured deadline")
	}
	close(release)
}

func TestRunnerShutdownDeadlineBoundsStateRecovery(t *testing.T) {
	cfg := validRunnerConfig()
	cfg.ShutdownTimeout = 30 * time.Millisecond
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	runner := Runner{
		Config: cfg,
		Store: &runnerStore{
			stateStarted: started,
			stateRelease: release,
		},
		Lock:             heldLock{},
		Logger:           discardLogger(),
		Metrics:          &Metrics{},
		ProgressInterval: time.Hour,
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- runner.Run(ctx)
	}()
	select {
	case <-started:
		cancel()
	case <-time.After(time.Second):
		close(release)
		t.Fatal("state recovery did not start")
	}
	select {
	case err := <-done:
		if !errors.Is(err, ErrShutdownTimeout) {
			close(release)
			t.Fatalf("shutdown error = %v, want deadline", err)
		}
	case <-time.After(500 * time.Millisecond):
		close(release)
		t.Fatal("shutdown exceeded its configured deadline")
	}
	close(release)
}

func TestRunAttemptRetriesWhenRelaysSelectDifferentIntersections(t *testing.T) {
	cfg := validRunnerConfig()
	newer := model.Point{
		Slot:        20,
		Hash:        model.Hash32{2},
		BlockNumber: 20,
	}
	older := model.Point{
		Slot:        10,
		Hash:        model.Hash32{1},
		BlockNumber: 10,
	}
	sessions := []*fakeRelaySession{
		newFakeRelaySession(newer),
		newFakeRelaySession(older),
	}
	runner := Runner{
		Config: cfg,
		Store:  &runnerStore{},
		Lock:   heldLock{},
		Logger: discardLogger(),
		NewSession: func(
			cfg relay.Config,
			_ *slog.Logger,
		) (RelaySession, error) {
			return sessions[cfg.RelayIndex], nil
		},
	}
	outcome, err := runner.runAttempt(
		context.Background(),
		newer,
		[]model.Point{newer, older},
	)
	var difference *intersectionDifference
	if !errors.As(err, &difference) {
		t.Fatalf("error = %v, want intersection difference", err)
	}
	if !outcome.retryable ||
		len(outcome.fallback) != 1 ||
		outcome.fallback[0] != older {
		t.Fatalf("unexpected outcome: %#v", outcome)
	}
}

func TestRunAttemptRollsBackBeforeStreamingFromOlderIntersection(t *testing.T) {
	cfg := validRunnerConfig()
	newer := model.Point{
		Slot:        20,
		Hash:        model.Hash32{2},
		BlockNumber: 20,
	}
	older := model.Point{
		Slot:        10,
		Hash:        model.Hash32{1},
		BlockNumber: 10,
	}
	sessions := []*fakeRelaySession{
		newFakeRelaySession(older),
		newFakeRelaySession(older),
	}
	for index, session := range sessions {
		session.identity = model.RelayIdentity{
			Host:       cfg.Relays[index].Host,
			Address:    cfg.Relays[index].Host,
			Operator:   cfg.Relays[index].Operator,
			N2NVersion: 15,
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	backend := &runnerStore{rollbackHook: cancel}
	runner := Runner{
		Config:  cfg,
		Store:   backend,
		Lock:    heldLock{},
		Logger:  discardLogger(),
		Metrics: &Metrics{},
		NewSession: func(
			cfg relay.Config,
			_ *slog.Logger,
		) (RelaySession, error) {
			return sessions[cfg.RelayIndex], nil
		},
		Decode: func(
			context.Context,
			model.AgreedEvent,
		) (DecodedBlock, error) {
			return DecodedBlock{}, errors.New("unexpected forward decode")
		},
	}
	outcome, err := runner.runAttempt(
		ctx,
		newer,
		[]model.Point{newer, older},
	)
	if err != nil || !outcome.graceful {
		t.Fatalf("attempt outcome = %#v, error %v", outcome, err)
	}
	backend.mu.Lock()
	defer backend.mu.Unlock()
	if len(backend.rollbacks) != 1 ||
		backend.rollbacks[0].To != store.PointFromModel(older) ||
		backend.rollbacks[0].Reason !=
			"unanimous relay intersection rollback" {
		t.Fatalf("intersection rollbacks = %#v", backend.rollbacks)
	}
}

func TestRunnerConvergesMixedIntersectionsBeforePublishingNewSuffix(
	t *testing.T,
) {
	cfg := validRunnerConfig()
	cfg.BatchBlocks = 1
	newer := model.Point{
		Slot:        20,
		Hash:        model.Hash32{2},
		BlockNumber: 20,
	}
	older := model.Point{
		Slot:        10,
		Hash:        model.Hash32{1},
		BlockNumber: 10,
	}
	replacement := model.Point{
		Slot:        21,
		Hash:        model.Hash32{3},
		BlockNumber: 11,
	}
	raw := []byte{0x82, 0x01}
	digest := relay.RawBlockDigest(7, raw)
	event := func(index int) relay.Event {
		value := relay.Event{
			Kind:      relay.Forward,
			Point:     replacement,
			BlockType: 7,
			RawLength: uint64(len(raw)),
			Digest:    digest,
			Relay: model.RelayIdentity{
				Host:       cfg.Relays[index].Host,
				Address:    cfg.Relays[index].Host,
				Operator:   cfg.Relays[index].Operator,
				N2NVersion: 15,
			},
		}
		if index == 0 {
			value.RawCBOR = raw
		}
		return value
	}
	attempts := [][]*fakeRelaySession{
		{
			newFakeRelaySession(newer),
			newFakeRelaySession(older),
		},
		{
			newFakeRelaySession(older, event(0)),
			newFakeRelaySession(older, event(1)),
		},
	}
	for attempt := range attempts {
		for index, session := range attempts[attempt] {
			session.identity = model.RelayIdentity{
				Host:       cfg.Relays[index].Host,
				Address:    cfg.Relays[index].Host,
				Operator:   cfg.Relays[index].Operator,
				N2NVersion: 15,
			}
		}
	}
	backend := &runnerStore{
		state: store.State{
			Snapshot: 100,
			Tip:      store.PointFromModel(newer),
			Intersections: []store.Point{
				store.PointFromModel(newer),
				store.PointFromModel(older),
			},
		},
		committed: make(chan struct{}, 1),
	}
	backend.rollbackHook = func() {
		backend.mu.Lock()
		backend.state.Snapshot++
		backend.state.Tip = store.PointFromModel(older)
		backend.state.Intersections = []store.Point{
			store.PointFromModel(older),
		}
		backend.mu.Unlock()
	}
	var factoryMu sync.Mutex
	factoryCalls := 0
	runner := Runner{
		Config:  cfg,
		Store:   backend,
		Lock:    heldLock{},
		Logger:  discardLogger(),
		Metrics: &Metrics{},
		NewSession: func(
			relayConfig relay.Config,
			_ *slog.Logger,
		) (RelaySession, error) {
			factoryMu.Lock()
			defer factoryMu.Unlock()
			attempt := factoryCalls / len(cfg.Relays)
			factoryCalls++
			if attempt >= len(attempts) {
				return nil, errors.New("unexpected extra sync attempt")
			}
			return attempts[attempt][relayConfig.RelayIndex], nil
		},
		Decode: func(
			_ context.Context,
			agreed model.AgreedEvent,
		) (DecodedBlock, error) {
			return DecodedBlock{
				Block: model.Block{
					Hash:   agreed.Point.Hash,
					Slot:   agreed.Point.Slot,
					Number: agreed.Point.BlockNumber,
				},
				ChainPoint:  agreed.Point,
				ContentHash: agreed.ContentHash,
				RawLength:   agreed.RawLength,
				Relays:      agreed.Relays,
			}, nil
		},
		ProgressInterval: time.Hour,
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- runner.Run(ctx)
	}()
	select {
	case <-backend.committed:
		cancel()
	case <-time.After(time.Second):
		cancel()
		t.Fatal("replacement suffix was not published")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("runner: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("runner did not stop")
	}

	backend.mu.Lock()
	if len(backend.rollbacks) != 1 ||
		backend.rollbacks[0].To != store.PointFromModel(older) {
		t.Fatalf("rollbacks = %#v", backend.rollbacks)
	}
	if len(backend.published) != 1 ||
		len(backend.published[0]) != 1 ||
		backend.published[0][0].Block.Hash != replacement.Hash {
		t.Fatalf("published suffix = %#v", backend.published)
	}
	backend.mu.Unlock()
	for index, session := range attempts[1] {
		session.mu.Lock()
		candidates := append([]model.Point(nil), session.candidates...)
		session.mu.Unlock()
		if len(candidates) != 1 || candidates[0] != older {
			t.Fatalf(
				"retry relay %d candidates = %#v, want only older point",
				index,
				candidates,
			)
		}
	}
}

func TestRunPipelineTreatsDecodeFailureAsTerminal(t *testing.T) {
	cfg := validRunnerConfig()
	point := model.Point{
		Slot:        10,
		Hash:        model.Hash32{1},
		BlockNumber: 10,
	}
	raw := []byte{1}
	digest := relay.RawBlockDigest(7, raw)
	source := func(index int) *fakeRelaySession {
		event := relay.Event{
			Kind:      relay.Forward,
			Point:     point,
			BlockType: 7,
			RawLength: 1,
			Digest:    digest,
			Relay: model.RelayIdentity{
				Host:     cfg.Relays[index].Host,
				Address:  cfg.Relays[index].Host,
				Operator: cfg.Relays[index].Operator,
			},
		}
		if index == 0 {
			event.RawCBOR = raw
		}
		return newFakeRelaySession(point, event)
	}
	first, second := source(0), source(1)
	engine, err := agreement.New(first, second)
	if err != nil {
		t.Fatalf("create agreement engine: %v", err)
	}
	sentinel := errors.New("unsupported block")
	runner := Runner{
		Config:  cfg,
		Store:   &runnerStore{},
		Lock:    heldLock{},
		Logger:  discardLogger(),
		Metrics: &Metrics{},
		Decode: func(
			context.Context,
			model.AgreedEvent,
		) (DecodedBlock, error) {
			return DecodedBlock{}, sentinel
		},
	}
	attemptCtx, cancelAttempt := context.WithCancelCause(context.Background())
	defer cancelAttempt(errAttemptStopped)
	outcome, err := runner.runPipeline(
		context.Background(),
		attemptCtx,
		cancelAttempt,
		engine,
		make(chan struct{}),
		&atomic.Bool{},
	)
	if outcome.retryable {
		t.Fatalf("decode failure was marked retryable: %#v", outcome)
	}
	if !errors.Is(err, sentinel) {
		t.Fatalf("error = %v, want %v", err, sentinel)
	}
}

func TestRunPipelineDoesNotCountUncommittedAgreementAsProgress(t *testing.T) {
	cfg := validRunnerConfig()
	point := model.Point{
		Slot:        10,
		Hash:        model.Hash32{1},
		BlockNumber: 10,
	}
	raw := []byte{1}
	digest := relay.RawBlockDigest(7, raw)
	source := func(index int) *fakeRelaySession {
		event := relay.Event{
			Kind:      relay.Forward,
			Point:     point,
			BlockType: 7,
			RawLength: 1,
			Digest:    digest,
			Relay: model.RelayIdentity{
				Host:     cfg.Relays[index].Host,
				Address:  cfg.Relays[index].Host,
				Operator: cfg.Relays[index].Operator,
			},
		}
		if index == 0 {
			event.RawCBOR = raw
		}
		return newFakeRelaySession(point, event)
	}
	first, second := source(0), source(1)
	engine, err := agreement.New(first, second)
	if err != nil {
		t.Fatalf("create agreement engine: %v", err)
	}
	decoded := make(chan struct{}, 1)
	runner := Runner{
		Config:  cfg,
		Store:   &runnerStore{},
		Lock:    heldLock{},
		Logger:  discardLogger(),
		Metrics: &Metrics{},
		Decode: func(
			context.Context,
			model.AgreedEvent,
		) (DecodedBlock, error) {
			decoded <- struct{}{}
			return DecodedBlock{
				Block: model.Block{
					Hash:   point.Hash,
					Slot:   point.Slot,
					Number: point.BlockNumber,
				},
				ChainPoint: point,
				RawLength:  1,
			}, nil
		},
	}
	attemptCtx, cancelAttempt := context.WithCancelCause(context.Background())
	done := make(chan struct {
		outcome attemptOutcome
		err     error
	}, 1)
	go func() {
		outcome, err := runner.runPipeline(
			context.Background(),
			attemptCtx,
			cancelAttempt,
			engine,
			make(chan struct{}),
			&atomic.Bool{},
		)
		done <- struct {
			outcome attemptOutcome
			err     error
		}{outcome: outcome, err: err}
	}()
	select {
	case <-decoded:
		cancelAttempt(errors.New("relay disconnected"))
	case <-time.After(time.Second):
		t.Fatal("agreed event did not reach decoder")
	}
	select {
	case result := <-done:
		if !result.outcome.retryable || result.outcome.progress {
			t.Fatalf("unexpected outcome after uncommitted agreement: %#v", result.outcome)
		}
		if result.err == nil {
			t.Fatal("missing relay failure")
		}
	case <-time.After(time.Second):
		t.Fatal("pipeline did not stop after relay failure")
	}
}

func validRunnerConfig() config.Sync {
	startHash := [32]byte{9}
	return config.Sync{
		Database: config.Database{
			Host:     "127.0.0.1",
			Port:     9000,
			User:     "clicksync",
			Password: "secret",
			Name:     "clicksync",
			OpenConn: 16,
		},
		NetworkName:  "test",
		NetworkMagic: 42,
		Relays: []config.Relay{
			{Host: "relay-a.example:3001", Operator: "a"},
			{Host: "relay-b.example:3001", Operator: "b"},
		},
		Start: config.Point{
			Slot:        1,
			Hash:        startHash,
			BlockNumber: 1,
		},
		LockPath:          "test.lock",
		DialTimeout:       time.Second,
		ProtocolTimeout:   time.Second,
		ShutdownTimeout:   time.Second,
		ReconnectInitial:  time.Millisecond,
		ReconnectMaximum:  10 * time.Millisecond,
		HeaderBatchSize:   2,
		ProtocolQueueSize: 2,
		RelayQueueSize:    2,
		AgreedQueueSize:   2,
		AgreedQueueBytes:  1 << 20,
		NormalizeWorkers:  1,
		ReorderSize:       2,
		ReorderBytes:      1 << 20,
		BatchBlocks:       10,
		BatchBytes:        1 << 20,
		BatchRows:         100,
		BatchAge:          30 * time.Second,
		RollbackDepth:     10,
	}
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
