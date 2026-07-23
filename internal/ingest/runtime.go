package ingest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	pcommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/google/uuid"

	"clicksync/internal/config"
	"clicksync/internal/genesis"
	"clicksync/internal/n2n"
	"clicksync/internal/publication"
	"clicksync/internal/store"
	"clicksync/internal/syncer"
	"clicksync/internal/writerlock"
)

const (
	rollbackMaximumDepth = uint32(2160)
	writerHeartbeatEvery = 30 * time.Second
	shutdownTimeout      = 45 * time.Second
	// A checkpoint is corroborated at the first committed batch crossing each
	// 512-block bucket. With a 256-block physical batch, the latest possible
	// crossing is 512 + 256 - 1 = 767 committed blocks after the prior bucket.
	checkpointEveryBlocks uint64 = 512
	checkpointMaximumGap  uint64 = checkpointEveryBlocks +
		uint64(publication.MaxBatchBlocks) - 1
	checkpointSafetyBound  uint64 = 1000
	checkpointSafetyMargin uint64 = checkpointSafetyBound - 1 -
		checkpointMaximumGap
)

func RunSync(
	ctx context.Context,
	cfg config.Config,
	db *store.DB,
	lock *writerlock.Lock,
	buildID string,
	logger *slog.Logger,
) (retErr error) {
	if ctx == nil || db == nil || lock == nil || logger == nil {
		return errors.New("sync context, database, writer lock, and logger are required")
	}
	if buildID == "" || buildID == "development" || buildID == "source-unset" {
		return errors.New("sync requires an explicit reproducible source build ID")
	}
	shutdownBudget, err := syncer.NewShutdownBudget(shutdownTimeout)
	if err != nil {
		return err
	}
	writerID, err := newID()
	if err != nil {
		return err
	}
	dialConfig := n2n.DialConfig{
		NetworkMagic:    cfg.NetworkMagic,
		QueueCapacity:   cfg.QueueCapacity,
		HeaderBatchSize: cfg.HeaderBatchSize,
		DialTimeout:     cfg.DialTimeout,
		BlockTimeout:    cfg.ProtocolTimeout,
	}
	peers := n2nPeers(cfg.Peers)
	identity, exists, err := db.LoadManifestIdentityIfExists(ctx)
	if err != nil {
		return err
	}
	var bootstrap *n2n.BoundaryBootstrap
	var originBundle *genesis.Bundle
	var start publication.Point
	switch cfg.Start {
	case "origin":
		if exists && !identity.Start.Origin {
			return errors.New("configured Origin conflicts with stored partial-history dataset")
		}
		start = publication.Point{Origin: true}
		bundle, err := genesis.Mainnet()
		if err != nil {
			return err
		}
		originBundle = &bundle
	case "intersection":
		configured, err := config.ParseStartPoint(cfg.StartPoint)
		if err != nil {
			return err
		}
		if exists {
			if identity.Start.Origin ||
				identity.Start.Slot != configured.Slot ||
				!bytes.Equal(identity.Start.Hash[:], configured.Hash[:]) {
				return errors.New("configured partial start point conflicts with stored dataset identity")
			}
			start = identity.Start
		} else {
			result, err := bootstrapBoundaryWithRetry(
				ctx,
				peers,
				cfg.Corroboration,
				dialConfig,
				pcommon.NewPoint(configured.Slot, append([]byte(nil), configured.Hash[:]...)),
				logger,
				n2n.BootstrapBoundary,
				waitForContext,
			)
			if err != nil {
				return err
			}
			bootstrap = &result
			start = publicationPoint(result.ChainPoint)
		}
	default:
		return fmt.Errorf("unsupported sync start mode %q", cfg.Start)
	}
	byronID, byronJSON, shelleyID, shelleyJSON := store.MainnetGenesisIdentity()
	seed := store.ManifestSeed{
		NetworkMagic:           cfg.NetworkMagic,
		NetworkName:            cfg.NetworkName,
		ByronGenesisID:         byronID,
		ByronGenesisJSONHash:   byronJSON,
		ShelleyGenesisID:       shelleyID,
		ShelleyGenesisJSONHash: shelleyJSON,
		Start:                  start,
		WriterID:               writerID,
		WriterBuild:            buildID,
		SourceBuild:            buildID,
		CreatedAt:              time.Now().UTC(),
	}
	if originBundle != nil && !exists {
		seed.OriginGenesis = &originBundle.Proof
	}
	identity, err = db.LoadOrCreateManifest(ctx, lock, seed)
	if err != nil {
		return err
	}
	audit, err := store.NewWriterAudit(identity.DatasetID, writerID, buildID, time.Now())
	if err != nil {
		return err
	}
	if err := db.BeginWriterAudit(ctx, lock, audit); err != nil {
		return err
	}
	auditActive := true
	defer func() {
		if !auditActive {
			return
		}
		reason := "graceful shutdown"
		if retErr != nil {
			reason = "terminal error: " + retErr.Error()
		}
		retErr = errors.Join(
			retErr,
			releaseWithinShutdownBudget(ctx, shutdownBudget, func(releaseCtx context.Context) error {
				return db.ReleaseWriterAudit(
					releaseCtx,
					lock,
					audit,
					time.Now(),
					reason,
				)
			}),
		)
		auditActive = false
	}()
	if bootstrap != nil {
		observations, err := BoundaryObservations(*bootstrap, cfg.NetworkMagic)
		if err != nil {
			return err
		}
		if err := db.InsertPeerObservations(ctx, observations); err != nil {
			return fmt.Errorf("persist boundary bootstrap evidence: %w", err)
		}
	}
	allocator, err := db.NewAllocator(ctx)
	if err != nil {
		return err
	}
	coordinator, err := publication.New(db, allocator, lock, publication.Config{
		WriterID:    writerID,
		WriterBuild: buildID,
		Now:         time.Now,
	})
	if err != nil {
		return err
	}
	if originBundle != nil {
		if err := genesis.EnsurePublished(
			ctx,
			db,
			coordinator,
			lock,
			*originBundle,
			writerID,
			buildID,
			time.Now(),
		); err != nil {
			return err
		}
		identity, err = db.LoadManifestIdentity(ctx)
		if err != nil {
			return err
		}
	}
	observer, err := NewObserver(db, cfg.NetworkMagic)
	if err != nil {
		return err
	}
	runCtx, cancelRun := context.WithCancelCause(ctx)
	defer cancelRun(context.Canceled)
	handler, err := NewHandler(runCtx, coordinator, db, HandlerConfig{
		NetworkMagic:          cfg.NetworkMagic,
		RollbackMaximumDepth:  rollbackMaximumDepth,
		RollbackCorroboration: cfg.Corroboration,
		FlushAfter:            defaultFlushAfter,
		FlushTimeout:          shutdownTimeout,
		ShutdownBudget:        shutdownBudget,
		Now:                   time.Now,
		Cancel:                cancelRun,
	})
	if err != nil {
		return err
	}
	transport, err := syncer.NewDirectTransport(dialConfig, logger)
	if err != nil {
		return err
	}
	supervisor, err := syncer.New(syncer.Config{
		Peers:                 peers,
		Corroboration:         cfg.Corroboration,
		AllowOrigin:           identity.Start.Origin && identity.CompleteHistory,
		InitialBackoff:        time.Second,
		MaxBackoff:            30 * time.Second,
		RollbackConfirmations: cfg.Corroboration,
		CheckpointEveryBlocks: checkpointEveryBlocks,
		FinalizeTimeout:       shutdownTimeout,
		ShutdownBudget:        shutdownBudget,
	}, db, handler, observer, transport)
	if err != nil {
		return err
	}
	heartbeatDone := make(chan error, 1)
	go func() {
		ticker := time.NewTicker(writerHeartbeatEvery)
		defer ticker.Stop()
		for {
			select {
			case <-runCtx.Done():
				heartbeatDone <- nil
				return
			case at := <-ticker.C:
				if err := db.HeartbeatWriterAudit(runCtx, lock, audit, at); err != nil {
					cancelRun(err)
					heartbeatDone <- err
					return
				}
			}
		}
	}()
	runErr := supervisor.Run(runCtx)
	cancelRun(runErr)
	heartbeatErr := <-heartbeatDone
	if heartbeatErr != nil {
		return fmt.Errorf("writer audit heartbeat: %w", heartbeatErr)
	}
	if ctx.Err() != nil && errors.Is(runErr, context.Canceled) {
		return nil
	}
	if cause := context.Cause(runCtx); cause != nil &&
		!errors.Is(cause, context.Canceled) &&
		errors.Is(runErr, context.Canceled) {
		return cause
	}
	return runErr
}

func releaseWithinShutdownBudget(
	ctx context.Context,
	budget *syncer.ShutdownBudget,
	release func(context.Context) error,
) error {
	releaseCtx, cancel := budget.Context(ctx)
	defer cancel()
	return release(releaseCtx)
}

func n2nPeers(peers []config.Peer) []n2n.Peer {
	ret := make([]n2n.Peer, len(peers))
	for index, peer := range peers {
		ret[index] = n2n.Peer{
			Host:     peer.Host,
			Operator: peer.Operator,
		}
	}
	return ret
}

type bootstrapBoundaryFunc func(
	context.Context,
	[]n2n.Peer,
	int,
	n2n.DialConfig,
	pcommon.Point,
	*slog.Logger,
) (n2n.BoundaryBootstrap, error)

type waitFunc func(context.Context, time.Duration) error

func bootstrapBoundaryWithRetry(
	ctx context.Context,
	peers []n2n.Peer,
	corroboration int,
	dialConfig n2n.DialConfig,
	point pcommon.Point,
	logger *slog.Logger,
	bootstrap bootstrapBoundaryFunc,
	wait waitFunc,
) (n2n.BoundaryBootstrap, error) {
	if bootstrap == nil || wait == nil {
		return n2n.BoundaryBootstrap{}, errors.New(
			"boundary bootstrap and backoff functions are required",
		)
	}
	backoff := time.Second
	quarantined := make(map[string]struct{}, len(peers))
	for {
		available := availableBoundaryPeers(peers, quarantined)
		if len(available) < corroboration {
			return n2n.BoundaryBootstrap{}, fmt.Errorf(
				"boundary quarantine leaves %d independent operators, need %d",
				len(available),
				corroboration,
			)
		}
		result, err := bootstrap(
			ctx,
			available,
			corroboration,
			dialConfig,
			point,
			logger,
		)
		if err == nil {
			return result, nil
		}
		if !retryableBoundaryBootstrap(
			err,
			quarantined,
			peers,
			corroboration,
		) {
			return n2n.BoundaryBootstrap{}, err
		}
		if err := wait(ctx, backoff); err != nil {
			return n2n.BoundaryBootstrap{}, err
		}
		if backoff < 30*time.Second {
			backoff *= 2
			if backoff > 30*time.Second {
				backoff = 30 * time.Second
			}
		}
	}
}

func retryableBoundaryBootstrap(
	err error,
	quarantined map[string]struct{},
	peers []n2n.Peer,
	corroboration int,
) bool {
	var closed *n2n.ProtocolChannelClosed
	if errors.As(err, &closed) {
		return true
	}
	var unavailable *n2n.BoundaryBootstrapError
	if !errors.As(err, &unavailable) || len(unavailable.Evidence) == 0 {
		return false
	}
	if unavailable.Kind == n2n.BoundaryConflict {
		return false
	}
	if unavailable.Kind != "" &&
		unavailable.Kind != n2n.BoundaryInsufficient {
		return false
	}
	remaining := availableBoundaryPeers(peers, quarantined)
	sawUnavailable := false
	sawQuarantine := false
	for _, evidence := range unavailable.Evidence {
		operator, ok := configuredBoundaryOperator(
			evidence.Peer.Operator,
			remaining,
		)
		if !ok {
			return false
		}
		switch evidence.Status {
		case n2n.BoundaryAccepted:
		case n2n.BoundaryUnavailable:
			sawUnavailable = true
		case n2n.BoundaryRejected, n2n.BoundaryPeerData:
			quarantined[operator] = struct{}{}
			sawQuarantine = true
		default:
			return false
		}
	}
	if len(availableBoundaryPeers(peers, quarantined)) < corroboration {
		return false
	}
	return sawUnavailable || sawQuarantine
}

func availableBoundaryPeers(
	peers []n2n.Peer,
	quarantined map[string]struct{},
) []n2n.Peer {
	ret := make([]n2n.Peer, 0, len(peers))
	for _, peer := range peers {
		if _, excluded := quarantined[boundaryOperatorKey(peer.Operator)]; excluded {
			continue
		}
		ret = append(ret, peer)
	}
	return ret
}

func configuredBoundaryOperator(
	operator string,
	peers []n2n.Peer,
) (string, bool) {
	key := boundaryOperatorKey(operator)
	if key == "" {
		return "", false
	}
	for _, peer := range peers {
		if boundaryOperatorKey(peer.Operator) == key {
			return key, true
		}
	}
	return "", false
}

func boundaryOperatorKey(operator string) string {
	return strings.ToLower(strings.TrimSpace(operator))
}

func waitForContext(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func newID() ([16]byte, error) {
	value, err := uuid.NewRandom()
	if err != nil {
		return [16]byte{}, fmt.Errorf("generate writer ID: %w", err)
	}
	var ret [16]byte
	copy(ret[:], value[:])
	return ret, nil
}
