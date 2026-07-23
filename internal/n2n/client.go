package n2n

import (
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
}

type Handler interface {
	Reconcile(context.Context, pcommon.Point, Peer) error
	RollForward(context.Context, lcommon.Block, chainsync.Tip, Peer) error
	RollBackward(context.Context, pcommon.Point, chainsync.Tip, Peer) error
}

type connection struct {
	peer     Peer
	conn     *ouroboros.Connection
	asyncErr <-chan error
	events   chan chainEvent
	cancel   context.CancelCauseFunc
	wg       sync.WaitGroup
}

type chainEvent struct {
	block    lcommon.Block
	rollback *pcommon.Point
	tip      chainsync.Tip
}

type DialConfig struct {
	NetworkMagic  uint32
	QueueCapacity int
	DialTimeout   time.Duration
	BlockTimeout  time.Duration
	Operator      string
}

func RunPeer(
	ctx context.Context,
	address string,
	cfg DialConfig,
	candidates []pcommon.Point,
	handler Handler,
	logger *slog.Logger,
) error {
	if handler == nil {
		return errors.New("nil ChainSync event handler")
	}
	if logger == nil {
		return errors.New("nil logger")
	}
	if cfg.NetworkMagic == 0 || cfg.QueueCapacity < 1 || cfg.QueueCapacity > 32 {
		return errors.New("invalid N2N dial configuration")
	}
	runCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(context.Canceled)
	current := &connection{
		peer: Peer{
			Host:     address,
			Address:  address,
			Operator: cfg.Operator,
		},
		events: make(chan chainEvent, cfg.QueueCapacity),
		cancel: cancel,
	}
	conn, asyncErr, err := dial(
		address,
		cfg,
		current.onRollForward,
		current.onRollBackward,
		logger,
	)
	if err != nil {
		return err
	}
	current.conn = conn
	current.asyncErr = asyncErr
	version, _ := conn.ProtocolVersion()
	current.peer.N2NVersion = version
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
		func(point pcommon.Point) error {
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
		"intersection_slot", chosen.Slot,
		"intersection_hash", safePointHash(chosen),
	)

	select {
	case <-runCtx.Done():
		return context.Cause(runCtx)
	case err := <-workerErr:
		return err
	case err, ok := <-asyncErr:
		if !ok {
			return errors.New("peer protocol error channel closed")
		}
		return fmt.Errorf("peer protocol: %w", err)
	}
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
				c.cancel(err)
				return err
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
		return fmt.Errorf("ChainSync returned unexpected header type %T", headerValue)
	}
	point := pcommon.NewPoint(header.SlotNumber(), header.Hash().Bytes())
	block, err := c.conn.BlockFetch().Client.GetBlock(point)
	if err != nil {
		return fmt.Errorf("BlockFetch %d:%s: %w", point.Slot, safePointHash(point), err)
	}
	if err := verifyHeaderBody(header, block, point); err != nil {
		return err
	}
	select {
	case c.events <- chainEvent{block: block, tip: tip}:
		return nil
	default:
		// Avoid silently expanding memory if the worker stops receiving between
		// the first select and the blocking backpressure select.
	}
	select {
	case c.events <- chainEvent{block: block, tip: tip}:
		return nil
	case <-c.conn.ChainSync().Client.DoneChan():
		return errors.New("ChainSync stopped during publication backpressure")
	}
}

func (c *connection) onRollBackward(
	_ chainsync.CallbackContext,
	point pcommon.Point,
	tip chainsync.Tip,
) error {
	select {
	case c.events <- chainEvent{rollback: &point, tip: tip}:
		return nil
	case <-c.conn.ChainSync().Client.DoneChan():
		return errors.New("ChainSync stopped during rollback backpressure")
	}
}

func dial(
	address string,
	cfg DialConfig,
	rollForward chainsync.RollForwardFunc,
	rollBackward chainsync.RollBackwardFunc,
	logger *slog.Logger,
) (*ouroboros.Connection, <-chan error, error) {
	blockCfg, err := blockfetch.NewConfig(
		blockfetch.WithBatchStartTimeout(cfg.BlockTimeout),
		blockfetch.WithBlockTimeout(cfg.BlockTimeout),
		blockfetch.WithRecvQueueSize(cfg.QueueCapacity),
	)
	if err != nil {
		return nil, nil, err
	}
	chainCfg := chainsync.NewConfig(
		chainsync.WithPipelineLimit(cfg.QueueCapacity),
		chainsync.WithRecvQueueSize(cfg.QueueCapacity),
		chainsync.WithIntersectTimeout(cfg.DialTimeout),
		chainsync.WithBlockTimeout(cfg.BlockTimeout),
		chainsync.WithRollForwardFunc(rollForward),
		chainsync.WithRollBackwardFunc(rollBackward),
	)
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
		return nil, nil, fmt.Errorf("peer negotiated unsupported N2N version %d", version)
	}
	return conn, asyncErr, nil
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
