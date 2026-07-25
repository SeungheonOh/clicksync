package relay

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/muxer"
	"github.com/blinklabs-io/gouroboros/protocol"
	"github.com/blinklabs-io/gouroboros/protocol/chainsync"
	pcommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

const (
	chainSyncRequestBatch   = 1
	chainSyncMaxOutstanding = chainsync.MaxPipelineLimit
)

var (
	chainSyncStateActive    = protocol.NewState(1, "Active")
	chainSyncStateMustReply = protocol.NewState(2, "MustReply")
)

type chainSyncIntersection struct {
	point pcommon.Point
	tip   chainsync.Tip
	err   error
}

// chainSyncClient keeps the public surface used by Session close to the
// gOuroboros client while owning request scheduling itself. gOuroboros still
// provides the mux, message types, decoder, bounded receive queue, and state
// machine.
type chainSyncClient struct {
	*protocol.Protocol

	muxer              *muxer.Muxer
	config             chainsync.Config
	callbackContext    chainsync.CallbackContext
	awaitReply         func() error
	requestNextPayload []byte

	intersectMu     sync.Mutex
	intersectWaitMu sync.Mutex
	intersectWait   chan chainSyncIntersection

	syncMu      sync.Mutex
	syncStarted bool

	outstandingMu sync.Mutex
	outstanding   int
	refillCredits int

	responseWatchMu   sync.Mutex
	responseDeadline  time.Time
	responseSuspended bool
	stopped           bool
}

func newChainSyncClient(
	options protocol.ProtocolOptions,
	config chainsync.Config,
	awaitReply func() error,
) (*chainSyncClient, error) {
	switch {
	case options.Muxer == nil:
		return nil, errors.New("ChainSync muxer is required")
	case awaitReply == nil:
		return nil, errors.New("ChainSync await-reply callback is required")
	case config.RollForwardFunc == nil:
		return nil, errors.New("ChainSync roll-forward callback is required")
	case config.RollBackwardFunc == nil:
		return nil, errors.New("ChainSync rollback callback is required")
	case config.BlockTimeout <= 0:
		return nil, errors.New("ChainSync block timeout must be positive")
	case config.PipelineLimit < 1 ||
		config.PipelineLimit > chainSyncMaxOutstanding:
		return nil, fmt.Errorf(
			"ChainSync pipeline must be in 1..%d",
			chainSyncMaxOutstanding,
		)
	}
	if config.RecvQueueSize < 1 {
		config.RecvQueueSize = config.PipelineLimit
	}
	requestNextPayload, err := cbor.Encode(chainsync.NewMsgRequestNext())
	if err != nil {
		return nil, fmt.Errorf("encode ChainSync RequestNext: %w", err)
	}
	client := &chainSyncClient{
		muxer:              options.Muxer,
		config:             config,
		callbackContext:    chainsync.CallbackContext{ConnectionId: options.ConnectionId},
		awaitReply:         awaitReply,
		requestNextPayload: requestNextPayload,
	}
	client.Protocol = protocol.New(protocol.ProtocolConfig{
		Name:                chainsync.ProtocolName,
		ProtocolId:          chainsync.ProtocolIdNtN,
		Muxer:               options.Muxer,
		Logger:              options.Logger,
		ErrorChan:           options.ErrorChan,
		Mode:                protocol.ProtocolModeNodeToNode,
		Role:                protocol.ProtocolRoleClient,
		MessageHandlerFunc:  client.handleMessage,
		MessageFromCborFunc: chainsync.NewMsgFromCborNtN,
		StateMap:            chainSyncStateMap(),
		InitialState:        chainSyncStateActive,
		RecvQueueSize:       config.RecvQueueSize,
	})
	return client, nil
}

func chainSyncStateMap() protocol.StateMap {
	// Response timing is owned below so local callback backpressure is never
	// mistaken for a stalled relay.
	limit := chainsync.MaxPendingMessageBytes
	return protocol.StateMap{
		chainSyncStateActive: {
			Agency:                  protocol.AgencyServer,
			PendingMessageByteLimit: limit,
			Transitions: []protocol.StateTransition{
				{
					MsgType:  chainsync.MessageTypeIntersectFound,
					NewState: chainSyncStateActive,
				},
				{
					MsgType:  chainsync.MessageTypeIntersectNotFound,
					NewState: chainSyncStateActive,
				},
				{
					MsgType:  chainsync.MessageTypeRollForward,
					NewState: chainSyncStateActive,
				},
				{
					MsgType:  chainsync.MessageTypeRollBackward,
					NewState: chainSyncStateActive,
				},
				{
					MsgType:  chainsync.MessageTypeAwaitReply,
					NewState: chainSyncStateMustReply,
				},
			},
		},
		chainSyncStateMustReply: {
			Agency:                  protocol.AgencyServer,
			PendingMessageByteLimit: limit,
			TimeoutFunc:             chainsync.MustReplyTimeoutFunc,
			Transitions: []protocol.StateTransition{
				{
					MsgType:  chainsync.MessageTypeRollForward,
					NewState: chainSyncStateActive,
				},
				{
					MsgType:  chainsync.MessageTypeRollBackward,
					NewState: chainSyncStateActive,
				},
			},
		},
	}
}

func (c *chainSyncClient) GetAvailableBlockRange(
	intersectPoints []pcommon.Point,
) (pcommon.Point, pcommon.Point, error) {
	result, err := c.findIntersection(intersectPoints)
	if err != nil {
		return pcommon.Point{}, pcommon.Point{}, err
	}
	if result.point.Slot >= result.tip.Point.Slot {
		return pcommon.Point{}, pcommon.Point{}, nil
	}
	return result.point, result.tip.Point, nil
}

func (c *chainSyncClient) Sync(intersectPoints []pcommon.Point) error {
	c.syncMu.Lock()
	if c.syncStarted {
		c.syncMu.Unlock()
		return errors.New("ChainSync is already running")
	}
	c.syncStarted = true
	c.syncMu.Unlock()

	if _, err := c.findIntersection(intersectPoints); err != nil {
		return err
	}
	for remaining := c.config.PipelineLimit; remaining > 0; {
		count := min(remaining, chainSyncRequestBatch)
		if err := c.reserve(count); err != nil {
			return err
		}
		if err := c.sendRequestNext(count); err != nil {
			return err
		}
		remaining -= count
	}
	c.armResponseWatch()
	go c.watchResponses()
	return nil
}

func (c *chainSyncClient) Stop() {
	c.responseWatchMu.Lock()
	c.stopped = true
	c.responseSuspended = true
	c.responseDeadline = time.Time{}
	c.responseWatchMu.Unlock()
	c.Protocol.Stop()
}

func (c *chainSyncClient) findIntersection(
	intersectPoints []pcommon.Point,
) (chainSyncIntersection, error) {
	c.intersectMu.Lock()
	defer c.intersectMu.Unlock()
	if len(intersectPoints) == 0 {
		intersectPoints = []pcommon.Point{pcommon.NewPointOrigin()}
	}
	wait := make(chan chainSyncIntersection, 1)
	c.intersectWaitMu.Lock()
	c.intersectWait = wait
	c.intersectWaitMu.Unlock()
	defer func() {
		c.intersectWaitMu.Lock()
		if c.intersectWait == wait {
			c.intersectWait = nil
		}
		c.intersectWaitMu.Unlock()
	}()

	if err := c.sendMessage(chainsync.NewMsgFindIntersect(intersectPoints)); err != nil {
		return chainSyncIntersection{}, err
	}
	timeout := c.config.IntersectTimeout
	if timeout <= 0 {
		timeout = chainsync.IntersectTimeout
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case result := <-wait:
		return result, result.err
	case <-timer.C:
		return chainSyncIntersection{}, errors.New("ChainSync intersection timed out")
	case <-c.DoneChan():
		return chainSyncIntersection{}, protocol.ErrProtocolShuttingDown
	}
}

func (c *chainSyncClient) handleMessage(message protocol.Message) error {
	switch msg := message.(type) {
	case *chainsync.MsgIntersectFound:
		return c.deliverIntersection(chainSyncIntersection{
			point: msg.Point,
			tip:   msg.Tip,
		})
	case *chainsync.MsgIntersectNotFound:
		return c.deliverIntersection(chainSyncIntersection{
			tip: msg.Tip,
			err: chainsync.ErrIntersectNotFound,
		})
	case *chainsync.MsgAwaitReply:
		c.suspendResponseWatch()
		return c.awaitReply()
	case *chainsync.MsgRollForwardNtN:
		c.suspendResponseWatch()
		if err := c.completeRequest(); err != nil {
			return err
		}
		blockType, err := blockTypeFromHeader(msg.WrappedHeader)
		if err != nil {
			return err
		}
		header, err := ledger.NewBlockHeaderFromCbor(
			blockType,
			msg.WrappedHeader.HeaderCbor(),
		)
		if err != nil {
			return fmt.Errorf("decode ChainSync header: %w", err)
		}
		if err := c.config.RollForwardFunc(
			c.callbackContext,
			blockType,
			header,
			msg.Tip,
		); err != nil {
			return err
		}
		if err := c.completeCallback(); err != nil {
			return err
		}
		c.resumeResponseWatch()
		return nil
	case *chainsync.MsgRollBackward:
		c.suspendResponseWatch()
		if err := c.completeRequest(); err != nil {
			return err
		}
		if err := c.config.RollBackwardFunc(
			c.callbackContext,
			msg.Point,
			msg.Tip,
		); err != nil {
			return err
		}
		if err := c.completeCallback(); err != nil {
			return err
		}
		c.resumeResponseWatch()
		return nil
	default:
		return fmt.Errorf("unexpected ChainSync message %T", message)
	}
}

func blockTypeFromHeader(header chainsync.WrappedHeader) (uint, error) {
	if header.Era == ledger.BlockHeaderTypeByron {
		return header.ByronType(), nil
	}
	blockType, ok := ledger.BlockHeaderToBlockTypeMap[header.Era]
	if !ok {
		return 0, fmt.Errorf("unknown ChainSync header era %d", header.Era)
	}
	return blockType, nil
}

func (c *chainSyncClient) deliverIntersection(
	result chainSyncIntersection,
) error {
	c.intersectWaitMu.Lock()
	wait := c.intersectWait
	c.intersectWaitMu.Unlock()
	if wait == nil {
		return errors.New("unexpected ChainSync intersection response")
	}
	select {
	case wait <- result:
		return nil
	case <-c.DoneChan():
		return protocol.ErrProtocolShuttingDown
	}
}

func (c *chainSyncClient) completeRequest() error {
	c.outstandingMu.Lock()
	defer c.outstandingMu.Unlock()
	if c.outstanding == 0 {
		return errors.New("ChainSync response without an outstanding request")
	}
	c.outstanding--
	return nil
}

func (c *chainSyncClient) completeCallback() error {
	c.refillCredits++
	batchSize := min(
		chainSyncRequestBatch,
		max(1, c.config.PipelineLimit/2),
	)
	if c.refillCredits < batchSize {
		return nil
	}
	if err := c.reserve(batchSize); err != nil {
		return err
	}
	if err := c.sendRequestNext(batchSize); err != nil {
		return err
	}
	c.refillCredits = 0
	return nil
}

func (c *chainSyncClient) suspendResponseWatch() {
	c.responseWatchMu.Lock()
	defer c.responseWatchMu.Unlock()
	if c.stopped {
		return
	}
	c.responseSuspended = true
	c.responseDeadline = time.Time{}
}

func (c *chainSyncClient) resumeResponseWatch() {
	c.responseWatchMu.Lock()
	defer c.responseWatchMu.Unlock()
	if c.stopped {
		return
	}
	c.responseSuspended = false
	c.responseDeadline = time.Now().Add(c.config.BlockTimeout)
}

func (c *chainSyncClient) armResponseWatch() {
	c.responseWatchMu.Lock()
	defer c.responseWatchMu.Unlock()
	if c.stopped || c.responseSuspended {
		return
	}
	c.responseDeadline = time.Now().Add(c.config.BlockTimeout)
}

func (c *chainSyncClient) watchResponses() {
	interval := min(
		time.Second,
		max(10*time.Millisecond, c.config.BlockTimeout/10),
	)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case now := <-ticker.C:
			c.responseWatchMu.Lock()
			expired := !c.stopped &&
				!c.responseSuspended &&
				!c.responseDeadline.IsZero() &&
				!now.Before(c.responseDeadline)
			if expired {
				c.stopped = true
				c.responseDeadline = time.Time{}
			}
			c.responseWatchMu.Unlock()
			if expired {
				c.SendError(errors.New(
					"ChainSync timed out waiting for a response",
				))
				return
			}
		case <-c.DoneChan():
			return
		}
	}
}

func (c *chainSyncClient) reserve(count int) error {
	c.outstandingMu.Lock()
	defer c.outstandingMu.Unlock()
	if count < 1 ||
		c.outstanding+count > c.config.PipelineLimit ||
		c.outstanding+count > chainSyncMaxOutstanding {
		return fmt.Errorf(
			"ChainSync outstanding request limit exceeded: %d + %d",
			c.outstanding,
			count,
		)
	}
	c.outstanding += count
	return nil
}

func (c *chainSyncClient) sendRequestNext(count int) error {
	payload := make([]byte, 0, count*len(c.requestNextPayload))
	for range count {
		payload = append(payload, c.requestNextPayload...)
	}
	return c.sendPayload(payload)
}

func (c *chainSyncClient) sendMessage(message protocol.Message) error {
	payload := message.Cbor()
	if payload == nil {
		var err error
		payload, err = cbor.Encode(message)
		if err != nil {
			return err
		}
	}
	return c.sendPayload(payload)
}

func (c *chainSyncClient) sendPayload(payload []byte) error {
	segment := muxer.NewSegment(
		chainsync.ProtocolIdNtN,
		payload,
		false,
	)
	if segment == nil {
		return errors.New("create ChainSync mux segment")
	}
	if err := c.muxer.Send(segment); err != nil {
		return fmt.Errorf("send ChainSync mux segment: %w", err)
	}
	return nil
}
