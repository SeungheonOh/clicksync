package syncer

import (
	"context"
	"errors"
	"fmt"
	"time"

	"cardano-clicksync/internal/model"
	"cardano-clicksync/internal/pipeline"
)

type BatchSink interface {
	Publish(context.Context, []DecodedBlock) error
	Rollback(context.Context, model.AgreedEvent) error
}

type BatchLimits struct {
	Blocks int
	Bytes  int64
	Rows   uint64
	Age    time.Duration
}

type Batcher struct {
	Sink   BatchSink
	Limits BatchLimits
	Now    func() time.Time
}

// Run turns ordered normalized events into physical publication batches.
// Context cancellation discards the pending in-memory batch. A clean input
// close flushes it, which lets graceful shutdown be distinguished from an
// attempt failure.
func (b Batcher) Run(
	ctx context.Context,
	input <-chan pipeline.Result[DecodedBlock, model.AgreedEvent],
) error {
	if b.Sink == nil || input == nil {
		return errors.New("batcher sink and input are required")
	}
	if b.Limits.Blocks < 1 || b.Limits.Bytes < 1 ||
		b.Limits.Rows < 1 || b.Limits.Age <= 0 {
		return errors.New("batcher limits must be positive")
	}
	if b.Now == nil {
		b.Now = time.Now
	}

	var (
		pending      []DecodedBlock
		pendingBytes int64
		pendingRows  uint64
		firstStaged  time.Time
		timer        *time.Timer
		timerC       <-chan time.Time
		nextSequence uint64
	)
	stopTimer := func() {
		if timer == nil {
			return
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer = nil
		timerC = nil
	}
	armTimer := func() {
		stopTimer()
		if len(pending) == 0 {
			return
		}
		remaining := b.Limits.Age - b.Now().Sub(firstStaged)
		if remaining < 0 {
			remaining = 0
		}
		timer = time.NewTimer(remaining)
		timerC = timer.C
	}
	defer stopTimer()

	flush := func() error {
		if len(pending) == 0 {
			return nil
		}
		stopTimer()
		batch := append([]DecodedBlock(nil), pending...)
		if err := b.Sink.Publish(ctx, batch); err != nil {
			return err
		}
		pending = pending[:0]
		pendingBytes = 0
		pendingRows = 0
		firstStaged = time.Time{}
		return nil
	}
	fits := func(block DecodedBlock) bool {
		rows := FactRows(block.Block)
		return len(pending) < b.Limits.Blocks &&
			block.RawLength <= uint64(b.Limits.Bytes-pendingBytes) &&
			rows <= b.Limits.Rows-pendingRows
	}
	add := func(block DecodedBlock) {
		if len(pending) == 0 {
			firstStaged = b.Now()
		}
		pending = append(pending, block)
		pendingBytes += int64(block.RawLength)
		pendingRows += FactRows(block.Block)
		if len(pending) == 1 {
			armTimer()
		}
	}
	atLimit := func() bool {
		return len(pending) >= b.Limits.Blocks ||
			pendingBytes >= b.Limits.Bytes ||
			pendingRows >= b.Limits.Rows
	}
	recount := func() {
		pendingBytes = 0
		pendingRows = 0
		for _, block := range pending {
			pendingBytes += int64(block.RawLength)
			pendingRows += FactRows(block.Block)
		}
		if len(pending) == 0 {
			firstStaged = time.Time{}
		}
		armTimer()
	}

	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-timerC:
			timer = nil
			timerC = nil
			if err := flush(); err != nil {
				return fmt.Errorf("publish age-limited batch: %w", err)
			}
		case event, ok := <-input:
			if !ok {
				if err := context.Cause(ctx); err != nil {
					return err
				}
				if err := flush(); err != nil {
					return fmt.Errorf("publish final batch: %w", err)
				}
				return nil
			}
			if event.Sequence != nextSequence {
				return fmt.Errorf(
					"ordered event sequence %d, want %d",
					event.Sequence,
					nextSequence,
				)
			}
			nextSequence++
			if (event.Forward == nil) == (event.Rollback == nil) {
				return errors.New("ordered event must contain one forward or rollback")
			}
			if event.Forward != nil {
				if !fits(*event.Forward) && len(pending) > 0 {
					if err := flush(); err != nil {
						return fmt.Errorf("publish full batch: %w", err)
					}
				}
				if !fits(*event.Forward) {
					return fmt.Errorf(
						"single block %d:%x exceeds publication limits",
						event.Forward.ChainPoint.Slot,
						event.Forward.ChainPoint.Hash,
					)
				}
				add(*event.Forward)
				if atLimit() {
					if err := flush(); err != nil {
						return fmt.Errorf("publish limit batch: %w", err)
					}
				}
				continue
			}

			rollback := *event.Rollback
			targetIndex := -1
			for index := len(pending) - 1; index >= 0; index-- {
				if sameChainPoint(pending[index].ChainPoint, rollback.Point) {
					targetIndex = index
					break
				}
			}
			if targetIndex >= 0 {
				pending = pending[:targetIndex+1]
				recount()
				continue
			}
			pending = pending[:0]
			recount()
			if err := b.Sink.Rollback(ctx, rollback); err != nil {
				return fmt.Errorf("commit rollback: %w", err)
			}
		}
	}
}

func sameChainPoint(left, right model.Point) bool {
	if left.Origin || right.Origin {
		return left.Origin == right.Origin
	}
	return left.Slot == right.Slot && left.Hash == right.Hash
}
