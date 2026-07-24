package syncer

import (
	"context"
	"sync"
	"testing"
	"time"

	"cardano-clicksync/internal/model"
	"cardano-clicksync/internal/pipeline"
)

type recordingSink struct {
	mu        sync.Mutex
	batches   [][]DecodedBlock
	rollbacks []model.AgreedEvent
}

func (s *recordingSink) Publish(_ context.Context, values []DecodedBlock) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.batches = append(s.batches, append([]DecodedBlock(nil), values...))
	return nil
}

func (s *recordingSink) Rollback(_ context.Context, value model.AgreedEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rollbacks = append(s.rollbacks, value)
	return nil
}

func TestBatcherLimitAndCleanCloseFlush(t *testing.T) {
	sink := &recordingSink{}
	input := make(chan pipeline.Result[DecodedBlock, model.AgreedEvent], 3)
	for index := range 3 {
		block := testDecodedBlock(uint64(index+1), byte(index+1))
		input <- pipeline.Result[DecodedBlock, model.AgreedEvent]{
			Sequence: uint64(index),
			Forward:  &block,
		}
	}
	close(input)
	err := (Batcher{
		Sink: sink,
		Limits: BatchLimits{
			Blocks: 2,
			Bytes:  100,
			Rows:   100,
			Age:    time.Minute,
		},
	}).Run(context.Background(), input)
	if err != nil {
		t.Fatalf("run batcher: %v", err)
	}
	if len(sink.batches) != 2 ||
		len(sink.batches[0]) != 2 ||
		len(sink.batches[1]) != 1 {
		t.Fatalf("unexpected batches: %#v", sink.batches)
	}
}

func TestBatcherRollbackTruncatesPendingBeforeCallingStore(t *testing.T) {
	sink := &recordingSink{}
	input := make(chan pipeline.Result[DecodedBlock, model.AgreedEvent], 5)
	for index := range 3 {
		block := testDecodedBlock(uint64(index+1), byte(index+1))
		input <- pipeline.Result[DecodedBlock, model.AgreedEvent]{
			Sequence: uint64(index),
			Forward:  &block,
		}
	}
	pendingTarget := model.AgreedEvent{
		Kind:  model.EventRollback,
		Point: testPoint(2, 2),
	}
	input <- pipeline.Result[DecodedBlock, model.AgreedEvent]{
		Sequence: 3,
		Rollback: &pendingTarget,
	}
	durableTarget := model.AgreedEvent{
		Kind:  model.EventRollback,
		Point: testPoint(0, 9),
	}
	input <- pipeline.Result[DecodedBlock, model.AgreedEvent]{
		Sequence: 4,
		Rollback: &durableTarget,
	}
	close(input)
	err := (Batcher{
		Sink: sink,
		Limits: BatchLimits{
			Blocks: 10,
			Bytes:  100,
			Rows:   100,
			Age:    time.Minute,
		},
	}).Run(context.Background(), input)
	if err != nil {
		t.Fatalf("run batcher: %v", err)
	}
	if len(sink.batches) != 0 {
		t.Fatalf("rollback unexpectedly published pending blocks: %#v", sink.batches)
	}
	if len(sink.rollbacks) != 1 ||
		!sameChainPoint(sink.rollbacks[0].Point, durableTarget.Point) {
		t.Fatalf("unexpected durable rollbacks: %#v", sink.rollbacks)
	}
}

func TestBatcherAgeFlushesAtLiveTip(t *testing.T) {
	sink := &recordingSink{}
	input := make(chan pipeline.Result[DecodedBlock, model.AgreedEvent], 1)
	block := testDecodedBlock(1, 1)
	input <- pipeline.Result[DecodedBlock, model.AgreedEvent]{
		Forward: &block,
	}
	done := make(chan error, 1)
	go func() {
		done <- (Batcher{
			Sink: sink,
			Limits: BatchLimits{
				Blocks: 10,
				Bytes:  100,
				Rows:   100,
				Age:    10 * time.Millisecond,
			},
		}).Run(context.Background(), input)
	}()
	time.Sleep(40 * time.Millisecond)
	close(input)
	if err := <-done; err != nil {
		t.Fatalf("run batcher: %v", err)
	}
	if len(sink.batches) != 1 || len(sink.batches[0]) != 1 {
		t.Fatalf("age timer batches: %#v", sink.batches)
	}
}

func testDecodedBlock(slot uint64, fill byte) DecodedBlock {
	return DecodedBlock{
		Block:      model.Block{Slot: slot, Hash: model.Hash32{fill}},
		ChainPoint: testPoint(slot, fill),
		RawLength:  1,
	}
}

func testPoint(slot uint64, fill byte) model.Point {
	return model.Point{Slot: slot, Hash: model.Hash32{fill}}
}
