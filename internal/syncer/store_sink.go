package syncer

import (
	"context"
	"errors"
	"time"

	"cardano-clicksync/internal/model"
	"cardano-clicksync/internal/store"
)

type PublicationStore interface {
	Publish(context.Context, store.Lock, []store.Candidate) (store.Commit, error)
	Rollback(
		context.Context,
		store.Lock,
		store.RollbackRequest,
	) (store.RollbackCommit, error)
}

type StoreSink struct {
	Store          PublicationStore
	Lock           store.Lock
	MaximumDepth   uint32
	Metrics        *Metrics
	RollbackReason string
	OnProgress     func()
}

func (s StoreSink) Publish(ctx context.Context, blocks []DecodedBlock) error {
	if s.Store == nil || s.Lock == nil {
		return errors.New("publication store and writer lock are required")
	}
	if len(blocks) == 0 {
		return errors.New("cannot publish an empty decoded batch")
	}
	started := time.Now()
	var rows uint64
	candidates := make([]store.Candidate, len(blocks))
	for index, block := range blocks {
		rows += FactRows(block.Block)
		candidates[index] = store.Candidate{
			Block:       block.Block,
			ContentHash: block.ContentHash,
			Relays:      storeRelays(block.Relays),
			RawLength:   block.RawLength,
		}
	}
	commit, err := s.Store.Publish(ctx, s.Lock, candidates)
	if err != nil {
		return err
	}
	if !commit.Committed {
		return errors.New("publication store returned an uncommitted success")
	}
	if s.OnProgress != nil {
		s.OnProgress()
	}
	if s.Metrics != nil {
		s.Metrics.observePublish(len(blocks), rows, time.Since(started))
	}
	return nil
}

func (s StoreSink) Rollback(
	ctx context.Context,
	event model.AgreedEvent,
) error {
	if s.Store == nil || s.Lock == nil {
		return errors.New("publication store and writer lock are required")
	}
	if s.MaximumDepth == 0 {
		return errors.New("rollback maximum depth must be positive")
	}
	if event.Kind != model.EventRollback {
		return errors.New("store rollback received a non-rollback event")
	}
	reason := s.RollbackReason
	if reason == "" {
		reason = "unanimous relay rollback"
	}
	commit, err := s.Store.Rollback(ctx, s.Lock, store.RollbackRequest{
		To:           store.PointFromModel(event.Point),
		Relays:       storeRelays(event.Relays),
		Reason:       reason,
		MaximumDepth: s.MaximumDepth,
	})
	if err != nil {
		return err
	}
	if !commit.Committed {
		return errors.New("rollback store returned an uncommitted success")
	}
	if commit.Noop {
		return nil
	}
	if s.OnProgress != nil {
		s.OnProgress()
	}
	if s.Metrics != nil {
		s.Metrics.observeRollback()
	}
	return nil
}

func storeRelays(values []model.RelayIdentity) []store.Relay {
	ret := make([]store.Relay, len(values))
	for index, value := range values {
		ret[index] = store.Relay{
			Host:       value.Host,
			Address:    value.Address,
			Operator:   value.Operator,
			N2NVersion: value.N2NVersion,
		}
	}
	return ret
}
