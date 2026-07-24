package syncer

import (
	"context"
	"testing"

	"cardano-clicksync/internal/model"
	"cardano-clicksync/internal/store"
)

type heldLock struct{}

func (heldLock) AssertHeld() error { return nil }

type fakePublicationStore struct {
	state     store.State
	published []store.Candidate
	rollback  *store.RollbackRequest
	noop      bool
}

func (f *fakePublicationStore) Publish(
	_ context.Context,
	_ store.Lock,
	values []store.Candidate,
) (store.Commit, error) {
	f.published = append([]store.Candidate(nil), values...)
	return store.Commit{Committed: true}, nil
}

func (f *fakePublicationStore) Rollback(
	_ context.Context,
	_ store.Lock,
	value store.RollbackRequest,
) (store.RollbackCommit, error) {
	f.rollback = &value
	return store.RollbackCommit{Committed: true, Noop: f.noop}, nil
}

func TestStoreSinkDoesNotCountNoopRollbackAsProgress(t *testing.T) {
	backend := &fakePublicationStore{noop: true}
	progress := 0
	metrics := &Metrics{}
	sink := StoreSink{
		Store:        backend,
		Lock:         heldLock{},
		MaximumDepth: 10,
		Metrics:      metrics,
		OnProgress: func() {
			progress++
		},
	}
	err := sink.Rollback(context.Background(), model.AgreedEvent{
		Kind:  model.EventRollback,
		Point: model.Point{Origin: true},
		Relays: []model.RelayIdentity{{
			Host:    "relay:3001",
			Address: "127.0.0.1:3001",
		}},
	})
	if err != nil {
		t.Fatalf("noop rollback: %v", err)
	}
	if progress != 0 || metrics.Snapshot().Rollbacks != 0 {
		t.Fatalf(
			"noop rollback progress = %d, rollback metric = %d",
			progress,
			metrics.Snapshot().Rollbacks,
		)
	}
}

func TestStoreSinkMapsPublicationAndRollback(t *testing.T) {
	point := store.Point{
		Slot:        42,
		Hash:        model.Hash32{1},
		BlockNumber: 9,
		IsByronEBB:  true,
	}
	backend := &fakePublicationStore{
		state: store.State{
			Dataset: store.DatasetIdentity{
				Start: store.Point{Slot: 1, Hash: model.Hash32{9}},
			},
			Canonical: []store.CanonicalBlock{{Point: point}},
		},
	}
	sink := StoreSink{
		Store:        backend,
		Lock:         heldLock{},
		MaximumDepth: 10,
	}
	block := DecodedBlock{
		Block:       model.Block{Slot: 43},
		ContentHash: model.Hash32{2},
		RawLength:   99,
		Relays: []model.RelayIdentity{{
			Host:       "relay:3001",
			Address:    "127.0.0.1:3001",
			Operator:   "operator",
			N2NVersion: 15,
		}},
	}
	if err := sink.Publish(context.Background(), []DecodedBlock{block}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if len(backend.published) != 1 ||
		backend.published[0].RawLength != block.RawLength ||
		len(backend.published[0].Relays) != 1 {
		t.Fatalf("unexpected candidates: %#v", backend.published)
	}
	event := model.AgreedEvent{
		Kind:   model.EventRollback,
		Point:  model.Point{Slot: point.Slot, Hash: point.Hash},
		Relays: block.Relays,
	}
	if err := sink.Rollback(context.Background(), event); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if backend.rollback == nil ||
		backend.rollback.To.Slot != point.Slot ||
		backend.rollback.To.Hash != point.Hash ||
		backend.rollback.To.BlockNumber != 0 {
		t.Fatalf("unexpected unresolved rollback request: %#v", backend.rollback)
	}
}
