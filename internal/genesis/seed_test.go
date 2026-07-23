package genesis

import (
	"context"
	"testing"
	"time"

	"clicksync/internal/model"
	"clicksync/internal/publication"
	"clicksync/internal/store"
)

type fakeState struct {
	origin    bool
	seeded    bool
	recovered store.GenesisPublication
	found     bool
	marked    []store.GenesisPublication
}

func (state *fakeState) GenesisState(context.Context) (bool, bool, error) {
	return state.origin, state.seeded, nil
}

func (state *fakeState) RecoverGenesisPublication(
	context.Context,
	model.Hash32,
) (store.GenesisPublication, bool, error) {
	return state.recovered, state.found, nil
}

func (state *fakeState) MarkGenesisSeeded(
	_ context.Context,
	_ store.LockAssertion,
	expected []store.GenesisPublication,
	_ [16]byte,
	_ string,
	_ time.Time,
) error {
	state.marked = append([]store.GenesisPublication(nil), expected...)
	return nil
}

type fakePublisher struct {
	calls int
}

func (publisher *fakePublisher) PublishBatch(
	context.Context,
	publication.Batch,
) (publication.BatchResult, error) {
	publisher.calls++
	return publication.BatchResult{PublicationIDs: []uint64{7}}, nil
}

type fakeLock struct{}

func (fakeLock) AssertHeld() error { return nil }
func (fakeLock) Path() string      { return "/tmp/test.lock" }

func TestEnsurePublishedFreshRecoveryAndSeededAudit(t *testing.T) {
	now := time.Date(2026, 7, 23, 14, 0, 0, 0, time.UTC)
	bundle := Bundle{
		Block:  model.Block{Hash: model.Hash32{1}, Synthetic: true},
		Source: publication.OfficialMainnetGenesisSource(),
	}
	state := &fakeState{origin: true}
	publisher := &fakePublisher{}
	if err := EnsurePublished(
		context.Background(),
		state,
		publisher,
		fakeLock{},
		bundle,
		[16]byte{1},
		"test",
		now,
	); err != nil {
		t.Fatal(err)
	}
	if publisher.calls != 1 ||
		len(state.marked) != 1 ||
		state.marked[0].PublicationID != 7 ||
		state.marked[0].TransactionCount != AVVMEntries ||
		state.marked[0].InitialSupply != InitialSupply {
		t.Fatalf("fresh seed publisher=%d marked=%+v", publisher.calls, state.marked)
	}

	recovered := store.GenesisPublication{
		PublicationID:    8,
		FactsDigest:      model.Hash32{2},
		TransactionCount: AVVMEntries,
		OutputCount:      AVVMEntries,
		InitialSupply:    InitialSupply,
	}
	state = &fakeState{origin: true, recovered: recovered, found: true}
	publisher = &fakePublisher{}
	if err := EnsurePublished(
		context.Background(),
		state,
		publisher,
		fakeLock{},
		bundle,
		[16]byte{1},
		"test",
		now,
	); err != nil {
		t.Fatal(err)
	}
	if publisher.calls != 0 ||
		len(state.marked) != 1 ||
		state.marked[0] != recovered {
		t.Fatalf("recovery publisher=%d marked=%+v", publisher.calls, state.marked)
	}

	state = &fakeState{origin: true, seeded: true}
	if err := EnsurePublished(
		context.Background(),
		state,
		&fakePublisher{},
		fakeLock{},
		bundle,
		[16]byte{1},
		"test",
		now,
	); err == nil {
		t.Fatal("genesis marker without exact active distribution was accepted")
	}
}

func TestEnsurePublishedRequiresOriginAndFlock(t *testing.T) {
	bundle := Bundle{
		Block:  model.Block{Hash: model.Hash32{1}, Synthetic: true},
		Source: publication.OfficialMainnetGenesisSource(),
	}
	if err := EnsurePublished(
		context.Background(),
		&fakeState{},
		&fakePublisher{},
		fakeLock{},
		bundle,
		[16]byte{1},
		"test",
		time.Now(),
	); err == nil {
		t.Fatal("partial dataset accepted official genesis")
	}
	if err := EnsurePublished(
		context.Background(),
		&fakeState{origin: true},
		&fakePublisher{},
		nil,
		bundle,
		[16]byte{1},
		"test",
		time.Now(),
	); err == nil {
		t.Fatal("nil flock was accepted")
	}
}
