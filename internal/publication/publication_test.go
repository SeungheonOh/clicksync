package publication

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/blinklabs-io/gouroboros/ledger"
	"golang.org/x/crypto/blake2b"

	"clicksync/internal/model"
)

type fakeAllocator struct {
	publication uint64
	event       uint64
}

func (allocator *fakeAllocator) ReservePublication() (uint64, error) {
	allocator.publication++
	return allocator.publication, nil
}

func (allocator *fakeAllocator) ReserveEvents(count uint64) (uint64, error) {
	first := allocator.event + 1
	allocator.event += count
	return first, nil
}

type fakeLock struct{ held bool }

func (lock *fakeLock) AssertHeld() error {
	if !lock.held {
		return errors.New("lost")
	}
	return nil
}

type fakeBackend struct {
	calls               []string
	snapshot            uint64
	resolved            map[OutputRef]ResolvedOutput
	resolutions         map[OutputRef]OutputResolution
	datums              map[model.Hash32][]byte
	adoptions           int
	rollbackHeaders     int
	descendants         []Descendant
	adoptionInsertError error
	adoptionStatus      bool
	adoptionStatusError error
	rollbackInsertError error
	rollbackStatus      bool
	rollbackStatusError error
	manifestUpdates     []ManifestUpdate
	lastAttempt         Attempt
	lastBatch           []Attempt
	lastRollback        RollbackCommit
	tip                 Point
	startOrigin         bool
	genesisSeeded       bool
}

func (backend *fakeBackend) call(value string) { backend.calls = append(backend.calls, value) }
func (backend *fakeBackend) CommittedSnapshot(context.Context) (uint64, error) {
	backend.call("snapshot")
	return backend.snapshot, nil
}
func (backend *fakeBackend) RawCommittedSnapshot(context.Context) (uint64, error) {
	backend.call("snapshot")
	return backend.snapshot, nil
}
func (backend *fakeBackend) CommittedTip(context.Context, uint64) (Point, error) {
	if backend.tip == (Point{}) {
		return Point{Origin: true}, nil
	}
	return backend.tip, nil
}
func (backend *fakeBackend) GenesisState(context.Context) (bool, bool, error) {
	return backend.startOrigin, backend.genesisSeeded, nil
}
func (backend *fakeBackend) ResolveOutputStates(
	_ context.Context,
	_ uint64,
	refs []OutputRef,
) (map[OutputRef]OutputResolution, error) {
	backend.call("resolve")
	ret := make(map[OutputRef]OutputResolution, len(refs))
	for _, ref := range refs {
		if resolution, found := backend.resolutions[ref]; found {
			ret[ref] = resolution
		} else if output, found := backend.resolved[ref]; found {
			ret[ref] = OutputResolution{
				State:          OutputActiveUnspent,
				Output:         output,
				OutputFactSeen: true,
			}
		} else {
			ret[ref] = OutputResolution{State: OutputNeverSeen}
		}
	}
	return ret, nil
}
func (backend *fakeBackend) ExistingDatumBodies(
	_ context.Context,
	_ []model.Hash32,
) (map[model.Hash32][]byte, error) {
	backend.call("datums")
	return backend.datums, nil
}
func (backend *fakeBackend) InsertBlockBatch(_ context.Context, attempts []Attempt) error {
	backend.call("blocks")
	backend.lastAttempt = attempts[len(attempts)-1]
	backend.lastBatch = append([]Attempt(nil), attempts...)
	return nil
}
func (backend *fakeBackend) InsertTransactionBatch(context.Context, []Attempt) error {
	backend.call("transactions")
	return nil
}
func (backend *fakeBackend) InsertInputBatch(context.Context, []Attempt) error {
	backend.call("inputs")
	return nil
}
func (backend *fakeBackend) InsertOutputBatch(context.Context, []Attempt) error {
	backend.call("outputs")
	return nil
}
func (backend *fakeBackend) InsertDatumBodyBatch(context.Context, []Attempt) error {
	backend.call("datum_bodies")
	return nil
}
func (backend *fakeBackend) InsertDatumObservationBatch(context.Context, []Attempt) error {
	backend.call("datum_observations")
	return nil
}
func (backend *fakeBackend) InsertWithdrawalBatch(context.Context, []Attempt) error {
	backend.call("withdrawals")
	return nil
}
func (backend *fakeBackend) InsertRedeemerBatch(context.Context, []Attempt) error {
	backend.call("redeemers")
	return nil
}
func (backend *fakeBackend) InsertMetadataBatch(context.Context, []Attempt) error {
	backend.call("metadata")
	return nil
}
func (backend *fakeBackend) VerifyFactBatch(context.Context, []Attempt) error {
	backend.call("verify")
	return nil
}
func (backend *fakeBackend) InsertAdoptionBatch(
	_ context.Context,
	attempts []Attempt,
	firstEvent uint64,
) error {
	backend.call("adoption")
	if backend.adoptionInsertError == nil || backend.adoptionStatus {
		backend.adoptions += len(attempts)
		backend.snapshot = firstEvent + uint64(len(attempts)) - 1
	}
	return backend.adoptionInsertError
}
func (backend *fakeBackend) AdoptionBatchCommitted(
	context.Context,
	[]Attempt,
	uint64,
) (bool, error) {
	backend.call("adoption_status")
	return backend.adoptionStatus, backend.adoptionStatusError
}
func (backend *fakeBackend) ActiveDescendants(
	context.Context,
	uint64,
	Point,
	uint32,
) ([]Descendant, error) {
	backend.call("descendants")
	return backend.descendants, nil
}
func (backend *fakeBackend) InsertInvalidations(
	context.Context,
	RollbackCommit,
	[]Descendant,
) error {
	backend.call("invalidations")
	return nil
}
func (backend *fakeBackend) InsertRollbackHeader(
	_ context.Context,
	commit RollbackCommit,
) error {
	backend.call("rollback_header")
	backend.lastRollback = commit
	if backend.rollbackInsertError == nil || backend.rollbackStatus {
		backend.rollbackHeaders++
		backend.snapshot = commit.EventSeq
	}
	return backend.rollbackInsertError
}
func (backend *fakeBackend) RollbackCommitted(context.Context, RollbackCommit) (bool, error) {
	backend.call("rollback_status")
	return backend.rollbackStatus, backend.rollbackStatusError
}
func (backend *fakeBackend) ReserveRollbackManifest(
	_ context.Context,
	_ Lock,
	commit RollbackCommit,
	_ string,
) (RollbackCommit, error) {
	backend.call("rollback_reserve")
	return commit, nil
}
func (backend *fakeBackend) MarkRollbackInvalidations(
	_ context.Context,
	_ Lock,
	_ RollbackCommit,
	_ string,
) error {
	backend.call("rollback_invalidations_manifest")
	return nil
}
func (backend *fakeBackend) FinalizeRollbackManifest(
	_ context.Context,
	_ Lock,
	commit RollbackCommit,
	build string,
) error {
	backend.call("manifest")
	backend.manifestUpdates = append(backend.manifestUpdates, ManifestUpdate{
		EventSeq:    commit.EventSeq,
		Tip:         commit.To,
		Kind:        ManifestRollback,
		WriterID:    commit.WriterID,
		WriterBuild: build,
		UpdatedAt:   commit.RecordedAt,
	})
	return nil
}
func (backend *fakeBackend) PersistManifest(_ context.Context, _ Lock, update ManifestUpdate) error {
	backend.call("manifest")
	backend.manifestUpdates = append(backend.manifestUpdates, update)
	return nil
}

func TestPublishExactFactOrderAndFreshRetry(t *testing.T) {
	backend := &fakeBackend{}
	allocator := &fakeAllocator{}
	lock := &fakeLock{held: true}
	fail := true
	coordinator := newFakeCoordinator(t, backend, allocator, lock, func(point FaultPoint) error {
		if fail && point == AfterInputFacts {
			fail = false
			return errors.New("crash")
		}
		return nil
	})
	if _, err := coordinator.Publish(context.Background(), validBlock(), validSource()); err == nil {
		t.Fatal("expected injected crash")
	}
	if backend.adoptions != 0 {
		t.Fatal("incomplete facts became adopted")
	}
	publicationID, err := coordinator.Publish(context.Background(), validBlock(), validSource())
	if err != nil {
		t.Fatal(err)
	}
	if publicationID != 2 {
		t.Fatalf("retry publication = %d, want fresh ID 2", publicationID)
	}
	wantSuffix := []string{
		"blocks", "transactions", "inputs", "outputs", "datum_bodies",
		"datum_observations", "withdrawals", "redeemers", "metadata", "verify",
		"adoption", "manifest",
	}
	if !reflect.DeepEqual(backend.calls[len(backend.calls)-len(wantSuffix):], wantSuffix) {
		t.Fatalf("successful fact order = %v", backend.calls)
	}
}

func TestCrashAtEveryPreCommitBoundaryNeverAdopts(t *testing.T) {
	points := []FaultPoint{
		AfterBlockFact,
		AfterTransactionFacts,
		AfterInputFacts,
		AfterOutputFacts,
		AfterDatumBodyFacts,
		AfterDatumObservationFacts,
		AfterWithdrawalFacts,
		AfterRedeemerFacts,
		AfterMetadataFacts,
		BeforeAdoption,
	}
	for _, point := range points {
		t.Run(string(point), func(t *testing.T) {
			backend := &fakeBackend{}
			coordinator := newFakeCoordinator(
				t,
				backend,
				&fakeAllocator{},
				&fakeLock{held: true},
				func(got FaultPoint) error {
					if got == point {
						return errors.New("stop")
					}
					return nil
				},
			)
			if _, err := coordinator.Publish(
				context.Background(),
				validBlock(),
				validSource(),
			); err == nil {
				t.Fatal("expected injected failure")
			}
			if backend.adoptions != 0 || len(backend.manifestUpdates) != 0 {
				t.Fatalf("boundary %s committed adoption/manifest", point)
			}
		})
	}
}

func TestPublishBatchUsesOneTableCallAndResolvesEarlierStagedOutput(t *testing.T) {
	backend := &fakeBackend{}
	allocator := &fakeAllocator{}
	coordinator := newFakeCoordinator(
		t,
		backend,
		allocator,
		&fakeLock{held: true},
		nil,
	)
	items := twoBlockBatch()
	result, err := coordinator.PublishBatch(context.Background(), Batch{
		Items:         items,
		FirstStagedAt: testNow(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(result.PublicationIDs, []uint64{1, 2}) ||
		result.FirstEventSeq != 1 ||
		result.LastEventSeq != 2 {
		t.Fatalf("batch result = %+v", result)
	}
	want := []string{
		"snapshot", "resolve", "datums",
		"blocks", "transactions", "inputs", "outputs", "datum_bodies",
		"datum_observations", "withdrawals", "redeemers", "metadata", "verify",
		"adoption", "manifest",
	}
	if !reflect.DeepEqual(backend.calls, want) {
		t.Fatalf("physical table calls = %v, want %v", backend.calls, want)
	}
	if len(backend.lastBatch) != 2 ||
		!backend.lastBatch[1].Block.Transactions[0].Inputs[0].SourceResolved {
		t.Fatal("cross-block staged output was not resolved")
	}
	if backend.adoptions != 2 || backend.snapshot != 2 {
		t.Fatalf("adoption state = %d/%d", backend.adoptions, backend.snapshot)
	}
}

func TestPublishBatchBoundsAndDoubleConsumption(t *testing.T) {
	coordinator := newFakeCoordinator(
		t,
		&fakeBackend{},
		&fakeAllocator{},
		&fakeLock{held: true},
		nil,
	)
	tooMany := make([]BatchItem, MaxBatchBlocks+1)
	if _, err := coordinator.PublishBatch(context.Background(), Batch{
		Items:         tooMany,
		FirstStagedAt: testNow(),
	}); err == nil {
		t.Fatal("oversized block-count batch was accepted")
	}
	if _, err := coordinator.PublishBatch(context.Background(), Batch{
		Items:         twoBlockBatch(),
		FirstStagedAt: testNow().Add(-MaxBatchAge - time.Nanosecond),
	}); err == nil {
		t.Fatal("over-age batch was accepted")
	}
	oversizedBytes := BatchItem{
		Block:  validBlock(),
		Source: validSource(),
	}
	oversizedBytes.Source.PeerHost = string(make([]byte, MaxBatchBytes+1))
	if _, err := coordinator.PublishBatch(context.Background(), Batch{
		Items:         []BatchItem{oversizedBytes},
		FirstStagedAt: testNow(),
	}); err == nil {
		t.Fatal("oversized byte batch was accepted")
	}

	items := twoBlockBatch()
	firstInput := items[1].Block.Transactions[0].Inputs[0]
	secondHash := filled32(0x45)
	fee := uint64(2)
	items[1].Block.Transactions = append(items[1].Block.Transactions, model.Transaction{
		Hash:         secondHash,
		Order:        1,
		Era:          "conway",
		Phase2Valid:  true,
		FlowKind:     "regular",
		DeclaredFee:  &fee,
		EffectiveFee: &fee,
		MintApplied:  true,
		Inputs: []model.Input{{
			TransactionHash:  secondHash,
			TransactionOrder: 1,
			SourceHash:       firstInput.SourceHash,
			SourceIndex:      firstInput.SourceIndex,
			BodyOrdinal:      0,
			Role:             "regular",
			Consumed:         true,
		}},
	})
	if _, err := coordinator.PublishBatch(context.Background(), Batch{
		Items:         items,
		FirstStagedAt: testNow(),
	}); err == nil {
		t.Fatal("double consumption in one batch was accepted")
	}
}

func TestBatchSuccessorByronEBBHeightRules(t *testing.T) {
	previous := validBlock()
	previous.Era = "Byron"
	previous.Type = int16(ledger.BlockTypeByronMain)
	previous.Slot = 100
	previous.Number = 10
	next := validBlock()
	next.Era = "Byron"
	next.Type = int16(ledger.BlockTypeByronEbb)
	next.ParentHash = &previous.Hash
	next.Slot = previous.Slot + 1

	next.Number = previous.Number
	if err := validateBatchSuccessor(previous, next); err != nil {
		t.Fatalf("same-height Byron EBB: %v", err)
	}
	next.Number = previous.Number + 1
	if err := validateBatchSuccessor(previous, next); err == nil {
		t.Fatal("incremented-height Byron EBB was accepted")
	}
	next.Type = int16(ledger.BlockTypeByronMain)
	next.Number = previous.Number
	if err := validateBatchSuccessor(previous, next); err == nil {
		t.Fatal("same-height non-EBB was accepted")
	}
	next.Type = int16(ledger.BlockTypeByronEbb)
	next.Number = previous.Number + 2
	if err := validateBatchSuccessor(previous, next); err == nil {
		t.Fatal("arbitrary EBB height gap was accepted")
	}
	next.Number = previous.Number + 1
	next.Slot = previous.Slot
	if err := validateBatchSuccessor(previous, next); err == nil {
		t.Fatal("same-slot EBB was accepted")
	}
	ebb := next
	ebb.Number = previous.Number
	ebb.Slot = previous.Slot + 1
	main := validBlock()
	main.Era = "Byron"
	main.Type = int16(ledger.BlockTypeByronMain)
	main.ParentHash = &ebb.Hash
	main.Number = ebb.Number + 1
	main.Slot = ebb.Slot
	if err := validateBatchSuccessor(ebb, main); err != nil {
		t.Fatalf("same-slot EBB-to-main transition: %v", err)
	}
	main.Slot++
	if err := validateBatchSuccessor(ebb, main); err != nil {
		t.Fatalf("later-slot EBB-to-main transition: %v", err)
	}
}

func TestCompleteHistoryRejectsUnresolvedButAcceptsCrossBlockInput(t *testing.T) {
	complete := &fakeBackend{startOrigin: true, genesisSeeded: true}
	coordinator := newFakeCoordinator(
		t,
		complete,
		&fakeAllocator{},
		&fakeLock{held: true},
		nil,
	)
	if _, err := coordinator.PublishBatch(context.Background(), Batch{
		Items: []BatchItem{{
			Block:  complexValidBlock(),
			Source: validSource(),
		}},
		FirstStagedAt: testNow(),
	}); err == nil {
		t.Fatal("complete-history dataset accepted an unresolved input")
	}
	if len(complete.lastBatch) != 0 {
		t.Fatal("unresolved complete-history input reached fact insertion")
	}
	if _, err := coordinator.PublishBatch(context.Background(), Batch{
		Items:         twoBlockBatch(),
		FirstStagedAt: testNow(),
	}); err != nil {
		t.Fatalf("complete-history cross-block input: %v", err)
	}
}

func TestBatchRejectsRepeatedPreBoundaryConsumption(t *testing.T) {
	ref := OutputRef{Hash: filled32(0x71), Index: 4}
	firstTx := filled32(0x72)
	secondTx := filled32(0x73)
	items := []BatchItem{
		{
			Block: model.Block{Transactions: []model.Transaction{{
				Hash:     firstTx,
				Order:    0,
				FlowKind: "regular",
				Inputs: []model.Input{
					testRoleInput(firstTx, 0, ref, "regular", true, 0),
				},
			}}},
		},
		{
			Block: model.Block{Transactions: []model.Transaction{{
				Hash:     secondTx,
				Order:    0,
				FlowKind: "regular",
				Inputs: []model.Input{
					testRoleInput(secondTx, 0, ref, "regular", true, 0),
				},
			}}},
		},
	}
	coordinator := &Coordinator{backend: &fakeBackend{}}
	if _, err := coordinator.resolveBatchInputs(
		context.Background(),
		0,
		items,
	); err == nil || !strings.Contains(err.Error(), "appears after an earlier consumption") {
		t.Fatalf("repeated pre-boundary consumption error = %v", err)
	}
}

func TestCommittedConsumptionWithoutOutputFactIsKnownInactive(t *testing.T) {
	ref := OutputRef{Hash: filled32(0x74), Index: 5}
	txHash := filled32(0x75)
	backend := &fakeBackend{resolutions: map[OutputRef]OutputResolution{
		ref: {
			State:             OutputKnownInactive,
			ActiveConsumption: true,
		},
	}}
	coordinator := &Coordinator{backend: backend}
	if _, err := coordinator.resolveBatchInputs(
		context.Background(),
		9,
		[]BatchItem{{Block: model.Block{Transactions: []model.Transaction{{
			Hash:     txHash,
			FlowKind: "regular",
			Inputs: []model.Input{
				testRoleInput(txHash, 0, ref, "regular", true, 0),
			},
		}}}}},
	); err == nil || !strings.Contains(err.Error(), "is known inactive") {
		t.Fatalf("committed repeat consumption error = %v", err)
	}
}

func TestBatchRejectsLocallyProducedOutputSpentTwice(t *testing.T) {
	producerHash := filled32(0x76)
	ref := OutputRef{Hash: producerHash, Index: 0}
	firstSpend := filled32(0x77)
	secondSpend := filled32(0x78)
	items := []BatchItem{{Block: model.Block{Transactions: []model.Transaction{
		{
			Hash:     producerHash,
			Order:    0,
			FlowKind: "regular",
			Outputs: []model.Output{{
				TransactionHash:       producerHash,
				TransactionOrder:      0,
				PaymentCredentialKind: "none",
				Lovelace:              10,
			}},
		},
		{
			Hash:     firstSpend,
			Order:    1,
			FlowKind: "regular",
			Inputs: []model.Input{
				testRoleInput(firstSpend, 1, ref, "regular", true, 0),
			},
		},
		{
			Hash:     secondSpend,
			Order:    2,
			FlowKind: "regular",
			Inputs: []model.Input{
				testRoleInput(secondSpend, 2, ref, "regular", true, 0),
			},
		},
	}}}}
	coordinator := &Coordinator{backend: &fakeBackend{}}
	if _, err := coordinator.resolveBatchInputs(
		context.Background(),
		0,
		items,
	); err == nil || !strings.Contains(err.Error(), "appears after an earlier consumption") {
		t.Fatalf("local output double-spend error = %v", err)
	}
}

func TestBatchRejectsAnyInputRoleAfterEarlierConsumption(t *testing.T) {
	ref := OutputRef{Hash: filled32(0x79), Index: 6}
	for _, role := range []string{"reference", "collateral"} {
		t.Run(role, func(t *testing.T) {
			firstTx := filled32(0x7a)
			secondTx := filled32(0x7b)
			items := []BatchItem{{Block: model.Block{Transactions: []model.Transaction{
				{
					Hash:     firstTx,
					Order:    0,
					FlowKind: "regular",
					Inputs: []model.Input{
						testRoleInput(firstTx, 0, ref, "regular", true, 0),
					},
				},
				{
					Hash:     secondTx,
					Order:    1,
					FlowKind: "regular",
					Inputs: []model.Input{
						testRoleInput(secondTx, 1, ref, role, false, 0),
					},
				},
			}}}}
			coordinator := &Coordinator{backend: &fakeBackend{}}
			if _, err := coordinator.resolveBatchInputs(
				context.Background(),
				0,
				items,
			); err == nil || !strings.Contains(err.Error(), "appears after an earlier consumption") {
				t.Fatalf("later %s input error = %v", role, err)
			}
		})
	}
}

func TestBatchPreservesAllowedSameTransactionRoleOverlaps(t *testing.T) {
	ref := OutputRef{Hash: filled32(0x7c), Index: 7}
	for _, test := range []struct {
		name       string
		flow       string
		phaseValid bool
		inputs     []model.Input
	}{
		{
			name:       "valid regular and collateral",
			flow:       "regular",
			phaseValid: true,
			inputs: []model.Input{
				testRoleInput(filled32(0x7d), 0, ref, "regular", true, 0),
				testRoleInput(filled32(0x7d), 0, ref, "collateral", false, 0),
			},
		},
		{
			name:       "invalid regular and collateral",
			flow:       "collateral",
			phaseValid: false,
			inputs: []model.Input{
				testRoleInput(filled32(0x7e), 0, ref, "regular", false, 0),
				testRoleInput(filled32(0x7e), 0, ref, "collateral", true, 0),
			},
		},
		{
			name:       "collateral and reference",
			flow:       "regular",
			phaseValid: true,
			inputs: []model.Input{
				testRoleInput(filled32(0x7f), 0, ref, "collateral", false, 0),
				testRoleInput(filled32(0x7f), 0, ref, "reference", false, 0),
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			txHash := test.inputs[0].TransactionHash
			fee := uint64(1)
			transaction := model.Transaction{
				Hash:        txHash,
				Era:         "conway",
				FlowKind:    test.flow,
				Phase2Valid: test.phaseValid,
				DeclaredFee: &fee,
				MintApplied: test.phaseValid,
				Inputs:      test.inputs,
			}
			if test.phaseValid {
				transaction.EffectiveFee = &fee
			}
			block := validBlock()
			block.Transactions = []model.Transaction{transaction}
			backend := &fakeBackend{}
			coordinator := newFakeCoordinator(
				t,
				backend,
				&fakeAllocator{},
				&fakeLock{held: true},
				nil,
			)
			if _, err := coordinator.PublishBatch(
				context.Background(),
				Batch{
					Items: []BatchItem{{
						Block:  block,
						Source: validSource(),
					}},
					FirstStagedAt: testNow(),
				},
			); err != nil {
				t.Fatal(err)
			}
			if len(backend.lastAttempt.Block.Transactions[0].Inputs) != 2 {
				t.Fatalf(
					"role rows = %#v",
					backend.lastAttempt.Block.Transactions[0].Inputs,
				)
			}
		})
	}
}

func TestBatchRejectsRollbackInactiveForkOutput(t *testing.T) {
	ref := OutputRef{Hash: filled32(0x80), Index: 8}
	txHash := filled32(0x81)
	backend := &fakeBackend{resolutions: map[OutputRef]OutputResolution{
		ref: {
			State:          OutputKnownInactive,
			OutputFactSeen: true,
		},
	}}
	coordinator := &Coordinator{backend: backend}
	if _, err := coordinator.resolveBatchInputs(
		context.Background(),
		10,
		[]BatchItem{{Block: model.Block{Transactions: []model.Transaction{{
			Hash:     txHash,
			FlowKind: "regular",
			Inputs: []model.Input{
				testRoleInput(txHash, 0, ref, "reference", false, 0),
			},
		}}}}},
	); err == nil || !strings.Contains(err.Error(), "is known inactive") {
		t.Fatalf("inactive fork output error = %v", err)
	}
}

func TestBatchRejectsSelfAndForwardOutputReferences(t *testing.T) {
	producerHash := filled32(0x82)
	ref := OutputRef{Hash: producerHash, Index: 0}
	output := model.Output{
		TransactionHash:       producerHash,
		PaymentCredentialKind: "none",
		Lovelace:              1,
	}
	consumerHash := filled32(0x83)
	consumer := model.Transaction{
		Hash:     consumerHash,
		FlowKind: "regular",
		Inputs: []model.Input{
			testRoleInput(consumerHash, 0, ref, "regular", true, 0),
		},
	}
	producer := model.Transaction{
		Hash:     producerHash,
		FlowKind: "regular",
		Outputs:  []model.Output{output},
	}
	self := producer
	self.Inputs = []model.Input{
		testRoleInput(producerHash, 0, ref, "regular", true, 0),
	}
	for _, test := range []struct {
		name  string
		items []BatchItem
	}{
		{
			name: "self reference",
			items: []BatchItem{{Block: model.Block{
				Transactions: []model.Transaction{self},
			}}},
		},
		{
			name: "later transaction",
			items: []BatchItem{{Block: model.Block{
				Transactions: []model.Transaction{consumer, producer},
			}}},
		},
		{
			name: "later block",
			items: []BatchItem{
				{Block: model.Block{Transactions: []model.Transaction{consumer}}},
				{Block: model.Block{Transactions: []model.Transaction{producer}}},
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			coordinator := &Coordinator{backend: &fakeBackend{}}
			if _, err := coordinator.resolveBatchInputs(
				context.Background(),
				0,
				test.items,
			); err == nil || !strings.Contains(
				err.Error(),
				"same or a future batch position",
			) {
				t.Fatalf("forward-reference error = %v", err)
			}
		})
	}
}

func TestSyntheticGenesisRequiresPinnedOfficialSource(t *testing.T) {
	txHash := filled32(0x61)
	block := model.Block{
		Hash:       filled32(0x60),
		Era:        "Byron",
		Type:       -1,
		Synthetic:  true,
		ObservedAt: testNow(),
		Transactions: []model.Transaction{{
			Hash:        txHash,
			Phase2Valid: true,
			FlowKind:    "genesis",
			Outputs: []model.Output{{
				TransactionHash:       txHash,
				Kind:                  "genesis",
				Address:               []byte{0x82, 0x01},
				PaymentCredentialKind: "none",
				Lovelace:              1,
				DatumKind:             "none",
			}},
		}},
	}
	backend := &fakeBackend{startOrigin: true}
	coordinator := newFakeCoordinator(
		t,
		backend,
		&fakeAllocator{},
		&fakeLock{held: true},
		nil,
	)
	if _, err := coordinator.Publish(
		context.Background(),
		block,
		OfficialMainnetGenesisSource(),
	); err != nil {
		t.Fatal(err)
	}
	badSource := OfficialMainnetGenesisSource()
	badSource.N2NVersion = 15
	if _, err := coordinator.Publish(
		context.Background(),
		block,
		badSource,
	); err == nil {
		t.Fatal("synthetic genesis accepted fabricated peer provenance")
	}
}

func TestLostAdoptionResponseAndPostCommitFaultAreTyped(t *testing.T) {
	backend := &fakeBackend{
		adoptionInsertError: errors.New("lost response"),
		adoptionStatus:      true,
	}
	coordinator := newFakeCoordinator(
		t,
		backend,
		&fakeAllocator{},
		&fakeLock{held: true},
		nil,
	)
	if _, err := coordinator.Publish(context.Background(), validBlock(), validSource()); err != nil {
		t.Fatalf("read-back-proven adoption failed: %v", err)
	}
	backend = &fakeBackend{}
	coordinator = newFakeCoordinator(
		t,
		backend,
		&fakeAllocator{},
		&fakeLock{held: true},
		func(point FaultPoint) error {
			if point == AfterAdoption {
				return errors.New("post-commit crash")
			}
			return nil
		},
	)
	_, err := coordinator.Publish(context.Background(), validBlock(), validSource())
	var committed *CommittedError
	if !errors.As(err, &committed) {
		t.Fatalf("post-commit fault = %T %v, want CommittedError", err, err)
	}
	if len(backend.manifestUpdates) != 0 {
		t.Fatal("post-adoption crash unexpectedly advanced authoritative manifest state")
	}
}

func TestIndeterminateAdoptionIsFatal(t *testing.T) {
	backend := &fakeBackend{
		adoptionInsertError: errors.New("lost response"),
		adoptionStatusError: errors.New("read-back unavailable"),
	}
	coordinator := newFakeCoordinator(
		t,
		backend,
		&fakeAllocator{},
		&fakeLock{held: true},
		nil,
	)
	_, err := coordinator.Publish(context.Background(), validBlock(), validSource())
	var indeterminate *IndeterminateCommitError
	if !errors.As(err, &indeterminate) {
		t.Fatalf("error = %T %v, want IndeterminateCommitError", err, err)
	}
}

func TestLostFlockAfterAuthoritativeHeadersSkipsManifest(t *testing.T) {
	adoptionBackend := &fakeBackend{}
	adoptionLock := &fakeLock{held: true}
	coordinator := newFakeCoordinator(
		t,
		adoptionBackend,
		&fakeAllocator{},
		adoptionLock,
		func(point FaultPoint) error {
			if point == AfterAdoption {
				adoptionLock.held = false
			}
			return nil
		},
	)
	_, err := coordinator.Publish(context.Background(), validBlock(), validSource())
	var committed *CommittedError
	if !errors.As(err, &committed) ||
		adoptionBackend.adoptions != 1 ||
		len(adoptionBackend.manifestUpdates) != 0 {
		t.Fatalf(
			"post-adoption flock loss err=%v adoptions=%d manifests=%d",
			err,
			adoptionBackend.adoptions,
			len(adoptionBackend.manifestUpdates),
		)
	}

	block := validBlock()
	rollbackBackend := &fakeBackend{
		snapshot: 1,
		descendants: []Descendant{{
			PublicationID: 1,
			Point:         pointFromBlock(block),
		}},
	}
	rollbackLock := &fakeLock{held: true}
	coordinator = newFakeCoordinator(
		t,
		rollbackBackend,
		&fakeAllocator{event: 1},
		rollbackLock,
		func(point FaultPoint) error {
			if point == AfterRollbackHeader {
				rollbackLock.held = false
			}
			return nil
		},
	)
	err = coordinator.Rollback(context.Background(), RollbackRequest{
		To:                    Point{Origin: true},
		CheckID:               rollbackCheckPointer(0x31),
		AgreementGroup:        rollbackCheckPointer(0x32),
		CheckAttempt:          1,
		CheckedEventSeq:       1,
		MaximumDepth:          2,
		RequiredCorroboration: 2,
		Observers: []RollbackObserver{
			{Peer: "peer-a", Operator: "operator-a"},
			{Peer: "peer-b", Operator: "operator-b"},
		},
	})
	if !errors.As(err, &committed) ||
		rollbackBackend.rollbackHeaders != 1 ||
		len(rollbackBackend.manifestUpdates) != 0 {
		t.Fatalf(
			"post-rollback flock loss err=%v headers=%d manifests=%d",
			err,
			rollbackBackend.rollbackHeaders,
			len(rollbackBackend.manifestUpdates),
		)
	}
}

func TestHeaderlessRollbackIsInertAndOperatorsMustBeIndependent(t *testing.T) {
	block := validBlock()
	backend := &fakeBackend{
		snapshot: 1,
		descendants: []Descendant{{
			PublicationID: 1,
			Point:         pointFromBlock(block),
		}},
	}
	fail := true
	coordinator := newFakeCoordinator(
		t,
		backend,
		&fakeAllocator{publication: 1, event: 1},
		&fakeLock{held: true},
		func(point FaultPoint) error {
			if fail && point == AfterInvalidations {
				fail = false
				return errors.New("crash")
			}
			return nil
		},
	)
	request := RollbackRequest{
		To:                    Point{Origin: true},
		CheckID:               rollbackCheckPointer(0x41),
		AgreementGroup:        rollbackCheckPointer(0x42),
		CheckAttempt:          1,
		CheckedEventSeq:       1,
		MaximumDepth:          2,
		RequiredCorroboration: 2,
		Observers: []RollbackObserver{
			{Peer: "peer-a", Operator: "operator-a"},
			{Peer: "peer-b", Operator: "operator-b"},
		},
	}
	if err := coordinator.Rollback(context.Background(), request); err == nil {
		t.Fatal("expected membership-only crash")
	}
	if backend.rollbackHeaders != 0 || backend.snapshot != 1 {
		t.Fatal("headerless rollback changed committed snapshot")
	}
	if err := coordinator.Rollback(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if backend.rollbackHeaders != 1 {
		t.Fatal("rollback header did not commit")
	}
	request.Observers[1].Operator = "operator-a"
	if err := coordinator.Rollback(context.Background(), request); err == nil {
		t.Fatal("same-operator endpoints counted as independent")
	}
}

func TestRollbackRejectsSingleCorroboratorBeforeAllocation(t *testing.T) {
	backend := &fakeBackend{}
	allocator := &fakeAllocator{}
	coordinator := newFakeCoordinator(
		t,
		backend,
		allocator,
		&fakeLock{held: true},
		nil,
	)
	err := coordinator.Rollback(context.Background(), RollbackRequest{
		To:                    Point{Origin: true},
		MaximumDepth:          1,
		RequiredCorroboration: 1,
		Observers: []RollbackObserver{{
			Peer:     "relay-a",
			Operator: "operator-a",
		}},
	})
	if err == nil || !strings.Contains(err.Error(), "at least two") {
		t.Fatalf("rollback error = %v", err)
	}
	if allocator.event != 0 ||
		allocator.publication != 0 ||
		len(backend.calls) != 0 {
		t.Fatalf(
			"invalid rollback allocated/mutated: allocator=%+v calls=%v",
			allocator,
			backend.calls,
		)
	}
}

func TestNoOpRollbackCommitsOnlyAtAuthoritativeTip(t *testing.T) {
	partialTip := Point{
		Slot:        10,
		Hash:        filled32(0x70),
		BlockNumber: 7,
	}
	request := func(to Point) RollbackRequest {
		return RollbackRequest{
			To:                    to,
			CheckID:               rollbackCheckPointer(0x51),
			AgreementGroup:        rollbackCheckPointer(0x52),
			CheckAttempt:          1,
			CheckedEventSeq:       4,
			MaximumDepth:          2,
			RequiredCorroboration: 2,
			Observers: []RollbackObserver{
				{Peer: "peer-a", Operator: "operator-a"},
				{Peer: "peer-b", Operator: "operator-b"},
			},
		}
	}
	for _, test := range []struct {
		name string
		tip  Point
	}{
		{name: "partial boundary", tip: partialTip},
		{name: "Origin empty dataset", tip: Point{Origin: true}},
	} {
		t.Run(test.name, func(t *testing.T) {
			backend := &fakeBackend{snapshot: 4, tip: test.tip}
			coordinator := newFakeCoordinator(
				t,
				backend,
				&fakeAllocator{event: 4},
				&fakeLock{held: true},
				nil,
			)
			if err := coordinator.Rollback(context.Background(), request(test.tip)); err != nil {
				t.Fatal(err)
			}
			if backend.rollbackHeaders != 1 ||
				backend.lastRollback.Depth != 0 ||
				!samePoint(backend.lastRollback.OldTip, test.tip) ||
				!samePoint(backend.lastRollback.To, test.tip) ||
				slices.Contains(backend.calls, "invalidations") ||
				len(backend.manifestUpdates) != 1 ||
				!samePoint(backend.manifestUpdates[0].Tip, test.tip) {
				t.Fatalf(
					"no-op rollback backend=%+v calls=%v",
					backend.lastRollback,
					backend.calls,
				)
			}
		})
	}

	lostResponse := &fakeBackend{
		snapshot:            4,
		tip:                 partialTip,
		rollbackInsertError: errors.New("lost response"),
		rollbackStatus:      true,
	}
	coordinator := newFakeCoordinator(
		t,
		lostResponse,
		&fakeAllocator{event: 4},
		&fakeLock{held: true},
		nil,
	)
	if err := coordinator.Rollback(context.Background(), request(partialTip)); err != nil {
		t.Fatalf("read-back-proven no-op rollback failed: %v", err)
	}
	if !slices.Contains(lostResponse.calls, "rollback_status") {
		t.Fatal("lost no-op rollback response was not read back")
	}

	notTip := &fakeBackend{snapshot: 4, tip: partialTip}
	coordinator = newFakeCoordinator(
		t,
		notTip,
		&fakeAllocator{event: 4},
		&fakeLock{held: true},
		nil,
	)
	unknown := partialTip
	unknown.Hash[0] ^= 1
	if err := coordinator.Rollback(context.Background(), request(unknown)); err == nil {
		t.Fatal("empty-descendant rollback to a non-tip point was accepted")
	}
	if notTip.rollbackHeaders != 0 {
		t.Fatal("non-tip no-op rollback inserted a header")
	}

	for _, malformed := range []Point{
		{Origin: true, Slot: 1},
		{Slot: 1},
	} {
		backend := &fakeBackend{snapshot: 4, tip: malformed}
		coordinator = newFakeCoordinator(
			t,
			backend,
			&fakeAllocator{event: 4},
			&fakeLock{held: true},
			nil,
		)
		if err := coordinator.Rollback(context.Background(), request(malformed)); err == nil {
			t.Fatalf("malformed rollback point %+v was accepted", malformed)
		}
		if len(backend.calls) != 0 {
			t.Fatalf("malformed rollback point reached backend calls %v", backend.calls)
		}
	}
}

func TestStrictBundleValidationRejectsCorruptContent(t *testing.T) {
	valid := complexValidBlock()
	tests := []struct {
		name   string
		mutate func(*model.Block)
	}{
		{
			name: "redeemer hash",
			mutate: func(block *model.Block) {
				block.Transactions[0].Redeemers[0].DataHash[0] ^= 1
			},
		},
		{
			name: "metadata hash",
			mutate: func(block *model.Block) {
				block.Transactions[0].Metadata.ContentHash[0] ^= 1
			},
		},
		{
			name: "input linkage",
			mutate: func(block *model.Block) {
				block.Transactions[0].Inputs[0].TransactionHash[0] ^= 1
			},
		},
		{
			name: "zero asset",
			mutate: func(block *model.Block) {
				block.Transactions[0].Outputs[0].Assets[0].Quantity = 0
			},
		},
		{
			name: "datum hash",
			mutate: func(block *model.Block) {
				block.Datums[0].Hash[0] ^= 1
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			block := valid
			block.Transactions = append([]model.Transaction(nil), valid.Transactions...)
			block.Transactions[0].Inputs = append([]model.Input(nil), valid.Transactions[0].Inputs...)
			block.Transactions[0].Outputs = append([]model.Output(nil), valid.Transactions[0].Outputs...)
			block.Transactions[0].Redeemers = append([]model.Redeemer(nil), valid.Transactions[0].Redeemers...)
			metadata := *valid.Transactions[0].Metadata
			block.Transactions[0].Metadata = &metadata
			block.Datums = append([]model.DatumBody(nil), valid.Datums...)
			test.mutate(&block)
			if err := validateBlock(block); err == nil {
				t.Fatal("corrupt bundle was accepted")
			}
		})
	}
}

func newFakeCoordinator(
	t *testing.T,
	backend Backend,
	allocator Allocator,
	lock Lock,
	fault FaultInjector,
) *Coordinator {
	t.Helper()
	coordinator, err := New(backend, allocator, lock, Config{
		WriterID:    [16]byte{1},
		WriterBuild: "test",
		Fault:       fault,
		Now: func() time.Time {
			return time.Date(2026, 7, 23, 1, 2, 3, 0, time.UTC)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return coordinator
}

func validBlock() model.Block {
	return model.Block{
		Hash:                   filled32(1),
		Slot:                   1,
		Number:                 1,
		Era:                    "conway",
		BodyHashVerified:       true,
		TransactionIDsVerified: true,
		ObservedAt:             time.Date(2026, 7, 23, 1, 2, 3, 0, time.UTC),
	}
}

func validSource() Source {
	return Source{
		PeerHost:     "relay-a",
		PeerAddress:  "192.0.2.1:3001",
		Operator:     "operator-a",
		N2NVersion:   15,
		NetworkMagic: 764824073,
	}
}

func complexValidBlock() model.Block {
	block := validBlock()
	txHash := filled32(2)
	sourceHash := filled32(3)
	datumCBOR := []byte{0x80}
	datumHash := model.Hash32(blake2b.Sum256(datumCBOR))
	redeemerCBOR := []byte{0x01}
	redeemerHash := model.Hash32(blake2b.Sum256(redeemerCBOR))
	metadataCBOR := []byte{0xa0}
	metadataHash := model.Hash32(blake2b.Sum256(metadataCBOR))
	policy := filled28(4)
	fee := uint64(2)
	block.Datums = []model.DatumBody{{Hash: datumHash, CBOR: datumCBOR}}
	block.Transactions = []model.Transaction{{
		Hash:         txHash,
		Order:        0,
		Era:          "conway",
		Phase2Valid:  true,
		FlowKind:     "regular",
		DeclaredFee:  &fee,
		EffectiveFee: &fee,
		MintApplied:  true,
		Inputs: []model.Input{{
			TransactionHash:  txHash,
			TransactionOrder: 0,
			SourceHash:       sourceHash,
			Role:             "regular",
			Consumed:         true,
		}},
		Outputs: []model.Output{{
			TransactionHash:       txHash,
			TransactionOrder:      0,
			Kind:                  "regular",
			Address:               []byte{1},
			PaymentCredentialKind: "none",
			Assets:                []model.Asset{{PolicyID: policy, Name: []byte{1}, Quantity: 1}},
			DatumKind:             "inline",
			DatumHash:             &datumHash,
		}},
		DatumObservations: []model.DatumObservation{{
			Hash:             datumHash,
			TransactionHash:  txHash,
			TransactionOrder: 0,
			SourceKind:       "inline_output",
			OutputIndex:      pointer32(0),
		}},
		Redeemers: []model.Redeemer{{
			TransactionHash:   txHash,
			TransactionOrder:  0,
			Purpose:           "spend",
			DataCBOR:          redeemerCBOR,
			DataHash:          redeemerHash,
			Applied:           true,
			TargetTxHash:      &sourceHash,
			TargetOutputIndex: pointer32(0),
		}},
		Metadata: &model.Metadata{
			TransactionHash: txHash,
			CBOR:            metadataCBOR,
			ContentHash:     metadataHash,
		},
	}}
	return block
}

func filled32(value byte) model.Hash32 {
	var ret model.Hash32
	for index := range ret {
		ret[index] = value
	}
	return ret
}

func filled28(value byte) model.Hash28 {
	var ret model.Hash28
	for index := range ret {
		ret[index] = value
	}
	return ret
}

func pointer32(value uint32) *uint32 { return &value }

func testRoleInput(
	txHash model.Hash32,
	txOrder uint32,
	ref OutputRef,
	role string,
	consumed bool,
	ordinal uint32,
) model.Input {
	return model.Input{
		TransactionHash:  txHash,
		TransactionOrder: txOrder,
		SourceHash:       ref.Hash,
		SourceIndex:      ref.Index,
		BodyOrdinal:      ordinal,
		Role:             role,
		Consumed:         consumed,
	}
}

func testNow() time.Time {
	return time.Date(2026, 7, 23, 1, 2, 3, 0, time.UTC)
}

func twoBlockBatch() []BatchItem {
	first := validBlock()
	first.Hash = filled32(0x41)
	first.Slot = 10
	first.Number = 10
	firstTx := filled32(0x42)
	firstFee := uint64(2)
	first.Transactions = []model.Transaction{{
		Hash:         firstTx,
		Order:        0,
		Era:          "conway",
		Phase2Valid:  true,
		FlowKind:     "regular",
		DeclaredFee:  &firstFee,
		EffectiveFee: &firstFee,
		MintApplied:  true,
		Outputs: []model.Output{{
			TransactionHash:       firstTx,
			TransactionOrder:      0,
			Index:                 0,
			BodyOrdinal:           0,
			Kind:                  "regular",
			Address:               []byte{1},
			PaymentCredentialKind: "none",
			Lovelace:              10,
			DatumKind:             "none",
		}},
	}}
	second := validBlock()
	second.Hash = filled32(0x43)
	second.ParentHash = &first.Hash
	second.Slot = 11
	second.Number = 11
	secondTx := filled32(0x44)
	secondFee := uint64(3)
	second.Transactions = []model.Transaction{{
		Hash:         secondTx,
		Order:        0,
		Era:          "conway",
		Phase2Valid:  true,
		FlowKind:     "regular",
		DeclaredFee:  &secondFee,
		EffectiveFee: &secondFee,
		MintApplied:  true,
		Inputs: []model.Input{{
			TransactionHash:  secondTx,
			TransactionOrder: 0,
			SourceHash:       firstTx,
			SourceIndex:      0,
			BodyOrdinal:      0,
			Role:             "regular",
			Consumed:         true,
		}},
	}}
	return []BatchItem{
		{Block: first, Source: validSource()},
		{Block: second, Source: validSource()},
	}
}

func TestCollateralEffectiveFeeResolutionSemantics(t *testing.T) {
	sourceHash := filled32(0x91)
	ref := OutputRef{Hash: sourceHash, Index: 3}
	base := func(declared *uint64) model.Transaction {
		txHash := filled32(0x92)
		return model.Transaction{
			Hash:         txHash,
			Phase2Valid:  false,
			FlowKind:     "collateral",
			DeclaredFee:  pointer64(99),
			EffectiveFee: declared,
			Inputs: []model.Input{{
				TransactionHash: txHash,
				SourceHash:      sourceHash,
				SourceIndex:     3,
				Role:            "collateral",
				Consumed:        true,
			}},
			Outputs: []model.Output{{
				TransactionHash:       txHash,
				Kind:                  "collateral_return",
				PaymentCredentialKind: "none",
				Lovelace:              40,
				DatumKind:             "none",
			}},
		}
	}
	t.Run("positive declared remains authoritative while unresolved", func(t *testing.T) {
		declared := uint64(60)
		tx := base(&declared)
		if err := resolveTransactionFacts(&tx, nil); err != nil {
			t.Fatal(err)
		}
		if tx.EffectiveFee == nil || *tx.EffectiveFee != 60 {
			t.Fatalf("effective fee = %v", tx.EffectiveFee)
		}
	})
	t.Run("zero or absent remains unknown while unresolved", func(t *testing.T) {
		tx := base(nil)
		if err := resolveTransactionFacts(&tx, nil); err != nil {
			t.Fatal(err)
		}
		if tx.EffectiveFee != nil {
			t.Fatalf("effective fee = %d, want unknown", *tx.EffectiveFee)
		}
	})
	t.Run("positive declared is cross-checked when resolved", func(t *testing.T) {
		declared := uint64(60)
		tx := base(&declared)
		if err := resolveTransactionFacts(&tx, map[OutputRef]ResolvedOutput{
			ref: {Lovelace: 100, PaymentCredentialKind: "none"},
		}); err != nil {
			t.Fatal(err)
		}
		if tx.EffectiveFee == nil || *tx.EffectiveFee != 60 {
			t.Fatalf("effective fee = %v", tx.EffectiveFee)
		}
	})
	t.Run("positive declared mismatch fails closed", func(t *testing.T) {
		declared := uint64(61)
		tx := base(&declared)
		err := resolveTransactionFacts(&tx, map[OutputRef]ResolvedOutput{
			ref: {Lovelace: 100, PaymentCredentialKind: "none"},
		})
		if err == nil || !strings.Contains(err.Error(), "differs from resolved") {
			t.Fatalf("error = %v", err)
		}
	})
	t.Run("zero or absent is derived when resolved", func(t *testing.T) {
		tx := base(nil)
		if err := resolveTransactionFacts(&tx, map[OutputRef]ResolvedOutput{
			ref: {Lovelace: 100, PaymentCredentialKind: "none"},
		}); err != nil {
			t.Fatal(err)
		}
		if tx.EffectiveFee == nil || *tx.EffectiveFee != 60 {
			t.Fatalf("effective fee = %v", tx.EffectiveFee)
		}
	})
	t.Run("zero-valued first duplicate collateral return fails closed", func(t *testing.T) {
		tx := base(nil)
		tx.Outputs[0].Lovelace = 0
		tx.Outputs = append(tx.Outputs, model.Output{
			Kind:                  "collateral_return",
			PaymentCredentialKind: "none",
			Lovelace:              40,
		})
		err := resolveTransactionFacts(&tx, map[OutputRef]ResolvedOutput{
			ref: {Lovelace: 100, PaymentCredentialKind: "none"},
		})
		if err == nil || !strings.Contains(err.Error(), "multiple collateral-return") {
			t.Fatalf("error = %v", err)
		}
	})
}

func TestSpendRedeemerResolvesPaymentScriptCredential(t *testing.T) {
	sourceHash := filled32(0xa1)
	targetIndex := uint32(2)
	scriptHash := filled28(0xa2)
	tx := model.Transaction{Redeemers: []model.Redeemer{{
		Purpose:           "spend",
		TargetTxHash:      &sourceHash,
		TargetOutputIndex: &targetIndex,
	}}}
	ref := OutputRef{Hash: sourceHash, Index: targetIndex}
	if err := resolveTransactionFacts(&tx, map[OutputRef]ResolvedOutput{
		ref: {
			Lovelace:              1,
			PaymentCredentialKind: "script",
			PaymentCredentialHash: &scriptHash,
		},
	}); err != nil {
		t.Fatal(err)
	}
	if tx.Redeemers[0].ResolvedScriptHash == nil ||
		*tx.Redeemers[0].ResolvedScriptHash != scriptHash {
		t.Fatalf("resolved script hash = %v", tx.Redeemers[0].ResolvedScriptHash)
	}
	tx.Redeemers[0].ResolvedScriptHash = nil
	if err := resolveTransactionFacts(&tx, map[OutputRef]ResolvedOutput{
		ref: {
			Lovelace:              1,
			PaymentCredentialKind: "key",
			PaymentCredentialHash: &scriptHash,
		},
	}); err != nil {
		t.Fatal(err)
	}
	if tx.Redeemers[0].ResolvedScriptHash != nil {
		t.Fatal("key payment credential produced a script hash")
	}
	wrong := filled28(0xa3)
	tx.Redeemers[0].ResolvedScriptHash = &wrong
	if err := resolveTransactionFacts(&tx, map[OutputRef]ResolvedOutput{
		ref: {
			Lovelace:              1,
			PaymentCredentialKind: "script",
			PaymentCredentialHash: &scriptHash,
		},
	}); err == nil || !strings.Contains(err.Error(), "disagrees") {
		t.Fatalf("mismatched pre-resolution error = %v", err)
	}
}

func pointer64(value uint64) *uint64 { return &value }

func rollbackCheckPointer(value byte) *[16]byte {
	var ret [16]byte
	for index := range ret {
		ret[index] = value
	}
	return &ret
}
