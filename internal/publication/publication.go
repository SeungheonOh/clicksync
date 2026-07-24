// Package publication coordinates append-only ClickHouse publication. It
// deliberately keeps commit ordering above the native store adapter so tests
// can inject a crash after every fact table.
package publication

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/blinklabs-io/gouroboros/ledger"
	"golang.org/x/crypto/blake2b"

	"clicksync/internal/model"
)

type OutputRef struct {
	Hash  model.Hash32
	Index uint32
}

type ResolvedOutput struct {
	Lovelace              uint64
	PaymentCredentialKind string
	PaymentCredentialHash *model.Hash28
}

type OutputResolutionState string

const (
	OutputNeverSeen     OutputResolutionState = "never_seen"
	OutputActiveUnspent OutputResolutionState = "active_unspent"
	OutputKnownInactive OutputResolutionState = "known_inactive"
)

type OutputResolution struct {
	State             OutputResolutionState
	Output            ResolvedOutput
	OutputFactSeen    bool
	ActiveConsumption bool
}

type Point struct {
	Origin      bool
	Slot        uint64
	Hash        model.Hash32
	BlockNumber uint64
	IsByronEBB  bool
}

type Source struct {
	PeerHost     string
	PeerAddress  string
	Operator     string
	N2NVersion   uint16
	NetworkMagic uint32
}

func OfficialMainnetGenesisSource() Source {
	return Source{
		PeerHost:     "embedded-mainnet-genesis",
		PeerAddress:  "blake2b-256:dbbdaeab0ea4ea58225892d8b1294f178b417f4a9d1ed3bbf629c40d8f74e86b",
		Operator:     "intersect-official-genesis",
		N2NVersion:   0,
		NetworkMagic: 764824073,
	}
}

type Counts struct {
	Blocks            uint64
	Transactions      uint64
	Inputs            uint64
	Outputs           uint64
	DatumBodies       uint64
	DatumObservations uint64
	Withdrawals       uint64
	Redeemers         uint64
	Metadata          uint64
}

type Attempt struct {
	PublicationID  uint64
	SnapshotEvent  uint64
	Block          model.Block
	Source         Source
	NewDatumBodies []model.DatumBody
	Counts         Counts
	FactsDigest    model.Hash32
	WriterID       [16]byte
	InsertedAt     time.Time
}

const (
	MaxBatchBlocks = 256
	MaxBatchBytes  = 32 * 1024 * 1024
	MaxBatchRows   = 1_000_000
	MaxBatchAge    = time.Second
)

type BatchItem struct {
	Block  model.Block
	Source Source
}

type Batch struct {
	Items         []BatchItem
	FirstStagedAt time.Time
}

type BatchResult struct {
	PublicationIDs []uint64
	FirstEventSeq  uint64
	LastEventSeq   uint64
	LastCommitted  Point
}

type ManifestUpdate struct {
	EventSeq        uint64
	Tip             Point
	Kind            ManifestUpdateKind
	RemoteAdoptions uint64
	WriterID        [16]byte
	WriterBuild     string
	UpdatedAt       time.Time
}

type ManifestUpdateKind string

const (
	ManifestAdoption  ManifestUpdateKind = "adoption"
	ManifestRollback  ManifestUpdateKind = "rollback"
	ManifestGenesis   ManifestUpdateKind = "genesis"
	ManifestReconcile ManifestUpdateKind = "reconcile"
)

type Descendant struct {
	PublicationID uint64
	Point         Point
}

type RollbackRequest struct {
	To                    Point
	Reason                string
	Observers             []RollbackObserver
	CheckID               *[16]byte
	AgreementGroup        *[16]byte
	CheckAttempt          uint32
	CheckedEventSeq       uint64
	MaximumDepth          uint32
	RequiredCorroboration int
	RecordedAt            time.Time
}

type RollbackObserver struct {
	Peer     string
	Operator string
}

type RollbackCommit struct {
	RollbackID            [16]byte
	EventSeq              uint64
	To                    Point
	OldTip                Point
	OldEventSeq           uint64
	Depth                 uint32
	Reason                string
	ObservedPeers         []string
	ObservedOperators     []string
	CorroborationRequired uint16
	CheckID               *[16]byte
	AgreementGroup        *[16]byte
	CheckAttempt          uint32
	CheckedEventSeq       uint64
	EvidenceCount         uint32
	EvidenceDigest        model.Hash32
	WriterID              [16]byte
	RecordedAt            time.Time
}

type Allocator interface {
	ReservePublication() (uint64, error)
	ReserveEvents(count uint64) (uint64, error)
}

type Lock interface {
	AssertHeld() error
}

type Backend interface {
	BatchBackend

	CommittedSnapshot(context.Context) (uint64, error)
	RawCommittedSnapshot(context.Context) (uint64, error)
	CommittedTip(context.Context, uint64) (Point, error)
	GenesisState(context.Context) (bool, bool, error)
	ResolveOutputStates(context.Context, uint64, []OutputRef) (map[OutputRef]OutputResolution, error)
	ExistingDatumBodies(context.Context, []model.Hash32) (map[model.Hash32][]byte, error)

	ActiveDescendants(context.Context, uint64, Point, uint32) ([]Descendant, error)
	InsertInvalidations(context.Context, RollbackCommit, []Descendant) error
	InsertRollbackHeader(context.Context, RollbackCommit) error
	RollbackCommitted(context.Context, RollbackCommit) (bool, error)
	ReserveRollbackManifest(context.Context, Lock, RollbackCommit, string) (RollbackCommit, error)
	MarkRollbackInvalidations(context.Context, Lock, RollbackCommit, string) error
	FinalizeRollbackManifest(context.Context, Lock, RollbackCommit, string) error

	PersistManifest(context.Context, Lock, ManifestUpdate) error
}

// BatchBackend is the table-oriented physical insert surface. One method call
// corresponds to at most one native ClickHouse insert for its fact table.
type BatchBackend interface {
	InsertBlockBatch(context.Context, []Attempt) error
	InsertTransactionBatch(context.Context, []Attempt) error
	InsertInputBatch(context.Context, []Attempt) error
	InsertOutputBatch(context.Context, []Attempt) error
	InsertDatumBodyBatch(context.Context, []Attempt) error
	InsertDatumObservationBatch(context.Context, []Attempt) error
	InsertWithdrawalBatch(context.Context, []Attempt) error
	InsertRedeemerBatch(context.Context, []Attempt) error
	InsertMetadataBatch(context.Context, []Attempt) error
	VerifyFactBatch(context.Context, []Attempt) error
	InsertAdoptionBatch(context.Context, []Attempt, uint64) error
	AdoptionBatchCommitted(context.Context, []Attempt, uint64) (bool, error)
}

type FaultPoint string

const (
	AfterBlockFact             FaultPoint = "after_blocks"
	AfterTransactionFacts      FaultPoint = "after_transactions"
	AfterInputFacts            FaultPoint = "after_inputs"
	AfterOutputFacts           FaultPoint = "after_outputs"
	AfterDatumBodyFacts        FaultPoint = "after_datum_bodies"
	AfterDatumObservationFacts FaultPoint = "after_datum_observations"
	AfterWithdrawalFacts       FaultPoint = "after_withdrawals"
	AfterRedeemerFacts         FaultPoint = "after_redeemers"
	AfterMetadataFacts         FaultPoint = "after_transaction_metadata"
	BeforeAdoption             FaultPoint = "before_adoption"
	AfterAdoption              FaultPoint = "after_adoption"
	AfterInvalidations         FaultPoint = "after_rollback_membership"
	BeforeRollbackHeader       FaultPoint = "before_rollback_header"
	AfterRollbackHeader        FaultPoint = "after_rollback_header"
)

type FaultInjector func(FaultPoint) error

type Config struct {
	WriterID    [16]byte
	WriterBuild string
	Fault       FaultInjector
	Now         func() time.Time
}

type Coordinator struct {
	backend   Backend
	allocator Allocator
	lock      Lock
	config    Config
}

// CommittedError means the adoption/rollback header is already authoritative
// but post-commit fault injection or manifest reconciliation did not finish.
// A caller must not publish the same chain step again. It should stop without
// acknowledging the peer and reconcile on restart.
type CommittedError struct {
	PublicationID uint64
	EventSeq      uint64
	Err           error
}

// IndeterminateCommitError is fatal: a commit insert returned an error and
// exact read-back could not prove whether the header is authoritative.
type IndeterminateCommitError struct {
	EventSeq uint64
	Err      error
}

func (err *IndeterminateCommitError) Error() string {
	return fmt.Sprintf("event %d commit state is indeterminate: %v", err.EventSeq, err.Err)
}

func (err *IndeterminateCommitError) Unwrap() error { return err.Err }

func (err *CommittedError) Error() string {
	return fmt.Sprintf(
		"event %d is committed (publication %d) but post-commit work failed: %v",
		err.EventSeq,
		err.PublicationID,
		err.Err,
	)
}

func (err *CommittedError) Unwrap() error {
	return err.Err
}

func New(backend Backend, allocator Allocator, lock Lock, config Config) (*Coordinator, error) {
	if backend == nil || allocator == nil || lock == nil {
		return nil, errors.New("publication backend, allocator, and writer lock are required")
	}
	if config.WriterID == ([16]byte{}) {
		return nil, errors.New("writer ID must be non-zero")
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	return &Coordinator{backend: backend, allocator: allocator, lock: lock, config: config}, nil
}

// Publish inserts immutable facts first and the adoption event last. Any
// failure consumes the allocated publication/event identities.
func (coordinator *Coordinator) Publish(
	ctx context.Context,
	block model.Block,
	source Source,
) (uint64, error) {
	result, err := coordinator.PublishBatch(ctx, Batch{
		Items: []BatchItem{{
			Block:  block,
			Source: source,
		}},
		FirstStagedAt: coordinator.config.Now().UTC(),
	})
	if len(result.PublicationIDs) == 0 {
		return 0, err
	}
	return result.PublicationIDs[0], err
}

// PublishBatch stages a bounded contiguous prefix and writes each populated
// fact table at most once. Every publication is content-verified independently
// before the single adoption-header insert makes the ordered prefix visible.
func (coordinator *Coordinator) PublishBatch(
	ctx context.Context,
	input Batch,
) (BatchResult, error) {
	if err := coordinator.assertWriter(); err != nil {
		return BatchResult{}, err
	}
	now := coordinator.config.Now().UTC()
	if len(input.Items) == 0 || len(input.Items) > MaxBatchBlocks {
		return BatchResult{}, fmt.Errorf(
			"physical microbatch contains %d blocks, allowed 1..%d",
			len(input.Items),
			MaxBatchBlocks,
		)
	}
	if input.FirstStagedAt.IsZero() {
		return BatchResult{}, errors.New("physical microbatch first-staged time is required")
	}
	firstStagedAt := input.FirstStagedAt.UTC()
	if firstStagedAt.After(now) {
		return BatchResult{}, errors.New("physical microbatch first-staged time is in the future")
	}
	if now.Sub(firstStagedAt) > MaxBatchAge {
		return BatchResult{}, fmt.Errorf(
			"physical microbatch staged for %s, exceeds %s",
			now.Sub(firstStagedAt),
			MaxBatchAge,
		)
	}
	encoded, err := json.Marshal(input.Items)
	if err != nil {
		return BatchResult{}, fmt.Errorf("size physical microbatch: %w", err)
	}
	if len(encoded) > MaxBatchBytes {
		return BatchResult{}, fmt.Errorf(
			"physical microbatch encoded bytes %d exceed %d",
			len(encoded),
			MaxBatchBytes,
		)
	}
	for index, item := range input.Items {
		if err := validateSource(item.Source, item.Block.Synthetic); err != nil {
			return BatchResult{}, fmt.Errorf("validate source provenance for block %d: %w", index, err)
		}
		if err := validateBlock(item.Block); err != nil {
			return BatchResult{}, fmt.Errorf("validate publication bundle for block %d: %w", index, err)
		}
		if index > 0 && !item.Block.Synthetic {
			previous := input.Items[index-1].Block
			if err := validateBatchSuccessor(previous, item.Block); err != nil {
				return BatchResult{}, fmt.Errorf("block %d is not a contiguous successor: %w", index, err)
			}
		}
	}
	startOrigin, seeded, err := coordinator.backend.GenesisState(ctx)
	if err != nil {
		return BatchResult{}, fmt.Errorf("read genesis publication state: %w", err)
	}
	for _, item := range input.Items {
		if item.Block.Synthetic {
			if !startOrigin || seeded {
				return BatchResult{}, errors.New("synthetic genesis publication is not allowed for this dataset state")
			}
		} else if startOrigin && !seeded {
			return BatchResult{}, errors.New("normal chain publication is forbidden until the exact genesis bundle is complete")
		}
	}
	snapshot, err := coordinator.backend.RawCommittedSnapshot(ctx)
	if err != nil {
		return BatchResult{}, fmt.Errorf("read physical committed snapshot: %w", err)
	}
	tip, err := coordinator.backend.CommittedTip(ctx, snapshot)
	if err != nil {
		return BatchResult{}, fmt.Errorf("read committed tip: %w", err)
	}
	if !tip.Origin && !input.Items[0].Block.Synthetic {
		previous := model.Block{
			Hash:   tip.Hash,
			Slot:   tip.Slot,
			Number: tip.BlockNumber,
		}
		if tip.IsByronEBB {
			previous.Era = "Byron"
			previous.Type = int16(ledger.BlockTypeByronEbb)
		}
		if err := validateBatchSuccessor(previous, input.Items[0].Block); err != nil {
			return BatchResult{}, fmt.Errorf("first block does not extend committed tip: %w", err)
		}
	}
	blocks, err := coordinator.resolveBatchInputs(ctx, snapshot, input.Items)
	if err != nil {
		return BatchResult{}, err
	}
	if startOrigin && seeded {
		for blockIndex, block := range blocks {
			for transactionIndex, transaction := range block.Transactions {
				for inputIndex, input := range transaction.Inputs {
					if !input.SourceResolved {
						return BatchResult{}, fmt.Errorf(
							"complete-history input is unresolved at block %d transaction %d input %d",
							blockIndex,
							transactionIndex,
							inputIndex,
						)
					}
				}
			}
		}
	}
	newDatumBodies, err := coordinator.precheckBatchDatumBodies(ctx, blocks)
	if err != nil {
		return BatchResult{}, err
	}

	publicationIDs := make([]uint64, len(input.Items))
	for index := range publicationIDs {
		publicationIDs[index], err = coordinator.allocator.ReservePublication()
		if err != nil {
			return BatchResult{}, err
		}
	}
	firstEvent, err := coordinator.allocator.ReserveEvents(uint64(len(input.Items)))
	if err != nil {
		return BatchResult{}, err
	}
	attempts := make([]Attempt, len(input.Items))
	var totalRows uint64
	for index, item := range input.Items {
		attempts[index] = Attempt{
			PublicationID:  publicationIDs[index],
			SnapshotEvent:  snapshot,
			Block:          blocks[index],
			Source:         item.Source,
			NewDatumBodies: newDatumBodies[index],
			WriterID:       coordinator.config.WriterID,
			InsertedAt:     now,
		}
		attempt := &attempts[index]
		attempt.Counts, err = countFacts(*attempt)
		if err != nil {
			return BatchResult{}, err
		}
		attempt.FactsDigest, err = digestFacts(*attempt)
		if err != nil {
			return BatchResult{}, err
		}
		rows := attempt.Counts.Blocks +
			attempt.Counts.Transactions +
			attempt.Counts.Inputs +
			attempt.Counts.Outputs +
			attempt.Counts.DatumBodies +
			attempt.Counts.DatumObservations +
			attempt.Counts.Withdrawals +
			attempt.Counts.Redeemers +
			attempt.Counts.Metadata
		if rows > MaxBatchRows-totalRows {
			return BatchResult{}, fmt.Errorf("physical microbatch rows exceed %d", MaxBatchRows)
		}
		totalRows += rows
	}
	steps := []struct {
		point FaultPoint
		write func(context.Context, []Attempt) error
	}{
		{AfterBlockFact, coordinator.backend.InsertBlockBatch},
		{AfterTransactionFacts, coordinator.backend.InsertTransactionBatch},
		{AfterInputFacts, coordinator.backend.InsertInputBatch},
		{AfterOutputFacts, coordinator.backend.InsertOutputBatch},
		{AfterDatumBodyFacts, coordinator.backend.InsertDatumBodyBatch},
		{AfterDatumObservationFacts, coordinator.backend.InsertDatumObservationBatch},
		{AfterWithdrawalFacts, coordinator.backend.InsertWithdrawalBatch},
		{AfterRedeemerFacts, coordinator.backend.InsertRedeemerBatch},
		{AfterMetadataFacts, coordinator.backend.InsertMetadataBatch},
	}
	for _, step := range steps {
		if err := coordinator.assertWriter(); err != nil {
			return BatchResult{}, err
		}
		if err := step.write(ctx, attempts); err != nil {
			return BatchResult{}, fmt.Errorf("%s: %w", step.point, err)
		}
		if err := coordinator.inject(step.point); err != nil {
			return BatchResult{}, err
		}
	}
	if err := coordinator.backend.VerifyFactBatch(ctx, attempts); err != nil {
		return BatchResult{}, fmt.Errorf("verify publication facts: %w", err)
	}
	if err := coordinator.inject(BeforeAdoption); err != nil {
		return BatchResult{}, err
	}
	if err := coordinator.assertWriter(); err != nil {
		return BatchResult{}, err
	}
	lastEvent := firstEvent + uint64(len(attempts)) - 1
	if insertErr := coordinator.backend.InsertAdoptionBatch(ctx, attempts, firstEvent); insertErr != nil {
		committed, verifyErr := coordinator.backend.AdoptionBatchCommitted(ctx, attempts, firstEvent)
		if verifyErr != nil {
			return BatchResult{}, &IndeterminateCommitError{
				EventSeq: lastEvent,
				Err:      errors.Join(insertErr, verifyErr),
			}
		}
		if !committed {
			return BatchResult{}, fmt.Errorf("commit adoption batch: %w", insertErr)
		}
	}
	lastCommitted := pointFromBlock(attempts[len(attempts)-1].Block)
	if attempts[len(attempts)-1].Block.Synthetic {
		lastCommitted = Point{Origin: true}
	}
	result := BatchResult{
		PublicationIDs: append([]uint64(nil), publicationIDs...),
		FirstEventSeq:  firstEvent,
		LastEventSeq:   lastEvent,
		LastCommitted:  lastCommitted,
	}
	if err := coordinator.inject(AfterAdoption); err != nil {
		return result, &CommittedError{
			PublicationID: publicationIDs[len(publicationIDs)-1],
			EventSeq:      lastEvent,
			Err:           err,
		}
	}
	if err := coordinator.assertWriter(); err != nil {
		return result, &CommittedError{
			PublicationID: publicationIDs[len(publicationIDs)-1],
			EventSeq:      lastEvent,
			Err:           fmt.Errorf("writer flock lost before post-adoption manifest: %w", err),
		}
	}
	last := attempts[len(attempts)-1]
	var remoteAdoptions uint64
	for _, attempt := range attempts {
		if !attempt.Block.Synthetic {
			remoteAdoptions++
		}
	}
	updateKind := ManifestAdoption
	if remoteAdoptions == 0 {
		updateKind = ManifestGenesis
	}
	if err := coordinator.backend.PersistManifest(ctx, coordinator.lock, ManifestUpdate{
		EventSeq:        lastEvent,
		Tip:             lastCommitted,
		Kind:            updateKind,
		RemoteAdoptions: remoteAdoptions,
		WriterID:        coordinator.config.WriterID,
		WriterBuild:     coordinator.config.WriterBuild,
		UpdatedAt:       coordinator.config.Now().UTC(),
	}); err != nil {
		return result, &CommittedError{
			PublicationID: last.PublicationID,
			EventSeq:      lastEvent,
			Err:           fmt.Errorf("persist post-publication manifest: %w", err),
		}
	}
	return result, nil
}

func (coordinator *Coordinator) Rollback(ctx context.Context, request RollbackRequest) error {
	if err := coordinator.assertWriter(); err != nil {
		return err
	}
	if err := validatePoint(request.To); err != nil {
		return fmt.Errorf("validate rollback target: %w", err)
	}
	if request.MaximumDepth == 0 {
		return errors.New("rollback maximum depth must be non-zero")
	}
	if request.RequiredCorroboration < 2 {
		return errors.New("rollback corroboration must require at least two operators")
	}
	if request.RequiredCorroboration > math.MaxUint16 {
		return errors.New("rollback corroboration exceeds UInt16")
	}
	if request.CheckID == nil ||
		request.AgreementGroup == nil ||
		*request.CheckID == ([16]byte{}) ||
		*request.AgreementGroup == ([16]byte{}) ||
		request.CheckAttempt == 0 {
		return errors.New("rollback request lacks exact trust check/group/attempt identity")
	}
	peers, operators, operatorCount, err := independentObservers(request.Observers)
	if err != nil {
		return err
	}
	if operatorCount < request.RequiredCorroboration {
		return fmt.Errorf(
			"rollback has %d independent peer observations, requires %d",
			operatorCount,
			request.RequiredCorroboration,
		)
	}
	snapshot, err := coordinator.backend.RawCommittedSnapshot(ctx)
	if err != nil {
		return fmt.Errorf("read rollback snapshot: %w", err)
	}
	descendants, err := coordinator.backend.ActiveDescendants(
		ctx,
		snapshot,
		request.To,
		request.MaximumDepth,
	)
	if err != nil {
		return fmt.Errorf("resolve active rollback descendants: %w", err)
	}
	if uint64(len(descendants)) > uint64(request.MaximumDepth) {
		return fmt.Errorf("rollback depth %d exceeds configured maximum %d", len(descendants), request.MaximumDepth)
	}
	for index, descendant := range descendants {
		if err := validatePoint(descendant.Point); err != nil {
			return fmt.Errorf("validate rollback descendant %d: %w", index, err)
		}
	}
	oldTip := request.To
	if len(descendants) == 0 {
		currentTip, err := coordinator.backend.CommittedTip(ctx, snapshot)
		if err != nil {
			return fmt.Errorf("read no-op rollback tip: %w", err)
		}
		if err := validatePoint(currentTip); err != nil {
			return fmt.Errorf("validate committed rollback tip: %w", err)
		}
		if !samePoint(currentTip, request.To) {
			return errors.New("rollback has no active descendants and target is not the committed tip")
		}
	} else {
		oldTip = descendants[0].Point
	}
	eventSeq, err := coordinator.allocator.ReserveEvents(1)
	if err != nil {
		return err
	}
	rollbackID, err := randomID()
	if err != nil {
		return err
	}
	recordedAt := request.RecordedAt.UTC().Truncate(time.Microsecond)
	if recordedAt.IsZero() {
		recordedAt = coordinator.config.Now().UTC().Truncate(time.Microsecond)
	}
	commit := RollbackCommit{
		RollbackID:            rollbackID,
		EventSeq:              eventSeq,
		To:                    request.To,
		OldTip:                oldTip,
		OldEventSeq:           snapshot,
		Depth:                 uint32(len(descendants)),
		Reason:                request.Reason,
		ObservedPeers:         peers,
		ObservedOperators:     operators,
		CorroborationRequired: uint16(request.RequiredCorroboration),
		CheckID:               request.CheckID,
		AgreementGroup:        request.AgreementGroup,
		CheckAttempt:          request.CheckAttempt,
		CheckedEventSeq:       request.CheckedEventSeq,
		WriterID:              coordinator.config.WriterID,
		RecordedAt:            recordedAt,
	}
	if err := coordinator.assertWriter(); err != nil {
		return err
	}
	commit, err = coordinator.backend.ReserveRollbackManifest(
		ctx,
		coordinator.lock,
		commit,
		coordinator.config.WriterBuild,
	)
	if err != nil {
		return fmt.Errorf("reserve conservative rollback manifest: %w", err)
	}
	if len(descendants) > 0 {
		if err := coordinator.backend.InsertInvalidations(ctx, commit, descendants); err != nil {
			return fmt.Errorf("insert rollback membership: %w", err)
		}
		if err := coordinator.inject(AfterInvalidations); err != nil {
			return err
		}
	}
	if err := coordinator.backend.MarkRollbackInvalidations(
		ctx,
		coordinator.lock,
		commit,
		coordinator.config.WriterBuild,
	); err != nil {
		return fmt.Errorf("persist rollback invalidation stage: %w", err)
	}
	if err := coordinator.inject(BeforeRollbackHeader); err != nil {
		return err
	}
	if err := coordinator.assertWriter(); err != nil {
		return err
	}
	if insertErr := coordinator.backend.InsertRollbackHeader(ctx, commit); insertErr != nil {
		committed, verifyErr := coordinator.backend.RollbackCommitted(ctx, commit)
		if verifyErr != nil {
			return &IndeterminateCommitError{
				EventSeq: eventSeq,
				Err:      errors.Join(insertErr, verifyErr),
			}
		}
		if !committed {
			return fmt.Errorf("commit rollback header: %w", insertErr)
		}
	}
	if err := coordinator.inject(AfterRollbackHeader); err != nil {
		return &CommittedError{EventSeq: eventSeq, Err: err}
	}
	if err := coordinator.assertWriter(); err != nil {
		return &CommittedError{
			EventSeq: eventSeq,
			Err:      fmt.Errorf("writer flock lost before post-rollback manifest: %w", err),
		}
	}
	if err := coordinator.backend.FinalizeRollbackManifest(
		ctx,
		coordinator.lock,
		commit,
		coordinator.config.WriterBuild,
	); err != nil {
		return &CommittedError{
			EventSeq: eventSeq,
			Err:      fmt.Errorf("persist post-rollback manifest: %w", err),
		}
	}
	return nil
}

func (coordinator *Coordinator) resolveBatchInputs(
	ctx context.Context,
	snapshot uint64,
	items []BatchItem,
) ([]model.Block, error) {
	refs := make([]OutputRef, 0)
	for _, item := range items {
		for _, transaction := range item.Block.Transactions {
			for _, input := range transaction.Inputs {
				refs = append(refs, OutputRef{Hash: input.SourceHash, Index: input.SourceIndex})
			}
		}
	}
	prior, err := coordinator.backend.ResolveOutputStates(ctx, snapshot, refs)
	if err != nil {
		return nil, fmt.Errorf("resolve prior output states: %w", err)
	}
	type stagedOutput struct {
		block       int
		transaction int
		output      ResolvedOutput
	}
	produced := make(map[OutputRef]stagedOutput)
	for blockIndex, item := range items {
		for transactionIndex, transaction := range item.Block.Transactions {
			for _, output := range transaction.Outputs {
				ref := OutputRef{Hash: output.TransactionHash, Index: output.Index}
				if _, duplicate := produced[ref]; duplicate {
					return nil, fmt.Errorf(
						"duplicate batch output %x#%d",
						ref.Hash,
						ref.Index,
					)
				}
				produced[ref] = stagedOutput{
					block:       blockIndex,
					transaction: transactionIndex,
					output: ResolvedOutput{
						Lovelace:              output.Lovelace,
						PaymentCredentialKind: output.PaymentCredentialKind,
						PaymentCredentialHash: cloneHash28(output.PaymentCredentialHash),
					},
				}
			}
		}
	}
	consumed := make(map[OutputRef]struct{})
	ret := make([]model.Block, len(items))
	for blockIndex, item := range items {
		block := item.Block
		block.Transactions = append([]model.Transaction(nil), item.Block.Transactions...)
		for transactionIndex := range block.Transactions {
			transaction := &block.Transactions[transactionIndex]
			transaction.Inputs = append([]model.Input(nil), transaction.Inputs...)
			transaction.Redeemers = append([]model.Redeemer(nil), transaction.Redeemers...)
			resolved := make(map[OutputRef]ResolvedOutput, len(transaction.Inputs))
			consumedByTransaction := make(map[OutputRef]struct{})
			for inputIndex := range transaction.Inputs {
				input := &transaction.Inputs[inputIndex]
				ref := OutputRef{Hash: input.SourceHash, Index: input.SourceIndex}
				if _, alreadyConsumed := consumed[ref]; alreadyConsumed {
					return nil, fmt.Errorf(
						"batch output %x#%d appears after an earlier consumption",
						ref.Hash,
						ref.Index,
					)
				}
				if input.Consumed {
					if _, duplicate := consumedByTransaction[ref]; duplicate {
						return nil, fmt.Errorf(
							"batch output %x#%d has more than one consumption effect in one transaction",
							ref.Hash,
							ref.Index,
						)
					}
					consumedByTransaction[ref] = struct{}{}
				}
				resolution, found := prior[ref]
				if !found {
					return nil, fmt.Errorf(
						"backend omitted output state for %x#%d",
						ref.Hash,
						ref.Index,
					)
				}
				staged, sameBatch := produced[ref]
				stagedBefore := sameBatch &&
					(staged.block < blockIndex ||
						(staged.block == blockIndex &&
							staged.transaction < transactionIndex))
				if sameBatch && !stagedBefore {
					return nil, fmt.Errorf(
						"output %x#%d is produced at the same or a future batch position",
						ref.Hash,
						ref.Index,
					)
				}
				if resolution.State == OutputActiveUnspent && stagedBefore {
					return nil, fmt.Errorf(
						"output %x#%d exists in both the active snapshot and staged prefix",
						ref.Hash,
						ref.Index,
					)
				}
				switch {
				case stagedBefore && !resolution.ActiveConsumption:
					input.SourceResolved = true
					resolved[ref] = staged.output
				case resolution.State == OutputActiveUnspent:
					if !resolution.OutputFactSeen || resolution.ActiveConsumption {
						return nil, fmt.Errorf(
							"backend returned inconsistent active output state for %x#%d",
							ref.Hash,
							ref.Index,
						)
					}
					input.SourceResolved = true
					resolved[ref] = resolution.Output
				case resolution.State == OutputNeverSeen:
					if resolution.OutputFactSeen || resolution.ActiveConsumption {
						return nil, fmt.Errorf(
							"backend returned inconsistent never-seen output state for %x#%d",
							ref.Hash,
							ref.Index,
						)
					}
					input.SourceResolved = false
				case resolution.State == OutputKnownInactive:
					if !resolution.OutputFactSeen && !resolution.ActiveConsumption {
						return nil, fmt.Errorf(
							"backend returned unevidenced inactive output state for %x#%d",
							ref.Hash,
							ref.Index,
						)
					}
					return nil, fmt.Errorf(
						"output %x#%d is known inactive",
						ref.Hash,
						ref.Index,
					)
				default:
					return nil, fmt.Errorf(
						"backend returned unknown output state %q for %x#%d",
						resolution.State,
						ref.Hash,
						ref.Index,
					)
				}
			}
			if err := resolveTransactionFacts(transaction, resolved); err != nil {
				return nil, fmt.Errorf(
					"resolve block %d transaction %d facts: %w",
					blockIndex,
					transactionIndex,
					err,
				)
			}
			for ref := range consumedByTransaction {
				consumed[ref] = struct{}{}
			}
		}
		ret[blockIndex] = block
	}
	return ret, nil
}

func resolveTransactionFacts(
	transaction *model.Transaction,
	resolved map[OutputRef]ResolvedOutput,
) error {
	if transaction.FlowKind == "collateral" {
		var (
			collateralTotal uint64
			collateralCount int
			allResolved     = true
		)
		for _, input := range transaction.Inputs {
			if input.Role != "collateral" {
				continue
			}
			collateralCount++
			source, ok := resolved[OutputRef{Hash: input.SourceHash, Index: input.SourceIndex}]
			if !ok {
				allResolved = false
				continue
			}
			if math.MaxUint64-collateralTotal < source.Lovelace {
				return errors.New("resolved collateral lovelace sum overflows UInt64")
			}
			collateralTotal += source.Lovelace
		}
		if collateralCount > 0 && allResolved {
			var collateralReturn uint64
			var collateralReturnSeen bool
			for _, output := range transaction.Outputs {
				if output.Kind != "collateral_return" {
					continue
				}
				if collateralReturnSeen {
					return errors.New("multiple collateral-return outputs")
				}
				collateralReturnSeen = true
				collateralReturn = output.Lovelace
			}
			if collateralReturn > collateralTotal {
				return fmt.Errorf(
					"collateral return %d exceeds resolved collateral inputs %d",
					collateralReturn,
					collateralTotal,
				)
			}
			derived := collateralTotal - collateralReturn
			if derived == 0 {
				return errors.New("resolved effective collateral fee is zero")
			}
			if transaction.EffectiveFee != nil && *transaction.EffectiveFee != derived {
				return fmt.Errorf(
					"declared total collateral %d differs from resolved effective fee %d",
					*transaction.EffectiveFee,
					derived,
				)
			}
			transaction.EffectiveFee = &derived
		}
	}
	for index := range transaction.Redeemers {
		redeemer := &transaction.Redeemers[index]
		if redeemer.Purpose != "spend" {
			continue
		}
		if redeemer.TargetTxHash == nil || redeemer.TargetOutputIndex == nil {
			return errors.New("spend redeemer target is unresolved")
		}
		ref := OutputRef{Hash: *redeemer.TargetTxHash, Index: *redeemer.TargetOutputIndex}
		source, ok := resolved[ref]
		if !ok {
			redeemer.ResolvedScriptHash = nil
			continue
		}
		switch source.PaymentCredentialKind {
		case "script":
			if source.PaymentCredentialHash == nil {
				return fmt.Errorf("resolved script output %x#%d lacks credential hash", ref.Hash, ref.Index)
			}
			if redeemer.ResolvedScriptHash != nil &&
				*redeemer.ResolvedScriptHash != *source.PaymentCredentialHash {
				return fmt.Errorf("spend redeemer script hash disagrees with output %x#%d", ref.Hash, ref.Index)
			}
			redeemer.ResolvedScriptHash = cloneHash28(source.PaymentCredentialHash)
		case "key", "none":
			if redeemer.ResolvedScriptHash != nil {
				return fmt.Errorf("non-script output %x#%d has a spend script hash", ref.Hash, ref.Index)
			}
		default:
			return fmt.Errorf(
				"resolved output %x#%d has unknown payment credential kind %q",
				ref.Hash,
				ref.Index,
				source.PaymentCredentialKind,
			)
		}
	}
	return nil
}

func cloneHash28(value *model.Hash28) *model.Hash28 {
	if value == nil {
		return nil
	}
	ret := *value
	return &ret
}

func (coordinator *Coordinator) precheckBatchDatumBodies(
	ctx context.Context,
	blocks []model.Block,
) ([][]model.DatumBody, error) {
	unique := make(map[model.Hash32]model.DatumBody)
	for _, block := range blocks {
		for _, body := range block.Datums {
			calculated := blake2b.Sum256(body.CBOR)
			if calculated != body.Hash {
				return nil, fmt.Errorf("datum body %x does not match its blake2b-256 hash", body.Hash)
			}
			if previous, ok := unique[body.Hash]; ok {
				if !bytes.Equal(previous.CBOR, body.CBOR) {
					return nil, fmt.Errorf("conflicting datum bodies for hash %x in physical microbatch", body.Hash)
				}
				continue
			}
			unique[body.Hash] = body
		}
	}
	hashes := make([]model.Hash32, 0, len(unique))
	for hash := range unique {
		hashes = append(hashes, hash)
	}
	sort.Slice(hashes, func(left, right int) bool {
		return bytes.Compare(hashes[left][:], hashes[right][:]) < 0
	})
	existing, err := coordinator.backend.ExistingDatumBodies(ctx, hashes)
	if err != nil {
		return nil, fmt.Errorf("precheck existing datum bodies: %w", err)
	}
	ret := make([][]model.DatumBody, len(blocks))
	assigned := make(map[model.Hash32]struct{}, len(unique))
	for blockIndex, block := range blocks {
		seenInBlock := make(map[model.Hash32]struct{}, len(block.Datums))
		for _, body := range block.Datums {
			if _, duplicate := seenInBlock[body.Hash]; duplicate {
				continue
			}
			seenInBlock[body.Hash] = struct{}{}
			if stored, ok := existing[body.Hash]; ok {
				if !bytes.Equal(stored, body.CBOR) {
					return nil, fmt.Errorf("stored datum body conflicts for hash %x", body.Hash)
				}
				continue
			}
			if _, alreadyAssigned := assigned[body.Hash]; alreadyAssigned {
				continue
			}
			assigned[body.Hash] = struct{}{}
			ret[blockIndex] = append(ret[blockIndex], body)
		}
		sort.Slice(ret[blockIndex], func(left, right int) bool {
			return bytes.Compare(
				ret[blockIndex][left].Hash[:],
				ret[blockIndex][right].Hash[:],
			) < 0
		})
	}
	return ret, nil
}

func countFacts(attempt Attempt) (Counts, error) {
	counts := Counts{
		Blocks:      1,
		DatumBodies: uint64(len(attempt.NewDatumBodies)),
	}
	for _, transaction := range attempt.Block.Transactions {
		counts.Transactions++
		counts.Inputs += uint64(len(transaction.Inputs))
		counts.Outputs += uint64(len(transaction.Outputs))
		counts.Withdrawals += uint64(len(transaction.Withdrawals))
		counts.Redeemers += uint64(len(transaction.Redeemers))
		counts.DatumObservations += uint64(len(transaction.DatumObservations))
		if transaction.Metadata != nil {
			counts.Metadata++
		}
	}
	for _, value := range []uint64{
		counts.Transactions,
		counts.Inputs,
		counts.Outputs,
		counts.DatumObservations,
		counts.Withdrawals,
		counts.Redeemers,
		counts.Metadata,
	} {
		if value > math.MaxUint32 {
			return Counts{}, errors.New("per-block fact count exceeds UInt32")
		}
	}
	return counts, nil
}

func digestFacts(attempt Attempt) (model.Hash32, error) {
	payload := struct {
		Block  model.Block
		Source Source
	}{
		Block:  attempt.Block,
		Source: attempt.Source,
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return model.Hash32{}, fmt.Errorf("encode fact digest: %w", err)
	}
	return sha256.Sum256(encoded), nil
}

func FactsDigest(block model.Block, source Source) (model.Hash32, error) {
	return digestFacts(Attempt{Block: block, Source: source})
}

func pointFromBlock(block model.Block) Point {
	return Point{
		Slot:        block.Slot,
		Hash:        block.Hash,
		BlockNumber: block.Number,
		IsByronEBB:  isByronEpochBoundaryBlock(block),
	}
}

func samePoint(left, right Point) bool {
	if left.Origin || right.Origin {
		return left.Origin == right.Origin
	}
	return left.Slot == right.Slot &&
		left.Hash == right.Hash &&
		left.BlockNumber == right.BlockNumber &&
		left.IsByronEBB == right.IsByronEBB
}

func validatePoint(point Point) error {
	if point.Origin {
		if point.Slot != 0 ||
			point.Hash != (model.Hash32{}) ||
			point.BlockNumber != 0 ||
			point.IsByronEBB {
			return errors.New("Origin point carries non-Origin metadata")
		}
		return nil
	}
	if point.Hash == (model.Hash32{}) {
		return errors.New("non-Origin point has a zero hash")
	}
	return nil
}

func validateBatchSuccessor(previous, current model.Block) error {
	if current.ParentHash == nil || *current.ParentHash != previous.Hash {
		return errors.New("parent hash does not extend the preceding block")
	}
	currentEBB := isByronEpochBoundaryBlock(current)
	previousEBB := isByronEpochBoundaryBlock(previous)
	switch {
	case currentEBB:
		if current.Number != previous.Number {
			return fmt.Errorf(
				"Byron EBB block number %d must equal predecessor %d",
				current.Number,
				previous.Number,
			)
		}
		if current.Slot <= previous.Slot {
			return errors.New("Byron EBB slot must be later than its predecessor")
		}
		return nil
	default:
		if previous.Number == math.MaxUint64 || current.Number != previous.Number+1 {
			return fmt.Errorf(
				"block number %d does not legally follow %d",
				current.Number,
				previous.Number,
			)
		}
		if previousEBB {
			if current.Slot < previous.Slot {
				return errors.New("post-EBB slot precedes the EBB")
			}
		} else if current.Slot <= previous.Slot {
			return errors.New("regular block slot must be later than its predecessor")
		}
		return nil
	}
}

func isByronEpochBoundaryBlock(block model.Block) bool {
	return block.Era == "Byron" &&
		block.Type == int16(ledger.BlockTypeByronEbb)
}

func (coordinator *Coordinator) assertWriter() error {
	if err := coordinator.lock.AssertHeld(); err != nil {
		return fmt.Errorf("writer gate lost: %w", err)
	}
	return nil
}

func (coordinator *Coordinator) inject(point FaultPoint) error {
	if coordinator.config.Fault == nil {
		return nil
	}
	if err := coordinator.config.Fault(point); err != nil {
		return fmt.Errorf("injected failure at %s: %w", point, err)
	}
	return nil
}

func randomID() ([16]byte, error) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return id, fmt.Errorf("generate rollback ID: %w", err)
	}
	// RFC 4122 v4 layout, solely for ClickHouse UUID compatibility.
	id[6] = (id[6] & 0x0f) | 0x40
	id[8] = (id[8] & 0x3f) | 0x80
	return id, nil
}

func independentObservers(values []RollbackObserver) ([]string, []string, int, error) {
	operators := make(map[string]struct{}, len(values))
	peers := make(map[string]struct{}, len(values))
	unique := make([]RollbackObserver, 0, len(values))
	for _, value := range values {
		if value.Peer == "" || value.Operator == "" {
			return nil, nil, 0, errors.New("rollback observers require non-empty peer and operator identities")
		}
		if _, duplicatePeer := peers[value.Peer]; duplicatePeer {
			continue
		}
		peers[value.Peer] = struct{}{}
		operators[value.Operator] = struct{}{}
		unique = append(unique, value)
	}
	sort.Slice(unique, func(i, j int) bool {
		if unique[i].Operator == unique[j].Operator {
			return unique[i].Peer < unique[j].Peer
		}
		return unique[i].Operator < unique[j].Operator
	})
	peerList := make([]string, 0, len(unique))
	operatorList := make([]string, 0, len(unique))
	for _, observer := range unique {
		peerList = append(peerList, observer.Peer)
		operatorList = append(operatorList, observer.Operator)
	}
	return peerList, operatorList, len(operators), nil
}
