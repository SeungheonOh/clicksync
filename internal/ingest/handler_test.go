package ingest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/protocol/chainsync"
	pcommon "github.com/blinklabs-io/gouroboros/protocol/common"
	utxorpc "github.com/utxorpc/go-codegen/utxorpc/v1alpha/cardano"

	"clicksync/internal/n2n"
	"clicksync/internal/publication"
	"clicksync/internal/syncer"
)

type fakePublisher struct {
	batches   []publication.Batch
	rollbacks []publication.RollbackRequest
	publish   func(publication.Batch) (publication.BatchResult, error)
	rollback  func(publication.RollbackRequest) error
}

func (publisher *fakePublisher) PublishBatch(
	_ context.Context,
	batch publication.Batch,
) (publication.BatchResult, error) {
	publisher.batches = append(publisher.batches, batch)
	if publisher.publish != nil {
		return publisher.publish(batch)
	}
	last := batch.Items[len(batch.Items)-1].Block
	ids := make([]uint64, len(batch.Items))
	for index := range ids {
		ids[index] = uint64(index + 1)
	}
	return publication.BatchResult{
		PublicationIDs: ids,
		FirstEventSeq:  1,
		LastEventSeq:   uint64(len(ids)),
		LastCommitted: publication.Point{
			Slot:        last.Slot,
			Hash:        last.Hash,
			BlockNumber: last.Number,
			IsByronEBB: last.Era == "Byron" &&
				last.Type == int16(ledger.BlockTypeByronEbb),
		},
	}, nil
}

func (publisher *fakePublisher) Rollback(
	_ context.Context,
	request publication.RollbackRequest,
) error {
	publisher.rollbacks = append(publisher.rollbacks, request)
	if publisher.rollback != nil {
		return publisher.rollback(request)
	}
	return nil
}

type fakeChainState struct {
	tip publication.Point
}

func (*fakeChainState) CommittedSnapshot(context.Context) (uint64, error) {
	return 1, nil
}

func (state *fakeChainState) CommittedTip(
	context.Context,
	uint64,
) (publication.Point, error) {
	return state.tip, nil
}

func TestHandlerStagesThenFinalizesExactCommittedTail(t *testing.T) {
	publisher := &fakePublisher{}
	handler := newTestHandler(t, publisher)
	block := adapterTestBlock{slot: 10, number: 10, hash: adapterHash(0x10)}
	tip := adapterTip(block)
	outcome, err := handler.RollForward(
		context.Background(),
		block,
		tip,
		adapterEvidence(tip),
	)
	if err != nil {
		t.Fatal(err)
	}
	if !outcome.Accepted || outcome.Committed || len(publisher.batches) != 0 {
		t.Fatalf("staging outcome = %#v, batches=%d", outcome, len(publisher.batches))
	}
	outcome, err = handler.EndAttempt(
		context.Background(),
		syncer.AttemptEnd{Cause: "shutdown"},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !outcome.Committed ||
		outcome.CommittedBlocks != 1 ||
		outcome.LastCommittedPoint == nil ||
		outcome.LastCommittedPoint.BlockNumber != block.number ||
		outcome.LastCommittedTip == nil ||
		outcome.LastCommittedTip.BlockNumber != tip.BlockNumber {
		t.Fatalf("final outcome = %#v", outcome)
	}
	if len(publisher.batches) != 1 || len(publisher.batches[0].Items) != 1 {
		t.Fatalf("published batches = %#v", publisher.batches)
	}
}

func TestRollbackRetainsEligiblePrefixThenRecordsHeader(t *testing.T) {
	publisher := &fakePublisher{}
	handler := newTestHandler(t, publisher)
	var blocks []adapterTestBlock
	for number := uint64(10); number <= 12; number++ {
		block := adapterTestBlock{
			slot:   number,
			number: number,
			hash:   adapterHash(byte(number)),
		}
		blocks = append(blocks, block)
		tip := adapterTip(block)
		if _, err := handler.RollForward(
			context.Background(),
			block,
			tip,
			adapterEvidence(tip),
		); err != nil {
			t.Fatal(err)
		}
	}
	target := n2n.NewChainPoint(
		pcommon.NewPoint(blocks[0].slot, blocks[0].hash.Bytes()),
		blocks[0].number,
	)
	outcome, err := handler.RollBackward(
		context.Background(),
		target,
		adapterTip(blocks[2]),
		syncer.RollbackEvidence{
			Confirmations: adapterEvidence(adapterTip(blocks[2])).CheckpointMembers,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !outcome.Committed || outcome.CommittedBlocks != 1 {
		t.Fatalf("rollback outcome = %#v", outcome)
	}
	if len(publisher.batches) != 1 || len(publisher.batches[0].Items) != 1 {
		t.Fatalf("retained physical batch = %#v", publisher.batches)
	}
	if len(publisher.rollbacks) != 1 ||
		publisher.rollbacks[0].To.Hash != publicationPoint(target).Hash {
		t.Fatalf("rollback requests = %#v", publisher.rollbacks)
	}
}

func TestCommittedPublicationErrorIsReportedOnlyByFinalizer(t *testing.T) {
	publisher := &fakePublisher{
		publish: func(batch publication.Batch) (publication.BatchResult, error) {
			last := batch.Items[len(batch.Items)-1].Block
			return publication.BatchResult{
					PublicationIDs: []uint64{7},
					FirstEventSeq:  9,
					LastEventSeq:   9,
					LastCommitted: publication.Point{
						Slot:        last.Slot,
						Hash:        last.Hash,
						BlockNumber: last.Number,
					},
				}, &publication.CommittedError{
					PublicationID: 7,
					EventSeq:      9,
					Err:           errors.New("manifest cache"),
				}
		},
	}
	handler := newTestHandler(t, publisher)
	block := adapterTestBlock{slot: 10, number: 10, hash: adapterHash(0x10)}
	tip := adapterTip(block)
	if _, err := handler.RollForward(
		context.Background(),
		block,
		tip,
		adapterEvidence(tip),
	); err != nil {
		t.Fatal(err)
	}
	outcome, err := handler.EndAttempt(
		context.Background(),
		syncer.AttemptEnd{Cause: "shutdown"},
	)
	if err == nil || !outcome.Committed || outcome.CommittedBlocks != 1 {
		t.Fatalf("outcome=%#v error=%v", outcome, err)
	}
}

func TestHandlerRejectsPartialIdentitySetAndMismatchedEBBTail(t *testing.T) {
	for name, publish := range map[string]func(publication.Batch) (publication.BatchResult, error){
		"partial identities": func(batch publication.Batch) (publication.BatchResult, error) {
			last := batch.Items[0].Block
			return publication.BatchResult{
				PublicationIDs: []uint64{1, 2},
				FirstEventSeq:  1,
				LastEventSeq:   2,
				LastCommitted: publication.Point{
					Slot:        last.Slot,
					Hash:        last.Hash,
					BlockNumber: last.Number,
				},
			}, nil
		},
		"mismatched EBB type": func(batch publication.Batch) (publication.BatchResult, error) {
			last := batch.Items[0].Block
			return publication.BatchResult{
				PublicationIDs: []uint64{1},
				FirstEventSeq:  1,
				LastEventSeq:   1,
				LastCommitted: publication.Point{
					Slot:        last.Slot,
					Hash:        last.Hash,
					BlockNumber: last.Number,
					IsByronEBB:  false,
				},
			}, nil
		},
		"arbitrary error with full identities": func(batch publication.Batch) (publication.BatchResult, error) {
			last := batch.Items[0].Block
			return publication.BatchResult{
				PublicationIDs: []uint64{1},
				FirstEventSeq:  1,
				LastEventSeq:   1,
				LastCommitted: publication.Point{
					Slot:        last.Slot,
					Hash:        last.Hash,
					BlockNumber: last.Number,
					IsByronEBB:  true,
				},
			}, errors.New("untyped insert failure")
		},
	} {
		t.Run(name, func(t *testing.T) {
			publisher := &fakePublisher{publish: publish}
			handler := newTestHandler(t, publisher)
			block := adapterTestBlock{
				slot:   21_600,
				number: 20_000,
				hash:   adapterHash(0xee),
				era:    "Byron",
				kind:   int(ledger.BlockTypeByronEbb),
			}
			tip := adapterTip(block)
			if _, err := handler.RollForward(
				context.Background(),
				block,
				tip,
				adapterEvidence(tip),
			); err != nil {
				t.Fatal(err)
			}
			outcome, err := handler.EndAttempt(
				context.Background(),
				syncer.AttemptEnd{Cause: "shutdown"},
			)
			if err == nil || outcome.Committed {
				t.Fatalf("outcome=%#v error=%v", outcome, err)
			}
		})
	}
}

func TestSizeTriggeredCommittedPrefixDoesNotAcknowledgeCurrentBlock(t *testing.T) {
	publisher := &fakePublisher{
		publish: func(batch publication.Batch) (publication.BatchResult, error) {
			last := batch.Items[len(batch.Items)-1].Block
			return publication.BatchResult{
					PublicationIDs: []uint64{7},
					FirstEventSeq:  9,
					LastEventSeq:   9,
					LastCommitted: publication.Point{
						Slot:        last.Slot,
						Hash:        last.Hash,
						BlockNumber: last.Number,
					},
				}, &publication.CommittedError{
					PublicationID: 7,
					EventSeq:      9,
					Err:           errors.New("manifest cache"),
				}
		},
	}
	handler := newTestHandler(t, publisher)
	first := adapterTestBlock{slot: 10, number: 10, hash: adapterHash(0x10)}
	firstTip := adapterTip(first)
	if _, err := handler.RollForward(
		context.Background(),
		first,
		firstTip,
		adapterEvidence(firstTip),
	); err != nil {
		t.Fatal(err)
	}
	handler.mu.Lock()
	handler.pendingRows = publication.MaxBatchRows
	handler.mu.Unlock()
	current := adapterTestBlock{slot: 11, number: 11, hash: adapterHash(0x11)}
	outcome, err := handler.RollForward(
		context.Background(),
		current,
		adapterTip(current),
		adapterEvidence(adapterTip(current)),
	)
	if err == nil ||
		outcome.Accepted ||
		!outcome.Committed ||
		outcome.CommittedBlocks != 1 ||
		outcome.LastCommittedPoint == nil ||
		outcome.LastCommittedPoint.BlockNumber != first.number {
		t.Fatalf("outcome=%#v error=%v", outcome, err)
	}
}

func TestNormalizationFailureFinalizesSafePendingPrefix(t *testing.T) {
	publisher := &fakePublisher{}
	handler := newTestHandler(t, publisher)
	first := adapterTestBlock{slot: 10, number: 10, hash: adapterHash(0x10)}
	tip := adapterTip(first)
	if _, err := handler.RollForward(
		context.Background(),
		first,
		tip,
		adapterEvidence(tip),
	); err != nil {
		t.Fatal(err)
	}
	outcome, err := handler.RollForward(
		context.Background(),
		nil,
		tip,
		adapterEvidence(tip),
	)
	if err == nil ||
		outcome.Accepted ||
		!outcome.Committed ||
		outcome.CommittedBlocks != 1 ||
		outcome.LastCommittedPoint == nil ||
		outcome.LastCommittedPoint.BlockNumber != first.number {
		t.Fatalf("outcome=%#v error=%v", outcome, err)
	}
}

func TestRollbackReportsRetainedAdoptionWhenHeaderFailsPrecommit(t *testing.T) {
	publisher := &fakePublisher{
		rollback: func(publication.RollbackRequest) error {
			return errors.New("rollback header precommit failure")
		},
	}
	handler := newTestHandler(t, publisher)
	block := adapterTestBlock{slot: 10, number: 10, hash: adapterHash(0x10)}
	tip := adapterTip(block)
	if _, err := handler.RollForward(
		context.Background(),
		block,
		tip,
		adapterEvidence(tip),
	); err != nil {
		t.Fatal(err)
	}
	target := n2n.NewChainPoint(
		pcommon.NewPoint(block.slot, block.hash.Bytes()),
		block.number,
	)
	outcome, err := handler.RollBackward(
		context.Background(),
		target,
		tip,
		syncer.RollbackEvidence{
			Confirmations: adapterEvidence(tip).CheckpointMembers,
		},
	)
	if err == nil ||
		!outcome.Committed ||
		outcome.CommittedBlocks != 1 ||
		outcome.LastCommittedPoint == nil ||
		!samePublicationPoint(publicationPoint(*outcome.LastCommittedPoint), publicationPoint(target)) {
		t.Fatalf("outcome=%#v error=%v", outcome, err)
	}
	if len(publisher.rollbacks) != 1 {
		t.Fatalf("rollback calls = %d, want 1", len(publisher.rollbacks))
	}
}

func newTestHandler(t *testing.T, publisher *fakePublisher) *Handler {
	t.Helper()
	handler, err := NewHandler(
		context.Background(),
		publisher,
		&fakeChainState{},
		HandlerConfig{
			NetworkMagic:          n2n.MainnetNetworkMagic,
			RollbackMaximumDepth:  100,
			RollbackCorroboration: 2,
			FlushAfter:            500 * time.Millisecond,
			Now:                   time.Now,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	return handler
}

func adapterEvidence(tip chainsync.Tip) syncer.SourceEvidence {
	return syncer.SourceEvidence{
		Primary: syncer.PeerEvidence{
			Peer: n2n.Peer{
				Host:       "relay-a:3001",
				Address:    "192.0.2.1:3001",
				Operator:   "operator-a",
				N2NVersion: 15,
			},
			Tip:        tip,
			N2NVersion: 15,
		},
		CheckpointMembers: []syncer.PeerEvidence{
			{Peer: n2n.Peer{Host: "relay-a:3001", Operator: "operator-a"}},
			{Peer: n2n.Peer{Host: "relay-b:3001", Operator: "operator-b"}},
		},
	}
}

func adapterTip(block adapterTestBlock) chainsync.Tip {
	return chainsync.Tip{
		Point:       pcommon.NewPoint(block.slot, block.hash.Bytes()),
		BlockNumber: block.number,
	}
}

func adapterHash(value byte) lcommon.Blake2b256 {
	var ret lcommon.Blake2b256
	for index := range ret {
		ret[index] = value
	}
	return ret
}

type adapterTestBlock struct {
	slot   uint64
	number uint64
	hash   lcommon.Blake2b256
	era    string
	kind   int
}

func (block adapterTestBlock) Hash() lcommon.Blake2b256     { return block.hash }
func (block adapterTestBlock) PrevHash() lcommon.Blake2b256 { return adapterHash(0x01) }
func (block adapterTestBlock) BlockNumber() uint64          { return block.number }
func (block adapterTestBlock) SlotNumber() uint64           { return block.slot }
func (adapterTestBlock) IssuerVkey() lcommon.IssuerVkey     { return lcommon.IssuerVkey{} }
func (adapterTestBlock) BlockBodySize() uint64              { return 0 }
func (block adapterTestBlock) Era() lcommon.Era {
	if block.era == "" {
		return lcommon.Era{Name: "Conway"}
	}
	return lcommon.Era{Name: block.era}
}
func (adapterTestBlock) Cbor() []byte                      { return nil }
func (adapterTestBlock) BlockBodyHash() lcommon.Blake2b256 { return lcommon.Blake2b256{} }
func (block adapterTestBlock) Header() lcommon.BlockHeader { return block }
func (block adapterTestBlock) Type() int {
	return block.kind
}
func (adapterTestBlock) Transactions() []lcommon.Transaction { return nil }
func (adapterTestBlock) Utxorpc() (*utxorpc.Block, error)    { return nil, nil }
