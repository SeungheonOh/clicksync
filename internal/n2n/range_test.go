package n2n

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/protocol/blockfetch"
	"github.com/blinklabs-io/gouroboros/protocol/chainsync"
	pcommon "github.com/blinklabs-io/gouroboros/protocol/common"
	utxorpc "github.com/utxorpc/go-codegen/utxorpc/v1alpha/cardano"
)

func TestHeaderWindowUsesBoundedInclusiveRanges(t *testing.T) {
	const (
		blocks = 100
		limit  = 32
	)
	tip := chainsync.Tip{Point: rangePoint(blocks), BlockNumber: blocks}
	pending := 0
	var batchSizes []int
	for number := 1; number <= blocks; number++ {
		pending++
		if shouldFlushHeaderRange(pending, limit, rangePoint(number), tip) {
			batchSizes = append(batchSizes, pending)
			pending = 0
		}
	}
	want := []int{32, 32, 32, 4}
	if len(batchSizes) != len(want) {
		t.Fatalf("batch sizes = %#v", batchSizes)
	}
	for index := range want {
		if batchSizes[index] != want[index] {
			t.Fatalf("batch sizes = %#v, want %#v", batchSizes, want)
		}
	}
	if ratio := float64(len(batchSizes)) / blocks; ratio > 0.04 {
		t.Fatalf("BlockFetch request/block ratio = %.4f, want <= 0.04", ratio)
	}
	if !shouldFlushHeaderRange(1, limit, tip.Point, tip) {
		t.Fatal("single live-tip block did not flush immediately")
	}
}

func TestRunPeerRejectsWrongNonzeroNetworkMagicBeforeDial(t *testing.T) {
	err := RunPeer(
		context.Background(),
		"127.0.0.1:1",
		DialConfig{
			NetworkMagic:    1,
			QueueCapacity:   4,
			HeaderBatchSize: 32,
			DialTimeout:     time.Second,
			BlockTimeout:    time.Second,
		},
		[]ChainPoint{NewChainPointOrigin()},
		&blockingRollbackHandler{},
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	if err == nil || !strings.Contains(err.Error(), "mainnet magic") {
		t.Fatalf("wrong-magic RunPeer error = %v", err)
	}
}

func TestRangeMappingRejectsWrongBodyPointAndHeight(t *testing.T) {
	first := rangeBlock(10, 100, 0x10)
	second := rangeBlock(11, 101, 0x11)
	expected := []expectedHeader{
		{
			header: first,
			point:  pcommon.NewPoint(first.SlotNumber(), first.Hash().Bytes()),
		},
		{
			header: second,
			point:  pcommon.NewPoint(second.SlotNumber(), second.Hash().Bytes()),
		},
	}
	if _, err := matchRangeBlock(expected, 0, first); err != nil {
		t.Fatal(err)
	}
	if _, err := matchRangeBlock(expected, 1, second); err != nil {
		t.Fatal(err)
	}
	if _, err := matchRangeBlock(expected, 0, second); err == nil ||
		!strings.Contains(err.Error(), "slot mismatch") {
		t.Fatalf("reordered body error = %v", err)
	}
	wrongHeight := second
	wrongHeight.number++
	if _, err := matchRangeBlock(expected, 1, wrongHeight); err == nil ||
		!strings.Contains(err.Error(), "height mismatch") {
		t.Fatalf("wrong-height error = %v", err)
	}
}

func TestRollbackDiscardsPendingUnfetchedHeaders(t *testing.T) {
	pending := []expectedHeader{
		{point: rangePoint(10)},
		{point: rangePoint(11)},
	}
	connection := &connection{
		pendingHeaders: pending,
	}
	rollback := rangePoint(8)
	if err := connection.resetHeaderChain(NewChainPoint(rollback, 8)); err != nil {
		t.Fatal(err)
	}
	if len(connection.pendingHeaders) != 0 {
		t.Fatalf("pending headers survived rollback: %#v", connection.pendingHeaders)
	}
	if connection.expectedParent == nil ||
		!pointsEqual(connection.expectedParent.Point, rollback) ||
		connection.expectedParent.BlockNumber != 8 {
		t.Fatalf("rollback expected parent = %#v", connection.expectedParent)
	}
}

func TestRollbackTargetExactFetchDerivesHeight(t *testing.T) {
	block := rangeBlock(20, 777, 0x20)
	point := pcommon.NewPoint(block.SlotNumber(), block.Hash().Bytes())
	connection := &connection{
		fetchSingleBlock: func(got pcommon.Point) (lcommon.Block, error) {
			if !pointsEqual(got, point) {
				t.Fatalf("rollback fetch point = %#v", got)
			}
			return block, nil
		},
	}
	parent, err := connection.resolveRollbackParent(point)
	if err != nil {
		t.Fatal(err)
	}
	if !pointsEqual(parent.Point, point) || parent.BlockNumber != 777 {
		t.Fatalf("rollback parent = %#v", parent)
	}
	if parent.IsByronEBB {
		t.Fatal("regular rollback target marked as Byron EBB")
	}
}

func TestRollbackTargetExactFetchDerivesByronEBB(t *testing.T) {
	block := pinnedByronEBB(t)
	point := pcommon.NewPoint(block.SlotNumber(), block.Hash().Bytes())
	connection := &connection{
		fetchSingleBlock: func(got pcommon.Point) (lcommon.Block, error) {
			if !pointsEqual(got, point) {
				t.Fatalf("rollback fetch point = %#v", got)
			}
			return block, nil
		},
	}
	parent, err := connection.resolveRollbackParent(point)
	if err != nil {
		t.Fatal(err)
	}
	if !pointsEqual(parent.Point, point) ||
		parent.BlockNumber != block.BlockNumber() ||
		!parent.IsByronEBB {
		t.Fatalf("Byron EBB rollback parent = %#v", parent)
	}
}

func TestOriginRollbackDoesNotFetchBody(t *testing.T) {
	fetches := 0
	connection := &connection{
		fetchSingleBlock: func(pcommon.Point) (lcommon.Block, error) {
			fetches++
			return nil, errors.New("unexpected")
		},
	}
	parent, err := connection.resolveRollbackParent(pcommon.NewPointOrigin())
	if err != nil {
		t.Fatal(err)
	}
	if !isOrigin(parent.Point) || fetches != 0 {
		t.Fatalf("Origin rollback parent = %#v, fetches=%d", parent, fetches)
	}
}

func TestRollbackTargetRejectsMismatchAndUnavailable(t *testing.T) {
	requested := rangePoint(20)
	mismatch := rangeBlock(21, 777, 0x21)
	connection := &connection{
		fetchSingleBlock: func(pcommon.Point) (lcommon.Block, error) {
			return mismatch, nil
		},
	}
	_, err := connection.resolveRollbackParent(requested)
	var violation *PeerDataViolation
	if !errors.As(err, &violation) ||
		violation.Kind != "rollback_point_mismatch" {
		t.Fatalf("rollback mismatch error = %T %v", err, err)
	}

	noBlocks := errors.New("synthetic NoBlocks")
	connection.fetchSingleBlock = func(pcommon.Point) (lcommon.Block, error) {
		return nil, noBlocks
	}
	_, err = connection.resolveRollbackParent(requested)
	var unavailable *RangeUnavailable
	if !errors.As(err, &unavailable) ||
		!pointsEqual(unavailable.Start, requested) ||
		!pointsEqual(unavailable.End, requested) {
		t.Fatalf("rollback unavailable error = %T %v", err, err)
	}
}

func TestRollbackCallbackWaitsForHandlerBeforeNextHeaderCanAdvance(t *testing.T) {
	block := rangeBlock(20, 777, 0x20)
	point := pcommon.NewPoint(block.SlotNumber(), block.Hash().Bytes())
	connection := &connection{
		events: make(chan chainEvent, 1),
		fetchSingleBlock: func(pcommon.Point) (lcommon.Block, error) {
			return block, nil
		},
	}
	handler := &blockingRollbackHandler{
		started: make(chan struct{}),
		release: make(chan struct{}),
		target:  make(chan ChainPoint, 1),
	}
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(context.Canceled)
	processDone := make(chan error, 1)
	go func() {
		processDone <- connection.process(ctx, handler)
	}()
	callbackDone := make(chan error, 1)
	go func() {
		callbackDone <- connection.onRollBackward(
			chainsync.CallbackContext{},
			point,
			chainsync.Tip{Point: rangePoint(30), BlockNumber: 900},
		)
	}()
	<-handler.started
	target := <-handler.target
	if !pointsEqual(target.Point, point) ||
		target.BlockNumber != block.BlockNumber() ||
		target.IsByronEBB {
		t.Fatalf("rollback target metadata = %#v", target)
	}
	select {
	case err := <-callbackDone:
		t.Fatalf("rollback callback advanced before handler commit: %v", err)
	default:
	}
	close(handler.release)
	if err := <-callbackDone; err != nil {
		t.Fatal(err)
	}
	cancel(context.Canceled)
	if err := <-processDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("process stop = %v", err)
	}
}

func TestFlushHeaderRangeCompletesOrderedCallbackState(t *testing.T) {
	first := rangeBlock(10, 100, 0x10)
	second := rangeBlock(11, 101, 0x11)
	connection := rangeTestConnection(first, second)
	connection.requestBlockRange = func(start, end pcommon.Point) error {
		if !pointsEqual(start, connection.pendingRangePoint(first)) {
			t.Fatalf("range start = %#v", start)
		}
		if !pointsEqual(end, connection.pendingRangePoint(second)) {
			t.Fatalf("range end = %#v", end)
		}
		if err := connection.onBlock(blockfetch.CallbackContext{}, 0, first); err != nil {
			return err
		}
		if err := connection.onBlock(blockfetch.CallbackContext{}, 1, second); err != nil {
			return err
		}
		return connection.onBatchDone(blockfetch.CallbackContext{})
	}

	if err := connection.flushHeaderRange(); err != nil {
		t.Fatal(err)
	}
	if connection.activeFetch != nil {
		t.Fatal("completed range left active state")
	}
	if connection.lastHeader == nil ||
		connection.lastHeader.header.Hash() != second.Hash() {
		t.Fatalf("last verified range header = %#v", connection.lastHeader)
	}
	third := rangeBlock(12, 102, 0x12)
	third.prev = second.Hash()
	if err := validateHeaderContinuity(
		nil,
		connection.lastHeader,
		third,
		pcommon.NewPoint(third.SlotNumber(), third.Hash().Bytes()),
	); err != nil {
		t.Fatalf("cross-range continuity: %v", err)
	}
	if connection.rangeRequests != 1 || connection.rangeBlocks != 2 {
		t.Fatalf(
			"range counters = requests:%d blocks:%d",
			connection.rangeRequests,
			connection.rangeBlocks,
		)
	}
	for index, want := range []rangeTestBlock{first, second} {
		event := <-connection.events
		if event.block == nil || event.block.Hash() != want.Hash() {
			t.Fatalf("event %d block = %#v, want %s", index, event.block, want.Hash())
		}
	}
}

func TestRangeBatchDoneRejectsNoBlocksAndShortBatch(t *testing.T) {
	for _, returned := range []int{0, 1} {
		t.Run(string(rune('0'+returned))+"_blocks", func(t *testing.T) {
			first := rangeBlock(10, 100, 0x10)
			second := rangeBlock(11, 101, 0x11)
			connection := rangeTestConnection(first, second)
			state := &rangeFetch{
				expected: append([]expectedHeader(nil), connection.pendingHeaders...),
				next:     returned,
				done:     make(chan error, 1),
			}
			connection.pendingHeaders = nil
			connection.activeFetch = state

			err := connection.onBatchDone(blockfetch.CallbackContext{})
			if err == nil || !strings.Contains(err.Error(), "returned") {
				t.Fatalf("short batch error = %v", err)
			}
			if connection.activeFetch != nil {
				t.Fatal("failed batch left active range state")
			}
			if doneErr := <-state.done; !errors.Is(doneErr, err) {
				t.Fatalf("completion error = %v, callback error = %v", doneErr, err)
			}
		})
	}
}

func TestRangeRejectsExtraBlock(t *testing.T) {
	first := rangeBlock(10, 100, 0x10)
	connection := rangeTestConnection(first)
	state := &rangeFetch{
		expected: append([]expectedHeader(nil), connection.pendingHeaders...),
		next:     1,
		done:     make(chan error, 1),
	}
	connection.pendingHeaders = nil
	connection.activeFetch = state

	err := connection.onBlock(blockfetch.CallbackContext{}, 1, first)
	if err == nil || !strings.Contains(err.Error(), "more blocks") {
		t.Fatalf("extra block error = %v", err)
	}
	if doneErr := <-state.done; !errors.Is(doneErr, err) {
		t.Fatalf("completion error = %v, callback error = %v", doneErr, err)
	}
}

func TestFlushHeaderRangeCleansStateOnRequestFailure(t *testing.T) {
	first := rangeBlock(10, 100, 0x10)
	connection := rangeTestConnection(first)
	requestErr := errors.New("synthetic request failure")
	connection.requestBlockRange = func(pcommon.Point, pcommon.Point) error {
		return requestErr
	}

	err := connection.flushHeaderRange()
	if !errors.Is(err, requestErr) {
		t.Fatalf("request error = %v", err)
	}
	var unavailable *RangeUnavailable
	if !errors.As(err, &unavailable) {
		t.Fatalf("non-network range refusal classification = %T %v", err, err)
	}
	if connection.activeFetch != nil {
		t.Fatal("failed request left active range state")
	}
}

func TestFlushHeaderRangePreservesTypedNetworkFailure(t *testing.T) {
	first := rangeBlock(10, 100, 0x10)
	connection := rangeTestConnection(first)
	networkErr := &net.OpError{
		Op:  "read",
		Net: "tcp",
		Err: errors.New("connection reset"),
	}
	connection.requestBlockRange = func(pcommon.Point, pcommon.Point) error {
		return networkErr
	}

	err := connection.flushHeaderRange()
	if !errors.Is(err, networkErr) {
		t.Fatalf("network request error = %v", err)
	}
	var unavailable *RangeUnavailable
	if errors.As(err, &unavailable) {
		t.Fatalf("network failure was mislabeled range-unavailable: %v", err)
	}
	if connection.activeFetch != nil {
		t.Fatal("network failure left active range state")
	}
}

func TestRangeBlockFetchConfigurationKeepsBodyHashValidation(t *testing.T) {
	config, err := newBlockFetchConfig(
		DialConfig{
			QueueCapacity: 4,
			BlockTimeout:  time.Second,
		},
		nil,
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if config.SkipBlockValidation {
		t.Fatal("range BlockFetch disabled upstream block/body-hash validation")
	}

	raw := readCompressedFixture(t, "babbage.cbor.gz")
	var outer []cbor.RawMessage
	if _, err := cbor.Decode(raw, &outer); err != nil {
		t.Fatalf("decode fixture envelope: %v", err)
	}
	if len(outer) < 2 {
		t.Fatalf("fixture envelope has %d fields", len(outer))
	}
	needle := []byte{0x58, 0x20}
	offset := bytes.Index(outer[1], needle)
	if offset < 0 || offset+len(needle) >= len(outer[1]) {
		t.Fatal("fixture transaction bodies lack a 32-byte input hash")
	}
	outer[1][offset+len(needle)] ^= 0x01
	corrupt, err := cbor.Encode(outer)
	if err != nil {
		t.Fatalf("encode corrupted fixture: %v", err)
	}
	_, decodeErr := ledger.NewBlockFromCbor(
		ledger.BlockTypeBabbage,
		corrupt,
		lcommon.VerifyConfig{
			SkipBodyHashValidation: config.SkipBlockValidation,
		},
	)
	if decodeErr == nil ||
		!strings.Contains(strings.ToLower(decodeErr.Error()), "body hash") {
		t.Fatalf("corrupted body validation error = %v", decodeErr)
	}
	var violation *PeerDataViolation
	disputed := rangePoint(42)
	connection := &connection{
		activeFetch: &rangeFetch{
			expected: []expectedHeader{{point: disputed}},
		},
	}
	disputed = connection.currentFetchPoint()
	classified := classifyPeerProtocolError(decodeErr, disputed)
	if !errors.As(classified, &violation) ||
		violation.Kind != "decoded_block_validation" ||
		!pointsEqual(violation.Point, disputed) {
		t.Fatalf("corrupted body classification = %T %v", classified, classified)
	}
}

func TestHeaderContinuityRejectsWrongParentHeightAndSlotOrder(t *testing.T) {
	checkpoint := rangePoint(9)
	first := rangeBlock(10, 100, 0x10)
	copy(first.prev[:], checkpoint.Hash)
	firstExpected := expectedHeader{
		header: first,
		point:  pcommon.NewPoint(first.SlotNumber(), first.Hash().Bytes()),
	}
	parent := knownChainParent(checkpoint, 99)
	if err := validateHeaderContinuity(
		&parent,
		nil,
		first,
		firstExpected.point,
	); err != nil {
		t.Fatalf("first header continuity: %v", err)
	}

	valid := rangeBlock(11, 101, 0x11)
	valid.prev = first.Hash()
	if err := validateHeaderContinuity(
		nil,
		&firstExpected,
		valid,
		pcommon.NewPoint(valid.SlotNumber(), valid.Hash().Bytes()),
	); err != nil {
		t.Fatalf("valid next header: %v", err)
	}
	equalSlot := rangeBlock(10, 101, 0x19)
	equalSlot.prev = first.Hash()
	err := validateHeaderContinuity(
		nil,
		&firstExpected,
		equalSlot,
		pcommon.NewPoint(equalSlot.SlotNumber(), equalSlot.Hash().Bytes()),
	)
	var equalSlotViolation *PeerDataViolation
	if !errors.As(err, &equalSlotViolation) ||
		equalSlotViolation.Kind != "header_slot_order" {
		t.Fatalf("regular equal-slot continuity = %T %v", err, err)
	}

	wrongParent := rangeBlock(11, 101, 0x21)
	heightGap := rangeBlock(11, 102, 0x22)
	heightGap.prev = first.Hash()
	reorderedSlot := rangeBlock(9, 101, 0x23)
	reorderedSlot.prev = first.Hash()
	tests := map[string]struct {
		block rangeTestBlock
		kind  string
	}{
		"wrong parent": {
			block: wrongParent,
			kind:  "header_parent_mismatch",
		},
		"height gap": {
			block: heightGap,
			kind:  "header_height_gap",
		},
		"reordered slot": {
			block: reorderedSlot,
			kind:  "header_slot_order",
		},
	}
	for name, testCase := range tests {
		t.Run(name, func(t *testing.T) {
			point := pcommon.NewPoint(
				testCase.block.SlotNumber(),
				testCase.block.Hash().Bytes(),
			)
			err := validateHeaderContinuity(
				nil,
				&firstExpected,
				testCase.block,
				point,
			)
			var violation *PeerDataViolation
			if !errors.As(err, &violation) || violation.Kind != testCase.kind {
				t.Fatalf("continuity error = %T %v", err, err)
			}
		})
	}
}

func TestFirstHeaderMustExtendReconciledCheckpoint(t *testing.T) {
	checkpoint := rangePoint(9)
	first := rangeBlock(10, 100, 0x10)
	point := pcommon.NewPoint(first.SlotNumber(), first.Hash().Bytes())
	parent := knownChainParent(checkpoint, 99)
	err := validateHeaderContinuity(&parent, nil, first, point)
	var violation *PeerDataViolation
	if !errors.As(err, &violation) ||
		violation.Kind != "header_parent_mismatch" {
		t.Fatalf("checkpoint parent error = %T %v", err, err)
	}
}

func TestFirstHeaderRejectsStoredHeightMismatch(t *testing.T) {
	checkpoint := rangePoint(9)
	first := rangeBlock(10, 100, 0x10)
	copy(first.prev[:], checkpoint.Hash)
	point := pcommon.NewPoint(first.SlotNumber(), first.Hash().Bytes())
	parent := knownChainParent(checkpoint, 98)
	err := validateHeaderContinuity(&parent, nil, first, point)
	var violation *PeerDataViolation
	if !errors.As(err, &violation) ||
		violation.Kind != "header_height_gap" {
		t.Fatalf("stored-height error = %T %v", err, err)
	}
}

func TestPinnedByronEBBStrictTransitionRules(t *testing.T) {
	block := pinnedByronEBB(t)
	decodedHeader, ok := block.Header().(*byron.ByronEpochBoundaryBlockHeader)
	if !ok || !isByronEpochBoundaryHeader(decodedHeader) {
		t.Fatalf("fixture header type = %T", block.Header())
	}
	// The pinned fixture is the testnet epoch-0 EBB. Copy its concrete,
	// validation-decoded header and set only the exported epoch/difficulty and
	// predecessor fields to form a nonzero synthetic transition oracle.
	header := *decodedHeader
	header.ConsensusData.Epoch = 1
	header.ConsensusData.Difficulty.Value = 100
	var predecessorHash lcommon.Blake2b256
	for index := range predecessorHash {
		predecessorHash[index] = 0x61
	}
	header.PrevBlock = predecessorHash
	parentPoint := pcommon.NewPoint(
		header.SlotNumber()-1,
		header.PrevHash().Bytes(),
	)
	parent := NewChainPoint(parentPoint, header.BlockNumber())
	point := pcommon.NewPoint(header.SlotNumber(), header.Hash().Bytes())
	if err := validateHeaderContinuity(&parent, nil, &header, point); err != nil {
		t.Fatalf("same-height Byron EBB transition: %v", err)
	}
	plusOneParent := NewChainPoint(parentPoint, header.BlockNumber()-1)
	err := validateHeaderContinuity(
		&plusOneParent,
		nil,
		&header,
		point,
	)
	var violation *PeerDataViolation
	if !errors.As(err, &violation) || violation.Kind != "header_height_gap" {
		t.Fatalf("Byron EBB +1 transition = %T %v", err, err)
	}
	equalSlotParent := NewChainPoint(
		pcommon.NewPoint(header.SlotNumber(), header.PrevHash().Bytes()),
		header.BlockNumber(),
	)
	err = validateHeaderContinuity(
		&equalSlotParent,
		nil,
		&header,
		point,
	)
	if !errors.As(err, &violation) || violation.Kind != "header_slot_order" {
		t.Fatalf("equal-slot current Byron EBB transition = %T %v", err, err)
	}

	ebbExpected := expectedHeader{header: &header, point: point}
	for name, slot := range map[string]uint64{
		"equal slot": header.SlotNumber(),
		"later slot": header.SlotNumber() + 1,
	} {
		t.Run("EBB to main "+name, func(t *testing.T) {
			main := rangeBlock(slot, header.BlockNumber()+1, 0x71)
			main.prev = header.Hash()
			if err := validateHeaderContinuity(
				nil,
				&ebbExpected,
				main,
				pcommon.NewPoint(main.SlotNumber(), main.Hash().Bytes()),
			); err != nil {
				t.Fatal(err)
			}
		})
	}

	restart := NewByronEBBChainPoint(point, header.BlockNumber())
	main := rangeBlock(header.SlotNumber(), header.BlockNumber()+1, 0x72)
	main.prev = header.Hash()
	mainPoint := pcommon.NewPoint(main.SlotNumber(), main.Hash().Bytes())
	if err := validateHeaderContinuity(&restart, nil, main, mainPoint); err != nil {
		t.Fatalf("EBB restart to equal-slot main: %v", err)
	}
	regularRestart := NewChainPoint(point, header.BlockNumber())
	err = validateHeaderContinuity(&regularRestart, nil, main, mainPoint)
	if !errors.As(err, &violation) || violation.Kind != "header_slot_order" {
		t.Fatalf("regular restart accepted equal-slot main = %T %v", err, err)
	}
}

func TestOriginRejectsRegularOrWrongNetworkEBBFirstHeader(t *testing.T) {
	first := rangeBlock(0, 0, 0x10)
	first.prev = rangeBlock(0, 0, 0x99).Hash()
	point := pcommon.NewPoint(first.SlotNumber(), first.Hash().Bytes())
	parent := NewChainPointOrigin()
	err := validateHeaderContinuity(&parent, nil, first, point)
	var violation *PeerDataViolation
	if !errors.As(err, &violation) || violation.Kind != "origin_first_header" {
		t.Fatalf("regular Origin first header = %T %v", err, err)
	}
	testnetEBB := pinnedByronEBB(t)
	testnetPoint := pcommon.NewPoint(
		testnetEBB.SlotNumber(),
		testnetEBB.Hash().Bytes(),
	)
	err = validateHeaderContinuity(
		&parent,
		nil,
		testnetEBB.Header(),
		testnetPoint,
	)
	if !errors.As(err, &violation) || violation.Kind != "origin_first_header" {
		t.Fatalf("wrong-network Origin EBB = %T %v", err, err)
	}
}

func TestRollForwardValidatesBufferedHeaderSequenceBeforeFetch(t *testing.T) {
	checkpoint := rangePoint(9)
	connection := &connection{
		pendingHeaders: make([]expectedHeader, 0, 32),
		expectedParent: func() *ChainPoint {
			parent := knownChainParent(checkpoint, 99)
			return &parent
		}(),
	}
	tip := chainsync.Tip{Point: rangePoint(100), BlockNumber: 100}
	first := rangeBlock(10, 100, 0x10)
	copy(first.prev[:], checkpoint.Hash)
	if err := connection.onRollForward(
		chainsync.CallbackContext{},
		0,
		first,
		tip,
	); err != nil {
		t.Fatal(err)
	}
	second := rangeBlock(11, 101, 0x11)
	second.prev = first.Hash()
	if err := connection.onRollForward(
		chainsync.CallbackContext{},
		0,
		second,
		tip,
	); err != nil {
		t.Fatal(err)
	}
	bad := rangeBlock(12, 102, 0x12)
	err := connection.onRollForward(
		chainsync.CallbackContext{},
		0,
		bad,
		tip,
	)
	var violation *PeerDataViolation
	if !errors.As(err, &violation) ||
		violation.Kind != "header_parent_mismatch" {
		t.Fatalf("buffered header error = %T %v", err, err)
	}
	if len(connection.pendingHeaders) != 2 {
		t.Fatalf("invalid header entered pending window: %#v", connection.pendingHeaders)
	}
}

func knownChainParent(point pcommon.Point, blockNumber uint64) ChainPoint {
	return NewChainPoint(point, blockNumber)
}

func rangeTestConnection(blocks ...rangeTestBlock) *connection {
	ret := &connection{
		events:         make(chan chainEvent, len(blocks)),
		pendingHeaders: make([]expectedHeader, 0, len(blocks)),
	}
	for _, block := range blocks {
		ret.pendingHeaders = append(ret.pendingHeaders, expectedHeader{
			header: block,
			point:  ret.pendingRangePoint(block),
			tip: chainsync.Tip{
				Point:       ret.pendingRangePoint(block),
				BlockNumber: block.BlockNumber(),
			},
		})
	}
	return ret
}

func (*connection) pendingRangePoint(block rangeTestBlock) pcommon.Point {
	return pcommon.NewPoint(block.SlotNumber(), block.Hash().Bytes())
}

func readCompressedFixture(t *testing.T, name string) []byte {
	t.Helper()
	file, err := os.Open(filepath.Join("..", "..", "testdata", "blocks", name))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	reader, err := gzip.NewReader(file)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	raw, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func pinnedByronEBB(t *testing.T) lcommon.Block {
	t.Helper()
	block, err := ledger.NewBlockFromCbor(
		ledger.BlockTypeByronEbb,
		readCompressedFixture(t, "byron-ebb-testnet.cbor.gz"),
		lcommon.VerifyConfig{SkipBodyHashValidation: false},
	)
	if err != nil {
		t.Fatal(err)
	}
	return block
}

type blockingRollbackHandler struct {
	started chan struct{}
	release chan struct{}
	target  chan ChainPoint
}

func (*blockingRollbackHandler) Reconcile(
	context.Context,
	ChainPoint,
	Peer,
) error {
	return nil
}

func (*blockingRollbackHandler) RollForward(
	context.Context,
	lcommon.Block,
	chainsync.Tip,
	Peer,
) error {
	return nil
}

func (h *blockingRollbackHandler) RollBackward(
	_ context.Context,
	point ChainPoint,
	_ chainsync.Tip,
	_ Peer,
) error {
	if h.target != nil {
		h.target <- point
	}
	close(h.started)
	<-h.release
	return nil
}

type rangeTestBlock struct {
	slot   uint64
	number uint64
	hash   lcommon.Blake2b256
	prev   lcommon.Blake2b256
}

func rangeBlock(slot, number uint64, fill byte) rangeTestBlock {
	var hash lcommon.Blake2b256
	for index := range hash {
		hash[index] = fill
	}
	return rangeTestBlock{slot: slot, number: number, hash: hash}
}

func rangePoint(slot int) pcommon.Point {
	block := rangeBlock(uint64(slot), uint64(slot), byte(slot))
	return pcommon.NewPoint(block.SlotNumber(), block.Hash().Bytes())
}

func (b rangeTestBlock) Hash() lcommon.Blake2b256          { return b.hash }
func (b rangeTestBlock) PrevHash() lcommon.Blake2b256      { return b.prev }
func (b rangeTestBlock) BlockNumber() uint64               { return b.number }
func (b rangeTestBlock) SlotNumber() uint64                { return b.slot }
func (b rangeTestBlock) IssuerVkey() lcommon.IssuerVkey    { return lcommon.IssuerVkey{} }
func (b rangeTestBlock) BlockBodySize() uint64             { return 0 }
func (b rangeTestBlock) Era() lcommon.Era                  { return lcommon.Era{Name: "test"} }
func (b rangeTestBlock) Cbor() []byte                      { return nil }
func (b rangeTestBlock) BlockBodyHash() lcommon.Blake2b256 { return lcommon.Blake2b256{} }
func (b rangeTestBlock) Header() lcommon.BlockHeader       { return b }
func (b rangeTestBlock) Type() int                         { return 0 }
func (b rangeTestBlock) Transactions() []lcommon.Transaction {
	return nil
}
func (b rangeTestBlock) Utxorpc() (*utxorpc.Block, error) { return nil, nil }
