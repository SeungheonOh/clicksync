package relay

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"log/slog"
	"net"
	"syscall"
	"testing"
	"time"

	"cardano-clicksync/internal/model"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/protocol"
	"github.com/blinklabs-io/gouroboros/protocol/blockfetch"
	"github.com/blinklabs-io/gouroboros/protocol/chainsync"
	pcommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

type fakeHeader struct {
	hash   lcommon.Blake2b256
	parent lcommon.Blake2b256
	slot   uint64
	number uint64
}

func (h *fakeHeader) Hash() lcommon.Blake2b256          { return h.hash }
func (h *fakeHeader) PrevHash() lcommon.Blake2b256      { return h.parent }
func (h *fakeHeader) BlockNumber() uint64               { return h.number }
func (h *fakeHeader) SlotNumber() uint64                { return h.slot }
func (h *fakeHeader) IssuerVkey() lcommon.IssuerVkey    { return lcommon.IssuerVkey{} }
func (h *fakeHeader) BlockBodySize() uint64             { return 0 }
func (h *fakeHeader) Era() lcommon.Era                  { return lcommon.EraInvalid }
func (h *fakeHeader) Cbor() []byte                      { return nil }
func (h *fakeHeader) BlockBodyHash() lcommon.Blake2b256 { return lcommon.Blake2b256{} }

type fakeRangeClient struct {
	done chan struct{}
	get  func(pcommon.Point, pcommon.Point) error
}

func (f *fakeRangeClient) GetBlockRange(
	start, end pcommon.Point,
) error {
	return f.get(start, end)
}

func (f *fakeRangeClient) DoneChan() <-chan struct{} {
	return f.done
}

type fakeIntersectionClient struct {
	accept model.Point
	calls  []pcommon.Point
	err    error
}

func (f *fakeIntersectionClient) GetAvailableBlockRange(
	points []pcommon.Point,
) (pcommon.Point, pcommon.Point, error) {
	f.calls = append(f.calls, points[0])
	if f.err != nil {
		return pcommon.Point{}, pcommon.Point{}, f.err
	}
	if sameProtocolPoint(points[0], toProtocolPoint(f.accept)) {
		return points[0], points[0], nil
	}
	return pcommon.Point{}, pcommon.Point{}, chainsync.ErrIntersectNotFound
}

func TestRawBlockDigestDomainAndFraming(t *testing.T) {
	raw := []byte{0x82, 0x01, 0xa0}
	got := RawBlockDigest(7, raw)
	hash := sha256.New()
	_, _ = hash.Write([]byte("cardano-clicksync/raw-block/v1"))
	var frame [16]byte
	binary.BigEndian.PutUint64(frame[:8], 7)
	binary.BigEndian.PutUint64(frame[8:], 3)
	_, _ = hash.Write(frame[:])
	_, _ = hash.Write(raw)
	var want model.Hash32
	copy(want[:], hash.Sum(nil))
	if got != want {
		t.Fatalf("digest = %x, want %x", got, want)
	}
	if got == RawBlockDigest(8, raw) ||
		got == RawBlockDigest(7, append(raw, 0)) {
		t.Fatal("block type and length must be in the digest domain")
	}
}

func TestProtocolConfigsUseOnlyRawBlockCallbackAndSkipValidation(t *testing.T) {
	session := newTestSession(t, 0, 8, 8, 1<<20)
	blockConfig, chainConfig, err := session.protocolConfigs()
	if err != nil {
		t.Fatal(err)
	}
	if blockConfig.BlockRawFunc == nil || blockConfig.BlockFunc != nil {
		t.Fatal("BlockFetch must use only BlockRawFunc")
	}
	if !blockConfig.SkipBlockValidation || !chainConfig.SkipBlockValidation {
		t.Fatal("block-body validation must remain disabled")
	}
	if blockConfig.RecvQueueSize != 8 {
		t.Fatalf("BlockFetch queue = %d, want 8", blockConfig.RecvQueueSize)
	}

	session = newTestSession(t, 0, 256, 4096, 1<<20)
	_, chainConfig, err = session.protocolConfigs()
	if err != nil {
		t.Fatal(err)
	}
	if chainConfig.RecvQueueSize != chainsync.MaxRecvQueueSize ||
		chainConfig.PipelineLimit != chainsync.MaxPipelineLimit {
		t.Fatalf(
			"ChainSync limits = (%d,%d), want (%d,%d)",
			chainConfig.RecvQueueSize,
			chainConfig.PipelineLimit,
			chainsync.MaxRecvQueueSize,
			chainsync.MaxPipelineLimit,
		)
	}
}

func TestSelectIntersectionAcceptedInCandidateOrder(t *testing.T) {
	newer := testPoint(20, 2)
	older := testPoint(10, 1)
	client := &fakeIntersectionClient{accept: older}
	got, err := selectIntersection(client, []model.Point{newer, older})
	if err != nil {
		t.Fatal(err)
	}
	if got != older {
		t.Fatalf("intersection = %+v, want %+v", got, older)
	}
	if len(client.calls) != 2 {
		t.Fatalf("intersection calls = %d, want 2", len(client.calls))
	}
}

func TestSelectIntersectionNotFound(t *testing.T) {
	client := &fakeIntersectionClient{accept: testPoint(99, 9)}
	_, err := selectIntersection(
		client,
		[]model.Point{testPoint(20, 2), testPoint(10, 1)},
	)
	if FailureOf(err) != FailureIntersection {
		t.Fatalf("failure = %v, want intersection: %v", FailureOf(err), err)
	}
}

func TestSingleRangeAtTipRetainsOnlySourceRaw(t *testing.T) {
	for _, relayIndex := range []int{0, 1} {
		t.Run(string(rune('0'+relayIndex)), func(t *testing.T) {
			session := newTestSession(t, relayIndex, 4, 4, 1<<20)
			raw := []byte{0x82, 0x01, byte(relayIndex)}
			header := testHeader(4, 40)
			armSession(t, session, func(start, end pcommon.Point) error {
				if !sameProtocolPoint(start, end) ||
					!sameProtocolPoint(start, pointForHeader(header)) {
					t.Fatalf("requested range %v..%v", start, end)
				}
				if err := session.onRawBlock(
					blockfetch.CallbackContext{},
					4,
					raw,
				); err != nil {
					return err
				}
				return session.onBatchDone(blockfetch.CallbackContext{})
			})

			if err := session.onRollForward(
				chainsync.CallbackContext{},
				4,
				header,
				tipForHeader(header),
			); err != nil {
				t.Fatal(err)
			}
			event := nextEvent(t, session)
			raw[0] = 0xff
			if event.Kind != Forward || event.Point != modelPointForHeader(header) {
				t.Fatalf("unexpected event: %+v", event)
			}
			if event.RawLength != 3 ||
				event.Digest != RawBlockDigest(4, []byte{0x82, 0x01, byte(relayIndex)}) {
				t.Fatal("raw length or digest differs")
			}
			if relayIndex == 0 {
				if !bytes.Equal(event.RawCBOR, []byte{0x82, 0x01, 0}) {
					t.Fatalf("source raw = %x", event.RawCBOR)
				}
			} else if event.RawCBOR != nil {
				t.Fatalf("follower retained raw: %x", event.RawCBOR)
			}
		})
	}
}

func TestOrderedMultiBlockRange(t *testing.T) {
	session := newTestSession(t, 0, 8, 3, 1<<20)
	headers := []*fakeHeader{
		testHeader(10, 100),
		testHeader(11, 101),
		testHeader(12, 102),
	}
	var requestedStart, requestedEnd pcommon.Point
	armSession(t, session, func(start, end pcommon.Point) error {
		requestedStart, requestedEnd = start, end
		for index := range headers {
			if err := session.onRawBlock(
				blockfetch.CallbackContext{},
				uint(index+1),
				[]byte{byte(index + 1)},
			); err != nil {
				return err
			}
		}
		return session.onBatchDone(blockfetch.CallbackContext{})
	})
	for index, header := range headers {
		if err := session.onRollForward(
			chainsync.CallbackContext{},
			uint(index+1),
			header,
			tipForHeader(headers[len(headers)-1]),
		); err != nil {
			t.Fatal(err)
		}
	}
	for index, header := range headers {
		event := nextEvent(t, session)
		if event.Point != modelPointForHeader(header) ||
			event.BlockType != uint(index+1) {
			t.Fatalf("event %d = %+v", index, event)
		}
	}
	if !sameProtocolPoint(requestedStart, pointForHeader(headers[0])) ||
		!sameProtocolPoint(requestedEnd, pointForHeader(headers[2])) {
		t.Fatalf("requested wrong range %v..%v", requestedStart, requestedEnd)
	}
}

func TestChainSyncPreparesNextRangeWhileBlockFetchStreams(t *testing.T) {
	session := newTestSession(t, 0, 8, 2, 1<<20)
	headers := []*fakeHeader{
		testHeader(10, 100),
		testHeader(11, 101),
		testHeader(12, 102),
		testHeader(13, 103),
	}
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	secondStarted := make(chan struct{})
	call := 0
	armSession(t, session, func(_, _ pcommon.Point) error {
		current := call
		call++
		if current == 0 {
			close(firstStarted)
			<-releaseFirst
		} else {
			close(secondStarted)
		}
		for index := range 2 {
			if err := session.onRawBlock(
				blockfetch.CallbackContext{},
				uint(current*2+index+1),
				[]byte{byte(current*2 + index + 1)},
			); err != nil {
				return err
			}
		}
		return session.onBatchDone(blockfetch.CallbackContext{})
	})

	tip := tipForHeader(testHeader(20, 200))
	for index := range 2 {
		if err := session.onRollForward(
			chainsync.CallbackContext{},
			uint(index+1),
			headers[index],
			tip,
		); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case <-firstStarted:
	case <-time.After(time.Second):
		t.Fatal("first range did not start")
	}
	for index := 2; index < 4; index++ {
		if err := session.onRollForward(
			chainsync.CallbackContext{},
			uint(index+1),
			headers[index],
			tip,
		); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, func() bool {
		return len(session.fetchJobs) == 1
	})

	close(releaseFirst)
	select {
	case <-secondStarted:
	case <-time.After(time.Second):
		t.Fatal("prepared range did not start after first BatchDone")
	}
	for index, header := range headers {
		event := nextEvent(t, session)
		if event.Point != modelPointForHeader(header) ||
			event.BlockType != uint(index+1) {
			t.Fatalf("event %d = %+v", index, event)
		}
	}
}

func TestProtocolCallbackCountAndOrderFailures(t *testing.T) {
	tests := map[string]func(*Session) func(pcommon.Point, pcommon.Point) error{
		"missing": func(session *Session) func(pcommon.Point, pcommon.Point) error {
			return func(_, _ pcommon.Point) error {
				if err := session.onRawBlock(
					blockfetch.CallbackContext{},
					1,
					[]byte{1},
				); err != nil {
					return err
				}
				return session.onBatchDone(blockfetch.CallbackContext{})
			}
		},
		"extra": func(session *Session) func(pcommon.Point, pcommon.Point) error {
			return func(_, _ pcommon.Point) error {
				if err := session.onRawBlock(
					blockfetch.CallbackContext{},
					1,
					[]byte{1},
				); err != nil {
					return err
				}
				return session.onRawBlock(
					blockfetch.CallbackContext{},
					2,
					[]byte{2},
				)
			}
		},
		"reordered": func(session *Session) func(pcommon.Point, pcommon.Point) error {
			return func(_, _ pcommon.Point) error {
				return session.onRawBlock(
					blockfetch.CallbackContext{},
					2,
					[]byte{2},
				)
			}
		},
	}
	for name, callbacks := range tests {
		t.Run(name, func(t *testing.T) {
			headerCount := 2
			if name == "extra" {
				headerCount = 1
			}
			session := newTestSession(t, 0, 4, headerCount, 1<<20)
			ctx, _ := armSession(t, session, callbacks(session))
			for index := range headerCount {
				if err := session.onRollForward(
					chainsync.CallbackContext{},
					uint(index+1),
					testHeader(uint64(index+1), uint64(index+1)),
					tipForHeader(testHeader(
						uint64(headerCount),
						uint64(headerCount),
					)),
				); err != nil && context.Cause(ctx) == nil {
					t.Fatal(err)
				}
			}
			select {
			case <-ctx.Done():
			case <-time.After(time.Second):
				t.Fatal("protocol failure did not stop the fetch loop")
			}
			err := context.Cause(ctx)
			if err == nil || FailureOf(err) != FailureProtocol {
				t.Fatalf("error = %v, failure = %s", err, FailureOf(err))
			}
		})
	}
}

func TestRollbackFlushesPendingForwardsBeforeTarget(t *testing.T) {
	session := newTestSession(t, 0, 8, 8, 1<<20)
	first := testHeader(20, 200)
	second := testHeader(21, 201)
	resumed := testHeader(22, 202)
	rangeCall := 0
	armSession(t, session, func(_, _ pcommon.Point) error {
		rangeCall++
		firstType := 1
		count := 2
		if rangeCall == 2 {
			firstType = 3
			count = 1
		}
		for index := range count {
			if err := session.onRawBlock(
				blockfetch.CallbackContext{},
				uint(firstType+index),
				[]byte{byte(firstType + index)},
			); err != nil {
				return err
			}
		}
		return session.onBatchDone(blockfetch.CallbackContext{})
	})
	for index, header := range []*fakeHeader{first, second} {
		if err := session.onRollForward(
			chainsync.CallbackContext{},
			uint(index+1),
			header,
			tipForHeader(testHeader(30, 300)),
		); err != nil {
			t.Fatal(err)
		}
	}
	target := testPoint(19, 19)
	if err := session.onRollBackward(
		chainsync.CallbackContext{},
		toProtocolPoint(target),
		chainsync.Tip{
			Point:       toProtocolPoint(testPoint(25, 25)),
			BlockNumber: 250,
		},
	); err != nil {
		t.Fatal(err)
	}
	if err := session.onRollForward(
		chainsync.CallbackContext{},
		3,
		resumed,
		tipForHeader(resumed),
	); err != nil {
		t.Fatal(err)
	}
	if event := nextEvent(t, session); event.Point != modelPointForHeader(first) {
		t.Fatalf("first event = %+v", event)
	}
	if event := nextEvent(t, session); event.Point != modelPointForHeader(second) {
		t.Fatalf("second event = %+v", event)
	}
	event := nextEvent(t, session)
	target.BlockNumber = 0
	if event.Kind != Rollback || event.Point != target || event.RawCBOR != nil {
		t.Fatalf("rollback = %+v, want target %+v", event, target)
	}
	if event := nextEvent(t, session); event.Point != modelPointForHeader(resumed) {
		t.Fatalf("resumed event = %+v", event)
	}
}

func TestEventItemBackpressureCancellation(t *testing.T) {
	session := newTestSessionWithQueue(t, 0, 4, 2, 1, 1<<20)
	rangeDone := make(chan error, 1)
	ctx, cancel := armSession(t, session, func(_, _ pcommon.Point) error {
		err := session.onRawBlock(
			blockfetch.CallbackContext{},
			1,
			[]byte{1},
		)
		if err == nil {
			err = session.onRawBlock(
				blockfetch.CallbackContext{},
				2,
				[]byte{2},
			)
		}
		rangeDone <- err
		return err
	})
	if err := session.onRollForward(
		chainsync.CallbackContext{},
		1,
		testHeader(1, 1),
		tipForHeader(testHeader(2, 2)),
	); err != nil {
		t.Fatal(err)
	}
	if err := session.onRollForward(
		chainsync.CallbackContext{},
		2,
		testHeader(2, 2),
		tipForHeader(testHeader(2, 2)),
	); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		depth, _ := session.QueueDepth()
		return depth == 1
	})
	for index := 3; index <= 4; index++ {
		if err := session.onRollForward(
			chainsync.CallbackContext{},
			uint(index),
			testHeader(uint64(index), uint64(index)),
			tipForHeader(testHeader(4, 4)),
		); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, func() bool {
		return len(session.fetchJobs) == 1
	})
	cancel(context.Canceled)
	select {
	case err := <-rangeDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("active range error = %v, want cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("active range remained blocked after cancellation")
	}
	if context.Cause(ctx) == nil {
		t.Fatal("test context was not canceled")
	}
}

func TestRawByteBackpressureAndRelease(t *testing.T) {
	session := newTestSessionWithQueue(t, 0, 4, 2, 4, 3)
	rangeDone := make(chan error, 1)
	armSession(t, session, func(_, _ pcommon.Point) error {
		err := session.onRawBlock(
			blockfetch.CallbackContext{},
			1,
			[]byte{1, 1, 1},
		)
		if err == nil {
			err = session.onRawBlock(
				blockfetch.CallbackContext{},
				2,
				[]byte{2, 2, 2},
			)
		}
		if err == nil {
			err = session.onBatchDone(blockfetch.CallbackContext{})
		}
		rangeDone <- err
		return err
	})
	if err := session.onRollForward(
		chainsync.CallbackContext{},
		1,
		testHeader(1, 1),
		tipForHeader(testHeader(2, 2)),
	); err != nil {
		t.Fatal(err)
	}
	if err := session.onRollForward(
		chainsync.CallbackContext{},
		2,
		testHeader(2, 2),
		tipForHeader(testHeader(2, 2)),
	); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		used, _ := session.RawQueueDepth()
		return used == 3
	})
	select {
	case err := <-rangeDone:
		t.Fatalf("range completed before raw bytes were released: %v", err)
	default:
	}
	_ = nextEvent(t, session)
	select {
	case err := <-rangeDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("raw-byte waiter did not resume")
	}
	_ = nextEvent(t, session)
	used, _ := session.RawQueueDepth()
	if used != 0 {
		t.Fatalf("queued raw bytes = %d, want 0", used)
	}
}

func TestOversizeRawBlockFailsBound(t *testing.T) {
	session := newTestSessionWithQueue(t, 0, 1, 1, 1, 3)
	ctx, _ := armSession(t, session, func(_, _ pcommon.Point) error {
		return session.onRawBlock(
			blockfetch.CallbackContext{},
			1,
			[]byte{1, 2, 3, 4},
		)
	})
	if err := session.onRollForward(
		chainsync.CallbackContext{},
		1,
		testHeader(1, 1),
		tipForHeader(testHeader(1, 1)),
	); err != nil && context.Cause(ctx) == nil {
		t.Fatal(err)
	}
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("oversize raw block did not stop the fetch loop")
	}
	err := context.Cause(ctx)
	if FailureOf(err) != FailureBound {
		t.Fatalf("failure = %s, want bound: %v", FailureOf(err), err)
	}
	used, _ := session.RawQueueDepth()
	if used != 0 {
		t.Fatalf("raw reservation leaked: %d", used)
	}
}

func TestFinishDrainsRawReservations(t *testing.T) {
	session := newTestSessionWithQueue(t, 0, 1, 1, 2, 100)
	rangeDone := make(chan error, 1)
	armSession(t, session, func(_, _ pcommon.Point) error {
		err := session.onRawBlock(
			blockfetch.CallbackContext{},
			1,
			[]byte{1, 2, 3},
		)
		if err == nil {
			err = session.onBatchDone(blockfetch.CallbackContext{})
		}
		rangeDone <- err
		return err
	})
	if err := session.onRollForward(
		chainsync.CallbackContext{},
		1,
		testHeader(1, 1),
		tipForHeader(testHeader(1, 1)),
	); err != nil {
		t.Fatal(err)
	}
	if err := <-rangeDone; err != nil {
		t.Fatal(err)
	}
	session.finish(io.EOF)
	used, _ := session.RawQueueDepth()
	if used != 0 {
		t.Fatalf("raw reservation after close = %d", used)
	}
	if _, err := session.Next(context.Background()); !errors.Is(err, io.EOF) {
		t.Fatalf("Next error = %v, want EOF cause", err)
	}
}

func TestFailureClassification(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want FailureKind
	}{
		{"canceled", context.Canceled, FailureCanceled},
		{"deadline", context.DeadlineExceeded, FailureTimeout},
		{"dns", &net.DNSError{Name: "relay.invalid"}, FailureDNS},
		{"refused", syscall.ECONNREFUSED, FailureConnection},
		{"disconnect", protocol.ErrProtocolShuttingDown, FailureConnection},
		{"protocol", errors.New("bad callback"), FailureProtocol},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := FailureOf(test.err); got != test.want {
				t.Fatalf("failure = %s, want %s", got, test.want)
			}
		})
	}
}

func newTestSession(
	t *testing.T,
	relayIndex, blockFetchQueue, rangeBlocks int,
	rawBytes int64,
) *Session {
	t.Helper()
	return newTestSessionWithQueue(
		t,
		relayIndex,
		blockFetchQueue,
		rangeBlocks,
		16,
		rawBytes,
	)
}

func newTestSessionWithQueue(
	t *testing.T,
	relayIndex, blockFetchQueue, rangeBlocks, relayQueue int,
	rawBytes int64,
) *Session {
	t.Helper()
	session, err := New(
		Config{
			RelayIndex:            relayIndex,
			Host:                  "relay.example:3001",
			Operator:              "operator",
			NetworkMagic:          764824073,
			BlockFetchRangeBlocks: rangeBlocks,
			BlockFetchQueueSize:   blockFetchQueue,
			RelayQueueSize:        relayQueue,
			RawQueueBytes:         rawBytes,
			DialTimeout:           time.Second,
			BlockTimeout:          time.Second,
		},
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	if err != nil {
		t.Fatal(err)
	}
	return session
}

func armSession(
	t *testing.T,
	session *Session,
	get func(pcommon.Point, pcommon.Point) error,
) (context.Context, context.CancelCauseFunc) {
	t.Helper()
	ctx, cancel := context.WithCancelCause(context.Background())
	client := &fakeRangeClient{done: make(chan struct{}), get: get}
	session.runMu.Lock()
	session.runCtx = ctx
	session.cancel = cancel
	session.runMu.Unlock()
	workerDone := make(chan error, 2)
	startWorker := func(run func() error) {
		go func() {
			err := run()
			if err == nil {
				err = errors.New("test session worker ended without an error")
			}
			if context.Cause(ctx) == nil {
				cancel(err)
			}
			workerDone <- err
		}()
	}
	startWorker(func() error {
		return session.runRangeBuilder(ctx)
	})
	startWorker(func() error {
		return session.runFetchLoop(ctx, client)
	})
	t.Cleanup(func() {
		cancel(context.Canceled)
		for range 2 {
			select {
			case <-workerDone:
			case <-time.After(time.Second):
				t.Fatal("test session worker did not stop")
			}
		}
	})
	return ctx, cancel
}

func testHeader(slot, number uint64) *fakeHeader {
	var hash lcommon.Blake2b256
	binary.BigEndian.PutUint64(hash[24:], slot)
	hash[0] = byte(number)
	return &fakeHeader{hash: hash, slot: slot, number: number}
}

func modelPointForHeader(header *fakeHeader) model.Point {
	var hash model.Hash32
	copy(hash[:], header.hash[:])
	return model.Point{
		Slot:        header.slot,
		Hash:        hash,
		BlockNumber: header.number,
	}
}

func pointForHeader(header *fakeHeader) pcommon.Point {
	return pcommon.NewPoint(header.slot, header.hash.Bytes())
}

func tipForHeader(header *fakeHeader) chainsync.Tip {
	return chainsync.Tip{
		Point:       pointForHeader(header),
		BlockNumber: header.number,
	}
}

func testPoint(slot uint64, marker byte) model.Point {
	var hash model.Hash32
	hash[0] = marker
	return model.Point{Slot: slot, Hash: hash, BlockNumber: slot * 10}
}

func nextEvent(t *testing.T, session *Session) Event {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	event, err := session.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return event
}

func waitFor(t *testing.T, ready func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for !ready() {
		if time.Now().After(deadline) {
			t.Fatal("condition was not reached")
		}
		time.Sleep(time.Millisecond)
	}
}
