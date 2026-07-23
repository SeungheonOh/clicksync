package syncer

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"testing"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/protocol/chainsync"
	pcommon "github.com/blinklabs-io/gouroboros/protocol/common"

	"clicksync/internal/n2n"
)

func TestDirectTransportMapsFreshProbeProvenance(t *testing.T) {
	transport := testDirectTransport()
	transport.probePeer = func(
		_ context.Context,
		peer n2n.Peer,
		_ n2n.DialConfig,
		point pcommon.Point,
		_ *slog.Logger,
	) (n2n.PointProbe, error) {
		peer.Address = "198.51.100.42:3001"
		peer.N2NVersion = 15
		return n2n.PointProbe{
			Accepted: true,
			Peer:     peer,
			Tip:      chainsync.Tip{Point: clonePoint(point), BlockNumber: 99},
		}, nil
	}
	result, err := transport.Probe(
		context.Background(),
		testPeer("relay.example:3001", "operator"),
		testPoint(10, 0x10),
	)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Accepted ||
		result.Address != "198.51.100.42:3001" ||
		result.N2NVersion != 15 ||
		result.Tip.BlockNumber != 99 {
		t.Fatalf("result = %#v", result)
	}
}

func TestDirectTransportClassifiesProbeErrorsByConcreteType(t *testing.T) {
	for name, testCase := range map[string]struct {
		input     error
		retryable bool
	}{
		"network": {
			input: &net.OpError{
				Op:  "dial",
				Net: "tcp",
				Err: errors.New("refused"),
			},
			retryable: true,
		},
		"ambiguous": {
			input: errors.New("ambiguous probe failure"),
		},
	} {
		t.Run(name, func(t *testing.T) {
			transport := testDirectTransport()
			transport.probePeer = func(
				context.Context,
				n2n.Peer,
				n2n.DialConfig,
				pcommon.Point,
				*slog.Logger,
			) (n2n.PointProbe, error) {
				return n2n.PointProbe{}, testCase.input
			}
			_, err := transport.Probe(
				context.Background(),
				testPeer("relay.example:3001", "operator"),
				testPoint(10, 0x10),
			)
			var transportErr *TransportError
			if got := errors.As(err, &transportErr); got != testCase.retryable {
				t.Fatalf("retryable classification = %t, error = %v", got, err)
			}
		})
	}
}

func TestDirectTransportOnlyClassifiesNetworkFailureAsRetryable(t *testing.T) {
	networkFailure := &net.OpError{
		Op:  "read",
		Net: "tcp",
		Err: errors.New("connection reset"),
	}
	transport := testDirectTransport()
	transport.runPeer = func(
		context.Context,
		string,
		n2n.DialConfig,
		[]n2n.ChainPoint,
		n2n.Handler,
		*slog.Logger,
	) error {
		return networkFailure
	}
	err := transport.Follow(
		context.Background(),
		testPeer("relay.example:3001", "operator"),
		[]n2n.ChainPoint{testChainPoint(10, 10, 0x10)},
		&n2nNoopHandler{},
	)
	var transportErr *TransportError
	if !errors.As(err, &transportErr) || !errors.Is(err, networkFailure) {
		t.Fatalf("error = %v", err)
	}
}

func TestDirectTransportDoesNotGuessUnclassifiedFailureIsRetryable(t *testing.T) {
	unclassified := errors.New("ambiguous protocol failure")
	transport := testDirectTransport()
	transport.runPeer = func(
		context.Context,
		string,
		n2n.DialConfig,
		[]n2n.ChainPoint,
		n2n.Handler,
		*slog.Logger,
	) error {
		return unclassified
	}
	err := transport.Follow(
		context.Background(),
		testPeer("relay.example:3001", "operator"),
		[]n2n.ChainPoint{testChainPoint(10, 10, 0x10)},
		&n2nNoopHandler{},
	)
	if !errors.Is(err, unclassified) {
		t.Fatalf("error = %v", err)
	}
	var transportErr *TransportError
	if errors.As(err, &transportErr) {
		t.Fatalf("ambiguous failure was marked retryable: %v", err)
	}
}

func TestDirectTransportPreservesPeerDataClassification(t *testing.T) {
	violation := &n2n.PeerDataViolation{
		Kind:  "header_body_mismatch",
		Point: testPoint(10, 0x10),
		Err:   errors.New("wrong hash"),
	}
	transport := testDirectTransport()
	transport.runPeer = func(
		context.Context,
		string,
		n2n.DialConfig,
		[]n2n.ChainPoint,
		n2n.Handler,
		*slog.Logger,
	) error {
		return violation
	}
	err := transport.Follow(
		context.Background(),
		testPeer("relay.example:3001", "operator"),
		[]n2n.ChainPoint{testChainPoint(10, 10, 0x10)},
		&n2nNoopHandler{},
	)
	var got *n2n.PeerDataViolation
	if !errors.As(err, &got) || got != violation {
		t.Fatalf("error = %T %v", err, err)
	}
	var transportErr *TransportError
	if errors.As(err, &transportErr) {
		t.Fatalf("peer-data violation was marked retryable: %v", err)
	}
}

func TestDirectTransportPreservesTerminalHandlerFailure(t *testing.T) {
	terminal := errors.New("ClickHouse publication failed")
	for name, returned := range map[string]error{"terminal": terminal} {
		t.Run(name, func(t *testing.T) {
			transport := testDirectTransport()
			transport.runPeer = func(
				context.Context,
				string,
				n2n.DialConfig,
				[]n2n.ChainPoint,
				n2n.Handler,
				*slog.Logger,
			) error {
				return returned
			}
			handler := &n2nNoopHandler{}
			if name == "terminal" {
				handler.terminal = terminal
			}
			err := transport.Follow(
				context.Background(),
				testPeer("relay.example:3001", "operator"),
				[]n2n.ChainPoint{testChainPoint(10, 10, 0x10)},
				handler,
			)
			if !errors.Is(err, returned) {
				t.Fatalf("error = %v, want %v", err, returned)
			}
			var transportErr *TransportError
			if errors.As(err, &transportErr) {
				t.Fatalf("non-transport outcome was marked retryable: %v", err)
			}
		})
	}
}

type n2nNoopHandler struct {
	terminal error
}

func (*n2nNoopHandler) Reconcile(context.Context, n2n.ChainPoint, n2n.Peer) error {
	return nil
}

func (*n2nNoopHandler) RollForward(
	context.Context,
	lcommon.Block,
	chainsync.Tip,
	n2n.Peer,
) error {
	return nil
}

func (*n2nNoopHandler) RollBackward(
	context.Context,
	n2n.ChainPoint,
	chainsync.Tip,
	n2n.Peer,
) error {
	return nil
}

func (h *n2nNoopHandler) terminalFailure() error {
	return h.terminal
}

func testDirectTransport() *DirectTransport {
	return &DirectTransport{
		config: n2n.DialConfig{},
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}
