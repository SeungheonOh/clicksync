package ingest

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	pcommon "github.com/blinklabs-io/gouroboros/protocol/common"

	"clicksync/internal/n2n"
)

func TestBoundaryBootstrapRetriesTransientAvailabilityThenSucceeds(t *testing.T) {
	calls := 0
	peers := []n2n.Peer{
		{Host: "a", Operator: "operator-a"},
		{Host: "b", Operator: "operator-b"},
	}
	expected := n2n.BoundaryBootstrap{
		ChainPoint: n2n.NewChainPoint(
			pcommon.NewPoint(10, adapterHash(0x10).Bytes()),
			10,
		),
	}
	bootstrap := func(
		context.Context,
		[]n2n.Peer,
		int,
		n2n.DialConfig,
		pcommon.Point,
		*slog.Logger,
	) (n2n.BoundaryBootstrap, error) {
		calls++
		if calls <= 3 {
			return n2n.BoundaryBootstrap{}, &n2n.BoundaryBootstrapError{
				Required: 2,
				Evidence: []n2n.BoundaryPeerEvidence{
					{Peer: peers[0], Status: n2n.BoundaryAccepted},
					{Peer: peers[1], Status: n2n.BoundaryUnavailable},
				},
				Reason: "only one available corroboration",
			}
		}
		return expected, nil
	}
	var waits []time.Duration
	result, err := bootstrapBoundaryWithRetry(
		context.Background(),
		peers,
		2,
		n2n.DialConfig{},
		expected.ChainPoint.Point,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		bootstrap,
		func(_ context.Context, duration time.Duration) error {
			waits = append(waits, duration)
			return nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if calls != 4 || !samePublicationPoint(
		publicationPoint(result.ChainPoint),
		publicationPoint(expected.ChainPoint),
	) {
		t.Fatalf("calls=%d result=%#v", calls, result)
	}
	expectedWaits := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second}
	if len(waits) != len(expectedWaits) {
		t.Fatalf("waits = %#v", waits)
	}
	for index := range waits {
		if waits[index] != expectedWaits[index] {
			t.Fatalf("waits = %#v", waits)
		}
	}
}

func TestBoundaryBootstrapRejectionAndPeerDataAreTerminal(t *testing.T) {
	peers := []n2n.Peer{
		{Host: "a", Operator: "operator-a"},
		{Host: "b", Operator: "operator-b"},
	}
	for name, status := range map[string]n2n.BoundaryEvidenceStatus{
		"configured point rejection": n2n.BoundaryRejected,
		"peer data violation":        n2n.BoundaryPeerData,
	} {
		t.Run(name, func(t *testing.T) {
			calls := 0
			expectedErr := &n2n.BoundaryBootstrapError{
				Required: 2,
				Evidence: []n2n.BoundaryPeerEvidence{
					{Peer: peers[0], Status: n2n.BoundaryAccepted},
					{Peer: peers[1], Status: status},
				},
				Reason: name,
			}
			_, err := bootstrapBoundaryWithRetry(
				context.Background(),
				peers,
				2,
				n2n.DialConfig{},
				pcommon.NewPoint(10, adapterHash(0x10).Bytes()),
				slog.New(slog.NewTextHandler(io.Discard, nil)),
				func(
					context.Context,
					[]n2n.Peer,
					int,
					n2n.DialConfig,
					pcommon.Point,
					*slog.Logger,
				) (n2n.BoundaryBootstrap, error) {
					calls++
					return n2n.BoundaryBootstrap{}, expectedErr
				},
				func(context.Context, time.Duration) error {
					t.Fatal("terminal bootstrap entered backoff")
					return nil
				},
			)
			if !errors.Is(err, expectedErr) || calls != 1 {
				t.Fatalf("calls=%d error=%v", calls, err)
			}
		})
	}
}

func TestBoundaryBootstrapContextCancellationInterruptsBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	peers := []n2n.Peer{
		{Host: "a", Operator: "operator-a"},
		{Host: "b", Operator: "operator-b"},
	}
	waits := 0
	_, err := bootstrapBoundaryWithRetry(
		ctx,
		peers,
		2,
		n2n.DialConfig{},
		pcommon.NewPoint(10, adapterHash(0x10).Bytes()),
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		func(
			context.Context,
			[]n2n.Peer,
			int,
			n2n.DialConfig,
			pcommon.Point,
			*slog.Logger,
		) (n2n.BoundaryBootstrap, error) {
			return n2n.BoundaryBootstrap{}, &n2n.BoundaryBootstrapError{
				Required: 2,
				Evidence: []n2n.BoundaryPeerEvidence{
					{Peer: peers[0], Status: n2n.BoundaryUnavailable},
					{Peer: peers[1], Status: n2n.BoundaryUnavailable},
				},
				Reason: "all peers unavailable",
			}
		},
		func(ctx context.Context, _ time.Duration) error {
			waits++
			cancel()
			return ctx.Err()
		},
	)
	if !errors.Is(err, context.Canceled) || waits != 1 {
		t.Fatalf("waits=%d error=%v", waits, err)
	}
}

func TestBoundaryBootstrapUnknownEvidenceOperatorFailsClosed(t *testing.T) {
	peers := []n2n.Peer{
		{Host: "a", Operator: "operator-a"},
		{Host: "b", Operator: "operator-b"},
		{Host: "c", Operator: "operator-c"},
	}
	expectedErr := &n2n.BoundaryBootstrapError{
		Required: 2,
		Kind:     n2n.BoundaryInsufficient,
		Evidence: []n2n.BoundaryPeerEvidence{
			{Peer: peers[0], Status: n2n.BoundaryAccepted},
			{
				Peer:   n2n.Peer{Host: "unknown", Operator: "spoofed"},
				Status: n2n.BoundaryPeerData,
			},
			{Peer: peers[1], Status: n2n.BoundaryUnavailable},
		},
		Reason: "spoofed evidence operator",
	}
	calls := 0
	_, err := bootstrapBoundaryWithRetry(
		context.Background(),
		peers,
		2,
		n2n.DialConfig{},
		pcommon.NewPoint(10, adapterHash(0x10).Bytes()),
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		func(
			context.Context,
			[]n2n.Peer,
			int,
			n2n.DialConfig,
			pcommon.Point,
			*slog.Logger,
		) (n2n.BoundaryBootstrap, error) {
			calls++
			return n2n.BoundaryBootstrap{}, expectedErr
		},
		func(context.Context, time.Duration) error {
			t.Fatal("unknown evidence operator entered backoff")
			return nil
		},
	)
	if !errors.Is(err, expectedErr) || calls != 1 {
		t.Fatalf("calls=%d error=%v", calls, err)
	}
}

func TestBoundaryBootstrapQuarantinesOneOperatorThenRemainingPeersRecover(t *testing.T) {
	peers := []n2n.Peer{
		{Host: "a:3001", Operator: "operator-a"},
		{Host: "b:3001", Operator: "operator-b"},
		{Host: "c:3001", Operator: "operator-c"},
	}
	calls := 0
	bootstrap := func(
		_ context.Context,
		got []n2n.Peer,
		_ int,
		_ n2n.DialConfig,
		point pcommon.Point,
		_ *slog.Logger,
	) (n2n.BoundaryBootstrap, error) {
		calls++
		switch calls {
		case 1:
			if len(got) != 3 {
				t.Fatalf("initial peers = %#v", got)
			}
			return n2n.BoundaryBootstrap{}, &n2n.BoundaryBootstrapError{
				Required: 2,
				Kind:     n2n.BoundaryInsufficient,
				Evidence: []n2n.BoundaryPeerEvidence{
					{Peer: peers[0], Status: n2n.BoundaryPeerData},
					{Peer: peers[1], Status: n2n.BoundaryUnavailable},
					{Peer: peers[2], Status: n2n.BoundaryUnavailable},
				},
				Reason: "one invalid peer and two transiently unavailable",
			}
		case 2:
			if len(got) != 2 ||
				got[0].Operator != "operator-b" ||
				got[1].Operator != "operator-c" {
				t.Fatalf("filtered peers = %#v", got)
			}
			return n2n.BoundaryBootstrap{}, &n2n.BoundaryBootstrapError{
				Required: 2,
				Kind:     n2n.BoundaryInsufficient,
				Evidence: []n2n.BoundaryPeerEvidence{
					{Peer: peers[1], Status: n2n.BoundaryUnavailable},
					{Peer: peers[2], Status: n2n.BoundaryUnavailable},
				},
				Reason: "remaining peers still unavailable",
			}
		default:
			return n2n.BoundaryBootstrap{
				ChainPoint: n2n.NewChainPoint(point, 10),
			}, nil
		}
	}
	waits := 0
	if _, err := bootstrapBoundaryWithRetry(
		context.Background(),
		peers,
		2,
		n2n.DialConfig{},
		pcommon.NewPoint(10, adapterHash(0x10).Bytes()),
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		bootstrap,
		func(context.Context, time.Duration) error {
			waits++
			return nil
		},
	); err != nil {
		t.Fatal(err)
	}
	if calls != 3 || waits != 2 {
		t.Fatalf("calls=%d waits=%d", calls, waits)
	}
}

func TestBoundaryBootstrapQuarantineExhaustionIsTerminal(t *testing.T) {
	peers := []n2n.Peer{
		{Host: "a:3001", Operator: "operator-a"},
		{Host: "b:3001", Operator: "operator-b"},
	}
	calls := 0
	_, err := bootstrapBoundaryWithRetry(
		context.Background(),
		peers,
		2,
		n2n.DialConfig{},
		pcommon.NewPoint(10, adapterHash(0x10).Bytes()),
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		func(
			context.Context,
			[]n2n.Peer,
			int,
			n2n.DialConfig,
			pcommon.Point,
			*slog.Logger,
		) (n2n.BoundaryBootstrap, error) {
			calls++
			return n2n.BoundaryBootstrap{}, &n2n.BoundaryBootstrapError{
				Required: 2,
				Kind:     n2n.BoundaryInsufficient,
				Evidence: []n2n.BoundaryPeerEvidence{
					{Peer: peers[0], Status: n2n.BoundaryRejected},
					{Peer: peers[1], Status: n2n.BoundaryUnavailable},
				},
				Reason: "one rejection exhausts threshold",
			}
		},
		func(context.Context, time.Duration) error {
			t.Fatal("exhausted quarantine entered backoff")
			return nil
		},
	)
	if err == nil || calls != 1 {
		t.Fatalf("calls=%d error=%v", calls, err)
	}
}
