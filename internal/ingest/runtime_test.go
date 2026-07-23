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
					{Status: n2n.BoundaryAccepted},
					{Status: n2n.BoundaryUnavailable},
				},
				Reason: "only one available corroboration",
			}
		}
		return expected, nil
	}
	var waits []time.Duration
	result, err := bootstrapBoundaryWithRetry(
		context.Background(),
		[]n2n.Peer{{Host: "a"}, {Host: "b"}},
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
	for name, status := range map[string]n2n.BoundaryEvidenceStatus{
		"configured point rejection": n2n.BoundaryRejected,
		"peer data violation":        n2n.BoundaryPeerData,
	} {
		t.Run(name, func(t *testing.T) {
			calls := 0
			expectedErr := &n2n.BoundaryBootstrapError{
				Required: 2,
				Evidence: []n2n.BoundaryPeerEvidence{
					{Status: n2n.BoundaryAccepted},
					{Status: status},
				},
				Reason: name,
			}
			_, err := bootstrapBoundaryWithRetry(
				context.Background(),
				nil,
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
	waits := 0
	_, err := bootstrapBoundaryWithRetry(
		ctx,
		nil,
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
					{Status: n2n.BoundaryUnavailable},
					{Status: n2n.BoundaryUnavailable},
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
