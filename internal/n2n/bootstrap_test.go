package n2n

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	pcommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

func TestBootstrapBoundaryReturnsCorroboratedHeightAndExactSource(t *testing.T) {
	point := testPoint(193253841, 0xe9)
	peers := bootstrapPeers()
	fetch := func(
		_ context.Context,
		peer Peer,
		_ DialConfig,
		got pcommon.Point,
		_ *slog.Logger,
	) (BoundaryPeerEvidence, error) {
		if !pointsEqual(got, point) {
			t.Fatalf("fetch point = %#v, want %#v", got, point)
		}
		switch peer.Operator {
		case "one":
			peer.Address = "192.0.2.1:3001"
			peer.N2NVersion = 14
			return BoundaryPeerEvidence{
				Peer:        peer,
				Status:      BoundaryAccepted,
				BlockNumber: 13715435,
			}, nil
		case "two":
			peer.Address = "192.0.2.2:3001"
			peer.N2NVersion = 14
			return BoundaryPeerEvidence{
				Peer:        peer,
				Status:      BoundaryAccepted,
				BlockNumber: 13715435,
			}, nil
		default:
			return BoundaryPeerEvidence{
				Peer:   peer,
				Status: BoundaryRejected,
			}, nil
		}
	}
	got, err := bootstrapBoundary(
		context.Background(),
		peers,
		2,
		bootstrapDialConfig(),
		point,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		fetch,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !pointsEqual(got.ChainPoint.Point, point) ||
		got.ChainPoint.BlockNumber != 13715435 {
		t.Fatalf("chain point = %#v", got.ChainPoint)
	}
	if got.Source.Peer.Operator != "one" ||
		got.Source.Peer.Address != "192.0.2.1:3001" ||
		got.Source.Peer.N2NVersion != 14 {
		t.Fatalf("source = %#v", got.Source)
	}
	if len(got.Evidence) != 3 ||
		got.Evidence[0].Status != BoundaryAccepted ||
		got.Evidence[1].Status != BoundaryAccepted ||
		got.Evidence[2].Status != BoundaryRejected {
		t.Fatalf("evidence = %#v", got.Evidence)
	}
}

func TestBootstrapBoundaryPreservesUnavailableEvidence(t *testing.T) {
	point := testPoint(10, 0x10)
	peers := bootstrapPeers()
	fetch := func(
		_ context.Context,
		peer Peer,
		_ DialConfig,
		_ pcommon.Point,
		_ *slog.Logger,
	) (BoundaryPeerEvidence, error) {
		if peer.Operator == "one" {
			return BoundaryPeerEvidence{Peer: peer}, io.EOF
		}
		return BoundaryPeerEvidence{
			Peer:        peer,
			Status:      BoundaryAccepted,
			BlockNumber: 9,
		}, nil
	}
	got, err := bootstrapBoundary(
		context.Background(),
		peers,
		2,
		bootstrapDialConfig(),
		point,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		fetch,
	)
	if err != nil {
		t.Fatal(err)
	}
	if got.Evidence[0].Status != BoundaryUnavailable ||
		!strings.Contains(got.Evidence[0].Failure, "EOF") {
		t.Fatalf("unavailable evidence = %#v", got.Evidence[0])
	}
	if got.Source.Peer.Operator != "two" {
		t.Fatalf("source = %#v", got.Source)
	}
}

func TestBootstrapBoundaryFailsClosedOnHeightDisagreement(t *testing.T) {
	point := testPoint(10, 0x10)
	peers := bootstrapPeers()
	fetch := func(
		_ context.Context,
		peer Peer,
		_ DialConfig,
		_ pcommon.Point,
		_ *slog.Logger,
	) (BoundaryPeerEvidence, error) {
		height := uint64(9)
		if peer.Operator == "two" {
			height = 10
		}
		return BoundaryPeerEvidence{
			Peer:        peer,
			Status:      BoundaryAccepted,
			BlockNumber: height,
		}, nil
	}
	_, err := bootstrapBoundary(
		context.Background(),
		peers,
		2,
		bootstrapDialConfig(),
		point,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		fetch,
	)
	var bootstrapErr *BoundaryBootstrapError
	if !errors.As(err, &bootstrapErr) ||
		!strings.Contains(bootstrapErr.Reason, "conflicting block metadata") ||
		len(bootstrapErr.Evidence) != 2 {
		t.Fatalf("height disagreement error = %T %v", err, err)
	}
}

func TestBootstrapBoundaryCarriesAndCorroboratesByronEBB(t *testing.T) {
	point := testPoint(10, 0x10)
	peers := bootstrapPeers()[:2]
	fetches := 0
	got, err := bootstrapBoundary(
		context.Background(),
		peers,
		2,
		bootstrapDialConfig(),
		point,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		func(
			_ context.Context,
			peer Peer,
			_ DialConfig,
			_ pcommon.Point,
			_ *slog.Logger,
		) (BoundaryPeerEvidence, error) {
			fetches++
			return BoundaryPeerEvidence{
				Peer:        peer,
				Status:      BoundaryAccepted,
				BlockNumber: 9,
				IsByronEBB:  true,
			}, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if fetches != 2 || !got.ChainPoint.IsByronEBB ||
		!got.Source.IsByronEBB {
		t.Fatalf("Byron EBB bootstrap = %#v, fetches=%d", got, fetches)
	}

	fetches = 0
	_, err = bootstrapBoundary(
		context.Background(),
		peers,
		2,
		bootstrapDialConfig(),
		point,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		func(
			_ context.Context,
			peer Peer,
			_ DialConfig,
			_ pcommon.Point,
			_ *slog.Logger,
		) (BoundaryPeerEvidence, error) {
			fetches++
			return BoundaryPeerEvidence{
				Peer:        peer,
				Status:      BoundaryAccepted,
				BlockNumber: 9,
				IsByronEBB:  fetches == 1,
			}, nil
		},
	)
	var bootstrapErr *BoundaryBootstrapError
	if !errors.As(err, &bootstrapErr) ||
		!strings.Contains(bootstrapErr.Reason, "conflicting block metadata") {
		t.Fatalf("Byron EBB metadata disagreement = %T %v", err, err)
	}
}

func TestBootstrapBoundaryRequiresExactIndependentInputs(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	point := testPoint(10, 0x10)
	tests := []struct {
		name  string
		peers []Peer
		point pcommon.Point
	}{
		{
			name:  "origin",
			peers: bootstrapPeers(),
			point: pcommon.NewPointOrigin(),
		},
		{
			name: "duplicate operator",
			peers: []Peer{
				{Host: "one.example:3001", Operator: "same"},
				{Host: "two.example:3001", Operator: "SAME"},
			},
			point: point,
		},
		{
			name: "duplicate endpoint",
			peers: []Peer{
				{Host: "ONE.example:3001", Operator: "one"},
				{Host: "one.EXAMPLE:3001", Operator: "two"},
			},
			point: point,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			called := false
			_, err := bootstrapBoundary(
				context.Background(),
				test.peers,
				2,
				bootstrapDialConfig(),
				test.point,
				logger,
				func(
					context.Context,
					Peer,
					DialConfig,
					pcommon.Point,
					*slog.Logger,
				) (BoundaryPeerEvidence, error) {
					called = true
					return BoundaryPeerEvidence{}, nil
				},
			)
			if err == nil {
				t.Fatal("invalid bootstrap input accepted")
			}
			if called {
				t.Fatal("fetch called for invalid bootstrap input")
			}
		})
	}
}

func TestBootstrapBoundaryRejectsWrongNonzeroNetworkMagic(t *testing.T) {
	config := bootstrapDialConfig()
	config.NetworkMagic = 1
	called := false
	_, err := bootstrapBoundary(
		context.Background(),
		bootstrapPeers(),
		2,
		config,
		testPoint(10, 0x10),
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		func(
			context.Context,
			Peer,
			DialConfig,
			pcommon.Point,
			*slog.Logger,
		) (BoundaryPeerEvidence, error) {
			called = true
			return BoundaryPeerEvidence{}, nil
		},
	)
	if err == nil ||
		!strings.Contains(err.Error(), "mainnet magic") ||
		called {
		t.Fatalf("wrong-magic bootstrap = called:%t error:%v", called, err)
	}
}

func TestBootstrapBlockFetchKeepsBodyHashValidation(t *testing.T) {
	config, err := newBlockFetchConfig(bootstrapDialConfig(), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if config.SkipBlockValidation {
		t.Fatal("boundary bootstrap disabled upstream block/body-hash validation")
	}
}

func TestBootstrapBoundaryHonorsCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	called := false
	_, err := bootstrapBoundary(
		ctx,
		bootstrapPeers(),
		2,
		bootstrapDialConfig(),
		testPoint(10, 0x10),
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		func(
			context.Context,
			Peer,
			DialConfig,
			pcommon.Point,
			*slog.Logger,
		) (BoundaryPeerEvidence, error) {
			called = true
			return BoundaryPeerEvidence{}, nil
		},
	)
	if !errors.Is(err, context.Canceled) || called {
		t.Fatalf("canceled bootstrap = called:%t error:%v", called, err)
	}
}

func TestBoundaryProtocolChannelClosureIsTypedTransportTermination(t *testing.T) {
	asyncErr := make(chan error)
	close(asyncErr)
	err := pollBoundaryAsyncError(asyncErr, testPoint(10, 0x10))
	var closed *ProtocolChannelClosed
	if !errors.As(err, &closed) {
		t.Fatalf("error = %T %v, want ProtocolChannelClosed", err, err)
	}
}

func bootstrapPeers() []Peer {
	return []Peer{
		{Host: "one.example:3001", Operator: "one"},
		{Host: "two.example:3001", Operator: "two"},
		{Host: "three.example:3001", Operator: "three"},
	}
}

func bootstrapDialConfig() DialConfig {
	return DialConfig{
		NetworkMagic:    MainnetNetworkMagic,
		QueueCapacity:   4,
		HeaderBatchSize: 32,
		DialTimeout:     time.Second,
		BlockTimeout:    time.Second,
	}
}
