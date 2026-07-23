package n2n

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/protocol/chainsync"
	pcommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

type BoundaryEvidenceStatus string

const (
	BoundaryAccepted    BoundaryEvidenceStatus = "accepted"
	BoundaryRejected    BoundaryEvidenceStatus = "rejected"
	BoundaryUnavailable BoundaryEvidenceStatus = "unavailable"
	BoundaryPeerData    BoundaryEvidenceStatus = "peer_data"
)

// BoundaryPeerEvidence records what one independently operated peer proved on
// a fresh N2N connection. Accepted means both that ChainSync selected the
// configured point and that BlockFetch returned the exact validation-enabled
// block body from which BlockNumber was decoded.
type BoundaryPeerEvidence struct {
	Peer        Peer
	Status      BoundaryEvidenceStatus
	BlockNumber uint64
	IsByronEBB  bool
	Failure     string
}

// BoundaryBootstrap is deliberately persistence-free. The caller must
// atomically initialize or verify its durable manifest from ChainPoint before
// it gives ChainPoint to Supervisor/RunPeer.
type BoundaryBootstrap struct {
	ChainPoint ChainPoint
	Source     BoundaryPeerEvidence
	Evidence   []BoundaryPeerEvidence
}

// BoundaryBootstrapError preserves all completed peer observations when the
// configured independent corroboration threshold cannot be met.
type BoundaryBootstrapError struct {
	Required int
	Evidence []BoundaryPeerEvidence
	Reason   string
}

func (e *BoundaryBootstrapError) Error() string {
	if e == nil {
		return "boundary bootstrap failed"
	}
	return fmt.Sprintf(
		"boundary bootstrap failed (%d independent acceptances required): %s",
		e.Required,
		e.Reason,
	)
}

type boundaryFetchFunc func(
	context.Context,
	Peer,
	DialConfig,
	pcommon.Point,
	*slog.Logger,
) (BoundaryPeerEvidence, error)

// BootstrapBoundary obtains a height-bearing start point without a local
// cardano-node. It performs a fresh ChainSync singleton membership proof and
// an exact, validation-enabled BlockFetch against every configured peer. A
// result is returned only when the same decoded height is corroborated by the
// requested number of independent operators.
func BootstrapBoundary(
	ctx context.Context,
	peers []Peer,
	corroboration int,
	cfg DialConfig,
	point pcommon.Point,
	logger *slog.Logger,
) (BoundaryBootstrap, error) {
	return bootstrapBoundary(
		ctx,
		peers,
		corroboration,
		cfg,
		point,
		logger,
		fetchBoundaryFromPeer,
	)
}

func bootstrapBoundary(
	ctx context.Context,
	peers []Peer,
	corroboration int,
	cfg DialConfig,
	point pcommon.Point,
	logger *slog.Logger,
	fetch boundaryFetchFunc,
) (BoundaryBootstrap, error) {
	if err := validateBootstrapInput(peers, corroboration, cfg, point, logger, fetch); err != nil {
		return BoundaryBootstrap{}, err
	}
	evidence := make([]BoundaryPeerEvidence, 0, len(peers))
	accepted := make([]BoundaryPeerEvidence, 0, corroboration)
	var height uint64
	heightSet := false
	var isByronEBB bool
	for _, peer := range peers {
		if err := ctx.Err(); err != nil {
			return BoundaryBootstrap{}, err
		}
		observation, err := fetch(ctx, peer, cfg, point, logger)
		if observation.Peer.Host == "" {
			observation.Peer = peer
		}
		if err != nil {
			switch {
			case errors.Is(err, context.Canceled),
				errors.Is(err, context.DeadlineExceeded):
				return BoundaryBootstrap{}, err
			case isNetworkFailure(err):
				observation.Status = BoundaryUnavailable
			default:
				var unavailable *RangeUnavailable
				var violation *PeerDataViolation
				var protocolClosed *ProtocolChannelClosed
				switch {
				case errors.As(err, &unavailable):
					observation.Status = BoundaryUnavailable
				case errors.As(err, &protocolClosed):
					observation.Status = BoundaryUnavailable
				case errors.As(err, &violation):
					observation.Status = BoundaryPeerData
				default:
					return BoundaryBootstrap{}, fmt.Errorf(
						"bootstrap boundary from %s: %w",
						peer.Host,
						err,
					)
				}
			}
			observation.Failure = err.Error()
			evidence = append(evidence, cloneBoundaryEvidence(observation))
			continue
		}
		evidence = append(evidence, cloneBoundaryEvidence(observation))
		if observation.Status != BoundaryAccepted {
			continue
		}
		if !heightSet {
			height = observation.BlockNumber
			isByronEBB = observation.IsByronEBB
			heightSet = true
		} else if observation.BlockNumber != height ||
			observation.IsByronEBB != isByronEBB {
			return BoundaryBootstrap{}, &BoundaryBootstrapError{
				Required: corroboration,
				Evidence: cloneBoundaryEvidenceSlice(evidence),
				Reason: fmt.Sprintf(
					"accepted peers decoded conflicting block metadata (%d, EBB=%t) and (%d, EBB=%t) for one exact point",
					height,
					isByronEBB,
					observation.BlockNumber,
					observation.IsByronEBB,
				),
			}
		}
		accepted = append(accepted, cloneBoundaryEvidence(observation))
	}
	if len(accepted) < corroboration {
		return BoundaryBootstrap{}, &BoundaryBootstrapError{
			Required: corroboration,
			Evidence: cloneBoundaryEvidenceSlice(evidence),
			Reason: fmt.Sprintf(
				"only %d independent peers accepted and fetched the exact point",
				len(accepted),
			),
		}
	}
	chainPoint := NewChainPoint(point, height)
	if isByronEBB {
		chainPoint = NewByronEBBChainPoint(point, height)
	}
	return BoundaryBootstrap{
		ChainPoint: chainPoint,
		Source:     cloneBoundaryEvidence(accepted[0]),
		Evidence:   cloneBoundaryEvidenceSlice(evidence),
	}, nil
}

func validateBootstrapInput(
	peers []Peer,
	corroboration int,
	cfg DialConfig,
	point pcommon.Point,
	logger *slog.Logger,
	fetch boundaryFetchFunc,
) error {
	switch {
	case logger == nil:
		return errors.New("nil logger")
	case fetch == nil:
		return errors.New("nil boundary fetcher")
	case isOrigin(point):
		return errors.New("boundary bootstrap requires a non-Origin point")
	case len(point.Hash) != 32:
		return fmt.Errorf("boundary point at slot %d has %d-byte hash", point.Slot, len(point.Hash))
	case corroboration < 2 || corroboration > len(peers):
		return fmt.Errorf("boundary corroboration must be in 2..%d", len(peers))
	}
	if err := validateDialConfig(cfg); err != nil {
		return err
	}
	operators := make(map[string]struct{}, len(peers))
	endpoints := make(map[string]struct{}, len(peers))
	for _, peer := range peers {
		host, port, err := net.SplitHostPort(strings.TrimSpace(peer.Host))
		if err != nil || host == "" || port == "" {
			return fmt.Errorf("invalid bootstrap peer endpoint %q", peer.Host)
		}
		operator := strings.ToLower(strings.TrimSpace(peer.Operator))
		if operator == "" {
			return fmt.Errorf("bootstrap peer %q has no operator identity", peer.Host)
		}
		endpoint := strings.ToLower(net.JoinHostPort(host, port))
		if _, duplicate := endpoints[endpoint]; duplicate {
			return fmt.Errorf("duplicate bootstrap peer endpoint %q", peer.Host)
		}
		endpoints[endpoint] = struct{}{}
		if _, duplicate := operators[operator]; duplicate {
			return fmt.Errorf("duplicate bootstrap operator %q", peer.Operator)
		}
		operators[operator] = struct{}{}
	}
	if len(operators) < corroboration {
		return fmt.Errorf(
			"boundary corroboration %d exceeds %d independent operators",
			corroboration,
			len(operators),
		)
	}
	return nil
}

func fetchBoundaryFromPeer(
	ctx context.Context,
	peer Peer,
	cfg DialConfig,
	point pcommon.Point,
	logger *slog.Logger,
) (BoundaryPeerEvidence, error) {
	cfg.Operator = peer.Operator
	unexpectedRollForward := func(
		chainsync.CallbackContext,
		uint,
		any,
		chainsync.Tip,
	) error {
		return errors.New("unexpected roll-forward during boundary bootstrap")
	}
	unexpectedRollBackward := func(
		chainsync.CallbackContext,
		pcommon.Point,
		chainsync.Tip,
	) error {
		return errors.New("unexpected rollback during boundary bootstrap")
	}
	conn, asyncErr, err := dial(
		peer.Host,
		cfg,
		unexpectedRollForward,
		unexpectedRollBackward,
		nil,
		nil,
		logger,
	)
	if err != nil {
		return BoundaryPeerEvidence{Peer: peer}, err
	}
	defer conn.Close()
	actualPeer := peer
	version, _ := conn.ProtocolVersion()
	actualPeer.N2NVersion = version
	if remote := conn.Id().RemoteAddr; remote != nil {
		actualPeer.Address = remote.String()
	}
	stopCancellation := make(chan struct{})
	defer close(stopCancellation)
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.Close()
		case <-stopCancellation:
		}
	}()

	_, _, err = conn.ChainSync().Client.GetAvailableBlockRange(
		[]pcommon.Point{point},
	)
	if errors.Is(err, chainsync.ErrIntersectNotFound) {
		tip, tipErr := conn.ChainSync().Client.GetCurrentTip()
		if tipErr != nil {
			return BoundaryPeerEvidence{Peer: actualPeer}, fmt.Errorf(
				"read rejecting peer tip: %w",
				tipErr,
			)
		}
		actualPeer.Tip = tip
		return BoundaryPeerEvidence{
			Peer:   actualPeer,
			Status: BoundaryRejected,
		}, nil
	}
	if err != nil {
		return BoundaryPeerEvidence{Peer: actualPeer}, fmt.Errorf(
			"probe exact boundary point: %w",
			err,
		)
	}
	block, err := conn.BlockFetch().Client.GetBlock(point)
	if err != nil {
		if async := pollBoundaryAsyncError(asyncErr, point); async != nil {
			return BoundaryPeerEvidence{Peer: actualPeer}, async
		}
		if isNetworkFailure(err) {
			return BoundaryPeerEvidence{Peer: actualPeer}, fmt.Errorf(
				"fetch exact boundary block: %w",
				err,
			)
		}
		var validationError *lcommon.ValidationError
		if errors.As(err, &validationError) {
			return BoundaryPeerEvidence{Peer: actualPeer}, peerDataViolation(
				"boundary_block_validation",
				point,
				err,
			)
		}
		return BoundaryPeerEvidence{Peer: actualPeer}, &RangeUnavailable{
			Start: point,
			End:   point,
			Err:   err,
		}
	}
	if err := validateBoundaryBlock(point, block); err != nil {
		return BoundaryPeerEvidence{Peer: actualPeer}, err
	}
	tip, err := conn.ChainSync().Client.GetCurrentTip()
	if err != nil {
		return BoundaryPeerEvidence{Peer: actualPeer}, fmt.Errorf(
			"read accepting peer tip: %w",
			err,
		)
	}
	actualPeer.Tip = tip
	if async := pollBoundaryAsyncError(asyncErr, point); async != nil {
		return BoundaryPeerEvidence{Peer: actualPeer}, async
	}
	return BoundaryPeerEvidence{
		Peer:        actualPeer,
		Status:      BoundaryAccepted,
		BlockNumber: block.BlockNumber(),
		IsByronEBB:  isByronEpochBoundaryHeader(block.Header()),
	}, nil
}

func validateBoundaryBlock(point pcommon.Point, block lcommon.Block) error {
	if block == nil ||
		block.SlotNumber() != point.Slot ||
		!bytes.Equal(block.Hash().Bytes(), point.Hash) {
		return peerDataViolation(
			"boundary_point_mismatch",
			point,
			errors.New("BlockFetch body does not match configured boundary"),
		)
	}
	return nil
}

func pollBoundaryAsyncError(
	asyncErr <-chan error,
	point pcommon.Point,
) error {
	select {
	case err, ok := <-asyncErr:
		if !ok {
			return &ProtocolChannelClosed{}
		}
		return classifyPeerProtocolError(err, point)
	default:
		return nil
	}
}

func cloneBoundaryEvidence(value BoundaryPeerEvidence) BoundaryPeerEvidence {
	ret := value
	if value.Peer.Tip != nil {
		tip := *value.Peer.Tip
		tip.Point = pcommon.NewPoint(
			value.Peer.Tip.Point.Slot,
			bytes.Clone(value.Peer.Tip.Point.Hash),
		)
		ret.Peer.Tip = &tip
	}
	return ret
}

func cloneBoundaryEvidenceSlice(
	values []BoundaryPeerEvidence,
) []BoundaryPeerEvidence {
	ret := make([]BoundaryPeerEvidence, len(values))
	for index := range values {
		ret[index] = cloneBoundaryEvidence(values[index])
	}
	return ret
}
